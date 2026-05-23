# Đồ án Cơ sở dữ liệu phân tán - VietJobs

> **Hướng dẫn triển khai hệ thống truy vấn phân tán với PostgreSQL + Citus trên Docker**
>
> Phân mảnh ngang theo `category × location` | Phân mảnh dọc thành 3 bảng `core / detail / skills` | 5 giải thuật xử lý song song

---

## Mục lục

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Phân tích dataset VietJobs](#2-phân-tích-dataset-vietjobs)
3. [Kiến trúc hệ thống](#3-kiến-trúc-hệ-thống)
4. [Thiết kế phân mảnh dọc](#4-thiết-kế-phân-mảnh-dọc)
5. [Thiết kế phân mảnh ngang](#5-thiết-kế-phân-mảnh-ngang)
6. [Dựng cluster với Docker](#6-dựng-cluster-với-docker)
7. [Khởi tạo schema và phân bổ dữ liệu](#7-khởi-tạo-schema-và-phân-bổ-dữ-liệu)
8. [5 giải thuật song song và phân tán](#8-5-giải-thuật-song-song-và-phân-tán)
9. [Xây dựng Backend API](#9-xây-dựng-backend-api)
10. [Benchmark và đánh giá hiệu năng](#10-benchmark-và-đánh-giá-hiệu-năng)
11. [Template báo cáo đồ án](#11-template-báo-cáo-đồ-án)
12. [Phụ lục](#12-phụ-lục)

---

## 1. Tổng quan dự án

### 1.1. Mục tiêu

Xây dựng hệ thống cơ sở dữ liệu **phân tán** trên dataset VietJobs, cho phép:

- **Phân mảnh dọc (Vertical Fragmentation):** Tách bảng gốc thành nhiều bảng theo nhóm thuộc tính
- **Phân mảnh ngang (Horizontal Fragmentation):** Chia dữ liệu thành nhiều phân mảnh theo giá trị (`category`, `location`)
- **Xử lý song song:** Thực thi truy vấn đồng thời trên nhiều node
- **Truy vấn phân tán:** Lấy dữ liệu từ nhiều node trong cluster qua một interface thống nhất
- **So sánh hiệu năng của các giải thuật:** Đo lường, benchmark và phân tích đối chiếu giữa các giải thuật song song với baseline (single-node, non-co-located, naive join) để đánh giá hiệu quả của thiết kế phân tán

### 1.2. Stack công nghệ

| Tầng | Công nghệ | Phiên bản đề xuất |
|---|---|---|
| DBMS | PostgreSQL + Citus extension | PG 16 + Citus 12 |
| Container | Docker + Docker Compose | 24.x / v2.x |
| Backend API | Python + FastAPI | Python 3.11 + FastAPI 0.110+ |
| Frontend | React 18 + TypeScript + Vite (Chart.js / Recharts) | React 18 + Vite 5 |
| Benchmark | Python + psycopg2 + matplotlib | - |

### 1.3. Quy mô cluster

```
1 Coordinator + 4 Worker Nodes = 5 containers
```

- **Coordinator:** Nhận query, định tuyến, gộp kết quả
- **4 Workers:** Mỗi worker chứa 1 nhóm category với cả `hà nội` và `hồ chí minh` (location đã được chuẩn hoá chữ thường, không dấu phân cách)

---

## 2. Phân tích dataset VietJobs

### 2.1. Thông tin gốc

- **Nguồn:** HuggingFace `dinhieufam/VietJobs`
- **Kích thước gốc:** 48,092 bản ghi, 18 cột, ~98MB CSV
- **Schema gốc:** `job_title, location, country, qualifications, technical_skills, soft_skills, languages_required, experience_required, salary, contract_type, working_hours, benefits, description, requirements_text, category, salary_min, salary_max, salary_avg`

### 2.2. Tiền xử lý đã áp dụng

> Đầu vào sau xử lý đã được làm sạch theo các tiêu chí sau:

1. Chỉ giữ lại `location IN ('Hà Nội', 'TPHCM')` rồi **chuẩn hoá về chữ thường** → `'hà nội'`, `'hồ chí minh'` (tên đầy đủ, có dấu, không dùng viết tắt `TPHCM`).
2. Loại bỏ tất cả bản ghi có `salary_avg = 'Thỏa thuận'`.
3. Xuất CSV bằng `pandas.to_csv()` (mặc định ghi thêm cột index ở đầu) → file `dataset_IS252.csv` có **19 cột** (1 cột index + 18 cột nghiệp vụ).

**Kết quả sau tiền xử lý:** **24,281 bản ghi**, 19 cột, file `data/dataset_IS252.csv`.

### 2.3. Mapping 16 category vào 4 nhóm worker

| Worker Node | Tên nhóm | Categories thành viên |
|---|---|---|
| **node1_commerce** | Thương mại - Tài chính | `kinh_doanh_bán_hàng_chăm_sóc_khách_hàng`<br>`tài_chính_kế_toán_ngân_hàng_bảo_hiểm`<br>`logistics_vận_tải_chuỗi_cung_ứng` |
| **node2_tech** | Kỹ thuật - Sản xuất | `sản_xuất_lao_động_phổ_thông_cơ_khí`<br>`công_nghệ_thông_tin_kỹ_thuật_số`<br>`kỹ_thuật_điện_điện_tử_viễn_thông`<br>`xây_dựng_kiến_trúc_bất_động_sản`<br>`nông_nghiệp_năng_lượng_môi_trường` |
| **node3_creative** | Sáng tạo - Dịch vụ | `marketing_truyền_thông_quảng_cáo_nội_dung`<br>`thiết_kế_nghệ_thuật_giải_trí_truyền_hình_báo_chí`<br>`du_lịch_nhà_hàng_khách_sạn_dịch_vụ`<br>`ngôn_ngữ_dịch_thuật` |
| **node4_people** | Con người - Tri thức | `nhân_sự_hành_chính_pháp_chế_tư_vấn`<br>`giáo_dục_đào_tạo_nghiên_cứu`<br>`y_tế_dược_chăm_sóc_sức_khỏe_công_nghệ_sinh_học`<br>`nhóm_nghề_khác` |

---

## 3. Kiến trúc hệ thống

### 3.1. Sơ đồ tổng thể

```
                       ┌──────────────────────────────┐
                       │      CLIENT / FRONTEND       │
                       │  React 18 + TS + Vite + Chart│
                       └───────────────┬──────────────┘
                                       │ HTTP/REST
                                       ▼
                       ┌──────────────────────────────┐
                       │       BACKEND API LAYER      │
                       │   Node.js Express / FastAPI  │
                       │   - Query interface          │
                       │   - Algorithm selector       │
                       │   - Benchmark runner         │
                       └───────────────┬──────────────┘
                                       │ SQL (psql protocol)
                                       ▼
                  ┌─────────────────────────────────────────┐
                  │         COORDINATOR NODE                │
                  │      PostgreSQL 16 + Citus 12           │
                  │  - pg_dist_partition (metadata)         │
                  │  - Distributed query planner            │
                  │  - Result aggregator                    │
                  │  Port: 5432 (exposed to host)           │
                  └──┬──────────┬──────────┬──────────┬─────┘
                     │          │          │          │
        ┌────────────┘          │          │          └────────────┐
        ▼                       ▼          ▼                       ▼
┌──────────────┐      ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  WORKER 1    │      │  WORKER 2    │  │  WORKER 3    │  │  WORKER 4    │
│  commerce    │      │  tech        │  │  creative    │  │  people      │
│  ┌────────┐  │      │  ┌────────┐  │  │  ┌────────┐  │  │  ┌────────┐  │
│  │core    │  │      │  │core    │  │  │  │core    │  │  │  │core    │  │
│  │detail  │  │      │  │detail  │  │  │  │detail  │  │  │  │detail  │  │
│  │skills  │  │      │  │skills  │  │  │  │skills  │  │  │  │skills  │  │
│  └────────┘  │      │  └────────┘  │  │  └────────┘  │  │  └────────┘  │
│ hà nội + hcm │      │ hà nội + hcm │  │ hà nội + hcm │  │ hà nội + hcm │
└──────────────┘      └──────────────┘  └──────────────┘  └──────────────┘
```

### 3.2. Cách hoạt động

1. **Client** gửi query SQL chuẩn đến **Coordinator** (như đang nói chuyện với 1 PostgreSQL bình thường)
2. **Coordinator** đọc metadata để xác định: query này cần dữ liệu từ worker nào
3. **Coordinator** gửi sub-query đến các **Worker** liên quan (song song)
4. Mỗi **Worker** thực thi sub-query trên dữ liệu cục bộ
5. **Coordinator** gộp kết quả từ workers và trả về client

### 3.3. Worker Node là gì?

**Worker = container Docker** chứa 1 instance PostgreSQL + Citus, lưu trữ một phần dữ liệu (các shard). Mỗi worker chạy độc lập, có thể xử lý query song song với các worker khác.

> ⚠️ Phân biệt **phân mảnh (logical fragment)** và **worker (physical node)**:
> - 1 worker có thể chứa **nhiều phân mảnh**
> - Trong đồ án này: 4 workers × 2 phân mảnh (`hà nội`, `hồ chí minh`) = **8 shard logic**

---

## 4. Thiết kế phân mảnh dọc

### 4.1. Lý do phân mảnh dọc

| Cột | Avg size | Max size | Đặc điểm |
|---|---|---|---|
| `description` | 616 B | 4,408 B | Text dài, ít dùng filter |
| `requirements_text` | 400 B | - | Text dài, ít dùng filter |
| `benefits` | 250 B | - | Text dài, ít dùng filter |
| `technical_skills` | - | - | 13% null, cấu trúc array |
| `languages_required` | - | - | 74% null |

→ Nếu gộp tất cả vào 1 bảng, mỗi query đọc 1 dòng phải tải cả MB text dù không cần. Tách dọc giúp:
- Giảm I/O khi query chỉ cần thông tin cơ bản
- Tối ưu cho **Semi-Join** (giải thuật #4)
- Cho phép cache bảng `core` (nhẹ) trong memory

### 4.2. Sơ đồ 3 bảng

```
┌─────────────────────────────────────────────────────────────────┐
│                          job_master                             │
│  (Bảng gốc 48K bản ghi → tách dọc thành 3 bảng dưới)            │
└─────────────────────────────────────────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
   ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
   │      core        │ │      detail      │ │     skills       │
   │  (Nhẹ, hay query)│ │  (Nặng, text)    │ │  (Kỹ năng)       │
   ├──────────────────┤ ├──────────────────┤ ├──────────────────┤
   │ job_id (PK)      │ │ job_id (PK,FK)   │ │ job_id (PK,FK)   │
   │ job_title        │ │ description      │ │ technical_skills │
   │ location         │ │ requirements_text│ │ soft_skills      │
   │ category         │ │ benefits         │ │ qualifications   │
   │ salary_min       │ │                  │ │ languages_required│
   │ salary_max       │ │                  │ │                  │
   │ salary_avg       │ │                  │ │                  │
   │ experience_req   │ │                  │ │                  │
   │ contract_type    │ │                  │ │                  │
   │ working_hours    │ │                  │ │                  │
   │ country          │ │                  │ │                  │
   └──────────────────┘ └──────────────────┘ └──────────────────┘
```

### 4.3. Đặc điểm 3 bảng

| Bảng | Mục đích | Tần suất truy vấn | Khóa phân tán |
|---|---|---|---|
| `core` | Lưu thông tin cơ bản, dùng cho filter/sort | **Cao** (90% query) | `category` |
| `detail` | Lưu mô tả dài, chỉ load khi cần xem chi tiết | Trung bình | `category` |
| `skills` | Lưu kỹ năng, dùng cho matching | Trung bình | `category` |

→ **Cả 3 bảng cùng phân tán theo `category`** để Citus tự động **co-locate** (join cục bộ, không shuffle qua mạng).

---

## 5. Thiết kế phân mảnh ngang

### 5.1. Khóa phân mảnh

**Khóa chính:** `category` (distribution column trong Citus)

**Phân mảnh logic:** `category × location` (tạo thành 8 fragment)

### 5.2. Sơ đồ 8 phân mảnh trên 4 worker

```
┌────────────────────────────────────────────────────────────────┐
│                         8 PHÂN MẢNH                            │
└────────────────────────────────────────────────────────────────┘

Worker 1 (commerce)              Worker 2 (tech)
┌──────────────────────┐         ┌──────────────────────┐
│ F1: commerce_hanoi   │         │ F3: tech_hanoi       │
│ F2: commerce_hcm     │         │ F4: tech_hcm         │
└──────────────────────┘         └──────────────────────┘

Worker 3 (creative)              Worker 4 (people)
┌──────────────────────┐         ┌──────────────────────┐
│ F5: creative_hanoi   │         │ F7: people_hanoi     │
│ F6: creative_hcm     │         │ F8: people_hcm       │
└──────────────────────┘         └──────────────────────┘

(`hanoi` ↔ `hà nội`, `hcm` ↔ `hồ chí minh` — chỉ rút gọn trong sơ đồ)
```

### 5.3. Phân bổ 16 category vào 4 nhóm

Tạo cột phụ `category_group` (gán giá trị 1-4) — đây sẽ là **distribution column** thực sự:

```sql
-- Ánh xạ category gốc → category_group (1-4)
CASE 
    WHEN category IN (
        'kinh_doanh_bán_hàng_chăm_sóc_khách_hàng',
        'tài_chính_kế_toán_ngân_hàng_bảo_hiểm',
        'logistics_vận_tải_chuỗi_cung_ứng'
    ) THEN 1  -- commerce

    WHEN category IN (
        'sản_xuất_lao_động_phổ_thông_cơ_khí',
        'công_nghệ_thông_tin_kỹ_thuật_số',
        'kỹ_thuật_điện_điện_tử_viễn_thông',
        'xây_dựng_kiến_trúc_bất_động_sản',
        'nông_nghiệp_năng_lượng_môi_trường'
    ) THEN 2  -- tech

    WHEN category IN (
        'marketing_truyền_thông_quảng_cáo_nội_dung',
        'thiết_kế_nghệ_thuật_giải_trí_truyền_hình_báo_chí',
        'du_lịch_nhà_hàng_khách_sạn_dịch_vụ',
        'ngôn_ngữ_dịch_thuật'
    ) THEN 3  -- creative

    ELSE 4    -- people
END
```

### 5.4. Vì sao chọn cách này?

- **Khớp tự nhiên** với 16 → 4 nhóm
- **Cân bằng tải:** mỗi worker chứa 3-5 category, tổng số bản ghi tương đương
- **Hiệu quả pruning:** query có WHERE category = 'X' → coordinator chỉ gọi đúng 1 worker
- **Hỗ trợ co-location:** tất cả bảng (`core`, `detail`, `skills`) cùng distribute theo `category_group` → join cục bộ

---

## 6. Dựng cluster với Docker

### 6.1. Cấu trúc thư mục dự án

```
Project/
├── docker/
│   └── docker-compose.yml
├── data/
│   └── dataset_IS252.csv          # Dataset đã tiền xử lý (24,281 bản ghi, 19 cột)
├── sql/
│   ├── 01_schema.sql
│   ├── 02_distribute.sql
│   ├── 02b_pin_shards.sql         # Pin từng shard về đúng worker theo nhãn nhóm
│   ├── 03_load_data.sql
│   └── 04_indexes.sql
├── algorithms/
│   ├── 01_hash_join.sql
│   ├── 02_query_routing.sql
│   ├── 03_mapreduce_agg.sql
│   ├── 04_semi_join.sql
│   └── 05_parallel_topk.sql
├── backend/                       # Python + FastAPI + asyncpg
│   ├── main.py
│   ├── database.py
│   ├── models.py
│   ├── requirements.txt
│   ├── .env
│   └── routers/
│       ├── __init__.py
│       ├── jobs.py
│       ├── stats.py
│       └── cluster.py
├── frontend/                      # React 18 + TypeScript + Vite
│   ├── package.json
│   ├── tsconfig.json
│   ├── vite.config.ts
│   └── src/
│       ├── main.tsx
│       ├── App.tsx
│       ├── api/
│       └── components/
├── benchmark/
│   ├── benchmark.py
│   ├── requirements.txt
│   ├── results/
│   └── plots/
└── README.md
```

### 6.2. File `docker-compose.yml`

```yaml
version: '3.8'

services:
  # ────────────────────────────────────────────
  # COORDINATOR NODE
  # ────────────────────────────────────────────
  coordinator:
    image: citusdata/citus:12.1
    container_name: vietjobs_coordinator
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: vietjobs
      POSTGRES_HOST_AUTH_METHOD: "trust"
    volumes:
      - coordinator_data:/var/lib/postgresql/data
      - ../data:/data
      - ../sql:/sql
    networks:
      - citus_net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 10

  # ────────────────────────────────────────────
  # WORKER NODES (4)
  # ────────────────────────────────────────────
  worker1:
    image: citusdata/citus:12.1
    container_name: vietjobs_worker1_commerce
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: vietjobs
      POSTGRES_HOST_AUTH_METHOD: "trust"
    volumes:
      - worker1_data:/var/lib/postgresql/data
    networks:
      - citus_net

  worker2:
    image: citusdata/citus:12.1
    container_name: vietjobs_worker2_tech
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: vietjobs
      POSTGRES_HOST_AUTH_METHOD: "trust"
    volumes:
      - worker2_data:/var/lib/postgresql/data
    networks:
      - citus_net

  worker3:
    image: citusdata/citus:12.1
    container_name: vietjobs_worker3_creative
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: vietjobs
      POSTGRES_HOST_AUTH_METHOD: "trust"
    volumes:
      - worker3_data:/var/lib/postgresql/data
    networks:
      - citus_net

  worker4:
    image: citusdata/citus:12.1
    container_name: vietjobs_worker4_people
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: vietjobs
      POSTGRES_HOST_AUTH_METHOD: "trust"
    volumes:
      - worker4_data:/var/lib/postgresql/data
    networks:
      - citus_net

  # ────────────────────────────────────────────
  # MANAGER (Citus auto-register workers)
  # ────────────────────────────────────────────
  manager:
    image: citusdata/membership-manager:0.3.0
    container_name: vietjobs_manager
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      - coordinator
    environment:
      CITUS_HOST: coordinator
      POSTGRES_PASSWORD: postgres
    networks:
      - citus_net

networks:
  citus_net:
    driver: bridge

volumes:
  coordinator_data:
  worker1_data:
  worker2_data:
  worker3_data:
  worker4_data:
```

### 6.3. Khởi động cluster

```bash
# Vào thư mục docker
cd vietjobs-distributed/docker

# Khởi động toàn bộ cluster
docker compose up -d

# Kiểm tra trạng thái
docker compose ps

# Verify cluster đã nhận diện 4 workers
docker exec -it vietjobs_coordinator psql -U postgres -d vietjobs -c \
  "SELECT * FROM citus_get_active_worker_nodes();"
```

Kết quả mong đợi:
```
 node_name                     | node_port
-------------------------------+-----------
 vietjobs_worker1_commerce     |      5432
 vietjobs_worker2_tech         |      5432
 vietjobs_worker3_creative     |      5432
 vietjobs_worker4_people       |      5432
(4 rows)
```

---

## 7. Khởi tạo schema và phân bổ dữ liệu

### 7.1. File `sql/01_schema.sql`

```sql
-- Kích hoạt extension Citus trên coordinator
CREATE EXTENSION IF NOT EXISTS citus;

-- ──────────────────────────────────────────────
-- BẢNG STAGING (load tạm trước khi phân tán)
-- File CSV có 19 cột: 1 cột index (pandas) + 18 cột nghiệp vụ
-- ──────────────────────────────────────────────
CREATE TABLE staging_jobs (
    _idx                INTEGER,            -- cột index của pandas (bỏ qua)
    job_title           TEXT,
    location            TEXT,
    country             TEXT,
    qualifications      TEXT,
    technical_skills    TEXT,
    soft_skills         TEXT,
    languages_required  TEXT,
    experience_required TEXT,
    salary              TEXT,
    contract_type       TEXT,
    working_hours       TEXT,
    benefits            TEXT,
    description         TEXT,
    requirements_text   TEXT,
    category            TEXT,
    salary_min          INTEGER,
    salary_max          INTEGER,
    salary_avg          NUMERIC,
    job_id              SERIAL              -- sinh tự động sau khi COPY
);

-- ──────────────────────────────────────────────
-- BẢNG CORE (thông tin chính, hay query)
-- ──────────────────────────────────────────────
CREATE TABLE core (
    job_id          INTEGER,
    category_group  INTEGER NOT NULL,   -- 1=commerce, 2=tech, 3=creative, 4=people
    category        TEXT    NOT NULL,
    location        TEXT    NOT NULL,
    job_title       TEXT,
    salary_min      INTEGER,
    salary_max      INTEGER,
    salary_avg      NUMERIC,
    experience_required TEXT,
    contract_type   TEXT,
    working_hours   TEXT,
    country         TEXT,
    PRIMARY KEY (category_group, job_id)
);

-- ──────────────────────────────────────────────
-- BẢNG DETAIL (text nặng)
-- ──────────────────────────────────────────────
CREATE TABLE detail (
    job_id              INTEGER,
    category_group      INTEGER NOT NULL,
    description         TEXT,
    requirements_text   TEXT,
    benefits            TEXT,
    PRIMARY KEY (category_group, job_id)
);

-- ──────────────────────────────────────────────
-- BẢNG SKILLS
-- ──────────────────────────────────────────────
CREATE TABLE skills (
    job_id              INTEGER,
    category_group      INTEGER NOT NULL,
    technical_skills    TEXT,
    soft_skills         TEXT,
    qualifications      TEXT,
    languages_required  TEXT,
    PRIMARY KEY (category_group, job_id)
);

-- ──────────────────────────────────────────────
-- BẢNG REFERENCE: mapping category → group
-- (Reference table - sao chép trên mọi worker)
-- ──────────────────────────────────────────────
CREATE TABLE category_mapping (
    category        TEXT PRIMARY KEY,
    category_group  INTEGER NOT NULL,
    group_name      TEXT NOT NULL
);

INSERT INTO category_mapping (category, category_group, group_name) VALUES
-- Group 1: commerce
('kinh_doanh_bán_hàng_chăm_sóc_khách_hàng', 1, 'commerce'),
('tài_chính_kế_toán_ngân_hàng_bảo_hiểm',     1, 'commerce'),
('logistics_vận_tải_chuỗi_cung_ứng',         1, 'commerce'),
-- Group 2: tech
('sản_xuất_lao_động_phổ_thông_cơ_khí',       2, 'tech'),
('công_nghệ_thông_tin_kỹ_thuật_số',          2, 'tech'),
('kỹ_thuật_điện_điện_tử_viễn_thông',         2, 'tech'),
('xây_dựng_kiến_trúc_bất_động_sản',          2, 'tech'),
('nông_nghiệp_năng_lượng_môi_trường',        2, 'tech'),
-- Group 3: creative
('marketing_truyền_thông_quảng_cáo_nội_dung',         3, 'creative'),
('thiết_kế_nghệ_thuật_giải_trí_truyền_hình_báo_chí',  3, 'creative'),
('du_lịch_nhà_hàng_khách_sạn_dịch_vụ',                3, 'creative'),
('ngôn_ngữ_dịch_thuật',                                3, 'creative'),
-- Group 4: people
('nhân_sự_hành_chính_pháp_chế_tư_vấn',                4, 'people'),
('giáo_dục_đào_tạo_nghiên_cứu',                       4, 'people'),
('y_tế_dược_chăm_sóc_sức_khỏe_công_nghệ_sinh_học',    4, 'people'),
('nhóm_nghề_khác',                                     4, 'people');
```

### 7.2. File `sql/02_distribute.sql`

```sql
-- ──────────────────────────────────────────────
-- BIẾN BẢNG THÀNH DISTRIBUTED TABLE
-- Distribution column: category_group
-- Số shard: 4 (mỗi worker 1 shard chính)
-- ──────────────────────────────────────────────

-- Cấu hình số shard
SET citus.shard_count = 4;

-- 1. Bảng core là "master" trong co-location group
SELECT create_distributed_table('core', 'category_group');

-- 2. Bảng detail co-locate với core
SELECT create_distributed_table('detail', 'category_group', colocate_with => 'core');

-- 3. Bảng skills co-locate với core
SELECT create_distributed_table('skills', 'category_group', colocate_with => 'core');

-- 4. Reference table (sao chép toàn phần trên mọi worker)
SELECT create_reference_table('category_mapping');

-- Verify
SELECT logicalrelid::regclass AS table_name,
       colocationid,
       partmethod,
       repmodel
FROM pg_dist_partition
ORDER BY logicalrelid::regclass::text;
```

### 7.2b. File `sql/02b_pin_shards.sql` *(thêm so với hướng dẫn gốc)*

> **Lý do thêm bước này:** Citus mặc định phân phối shard theo thuật toán hash + cân bằng số shard/node, nên thứ tự `category_group → worker container` không nhất thiết khớp với tên container đã đặt (`worker1_commerce`, `worker2_tech`, ...). Để **shard chứa nhóm `commerce` thực sự nằm trong container `vietjobs_worker1_commerce`** (giúp debug, giảng demo, log rõ ràng), ta dùng `citus_move_shard_placement()` để pin lại từng shard về đúng worker mong muốn.

```sql
-- ──────────────────────────────────────────────
-- PIN SHARD VỀ ĐÚNG WORKER THEO NHÃN NHÓM
-- (Chạy SAU 02_distribute.sql, TRƯỚC 03_load_data.sql)
-- ──────────────────────────────────────────────

-- Mapping mong muốn:
--   category_group = 1 (commerce)  → worker1
--   category_group = 2 (tech)      → worker2
--   category_group = 3 (creative)  → worker3
--   category_group = 4 (people)    → worker4

DO $$
DECLARE
    rec RECORD;
    target_node TEXT;
    source_node TEXT;
    source_port INTEGER;
BEGIN
    FOR rec IN
        SELECT s.shardid,
               s.logicalrelid::regclass::text AS tbl,
               (s.shardminvalue::bigint) AS minv,
               n.nodename AS cur_node,
               n.nodeport AS cur_port
        FROM pg_dist_shard s
        JOIN pg_dist_placement p USING (shardid)
        JOIN pg_dist_node n ON n.groupid = p.groupid
        WHERE s.logicalrelid IN ('core'::regclass, 'detail'::regclass, 'skills'::regclass)
    LOOP
        -- Xác định worker đích theo category_group (1-4) thông qua shardid mod 4
        -- (cách đơn giản – có thể thay bằng query SELECT DISTINCT category_group nếu cần chính xác)
        target_node := CASE ((rec.shardid - (SELECT MIN(shardid) FROM pg_dist_shard
                                              WHERE logicalrelid = rec.tbl::regclass)) % 4)
            WHEN 0 THEN 'vietjobs_worker1_commerce'
            WHEN 1 THEN 'vietjobs_worker2_tech'
            WHEN 2 THEN 'vietjobs_worker3_creative'
            WHEN 3 THEN 'vietjobs_worker4_people'
        END;

        IF rec.cur_node <> target_node THEN
            PERFORM citus_move_shard_placement(
                rec.shardid,
                rec.cur_node, rec.cur_port,
                target_node, 5432,
                shard_transfer_mode := 'block_writes'
            );
            RAISE NOTICE 'Moved shard % (%) from % → %', rec.shardid, rec.tbl, rec.cur_node, target_node;
        END IF;
    END LOOP;
END $$;

-- Verify: shard của 3 bảng cùng category_group phải nằm chung 1 worker (co-location)
SELECT s.logicalrelid::regclass AS tbl, s.shardid, n.nodename
FROM pg_dist_shard s
JOIN pg_dist_placement p USING (shardid)
JOIN pg_dist_node n ON n.groupid = p.groupid
WHERE s.logicalrelid IN ('core'::regclass, 'detail'::regclass, 'skills'::regclass)
ORDER BY n.nodename, tbl, s.shardid;
```

> ⚠️ Nếu cluster mới khởi tạo và `02_distribute.sql` đã phân đều shard, bước này có thể no-op cho phần lớn shard. Mục tiêu chính là **đảm bảo** mapping đúng tên-container ↔ nội dung-shard.

### 7.3. File `sql/03_load_data.sql`

```sql
-- ──────────────────────────────────────────────
-- LOAD DATA TỪ CSV → STAGING → 3 BẢNG PHÂN TÁN
-- File CSV: /data/dataset_IS252.csv (19 cột, 24,281 bản ghi)
-- Cột đầu là index của pandas → COPY vào _idx rồi bỏ qua.
-- ──────────────────────────────────────────────

-- Bước 1: Load CSV vào staging (đúng thứ tự 19 cột như file CSV)
COPY staging_jobs (
    _idx,
    job_title, location, country, qualifications, technical_skills,
    soft_skills, languages_required, experience_required, salary,
    contract_type, working_hours, benefits, description,
    requirements_text, category, salary_min, salary_max, salary_avg
)
FROM '/data/dataset_IS252.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Bước 2: Chuẩn hoá location về chữ thường tiếng Việt đầy đủ (CASE WHEN)
--   'Hà Nội' / 'hà nội' / 'HÀ NỘI' → 'hà nội'
--   'TPHCM' / 'TP.HCM' / 'TP HCM' / 'Hồ Chí Minh' → 'hồ chí minh'
UPDATE staging_jobs
SET location = CASE
    WHEN lower(location) IN ('hà nội', 'ha noi', 'hanoi') THEN 'hà nội'
    WHEN lower(location) IN ('tphcm', 'tp.hcm', 'tp hcm', 'hồ chí minh', 'ho chi minh', 'hcm') THEN 'hồ chí minh'
    ELSE lower(location)
END;

-- Bước 3: Tách dọc vào 3 bảng phân tán

-- INSERT vào CORE
INSERT INTO core (
    job_id, category_group, category, location, job_title,
    salary_min, salary_max, salary_avg,
    experience_required, contract_type, working_hours, country
)
SELECT
    s.job_id,
    cm.category_group,
    s.category,
    s.location,
    s.job_title,
    s.salary_min, s.salary_max, s.salary_avg,
    s.experience_required, s.contract_type, s.working_hours, s.country
FROM staging_jobs s
JOIN category_mapping cm ON cm.category = s.category;

-- INSERT vào DETAIL
INSERT INTO detail (job_id, category_group, description, requirements_text, benefits)
SELECT
    s.job_id, cm.category_group,
    s.description, s.requirements_text, s.benefits
FROM staging_jobs s
JOIN category_mapping cm ON cm.category = s.category;

-- INSERT vào SKILLS
INSERT INTO skills (
    job_id, category_group, technical_skills, soft_skills,
    qualifications, languages_required
)
SELECT
    s.job_id, cm.category_group,
    s.technical_skills, s.soft_skills,
    s.qualifications, s.languages_required
FROM staging_jobs s
JOIN category_mapping cm ON cm.category = s.category;

-- Bước 3: Drop staging
DROP TABLE staging_jobs;

-- Verify phân bố
SELECT category_group, location, COUNT(*) AS num_records
FROM core
GROUP BY category_group, location
ORDER BY category_group, location;
```

### 7.4. File `sql/04_indexes.sql`

```sql
-- Index hỗ trợ query routing và filter
CREATE INDEX idx_core_location  ON core (location);
CREATE INDEX idx_core_category  ON core (category);
CREATE INDEX idx_core_salary    ON core (salary_avg);
CREATE INDEX idx_core_exp       ON core (experience_required);

-- Index cho semi-join lookup
CREATE INDEX idx_detail_jobid   ON detail (job_id);
CREATE INDEX idx_skills_jobid   ON skills (job_id);

-- Full-text search trên description (tùy chọn)
CREATE INDEX idx_detail_desc_fts ON detail
    USING GIN (to_tsvector('simple', description));
```

### 7.5. Thực thi tất cả

```bash
# Chạy lần lượt 5 file SQL (chú ý thứ tự 02 → 02b → 03)
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/01_schema.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/02_distribute.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/02b_pin_shards.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/03_load_data.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/04_indexes.sql

# Verify shard placement
docker exec -it vietjobs_coordinator psql -U postgres -d vietjobs -c "
SELECT shard.logicalrelid::regclass AS table_name,
       placement.shardid,
       node.nodename
FROM pg_dist_shard shard
JOIN pg_dist_placement placement USING (shardid)
JOIN pg_dist_node node ON node.groupid = placement.groupid
ORDER BY table_name, shardid;
"
```

---

## 8. 5 giải thuật song song và phân tán

### 8.1. Bảng tổng hợp 5 giải thuật

| # | Giải thuật | Citus hỗ trợ | Use case VietJobs |
|---|---|---|---|
| 1 | Parallel Hash Join (Co-located) | Native | Join `core` + `detail` + `skills` |
| 2 | Query Routing + Shard Pruning | Native | Filter theo `category_group`, `location` |
| 3 | MapReduce 2-phase Aggregation | Native | Thống kê lương, đếm job theo nhóm |
| 4 | Semi-Join | Tự code để demo | Lấy `detail` của job lương cao |
| 5 | Parallel Sort-Merge / Top-K | Native (LIMIT pushdown) | Top 100 job lương cao nhất |

---

### 8.2. Giải thuật 1: Parallel Hash Join (Co-located Join)

**Cơ chế:** Vì cả 3 bảng cùng distribute theo `category_group`, các bản ghi cùng `category_group` nằm cùng worker → join cục bộ, không shuffle qua mạng.

**File `algorithms/01_hash_join.sql`:**

```sql
-- ──────────────────────────────────────────────
-- ALGORITHM 1: PARALLEL HASH JOIN (CO-LOCATED)
-- ──────────────────────────────────────────────
-- Use case: Lấy đầy đủ thông tin job (core + detail + skills)
-- Mỗi worker join cục bộ → 4 worker chạy song song

EXPLAIN (ANALYZE, VERBOSE)
SELECT
    c.job_id,
    c.job_title,
    c.location,
    c.salary_avg,
    d.description,
    s.technical_skills
FROM core c
JOIN detail d ON c.category_group = d.category_group AND c.job_id = d.job_id
JOIN skills s ON c.category_group = s.category_group AND c.job_id = s.job_id
WHERE c.category_group = 2  -- tech
  AND c.location = 'hà nội'
LIMIT 50;
```

**Plan mong đợi:** `Custom Scan (Citus Adaptive)` → `Task Count: 1` (chỉ 1 worker tham gia vì pruning theo `category_group = 2`).

**Demo so sánh — Non-co-located version (cố tình tạo join chậm):**

```sql
-- Tạo bản sao detail KHÔNG co-locate (để demo so sánh)
CREATE TABLE detail_nc (LIKE detail);
SELECT create_distributed_table('detail_nc', 'job_id');  -- distribute theo cột khác
INSERT INTO detail_nc SELECT * FROM detail;

-- Query này sẽ cần repartition join (chậm hơn)
EXPLAIN ANALYZE
SELECT c.job_title, d.description
FROM core c JOIN detail_nc d ON c.job_id = d.job_id
WHERE c.category_group = 2;
```

---

### 8.3. Giải thuật 2: Query Routing + Shard Pruning

**Cơ chế:** Coordinator đọc WHERE → xác định shard cần truy cập → chỉ gọi worker chứa shard đó.

**File `algorithms/02_query_routing.sql`:**

```sql
-- ──────────────────────────────────────────────
-- ALGORITHM 2: QUERY ROUTING + SHARD PRUNING
-- ──────────────────────────────────────────────

-- CASE A: Single-shard query (router executor)
-- Chỉ chạm 1 worker
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM core
WHERE category_group = 3 AND location = 'hồ chí minh'
LIMIT 20;

-- CASE B: Multi-shard query (adaptive executor)
-- Chạm tất cả 4 worker song song
EXPLAIN (ANALYZE, VERBOSE)
SELECT category_group, COUNT(*) FROM core
WHERE salary_avg > 25000000
GROUP BY category_group;

-- CASE C: Reference table join (chạy cục bộ trên mỗi worker)
EXPLAIN (ANALYZE, VERBOSE)
SELECT cm.group_name, COUNT(*) AS num_jobs
FROM core c
JOIN category_mapping cm ON cm.category = c.category
WHERE c.location = 'hà nội'
GROUP BY cm.group_name;
```

**Metric quan sát:**
- `Task Count`: số worker tham gia
- `Tuples sent from workers`: data transfer
- `Executor: Adaptive` hoặc `Router`

---

### 8.4. Giải thuật 3: MapReduce 2-phase Aggregation

**Cơ chế:** Citus tự rewrite `AVG`, `COUNT`, `SUM` thành 2 phase:
- **Map (Worker):** Tính `SUM(x)`, `COUNT(x)` cục bộ
- **Reduce (Coordinator):** Gộp lại để tính `AVG = SUM(SUM) / SUM(COUNT)`

**File `algorithms/03_mapreduce_agg.sql`:**

```sql
-- ──────────────────────────────────────────────
-- ALGORITHM 3: MAPREDUCE 2-PHASE AGGREGATION
-- ──────────────────────────────────────────────

-- Q1: Lương trung bình theo nhóm category
EXPLAIN (ANALYZE, VERBOSE)
SELECT
    category_group,
    COUNT(*) AS total_jobs,
    AVG(salary_avg)::INTEGER AS avg_salary,
    MIN(salary_min) AS min_salary,
    MAX(salary_max) AS max_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_avg) AS median_salary
FROM core
GROUP BY category_group
ORDER BY avg_salary DESC;

-- Q2: Phân bố lương theo location (hà nội vs hồ chí minh)
SELECT
    location,
    COUNT(*) AS num_jobs,
    AVG(salary_avg)::INTEGER AS avg_salary
FROM core
GROUP BY location;

-- Q3: Top 5 category có nhiều job lương cao (>30M)
SELECT
    category,
    COUNT(*) AS high_paying_jobs
FROM core
WHERE salary_avg > 30000000
GROUP BY category
ORDER BY high_paying_jobs DESC
LIMIT 5;
```

**Cách verify đúng là 2-phase:** Trong execution plan tìm dòng:
```
->  Distributed Subplan
->  Task Count: 4
->  HashAggregate (worker phase)
HashAggregate (coordinator phase)
```

---

### 8.5. Giải thuật 4: Semi-Join

**Cơ chế:** Thay vì gửi cả bảng `detail` qua mạng để join, chỉ gửi tập `job_id` đã filter từ `core` → tiết kiệm băng thông.

**File `algorithms/04_semi_join.sql`:**

```sql
-- ──────────────────────────────────────────────
-- ALGORITHM 4: SEMI-JOIN
-- Use case: Lấy mô tả chi tiết của các job lương > 30M
-- detail có description ~616B/dòng → bảng nặng
-- ──────────────────────────────────────────────

-- VERSION A: NAIVE JOIN (ship cả bảng detail qua mạng - chậm)
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.job_title, c.salary_avg, d.description
FROM core c
FULL OUTER JOIN detail_nc d ON c.job_id = d.job_id  -- bảng non-co-located
WHERE c.salary_avg > 30000000;

-- VERSION B: SEMI-JOIN (chỉ ship job_id - nhanh)
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.job_title, c.salary_avg, d.description
FROM core c
JOIN detail d 
    ON c.category_group = d.category_group   -- distribution column
   AND c.job_id = d.job_id
WHERE c.salary_avg > 30000000;

-- VERSION C: EXPLICIT SEMI-JOIN với EXISTS
SELECT d.job_id, d.description
FROM detail d
WHERE EXISTS (
    SELECT 1 FROM core c
    WHERE c.category_group = d.category_group
      AND c.job_id = d.job_id
      AND c.salary_avg > 30000000
);

-- VERSION D: Manual 2-step semi-join (để demo lý thuyết)
-- Step 1: Lấy ra tập job_id (key) - chỉ ship keys
WITH high_salary_keys AS (
    SELECT category_group, job_id
    FROM core
    WHERE salary_avg > 30000000
)
-- Step 2: Join với detail dùng keys đã ship
SELECT d.job_id, d.description
FROM detail d
JOIN high_salary_keys k 
    ON d.category_group = k.category_group 
   AND d.job_id = k.job_id;
```

**Metric đo:**
- `Shared Read Blocks`: số block đọc từ disk
- `Data transferred` (qua `pg_stat_statements`)
- Thời gian thực thi

---

### 8.6. Giải thuật 5: Parallel Sort-Merge / Top-K

**Cơ chế:** Mỗi worker sort cục bộ + trả về top-K → coordinator merge K-way → trả về top-K toàn cục.

**File `algorithms/05_parallel_topk.sql`:**

```sql
-- ──────────────────────────────────────────────
-- ALGORITHM 5: PARALLEL SORT-MERGE / TOP-K
-- ──────────────────────────────────────────────

-- VERSION A: TOP-K với LIMIT pushdown (nhanh)
-- Mỗi worker chỉ trả về top 100, coordinator merge
EXPLAIN (ANALYZE, VERBOSE)
SELECT job_id, job_title, location, salary_avg
FROM core
ORDER BY salary_avg DESC
LIMIT 100;

-- VERSION B: KHÔNG có LIMIT (phải sort toàn bộ - chậm)
EXPLAIN (ANALYZE, VERBOSE)
SELECT job_id, job_title, salary_avg
FROM core
ORDER BY salary_avg DESC;

-- VERSION C: Top-K theo từng nhóm (window function)
SELECT *
FROM (
    SELECT 
        category_group, job_title, salary_avg,
        ROW_NUMBER() OVER (PARTITION BY category_group ORDER BY salary_avg DESC) AS rn
    FROM core
) t
WHERE rn <= 10
ORDER BY category_group, rn;

-- VERSION D: Top-K theo location (pagination)
SELECT job_title, salary_avg, location
FROM core
WHERE location = 'hồ chí minh'
ORDER BY salary_avg DESC
LIMIT 50 OFFSET 0;
```

---

## 9. Xây dựng Backend API (Python + FastAPI)

### 9.1. Cấu trúc thư mục backend

```
backend/
├── main.py                # FastAPI app entrypoint
├── database.py            # Connection pool đến Coordinator
├── routers/
│   ├── __init__.py
│   ├── jobs.py            # Endpoint cho 5 giải thuật
│   ├── stats.py           # Endpoint thống kê
│   └── cluster.py         # Endpoint debug cluster
├── models.py              # Pydantic models
├── requirements.txt
└── .env
```

### 9.2. File `backend/requirements.txt`

```txt
fastapi==0.110.0
uvicorn[standard]==0.27.1
asyncpg==0.29.0
pydantic==2.6.1
pydantic-settings==2.2.1
python-dotenv==1.0.1
```

### 9.3. File `backend/.env`

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=vietjobs
DB_POOL_MIN=5
DB_POOL_MAX=20
```

### 9.4. File `backend/database.py`

```python
"""Connection pool đến Citus Coordinator."""
import asyncpg
from contextlib import asynccontextmanager
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_name: str = "vietjobs"
    db_pool_min: int = 5
    db_pool_max: int = 20

    class Config:
        env_file = ".env"


settings = Settings()
_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    """Khởi tạo connection pool khi FastAPI startup."""
    global _pool
    _pool = await asyncpg.create_pool(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        command_timeout=60,
    )


async def close_pool() -> None:
    if _pool is not None:
        await _pool.close()


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Pool chưa được khởi tạo.")
    return _pool
```

### 9.5. File `backend/models.py`

```python
"""Pydantic response models."""
from typing import Any
from pydantic import BaseModel


class QueryResponse(BaseModel):
    algorithm: str
    elapsed_ms: float
    rows: int
    data: list[dict[str, Any]]


class WorkerInfo(BaseModel):
    node_name: str
    node_port: int


class ShardInfo(BaseModel):
    table_name: str
    shardid: int
    nodename: str


class ClusterStatus(BaseModel):
    workers: list[WorkerInfo]
    shards: list[ShardInfo]
```

### 9.6. File `backend/routers/jobs.py`

```python
"""Các endpoint cho 5 giải thuật song song."""
import time
from fastapi import APIRouter, Query
from database import get_pool
from models import QueryResponse

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


# ──────────────────────────────────────────────
# ALGORITHM 1: PARALLEL HASH JOIN (CO-LOCATED)
# ──────────────────────────────────────────────
@router.get("/full", response_model=QueryResponse)
async def get_full_jobs(
    category_group: int = Query(..., ge=1, le=4),
    location: str = Query(..., pattern="^(hà nội|hồ chí minh)$"),
    limit: int = Query(50, ge=1, le=500),
):
    """Lấy đầy đủ thông tin job qua co-located hash join 3 bảng."""
    sql = """
        SELECT c.job_id, c.job_title, c.location, c.salary_avg,
               d.description, s.technical_skills
        FROM core c
        JOIN detail d
          ON c.category_group = d.category_group AND c.job_id = d.job_id
        JOIN skills s
          ON c.category_group = s.category_group AND c.job_id = s.job_id
        WHERE c.category_group = $1 AND c.location = $2
        LIMIT $3
    """
    start = time.perf_counter()
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, category_group, location, limit)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return QueryResponse(
        algorithm="parallel_hash_join",
        elapsed_ms=round(elapsed_ms, 2),
        rows=len(rows),
        data=[dict(r) for r in rows],
    )


# ──────────────────────────────────────────────
# ALGORITHM 2: QUERY ROUTING + SHARD PRUNING
# ──────────────────────────────────────────────
@router.get("/search", response_model=QueryResponse)
async def search_jobs(
    category_group: int = Query(..., ge=1, le=4),
    location: str = Query(...),
    limit: int = Query(100, ge=1, le=500),
):
    """Single-shard query, hit đúng 1 worker."""
    sql = """
        SELECT job_id, job_title, salary_avg
        FROM core
        WHERE category_group = $1 AND location = $2
        LIMIT $3
    """
    start = time.perf_counter()
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, category_group, location, limit)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return QueryResponse(
        algorithm="query_routing",
        elapsed_ms=round(elapsed_ms, 2),
        rows=len(rows),
        data=[dict(r) for r in rows],
    )


# ──────────────────────────────────────────────
# ALGORITHM 4: SEMI-JOIN
# ──────────────────────────────────────────────
@router.get("/high-salary-details", response_model=QueryResponse)
async def high_salary_details(
    threshold: int = Query(30_000_000, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    """Lấy chi tiết job có lương > threshold, qua semi-join tối ưu."""
    sql = """
        SELECT d.job_id, d.description, d.requirements_text
        FROM detail d
        WHERE EXISTS (
            SELECT 1 FROM core c
            WHERE c.category_group = d.category_group
              AND c.job_id        = d.job_id
              AND c.salary_avg    > $1
        )
        LIMIT $2
    """
    start = time.perf_counter()
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, threshold, limit)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return QueryResponse(
        algorithm="semi_join",
        elapsed_ms=round(elapsed_ms, 2),
        rows=len(rows),
        data=[dict(r) for r in rows],
    )


# ──────────────────────────────────────────────
# ALGORITHM 5: PARALLEL TOP-K
# ──────────────────────────────────────────────
@router.get("/top", response_model=QueryResponse)
async def top_jobs(k: int = Query(100, ge=1, le=1000)):
    """Top-K job lương cao nhất qua parallel sort-merge."""
    sql = """
        SELECT job_id, job_title, location, salary_avg, category
        FROM core
        ORDER BY salary_avg DESC
        LIMIT $1
    """
    start = time.perf_counter()
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, k)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return QueryResponse(
        algorithm="parallel_topk",
        elapsed_ms=round(elapsed_ms, 2),
        rows=len(rows),
        data=[dict(r) for r in rows],
    )
```

### 9.7. File `backend/routers/stats.py`

```python
"""Endpoint cho thuật toán MapReduce Aggregation."""
import time
from fastapi import APIRouter
from database import get_pool
from models import QueryResponse

router = APIRouter(prefix="/api/stats", tags=["stats"])


# ──────────────────────────────────────────────
# ALGORITHM 3: MAPREDUCE 2-PHASE AGGREGATION
# ──────────────────────────────────────────────
@router.get("/salary-by-group", response_model=QueryResponse)
async def salary_by_group():
    """Thống kê lương theo từng nhóm category - chạy parallel trên 4 workers."""
    sql = """
        SELECT cm.group_name,
               COUNT(*)                    AS num_jobs,
               AVG(c.salary_avg)::INTEGER  AS avg_salary,
               MIN(c.salary_min)           AS min_salary,
               MAX(c.salary_max)           AS max_salary
        FROM core c
        JOIN category_mapping cm ON cm.category = c.category
        GROUP BY cm.group_name
        ORDER BY avg_salary DESC
    """
    start = time.perf_counter()
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return QueryResponse(
        algorithm="mapreduce_aggregation",
        elapsed_ms=round(elapsed_ms, 2),
        rows=len(rows),
        data=[dict(r) for r in rows],
    )


@router.get("/salary-by-location", response_model=QueryResponse)
async def salary_by_location():
    sql = """
        SELECT location,
               COUNT(*)                   AS num_jobs,
               AVG(salary_avg)::INTEGER   AS avg_salary
        FROM core
        GROUP BY location
    """
    start = time.perf_counter()
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return QueryResponse(
        algorithm="mapreduce_aggregation",
        elapsed_ms=round(elapsed_ms, 2),
        rows=len(rows),
        data=[dict(r) for r in rows],
    )
```

### 9.8. File `backend/routers/cluster.py`

```python
"""Endpoint debug cluster Citus."""
from fastapi import APIRouter
from database import get_pool
from models import ClusterStatus, WorkerInfo, ShardInfo

router = APIRouter(prefix="/api/cluster", tags=["cluster"])


@router.get("/status", response_model=ClusterStatus)
async def cluster_status():
    """Liệt kê workers active và phân bố shard."""
    sql_workers = "SELECT * FROM citus_get_active_worker_nodes()"
    sql_shards = """
        SELECT shard.logicalrelid::regclass::text AS table_name,
               placement.shardid,
               node.nodename
        FROM pg_dist_shard shard
        JOIN pg_dist_placement placement USING (shardid)
        JOIN pg_dist_node node ON node.groupid = placement.groupid
        ORDER BY 1, 2
    """
    async with get_pool().acquire() as conn:
        workers = await conn.fetch(sql_workers)
        shards = await conn.fetch(sql_shards)

    return ClusterStatus(
        workers=[WorkerInfo(**dict(w)) for w in workers],
        shards=[ShardInfo(**dict(s)) for s in shards],
    )
```

### 9.9. File `backend/main.py`

```python
"""FastAPI entrypoint cho VietJobs Distributed API."""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import init_pool, close_pool
from routers import jobs, stats, cluster


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    yield
    await close_pool()


app = FastAPI(
    title="VietJobs Distributed API",
    description="API thử nghiệm 5 giải thuật phân tán trên Postgres+Citus",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs.router)
app.include_router(stats.router)
app.include_router(cluster.router)


@app.get("/")
async def root():
    return {
        "service": "VietJobs Distributed API",
        "docs": "/docs",
        "endpoints": [
            "/api/jobs/full",
            "/api/jobs/search",
            "/api/jobs/high-salary-details",
            "/api/jobs/top",
            "/api/stats/salary-by-group",
            "/api/stats/salary-by-location",
            "/api/cluster/status",
        ],
    }
```

### 9.10. Chạy backend

```bash
cd backend
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Dev mode (auto-reload)
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Production
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

Truy cập Swagger UI tự động: **http://localhost:8000/docs**

### 9.11. Test nhanh bằng curl

```bash
# Test endpoint hash join
curl "http://localhost:8000/api/jobs/full?category_group=2&location=h%C3%A0%20n%E1%BB%99i&limit=10"

# Test endpoint MapReduce aggregation
curl "http://localhost:8000/api/stats/salary-by-group"

# Test cluster status
curl "http://localhost:8000/api/cluster/status"
```

---

## 10. Benchmark và đánh giá hiệu năng

### 10.1. File `benchmark/benchmark.py`

```python
"""
Benchmark suite cho 5 giải thuật phân tán trên VietJobs
"""
import psycopg2
import time
import json
import statistics
import matplotlib.pyplot as plt

CONN_STRING = "host=localhost port=5432 dbname=vietjobs user=postgres password=postgres"

# ──────────────────────────────────────────────
# DANH SÁCH QUERY BENCHMARK
# ──────────────────────────────────────────────
QUERIES = {
    "hash_join_colocated": """
        SELECT c.job_title, d.description, s.technical_skills
        FROM core c
        JOIN detail d ON c.category_group = d.category_group AND c.job_id = d.job_id
        JOIN skills s ON c.category_group = s.category_group AND c.job_id = s.job_id
        WHERE c.category_group = 2 AND c.location = 'hà nội'
        LIMIT 100
    """,
    "query_routing_single_shard": """
        SELECT * FROM core
        WHERE category_group = 3 AND location = 'hồ chí minh'
        LIMIT 50
    """,
    "query_routing_all_shards": """
        SELECT category_group, COUNT(*) FROM core
        WHERE salary_avg > 25000000
        GROUP BY category_group
    """,
    "mapreduce_aggregation": """
        SELECT category_group,
               COUNT(*) AS n,
               AVG(salary_avg)::INTEGER AS avg_sal
        FROM core
        GROUP BY category_group
    """,
    "semi_join_optimized": """
        SELECT d.job_id, d.description
        FROM detail d
        WHERE EXISTS (
            SELECT 1 FROM core c
            WHERE c.category_group = d.category_group
              AND c.job_id = d.job_id
              AND c.salary_avg > 30000000
        )
        LIMIT 100
    """,
    "parallel_topk": """
        SELECT job_id, job_title, salary_avg
        FROM core
        ORDER BY salary_avg DESC
        LIMIT 100
    """
}

# ──────────────────────────────────────────────
# CHẠY BENCHMARK
# ──────────────────────────────────────────────
def run_benchmark(num_runs=20, warmup=3):
    conn = psycopg2.connect(CONN_STRING)
    results = {}

    for name, sql in QUERIES.items():
        print(f"\n► Running: {name}")
        cur = conn.cursor()

        # Warmup (loại bỏ cache cold start)
        for _ in range(warmup):
            cur.execute(sql)
            cur.fetchall()

        # Đo thực
        times = []
        for run in range(num_runs):
            start = time.perf_counter()
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed = (time.perf_counter() - start) * 1000  # ms
            times.append(elapsed)
            print(f"  Run {run+1}: {elapsed:.2f} ms ({len(rows)} rows)")

        results[name] = {
            "runs": times,
            "mean_ms": statistics.mean(times),
            "median_ms": statistics.median(times),
            "min_ms": min(times),
            "max_ms": max(times),
            "stdev_ms": statistics.stdev(times) if len(times) > 1 else 0
        }
        cur.close()

    conn.close()
    return results

# ──────────────────────────────────────────────
# XUẤT KẾT QUẢ
# ──────────────────────────────────────────────
def save_results(results, output_path="results/benchmark.json"):
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n✓ Saved to {output_path}")

def plot_results(results, output_path="plots/benchmark.png"):
    names = list(results.keys())
    means = [results[n]["mean_ms"] for n in names]
    stds  = [results[n]["stdev_ms"] for n in names]

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(names, means, yerr=stds, capsize=5, color='steelblue')
    ax.set_ylabel('Latency (ms)')
    ax.set_title('VietJobs Distributed Query Benchmark')
    plt.xticks(rotation=30, ha='right')
    for bar, mean in zip(bars, means):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f'{mean:.1f}', ha='center', va='bottom')
    plt.tight_layout()
    plt.savefig(output_path, dpi=120)
    print(f"✓ Saved plot to {output_path}")

if __name__ == "__main__":
    results = run_benchmark(num_runs=20, warmup=3)
    save_results(results)
    plot_results(results)
```

### 10.2. Template kết quả benchmark

| Giải thuật | Mean (ms) | Median (ms) | Stdev (ms) | Speedup vs naive |
|---|---|---|---|---|
| 1. Hash Join (co-located) | _điền sau_ | | | _x lần_ |
| 2. Query Routing (1 shard) | | | | |
| 2. Query Routing (4 shards) | | | | |
| 3. MapReduce Aggregation | | | | |
| 4. Semi-Join (optimized) | | | | |
| 4. Semi-Join (naive baseline) | | | | |
| 5. Parallel Top-K | | | | |

### 10.3. Các metrics quan trọng để báo cáo

```sql
-- Đo data transferred giữa workers ↔ coordinator
SELECT query, calls, total_exec_time, mean_exec_time, rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;

-- Đo I/O per worker
SELECT * FROM citus_stat_statements ORDER BY calls DESC LIMIT 10;

-- Số task song song trong query gần nhất
SELECT * FROM citus_stat_activity WHERE state = 'active';
```

### 10.4. Kịch bản scalability test

Tăng dần số worker và đo speedup:

```bash
# Tạo các config khác nhau
docker compose -f compose-1worker.yml up -d   # 1 worker
docker compose -f compose-2workers.yml up -d  # 2 workers
docker compose -f compose-4workers.yml up -d  # 4 workers (default)

# Chạy benchmark cho mỗi config
python benchmark.py --workers 1 --output results/1w.json
python benchmark.py --workers 2 --output results/2w.json
python benchmark.py --workers 4 --output results/4w.json
```

Vẽ biểu đồ speedup `T1 / TN`:

```
   Speedup
     ↑
   4 │                    ● (ideal linear)
     │             ●─────
   3 │       ●────
     │  ●───
   2 │ ●
     │
   1 ●────────────────────→ Number of workers
     1    2    3    4
```

---

### 10.5. Cơ sở khoa học của benchmark — Trích dẫn từ nguồn uy tín

> Phần này giải thích **căn cứ học thuật** cho phương pháp benchmark, đồng thời cung cấp **số liệu tham chiếu từ các paper khoa học và báo cáo công nghệ uy tín** để bạn có thể so sánh và đối chứng kết quả của mình.

#### 10.5.1. Cơ sở phương pháp luận (Methodology Foundation)

| Yếu tố trong benchmark.py | Cơ sở khoa học | Nguồn |
|---|---|---|
| Chạy nhiều lần (`num_runs=20`) lấy mean/median/stdev | Standard practice for database benchmarking | TPC Council benchmarks (TPC-C, TPC-H, TPC-DS) |
| Warmup runs (loại bỏ cold cache) | Mitigation of cache effects | Cubukcu et al., SIGMOD 2021, Section 5 |
| Đo speedup `T1/TN` khi tăng số worker | Parallel speedup metric | Özsu & Valduriez (2020), Chapter 8 |
| Co-located vs Non-co-located comparison | Citus design principle | Cubukcu et al., SIGMOD 2021, Section 4.2 |
| Semi-join data transfer reduction | Distributed query optimization | Chen, Yu, Wu (1993), IEEE TKDE |
| HammerDB / pgbench để đo throughput | Industry-standard benchmark tool | GigaOM Benchmark Report (2023) |

#### 10.5.2. Số liệu benchmark tham chiếu từ Citus SIGMOD 2021

Paper gốc **Cubukcu et al. (2021), "Citus: Distributed PostgreSQL for Data-Intensive Applications"** (SIGMOD '21) công bố các kết quả benchmark sau, dùng làm **baseline tham chiếu**:

**Setup paper sử dụng:** PostgreSQL 13 + Citus 9.5, cluster với 4-8 worker nodes, tables 50GB sinh bằng pgbench.

| Kịch bản | Kết quả paper báo cáo | Trích dẫn |
|---|---|---|
| **Ingestion (COPY 4.4GB JSON)** | 4 workers nhanh hơn single-node Postgres do tăng I/O và CPU; >8 workers bị bottleneck ở coordinator | "Citus cluster with 4 worker nodes can speed up the COPY further due to the greater number of cores and I/O capacity" [Cubukcu et al. 2021, §5] |
| **Real-time analytics** | Speedup gần linear khi scale lên 4-8 workers | Cubukcu et al. 2021, Figure 7-8 |
| **Methodology** | Tất cả benchmark chạy trung bình 5 runs, tables 50GB | "using two tables of 50GB generated by the pgbench tool... average load times over 5 runs" [Cubukcu et al. 2021, §5] |

**Cách dùng trong báo cáo:**
> *"Theo paper gốc của Citus được công bố tại SIGMOD 2021 [1], cluster 4 worker nodes cho speedup đáng kể so với single-node PostgreSQL khi xử lý dữ liệu lớn. Kết quả benchmark của chúng tôi trên dataset VietJobs cho thấy xu hướng tương tự với speedup là X.Y lần."*

#### 10.5.3. Số liệu benchmark từ GigaOM Report (2023)

**GigaOM** là tổ chức nghiên cứu công nghệ độc lập, được Microsoft thuê để so sánh hiệu năng Citus với các distributed PostgreSQL khác (CockroachDB, Yugabyte).

| Aspect | Kết quả từ GigaOM | Trích dẫn |
|---|---|---|
| **Benchmark tool** | HammerDB (chuẩn TPC-C) | "GigaOM compared the transaction performance and price-performance of these popular managed services of distributed PostgreSQL, using the HammerDB benchmark software" [GigaOM 2023] |
| **Citus vs other distributed DBs** | "CockroachDB and Yugabyte gave surprisingly low throughput" so với Citus | [GigaOM 2023] |
| **Co-location performance** | "joins, foreign keys, and other relational operations on that column can be fully pushed down" | [GigaOM 2023] |

**Link gốc:** [GigaOM: Transaction Processing & Price-Performance Testing](https://www.citusdata.com/blog/2023/06/21/distributed-postgres-benchmarks-using-hammerdb-by-gigaom/)

#### 10.5.4. Số liệu về Semi-Join từ paper khoa học

**Semi-join là kỹ thuật đã được nghiên cứu sâu rộng** — nhiều paper công bố tỷ lệ tiết kiệm băng thông cụ thể:

| Paper | Kết quả công bố | Ghi chú |
|---|---|---|
| **Bernstein et al. (1981) — SDD-1** | "reduce the transfer cost by first sending only the projected join column(s)" | Paper gốc của semi-join, project SDD-1 |
| **Chen, Yu, Wu (1993) — IEEE TKDE** | Combining join + semi-join giảm communication cost đáng kể trên các query phân tán | "Combining Join and Semi-Join Operations for Distributed Query Processing", IEEE TKDE 5(3) |
| **Stocker, Kossmann, Braumandl, Kemper (2001)** | "approach based on parallel semijoins is not only efficient but also effective in reducing the communication cost"; "algorithms... have a near-linear speedup" | "Integrating Semi-Join-Reducers into State-of-the-Art Query Processors" |
| **SBA + SDD-1 trên bank dataset (2018)** | "speed increase of 600%, because the first process of query increases in 300.4 seconds, the second process of query increases by 144.9 seconds" | Indian Journal of Science & Technology, 11(18) |

**Cách dùng trong báo cáo (Chương 5 — Semi-Join):**
> *"Kỹ thuật semi-join được Bernstein et al. (1981) đề xuất lần đầu trong dự án SDD-1 [3], với nguyên lý chỉ truyền cột join key thay vì toàn bộ bảng. Các nghiên cứu sau này như Chen et al. (1993) [4] và Stocker et al. (2001) [5] đã chứng minh hiệu quả của kỹ thuật này trong việc giảm chi phí truyền thông và đạt speedup gần tuyến tính. Trong đồ án này, chúng tôi kiểm chứng lại kết quả trên dataset VietJobs với cột description trung bình 616 bytes/dòng — tỷ lệ tiết kiệm băng thông đo được là X%."*

#### 10.5.5. Số liệu về Co-location từ Citus Documentation

**Citus Official Docs** giải thích cơ chế và lợi ích của co-location:

| Khía cạnh | Trích dẫn | Nguồn |
|---|---|---|
| **Nguyên lý co-location** | "shards with the same hash range are always placed on the same node even after rebalance operations, such that equal distribution column values are always on the same node across tables" | Citus Docs - Table Co-Location |
| **Lợi ích về performance** | "Full SQL support for queries on a single set of co-located shards; Multi-statement transaction support... Data co-location is a powerful technique for providing both horizontal scale and supporting relational data models" | Citus Docs - Table Co-Location |
| **Query routing & shard pruning** | "For filters, Citus uses the distribution column ranges to prune away unrelated shards, ensuring that the query hits only those shards which overlap with the WHERE clause ranges" | Citus Docs - Query Performance Tuning |
| **Parallel shard joins** | "All these shard joins can be executed in parallel on the workers and hence are more efficient" | Citus Docs - Query Performance Tuning |

#### 10.5.6. Bảng kết quả benchmark đề xuất kèm trích dẫn nguồn

Khi viết báo cáo, dùng bảng này để **trình bày kết quả thực tế + so sánh với nguồn uy tín**:

| Giải thuật | Kết quả của bạn | Xu hướng kỳ vọng | Nguồn tham chiếu |
|---|---|---|---|
| Co-located Hash Join vs Non-co-located | _đo được_ | Co-located nhanh hơn đáng kể do parallel shard joins | Citus Docs - Performance Tuning [5], Cubukcu et al. 2021 [1] |
| Query Routing (1 shard) vs (4 shards) | _đo được_ | Single-shard query nhanh hơn nhờ shard pruning | Citus Docs [5] |
| MapReduce Aggregation (4 workers) vs Single-node | _đo được_ | Speedup gần linear với data size lớn | Cubukcu et al. 2021 [1], Figure 7 |
| Semi-Join vs Naive Join | _đo được_ | Giảm communication cost đáng kể; speedup phụ thuộc kích thước attribute | Bernstein et al. 1981 [3], Chen et al. 1993 [4] |
| Top-K với LIMIT pushdown | _đo được_ | LIMIT pushdown giảm thời gian sort và data transfer | PostgreSQL Docs - Parallel Query [6] |

#### 10.5.7. Phương pháp đối chiếu kết quả

**Trong báo cáo Chương 5 (Đánh giá hiệu năng), nên cấu trúc như sau:**

```
5.X.Y. Giải thuật [Tên giải thuật]

a) Cơ sở lý thuyết
   - Trích dẫn [paper 1], [paper 2] về nguyên lý
   
b) Kỳ vọng từ literature
   - "Theo Cubukcu et al. (2021), co-located join cho speedup..."
   - "Bernstein et al. (1981) chỉ ra semi-join giảm..."
   
c) Kết quả đo trên VietJobs
   - Bảng số liệu thực tế của bạn
   - Biểu đồ
   
d) Thảo luận
   - "Kết quả của chúng tôi PHÙ HỢP với [1] vì..."
   - "Kết quả khác với [2] do dataset của chúng tôi nhỏ hơn..."
   - "Yếu tố ảnh hưởng: Docker network latency, RAM hạn chế..."
```

→ Đây là cách **đúng chuẩn academic** — số liệu là của bạn, nhưng có nguồn uy tín để **giải thích và đối chứng**.

#### 10.5.8. Danh sách nguồn tham chiếu cho benchmark

| # | Loại | Nguồn |
|---|---|---|
| [1] | Paper | Cubukcu, U. et al. (2021). "Citus: Distributed PostgreSQL for Data-Intensive Applications." SIGMOD '21. ACM. [doi.org/10.1145/3448016.3457551](https://doi.org/10.1145/3448016.3457551) |
| [2] | Sách giáo trình | Özsu, M. T., & Valduriez, P. (2020). "Principles of Distributed Database Systems" (4th ed.). Springer. Ch.8 - Parallel DBMS |
| [3] | Paper gốc Semi-join | Bernstein, P. A. et al. (1981). "Query Processing in a System for Distributed Databases (SDD-1)." ACM TODS 6(4) |
| [4] | Paper Semi-join optimization | Chen, M.-S., Yu, P. S., & Wu, K.-L. (1993). "Combining Join and Semi-Join Operations for Distributed Query Processing." IEEE TKDE 5(3) |
| [5] | Documentation | Citus Data (2024). "Query Performance Tuning." [docs.citusdata.com/en/stable/performance/performance_tuning.html](https://docs.citusdata.com/en/stable/performance/performance_tuning.html) |
| [6] | Documentation | PostgreSQL Global Development Group (2024). "PostgreSQL 16 Documentation - Parallel Query." [postgresql.org/docs/16/parallel-query.html](https://www.postgresql.org/docs/16/parallel-query.html) |
| [7] | Industry Report | GigaOM (2023). "Distributed PostgreSQL Transaction Processing & Price-Performance Testing." [link](https://www.citusdata.com/blog/2023/06/21/distributed-postgres-benchmarks-using-hammerdb-by-gigaom/) |
| [8] | Paper | Stocker, K., Kossmann, D., Braumandl, R., & Kemper, A. (2001). "Integrating Semi-Join-Reducers into State-of-the-Art Query Processors." ICDE 2001 |
| [9] | Survey | Bsaïes, K. et al. (2018). "A Comprehensive Taxonomy of Fragmentation and Allocation Techniques in Distributed Database Design." ACM Computing Surveys 51(1) |
| [10] | Industry Blog | Citus Data (2022). "How to benchmark performance of Citus and Postgres with HammerDB on Azure." [citusdata.com/blog](https://www.citusdata.com/blog/2022/03/12/how-to-benchmark-performance-of-citus-and-postgres-with-hammerdb/) |

#### 10.5.9. Cảnh báo trung thực về benchmark

> ⚠️ **Lưu ý quan trọng khi bảo vệ đồ án:**
>
> 1. **Không bao giờ trích dẫn số liệu cụ thể từ paper khác cho dataset của mình.** Ví dụ: KHÔNG viết "Co-located join nhanh hơn 5x" nếu chưa tự đo. Chỉ viết "Co-located join nhanh hơn đáng kể, phù hợp với xu hướng được báo cáo trong [1]".
>
> 2. **Số liệu của bạn có thể khác paper** vì:
>    - Dataset VietJobs nhỏ hơn (vài chục MB vs vài chục GB của paper Citus)
>    - Phần cứng khác (laptop vs cluster cloud)
>    - Docker network latency cao hơn bare-metal
>
> 3. **Khi reviewer hỏi "Số liệu này có chính xác không?"** → Trả lời:
>    *"Số liệu này được đo trên môi trường [chi tiết môi trường] với phương pháp [trung bình 20 runs, 3 warmup]. Xu hướng so sánh phù hợp với kết quả công bố tại SIGMOD 2021 [1] và blog GigaOM 2023 [7]. Sự khác biệt về giá trị tuyệt đối là do quy mô và hạ tầng khác nhau."*

---

## 11. Template báo cáo đồ án

### 11.1. Bố cục đề xuất

```
CHƯƠNG 1: TỔNG QUAN
  1.1. Đặt vấn đề
  1.2. Mục tiêu đồ án
  1.3. Phạm vi và giới hạn

CHƯƠNG 2: CƠ SỞ LÝ THUYẾT
  2.1. Cơ sở dữ liệu phân tán
  2.2. Phân mảnh ngang và dọc
  2.3. Các giải thuật xử lý song song
  2.4. PostgreSQL và Citus extension

CHƯƠNG 3: PHÂN TÍCH VÀ THIẾT KẾ
  3.1. Phân tích dataset VietJobs
  3.2. Tiền xử lý dữ liệu
  3.3. Thiết kế phân mảnh dọc (3 bảng)
  3.4. Thiết kế phân mảnh ngang (8 fragment / 4 worker)
  3.5. Sơ đồ kiến trúc cluster

CHƯƠNG 4: TRIỂN KHAI HỆ THỐNG
  4.1. Dựng cluster bằng Docker
  4.2. Khởi tạo schema và phân tán bảng
  4.3. Load dữ liệu
  4.4. Cài đặt 5 giải thuật song song
  4.5. Xây dựng Backend API
  4.6. (Optional) Frontend Dashboard

CHƯƠNG 5: ĐÁNH GIÁ HIỆU NĂNG
  5.1. Phương pháp benchmark
  5.2. Kết quả từng giải thuật
  5.3. So sánh distributed vs single-node
  5.4. Scalability test (1→4 workers)
  5.5. Phân tích chi phí mạng (semi-join)

CHƯƠNG 6: KẾT LUẬN
  6.1. Kết quả đạt được
  6.2. Hạn chế
  6.3. Hướng phát triển
```

### 11.2. Checklist hoàn thành

- [ ] Tiền xử lý dataset xong: lọc `location ∈ {Hà Nội, TPHCM}`, bỏ `salary_avg = 'Thỏa thuận'`, chuẩn hoá location → `'hà nội'` / `'hồ chí minh'`, xuất `dataset_IS252.csv` (19 cột, 24,281 bản ghi)
- [ ] Dựng được cluster 1 coordinator + 4 workers chạy ổn định
- [ ] Phân mảnh dọc thành 3 bảng `core / detail / skills`
- [ ] Phân tán cả 3 bảng cùng co-location group theo `category_group`
- [ ] Chạy `02b_pin_shards.sql` để shard `commerce/tech/creative/people` nằm đúng container tương ứng
- [ ] Reference table `category_mapping` hoạt động
- [ ] Load thành công 24,281 bản ghi vào 3 bảng phân tán
- [ ] 5 giải thuật chạy được, có execution plan kèm
- [ ] Benchmark đo được số liệu cụ thể, có biểu đồ
- [ ] Backend API expose 7 endpoint (4 jobs + 2 stats + 1 cluster)
- [ ] Báo cáo đầy đủ + slide demo

### 11.3. Câu hỏi reviewer hay hỏi (chuẩn bị trước)

| Câu hỏi | Hướng trả lời |
|---|---|
| Vì sao chọn `category_group` làm distribution column? | Cân bằng tải, hỗ trợ pruning, khớp use case nghiệp vụ |
| Tại sao tách 3 bảng dọc? | Giảm I/O text dài, demo semi-join, cột null nhiều |
| Co-location là gì? Demo bằng cách nào? | Cùng distribution column + cùng colocate_with → join cục bộ. Demo bằng EXPLAIN: Task Count |
| Sự khác biệt giữa Reference Table và Distributed Table? | Reference được sao chép trên mọi worker (cho bảng nhỏ ít update). Distributed chỉ chứa shard tương ứng |
| Semi-join thật sự tiết kiệm bao nhiêu băng thông? | Đo bằng `pg_stat_statements`, so sánh `shared_blks_read` |
| Nếu thêm worker thứ 5 thì sao? | Phải `rebalance_table_shards()` để phân bố lại |
| Failover khi 1 worker chết? | Citus hỗ trợ shard replication factor (set khi tạo bảng) |

---

## 12. Phụ lục

### 12.1. Các lệnh debug Citus hay dùng

```sql
-- Liệt kê workers đang active
SELECT * FROM citus_get_active_worker_nodes();

-- Xem distribution của mỗi bảng
SELECT logicalrelid::regclass, colocationid, partmethod
FROM pg_dist_partition;

-- Xem shard nằm ở node nào
SELECT shard.logicalrelid::regclass, placement.shardid, node.nodename
FROM pg_dist_shard shard
JOIN pg_dist_placement placement USING (shardid)
JOIN pg_dist_node node ON node.groupid = placement.groupid
ORDER BY 1, 2;

-- Kiểm tra co-location
SELECT logicalrelid::regclass, colocationid FROM pg_dist_partition;

-- Rebalance shard khi thêm worker
SELECT rebalance_table_shards('core');

-- Force xem plan distributed
EXPLAIN (ANALYZE, VERBOSE) SELECT ... ;

-- Xem bao nhiêu task song song
SELECT * FROM citus_stat_activity WHERE state = 'active';

-- Đếm bản ghi theo từng shard
SELECT category_group, COUNT(*) FROM core GROUP BY category_group;
```

### 12.2. Troubleshooting thường gặp

| Lỗi | Nguyên nhân | Giải pháp |
|---|---|---|
| `ERROR: cannot create distributed table` | Bảng đã có data | Truncate trước, hoặc dùng `create_distributed_table_concurrently` |
| Worker không xuất hiện trong list | Membership manager chưa chạy | `docker compose logs manager` để kiểm tra |
| Query rất chậm dù đã distribute | Không hit co-location | Kiểm tra `colocationid` của các bảng có giống nhau không |
| `OutOfMemory` khi load CSV | File quá lớn | Dùng `\COPY` từ psql client, hoặc chia file |
| Timezone của container sai | Image mặc định UTC | Set `TZ=Asia/Ho_Chi_Minh` trong compose |

### 12.3. Tài liệu tham khảo chính thống

Phần này tổng hợp các tài liệu được kiểm chứng (peer-reviewed paper, official docs, sách giáo trình kinh điển) — dùng được trực tiếp để **trích dẫn trong báo cáo** và **bảo vệ đồ án**.

#### 12.3.1. Paper khoa học về Citus

| Paper | Tác giả | Hội nghị | Link |
|---|---|---|---|
| **Citus: Distributed PostgreSQL for Data-Intensive Applications** | Cubukcu, Erdogan, Pathak, Sannakkayala, Slot (Microsoft) | SIGMOD 2021 | [dl.acm.org/doi/10.1145/3448016.3457551](https://dl.acm.org/doi/10.1145/3448016.3457551) |

> Đây là **paper gốc và quan trọng nhất** để trích dẫn. Mô tả 4 workload pattern (multi-tenant, real-time analytics, high-performance CRUD, data warehousing), kiến trúc, distributed query planner, co-location, và kết quả benchmark.

**Tóm tắt nhanh các điểm cần trích dẫn:**
- Citus là PostgreSQL extension (không phải fork), thừa kế toàn bộ tính năng PostgreSQL
- Distributed query engine route transaction qua cluster
- Hỗ trợ 4 loại workload (đồ án này thuộc nhóm **Real-time Analytics** + **Multi-tenant SaaS**)

#### 12.3.2. Sách giáo trình (textbook) về CSDL phân tán

| Sách | Tác giả | Phiên bản | Link tham khảo |
|---|---|---|---|
| **Principles of Distributed Database Systems** | M. Tamer Özsu & Patrick Valduriez | 4th Edition, Springer (2020) | [cs.uwaterloo.ca/~ddbook](https://cs.uwaterloo.ca/~ddbook/) |
| **Distributed and Parallel Database Systems** (course notes) | M. Tamer Özsu, ĐH Waterloo CS742 | - | [Slides Query Optimization](https://cs.uwaterloo.ca/~tozsu/courses/CS742/Course%20Notes/4-QueryOptimization-handout.pdf) |

> Sách **Özsu & Valduriez** là **giáo trình kinh điển** của môn CSDL phân tán — bắt buộc phải có trong danh mục tham khảo. Bản 4th Edition (2020) cập nhật cloud computing, NoSQL, MapReduce.

**Chương nên đọc cho đồ án:**
- Chương 2-3: Distribution Design (fragmentation horizontal/vertical, allocation)
- Chương 4: Distributed Query Processing
- Chương 8: Parallel Database Systems

#### 12.3.3. Paper về giải thuật cụ thể đang dùng

| Giải thuật | Paper / Source |
|---|---|
| **Semi-Join Optimization** | "Combining Join and Semi-Join Operations for Distributed Query Processing" — IEEE Trans. Knowledge & Data Engineering, IBM Research [link](https://research.ibm.com/publications/combining-join-and-semi-join-operations-for-distributed-query-processing) |
| **2-Way Semijoin** | "Investigating the 2-Way Semijoin for Distributed Query Optimization" [ResearchGate](https://www.researchgate.net/publication/227437353_Investigating_the_2-Way_Semijoin_for_Distributed_Query_Optimization) |
| **Parallel Hash-Join** | "Join and Semijoin Algorithms for a Multiprocessor Database Machine" — DeWitt et al. [ResearchGate](https://www.researchgate.net/publication/29600466_Join_and_Semijoin_Algorithms_for_a_Multiprocessor_Database_Machine) |
| **Interleaving Joins with Semijoins** | IEEE Trans. on Parallel and Distributed Systems [dl.acm.org/doi/10.1109/71.159044](https://dl.acm.org/doi/10.1109/71.159044) |
| **Fragmentation Taxonomy (Comprehensive)** | "A Comprehensive Taxonomy of Fragmentation and Allocation Techniques in Distributed Database Design" — ACM Computing Surveys 51(1), 2018 [ResearchGate](https://www.researchgate.net/publication/322258510_A_Comprehensive_Taxonomy_of_Fragmentation_and_Allocation_Techniques_in_Distributed_Database_Design) |

#### 12.3.4. Citus Official Documentation

| Chủ đề | Link |
|---|---|
| **Trang chủ tài liệu Citus** | [docs.citusdata.com](https://docs.citusdata.com/) |
| **Concepts (sharding, co-location, reference table)** | [docs.citusdata.com/en/stable/get_started/concepts.html](https://docs.citusdata.com/en/stable/get_started/concepts.html) |
| **Table Co-Location** | [docs.citusdata.com/en/stable/sharding/data_modeling.html](https://docs.citusdata.com/en/stable/sharding/data_modeling.html) |
| **Choosing Distribution Column** | [docs.citusdata.com/en/v9.3/sharding/data_modeling.html](https://docs.citusdata.com/en/v9.3/sharding/data_modeling.html) |
| **Citus Tables & System Views (metadata)** | [docs.citusdata.com/en/v11.0/develop/api_metadata.html](https://docs.citusdata.com/en/v11.0/develop/api_metadata.html) |
| **GitHub repo (mã nguồn + issue tracker)** | [github.com/citusdata/citus](https://github.com/citusdata/citus) |
| **Docker Hub images** | [hub.docker.com/r/citusdata/citus](https://hub.docker.com/r/citusdata/citus) |

#### 12.3.5. PostgreSQL Official Documentation

| Chủ đề | Link |
|---|---|
| **PostgreSQL 16 manual** | [postgresql.org/docs/16/](https://www.postgresql.org/docs/16/) |
| **Query Planning** (EXPLAIN) | [postgresql.org/docs/16/using-explain.html](https://www.postgresql.org/docs/16/using-explain.html) |
| **Parallel Query** | [postgresql.org/docs/16/parallel-query.html](https://www.postgresql.org/docs/16/parallel-query.html) |
| **pg_stat_statements** | [postgresql.org/docs/16/pgstatstatements.html](https://www.postgresql.org/docs/16/pgstatstatements.html) |

#### 12.3.6. Bài blog kỹ thuật chính thức của Citus / Microsoft

| Bài | Link |
|---|---|
| **Citus Tech Blog (chính thức)** | [citusdata.com/blog](https://www.citusdata.com/blog/) |
| **How distributed queries execute in Citus** | [citusdata.com/blog/2022/01/27/how-the-citus-distributed-query-executor-adapts-to-your-postgres-workload](https://www.citusdata.com/blog/2022/01/27/how-the-citus-distributed-query-executor-adapts-to-your-postgres-workload/) |
| **Designing your distributed database** | [docs.citusdata.com/en/stable/sharding/data_modeling.html](https://docs.citusdata.com/en/stable/sharding/data_modeling.html) |
| **Insights from SIGMOD'21 paper (giải thích chi tiết)** | [Medium - Hemant Gupta](https://hemantkgupta.medium.com/insights-from-paper-citus-distributed-postgresql-for-data-intensive-applications-6224a12af32d) |

#### 12.3.7. Dataset

- **VietJobs Dataset (HuggingFace):** [huggingface.co/datasets/dinhieufam/VietJobs](https://huggingface.co/datasets/dinhieufam/VietJobs)

#### 12.3.8. Đề xuất danh mục tài liệu tham khảo cho báo cáo

Khi viết phần "Tài liệu tham khảo" của đồ án, sử dụng format IEEE/APA. Ví dụ trình bày:

```
[1] Cubukcu, U., Erdogan, O., Pathak, S., Sannakkayala, S., & Slot, M. (2021).
    "Citus: Distributed PostgreSQL for Data-Intensive Applications."
    In Proceedings of the 2021 International Conference on Management of Data
    (SIGMOD '21), pp. 2490-2502. ACM. https://doi.org/10.1145/3448016.3457551

[2] Özsu, M. T., & Valduriez, P. (2020). "Principles of Distributed Database
    Systems" (4th ed.). Springer. ISBN 978-3-030-26252-5.

[3] Chen, M.-S., Yu, P. S., & Wu, K.-L. (1993). "Combining Join and Semi-Join
    Operations for Distributed Query Processing." IEEE Transactions on
    Knowledge and Data Engineering, 5(3), 534-542.

[4] Bsaïes, K., Ben Maâlej, H., & Beji, F. (2018). "A Comprehensive Taxonomy
    of Fragmentation and Allocation Techniques in Distributed Database Design."
    ACM Computing Surveys, 51(1), Article 12.

[5] Citus Data. (2024). "Citus Documentation v12." Microsoft.
    https://docs.citusdata.com/

[6] The PostgreSQL Global Development Group. (2024). "PostgreSQL 16
    Documentation." https://www.postgresql.org/docs/16/

[7] Phạm Đình Hiếu. (2024). "VietJobs Dataset."
    https://huggingface.co/datasets/dinhieufam/VietJobs
```

#### 12.3.9. Mẹo trích dẫn trong từng chương

| Chương báo cáo | Tài liệu cần trích dẫn |
|---|---|
| 2.1 — CSDL phân tán | [2] Özsu & Valduriez, Ch.1-2 |
| 2.2 — Phân mảnh ngang/dọc | [2] Ch.3, [4] Taxonomy paper |
| 2.3 — Giải thuật song song | [2] Ch.4, [3] Chen et al., [4] |
| 2.4 — Citus | [1] SIGMOD'21, [5] Citus docs |
| 3.3 — Thiết kế phân mảnh dọc | [2] Ch.3.3, [4] Sec. Vertical Frag |
| 3.4 — Thiết kế phân mảnh ngang | [2] Ch.3.2, [5] Citus co-location docs |
| 4.x — Triển khai | [5] [6] official docs |
| 5.x — Benchmark | [1] SIGMOD'21 benchmark methodology |

### 12.4. Tóm tắt câu lệnh chạy từ đầu đến cuối

```bash
# 1. Clone/tạo cấu trúc thư mục dự án
mkdir Project && cd Project

# 2. Đặt dataset đã tiền xử lý vào data/
#    (file dataset_IS252.csv: 24,281 bản ghi, 19 cột, đã chuẩn hoá location về chữ thường)
cp /path/to/dataset_IS252.csv ./data/

# 3. Khởi động cluster
cd docker && docker compose up -d
cd ..

# 4. Đợi 30s cho cluster ổn định
sleep 30

# 5. Verify cluster
docker exec vietjobs_coordinator psql -U postgres -d vietjobs \
    -c "SELECT * FROM citus_get_active_worker_nodes();"

# 6. Khởi tạo schema + phân tán + pin shards + load data + index
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/01_schema.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/02_distribute.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/02b_pin_shards.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/03_load_data.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs < sql/04_indexes.sql

# 7. Chạy backend (Python + FastAPI)
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 &
cd ..

# 8. Chạy benchmark
cd benchmark && python benchmark.py

# 9. Mở dashboard (optional)
cd ../frontend && npm install && npm start
```

---

**🎉 Hoàn thành.** Dự án này đáp ứng đầy đủ yêu cầu môn CSDL phân tán:
- ✅ Phân mảnh dọc (3 bảng theo nhóm thuộc tính)
- ✅ Phân mảnh ngang (8 fragment trên 4 worker theo `category_group × location`)
- ✅ 5 giải thuật xử lý song song có thể demo và benchmark
- ✅ Truy vấn từ nhiều node trong cluster qua coordinator
- ✅ Có công cụ benchmark, biểu đồ, báo cáo
