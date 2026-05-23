# VietJobs Distributed Database — Đồ án CSDL Phân tán

> Triển khai PostgreSQL + Citus trên Docker với phân mảnh dọc (3 bảng) + phân
> mảnh ngang (4 worker × 2 location) và 5 giải thuật xử lý song song.

---

## Quyết định khác với hướng dẫn gốc

| Hạng mục | Hướng dẫn gốc | Dự án thực tế | Lý do |
|---|---|---|---|
| Tên dataset | `vietjobs_clean.csv` | **`dataset_IS252.csv`** | Giữ tên file tiền xử lý thực tế |
| Số cột CSV | 18 | **19** (có cột index đầu) | Pandas xuất kèm index column |
| Format location | `'Hà Nội'` / `'TPHCM'` | **`'hà nội'` / `'hồ chí minh'`** | Chuẩn hoá ngay trong INSERT (CASE WHEN) |
| Pin shard theo worker | Không có | **Có** (`02b_pin_shards.sql`) | Cho khớp tên container với nội dung shard |

---

## Cấu trúc thư mục

```
Project/
├── docker/docker-compose.yml     # 1 coordinator + 4 workers + manager
├── data/dataset_IS252.csv        # Dataset đã tiền xử lý (24,281 bản ghi)
├── sql/                          # Scripts khởi tạo CSDL
│   ├── 01_schema.sql
│   ├── 02_distribute.sql
│   ├── 02b_pin_shards.sql        # Pin shard về đúng worker theo nhãn
│   ├── 03_load_data.sql
│   └── 04_indexes.sql
├── algorithms/                   # 5 giải thuật song song
│   ├── 01_hash_join.sql
│   ├── 02_query_routing.sql
│   ├── 03_mapreduce_agg.sql
│   ├── 04_semi_join.sql
│   └── 05_parallel_topk.sql
├── backend/                      # FastAPI + asyncpg
│   ├── main.py, database.py, models.py
│   ├── routers/{jobs,stats,cluster}.py
│   ├── requirements.txt
│   └── .env
├── frontend/                     # React 18 + TypeScript + Vite
│   ├── package.json, tsconfig.json, vite.config.ts
│   └── src/{main.tsx, App.tsx, api/, components/}
├── benchmark/
│   ├── benchmark.py
│   └── requirements.txt
└── README.md
```

---

## Lộ trình triển khai

### Pha 1 — Hạ tầng & Schema

- [x] Tạo cấu trúc thư mục
- [x] Viết `docker/docker-compose.yml`
- [x] Đặt dataset `dataset_IS252.csv` vào `data/`
- [x] Viết `sql/02b_pin_shards.sql`
- [ ] Viết `sql/01_schema.sql` — staging 19 cột + 3 bảng phân tán + reference table
- [ ] Viết `sql/02_distribute.sql` — `create_distributed_table` + co-location
- [ ] Viết `sql/03_load_data.sql` — COPY từ `/data/dataset_IS252.csv` + chuẩn hoá location
- [ ] Viết `sql/04_indexes.sql` — index cho routing + semi-join

### Pha 2 — Khởi động cluster

- [ ] `docker compose up -d`
- [ ] Verify 4 workers active (`citus_get_active_worker_nodes()`)
- [ ] Chạy lần lượt: `01_schema.sql` → `02_distribute.sql` → `02b_pin_shards.sql` → `03_load_data.sql` → `04_indexes.sql`
- [ ] Verify phân bố: mỗi worker chứa 3 shard (core/detail/skills) cùng `category_group`

### Pha 3 — 5 giải thuật song song

- [ ] Viết `algorithms/01_hash_join.sql` — Parallel Hash Join (co-located)
- [ ] Viết `algorithms/02_query_routing.sql` — Query Routing + Shard Pruning
- [ ] Viết `algorithms/03_mapreduce_agg.sql` — MapReduce 2-phase Aggregation
- [ ] Viết `algorithms/04_semi_join.sql` — Semi-Join (4 versions)
- [ ] Viết `algorithms/05_parallel_topk.sql` — Parallel Sort-Merge / Top-K
- [ ] Chạy `EXPLAIN ANALYZE` từng query, lưu execution plan

### Pha 4 — Backend API

- [ ] Cài đặt: `cd backend && python -m venv venv && pip install -r requirements.txt`
- [ ] Viết `backend/database.py` — asyncpg pool
- [ ] Viết `backend/models.py` — Pydantic response models
- [ ] Viết `backend/routers/{jobs,stats,cluster}.py` — 7 endpoint
- [ ] Viết `backend/main.py` — FastAPI app entry
- [ ] Test endpoints qua Swagger UI tại `http://localhost:8000/docs`

### Pha 5 — Frontend (optional)

- [ ] `cd frontend && npm install`
- [ ] Viết `src/main.tsx`, `src/App.tsx`
- [ ] Component hiển thị: cluster status, query runner, benchmark chart
- [ ] `npm run dev` chạy ở `http://localhost:5173`

### Pha 6 — Benchmark & Báo cáo

- [ ] Cài đặt: `cd benchmark && pip install -r requirements.txt`
- [ ] Viết `benchmark/benchmark.py` — 6 query × 20 runs × 3 warmup
- [ ] Chạy benchmark, lưu kết quả `results/benchmark.json`
- [ ] Vẽ biểu đồ `plots/benchmark.png` (bar chart + error bars)
- [ ] Scalability test: 1 → 2 → 4 workers, vẽ speedup chart
- [ ] Viết báo cáo theo template Chương 1-6 (mục 11.1 hướng dẫn)

---

## Lệnh nhanh

```powershell
# Khởi động cluster
cd docker; docker compose up -d

# Verify
docker exec vietjobs_coordinator psql -U postgres -d vietjobs `
  -c "SELECT * FROM citus_get_active_worker_nodes();"

# Chạy schema theo thứ tự
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs -f /sql/01_schema.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs -f /sql/02_distribute.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs -f /sql/02b_pin_shards.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs -f /sql/03_load_data.sql
docker exec -i vietjobs_coordinator psql -U postgres -d vietjobs -f /sql/04_indexes.sql

# Backend
cd ..\backend; python -m venv venv; venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

# Frontend
cd ..\frontend; npm install; npm run dev

# Benchmark
cd ..\benchmark; pip install -r requirements.txt; python benchmark.py
```
