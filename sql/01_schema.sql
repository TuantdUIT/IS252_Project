-- =============================================================================
-- FILE: 01_schema.sql
-- Định nghĩa cấu trúc bảng (DDL) cho coordinator.
-- Thực hiện phân mảnh dọc ở tầng schema: tách bảng gốc thành 3 bảng
-- core / detail / skills theo nhóm thuộc tính.
-- Chạy đầu tiên trong pipeline, trước 02_distribute.sql.
-- =============================================================================


-- Kích hoạt Citus extension trên database vietjobs (coordinator)
CREATE EXTENSION IF NOT EXISTS citus;


-- Drop để cho phép chạy lại script không bị lỗi "table already exists"
DROP TABLE IF EXISTS staging_jobs    CASCADE;
DROP TABLE IF EXISTS skills          CASCADE;
DROP TABLE IF EXISTS detail          CASCADE;
DROP TABLE IF EXISTS core            CASCADE;
DROP TABLE IF EXISTS category_mapping CASCADE;


-- =============================================================================
-- BẢNG STAGING: vùng trung chuyển nhận dữ liệu thô từ CSV
-- -----------------------------------------------------------------------------
-- Khớp 100% với 19 cột của dataset_IS252.csv:
--   - Cột đầu _idx nuốt cột index dư thừa do pandas to_csv() sinh ra
--   - 18 cột nghiệp vụ giữ kiểu TEXT/INTEGER/NUMERIC tương ứng
-- Bảng này là PostgreSQL thường, không phân tán, sẽ DROP ở 03_load_data.sql.
-- =============================================================================
CREATE TABLE staging_jobs (
    _idx                INTEGER,
    job_id              SERIAL,
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
    salary_avg          NUMERIC
);


-- =============================================================================
-- BẢNG CORE: phân mảnh dọc #1 — cột nhẹ, hay query (90% workload)
-- -----------------------------------------------------------------------------
-- Chứa thông tin cơ bản dùng cho filter/sort: location, salary, experience...
-- PRIMARY KEY kép (category_group, job_id) — Citus yêu cầu PK phải chứa
-- distribution column thì mới gọi được create_distributed_table().
-- =============================================================================
CREATE TABLE core (
    job_id              INTEGER,
    category_group      INTEGER NOT NULL,
    category            TEXT    NOT NULL,
    location            TEXT    NOT NULL,
    job_title           TEXT,
    salary_min          INTEGER,
    salary_max          INTEGER,
    salary_avg          NUMERIC,
    experience_required TEXT,
    contract_type       TEXT,
    working_hours       TEXT,
    country             TEXT,
    PRIMARY KEY (category_group, job_id)
);


-- =============================================================================
-- BẢNG DETAIL: phân mảnh dọc #2 — text dài, lazy-load
-- -----------------------------------------------------------------------------
-- Tách riêng description (~616B/dòng), requirements_text, benefits để query
-- không cần text không phải đọc các block lớn này.
-- Co-locate với core qua cùng distribution key (category_group).
-- =============================================================================
CREATE TABLE detail (
    job_id              INTEGER,
    category_group      INTEGER NOT NULL,
    description         TEXT,
    requirements_text   TEXT,
    benefits            TEXT,
    PRIMARY KEY (category_group, job_id)
);


-- =============================================================================
-- BẢNG SKILLS: phân mảnh dọc #3 — kỹ năng & yêu cầu
-- -----------------------------------------------------------------------------
-- Chứa các cột có tỷ lệ null cao (languages_required ~74% null) và cấu trúc
-- array-like (technical_skills) — không nên để chung core làm phình row size.
-- =============================================================================
CREATE TABLE skills (
    job_id              INTEGER,
    category_group      INTEGER NOT NULL,
    technical_skills    TEXT,
    soft_skills         TEXT,
    qualifications      TEXT,
    languages_required  TEXT,
    PRIMARY KEY (category_group, job_id)
);


-- =============================================================================
-- BẢNG CATEGORY_MAPPING: reference table — ánh xạ 16 category gốc → 4 nhóm
-- -----------------------------------------------------------------------------
-- Vai trò:
--   1. Logic: derive category_group khi INSERT từ staging vào core/detail/skills
--   2. Reference table: ở 02_distribute.sql sẽ được sao chép trên mọi worker
--      để JOIN cục bộ không tốn network.
-- 16 dòng dữ liệu là tĩnh, viết inline luôn trong schema file.
-- =============================================================================
CREATE TABLE category_mapping (
    category        TEXT    PRIMARY KEY,
    category_group  INTEGER NOT NULL,
    group_name      TEXT    NOT NULL
);

INSERT INTO category_mapping (category, category_group, group_name) VALUES
    -- Nhóm 1: commerce (3 category) → worker1_commerce
    ('kinh_doanh_bán_hàng_chăm_sóc_khách_hàng', 1, 'commerce'),
    ('tài_chính_kế_toán_ngân_hàng_bảo_hiểm',    1, 'commerce'),
    ('logistics_vận_tải_chuỗi_cung_ứng',        1, 'commerce'),

    -- Nhóm 2: tech (5 category) → worker2_tech
    ('sản_xuất_lao_động_phổ_thông_cơ_khí',      2, 'tech'),
    ('công_nghệ_thông_tin_kỹ_thuật_số',         2, 'tech'),
    ('kỹ_thuật_điện_điện_tử_viễn_thông',        2, 'tech'),
    ('xây_dựng_kiến_trúc_bất_động_sản',         2, 'tech'),
    ('nông_nghiệp_năng_lượng_môi_trường',       2, 'tech'),

    -- Nhóm 3: creative (4 category) → worker3_creative
    ('marketing_truyền_thông_quảng_cáo_nội_dung',           3, 'creative'),
    ('thiết_kế_nghệ_thuật_giải_trí_truyền_hình_báo_chí',    3, 'creative'),
    ('du_lịch_nhà_hàng_khách_sạn_dịch_vụ',                  3, 'creative'),
    ('ngôn_ngữ_dịch_thuật',                                 3, 'creative'),

    -- Nhóm 4: people (4 category) → worker4_people
    ('nhân_sự_hành_chính_pháp_chế_tư_vấn',                  4, 'people'),
    ('giáo_dục_đào_tạo_nghiên_cứu',                         4, 'people'),
    ('y_tế_dược_chăm_sóc_sức_khỏe_công_nghệ_sinh_học',      4, 'people'),
    ('nhóm_nghề_khác',                                      4, 'people');
