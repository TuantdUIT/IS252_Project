-- =============================================================================
-- FILE: 03_load_data.sql
-- Pipeline ETL: CSV → staging → 3 bảng phân tán.
-- Chạy sau 02b_pin_shards.sql (khi shard đã pin về đúng worker).
--
-- Flow:
--   1. COPY bulk-load dataset_IS252.csv vào staging_jobs
--   2. INSERT...SELECT từ staging sang core/detail/skills, kèm:
--      - Bỏ cột _idx thừa của pandas
--      - JOIN category_mapping để derive category_group
--      - Chuẩn hoá location ('hà nội' → 'Hà Nội', 'hồ chí minh' → 'TPHCM')
--      - Phân mảnh dọc: 1 dòng CSV → 3 dòng trong 3 bảng
--   3. DROP staging_jobs sau khi xong
--   4. Verify phân bố
-- =============================================================================


-- =============================================================================
-- BƯỚC 1: COPY CSV vào staging_jobs
-- -----------------------------------------------------------------------------
-- /data là volume mount từ ../data của host vào container coordinator.
-- HEADER true: dòng đầu CSV là tên cột, không phải data.
-- ENCODING 'UTF8': cần thiết để đọc đúng tiếng Việt có dấu.
-- Liệt kê cột tường minh (gồm _idx) để khớp với 19 cột CSV; job_id SERIAL
-- không nằm trong CSV nên không khai báo ở đây — Citus tự sinh.
-- =============================================================================
COPY staging_jobs (
    _idx, job_title, location, country, qualifications, technical_skills,
    soft_skills, languages_required, experience_required, salary,
    contract_type, working_hours, benefits, description,
    requirements_text, category, salary_min, salary_max, salary_avg
)
FROM '/data/dataset_IS252.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');


-- =============================================================================
-- BƯỚC 2A: INSERT vào CORE — phân mảnh dọc #1
-- -----------------------------------------------------------------------------
-- Các phép biến đổi trong query:
--   - JOIN category_mapping cm: derive category_group từ category gốc
--   - CASE WHEN: chuẩn hoá location về format thống nhất
--   - Bỏ cột _idx và các cột không thuộc nhóm "nhẹ"
-- Dùng INSERT...SELECT thay vì INSERT per row vì:
--   - Tận dụng bulk operation của PostgreSQL
--   - Citus tự route từng dòng về đúng worker dựa trên category_group
-- =============================================================================
INSERT INTO core (
    job_id, category_group, category, location, job_title,
    salary_min, salary_max, salary_avg,
    experience_required, contract_type, working_hours, country
)
SELECT
    s.job_id,
    cm.category_group,
    s.category,
    CASE
        WHEN LOWER(s.location) = 'hà nội'      THEN 'Hà Nội'
        WHEN LOWER(s.location) = 'hồ chí minh' THEN 'TPHCM'
        ELSE s.location
    END AS location,
    s.job_title,
    s.salary_min, s.salary_max, s.salary_avg,
    s.experience_required, s.contract_type, s.working_hours, s.country
  FROM staging_jobs s
  JOIN category_mapping cm ON cm.category = s.category;


-- =============================================================================
-- BƯỚC 2B: INSERT vào DETAIL — phân mảnh dọc #2
-- -----------------------------------------------------------------------------
-- Chỉ giữ 3 cột text nặng. Cùng job_id và category_group với core
-- để co-locate join hoạt động.
-- =============================================================================
INSERT INTO detail (job_id, category_group, description, requirements_text, benefits)
SELECT
    s.job_id,
    cm.category_group,
    s.description,
    s.requirements_text,
    s.benefits
  FROM staging_jobs s
  JOIN category_mapping cm ON cm.category = s.category;


-- =============================================================================
-- BƯỚC 2C: INSERT vào SKILLS — phân mảnh dọc #3
-- -----------------------------------------------------------------------------
-- 4 cột kỹ năng & yêu cầu trình độ.
-- =============================================================================
INSERT INTO skills (
    job_id, category_group, technical_skills, soft_skills,
    qualifications, languages_required
)
SELECT
    s.job_id,
    cm.category_group,
    s.technical_skills,
    s.soft_skills,
    s.qualifications,
    s.languages_required
  FROM staging_jobs s
  JOIN category_mapping cm ON cm.category = s.category;


-- =============================================================================
-- BƯỚC 3: DROP staging_jobs
-- -----------------------------------------------------------------------------
-- Staging chỉ là trung gian ETL, không cần giữ. DROP giải phóng ~50MB.
-- =============================================================================
DROP TABLE staging_jobs;


-- =============================================================================
-- BƯỚC 4: VERIFY phân bố dữ liệu
-- -----------------------------------------------------------------------------
-- Kỳ vọng: ~24,281 dòng tổng, phân bố vào 4 nhóm × 2 location.
-- Kiểm tra:
--   - Tổng số dòng mỗi bảng có khớp (core = detail = skills)
--   - Mỗi (category_group, location) có số dòng hợp lý
--   - Không có category_group NULL (báo lỗi JOIN với category_mapping)
-- =============================================================================
SELECT 'core'    AS table_name, COUNT(*) AS rows FROM core
UNION ALL
SELECT 'detail',  COUNT(*) FROM detail
UNION ALL
SELECT 'skills',  COUNT(*) FROM skills;

SELECT category_group, location, COUNT(*) AS num_records
  FROM core
 GROUP BY category_group, location
 ORDER BY category_group, location;
