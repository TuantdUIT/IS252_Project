-- =============================================================================
-- FILE: 02_query_routing.sql
-- Thuật toán: Query Routing và Shard Pruning
--
-- Nguyên lý:
--   Citus lưu metadata mapping: hash(distribution_value) → shard_id → worker.
--   Khi query có điều kiện đẳng thức trên distribution column
--   (category_group = X), Citus tính hash(X) tại coordinator, xác định đúng
--   shard, và chỉ gửi sub-query đến worker chứa shard đó — Shard Pruning.
--
--   Hai chế độ:
--     Scatter-Gather : WHERE chỉ có location (không phải dist. col)
--       → 4 sub-tasks đến 4 workers, tổng hợp về coordinator
--     Single-Shard  : WHERE category_group = X
--       → 1 sub-task đến đúng 1 worker, I/O giảm ~75%
--
--   Ghi chú Citus: dùng LANGUAGE plpgsql thay vì SQL vì SQL functions
--   với tham số trên distributed tables không được hỗ trợ.
-- =============================================================================


-- =============================================================================
-- FUNCTION: route_jobs_by_group
-- -----------------------------------------------------------------------------
-- Demo shard pruning rõ nhất: filter theo category_group + location.
-- Citus sử dụng index idx_core_group_loc (category_group, location) trên
-- worker, kết hợp shard pruning → chỉ đọc 1 shard trên 1 worker.
-- =============================================================================
CREATE OR REPLACE FUNCTION route_jobs_by_group(
    p_category_group INTEGER,
    p_location       TEXT    DEFAULT NULL,
    p_limit          INTEGER DEFAULT 20
)
RETURNS TABLE (
    job_id              INTEGER,
    job_title           TEXT,
    category            TEXT,
    location            TEXT,
    salary_min          INTEGER,
    salary_max          INTEGER,
    salary_avg          NUMERIC,
    experience_required TEXT,
    contract_type       TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.job_id, c.job_title, c.category, c.location,
            c.salary_min, c.salary_max, c.salary_avg,
            c.experience_required, c.contract_type
        FROM core c
        WHERE c.category_group = p_category_group
          AND (p_location IS NULL OR c.location = p_location)
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT p_limit;
END;
$$;


-- =============================================================================
-- FUNCTION: search_jobs_keyword
-- -----------------------------------------------------------------------------
-- Tìm kiếm theo từ khoá trong job_title.
--   Có p_category_group → shard pruning: 1 worker xử lý
--   Không có            → scatter-gather: 4 workers song song
-- Minh hoạ trade-off: biết thêm distribution key → cắt giảm I/O đáng kể.
-- =============================================================================
CREATE OR REPLACE FUNCTION search_jobs_keyword(
    p_keyword        TEXT,
    p_category_group INTEGER DEFAULT NULL,
    p_location       TEXT    DEFAULT NULL
)
RETURNS TABLE (
    job_id     INTEGER,
    job_title  TEXT,
    category   TEXT,
    location   TEXT,
    salary_avg NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT c.job_id, c.job_title, c.category, c.location, c.salary_avg
        FROM core c
        WHERE c.job_title ILIKE '%' || p_keyword || '%'
          AND (p_category_group IS NULL OR c.category_group = p_category_group)
          AND (p_location IS NULL OR c.location = p_location)
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT 50;
END;
$$;


-- =============================================================================
-- FUNCTION: route_salary_range
-- -----------------------------------------------------------------------------
-- Tìm việc theo khoảng lương. Kết hợp shard pruning với range scan trên
-- index idx_core_salary (salary_avg DESC). Worker đọc các row thỏa điều
-- kiện salary mà không scan toàn shard.
-- =============================================================================
CREATE OR REPLACE FUNCTION route_salary_range(
    p_min_salary     NUMERIC,
    p_max_salary     NUMERIC  DEFAULT NULL,
    p_category_group INTEGER  DEFAULT NULL,
    p_location       TEXT     DEFAULT NULL
)
RETURNS TABLE (
    job_id     INTEGER,
    job_title  TEXT,
    category   TEXT,
    location   TEXT,
    salary_avg NUMERIC,
    salary_min INTEGER,
    salary_max INTEGER
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT c.job_id, c.job_title, c.category, c.location,
               c.salary_avg, c.salary_min, c.salary_max
        FROM core c
        WHERE c.salary_avg >= p_min_salary
          AND (p_max_salary IS NULL OR c.salary_avg <= p_max_salary)
          AND (p_category_group IS NULL OR c.category_group = p_category_group)
          AND (p_location IS NULL OR c.location = p_location)
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT 100;
END;
$$;


-- =============================================================================
-- DEMO 1: EXPLAIN — Scatter-Gather (không pruning, Task Count: 8)
-- -----------------------------------------------------------------------------
-- Filter chỉ theo location → không phải distribution column
-- Citus buộc hỏi tất cả 4 workers (8 shards, 4 có data)
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT job_id, job_title, salary_avg
  FROM core
 WHERE location = 'Hà Nội'
 LIMIT 10;


-- =============================================================================
-- DEMO 2: EXPLAIN — Single-Shard (shard pruning, Task Count: 1)
-- -----------------------------------------------------------------------------
-- Thêm category_group = 2 → hash(2) → shard trên worker2
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT job_id, job_title, salary_avg
  FROM core
 WHERE category_group = 2
   AND location = 'Hà Nội'
 LIMIT 10;


-- =============================================================================
-- DEMO 3: EXPLAIN ANALYZE — đo thực tế thời gian 2 chế độ
-- -----------------------------------------------------------------------------
-- So sánh Execution Time: scatter-gather vs single-shard (shard pruning).
-- Cột "Bitmap Index Scan" chứng minh index idx_core_group_loc được dùng.
-- =============================================================================
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT COUNT(*), ROUND(AVG(salary_avg), 0) AS avg_salary
  FROM core
 WHERE location = 'TPHCM';           -- không prune: 8 tasks

EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT COUNT(*), ROUND(AVG(salary_avg), 0) AS avg_salary
  FROM core
 WHERE category_group = 1
   AND location = 'TPHCM';           -- prune: 1 task (worker1 commerce)


-- =============================================================================
-- CHẠY THỬ
-- =============================================================================
SELECT * FROM route_jobs_by_group(2, 'Hà Nội', 5);
SELECT * FROM search_jobs_keyword('marketing');
SELECT * FROM search_jobs_keyword('marketing', 3);
SELECT * FROM route_salary_range(20, 50) LIMIT 10;    -- 20-50 triệu VND
