-- =============================================================================
-- FILE: 03_mapreduce_agg.sql
-- Thuật toán: MapReduce 2-Phase Aggregation
--
-- Nguyên lý:
--   Citus tự động chia aggregation query thành 2 phase:
--
--   Phase 1 — MAP (song song trên 4 workers):
--     Mỗi worker tính PARTIAL aggregate trên shard cục bộ:
--       COUNT(*)        → partial_count
--       SUM(salary_avg) → partial_sum  (dùng tính AVG toàn cục)
--       MIN(salary_min) → partial_min
--       MAX(salary_max) → partial_max
--     Chỉ gửi vài dòng tổng hợp về coordinator (không gửi raw rows).
--
--   Phase 2 — REDUCE (tại coordinator):
--     Coordinator nhận 4 partial results, tổng hợp:
--       COUNT = SUM(partial_count)
--       AVG   = SUM(partial_sum) / SUM(partial_count)
--       MIN   = MIN(partial_min)
--       MAX   = MAX(partial_max)
--
--   Network savings: O(n_workers × n_groups) thay vì O(n_rows).
--   Dataset 24,281 rows, GROUP BY 8 groups → coordinator nhận 32 rows
--   thay vì 24,281 rows. Tiết kiệm ~760 lần.
--
--   Ghi chú Citus: dùng LANGUAGE plpgsql thay vì SQL.
-- =============================================================================


-- =============================================================================
-- FUNCTION: mapreduce_salary_stats
-- -----------------------------------------------------------------------------
-- Thống kê lương theo nhóm nghề và địa điểm.
-- MAP:    worker tính partial COUNT, SUM, MIN, MAX per (category_group, location)
-- REDUCE: coordinator tổng hợp partial results
--
-- Tham số: p_location — lọc theo thành phố; NULL = tất cả
-- =============================================================================
CREATE OR REPLACE FUNCTION mapreduce_salary_stats(
    p_location TEXT DEFAULT NULL
)
RETURNS TABLE (
    category_group  INTEGER,
    group_name      TEXT,
    location        TEXT,
    job_count       BIGINT,
    avg_salary      NUMERIC,
    min_salary      INTEGER,
    max_salary      INTEGER
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.category_group,
            cm.group_name,
            c.location,
            COUNT(*)                    AS job_count,
            ROUND(AVG(c.salary_avg), 0) AS avg_salary,
            MIN(c.salary_min)           AS min_salary,
            MAX(c.salary_max)           AS max_salary
        FROM core c
        -- category_mapping là reference table → join cục bộ tại worker, no shuffle
        JOIN category_mapping cm ON cm.category = c.category
        WHERE c.salary_avg IS NOT NULL
          AND (p_location IS NULL OR c.location = p_location)
        GROUP BY c.category_group, cm.group_name, c.location
        ORDER BY c.category_group, c.location;
END;
$$;


-- =============================================================================
-- FUNCTION: mapreduce_category_report
-- -----------------------------------------------------------------------------
-- Báo cáo tổng hợp thị trường lao động theo 16 ngành chi tiết.
-- CTE 2 bước để minh hoạ rõ MAP → REDUCE:
--   CTE base : MAP — partial aggregate trên từng worker (category, location)
--   pivot    : REDUCE — coordinator tổng hợp, pivot location thành cột %
-- =============================================================================
CREATE OR REPLACE FUNCTION mapreduce_category_report()
RETURNS TABLE (
    category     TEXT,
    group_name   TEXT,
    total_jobs   BIGINT,
    avg_salary   NUMERIC,
    pct_hanoi    NUMERIC,
    pct_hcm      NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        WITH base AS (
            -- MAP phase: partial aggregate per (category, location) tại worker
            SELECT
                c.category,
                c.location,
                COUNT(*)        AS cnt,
                AVG(c.salary_avg) AS avg_sal
            FROM core c
            GROUP BY c.category, c.location
        ),
        pivot AS (
            -- REDUCE phase: coordinator pivot location → cột, weighted average
            SELECT
                b.category,
                SUM(b.cnt)::BIGINT                                      AS total_jobs,
                ROUND(SUM(b.cnt * b.avg_sal) / NULLIF(SUM(b.cnt), 0), 0) AS avg_salary,
                ROUND(100.0 * SUM(b.cnt) FILTER (WHERE b.location = 'Hà Nội')
                      / NULLIF(SUM(b.cnt), 0), 1)                       AS pct_hanoi,
                ROUND(100.0 * SUM(b.cnt) FILTER (WHERE b.location = 'TPHCM')
                      / NULLIF(SUM(b.cnt), 0), 1)                       AS pct_hcm
            FROM base b
            GROUP BY b.category
        )
        SELECT p.category, cm.group_name,
               p.total_jobs, p.avg_salary,
               p.pct_hanoi, p.pct_hcm
        FROM pivot p
        JOIN category_mapping cm ON cm.category = p.category
        ORDER BY p.total_jobs DESC;
END;
$$;


-- =============================================================================
-- FUNCTION: mapreduce_experience_salary
-- -----------------------------------------------------------------------------
-- Phân tích mức lương theo yêu cầu kinh nghiệm.
-- MAP:    worker tính partial per experience_required
-- REDUCE: coordinator tổng hợp toàn hệ thống
-- =============================================================================
CREATE OR REPLACE FUNCTION mapreduce_experience_salary()
RETURNS TABLE (
    experience_required TEXT,
    job_count           BIGINT,
    avg_salary          NUMERIC,
    min_salary          INTEGER,
    max_salary          INTEGER
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.experience_required,
            COUNT(*)                    AS job_count,
            ROUND(AVG(c.salary_avg), 0) AS avg_salary,
            MIN(c.salary_min)           AS min_salary,
            MAX(c.salary_max)           AS max_salary
        FROM core c
        WHERE c.experience_required IS NOT NULL
          AND c.salary_avg IS NOT NULL
        GROUP BY c.experience_required
        ORDER BY avg_salary DESC NULLS LAST;
END;
$$;


-- =============================================================================
-- FUNCTION: mapreduce_contract_distribution
-- -----------------------------------------------------------------------------
-- Phân bố loại hợp đồng theo nhóm nghề — GROUP BY 2 chiều.
-- Citus sinh 4 MAP tasks song song, coordinator REDUCE COUNT.
-- Join category_mapping trong MAP phase (reference table → cục bộ tại worker).
-- =============================================================================
CREATE OR REPLACE FUNCTION mapreduce_contract_distribution()
RETURNS TABLE (
    category_group INTEGER,
    group_name     TEXT,
    contract_type  TEXT,
    job_count      BIGINT,
    pct_in_group   NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        WITH counts AS (
            -- MAP: partial count per (category_group, group_name, contract_type)
            -- Join category_mapping tại worker (reference table, không tốn network)
            SELECT
                c.category_group,
                cm.group_name,
                c.contract_type,
                COUNT(*) AS cnt
            FROM core c
            JOIN category_mapping cm ON cm.category = c.category
            WHERE c.contract_type IS NOT NULL
            GROUP BY c.category_group, cm.group_name, c.contract_type
        ),
        group_totals AS (
            -- REDUCE step 1: tổng số việc mỗi nhóm để tính tỷ lệ %
            -- Dùng alias x để tránh ambiguous với output variable cùng tên trong plpgsql
            SELECT x.category_group, SUM(x.cnt) AS total
            FROM counts x
            GROUP BY x.category_group
        )
        SELECT
            ct.category_group,
            ct.group_name,
            ct.contract_type,
            ct.cnt                                                    AS job_count,
            ROUND(100.0 * ct.cnt / NULLIF(g.total, 0), 1)            AS pct_in_group
        FROM counts ct
        JOIN group_totals g USING (category_group)
        ORDER BY ct.category_group, ct.cnt DESC;
END;
$$;


-- =============================================================================
-- DEMO: EXPLAIN — kiểm chứng 2-phase aggregation plan
-- -----------------------------------------------------------------------------
-- Tìm "HashAggregate" tại worker node  ← MAP phase
-- Tìm "Aggregate"/"Sort" tại coordinator ← REDUCE phase
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT category_group, location, COUNT(*), ROUND(AVG(salary_avg), 0)
  FROM core
 GROUP BY category_group, location
 ORDER BY category_group;


-- =============================================================================
-- CHẠY THỬ
-- =============================================================================
SELECT * FROM mapreduce_salary_stats();
SELECT * FROM mapreduce_salary_stats('Hà Nội');
SELECT * FROM mapreduce_category_report();
SELECT * FROM mapreduce_experience_salary();
SELECT * FROM mapreduce_contract_distribution();
