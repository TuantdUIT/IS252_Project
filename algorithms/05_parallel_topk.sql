-- =============================================================================
-- FILE: 05_parallel_topk.sql
-- Thuật toán: Parallel Top-K
--
-- Nguyên lý:
--   Bài toán Top-K: "Tìm K phần tử lớn nhất trong N phần tử phân tán".
--   Naive approach: fetch toàn bộ N rows về coordinator, sort, lấy K → O(N).
--
--   Parallel Top-K — thuật toán 2 pha:
--     Phase 1 — Local Top-K (song song tại 4 workers):
--       Mỗi worker sort shard cục bộ, lấy top-K_local
--       Chỉ gửi K_local dòng về coordinator
--     Phase 2 — Global Top-K (tại coordinator):
--       Coordinator nhận 4 × K dòng ứng viên, sort và lấy K cuối cùng
--
--   Citus tự thực hiện khi query có ORDER BY + LIMIT:
--     Bước 1: Push "ORDER BY col LIMIT K" xuống từng worker → Local Top-K
--     Bước 2: Coordinator sort 4K rows và LIMIT K → Global Top-K
--
--   Network savings: O(4K) thay vì O(N)
--   Với N=24,281, K=10: gửi 40 rows thay vì 24,281 → giảm ~600 lần.
--
--   Index usage: idx_core_salary (salary_avg DESC) → worker dùng index scan
--   thay vì sort toàn bộ shard → Local Top-K thực sự là O(K) I/O tại worker.
--
--   Ghi chú Citus: dùng LANGUAGE plpgsql thay vì SQL.
-- =============================================================================


-- =============================================================================
-- FUNCTION: topk_salary_global
-- -----------------------------------------------------------------------------
-- Top-K công việc lương cao nhất toàn hệ thống.
-- Citus push ORDER BY salary_avg DESC LIMIT p_k xuống 4 workers (phase 1),
-- coordinator sort 4K rows và LIMIT K (phase 2).
--
-- Tham số:
--   p_k              : số kết quả cần lấy
--   p_location       : lọc theo thành phố; NULL = toàn quốc
--   p_category_group : lọc theo nhóm; NULL = tất cả (nếu có → shard pruning)
-- =============================================================================
CREATE OR REPLACE FUNCTION topk_salary_global(
    p_k              INTEGER DEFAULT 10,
    p_location       TEXT    DEFAULT NULL,
    p_category_group INTEGER DEFAULT NULL
)
RETURNS TABLE (
    rank_num            BIGINT,
    job_id              INTEGER,
    job_title           TEXT,
    category            TEXT,
    location            TEXT,
    salary_avg          NUMERIC,
    salary_min          INTEGER,
    salary_max          INTEGER,
    experience_required TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            ROW_NUMBER() OVER (ORDER BY c.salary_avg DESC NULLS LAST) AS rank_num,
            c.job_id,
            c.job_title,
            c.category,
            c.location,
            c.salary_avg,
            c.salary_min,
            c.salary_max,
            c.experience_required
        FROM core c
        WHERE c.salary_avg IS NOT NULL
          AND (p_location IS NULL OR c.location = p_location)
          AND (p_category_group IS NULL OR c.category_group = p_category_group)
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT p_k;
END;
$$;


-- =============================================================================
-- FUNCTION: topk_per_group
-- -----------------------------------------------------------------------------
-- Top-K lương cao nhất trong MỖI nhóm nghề (partitioned Top-K).
-- PARTITION BY category_group = distribution key → window function chạy
-- cục bộ tại worker, không cần cross-node shuffle.
-- =============================================================================
CREATE OR REPLACE FUNCTION topk_per_group(
    p_k        INTEGER DEFAULT 5,
    p_location TEXT    DEFAULT NULL
)
RETURNS TABLE (
    category_group  INTEGER,
    group_name      TEXT,
    rank_in_group   BIGINT,
    job_id          INTEGER,
    job_title       TEXT,
    location        TEXT,
    salary_avg      NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        WITH ranked AS (
            -- Phase 1: worker rank jobs trong từng category_group (partition key = dist. key)
            SELECT
                c.category_group,
                cm.group_name,
                c.job_id,
                c.job_title,
                c.location,
                c.salary_avg,
                ROW_NUMBER() OVER (
                    PARTITION BY c.category_group
                    ORDER BY c.salary_avg DESC NULLS LAST
                ) AS rn
            FROM core c
            JOIN category_mapping cm ON cm.category = c.category
            WHERE c.salary_avg IS NOT NULL
              AND (p_location IS NULL OR c.location = p_location)
        )
        -- Phase 2: coordinator filter top-K từ mỗi nhóm
        SELECT r.category_group, r.group_name, r.rn AS rank_in_group,
               r.job_id, r.job_title, r.location, r.salary_avg
        FROM ranked r
        WHERE r.rn <= p_k
        ORDER BY r.category_group, r.rn;
END;
$$;


-- =============================================================================
-- FUNCTION: topk_salary_bracket
-- -----------------------------------------------------------------------------
-- Histogram lương: phân bố jobs theo khoảng salary (bucket analysis).
-- MAP:    worker đếm jobs trong từng bucket salary cục bộ
-- REDUCE: coordinator tổng hợp COUNT, tính tỷ lệ %
-- =============================================================================
CREATE OR REPLACE FUNCTION topk_salary_bracket(
    p_bucket_size NUMERIC DEFAULT 10   -- đơn vị: triệu VND
)
RETURNS TABLE (
    bracket_label TEXT,
    salary_from   NUMERIC,
    salary_to     NUMERIC,
    job_count     BIGINT,
    pct_total     NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        WITH buckets AS (
            -- MAP: đếm jobs per salary bucket trên từng worker
            SELECT
                FLOOR(c.salary_avg / p_bucket_size) * p_bucket_size AS bucket_floor,
                COUNT(*) AS cnt
            FROM core c
            WHERE c.salary_avg IS NOT NULL
            GROUP BY bucket_floor
        ),
        totals AS (
            -- REDUCE: coordinator tổng COUNT để tính %
            SELECT SUM(cnt) AS grand_total FROM buckets
        )
        SELECT
            TO_CHAR(b.bucket_floor, 'FM999') || '-'
                || TO_CHAR(b.bucket_floor + p_bucket_size, 'FM999')
                || 'M'                                          AS bracket_label,
            b.bucket_floor                                     AS salary_from,
            b.bucket_floor + p_bucket_size                     AS salary_to,
            b.cnt                                              AS job_count,
            ROUND(100.0 * b.cnt / NULLIF(t.grand_total, 0), 1) AS pct_total
        FROM buckets b, totals t
        ORDER BY b.bucket_floor;
END;
$$;


-- =============================================================================
-- FUNCTION: topk_recent_high_salary
-- -----------------------------------------------------------------------------
-- Top-K lương cao nhất per mức kinh nghiệm — phân tích seniority vs salary.
-- Citus push PARTITION BY + ORDER BY + LIMIT xuống workers, coordinator merge.
-- =============================================================================
CREATE OR REPLACE FUNCTION topk_recent_high_salary(p_k INTEGER DEFAULT 5)
RETURNS TABLE (
    experience_required TEXT,
    rank_num            BIGINT,
    job_id              INTEGER,
    job_title           TEXT,
    category            TEXT,
    location            TEXT,
    salary_avg          NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        WITH exp_ranked AS (
            -- Phase 1: rank jobs per experience level tại worker
            SELECT
                c.experience_required,
                c.job_id,
                c.job_title,
                c.category,
                c.location,
                c.salary_avg,
                ROW_NUMBER() OVER (
                    PARTITION BY c.experience_required
                    ORDER BY c.salary_avg DESC NULLS LAST
                ) AS rn
            FROM core c
            WHERE c.salary_avg IS NOT NULL
              AND c.experience_required IS NOT NULL
        )
        -- Phase 2: coordinator filter top-K per experience
        SELECT e.experience_required, e.rn AS rank_num,
               e.job_id, e.job_title, e.category, e.location, e.salary_avg
        FROM exp_ranked e
        WHERE e.rn <= p_k
        ORDER BY e.experience_required, e.rn;
END;
$$;


-- =============================================================================
-- DEMO 1: EXPLAIN — LIMIT pushdown (kiểm chứng Phase 1 tại worker)
-- -----------------------------------------------------------------------------
-- Tìm "Limit" bên trong task của worker → Phase 1 Local Top-K
-- Cột "Tuple data received from nodes" cho thấy chỉ 40 rows gửi về (4×10)
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT job_id, job_title, salary_avg
  FROM core
 WHERE salary_avg IS NOT NULL
 ORDER BY salary_avg DESC
 LIMIT 10;


-- =============================================================================
-- DEMO 2: EXPLAIN ANALYZE — đo thực tế LIMIT pushdown vs full sort
-- -----------------------------------------------------------------------------
-- Query 1 với LIMIT 10   → workers gửi 40 rows → coordinator sort 40 rows
-- Query 2 không có LIMIT → workers gửi 24,281 rows → coordinator sort tất cả
-- So sánh Execution Time để thấy ưu thế LIMIT pushdown.
-- =============================================================================
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT job_id, salary_avg FROM core
 WHERE salary_avg IS NOT NULL
 ORDER BY salary_avg DESC
 LIMIT 10;

EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT job_id, salary_avg FROM core
 WHERE salary_avg IS NOT NULL
 ORDER BY salary_avg DESC;


-- =============================================================================
-- CHẠY THỬ
-- =============================================================================
SELECT * FROM topk_salary_global(10);
SELECT * FROM topk_salary_global(5, 'Hà Nội');
SELECT * FROM topk_salary_global(5, 'TPHCM', 2);
SELECT * FROM topk_per_group(3);
SELECT * FROM topk_salary_bracket(5000000);
SELECT * FROM topk_recent_high_salary(3);
