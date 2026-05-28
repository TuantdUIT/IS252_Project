-- =============================================================================
-- FILE: 04_semi_join.sql
-- Thuật toán: Distributed Semi-Join
--
-- Nguyên lý:
--   Semi-join trả về dòng từ bảng trái khi tồn tại ít nhất 1 dòng khớp
--   trong bảng phải — không nhân bản dòng trái, không đọc toàn bộ bảng phải.
--
--   SQL biểu diễn semi-join:
--     Dạng EXISTS : WHERE EXISTS (SELECT 1 FROM R WHERE R.key = L.key AND cond)
--     Dạng IN     : WHERE L.key IN (SELECT key FROM R WHERE cond)
--
--   Trong Citus với co-located tables:
--     1. Citus gửi sub-query xuống từng worker
--     2. Worker thực hiện Hash/Nested Loop Semi Join cục bộ (không cross-node)
--     3. Early stop: dừng scan bảng phải ngay khi tìm thấy 1 match
--     4. Coordinator thu partial results, UNION ALL lại
--
--   Anti semi-join (NOT EXISTS): tìm dòng bảng trái KHÔNG có match
--   → phát hiện dữ liệu thiếu hoặc orphan records.
--
--   Ghi chú Citus: dùng LANGUAGE plpgsql thay vì SQL.
-- =============================================================================


-- =============================================================================
-- FUNCTION: semi_join_jobs_with_skill
-- -----------------------------------------------------------------------------
-- Semi-join: tìm jobs trong core có kỹ năng thoả điều kiện trong skills.
-- Co-located: core và skills cùng category_group → join cục bộ, không shuffle.
--
-- Tham số:
--   p_skill          : từ khoá trong technical_skills
--   p_category_group : tuỳ chọn — shard pruning nếu có
--   p_location       : tuỳ chọn — filter thêm trên worker
-- =============================================================================
CREATE OR REPLACE FUNCTION semi_join_jobs_with_skill(
    p_skill          TEXT,
    p_category_group INTEGER DEFAULT NULL,
    p_location       TEXT    DEFAULT NULL
)
RETURNS TABLE (
    job_id              INTEGER,
    job_title           TEXT,
    category            TEXT,
    location            TEXT,
    salary_avg          NUMERIC,
    experience_required TEXT,
    technical_skills    TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.job_id,
            c.job_title,
            c.category,
            c.location,
            c.salary_avg,
            c.experience_required,
            s.technical_skills
        FROM core c
        -- Semi-join qua JOIN: worker dừng scan khi tìm thấy match đầu tiên
        -- nhờ LIMIT + early termination trong nested loop plan
        JOIN skills s ON s.category_group = c.category_group
                     AND s.job_id = c.job_id
        WHERE s.technical_skills ILIKE '%' || p_skill || '%'
          AND (p_category_group IS NULL OR c.category_group = p_category_group)
          AND (p_location IS NULL OR c.location = p_location)
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT 50;
END;
$$;


-- =============================================================================
-- FUNCTION: semi_join_jobs_qualified
-- -----------------------------------------------------------------------------
-- Semi-join tổ hợp: tìm jobs đồng thời thoả 2 điều kiện EXISTS trên 2 bảng:
--   1. Tồn tại trong detail với description IS NOT NULL (có mô tả đầy đủ)
--   2. Tồn tại trong skills với soft_skills IS NOT NULL
-- Citus đẩy cả 2 EXISTS sub-query xuống worker, chạy cục bộ co-located.
-- =============================================================================
CREATE OR REPLACE FUNCTION semi_join_jobs_qualified(
    p_min_salary NUMERIC,
    p_experience TEXT DEFAULT NULL
)
RETURNS TABLE (
    job_id              INTEGER,
    job_title           TEXT,
    category            TEXT,
    location            TEXT,
    salary_avg          NUMERIC,
    experience_required TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.job_id,
            c.job_title,
            c.category,
            c.location,
            c.salary_avg,
            c.experience_required
        FROM core c
        WHERE c.salary_avg >= p_min_salary
          AND (p_experience IS NULL OR c.experience_required = p_experience)
          -- Semi-join 1: job có mô tả đầy đủ trong detail
          AND EXISTS (
              SELECT 1 FROM detail d
               WHERE d.category_group = c.category_group
                 AND d.job_id = c.job_id
                 AND d.description IS NOT NULL
          )
          -- Semi-join 2: job có kỹ năng mềm trong skills
          AND EXISTS (
              SELECT 1 FROM skills s
               WHERE s.category_group = c.category_group
                 AND s.job_id = c.job_id
                 AND s.soft_skills IS NOT NULL
          )
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT 100;
END;
$$;


-- =============================================================================
-- FUNCTION: anti_semi_join_missing_data
-- -----------------------------------------------------------------------------
-- Anti semi-join (NOT EXISTS): phát hiện bản ghi thiếu dữ liệu.
-- Tìm jobs trong core nhưng KHÔNG có dòng tương ứng trong detail hoặc skills.
-- Hữu ích cho data quality check — kỳ vọng trả về 0 dòng nếu ETL đúng.
-- =============================================================================
CREATE OR REPLACE FUNCTION anti_semi_join_missing_data()
RETURNS TABLE (
    job_id          INTEGER,
    category_group  INTEGER,
    job_title       TEXT,
    missing_in      TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        -- Anti semi-join: core có nhưng detail không có
        SELECT c.job_id, c.category_group, c.job_title, 'detail'::TEXT AS missing_in
        FROM core c
        WHERE NOT EXISTS (
            SELECT 1 FROM detail d
             WHERE d.category_group = c.category_group
               AND d.job_id = c.job_id
        )

        UNION ALL

        -- Anti semi-join: core có nhưng skills không có
        SELECT c.job_id, c.category_group, c.job_title, 'skills'::TEXT AS missing_in
        FROM core c
        WHERE NOT EXISTS (
            SELECT 1 FROM skills s
             WHERE s.category_group = c.category_group
               AND s.job_id = c.job_id
        )
        ORDER BY category_group, job_id;
END;
$$;


-- =============================================================================
-- FUNCTION: semi_join_soft_skill_filter
-- -----------------------------------------------------------------------------
-- Tìm jobs có kỹ năng mềm theo từ khoá, kết hợp filter lương.
-- Minh hoạ co-located semi-join với điều kiện số học (salary range).
-- =============================================================================
CREATE OR REPLACE FUNCTION semi_join_soft_skill_filter(
    p_soft_skill TEXT,
    p_min_salary NUMERIC DEFAULT 0
)
RETURNS TABLE (
    job_id              INTEGER,
    job_title           TEXT,
    category            TEXT,
    location            TEXT,
    salary_avg          NUMERIC,
    soft_skills         TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.job_id,
            c.job_title,
            c.category,
            c.location,
            c.salary_avg,
            s.soft_skills
        FROM core c
        JOIN skills s ON s.category_group = c.category_group
                     AND s.job_id = c.job_id
        WHERE s.soft_skills ILIKE '%' || p_soft_skill || '%'
          AND c.salary_avg >= p_min_salary
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT 50;
END;
$$;


-- =============================================================================
-- DEMO 1: EXPLAIN — Semi-join với shard pruning (category_group = 2)
-- -----------------------------------------------------------------------------
-- Kỳ vọng: "Nested Loop" hoặc "Hash Semi Join" tại worker, "Task Count: 1"
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT c.job_id, c.job_title
  FROM core c
 WHERE c.category_group = 2
   AND EXISTS (
       SELECT 1 FROM skills s
        WHERE s.category_group = c.category_group
          AND s.job_id = c.job_id
          AND s.technical_skills ILIKE '%python%'
   );


-- =============================================================================
-- DEMO 2: EXPLAIN — Anti semi-join toàn hệ thống (Task Count: 8)
-- -----------------------------------------------------------------------------
-- Kỳ vọng: "Merge Anti Join" tại worker (dùng index scan cả 2 bảng)
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT c.job_id, c.category_group
  FROM core c
 WHERE NOT EXISTS (
     SELECT 1 FROM detail d
      WHERE d.category_group = c.category_group
        AND d.job_id = c.job_id
 )
 LIMIT 10;


-- =============================================================================
-- CHẠY THỬ
-- =============================================================================
SELECT * FROM semi_join_jobs_with_skill('Python', 2, 'Hà Nội');
SELECT * FROM semi_join_jobs_qualified(30) LIMIT 10;    -- >= 30 triệu VND
SELECT COUNT(*), missing_in FROM anti_semi_join_missing_data() GROUP BY missing_in;
SELECT * FROM semi_join_soft_skill_filter('giao tiếp', 10) LIMIT 10;    -- >= 10 triệu VND
