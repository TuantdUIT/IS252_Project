-- =============================================================================
-- FILE: 01_hash_join.sql
-- Thuật toán: Parallel Hash Join (Co-located)
--
-- Nguyên lý:
--   core, detail, skills cùng distribution key (category_group) và cùng
--   co-location group (thiết lập trong 02_distribute.sql). Mỗi worker giữ
--   shard của cả 3 bảng cho cùng 1 group.
--
--   Khi JOIN 3 bảng, Citus gửi sub-query xuống từng worker; worker tự thực
--   hiện Hash Join cục bộ:
--     Build phase : nạp bảng nhỏ hơn (detail hoặc skills) vào hash table
--                   trong bộ nhớ, key = (category_group, job_id)
--     Probe phase : quét bảng lớn (core), tra hash table tìm match
--   Coordinator chỉ nhận kết quả đã join — không có network shuffle
--   giữa các worker (zero cross-node data transfer).
--
--   Ghi chú Citus: LANGUAGE SQL function có tham số trên distributed table
--   không được hỗ trợ (lỗi "parameterized queries not supported").
--   Tất cả function phải dùng LANGUAGE plpgsql với RETURN QUERY.
-- =============================================================================


-- =============================================================================
-- VIEW: v_jobs_full
-- -----------------------------------------------------------------------------
-- Kết hợp đầy đủ thông tin từ 3 bảng phân mảnh dọc (core + detail + skills).
-- Citus sinh 4 sub-task song song — mỗi worker Hash Join 3 shard cục bộ,
-- coordinator UNION ALL kết quả. Dùng làm base cho báo cáo cần join đầy đủ.
-- =============================================================================
CREATE OR REPLACE VIEW v_jobs_full AS
    SELECT
        c.job_id,
        c.category_group,
        c.category,
        c.location,
        c.job_title,
        c.salary_min,
        c.salary_max,
        c.salary_avg,
        c.experience_required,
        c.contract_type,
        c.working_hours,
        d.description,
        d.requirements_text,
        d.benefits,
        s.technical_skills,
        s.soft_skills,
        s.qualifications,
        s.languages_required
    FROM core    c
    JOIN detail  d USING (category_group, job_id)
    JOIN skills  s USING (category_group, job_id);


-- =============================================================================
-- FUNCTION: hash_join_search
-- -----------------------------------------------------------------------------
-- Tìm kiếm việc làm với thông tin đầy đủ bằng co-located Hash Join.
-- Citus routing:
--   p_category_group NOT NULL → shard pruning: 1 sub-task (1 worker)
--   p_category_group NULL     → broadcast: 4 sub-tasks song song
--
-- Dùng plpgsql thay vì SQL vì Citus không hỗ trợ parameterized SQL functions
-- trên distributed tables. plpgsql evaluate tham số trước khi gửi query.
-- =============================================================================
CREATE OR REPLACE FUNCTION hash_join_search(
    p_category_group INTEGER DEFAULT NULL,
    p_location       TEXT    DEFAULT NULL,
    p_limit          INTEGER DEFAULT 20
)
RETURNS TABLE (
    job_id              INTEGER,
    job_title           TEXT,
    category            TEXT,
    location            TEXT,
    salary_avg          NUMERIC,
    experience_required TEXT,
    contract_type       TEXT,
    description         TEXT,
    technical_skills    TEXT,
    soft_skills         TEXT,
    qualifications      TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    -- USING (category_group, job_id) là điều kiện bắt buộc để Citus
    -- nhận ra co-location và không sinh thêm Repartition node.
    RETURN QUERY
        SELECT
            c.job_id,
            c.job_title,
            c.category,
            c.location,
            c.salary_avg,
            c.experience_required,
            c.contract_type,
            d.description,
            s.technical_skills,
            s.soft_skills,
            s.qualifications
        FROM core    c
        JOIN detail  d USING (category_group, job_id)
        JOIN skills  s USING (category_group, job_id)
        WHERE (p_category_group IS NULL OR c.category_group = p_category_group)
          AND (p_location IS NULL OR c.location = p_location)
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT p_limit;
END;
$$;


-- =============================================================================
-- FUNCTION: hash_join_job_detail
-- -----------------------------------------------------------------------------
-- Tra cứu thông tin chi tiết 1 công việc theo primary key.
-- category_group bắt buộc để Citus prune về đúng 1 worker chứa shard.
-- Hash join xảy ra tức thì vì hash table chỉ có 1 row (PK lookup).
-- =============================================================================
CREATE OR REPLACE FUNCTION hash_join_job_detail(
    p_job_id         INTEGER,
    p_category_group INTEGER
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
    contract_type       TEXT,
    working_hours       TEXT,
    description         TEXT,
    requirements_text   TEXT,
    benefits            TEXT,
    technical_skills    TEXT,
    soft_skills         TEXT,
    qualifications      TEXT,
    languages_required  TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.job_id,       c.job_title,           c.category,
            c.location,     c.salary_min,          c.salary_max,
            c.salary_avg,   c.experience_required, c.contract_type,
            c.working_hours,
            d.description,  d.requirements_text,   d.benefits,
            s.technical_skills, s.soft_skills, s.qualifications, s.languages_required
        FROM core    c
        JOIN detail  d USING (category_group, job_id)
        JOIN skills  s USING (category_group, job_id)
        WHERE c.category_group = p_category_group
          AND c.job_id = p_job_id;
END;
$$;


-- =============================================================================
-- DEMO 1: EXPLAIN — Hash Join song song (Task Count: 8, không pruning)
-- -----------------------------------------------------------------------------
-- Kỳ vọng:
--   "Custom Scan (Citus Adaptive)" ở top node
--   "Task Count: 8"  — 8 shard trên 4 workers (chỉ 4 shard có data)
--   Mỗi task: "Merge Join" hoặc "Hash Join" trên (category_group, job_id)
--   Không có "Redistribute" hay "Repartition" node
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT c.job_id, c.job_title, c.salary_avg,
       d.description, s.technical_skills
  FROM core    c
  JOIN detail  d USING (category_group, job_id)
  JOIN skills  s USING (category_group, job_id)
 WHERE c.location = 'Hà Nội'
 LIMIT 10;


-- =============================================================================
-- DEMO 2: EXPLAIN — Hash Join + shard pruning (Task Count: 1)
-- -----------------------------------------------------------------------------
-- Thêm category_group = 2 → Citus tính hash(2) → chỉ shard trên worker2
-- =============================================================================
EXPLAIN (COSTS OFF)
SELECT c.job_id, c.job_title, c.salary_avg,
       d.description, s.technical_skills
  FROM core    c
  JOIN detail  d USING (category_group, job_id)
  JOIN skills  s USING (category_group, job_id)
 WHERE c.category_group = 2
   AND c.location = 'Hà Nội'
 LIMIT 10;


-- =============================================================================
-- CHẠY THỬ: 5 công việc nhóm tech tại Hà Nội
-- =============================================================================
SELECT * FROM hash_join_search(2, 'Hà Nội', 5);


-- =============================================================================
-- VERIFY: kiểm tra số dòng join khớp với core
-- -----------------------------------------------------------------------------
-- JOIN category_mapping trên cm.category = c.category (1-to-1 match).
-- Không dùng cm.category_group = c.category_group vì sẽ nhân bản dòng
-- theo số category trong mỗi group (3-5 dòng / group).
-- Kỳ vọng: joined_rows = số dòng core cho từng (category_group, location).
-- =============================================================================
SELECT
    c.category_group,
    cm.group_name,
    c.location,
    COUNT(*) AS joined_rows
FROM core    c
JOIN detail  d USING (category_group, job_id)
JOIN skills  s USING (category_group, job_id)
JOIN category_mapping cm ON cm.category = c.category
GROUP BY c.category_group, cm.group_name, c.location
ORDER BY c.category_group, c.location;
