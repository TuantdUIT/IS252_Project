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
        JOIN skills s ON s.category_group = c.category_group
                     AND s.job_id = c.job_id
        WHERE s.technical_skills ILIKE '%' || p_skill || '%'
          AND (p_category_group IS NULL OR c.category_group = p_category_group)
          AND (p_location IS NULL OR c.location = p_location)
        ORDER BY c.salary_avg DESC NULLS LAST
        LIMIT 50;
END;
$$;


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
          AND EXISTS (
              SELECT 1 FROM detail d
               WHERE d.category_group = c.category_group
                 AND d.job_id = c.job_id
                 AND d.description IS NOT NULL
          )
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
        SELECT c.job_id, c.category_group, c.job_title, 'detail'::TEXT AS missing_in
        FROM core c
        WHERE NOT EXISTS (
            SELECT 1 FROM detail d
             WHERE d.category_group = c.category_group
               AND d.job_id = c.job_id
        )

        UNION ALL

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


EXPLAIN (COSTS OFF)
SELECT c.job_id, c.category_group
  FROM core c
 WHERE NOT EXISTS (
     SELECT 1 FROM detail d
      WHERE d.category_group = c.category_group
        AND d.job_id = c.job_id
 )
 LIMIT 10;


SELECT * FROM semi_join_jobs_with_skill('Python', 2, 'Hà Nội');
SELECT * FROM semi_join_jobs_qualified(30) LIMIT 10;
SELECT COUNT(*), missing_in FROM anti_semi_join_missing_data() GROUP BY missing_in;
SELECT * FROM semi_join_soft_skill_filter('giao tiếp', 10) LIMIT 10;
