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


EXPLAIN (COSTS OFF)
SELECT job_id, job_title, salary_avg
  FROM core
 WHERE location = 'Hà Nội'
 LIMIT 10;


EXPLAIN (COSTS OFF)
SELECT job_id, job_title, salary_avg
  FROM core
 WHERE category_group = 2
   AND location = 'Hà Nội'
 LIMIT 10;


EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT COUNT(*), ROUND(AVG(salary_avg), 0) AS avg_salary
  FROM core
 WHERE location = 'TPHCM';

EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT COUNT(*), ROUND(AVG(salary_avg), 0) AS avg_salary
  FROM core
 WHERE category_group = 1
   AND location = 'TPHCM';


SELECT * FROM route_jobs_by_group(2, 'Hà Nội', 5);
SELECT * FROM search_jobs_keyword('marketing');
SELECT * FROM search_jobs_keyword('marketing', 3);
SELECT * FROM route_salary_range(20, 50) LIMIT 10;
