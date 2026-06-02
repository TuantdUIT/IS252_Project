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
        JOIN category_mapping cm ON cm.category = c.category
        WHERE c.salary_avg IS NOT NULL
          AND (p_location IS NULL OR c.location = p_location)
        GROUP BY c.category_group, cm.group_name, c.location
        ORDER BY c.category_group, c.location;
END;
$$;


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
            SELECT
                c.category,
                c.location,
                COUNT(*)          AS cnt,
                AVG(c.salary_avg) AS avg_sal
            FROM core c
            GROUP BY c.category, c.location
        ),
        pivot AS (
            SELECT
                b.category,
                SUM(b.cnt)::BIGINT                                        AS total_jobs,
                ROUND(SUM(b.cnt * b.avg_sal) / NULLIF(SUM(b.cnt), 0), 0) AS avg_salary,
                ROUND(100.0 * SUM(b.cnt) FILTER (WHERE b.location = 'Hà Nội')
                      / NULLIF(SUM(b.cnt), 0), 1)                         AS pct_hanoi,
                ROUND(100.0 * SUM(b.cnt) FILTER (WHERE b.location = 'TPHCM')
                      / NULLIF(SUM(b.cnt), 0), 1)                         AS pct_hcm
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
            SELECT x.category_group, SUM(x.cnt) AS total
            FROM counts x
            GROUP BY x.category_group
        )
        SELECT
            ct.category_group,
            ct.group_name,
            ct.contract_type,
            ct.cnt                                         AS job_count,
            ROUND(100.0 * ct.cnt / NULLIF(g.total, 0), 1) AS pct_in_group
        FROM counts ct
        JOIN group_totals g USING (category_group)
        ORDER BY ct.category_group, ct.cnt DESC;
END;
$$;


EXPLAIN (COSTS OFF)
SELECT category_group, location, COUNT(*), ROUND(AVG(salary_avg), 0)
  FROM core
 GROUP BY category_group, location
 ORDER BY category_group;


SELECT * FROM mapreduce_salary_stats();
SELECT * FROM mapreduce_salary_stats('Hà Nội');
SELECT * FROM mapreduce_category_report();
SELECT * FROM mapreduce_experience_salary();
SELECT * FROM mapreduce_contract_distribution();
