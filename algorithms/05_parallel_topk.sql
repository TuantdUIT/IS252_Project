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
        SELECT r.category_group, r.group_name, r.rn AS rank_in_group,
               r.job_id, r.job_title, r.location, r.salary_avg
        FROM ranked r
        WHERE r.rn <= p_k
        ORDER BY r.category_group, r.rn;
END;
$$;


CREATE OR REPLACE FUNCTION topk_salary_bracket(
    p_bucket_size NUMERIC DEFAULT 10
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
            SELECT
                FLOOR(c.salary_avg / p_bucket_size) * p_bucket_size AS bucket_floor,
                COUNT(*) AS cnt
            FROM core c
            WHERE c.salary_avg IS NOT NULL
            GROUP BY bucket_floor
        ),
        totals AS (
            SELECT SUM(cnt) AS grand_total FROM buckets
        )
        SELECT
            TO_CHAR(b.bucket_floor, 'FM999') || '-'
                || TO_CHAR(b.bucket_floor + p_bucket_size, 'FM999')
                || 'M'                                           AS bracket_label,
            b.bucket_floor                                      AS salary_from,
            b.bucket_floor + p_bucket_size                      AS salary_to,
            b.cnt                                               AS job_count,
            ROUND(100.0 * b.cnt / NULLIF(t.grand_total, 0), 1)  AS pct_total
        FROM buckets b, totals t
        ORDER BY b.bucket_floor;
END;
$$;


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
        SELECT e.experience_required, e.rn AS rank_num,
               e.job_id, e.job_title, e.category, e.location, e.salary_avg
        FROM exp_ranked e
        WHERE e.rn <= p_k
        ORDER BY e.experience_required, e.rn;
END;
$$;


EXPLAIN (COSTS OFF)
SELECT job_id, job_title, salary_avg
  FROM core
 WHERE salary_avg IS NOT NULL
 ORDER BY salary_avg DESC
 LIMIT 10;


EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT job_id, salary_avg FROM core
 WHERE salary_avg IS NOT NULL
 ORDER BY salary_avg DESC
 LIMIT 10;

EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT job_id, salary_avg FROM core
 WHERE salary_avg IS NOT NULL
 ORDER BY salary_avg DESC;


SELECT * FROM topk_salary_global(10);
SELECT * FROM topk_salary_global(5, 'Hà Nội');
SELECT * FROM topk_salary_global(5, 'TPHCM', 2);
SELECT * FROM topk_per_group(3);
SELECT * FROM topk_salary_bracket(5000000);
SELECT * FROM topk_recent_high_salary(3);
