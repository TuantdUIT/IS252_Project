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


EXPLAIN (COSTS OFF)
SELECT c.job_id, c.job_title, c.salary_avg,
       d.description, s.technical_skills
  FROM core    c
  JOIN detail  d USING (category_group, job_id)
  JOIN skills  s USING (category_group, job_id)
 WHERE c.location = 'Hà Nội'
 LIMIT 10;


EXPLAIN (COSTS OFF)
SELECT c.job_id, c.job_title, c.salary_avg,
       d.description, s.technical_skills
  FROM core    c
  JOIN detail  d USING (category_group, job_id)
  JOIN skills  s USING (category_group, job_id)
 WHERE c.category_group = 2
   AND c.location = 'Hà Nội'
 LIMIT 10;


SELECT * FROM hash_join_search(2, 'Hà Nội', 5);


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
