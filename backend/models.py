"""
models.py — Pydantic schemas cho request/response

Cấu trúc theo 3 tầng tương ứng database.py:
  Tier 1 (core-only)       : JobListItem, JobSkillItem
  Tier 2 (full-join)       : JobDetail
  Stats (MapReduce output) : SalaryStats, CategoryReport, ...
  Cluster (metadata)       : ClusterNode, ShardPlacement, DataDistribution
"""

from decimal import Decimal
from typing import Optional
from pydantic import BaseModel


# =============================================================================
# PAGINATED — Wrapper cho list response có phân trang
# =============================================================================

class PaginatedJobs(BaseModel):
    """Response cho GET /jobs — danh sách có phân trang."""
    items: list["JobListItem"]
    total: int
    page: int
    limit: int
    pages: int


# =============================================================================
# TIER 1 — Core-only (Shard Layout / fast path)
# Dùng cho list view, search, filter — không có description/skills.
# =============================================================================

class JobListItem(BaseModel):
    """Response cho GET /jobs, /jobs/search, /jobs/salary — Tier 1 query."""
    job_id: int
    job_title: Optional[str] = None
    category: Optional[str] = None
    location: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_avg: Optional[Decimal] = None
    experience_required: Optional[str] = None
    contract_type: Optional[str] = None

    model_config = {"from_attributes": True}


class JobSkillItem(BaseModel):
    """Response cho GET /jobs/skill — semi-join core + skills."""
    job_id: int
    job_title: Optional[str] = None
    category: Optional[str] = None
    location: Optional[str] = None
    salary_avg: Optional[Decimal] = None
    experience_required: Optional[str] = None
    technical_skills: Optional[str] = None

    model_config = {"from_attributes": True}


# =============================================================================
# TIER 2 — Full-join (Vertical Fragmentation / lazy-load)
# Dùng cho detail view — bao gồm description, requirements, skills.
# Kế thừa JobListItem để tái sử dụng 9 field cơ bản.
# =============================================================================

class JobDetail(JobListItem):
    """Response cho GET /jobs/{category_group}/{job_id} — Tier 2 query."""
    working_hours: Optional[str] = None
    description: Optional[str] = None
    requirements_text: Optional[str] = None
    benefits: Optional[str] = None
    technical_skills: Optional[str] = None
    soft_skills: Optional[str] = None
    qualifications: Optional[str] = None
    languages_required: Optional[str] = None


# =============================================================================
# STATS — MapReduce aggregation output
# =============================================================================

class SalaryStats(BaseModel):
    """Response cho GET /stats/salary — mapreduce_salary_stats()."""
    category_group: int
    group_name: str
    location: str
    job_count: int
    avg_salary: Optional[Decimal] = None
    min_salary: Optional[int] = None
    max_salary: Optional[int] = None

    model_config = {"from_attributes": True}


class CategoryReport(BaseModel):
    """Response cho GET /stats/categories — mapreduce_category_report()."""
    category: str
    group_name: str
    total_jobs: int
    avg_salary: Optional[Decimal] = None
    pct_hanoi: Optional[Decimal] = None
    pct_hcm: Optional[Decimal] = None

    model_config = {"from_attributes": True}


class ExperienceSalary(BaseModel):
    """Response cho GET /stats/experience — mapreduce_experience_salary()."""
    experience_required: str
    job_count: int
    avg_salary: Optional[Decimal] = None
    min_salary: Optional[int] = None
    max_salary: Optional[int] = None

    model_config = {"from_attributes": True}


class ContractDistribution(BaseModel):
    """Response cho GET /stats/contracts — mapreduce_contract_distribution()."""
    category_group: int
    group_name: str
    contract_type: str
    job_count: int
    pct_in_group: Optional[Decimal] = None

    model_config = {"from_attributes": True}


# =============================================================================
# TOP-K — Parallel Top-K output
# =============================================================================

class TopKJob(BaseModel):
    """Response cho GET /stats/topk — topk_salary_global()."""
    rank_num: int
    job_id: int
    job_title: Optional[str] = None
    category: Optional[str] = None
    location: Optional[str] = None
    salary_avg: Optional[Decimal] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    experience_required: Optional[str] = None

    model_config = {"from_attributes": True}


class TopKGroupJob(BaseModel):
    """Response cho GET /stats/topk/groups — topk_per_group()."""
    category_group: int
    group_name: str
    rank_in_group: int
    job_id: int
    job_title: Optional[str] = None
    location: Optional[str] = None
    salary_avg: Optional[Decimal] = None

    model_config = {"from_attributes": True}


class SalaryBracket(BaseModel):
    """Response cho GET /stats/histogram — topk_salary_bracket()."""
    bracket_label: str
    salary_from: Decimal
    salary_to: Decimal
    job_count: int
    pct_total: Optional[Decimal] = None

    model_config = {"from_attributes": True}


# =============================================================================
# CLUSTER — Citus metadata
# =============================================================================

class ClusterNode(BaseModel):
    """Response cho GET /cluster/nodes — pg_dist_node."""
    nodeid: int
    nodename: str
    nodeport: int
    isactive: bool
    noderole: str
    nodecluster: str

    model_config = {"from_attributes": True}


class ShardPlacement(BaseModel):
    """Response cho GET /cluster/shards — pg_dist_shard + pg_dist_placement."""
    table_name: str
    shardid: int
    worker: str
    nodeport: int
    group_min: Optional[int] = None
    group_max: Optional[int] = None

    model_config = {"from_attributes": True}


class DataDistribution(BaseModel):
    """Response cho GET /cluster/distribution — phân bố bản ghi theo shard."""
    category_group: int
    group_name: str
    location: str
    job_count: int

    model_config = {"from_attributes": True}
