"""
routers/jobs.py — Job search và detail endpoints

Thiết kế 2-tier tương ứng Vertical Fragmentation:
  Tier 1 (core-only, fast) : list, search, salary-filter, skill-filter
  Tier 2 (full-join, heavy): detail view một job cụ thể

Mọi endpoint có tham số category_group tuỳ chọn:
  Có     → Citus shard pruning → Task Count: 1 (1 worker xử lý)
  Không  → Citus scatter-gather → Task Count: 4 (4 workers song song)
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from models import JobDetail, JobListItem, JobSkillItem, PaginatedJobs
import database as db

router = APIRouter(prefix="/jobs", tags=["jobs"])


# =============================================================================
# GET /jobs — Tier 1: danh sách việc làm (core only)
# -----------------------------------------------------------------------------
# Gọi route_jobs_by_group() — index scan trên (category_group, location).
# category_group bắt buộc → shard pruning về 1 worker.
# category_group vắng mặt → scatter-gather 4 workers.
# =============================================================================
@router.get("", response_model=PaginatedJobs)
async def list_jobs(
    category_group: Optional[int] = Query(
        default=None,
        description="1=commerce, 2=tech, 3=creative, 4=people. "
                    "Có → shard pruning (1 worker). Không → 4 workers song song.",
        ge=1, le=4,
    ),
    location: Optional[str] = Query(
        default=None,
        description="'Hà Nội' hoặc 'TPHCM'",
    ),
    page: int = Query(default=1, ge=1, description="Số trang (bắt đầu từ 1)"),
    limit: int = Query(default=50, ge=1, le=100, description="Số bản ghi mỗi trang"),
):
    return await db.query_jobs_list(category_group, location, limit, page)


# =============================================================================
# GET /jobs/search — Tier 1: tìm kiếm theo từ khoá trong job_title
# -----------------------------------------------------------------------------
# Gọi search_jobs_keyword() — ILIKE scan.
# Tốc độ phụ thuộc vào có hay không có category_group:
#   Có  → 1 worker, ILIKE trên ~3,000-8,000 rows
#   Không → 4 workers song song, ILIKE trên 24,281 rows song song
# =============================================================================
@router.get("/search", response_model=list[JobListItem])
async def search_jobs(
    q: str = Query(..., min_length=1, description="Từ khoá trong tên công việc"),
    category_group: Optional[int] = Query(default=None, ge=1, le=4),
    location: Optional[str] = None,
):
    return await db.query_jobs_keyword(q, category_group, location)


# =============================================================================
# GET /jobs/salary — Tier 1: filter theo khoảng lương
# -----------------------------------------------------------------------------
# Gọi route_salary_range() — index scan trên idx_core_salary (salary_avg DESC).
# Đơn vị: triệu VND. Ví dụ: min=10, max=30 → 10–30 triệu/tháng.
# =============================================================================
@router.get("/salary", response_model=list[JobListItem])
async def filter_by_salary(
    min: float = Query(..., description="Lương tối thiểu (triệu VND)", ge=0),
    max: Optional[float] = Query(default=None, description="Lương tối đa (triệu VND)"),
    category_group: Optional[int] = Query(default=None, ge=1, le=4),
    location: Optional[str] = None,
):
    return await db.query_jobs_salary(min, max, category_group, location)


# =============================================================================
# GET /jobs/skill — Tier 1 + Semi-join: tìm theo kỹ năng kỹ thuật
# -----------------------------------------------------------------------------
# Gọi semi_join_jobs_with_skill() — co-located semi-join core × skills.
# Worker thực hiện Nested Loop Semi Join cục bộ (không cross-node).
# =============================================================================
@router.get("/skill", response_model=list[JobSkillItem])
async def filter_by_skill(
    q: str = Query(..., min_length=1, description="Tên kỹ năng, vd: Python, Java, Excel"),
    category_group: Optional[int] = Query(default=None, ge=1, le=4),
    location: Optional[str] = None,
):
    return await db.query_jobs_skill(q, category_group, location)


# =============================================================================
# GET /jobs/{category_group}/{job_id} — Tier 2: chi tiết 1 việc làm (full-join)
# -----------------------------------------------------------------------------
# Gọi hash_join_job_detail() — co-located Hash Join core + detail + skills.
# category_group bắt buộc trong URL để Citus prune về đúng 1 worker.
# Trả về 404 nếu không tìm thấy job.
# =============================================================================
@router.get("/{category_group}/{job_id}", response_model=JobDetail)
async def get_job_detail(
    category_group: int,
    job_id: int,
):
    result = await db.query_job_detail(job_id, category_group)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return result
