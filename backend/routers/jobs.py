"""
routers/jobs.py — Job search và detail endpoints

Thiết kế 2-tier tương ứng Vertical Fragmentation:
  Tier 1 (core-only, fast) : list, search, salary-filter, skill-filter
  Tier 2 (full-join, heavy): detail view một job cụ thể

Mọi endpoint có tham số category_group tuỳ chọn:
  Có     → Citus shard pruning → Task Count: 1 (1 worker xử lý)
  Không  → Citus scatter-gather → Task Count: 4 (4 workers song song)
"""

import asyncio
import time
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
# =============================================================================
# GET /jobs/benchmark — Chạy đồng thời 5 thuật toán, trả về thời gian ms
# -----------------------------------------------------------------------------
# asyncio.gather() chạy 5 coroutine song song trên event loop.
# Mỗi thuật toán được đo độc lập bằng perf_counter trước/sau await.
# Params giống /jobs/search — dùng cùng keyword/category_group/location
# để so sánh táo với táo.
# =============================================================================
def _pg_lit(v) -> str:
    """Chuyển Python value → PostgreSQL literal để dùng trong EXPLAIN ANALYZE."""
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


@router.get("/benchmark")
async def benchmark_algorithms(
    q: str = Query(default="", description="Từ khoá tìm kiếm (để trống = toàn bộ)"),
    category_group: Optional[int] = Query(default=None, ge=1, le=4),
    location: Optional[str] = None,
):
    kw = q.strip() or "a"

    # Trả về cả wall_ms (perf_counter Python) lẫn pg_ms (Execution Time PostgreSQL)
    # để người dùng có thể đối chiếu 2 nguồn độc lập.
    async def timed_with_pg(sql: str, query_coro):
        t0 = time.perf_counter()
        await query_coro
        wall_ms = round((time.perf_counter() - t0) * 1000)

        pg_ms = await db.explain_execution_time(sql)
        return wall_ms, pg_ms

    cg  = category_group
    loc = location

    results = await asyncio.gather(
        timed_with_pg(
            f"SELECT * FROM hash_join_search({_pg_lit(cg)}, {_pg_lit(loc)}, 20)",
            db.query_jobs_full(cg, loc, 20),
        ),
        timed_with_pg(
            f"SELECT * FROM search_jobs_keyword({_pg_lit(kw)}, {_pg_lit(cg)}, {_pg_lit(loc)})",
            db.query_jobs_keyword(kw, cg, loc),
        ),
        timed_with_pg(
            "SELECT category_group, location, COUNT(*), ROUND(AVG(salary_avg),0) FROM core GROUP BY category_group, location",
            db.query_data_distribution(),
        ),
        timed_with_pg(
            f"SELECT * FROM core WHERE ({_pg_lit(loc)}::TEXT IS NULL OR location={_pg_lit(loc)}) ORDER BY salary_avg DESC LIMIT 20",
            db.query_jobs_list(cg, loc, 20, 1),
        ),
        timed_with_pg(
            f"SELECT * FROM semi_join_jobs_with_skill({_pg_lit(kw)}, {_pg_lit(cg)}, {_pg_lit(loc)})",
            db.query_jobs_skill(kw, cg, loc),
        ),
    )

    algorithms = ["hash_join", "query_routing", "mapreduce", "top_k", "semi_join"]
    workers = 1 if category_group else 4
    tasks   = 1 if category_group else 4

    return [
        {
            "algorithm": alg,
            "ms":        wall_ms,
            "pg_ms":     pg_ms,
            "overhead_ms": wall_ms - pg_ms,
            "workers":   workers,
            "tasks":     tasks,
        }
        for alg, (wall_ms, pg_ms) in zip(algorithms, results)
    ]


@router.get("/{category_group}/{job_id}", response_model=JobDetail)
async def get_job_detail(
    category_group: int,
    job_id: int,
):
    result = await db.query_job_detail(job_id, category_group)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return result
