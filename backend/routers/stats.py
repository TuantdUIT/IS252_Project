"""
routers/stats.py — Statistics và analytics endpoints

Gọi 3 nhóm thuật toán từ Pha 3:
  MapReduce Aggregation : salary-stats, categories, experience, contracts
  Parallel Top-K        : topk global, topk per group, salary histogram
  Semi-join             : data quality check

Tất cả query là aggregate — Citus tự chia MAP (tại worker) + REDUCE (tại coordinator).
Không có tham số phân trang vì kết quả aggregate đã nhỏ (< 100 rows).
"""

from typing import Optional
from fastapi import APIRouter, Query
from models import (
    CategoryReport,
    ContractDistribution,
    ExperienceSalary,
    SalaryBracket,
    SalaryStats,
    TopKGroupJob,
    TopKJob,
)
import database as db

router = APIRouter(prefix="/stats", tags=["stats"])


# =============================================================================
# MapReduce Aggregation
# =============================================================================

# GET /stats/salary
# -----------------------------------------------------------------------------
# Gọi mapreduce_salary_stats(location) — 2-phase aggregation:
#   MAP:    mỗi worker tính COUNT, SUM, MIN, MAX per (category_group, location)
#   REDUCE: coordinator tổng hợp → AVG = SUM/COUNT, MIN/MAX global
# =============================================================================
@router.get("/salary", response_model=list[SalaryStats])
async def salary_stats(
    location: Optional[str] = Query(
        default=None,
        description="'Hà Nội' hoặc 'TPHCM'. Không có → cả 2 thành phố.",
    ),
):
    """Thống kê lương theo nhóm nghề và địa điểm (MapReduce 2-phase)."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM mapreduce_salary_stats($1)", location
        )
    return [dict(r) for r in rows]


# GET /stats/categories
# -----------------------------------------------------------------------------
# Gọi mapreduce_category_report() — báo cáo 16 ngành:
#   MAP: worker tính partial count/avg per (category, location)
#   REDUCE: coordinator pivot → pct_hanoi, pct_hcm
# =============================================================================
@router.get("/categories", response_model=list[CategoryReport])
async def category_report():
    """Báo cáo thị trường 16 ngành nghề: số việc, lương TB, tỷ lệ % Hà Nội/TPHCM."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM mapreduce_category_report()")
    return [dict(r) for r in rows]


# GET /stats/experience
# -----------------------------------------------------------------------------
# Gọi mapreduce_experience_salary() — phân tích lương × kinh nghiệm:
#   MAP:    worker tính partial per experience_required
#   REDUCE: coordinator tổng hợp, sort theo avg_salary DESC
# =============================================================================
@router.get("/experience", response_model=list[ExperienceSalary])
async def experience_salary():
    """Phân tích mức lương theo yêu cầu kinh nghiệm (MapReduce 2-phase)."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM mapreduce_experience_salary()")
    return [dict(r) for r in rows]


# GET /stats/contracts
# -----------------------------------------------------------------------------
# Gọi mapreduce_contract_distribution() — phân bố loại hợp đồng:
#   MAP:    worker đếm per (category_group, contract_type)
#   REDUCE: coordinator tổng hợp, tính tỷ lệ % trong từng nhóm
# =============================================================================
@router.get("/contracts", response_model=list[ContractDistribution])
async def contract_distribution():
    """Phân bố loại hợp đồng (toàn thời gian / bán thời gian...) theo nhóm nghề."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM mapreduce_contract_distribution()")
    return [dict(r) for r in rows]


# =============================================================================
# Parallel Top-K
# =============================================================================

# GET /stats/topk
# -----------------------------------------------------------------------------
# Gọi topk_salary_global(k, location, category_group):
#   Phase 1: mỗi worker trả về top-K cục bộ (LIMIT pushdown, index scan)
#   Phase 2: coordinator sort 4K rows, LIMIT K cuối cùng
# =============================================================================
@router.get("/topk", response_model=list[TopKJob])
async def top_k_salary(
    k: int = Query(default=10, ge=1, le=100, description="Số kết quả cần lấy"),
    location: Optional[str] = Query(default=None, description="'Hà Nội' hoặc 'TPHCM'"),
    category_group: Optional[int] = Query(default=None, ge=1, le=4),
):
    """Top-K việc làm lương cao nhất (Parallel Top-K với LIMIT pushdown)."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM topk_salary_global($1, $2, $3)",
            k, location, category_group,
        )
    return [dict(r) for r in rows]


# GET /stats/topk/groups
# -----------------------------------------------------------------------------
# Gọi topk_per_group(k, location):
#   PARTITION BY category_group = distribution key
#   → window function chạy cục bộ tại worker, không cross-node shuffle
# =============================================================================
@router.get("/topk/groups", response_model=list[TopKGroupJob])
async def top_k_per_group(
    k: int = Query(default=5, ge=1, le=50, description="Top-K cho mỗi nhóm"),
    location: Optional[str] = Query(default=None),
):
    """Top-K lương cao nhất trong từng nhóm nghề (partitioned Top-K)."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM topk_per_group($1, $2)", k, location
        )
    return [dict(r) for r in rows]


# GET /stats/histogram
# -----------------------------------------------------------------------------
# Gọi topk_salary_bracket(bucket_size):
#   MAP:    worker đếm jobs per salary bucket cục bộ
#   REDUCE: coordinator SUM counts, tính % tổng
# =============================================================================
@router.get("/histogram", response_model=list[SalaryBracket])
async def salary_histogram(
    bucket: float = Query(
        default=10,
        description="Độ rộng mỗi bucket (triệu VND). Mặc định: 10 triệu.",
        ge=1,
    ),
):
    """Histogram phân bố lương (MapReduce bucket aggregation)."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM topk_salary_bracket($1)", bucket
        )
    return [dict(r) for r in rows]


# =============================================================================
# Data Quality (Semi-join)
# =============================================================================

# GET /stats/quality
# -----------------------------------------------------------------------------
# Gọi anti_semi_join_missing_data() — NOT EXISTS semi-join:
#   Tìm jobs trong core nhưng thiếu dữ liệu trong detail hoặc skills.
#   Kỳ vọng: trả về 0 dòng nếu ETL thành công.
# =============================================================================
@router.get("/quality")
async def data_quality():
    """Kiểm tra tính toàn vẹn dữ liệu (Anti Semi-Join). Kỳ vọng: empty list."""
    pool = await db.get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM anti_semi_join_missing_data()")
    missing = [dict(r) for r in rows]
    return {
        "status": "ok" if not missing else "warning",
        "missing_count": len(missing),
        "details": missing[:20],  # giới hạn 20 dòng đầu nếu có lỗi
    }
