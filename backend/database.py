"""
database.py — Connection layer cho VietJobs Distributed DB

3 kiến trúc con ánh xạ vào 3 quyết định thiết kế:

  Kiến trúc 1 (ERD / quan hệ bảng):
    → Chỉ kết nối đến COORDINATOR duy nhất. Coordinator giữ metadata
      pg_dist_node, pg_dist_shard và làm query routing đến đúng worker.
      Không kết nối thẳng vào worker — worker không có routing table.

  Kiến trúc 2 (Shard Layout / phân tán ngang):
    → Tier 1: query chỉ trên core. Khi truyền category_group, Citus prune
      về 1 worker (Task Count: 1) thay vì broadcast 4 workers (Task Count: 4).
      Đây là "fast path" cho list/filter/search.

  Kiến trúc 3 (Vertical Fragmentation / phân mảnh dọc):
    → Tier 2: join 3 bảng co-located (core + detail + skills).
      Chỉ dùng khi client cần mô tả và kỹ năng (detail view).
      Citus đảm bảo join cục bộ trên từng worker — zero cross-node shuffle.
"""

import os
import asyncpg
from typing import Any, Optional
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# KIẾN TRÚC 1 — Connection đến Coordinator (ERD)
# -----------------------------------------------------------------------------
# Một pool duy nhất trỏ đến coordinator:5432.
# Mọi query đều đi qua coordinator — không bypass để kết nối thẳng worker,
# vì worker không có metadata để route query hoặc biết co-location rules.
# =============================================================================

_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'postgres')}"
    f":{os.getenv('DB_PASSWORD', 'postgres')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'vietjobs')}"
)

_pool: Optional[asyncpg.Pool] = None


async def connect() -> None:
    """Khởi tạo connection pool khi app startup."""
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=_DSN,
        min_size=int(os.getenv("DB_POOL_MIN", 5)),
        max_size=int(os.getenv("DB_POOL_MAX", 20)),
        # Citus cần search_path = public để resolve distributed table names
        init=_init_conn,
    )


async def _init_conn(conn: asyncpg.Connection) -> None:
    """Chạy một lần cho mỗi connection mới trong pool."""
    await conn.execute("SET search_path = public")


async def disconnect() -> None:
    """Đóng pool khi app shutdown."""
    if _pool:
        await _pool.close()


async def get_db() -> asyncpg.Pool:
    """
    FastAPI dependency injection — inject pool vào router.
    Trả về pool (không phải connection đơn) để router tự acquire/release.
    """
    return _pool


# =============================================================================
# KIẾN TRÚC 2 — Tier 1: Core-only queries (Shard Layout / phân tán ngang)
# -----------------------------------------------------------------------------
# Các hàm này gọi FUNCTION đã định nghĩa trong Pha 3 chỉ trên bảng core
# (hoặc core + category_mapping reference table).
#
# Nguyên tắc shard pruning:
#   category_group = None  → Citus broadcast 4 workers (Task Count: 4)
#   category_group = X     → Citus prune về 1 worker (Task Count: 1, nhanh 4×)
#
# Tier 1 KHÔNG join detail/skills — phù hợp với: list view, filter, search.
# =============================================================================

async def query_jobs_list(
    category_group: Optional[int] = None,
    location: Optional[str] = None,
    limit: int = 50,
    page: int = 1,
) -> dict[str, Any]:
    """
    Tier 1 — Trả về danh sách phân trang (items + total + pages).
    Shard pruning khi có category_group (Task Count: 1).
    Scatter-gather khi không có category_group (Task Count: 4, tất cả workers).
    """
    offset = (page - 1) * limit
    async with _pool.acquire() as conn:
        if category_group is not None:
            total: int = await conn.fetchval(
                "SELECT COUNT(*) FROM core WHERE category_group=$1 AND ($2::TEXT IS NULL OR location=$2)",
                category_group, location,
            )
            rows = await conn.fetch("""
                SELECT job_id, job_title, category, location,
                       salary_min, salary_max, salary_avg,
                       experience_required, contract_type
                FROM core
                WHERE category_group = $1
                  AND ($2::TEXT IS NULL OR location = $2)
                ORDER BY salary_avg DESC NULLS LAST
                LIMIT $3 OFFSET $4
            """, category_group, location, limit, offset)
        else:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM core WHERE ($1::TEXT IS NULL OR location=$1)",
                location,
            )
            rows = await conn.fetch("""
                SELECT job_id, job_title, category, location,
                       salary_min, salary_max, salary_avg,
                       experience_required, contract_type
                FROM core
                WHERE ($1::TEXT IS NULL OR location = $1)
                ORDER BY salary_avg DESC NULLS LAST
                LIMIT $2 OFFSET $3
            """, location, limit, offset)
    pages = max(1, (total + limit - 1) // limit)
    return {"items": [dict(r) for r in rows], "total": total, "page": page, "limit": limit, "pages": pages}


async def query_jobs_keyword(
    keyword: str,
    category_group: Optional[int] = None,
    location: Optional[str] = None,
) -> list[dict]:
    """
    Tier 1 — Tìm từ khoá trong job_title.
    Có category_group → prune 1 worker. Không có → scatter-gather 4 workers.
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM search_jobs_keyword($1, $2, $3)",
            keyword, category_group, location,
        )
    return [dict(r) for r in rows]


async def query_jobs_salary(
    min_salary: float,
    max_salary: Optional[float] = None,
    category_group: Optional[int] = None,
    location: Optional[str] = None,
) -> list[dict]:
    """Tier 1 — Filter theo khoảng lương, đơn vị triệu VND."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM route_salary_range($1, $2, $3, $4)",
            min_salary, max_salary, category_group, location,
        )
    return [dict(r) for r in rows]


async def query_jobs_skill(
    skill: str,
    category_group: Optional[int] = None,
    location: Optional[str] = None,
) -> list[dict]:
    """
    Tier 1 (+ semi-join) — Tìm jobs có kỹ năng kỹ thuật cụ thể.
    Gọi semi_join_jobs_with_skill() — join core + skills co-located.
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM semi_join_jobs_with_skill($1, $2, $3)",
            skill, category_group, location,
        )
    return [dict(r) for r in rows]


# =============================================================================
# KIẾN TRÚC 3 — Tier 2: Full-join queries (Vertical Fragmentation)
# -----------------------------------------------------------------------------
# Các hàm này gọi FUNCTION join 3 bảng phân mảnh dọc:
#   core (12 cột nhẹ) + detail (3 cột text nặng) + skills (4 cột kỹ năng)
#
# Điều kiện co-located join: cả 3 bảng cùng distribution key (category_group)
# → Citus join cục bộ trên từng worker, không shuffle qua network.
#
# Chỉ gọi Tier 2 khi client cần description / requirements / skills.
# List view dùng Tier 1 để tránh đọc các cột TEXT nặng không cần thiết.
# =============================================================================

async def query_job_detail(
    job_id: int,
    category_group: int,
) -> Optional[dict]:
    """
    Tier 2 — Hash Join 3 bảng: core + detail + skills theo PK.
    category_group bắt buộc để Citus prune về đúng 1 worker.
    Trả về None nếu không tìm thấy job.
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM hash_join_job_detail($1, $2)",
            job_id, category_group,
        )
    return dict(rows[0]) if rows else None


async def query_jobs_full(
    category_group: Optional[int] = None,
    location: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """
    Tier 2 — Hash Join đầy đủ 3 bảng (cho export hoặc trang detail có filter).
    Citus gửi 4 sub-task song song nếu không có category_group.
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM hash_join_search($1, $2, $3)",
            category_group, location, limit,
        )
    return [dict(r) for r in rows]


# =============================================================================
# METADATA — Cluster info (pg_dist_node, pg_dist_shard)
# -----------------------------------------------------------------------------
# Query trực tiếp metadata Citus trên coordinator.
# Không liên quan đến data tables — dùng cho /cluster API endpoint.
# =============================================================================

async def query_cluster_nodes() -> list[dict]:
    """Danh sách nodes trong Citus cluster (coordinator + workers)."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                nodeid,
                nodename,
                nodeport,
                isactive,
                noderole::text,
                nodecluster
            FROM pg_dist_node
            ORDER BY nodeid
        """)
    return [dict(r) for r in rows]


async def query_shard_placement() -> list[dict]:
    """Placement của từng shard: table → shard_id → worker."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                shard.logicalrelid::regclass::text   AS table_name,
                shard.shardid,
                node.nodename                        AS worker,
                node.nodeport,
                shard.shardminvalue::int             AS group_min,
                shard.shardmaxvalue::int             AS group_max
            FROM pg_dist_shard      shard
            JOIN pg_dist_placement  placement USING (shardid)
            JOIN pg_dist_node       node ON node.groupid = placement.groupid
            WHERE shard.logicalrelid::text IN ('core', 'detail', 'skills')
            ORDER BY node.nodename, table_name
        """)
    return [dict(r) for r in rows]


async def explain_execution_time(sql: str) -> float:
    """Chạy EXPLAIN ANALYZE và trả về Execution Time (ms) mà PostgreSQL báo.
    Dùng để đối chiếu độc lập với wall-clock time của perf_counter."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(f"EXPLAIN (ANALYZE, COSTS OFF, TIMING ON) {sql}")
    for row in rows:
        line: str = row[0]
        if "Execution Time:" in line:
            return float(line.split("Execution Time:")[1].replace("ms", "").strip())
    return 0.0


async def query_data_distribution() -> list[dict]:
    """Phân bố số bản ghi theo (category_group, location) — verify shard balance."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                c.category_group,
                cm.group_name,
                c.location,
                COUNT(*) AS job_count
            FROM core c
            JOIN category_mapping cm ON cm.category = c.category
            GROUP BY c.category_group, cm.group_name, c.location
            ORDER BY c.category_group, c.location
        """)
    return [dict(r) for r in rows]
