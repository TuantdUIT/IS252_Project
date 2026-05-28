"""
routers/cluster.py — Cluster metadata endpoints

Expose thông tin Citus cluster qua REST API:
  GET /cluster/nodes        : danh sách coordinator + workers
  GET /cluster/shards       : placement shard → worker
  GET /cluster/distribution : phân bố bản ghi theo (category_group, location)

Tất cả query đều đọc metadata trên coordinator (pg_dist_*), không đọc data tables.
"""

from fastapi import APIRouter
from models import ClusterNode, ShardPlacement, DataDistribution
import database as db

router = APIRouter(prefix="/cluster", tags=["cluster"])


# =============================================================================
# GET /cluster/nodes
# -----------------------------------------------------------------------------
# Trả về danh sách node trong Citus cluster: 1 coordinator + 4 workers.
# isactive=true: node đang tham gia cluster và nhận query.
# noderole: 'primary' (coordinator) hoặc 'secondary' (worker).
# =============================================================================
@router.get("/nodes", response_model=list[ClusterNode])
async def get_cluster_nodes():
    """Danh sách tất cả node trong Citus cluster."""
    return await db.query_cluster_nodes()


# =============================================================================
# GET /cluster/shards
# -----------------------------------------------------------------------------
# Placement của từng shard: bảng nào, shard nào, nằm trên worker nào.
# group_min/group_max: hash range của shard (category_group value range).
# =============================================================================
@router.get("/shards", response_model=list[ShardPlacement])
async def get_shard_placement():
    """Placement shard của 3 bảng phân tán (core, detail, skills)."""
    return await db.query_shard_placement()


# =============================================================================
# GET /cluster/distribution
# -----------------------------------------------------------------------------
# Số bản ghi thực tế trên mỗi shard — verify load balance giữa 4 workers.
# Kỳ vọng: mỗi worker có 2 location × 1 category_group.
# =============================================================================
@router.get("/distribution", response_model=list[DataDistribution])
async def get_data_distribution():
    """Phân bố số bản ghi theo (category_group, location) — kiểm tra shard balance."""
    return await db.query_data_distribution()
