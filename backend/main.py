"""
main.py — FastAPI application entry point

Khởi động app theo thứ tự:
  1. lifespan startup: tạo connection pool đến coordinator
  2. Mount 3 routers: /jobs, /stats, /cluster
  3. lifespan shutdown: đóng pool khi app dừng
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import database as db
from routers import cluster, jobs, stats


# =============================================================================
# Lifespan — quản lý vòng đời connection pool
# -----------------------------------------------------------------------------
# Dùng asynccontextmanager thay vì @app.on_event (deprecated trong FastAPI 0.95+).
# Pool tạo 1 lần khi startup, dùng chung cho mọi request, đóng khi shutdown.
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    yield
    await db.disconnect()


# =============================================================================
# FastAPI app
# =============================================================================
app = FastAPI(
    title="VietJobs Distributed DB — API",
    description=(
        "REST API cho dự án cơ sở dữ liệu phân tán VietJobs.\n\n"
        "**Backend**: PostgreSQL + Citus (1 coordinator + 4 workers)\n\n"
        "**Thuật toán**: Hash Join, Query Routing, MapReduce Aggregation, "
        "Semi-Join, Parallel Top-K"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# =============================================================================
# CORS — cho phép frontend React gọi API
# =============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # production: giới hạn về domain frontend
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Routers
# =============================================================================
app.include_router(jobs.router)     # /jobs/*
app.include_router(stats.router)    # /stats/*
app.include_router(cluster.router)  # /cluster/*


# =============================================================================
# Health check
# =============================================================================
@app.get("/health", tags=["system"])
async def health():
    """Kiểm tra app đang chạy và pool đã khởi tạo."""
    pool = await db.get_db()
    return {
        "status": "ok",
        "pool_size": pool.get_size(),
        "pool_free": pool.get_idle_size(),
    }


@app.get("/", tags=["system"])
async def root():
    return {
        "app": "VietJobs Distributed DB API",
        "docs": "/docs",
        "endpoints": {
            "jobs":    "/jobs",
            "stats":   "/stats",
            "cluster": "/cluster",
        },
    }
