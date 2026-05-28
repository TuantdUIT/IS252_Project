# Tiến độ dự án VietJobs Distributed DB

> Cập nhật lần cuối: 2026-05-28 (session 2)

---

## Pha 1 — Hạ tầng & Schema ✅ HOÀN THÀNH

- [x] Tạo cấu trúc thư mục
- [x] Viết `docker/docker-compose.yml`
- [x] Đặt dataset `dataset_IS252.csv` vào `data/` (24,281 bản ghi, 19 cột)
- [x] Viết `sql/01_schema.sql` (150 dòng)
- [x] Viết `sql/02_distribute.sql` (75 dòng)
- [x] Viết `sql/02b_pin_shards.sql` (81 dòng)
- [x] Viết `sql/03_load_data.sql` (132 dòng)
- [x] Viết `sql/04_indexes.sql` (94 dòng)

---

## Pha 2 — Khởi động cluster ✅ HOÀN THÀNH

- [x] `docker compose up -d` — 5 container healthy (coordinator + 4 workers)
- [x] Xóa service `manager` khỏi compose (không tương thích Docker API v1.44)
- [x] Đặt `name: vietjobs` trong compose để tránh xung đột project name với YugabyteDB cũ
- [x] Đăng ký 4 workers thủ công qua `citus_add_node()` + `citus_set_coordinator_host()`
- [x] Chạy 5 file SQL bằng `psql -f /sql/...` (đọc từ trong container, tránh lỗi encoding UTF-8 của PowerShell pipe)
- [x] Verify phân bố dữ liệu: 24,281 bản ghi trên 8 phân mảnh (4 nhóm × 2 location)

### Phân bố dữ liệu đã xác nhận

| category_group | location | bản ghi |
|---|---|---|
| 1 (commerce) | Hà Nội | 4,546 |
| 1 (commerce) | TPHCM | 3,471 |
| 2 (tech) | Hà Nội | 3,039 |
| 2 (tech) | TPHCM | 2,366 |
| 3 (creative) | Hà Nội | 4,848 |
| 3 (creative) | TPHCM | 3,174 |
| 4 (people) | Hà Nội | 1,788 |
| 4 (people) | TPHCM | 1,049 |
| **Tổng** | | **24,281** |

### Ghi chú kỹ thuật quan trọng

- Luôn dùng `psql -f /sql/<file>.sql` thay vì `Get-Content | docker exec` để tránh lỗi encoding UTF-8
- Workers đăng ký thủ công mỗi khi cluster reset (thông tin lưu trong volume `vietjobs_coordinator_data`)
- Coordinator hostname phải set về `coordinator` (không phải `localhost`) để workers kết nối ngược lại được

---

## Pha 3 — 5 giải thuật song song ✅ HOÀN THÀNH

- [x] Viết và chạy `algorithms/01_hash_join.sql` — VIEW `v_jobs_full`, FUNCTION `hash_join_search`, `hash_join_job_detail`
- [x] Viết và chạy `algorithms/02_query_routing.sql` — FUNCTION `route_jobs_by_group`, `search_jobs_keyword`, `route_salary_range`
- [x] Viết và chạy `algorithms/03_mapreduce_agg.sql` — FUNCTION `mapreduce_salary_stats`, `mapreduce_category_report`, `mapreduce_experience_salary`, `mapreduce_contract_distribution`
- [x] Viết và chạy `algorithms/04_semi_join.sql` — FUNCTION `semi_join_jobs_with_skill`, `semi_join_jobs_qualified`, `anti_semi_join_missing_data`, `semi_join_soft_skill_filter`
- [x] Viết và chạy `algorithms/05_parallel_topk.sql` — FUNCTION `topk_salary_global`, `topk_per_group`, `topk_salary_bracket`, `topk_recent_high_salary`
- [x] EXPLAIN ANALYZE đã chạy inline trong từng file — kết quả ghi nhận bên dưới

### Execution plan đã xác nhận

| Thuật toán | EXPLAIN kết quả | Hiệu năng |
|---|---|---|
| Hash Join (01) | Task Count: 8 (broadcast) / **Task Count: 1** (shard pruning); Merge Join cục bộ trên worker | Join 3 bảng không shuffle network |
| Query Routing (02) | **36.9ms** (scatter, 8 tasks) vs **9.6ms** (shard pruning, 1 task); Bitmap Index Scan `idx_core_group_loc` | Nhanh **~4×** khi biết category_group |
| MapReduce Agg (03) | `HashAggregate` tại worker (MAP phase); `Sort+Aggregate` tại coordinator (REDUCE phase) | Coordinator nhận 32 rows thay vì 24,281 |
| Semi-Join (04) | `Nested Loop Semi Join` (EXISTS); `Merge Anti Join` (NOT EXISTS); data quality: **0 dòng thiếu** | ETL toàn vẹn 100% |
| Parallel Top-K (05) | `Limit` tại worker (push-down); coordinator sort **40 rows** (4×10) | **26.8ms** (LIMIT 10) vs **84ms** (full sort) — nhanh **3×** |

### Ghi chú kỹ thuật quan trọng (Pha 3)

- Citus **không hỗ trợ** `LANGUAGE SQL` function có tham số truy vấn distributed table → phải dùng `LANGUAGE plpgsql` với `RETURN QUERY`
- JOIN `category_mapping` phải dùng `cm.category = c.category` (1-to-1); nếu dùng `cm.category_group = c.category_group` sẽ nhân bản dòng (×3–5)
- Trong `plpgsql RETURNS TABLE`, tên cột output là biến cục bộ → dùng alias khi truy vấn CTE cùng tên để tránh `ambiguous column`
- Salary lưu đơn vị **triệu VND** (range 0–500); phân bố: 60% nằm trong khoảng 10–20M/tháng
- `../algorithms:/algorithms` đã được thêm vào volume mount của coordinator trong `docker-compose.yml`

---

## Pha 4 — Backend API ⚠️ CODE ĐÃ VIẾT — CHƯA CHẠY THỬ

> **Trạng thái**: 6 file Python đã implement đầy đủ, chưa khởi động `uvicorn` và test endpoint thực tế.

- [x] `backend/database.py` — asyncpg pool, 3-tier query layer (Tier 1 / Tier 2 / metadata)
- [x] `backend/models.py` — Pydantic v2 schemas: JobListItem, JobDetail, SalaryStats, TopKJob, ClusterNode...
- [x] `backend/routers/jobs.py` — GET /jobs, /jobs/search, /jobs/salary, /jobs/skill, /jobs/{group}/{id}
- [x] `backend/routers/stats.py` — GET /stats/salary|categories|experience|contracts|topk|histogram|quality
- [x] `backend/routers/cluster.py` — GET /cluster/nodes|shards|distribution
- [x] `backend/main.py` — FastAPI app: lifespan pool, CORS *, 3 routers, /health, /
- [ ] **Chạy thử**: `uvicorn main:app --reload --port 8000` và verify `/docs`
- [ ] **Test endpoint**: `/health`, `/jobs`, `/stats/salary`, `/cluster/nodes`

### Thiết kế 2-tier

| Tier | Endpoint | Function | Join |
|---|---|---|---|
| Tier 1 (fast) | /jobs, /search, /salary, /skill | `route_*`, `search_*`, `semi_join_*` | core only |
| Tier 2 (full) | /jobs/{group}/{id} | `hash_join_job_detail()` | core + detail + skills |

Shard pruning: truyền `category_group` → Task Count: 1 (1 worker); bỏ trống → Task Count: 4 (4 workers song song).

### Lệnh khởi động backend
```powershell
cd f:\NAM3\HKII\CSDL_Phan_tan\Project\backend
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

---

## Pha 5 — Frontend ⚠️ CODE ĐÃ VIẾT — CHƯA CHẠY THỬ

> **Trạng thái**: 3 file đã implement đầy đủ, chưa `npm install` và chạy dev server để verify UI.

- [x] `frontend/vite.config.ts` — proxy `/api` → `http://localhost:8000`, rewrite strip `/api` prefix
- [x] `frontend/src/main.tsx` — React 18 entry point (`createRoot`)
- [x] `frontend/src/App.tsx` — Dashboard 3 tab: Jobs / Stats / Cluster (~370 dòng, TypeScript strict)
- [ ] **Chạy thử**: `npm install && npm run dev` → verify tại `http://localhost:5173`
- [ ] **Test UI**: tab Jobs (search + filter + detail modal), Stats (4 biểu đồ/bảng), Cluster (nodes + distribution)

### Tính năng Frontend

| Tab | Nội dung |
|---|---|
| Jobs | Tìm từ khoá / kỹ năng + filter nhóm ngành + địa điểm + danh sách + modal chi tiết Tier 2 |
| Stats | Bar chart lương MapReduce, bảng kinh nghiệm, Top-10 lương cao (Parallel Top-K), histogram 10M bucket |
| Cluster | Danh sách nodes (coordinator + 4 workers) + phân bố bản ghi theo shard có progress bar |

### Lệnh khởi động frontend
```powershell
cd f:\NAM3\HKII\CSDL_Phan_tan\Project\frontend
npm install
npm run dev
# → http://localhost:5173
```

---

## Pha 6 — Benchmark & Báo cáo ❌ CHƯA BẮT ĐẦU

- [ ] `benchmark/benchmark.py` — đo thời gian 5 giải thuật (asyncpg, lặp 10 lần, tính mean/p95)
- [ ] Chạy benchmark, lưu `results/benchmark.json`
- [ ] Vẽ biểu đồ `plots/benchmark.png` (bar chart thời gian + scalability)
- [ ] Scalability test: 1 → 2 → 4 workers (tắt/bật worker bằng docker stop)
- [ ] Viết báo cáo Chương 1–6

---

## Tổng quan tiến độ

| Pha | Trạng thái | Ghi chú |
|---|---|---|
| 1 — Schema & Hạ tầng | ✅ Hoàn thành & đã verify | Cluster chạy ổn định |
| 2 — Khởi động cluster | ✅ Hoàn thành & đã verify | 24,281 bản ghi, 8 shards |
| 3 — 5 giải thuật SQL | ✅ Hoàn thành & đã verify | EXPLAIN ANALYZE đã xác nhận |
| 4 — Backend API | ⚠️ Code xong, chưa test | Cần chạy uvicorn |
| 5 — Frontend | ⚠️ Code xong, chưa test | Cần npm install + dev server |
| 6 — Benchmark & Báo cáo | ❌ Chưa bắt đầu | |
