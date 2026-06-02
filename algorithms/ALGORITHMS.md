# Distributed Query Algorithms — VietJobs (Citus + PostgreSQL)

Tài liệu mô tả chi tiết từng phase thực hiện của 5 thuật toán phân tán được triển khai trong hệ thống. Tất cả bảng (`core`, `detail`, `skills`) được phân phối theo `category_group` (hash distribution), đảm bảo co-location.

---

## 1. Parallel Hash Join — `01_hash_join.sql`

### Bối cảnh
Ba bảng `core`, `detail`, `skills` phân mảnh dọc (vertical partitioning) cùng distribution key `category_group`. Mỗi worker giữ shard của cả 3 bảng cho cùng 1 group value.

### Các phase thực hiện

#### Phase 1 — Coordinator Dispatch
Coordinator phân tích query, xác định co-location condition `USING (category_group, job_id)`. Nếu query có `WHERE category_group = X`, coordinator tính `hash(X)` → chọn đúng 1 worker (shard pruning, Task Count: 1). Nếu không có filter trên distribution key → gửi sub-query đến tất cả workers (scatter-gather, Task Count: 8 với 4 workers × 2 shards/worker).

#### Phase 2 — Build Phase (tại mỗi worker)
Worker nạp bảng nhỏ hơn (`detail` hoặc `skills`) vào **hash table trong bộ nhớ**, key là `(category_group, job_id)`. Hash table được xây dựng một lần, giữ nguyên cho toàn bộ probe phase tiếp theo.

#### Phase 3 — Probe Phase (tại mỗi worker)
Worker quét bảng lớn nhất (`core`) tuần tự, với mỗi row tra cứu hash table theo key `(category_group, job_id)`. Khi tìm thấy match, worker nối thêm bảng thứ 3 bằng cách tra hash table thứ hai (nếu có). Toàn bộ join diễn ra **cục bộ — không có cross-node data transfer**.

#### Phase 4 — Result Merge (tại coordinator)
Coordinator nhận kết quả đã join từ mỗi worker (chỉ các rows thỏa điều kiện, không phải raw rows). Coordinator thực hiện `UNION ALL` các partial results, áp dụng `ORDER BY` và `LIMIT` toàn cục rồi trả về client.

### Functions
| Function | Mô tả |
|---|---|
| `v_jobs_full` | View join đầy đủ 3 bảng, scatter-gather 4 sub-tasks |
| `hash_join_search` | Tìm kiếm có filter, hỗ trợ shard pruning |
| `hash_join_job_detail` | PK lookup — bắt buộc truyền `category_group` để prune về 1 worker |

---

## 2. Query Routing & Shard Pruning — `02_query_routing.sql`

### Bối cảnh
Citus lưu metadata `hash(distribution_value) → shard_id → worker_node`. Coordinator dùng metadata này để quyết định gửi query đến bao nhiêu workers — đây là cơ chế Query Routing.

### Các phase thực hiện

#### Phase 1 — Predicate Analysis (tại coordinator)
Coordinator phân tích mệnh đề `WHERE`. Nếu phát hiện điều kiện đẳng thức trên distribution column (`category_group = X`), chuyển sang Shard Pruning. Nếu không, chuyển sang Scatter-Gather.

#### Phase 2a — Shard Pruning (single-shard path)
Coordinator tính `hash(X)` → tra bảng metadata → xác định đúng `shard_id` và `worker_node`. Chỉ 1 sub-task được tạo và gửi đến đúng 1 worker. Worker sử dụng index `idx_core_group_loc(category_group, location)` → **I/O giảm ~75%** so với scatter-gather.

#### Phase 2b — Scatter-Gather (multi-shard path)
Coordinator tạo 8 sub-tasks (4 workers × 2 shards/worker) và gửi đồng thời. Mỗi worker scan shard cục bộ với filter (vd: `location = 'Hà Nội'`). Cả 4 workers chạy **song song**, tổng latency ≈ thời gian của worker chậm nhất.

#### Phase 3 — Result Aggregation (tại coordinator)
Coordinator thu kết quả từ tất cả workers tham gia (1 hoặc 4), merge và áp dụng `ORDER BY`, `LIMIT` toàn cục trước khi trả về client.

### Functions
| Function | Mô tả |
|---|---|
| `route_jobs_by_group` | Bắt buộc `category_group` → luôn shard pruning |
| `search_jobs_keyword` | Tùy chọn `category_group` → pruning hoặc scatter-gather |
| `route_salary_range` | Kết hợp range scan trên `idx_core_salary` với optional pruning |

---

## 3. MapReduce 2-Phase Aggregation — `03_mapreduce_agg.sql`

### Bối cảnh
Citus tự động chuyển đổi aggregation queries thành mô hình 2-phase tương tự MapReduce khi gặp `GROUP BY` trên distributed tables. Network savings đạt O(n_workers × n_groups) thay vì O(n_rows).

### Các phase thực hiện

#### Phase 1 — MAP: Partial Aggregation (song song tại 4 workers)
Mỗi worker tính **partial aggregate** trên shard cục bộ:
- `COUNT(*)` → `partial_count`
- `SUM(salary_avg)` → `partial_sum` (dùng để tính AVG chính xác toàn cục)
- `MIN(salary_min)` → `partial_min`
- `MAX(salary_max)` → `partial_max`

`category_mapping` là **reference table** (replicated trên tất cả workers) → JOIN diễn ra cục bộ tại worker, không tốn network. Worker chỉ gửi vài dòng tổng hợp về coordinator, không gửi raw rows.

#### Phase 2 — REDUCE: Global Aggregation (tại coordinator)
Coordinator nhận 4 partial result sets (O(n_workers × n_groups) rows). Tổng hợp:
- `COUNT_global = Σ partial_count`
- `AVG_global = Σ(partial_sum) / Σ(partial_count)`
- `MIN_global = MIN(partial_min)`
- `MAX_global = MAX(partial_max)`

Với dataset 24,281 rows, GROUP BY 8 groups → coordinator nhận **32 rows** thay vì 24,281 rows (**tiết kiệm ~760×**).

#### Phase 3 — Final Sort & Return
Coordinator áp dụng `ORDER BY`, `HAVING` (nếu có) trên kết quả đã reduce rồi trả về client.

### Functions
| Function | Mô tả |
|---|---|
| `mapreduce_salary_stats` | MAP/REDUCE thống kê lương per (category_group, location) |
| `mapreduce_category_report` | CTE 2 bước minh họa MAP (base) → REDUCE (pivot) |
| `mapreduce_experience_salary` | Phân tích lương theo mức kinh nghiệm |
| `mapreduce_contract_distribution` | Phân bố loại hợp đồng, GROUP BY 2 chiều |

---

## 4. Distributed Semi-Join — `04_semi_join.sql`

### Bối cảnh
Semi-join trả về dòng từ bảng trái khi tồn tại ít nhất 1 dòng khớp trong bảng phải — không nhân bản dòng, không đọc toàn bộ bảng phải. Với co-located tables, toàn bộ semi-join diễn ra cục bộ tại worker.

### Các phase thực hiện

#### Phase 1 — Coordinator Dispatch
Tương tự Hash Join: coordinator xác định co-location, quyết định shard pruning (1 sub-task) hoặc scatter-gather (8 sub-tasks). Sub-query gồm cả semi-join condition (`EXISTS` / `IN`) được đẩy xuống worker nguyên vẹn.

#### Phase 2 — Semi-Join Execution (tại mỗi worker)
Worker thực hiện semi-join theo một trong hai chiến lược planner chọn dựa trên selectivity:
- **Hash Semi Join**: build hash table từ bảng phải (sub-query), probe từ bảng trái. Early stop ngay khi tìm được match đầu tiên cho mỗi row trái → không đọc toàn bộ bảng phải.
- **Nested Loop Semi Join**: với mỗi row bảng trái, scan bảng phải cho đến khi tìm được match hoặc hết. Phù hợp khi bảng phải nhỏ hoặc có index.

#### Phase 2b — Anti Semi-Join (NOT EXISTS variant)
Planner chọn **Merge Anti Join** khi cả 2 bảng có index trên join key `(category_group, job_id)`. Worker scan song song cả 2 index, trả về row bảng trái **không có** bất kỳ match nào trong bảng phải. Dùng để phát hiện orphan records sau ETL.

#### Phase 3 — Result Merge
Coordinator thu partial results từ các workers, `UNION ALL` và áp dụng `ORDER BY`, `LIMIT` toàn cục.

### Functions
| Function | Mô tả |
|---|---|
| `semi_join_jobs_with_skill` | Semi-join qua JOIN với LIMIT (early stop) |
| `semi_join_jobs_qualified` | 2 EXISTS song song trên detail + skills |
| `anti_semi_join_missing_data` | NOT EXISTS — phát hiện dữ liệu thiếu sau ETL |
| `semi_join_soft_skill_filter` | Co-located semi-join kết hợp filter số học |

---

## 5. Parallel Top-K — `05_parallel_topk.sql`

### Bối cảnh
Bài toán Top-K phân tán: tìm K phần tử lớn nhất trong N phần tử trải rộng trên nhiều workers. Naive approach (fetch toàn bộ N rows về coordinator rồi sort) có chi phí O(N). Parallel Top-K giảm xuống O(4K) trên network.

### Các phase thực hiện

#### Phase 1 — Local Top-K (song song tại 4 workers)
Citus tự động push `ORDER BY col DESC LIMIT K` xuống từng worker. Mỗi worker dùng index `idx_core_salary(salary_avg DESC)` để thực hiện **index scan thay vì full sort** → lấy K rows đầu tiên với chi phí O(K) I/O. Mỗi worker gửi về coordinator đúng K rows tốt nhất của shard cục bộ.

Network transfer: `4 × K` rows thay vì `N` rows. Với N=24,281, K=10 → gửi **40 rows thay vì 24,281 (~giảm 600×)**.

#### Phase 2 — Global Top-K (tại coordinator)
Coordinator nhận `4 × K` ứng viên, thực hiện sort và `LIMIT K` → chọn ra K phần tử tốt nhất toàn cục. Kết quả **chính xác 100%** (không phải xấp xỉ).

#### Phase 1b — Partitioned Top-K (variant cho `topk_per_group`)
Với `PARTITION BY category_group` (= distribution key), window function `ROW_NUMBER()` chạy **hoàn toàn cục bộ** tại mỗi worker vì mỗi shard chỉ chứa 1 giá trị `category_group`. Không cần cross-node shuffle. Coordinator chỉ filter `WHERE rn <= K` trên kết quả trả về.

#### Phase 3 — Final Merge & Rank
Coordinator áp dụng `ORDER BY` toàn cục (theo `category_group`, `rank`) và trả về kết quả đã rank cho client.

### Functions
| Function | Mô tả |
|---|---|
| `topk_salary_global` | Top-K lương cao nhất toàn hệ thống, hỗ trợ shard pruning |
| `topk_per_group` | Partitioned Top-K — window function cục bộ tại worker |
| `topk_salary_bracket` | Histogram bucket lương — MAP/REDUCE pattern |
| `topk_recent_high_salary` | Top-K per mức kinh nghiệm — phân tích seniority vs salary |

---

## Tổng hợp so sánh

| Thuật toán | Network Transfer | Citus Mechanism | Index sử dụng |
|---|---|---|---|
| Hash Join | Chỉ joined rows | Co-location, zero shuffle | `(category_group, job_id)` PK |
| Query Routing | Chỉ matching rows | Shard pruning (hash metadata) | `idx_core_group_loc`, `idx_core_salary` |
| MapReduce Agg | O(workers × groups) | 2-phase aggregation | — (seq scan + HashAggregate) |
| Semi-Join | Chỉ semi-matched rows | Co-location, early stop | `(category_group, job_id)` PK |
| Parallel Top-K | O(4K) thay vì O(N) | LIMIT pushdown, index scan | `idx_core_salary (salary_avg DESC)` |
