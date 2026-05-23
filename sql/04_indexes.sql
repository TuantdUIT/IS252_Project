-- =============================================================================
-- FILE: 04_indexes.sql
-- Tạo index để tăng tốc 5 giải thuật song song.
-- Chạy sau 03_load_data.sql để build 1 lần trên data đã có (nhanh hơn build
-- dần khi insert).
--
-- Citus tự lan toả CREATE INDEX ra mọi shard trên 4 worker — không cần thao
-- tác thủ công với từng worker.
--
-- Lưu ý chiến lược:
--   - Bỏ qua index đơn trên `location` (chỉ 2 giá trị → selectivity ~50% →
--     PostgreSQL planner có thể bỏ index, dùng seq scan vì rẻ hơn)
--   - Dùng composite index khi cột thường đi cùng nhau trong WHERE
--   - PRIMARY KEY (category_group, job_id) đã tự tạo index → không tạo lại
-- =============================================================================


-- =============================================================================
-- INDEX 1: Composite (category_group, location)
-- -----------------------------------------------------------------------------
-- Dùng cho: Algo #2 Query Routing — query phổ biến nhất của app
--   WHERE category_group = X AND location = Y
-- Đặt category_group trước để leveraging shard pruning của Citus.
-- =============================================================================
CREATE INDEX idx_core_group_loc ON core (category_group, location);


-- =============================================================================
-- INDEX 2: salary_avg DESC
-- -----------------------------------------------------------------------------
-- Dùng cho:
--   - Algo #5 Top-K: ORDER BY salary_avg DESC LIMIT N
--   - Algo #4 Semi-join filter: WHERE salary_avg > 30000000 (range query)
-- DESC để phù hợp với chiều sort thường dùng (lương cao nhất trước).
-- =============================================================================
CREATE INDEX idx_core_salary ON core (salary_avg DESC);


-- =============================================================================
-- INDEX 3: category (16 giá trị)
-- -----------------------------------------------------------------------------
-- Dùng cho query filter theo category gốc (không qua category_group).
-- 16 giá trị → selectivity ~6% → B-tree hiệu quả.
-- =============================================================================
CREATE INDEX idx_core_category ON core (category);


-- =============================================================================
-- INDEX 4: experience_required
-- -----------------------------------------------------------------------------
-- Dùng cho filter theo kinh nghiệm ('1 năm', '3 năm', '5 năm'...).
-- Số giá trị khoảng ~10-20 (tuỳ dataset) → selectivity trung bình.
-- =============================================================================
CREATE INDEX idx_core_exp ON core (experience_required);


-- =============================================================================
-- INDEX TRÊN BẢNG DETAIL VÀ SKILLS
-- -----------------------------------------------------------------------------
-- PRIMARY KEY (category_group, job_id) đã tự tạo composite index → đủ cho
-- semi-join lookup từ detail/skills về core. Không cần index thêm.
--
-- Trường hợp đặc biệt: nếu cần full-text search trên description, thêm GIN:
-- =============================================================================

-- (Tuỳ chọn) Full-text search trên description cho tìm kiếm từ khoá
-- CREATE INDEX idx_detail_desc_fts ON detail
--     USING GIN (to_tsvector('simple', description));


-- =============================================================================
-- ANALYZE: cập nhật statistics cho query planner
-- -----------------------------------------------------------------------------
-- PostgreSQL cần biết phân bố dữ liệu (cardinality, histogram) để chọn
-- giữa index scan và seq scan. Bulk load không tự cập nhật → chạy ANALYZE
-- để planner ra quyết định tối ưu.
-- =============================================================================
ANALYZE core;
ANALYZE detail;
ANALYZE skills;
ANALYZE category_mapping;


-- =============================================================================
-- VERIFY: liệt kê index đã tạo
-- -----------------------------------------------------------------------------
-- Kiểm tra xem các index có được tạo đầy đủ trên coordinator metadata.
-- Citus tự lan ra worker — không cần verify trên worker.
-- =============================================================================
SELECT schemaname, tablename, indexname
  FROM pg_indexes
 WHERE schemaname = 'public'
   AND tablename IN ('core', 'detail', 'skills', 'category_mapping')
 ORDER BY tablename, indexname;
