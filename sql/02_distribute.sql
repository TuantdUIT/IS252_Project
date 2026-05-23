-- =============================================================================
-- FILE: 02_distribute.sql
-- Phân mảnh ngang: biến 3 bảng core/detail/skills thành distributed tables
-- và category_mapping thành reference table.
-- Chạy sau 01_schema.sql, trước 02b_pin_shards.sql.
-- =============================================================================


-- Cấu hình số shard cho phiên hiện tại. Phải SET trước create_distributed_table()
-- vì Citus "đông cứng" số shard ngay khi tạo distributed table đầu tiên.
-- Chọn 8 thay vì 4 vì hash(3) và hash(4) collision khi shard_count=4
-- → 2 group bị ép vào cùng 1 shard, không thể pin về 2 worker khác nhau.
-- Với 8 bucket: hash(1)→0, hash(2)→6, hash(3)→3, hash(4)→2 (4 bucket riêng biệt).
-- Hệ quả: 4 shard có dữ liệu + 4 shard rỗng. Pin shard có dữ liệu về đúng worker.
SET citus.shard_count = 8;


-- =============================================================================
-- DISTRIBUTE: core (master của co-location group)
-- -----------------------------------------------------------------------------
-- Bảng đầu tiên được distribute → tự tạo co-location group mới với
-- colocationid riêng. Các bảng sau colocate_with => 'core' sẽ kế thừa.
-- Hiệu ứng: tạo 4 shard core_xxx, phát tán ra 4 worker theo hash(category_group).
-- =============================================================================
SELECT create_distributed_table('core', 'category_group');


-- =============================================================================
-- DISTRIBUTE: detail (co-locate với core)
-- -----------------------------------------------------------------------------
-- colocate_with => 'core' buộc Citus đặt shard detail có cùng hash range với
-- shard core tương ứng trên cùng worker. Điều kiện cứng: cùng distribution
-- column ('category_group') và cùng shard_count (kế thừa từ core).
-- → Tiền đề cho Algo #1 Parallel Hash Join Co-located.
-- =============================================================================
SELECT create_distributed_table('detail', 'category_group', colocate_with => 'core');


-- =============================================================================
-- DISTRIBUTE: skills (co-locate với core)
-- -----------------------------------------------------------------------------
-- Hoàn tất co-location group: core + detail + skills cùng colocationid.
-- Sau lệnh này, mọi join 3 bảng theo (category_group, job_id) sẽ chạy cục bộ
-- trên từng worker, không shuffle qua network.
-- =============================================================================
SELECT create_distributed_table('skills', 'category_group', colocate_with => 'core');


-- =============================================================================
-- REFERENCE TABLE: category_mapping
-- -----------------------------------------------------------------------------
-- Khác với distributed table, reference table được SAO CHÉP ĐẦY ĐỦ 16 dòng
-- sang mọi worker. Lý do chọn:
--   - Bảng rất nhỏ (16 dòng, ~1KB)
--   - JOIN với core/detail/skills rất thường xuyên để derive group_name
--   - Sao chép trên worker → JOIN cục bộ, không tốn network
-- Đánh đổi: UPDATE/INSERT phải sync ra 4 worker (chậm), nhưng bảng này tĩnh.
-- =============================================================================
SELECT create_reference_table('category_mapping');


-- =============================================================================
-- VERIFY: kiểm tra metadata phân tán
-- -----------------------------------------------------------------------------
-- Kỳ vọng:
--   - core/detail/skills: cùng colocationid (vd: 5), partmethod='h' (hash)
--   - category_mapping: colocationid khác (reference table có group riêng),
--                       partmethod='n' (none)
-- =============================================================================
SELECT logicalrelid::regclass AS table_name,
       colocationid,
       partmethod,
       repmodel
  FROM pg_dist_partition
 ORDER BY logicalrelid::regclass::text;
