-- =============================================================================
-- FILE: 02b_pin_shards.sql
-- Ép shard của mỗi category_group nằm đúng worker theo nhãn nghiệp vụ.
-- Chạy sau 02_distribute.sql, trước 03_load_data.sql (khi bảng còn trống).
-- =============================================================================


-- =============================================================================
-- FUNCTION: pin_group_to_worker
-- -----------------------------------------------------------------------------
-- Di chuyển shard chứa category_group = target_group về worker target_worker.
--
-- Cơ chế:
--   1. get_shard_id_for_distribution_column() → tìm shard_id của giá trị
--   2. pg_dist_shard_placement → tra worker đang chứa shard đó
--   3. Nếu chưa đúng worker → citus_move_shard_placement() di chuyển
--
-- Vì 3 bảng (core, detail, skills) cùng co-location group, di chuyển shard
-- của core sẽ kéo theo shard tương ứng của detail và skills.
--
-- shard_transfer_mode = 'block_writes': bảng đang trống nên block là instant,
-- không cần setup logical replication.
-- =============================================================================
CREATE OR REPLACE FUNCTION pin_group_to_worker(
    target_group  INTEGER,
    target_worker TEXT
) RETURNS VOID AS $$
DECLARE
    v_shard_id     BIGINT;
    v_current_node TEXT;
BEGIN
    v_shard_id := get_shard_id_for_distribution_column('core', target_group);

    SELECT nodename
      INTO v_current_node
      FROM pg_dist_shard_placement
     WHERE shardid = v_shard_id
     LIMIT 1;

    RAISE NOTICE 'category_group=% → shard_id=% đang ở [%], target=[%]',
        target_group, v_shard_id, v_current_node, target_worker;

    IF v_current_node IS DISTINCT FROM target_worker THEN
        PERFORM citus_move_shard_placement(
            v_shard_id,
            v_current_node, 5432,
            target_worker,  5432,
            shard_transfer_mode := 'block_writes'
        );
        RAISE NOTICE '  ✓ Đã chuyển shard % từ % sang %',
            v_shard_id, v_current_node, target_worker;
    ELSE
        RAISE NOTICE '  ✓ Shard % đã ở đúng worker %, bỏ qua',
            v_shard_id, target_worker;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Pin 4 nhóm về 4 worker tương ứng (hostname Docker network: worker1..worker4)
SELECT pin_group_to_worker(1, 'worker1');  -- commerce
SELECT pin_group_to_worker(2, 'worker2');  -- tech
SELECT pin_group_to_worker(3, 'worker3');  -- creative
SELECT pin_group_to_worker(4, 'worker4');  -- people


-- Verify placement: mỗi worker nên có 3 shard (core, detail, skills) cùng range
SELECT shard.logicalrelid::regclass AS table_name,
       placement.shardid,
       node.nodename                AS worker,
       shard.shardminvalue,
       shard.shardmaxvalue
  FROM pg_dist_shard      shard
  JOIN pg_dist_placement  placement USING (shardid)
  JOIN pg_dist_node       node      ON node.groupid = placement.groupid
 WHERE shard.logicalrelid::text IN ('core', 'detail', 'skills')
 ORDER BY node.nodename, table_name;


-- Dọn function tiện ích sau khi xong setup
DROP FUNCTION pin_group_to_worker(INTEGER, TEXT);
