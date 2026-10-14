-- The merge per bucket over shuffled partial states merges every block as it arrives and spills to disk
-- under the external aggregation settings. Without that it would keep all received states in memory.

DROP TABLE IF EXISTS t_shuffle_states_spill;
CREATE TABLE t_shuffle_states_spill (k String, v UInt64) ENGINE = MergeTree ORDER BY tuple()
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_shuffle_states_spill;
INSERT INTO t_shuffle_states_spill SELECT concat('key ', toString(number % 20000)), number FROM numbers(100000);

SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET distributed_plan_default_reader_bucket_count = 2;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
SET param__internal_cascades_cluster_node_count = 2;
SET param__internal_join_table_stat_hints = '{"t_shuffle_states_spill": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 10000000}}}';
-- Spill as soon as possible. The spill needs the two-level hash table.
SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

SELECT '-- results match the local execution';
SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_shuffle_states_spill GROUP BY k) SETTINGS make_distributed_plan = 0;
SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_shuffle_states_spill GROUP BY k)
SETTINGS enable_cascades_optimizer = 0, log_processors_profiles = 1, log_comment = '05394_rule_based';
SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_shuffle_states_spill GROUP BY k)
SETTINGS enable_cascades_optimizer = 1, log_processors_profiles = 1, log_comment = '05394_cascades';

-- The log queries below are not the subject of the test; they run without the distributed plan, which
-- would read the system tables on the workers while they still merge their parts.
SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT '-- the tasks that merge the shuffled states spill to disk';
-- The merging tasks are the ones that do not read the table.
SELECT log_comment, sum(ProfileEvents['ExternalAggregationWritePart']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
            AND log_comment IN ('05394_rule_based', '05394_cascades'))
    AND query_id NOT IN (
        SELECT query_id FROM system.processors_profile_log
        WHERE event_date >= yesterday() AND name LIKE 'MergeTreeSelect%')
    AND query_id LIKE '%::stage_%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_shuffle_states_spill;
