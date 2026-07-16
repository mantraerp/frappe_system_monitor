"""Netdata-style in-depth MariaDB/MySQL stats collector.

Pure-python, no frappe dependency, so it can be shared by both the
Frappe-context page backend (system_monitor.py) and the standalone
websocket server (ws_server.py) which talks to the DB directly via pymysql.

Callers are responsible for fetching raw rows from the server:
    - status:      dict from `SHOW GLOBAL STATUS`      (Variable_name -> Value)
    - variables:    dict from `SHOW GLOBAL VARIABLES`    (Variable_name -> Value)
    - slave_status: dict from `SHOW SLAVE STATUS` (or None if not a replica / unavailable)
    - processlist:  list of dict rows from `SHOW FULL PROCESSLIST` (or None)

collect() is stateful per-instance (keeps previous snapshot + timestamp) so
each process should keep a single MariaDBStatsCollector around and call
collect() on every poll.
"""

import time

# Counters -> rate (per second) output keys are "mdb_<name>_ps"
RATE_STATUS_KEYS = {
    "bytes_in": "Bytes_received",
    "bytes_out": "Bytes_sent",
    "queries": "Queries",
    "questions": "Questions",
    "slow_queries": "Slow_queries",
    "com_select": "Com_select",
    "com_insert": "Com_insert",
    "com_update": "Com_update",
    "com_delete": "Com_delete",
    "com_replace": "Com_replace",
    "com_commit": "Com_commit",
    "com_rollback": "Com_rollback",
    "connections": "Connections",
    "aborted_connects": "Aborted_connects",
    "aborted_clients": "Aborted_clients",
    "threads_created": "Threads_created",
    "table_locks_immediate": "Table_locks_immediate",
    "table_locks_waited": "Table_locks_waited",
    "select_full_join": "Select_full_join",
    "select_full_range_join": "Select_full_range_join",
    "select_range": "Select_range",
    "select_range_check": "Select_range_check",
    "select_scan": "Select_scan",
    "sort_merge_passes": "Sort_merge_passes",
    "sort_range": "Sort_range",
    "sort_scan": "Sort_scan",
    "created_tmp_tables": "Created_tmp_tables",
    "created_tmp_disk_tables": "Created_tmp_disk_tables",
    "created_tmp_files": "Created_tmp_files",
    "opened_tables": "Opened_tables",
    "handler_commit": "Handler_commit",
    "handler_rollback": "Handler_rollback",
    "handler_delete": "Handler_delete",
    "handler_write": "Handler_write",
    "handler_update": "Handler_update",
    "handler_read_first": "Handler_read_first",
    "handler_read_key": "Handler_read_key",
    "handler_read_next": "Handler_read_next",
    "handler_read_prev": "Handler_read_prev",
    "handler_read_rnd": "Handler_read_rnd",
    "handler_read_rnd_next": "Handler_read_rnd_next",
    "innodb_rows_read": "Innodb_rows_read",
    "innodb_rows_inserted": "Innodb_rows_inserted",
    "innodb_rows_updated": "Innodb_rows_updated",
    "innodb_rows_deleted": "Innodb_rows_deleted",
    "innodb_data_reads": "Innodb_data_reads",
    "innodb_data_writes": "Innodb_data_writes",
    "innodb_data_fsyncs": "Innodb_data_fsyncs",
    "innodb_os_log_written": "Innodb_os_log_written",
    "innodb_row_lock_waits": "Innodb_row_lock_waits",
    "innodb_buffer_pool_read_requests": "Innodb_buffer_pool_read_requests",
    "innodb_buffer_pool_reads": "Innodb_buffer_pool_reads",
    "innodb_buffer_pool_pages_flushed": "Innodb_buffer_pool_pages_flushed",
    "innodb_deadlocks": "Innodb_deadlocks",
    "qcache_hits": "Qcache_hits",
    "qcache_inserts": "Qcache_inserts",
    "qcache_not_cached": "Qcache_not_cached",
    "qcache_lowmem_prunes": "Qcache_lowmem_prunes",
    "binlog_cache_use": "Binlog_cache_use",
    "binlog_cache_disk_use": "Binlog_cache_disk_use",
}

# Point-in-time values -> output keys are "mdb_<name>"
GAUGE_STATUS_KEYS = {
    "threads_connected": "Threads_connected",
    "threads_running": "Threads_running",
    "threads_cached": "Threads_cached",
    "open_tables": "Open_tables",
    "open_files": "Open_files",
    "open_table_definitions": "Open_table_definitions",
    "innodb_buffer_pool_pages_total": "Innodb_buffer_pool_pages_total",
    "innodb_buffer_pool_pages_free": "Innodb_buffer_pool_pages_free",
    "innodb_buffer_pool_pages_dirty": "Innodb_buffer_pool_pages_dirty",
    "innodb_buffer_pool_pages_data": "Innodb_buffer_pool_pages_data",
    "innodb_buffer_pool_bytes_data": "Innodb_buffer_pool_bytes_data",
    "innodb_row_lock_current_waits": "Innodb_row_lock_current_waits",
    "innodb_row_lock_time_avg": "Innodb_row_lock_time_avg",
    "innodb_row_lock_time_max": "Innodb_row_lock_time_max",
    "qcache_free_memory": "Qcache_free_memory",
    "qcache_free_blocks": "Qcache_free_blocks",
    "qcache_total_blocks": "Qcache_total_blocks",
    "qcache_queries_in_cache": "Qcache_queries_in_cache",
    "uptime": "Uptime",
    "slow_queries_total": "Slow_queries",
    "queries_total": "Queries",
}

VARIABLE_KEYS = {
    "max_connections": "max_connections",
    "innodb_buffer_pool_size": "innodb_buffer_pool_size",
    "table_open_cache": "table_open_cache",
    "table_definition_cache": "table_definition_cache",
    "open_files_limit": "open_files_limit",
    "query_cache_size": "query_cache_size",
    "query_cache_type": "query_cache_type",
    "innodb_log_file_size": "innodb_log_file_size",
    "key_buffer_size": "key_buffer_size",
}

STATUS_KEYS_NEEDED = sorted(set(RATE_STATUS_KEYS.values()) | set(GAUGE_STATUS_KEYS.values()))
VARIABLE_KEYS_NEEDED = sorted(set(VARIABLE_KEYS.values()) | {"version"})

# Cap how many processlist rows we transmit/render per poll to keep the
# websocket payload small on busy servers with many connections.
PROCESSLIST_LIMIT = 100


def _num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class MariaDBStatsCollector:
    """Keeps previous snapshot so collect() can compute per-second rates."""

    def __init__(self):
        self._prev = None
        self._prev_time = None

    def collect(self, status, variables, slave_status=None, processlist=None, own_connection_id=None):
        out = {}
        now = time.time()
        status = status or {}
        variables = variables or {}

        try:
            own_connection_id = int(own_connection_id) if own_connection_id is not None else None
        except (TypeError, ValueError):
            own_connection_id = None

        for out_key, status_key in GAUGE_STATUS_KEYS.items():
            out["mdb_" + out_key] = _num(status.get(status_key))

        for out_key, var_key in VARIABLE_KEYS.items():
            out["mdb_" + out_key] = _num(variables.get(var_key))
        out["mdb_version"] = variables.get("version") or ""

        if self._prev and self._prev_time and now > self._prev_time:
            dt = now - self._prev_time
            for out_key, status_key in RATE_STATUS_KEYS.items():
                cur = _num(status.get(status_key))
                prev = _num(self._prev.get(status_key), cur)
                out["mdb_" + out_key + "_ps"] = round(max(0.0, (cur - prev) / dt), 2)
        else:
            for out_key in RATE_STATUS_KEYS:
                out["mdb_" + out_key + "_ps"] = 0

        self._prev = dict(status)
        self._prev_time = now

        bp_read_requests = out.get("mdb_innodb_buffer_pool_read_requests_ps", 0)
        bp_reads = out.get("mdb_innodb_buffer_pool_reads_ps", 0)
        total_bp = bp_read_requests + bp_reads
        out["mdb_innodb_buffer_pool_hit_pct"] = (
            round((bp_read_requests / total_bp) * 100, 2) if total_bp > 0 else 100.0
        )

        bp_total_pages = out.get("mdb_innodb_buffer_pool_pages_total", 0)
        out["mdb_innodb_buffer_pool_used_pct"] = (
            round((1 - out.get("mdb_innodb_buffer_pool_pages_free", 0) / bp_total_pages) * 100, 2)
            if bp_total_pages
            else 0
        )

        qc_hits = out.get("mdb_qcache_hits_ps", 0)
        qc_inserts = out.get("mdb_qcache_inserts_ps", 0)
        total_qc = qc_hits + qc_inserts
        out["mdb_qcache_hit_pct"] = round((qc_hits / total_qc) * 100, 2) if total_qc > 0 else 0

        max_conn = out.get("mdb_max_connections", 0)
        out["mdb_connections_pct"] = (
            round((out.get("mdb_threads_connected", 0) / max_conn) * 100, 2) if max_conn else 0
        )

        table_open_cache = out.get("mdb_table_open_cache", 0)
        out["mdb_table_cache_used_pct"] = (
            round((out.get("mdb_open_tables", 0) / table_open_cache) * 100, 2)
            if table_open_cache
            else 0
        )

        repl = {"is_replica": False}
        if slave_status:
            repl["is_replica"] = True
            repl["seconds_behind"] = slave_status.get("Seconds_Behind_Master")
            repl["io_running"] = slave_status.get("Slave_IO_Running", "") or ""
            repl["sql_running"] = slave_status.get("Slave_SQL_Running", "") or ""
            repl["last_error"] = (
                slave_status.get("Last_Error") or slave_status.get("Last_SQL_Error") or ""
            )
        out["mdb_replication"] = repl

        proclist = []
        if processlist:
            for p in processlist:
                raw_id = p.get("Id") if p.get("Id") is not None else p.get("id")
                try:
                    pid = int(raw_id)
                except (TypeError, ValueError):
                    pid = raw_id
                if own_connection_id is not None and pid == own_connection_id:
                    continue
                cmd = p.get("Command") or p.get("command") or ""
                t = _num(p.get("Time") if p.get("Time") is not None else p.get("time"))
                host = p.get("Host") or p.get("host") or ""
                proclist.append(
                    {
                        "id": pid,
                        "user": p.get("User") or p.get("user") or "",
                        "host": str(host).split(":")[0],
                        "db": p.get("db") or p.get("Db") or "",
                        "command": cmd or "",
                        "time": int(t),
                        "state": p.get("State") or p.get("state") or "",
                        "query": ((p.get("Info") or p.get("info") or "") or "")[:300],
                        "active": cmd not in ("Sleep", ""),
                    }
                )
            proclist.sort(key=lambda x: (not x["active"], -x["time"]))

        out["mdb_processlist_total"] = len(proclist)
        out["mdb_processlist"] = proclist[:PROCESSLIST_LIMIT]
        out["mdb_top_queries"] = [q for q in proclist if q["active"] and q["time"] >= 1][:10]
        out["mdb_active_queries"] = len([q for q in proclist if q["active"]])

        return out
