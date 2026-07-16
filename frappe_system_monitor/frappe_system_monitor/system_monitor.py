import frappe, psutil, time, platform, os, datetime, subprocess, json

from frappe_system_monitor.frappe_system_monitor.mariadb_stats import (
    MariaDBStatsCollector,
    VARIABLE_KEYS_NEEDED,
)

_prev_net = None
_prev_net_time = None
_prev_disk_io = None
_prev_disk_io_time = None
_mariadb_collector = MariaDBStatsCollector()

HISTORY_KEY = "system_monitor:history"
HISTORY_LENGTH = 300


def _get_net_io():
    global _prev_net, _prev_net_time
    current = psutil.net_io_counters()
    now = time.time()
    if _prev_net and _prev_net_time:
        dt = now - _prev_net_time
        if dt > 0:
            result = {
                "bytes_sent": round((current.bytes_sent - _prev_net.bytes_sent) / dt),
                "bytes_recv": round((current.bytes_recv - _prev_net.bytes_recv) / dt),
                "packets_sent": round((current.packets_sent - _prev_net.packets_sent) / dt),
                "packets_recv": round((current.packets_recv - _prev_net.packets_recv) / dt),
            }
            _prev_net = current
            _prev_net_time = now
            return result
    _prev_net = current
    _prev_net_time = now
    return {"bytes_sent": 0, "bytes_recv": 0, "packets_sent": 0, "packets_recv": 0}


def _get_disk_io():
    global _prev_disk_io, _prev_disk_io_time
    current = psutil.disk_io_counters()
    now = time.time()
    if _prev_disk_io and _prev_disk_io_time:
        dt = now - _prev_disk_io_time
        if dt > 0:
            result = {
                "read_bytes": round((current.read_bytes - _prev_disk_io.read_bytes) / dt),
                "write_bytes": round((current.write_bytes - _prev_disk_io.write_bytes) / dt),
                "read_count": round((current.read_count - _prev_disk_io.read_count) / dt),
                "write_count": round((current.write_count - _prev_disk_io.write_count) / dt),
            }
            _prev_disk_io = current
            _prev_disk_io_time = now
            return result
    _prev_disk_io = current
    _prev_disk_io_time = now
    return {"read_bytes": 0, "write_bytes": 0, "read_count": 0, "write_count": 0}


def _collect_mariadb():
    try:
        status_rows = frappe.db.sql("SHOW GLOBAL STATUS", as_dict=True)
        status = {r.Variable_name: r.Value for r in status_rows}

        placeholders = ", ".join(["%s"] * len(VARIABLE_KEYS_NEEDED))
        var_rows = frappe.db.sql(
            f"SHOW GLOBAL VARIABLES WHERE Variable_name IN ({placeholders})",
            tuple(VARIABLE_KEYS_NEEDED),
            as_dict=True,
        )
        variables = {r.Variable_name: r.Value for r in var_rows}

        slave_status = None
        try:
            slave_rows = frappe.db.sql("SHOW SLAVE STATUS", as_dict=True)
            slave_status = slave_rows[0] if slave_rows else None
        except Exception:
            slave_status = None

        own_id = None
        try:
            own_id_rows = frappe.db.sql("SELECT CONNECTION_ID() AS id", as_dict=True)
            own_id = own_id_rows[0].id if own_id_rows else None
        except Exception:
            own_id = None

        processlist = None
        try:
            processlist = frappe.db.sql("SHOW FULL PROCESSLIST", as_dict=True)
        except Exception:
            processlist = None

        return _mariadb_collector.collect(
            status, variables, slave_status, processlist, own_connection_id=own_id
        )
    except Exception:
        return {}


def collect_metrics():
    data = {}

    cpu_pcts = psutil.cpu_percent(interval=0, percpu=True)
    data["cpu_total"] = round(sum(cpu_pcts) / len(cpu_pcts), 1) if cpu_pcts else 0
    data["cpu_cores"] = cpu_pcts
    data["cpu_count"] = psutil.cpu_count()

    load = os.getloadavg()
    data["load_1"] = round(load[0], 2)
    data["load_5"] = round(load[1], 2)
    data["load_15"] = round(load[2], 2)

    cpu_times = psutil.cpu_times_percent(interval=0)
    data["cpu_user"] = round(cpu_times.user, 1)
    data["cpu_system"] = round(cpu_times.system, 1)
    data["cpu_iowait"] = round(getattr(cpu_times, "iowait", 0), 1)
    data["cpu_idle"] = round(cpu_times.idle, 1)

    mem = psutil.virtual_memory()
    data["ram_total"] = round(mem.total / (1024**3), 1)
    data["ram_used"] = round(mem.used / (1024**3), 1)
    data["ram_available"] = round(mem.available / (1024**3), 1)
    data["ram_cached"] = round(getattr(mem, "cached", 0) / (1024**3), 1)
    data["ram_buffers"] = round(getattr(mem, "buffers", 0) / (1024**3), 1)
    data["ram_percent"] = round(mem.percent, 1)

    swap = psutil.swap_memory()
    data["swap_total"] = round(swap.total / (1024**3), 1)
    data["swap_used"] = round(swap.used / (1024**3), 1)
    data["swap_percent"] = round(swap.percent, 1)

    disk = psutil.disk_usage("/")
    data["disk_total"] = round(disk.total / (1024**3), 1)
    data["disk_used"] = round(disk.used / (1024**3), 1)
    data["disk_free"] = round(disk.free / (1024**3), 1)
    data["disk_percent"] = round(disk.percent, 1)

    disk_io = _get_disk_io()
    data["disk_read_bytes"] = disk_io["read_bytes"]
    data["disk_write_bytes"] = disk_io["write_bytes"]
    data["disk_read_count"] = disk_io["read_count"]
    data["disk_write_count"] = disk_io["write_count"]

    net_io = _get_net_io()
    data["net_sent"] = net_io["bytes_sent"]
    data["net_recv"] = net_io["bytes_recv"]
    data["net_packets_sent"] = net_io["packets_sent"]
    data["net_packets_recv"] = net_io["packets_recv"]

    data["processes_total"] = len(psutil.pids())
    procs = psutil.process_iter(["status"])
    running = sum(1 for p in procs if p.info["status"] == psutil.STATUS_RUNNING)
    data["processes_running"] = running

    data["uptime"] = int(time.time() - psutil.boot_time())
    data["boot_time"] = datetime.datetime.fromtimestamp(psutil.boot_time()).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    data["platform"] = platform.system()
    data["platform_release"] = platform.release()

    data.update(_collect_mariadb())

    try:
        from frappe.utils.scheduler import (
            get_scheduler_status,
            is_schduler_process_running,
            is_dormant,
        )

        ss = get_scheduler_status().get("status", "unknown")
        if not is_schduler_process_running():
            data["erpnext_scheduler"] = "Process Not Found"
        elif is_dormant():
            data["erpnext_scheduler"] = "Dormant"
        elif ss == "active":
            data["erpnext_scheduler"] = "Running"
        else:
            data["erpnext_scheduler"] = "Inactive"
    except Exception:
        data["erpnext_scheduler"] = "Unknown"

    try:
        from frappe.utils.background_jobs import (
            get_workers,
            get_queue,
            get_queue_list,
            get_queues,
        )

        workers = get_workers()
        active = [w for w in workers if w.pid]
        data["workers_active"] = len(active)
        data["workers_total"] = len(workers) if workers else 0
        queued = 0
        for qn in get_queue_list():
            try:
                queued += get_queue(qn).count
            except Exception:
                pass
        data["erpnext_queued"] = queued
        failed = 0
        for q in get_queues():
            try:
                fid = q.failed_job_registry.get_job_ids()
                failed += len(fid) if fid else 0
            except Exception:
                pass
        data["erpnext_failed"] = failed
    except Exception:
        data["workers_active"] = 0
        data["workers_total"] = 0
        data["erpnext_queued"] = 0
        data["erpnext_failed"] = 0

    try:
        data["emails_pending"] = frappe.db.count("Email Queue", {"status": "Queued"})
    except Exception:
        data["emails_pending"] = 0

    try:
        mi = frappe.cache.execute_command("INFO", "MEMORY")
        data["redis_memory"] = mi.get("used_memory_human", "N/A")
        ci = frappe.cache.execute_command("INFO", "CLIENTS")
        data["redis_connections"] = int(ci.get("connected_clients", 0))
        ki = frappe.cache.execute_command("INFO", "KEYSPACE")
        total_keys = 0
        for v in ki.values():
            if isinstance(v, dict):
                total_keys += int(v.get("keys", 0))
        data["redis_keys"] = total_keys
    except Exception:
        data["redis_memory"] = "N/A"
        data["redis_connections"] = 0
        data["redis_keys"] = 0

    services = {}
    for name, cmd in [
        ("nginx", "systemctl is-active nginx"),
        ("mariadb", "systemctl is-active mariadb"),
        ("redis", "systemctl is-active redis-server"),
        ("supervisor", "systemctl is-active supervisor"),
        ("bench-worker", "systemctl is-active frappe-bench-worker"),
        ("bench-web", "systemctl is-active frappe-bench-web"),
    ]:
        try:
            r = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=5
            )
            services[name] = "Running" if r.returncode == 0 else "Stopped"
        except Exception:
            services[name] = "Unknown"
    data["services"] = services

    try:
        data["active_sessions"] = frappe.db.count("Sessions")
        recent = frappe.get_all(
            "User",
            filters={"enabled": 1, "last_active": ("is", "set")},
            fields=["name", "last_active", "full_name"],
            order_by="last_active desc",
            limit=10,
        )
        data["recent_users"] = [
            {
                "name": u.name,
                "full_name": u.full_name or u.name,
                "last_active": str(u.last_active) if u.last_active else "",
            }
            for u in recent
        ]
    except Exception:
        data["active_sessions"] = 0
        data["recent_users"] = []

    try:
        threshold = frappe.utils.add_to_date(None, days=-1, as_datetime=True)
        data["errors_24h"] = frappe.db.count(
            "Error Log", {"creation": (">", threshold)}
        )
        errs = frappe.get_all(
            "Error Log",
            filters={"creation": (">", threshold)},
            fields=["method", "error", "creation"],
            order_by="creation desc",
            limit=10,
        )
        data["recent_errors"] = [
            {
                "method": e.method or "Unknown",
                "error": (e.error or "")[:200],
                "time": str(e.creation) if e.creation else "",
            }
            for e in errs
        ]
    except Exception:
        data["errors_24h"] = 0
        data["recent_errors"] = []

    data["timestamp"] = time.time()
    return data


def _push_history(data):
    try:
        raw = frappe.cache.get_value(HISTORY_KEY)
        history = json.loads(raw) if isinstance(raw, str) else (raw or [])
        history.append(data)
        if len(history) > HISTORY_LENGTH:
            history = history[-HISTORY_LENGTH:]
        frappe.cache.set_value(HISTORY_KEY, json.dumps(history), expires_in=600)
    except Exception:
        pass


def _get_history():
    try:
        raw = frappe.cache.get_value(HISTORY_KEY)
        if raw is None:
            return []
        if isinstance(raw, str):
            return json.loads(raw)
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


def _build_history_out(history):
    out = {}
    keys = [
        "cpu_total", "ram_percent", "disk_percent", "load_1",
        "disk_read_bytes", "disk_write_bytes", "net_sent", "net_recv",
        "cpu_iowait", "processes_total", "swap_percent",
        "mdb_threads_connected", "mdb_threads_running",
        "mdb_queries_ps", "mdb_slow_queries_ps",
        "mdb_bytes_in_ps", "mdb_bytes_out_ps",
        "mdb_com_select_ps", "mdb_com_insert_ps", "mdb_com_update_ps", "mdb_com_delete_ps",
        "mdb_innodb_buffer_pool_hit_pct", "mdb_qcache_hit_pct",
        "mdb_created_tmp_disk_tables_ps", "mdb_table_locks_waited_ps",
        "mdb_innodb_rows_read_ps", "mdb_innodb_row_lock_waits_ps",
    ]
    for key in keys:
        out[key] = [h.get(key, 0) for h in history if key in h]
    return out


def _has_access():
    return True


@frappe.whitelist(allow_guest=True)
def server_status(**kwargs):
    if not _has_access():
        frappe.throw("Insufficient permissions", frappe.DoesNotExistError)
    metrics = collect_metrics()
    metrics["slow_queries"] = _get_slow_queries_data()
    metrics["background_jobs"] = _get_background_jobs_data()
    metrics["top_processes"] = _get_top_processes_data()
    _push_history(metrics)
    history = _get_history()
    history_out = _build_history_out(history)
    result = {"current": metrics, "history": history_out}

    try:
        frappe.publish_realtime(
            "system_monitor_data",
            result,
            room="all",
            after_commit=False,
        )
    except Exception:
        pass

    return result


def _get_slow_queries_data():
    queries = []
    try:
        db_name = frappe.conf.db_name
        rows = frappe.db.sql(
            """SELECT id, user, host, db, command, time, state, info
               FROM information_schema.processlist
               WHERE command != 'Sleep'
                 AND time > 0
                 AND db = %s
               ORDER BY time DESC
               LIMIT 30""",
            (db_name,),
            as_dict=True,
        )
        for r in rows:
            queries.append({
                "id": r.id,
                "user": r.user,
                "host": r.host,
                "db": r.db or "",
                "command": r.command,
                "time": r.time,
                "state": r.state or "",
                "query": (r.info or "")[:2000],
            })
    except Exception:
        pass
    return queries


def _get_background_jobs_data():
    jobs = []
    try:
        from frappe.utils.background_jobs import get_queue_list, get_queue, get_queues

        for qname in get_queue_list():
            try:
                q = get_queue(qname)
                for job_id in q.job_ids[:30]:
                    try:
                        job = q.fetch_job(job_id)
                        if job and getattr(job, "is_started", False):
                            jobs.append({
                                "id": job.id,
                                "queue": qname,
                                "method": getattr(job, "func_name", "unknown"),
                                "status": "started",
                                "started_at": str(getattr(job, "started_at", "")),
                            })
                    except Exception:
                        pass
            except Exception:
                pass

        for q in get_queues():
            try:
                reg = q.failed_job_registry
                for job_id in (reg.get_job_ids() or [])[:10]:
                    try:
                        job = q.fetch_job(job_id)
                        if job:
                            jobs.append({
                                "id": job.id,
                                "queue": q.name,
                                "method": getattr(job, "func_name", "unknown"),
                                "status": "failed",
                                "ended_at": str(getattr(job, "ended_at", "")),
                                "exc_info": str(getattr(job, "exc_info", ""))[:500],
                            })
                    except Exception:
                        pass
            except Exception:
                pass

        for qname in get_queue_list():
            try:
                q = get_queue(qname)
                for job_id in q.job_ids[:20]:
                    try:
                        job = q.fetch_job(job_id)
                        if job and not getattr(job, "is_started", False) and not getattr(job, "is_failed", False):
                            jobs.append({
                                "id": job.id,
                                "queue": qname,
                                "method": getattr(job, "func_name", "unknown"),
                                "status": "queued",
                                "enqueued_at": str(getattr(job, "enqueued_at", "")),
                            })
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass
    return jobs


def _get_process_detail(p):
    try:
        import os
        cmd = p.info.get('cmdline')
        if not cmd or len(cmd) <= 1:
            return ""
        
        exe_name = os.path.basename(cmd[0])
        if exe_name in ('python', 'python3', 'node', 'sh', 'bash', 'sudo'):
            for arg in cmd[1:]:
                if not arg.startswith('-'):
                    basename = os.path.basename(arg)
                    if basename:
                        return basename
        else:
            for arg in cmd[1:]:
                if not arg.startswith('-'):
                    basename = os.path.basename(arg)
                    if basename:
                        return basename
    except Exception:
        pass
    return ""


def _get_top_processes_data():
    out = []
    try:
        import psutil
        for p in psutil.process_iter(['pid', 'name', 'username', 'memory_info', 'io_counters', 'cpu_percent', 'cmdline', 'memory_percent']):
            try:
                mem_info = p.info.get('memory_info')
                rss = mem_info.rss if mem_info else 0
                cpu = p.info.get('cpu_percent') or 0.0
                mem_pct = p.info.get('memory_percent') or 0.0
                io = p.info.get('io_counters')
                read_bytes = io.read_bytes if io else 0
                write_bytes = io.write_bytes if io else 0
                
                out.append({
                    "pid": p.info['pid'],
                    "name": p.info['name'] or "unknown",
                    "user": p.info['username'] or "system",
                    "cpu": round(cpu, 1),
                    "mem": rss,
                    "mem_pct": round(mem_pct, 1),
                    "disk_read": read_bytes,
                    "disk_write": write_bytes,
                    "detail": _get_process_detail(p),
                    "cmdline": " ".join(p.info['cmdline']) if p.info.get('cmdline') else ""
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    except Exception:
        pass
    
    top_cpu = sorted(out, key=lambda x: x['cpu'], reverse=True)[:10]
    top_mem = sorted(out, key=lambda x: x['mem'], reverse=True)[:10]
    top_disk = sorted(out, key=lambda x: x['disk_read'] + x['disk_write'], reverse=True)[:10]
    
    return {
        "cpu": top_cpu,
        "mem": top_mem,
        "disk": top_disk
    }


@frappe.whitelist(allow_guest=True)
def get_background_jobs(**kwargs):
    if not _has_access():
        frappe.throw("Insufficient permissions", frappe.DoesNotExistError)
    return _get_background_jobs_data()


@frappe.whitelist(allow_guest=True)
def get_sitename():
    import frappe
    port = frappe.conf.get("websocket_port") or 9000
    return {
        "site_name": frappe.local.site,
        "socketio_port": port
    }


@frappe.whitelist(allow_guest=True)
def get_slow_queries(**kwargs):
    if not _has_access():
        frappe.throw("Insufficient permissions", frappe.DoesNotExistError)

    queries = []
    try:
        db_name = frappe.conf.db_name
        rows = frappe.db.sql(
            """SELECT id, user, host, db, command, time, state, info
               FROM information_schema.processlist
               WHERE command != 'Sleep'
                 AND time > 0
                 AND db = %s
               ORDER BY time DESC
               LIMIT 30""",
            (db_name,),
            as_dict=True,
        )
        for r in rows:
            queries.append({
                "id": r.id,
                "user": r.user,
                "host": r.host,
                "db": r.db or "",
                "command": r.command,
                "time": r.time,
                "state": r.state or "",
                "query": (r.info or "")[:2000],
            })
    except Exception:
        pass

    return queries


def check_and_start_ws_server():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect(("127.0.0.1", 8765))
        s.close()
        return
    except Exception:
        pass

    import sys
    import subprocess
    python_bin = sys.executable
    ws_server_path = frappe.get_app_path("frappe_system_monitor", "..", "ws_server.py")
    try:
        subprocess.Popen(
            [python_bin, ws_server_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )
    except Exception:
        pass
