#!/usr/bin/env python3
"""Standalone WebSocket server for System Monitor dashboard.

Collects OS + Frappe metrics and pushes to all connected clients every 0.9s.
Run alongside Frappe via Procfile:
    ws_monitor: python apps/frappe_system_monitor/ws_server.py
"""

import asyncio
import json
import os
import platform
import subprocess
import sys
import time
import datetime
import signal

import psutil

try:
    import websockets
except ImportError:
    print("Install websockets: pip install websockets")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from frappe_system_monitor.frappe_system_monitor.mariadb_stats import (
    MariaDBStatsCollector,
    VARIABLE_KEYS_NEEDED,
)

WS_PORT = int(os.environ.get("WS_MONITOR_PORT", 8765))
HISTORY_LENGTH = 300

_prev_net = None
_prev_net_time = None
_prev_disk_io = None
_prev_disk_io_time = None
_history = []
_connected = set()
_mariadb_collector = MariaDBStatsCollector()

# --- Frappe DB / Redis handles (lazy init) ---
_db = None
_redis = None
_redis_queue = None
_redis = None
_site_config = None


def _get_site_config():
    global _site_config
    if _site_config is not None:
        return _site_config
    bench_path = os.environ.get(
        "FRAPPE_BENCH_PATH",
        os.path.join(os.path.dirname(__file__), "..", ".."),
    )
    sites_path = os.path.join(bench_path, "sites")
    site_name = os.environ.get("FRAPPE_SITE", None)
    if not site_name:
        apps_txt = os.path.join(sites_path, "apps.txt")
        common_cfg = os.path.join(sites_path, "common_site_config.json")
        if os.path.exists(common_cfg):
            with open(common_cfg) as f:
                common = json.load(f)
            site_name = common.get("default_site")
        if not site_name and os.path.exists(apps_txt):
            with open(apps_txt) as f:
                pass
        if not site_name:
            for d in os.listdir(sites_path):
                if os.path.isfile(os.path.join(sites_path, d, "site_config.json")) and d not in (
                    "assets", "common_site_config.json", "apps.txt",
                ):
                    site_name = d
                    break
    if not site_name:
        return {}
    cfg_path = os.path.join(sites_path, site_name, "site_config.json")
    if os.path.exists(cfg_path):
        with open(cfg_path) as f:
            _site_config = json.load(f)
        _site_config["_site_name"] = site_name
        _site_config["_bench_path"] = bench_path
        return _site_config
    return {}


def _get_db():
    global _db
    if _db is not None:
        return _db
    if _db is False:
        return None
    cfg = _get_site_config()
    db_name = cfg.get("db_name")
    db_password = cfg.get("db_password")
    db_type = cfg.get("db_type", "mariadb")

    if not db_name:
        _db = False
        return None

    if db_type == "sqlite":
        import sqlite3
        db_path = os.path.join(
            cfg.get("_bench_path", ""), "sites", db_name
        )
        if not os.path.exists(db_path):
            db_path += ".db"
        _db = sqlite3.connect(db_path, check_same_thread=False)
        _db.row_factory = sqlite3.Row
        return _db

    try:
        import pymysql
        _db = pymysql.connect(
            host=cfg.get("db_host", "127.0.0.1"),
            port=int(cfg.get("db_port", 3306)),
            user=db_name,
            password=db_password,
            database=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=3,
        )
        return _db
    except Exception as e:
        print(f"[ws] MariaDB connection failed: {e}")
        _db = False
        return None


def _get_redis_queue():
    global _redis_queue
    if _redis_queue is not None:
        return _redis_queue
    try:
        import redis
        redis_port = 11000
        redis_conf = os.path.join(
            _get_site_config().get("_bench_path", ""), "config", "redis_queue.conf"
        )
        if os.path.exists(redis_conf):
            with open(redis_conf) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("port "):
                        redis_port = int(line.split()[1])
                        break
        _redis_queue = redis.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        _redis_queue.ping()
        return _redis_queue
    except Exception as e:
        print(f"[ws] Redis queue connection failed: {e}")
        _redis_queue = None
        return None


def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    try:
        import redis
        redis_port = 13000
        redis_conf = os.path.join(
            _get_site_config().get("_bench_path", ""), "config", "redis_cache.conf"
        )
        if os.path.exists(redis_conf):
            with open(redis_conf) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("port "):
                        redis_port = int(line.split()[1])
                        break
        _redis = redis.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        _redis.ping()
        return _redis
    except Exception as e:
        print(f"[ws] Redis connection failed: {e}")
        return None


def _query_db(sql, params=None):
    db = _get_db()
    if db is None:
        return []
    try:
        cfg = _get_site_config()
        if cfg.get("db_type") == "sqlite":
            cur = db.execute(sql, params or [])
            return [dict(row) for row in cur.fetchall()]
        else:
            with db.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
    except Exception:
        return []


# --- System metrics ---

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


def _collect_services():
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
            r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3)
            services[name] = "Running" if r.returncode == 0 else "Stopped"
        except Exception:
            services[name] = "Unknown"
    return services


def _collect_mariadb():
    status_rows = _query_db("SHOW GLOBAL STATUS")
    status = {r.get("Variable_name"): r.get("Value") for r in status_rows if r.get("Variable_name")}

    placeholders = ", ".join(["%s"] * len(VARIABLE_KEYS_NEEDED))
    var_rows = _query_db(
        f"SHOW GLOBAL VARIABLES WHERE Variable_name IN ({placeholders})",
        tuple(VARIABLE_KEYS_NEEDED),
    )
    variables = {r.get("Variable_name"): r.get("Value") for r in var_rows if r.get("Variable_name")}

    slave_rows = _query_db("SHOW SLAVE STATUS")
    slave_status = slave_rows[0] if slave_rows else None

    own_rows = _query_db("SELECT CONNECTION_ID() AS id")
    own_id = own_rows[0].get("id") if own_rows else None

    processlist = _query_db("SHOW FULL PROCESSLIST") or None

    return _mariadb_collector.collect(
        status, variables, slave_status, processlist, own_connection_id=own_id
    )


def _collect_erpnext():
    out = {
        "erpnext_scheduler": "Unknown",
        "workers_active": 0, "workers_total": 0,
        "erpnext_queued": 0, "erpnext_failed": 0, "emails_pending": 0,
    }
    rows = _query_db("SELECT enabled FROM `tabSystem Settings`")
    if rows:
        enabled = rows[0].get("enabled") or rows[0].get(1) or 0
        out["erpnext_scheduler"] = "Running" if enabled else "Inactive"
    rows = _query_db("SELECT status, COUNT(*) as cnt FROM `tabRQ Job` GROUP BY status")
    for r in rows:
        status = r.get("status") or r.get(0, "")
        cnt = r.get("cnt") or r.get(1, 0)
        if status == "queued":
            out["erpnext_queued"] = int(cnt)
        elif status == "failed":
            out["erpnext_failed"] = int(cnt)
    rows = _query_db("SELECT COUNT(*) as cnt FROM `tabEmail Queue` WHERE status='Queued'")
    if rows:
        out["emails_pending"] = int(rows[0].get("cnt") or rows[0].get(0, 0))
    return out


def _collect_redis_metrics():
    out = {"redis_memory": "N/A", "redis_connections": 0, "redis_keys": 0}
    r = _get_redis()
    if r is None:
        return out
    try:
        info = r.info("memory")
        out["redis_memory"] = info.get("used_memory_human", "N/A")
    except Exception:
        pass
    try:
        info = r.info("clients")
        out["redis_connections"] = info.get("connected_clients", 0)
    except Exception:
        pass
    try:
        out["redis_keys"] = r.dbsize()
    except Exception:
        pass
    return out


def _collect_users():
    out = {"active_sessions": 0, "recent_users": []}
    rows = _query_db("SELECT COUNT(*) as cnt FROM `tabSessions`")
    if rows:
        out["active_sessions"] = int(rows[0].get("cnt") or rows[0].get(0, 0))
    rows = _query_db(
        "SELECT name, full_name, last_active FROM `tabUser` "
        "WHERE enabled=1 AND last_active IS NOT NULL "
        "ORDER BY last_active DESC LIMIT 10"
    )
    for r in rows:
        name = r.get("name", "")
        out["recent_users"].append({
            "name": name,
            "full_name": r.get("full_name") or name,
            "last_active": str(r.get("last_active") or ""),
        })
    return out


def _collect_errors():
    out = {"errors_24h": 0, "recent_errors": []}
    threshold = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    rows = _query_db(
        "SELECT COUNT(*) as cnt FROM `tabError Log` WHERE creation > %s", (threshold,)
    )
    if rows:
        out["errors_24h"] = int(rows[0].get("cnt") or rows[0].get(0, 0))
    rows = _query_db(
        "SELECT method, error, creation FROM `tabError Log` "
        "WHERE creation > %s ORDER BY creation DESC LIMIT 10", (threshold,)
    )
    for r in rows:
        out["recent_errors"].append({
            "method": r.get("method") or "Unknown",
            "error": (r.get("error") or "")[:200],
            "time": str(r.get("creation") or ""),
        })
    return out


def _collect_slow_queries():
    out = []
    try:
        db_name = _get_site_config().get("db_name", "")
        rows = _query_db(
            "SELECT id, user, host, db, command, time, state, info "
            "FROM information_schema.processlist "
            "WHERE command != 'Sleep' AND time > 0 AND db = %s "
            "ORDER BY time DESC LIMIT 30",
            (db_name,),
        )
        for r in rows:
            out.append({
                "id": r.get("id"),
                "user": r.get("user", ""),
                "host": r.get("host", ""),
                "db": r.get("db") or "",
                "command": r.get("command", ""),
                "time": r.get("time", 0),
                "state": r.get("state") or "",
                "query": (r.get("info") or "")[:2000],
            })
    except Exception:
        pass
    return out


def _collect_background_jobs():
    out = []
    try:
        rows = _query_db(
            "SELECT id, queue_name, job_name, status, creation, modified, "
            "start_time, end_time, exc_type, exc_info "
            "FROM `tabRQ Job` "
            "WHERE status IN ('started', 'queued', 'failed') "
            "ORDER BY modified DESC LIMIT 50"
        )
        for r in rows:
            status = r.get("status", "")
            job = {
                "id": r.get("id", ""),
                "queue": r.get("queue_name", ""),
                "method": r.get("job_name", ""),
                "status": status,
            }
            if status == "started":
                job["started_at"] = str(r.get("start_time") or "")
            elif status == "queued":
                job["enqueued_at"] = str(r.get("creation") or "")
            elif status == "failed":
                job["ended_at"] = str(r.get("end_time") or "")
                job["exc_info"] = (r.get("exc_info") or "")[:500]
            out.append(job)
    except Exception:
        pass
    return out

def _get_process_detail(p):
    try:
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


def _collect_top_processes():
    out = []
    try:
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
    data["boot_time"] = datetime.datetime.fromtimestamp(psutil.boot_time()).strftime("%Y-%m-%d %H:%M:%S")
    data["platform"] = platform.system()
    data["platform_release"] = platform.release()

    data["services"] = _collect_services()
    data.update(_collect_mariadb())
    data.update(_collect_erpnext())
    data.update(_collect_redis_metrics())
    data.update(_collect_users())
    data.update(_collect_errors())
    data["slow_queries"] = _collect_slow_queries()
    data["background_jobs"] = _collect_background_jobs()
    data["top_processes"] = _collect_top_processes()

    data["timestamp"] = time.time()
    return data


def _push_history(data):
    global _history
    _history.append(data)
    if len(_history) > HISTORY_LENGTH:
        _history = _history[-HISTORY_LENGTH:]

    # Also write to Frappe Redis cache so HTTP fallback has access to history
    try:
        r = _get_redis()
        if r is not None:
            cfg = _get_site_config()
            db_name = cfg.get("db_name")
            if db_name:
                key = f"{db_name}|system_monitor:history"
                r.setex(key, 600, json.dumps(_history))
    except Exception as e:
        print(f"[ws] Failed to write history to Redis cache: {e}")


def _build_history_out():
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
    out = {}
    for key in keys:
        out[key] = [h.get(key, 0) for h in _history if key in h]
    return out


async def handler(websocket):
    _connected.add(websocket)
    client_id = id(websocket)
    print(f"[ws] Client connected: {client_id} (total: {len(_connected)})")
    try:
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        _connected.discard(websocket)
        print(f"[ws] Client disconnected: {client_id} (total: {len(_connected)})")


async def broadcast():
    global _history
    collect_metrics()
    while True:
        try:
            metrics = collect_metrics()
            _push_history(metrics)
            payload = json.dumps(
                {"current": metrics, "history": _build_history_out()}
            )

            # Publish to Frappe Socket.IO via Redis pub/sub
            try:
                r = _get_redis_queue()
                if r is not None:
                    site_name = _get_site_config().get("_site_name", "")
                    r.publish("events", json.dumps({
                        "event": "system_monitor_data",
                        "message": json.loads(payload),
                        "room": "all",
                        "namespace": site_name,
                    }))
            except Exception:
                pass

            # Broadcast to standalone WebSocket clients
            if _connected:
                await asyncio.gather(
                    *[c.send(payload) for c in _connected.copy()],
                    return_exceptions=True,
                )
        except Exception as e:
            print(f"[ws] Broadcast error: {e}")
        await asyncio.sleep(0.9)


async def main():
    print("[ws] System Monitor WebSocket daemon starting...")
    print("[ws] Publishing telemetry securely via Redis to Frappe Socket.IO")
    await broadcast()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    asyncio.run(main())
