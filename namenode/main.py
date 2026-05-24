from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
import sqlite3
import uuid
import os
import math
import asyncio

from database import init_db, get_db_connection
from auth import (
    get_password_hash, verify_password, create_access_token,
    get_current_user_id, verify_cluster_token,
)

BLOCK_SIZE_BYTES = int(os.environ.get("BLOCK_SIZE_MB", 64)) * 1024 * 1024
MAX_FILE_BYTES = int(os.environ.get("MAX_FILE_BYTES", 500 * 1024**3))
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", 2))
HEARTBEAT_TIMEOUT_S = 30
LIVE_DN = (
    "status='ACTIVE' AND "
    f"(strftime('%s','now') - strftime('%s', last_heartbeat)) < {HEARTBEAT_TIMEOUT_S}"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(self_healing_loop())
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(title="DFS NameNode", lifespan=lifespan)


async def self_healing_loop():
    while True:
        try:
            await asyncio.to_thread(_run_self_healing)
        except Exception as e:
            print(f"self_healing error: {e}")
        await asyncio.sleep(30)


def _run_self_healing():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT b.block_id FROM blocks b
            JOIN files f ON b.file_id = f.id AND f.status = 'READY'
            LEFT JOIN block_locations bl ON b.block_id = bl.block_id
            LEFT JOIN datanodes d ON bl.datanode_id = d.datanode_id AND d.{LIVE_DN}
            GROUP BY b.block_id HAVING COUNT(d.datanode_id) < ?
        """, (REPLICATION_FACTOR,))
        for (block_id,) in cur.fetchall():
            src = cur.execute(f"""
                SELECT d.datanode_id FROM block_locations bl
                JOIN datanodes d ON bl.datanode_id = d.datanode_id
                WHERE bl.block_id=? AND d.{LIVE_DN} LIMIT 1
            """, (block_id,)).fetchone()
            tgt = cur.execute(f"""
                SELECT datanode_id FROM datanodes WHERE {LIVE_DN}
                  AND datanode_id NOT IN (SELECT datanode_id FROM block_locations WHERE block_id=?)
                LIMIT 1
            """, (block_id,)).fetchone()
            if src and tgt:
                # datanode_id es el hostname Docker interno; puerto interno 8000
                cur.execute(
                    "INSERT OR IGNORE INTO replicate_cmds (block_id, source_url, target_datanode) VALUES (?, ?, ?)",
                    (block_id, f"http://{src['datanode_id']}:8000/blocks/{block_id}", tgt["datanode_id"]),
                )
        conn.commit()


@app.get("/health")
def health():
    return {"status": "ok"}


class UserCreate(BaseModel):
    username: str
    password: str


@app.post("/register")
def register(user: UserCreate):
    with get_db_connection() as conn:
        try:
            conn.execute(
                "INSERT INTO users (username, password_hash) VALUES (?, ?)",
                (user.username, get_password_hash(user.password)),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(400, "Username already exists")
    return {"message": "User registered"}


@app.post("/login")
def login(user: UserCreate):
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT id, password_hash FROM users WHERE username=?", (user.username,)
        ).fetchone()
    if not row or not verify_password(user.password, row["password_hash"]):
        raise HTTPException(401, "Invalid credentials")
    return {"access_token": create_access_token({"sub": row["id"]}), "token_type": "bearer"}


class PathReq(BaseModel):
    path: str


class AllocateReq(PathReq):
    file_size: int


class CommitReq(PathReq):
    checksums: dict = {}


def _parts(path: str):
    return [p for p in path.split("/") if p and p not in (".", "..")]


def _resolve_parent(cur, user_id: int, path: str):
    parent_id = None
    for part in _parts(path):
        row = cur.execute(
            "SELECT id FROM files WHERE user_id=? AND parent_id IS ? AND name=? AND is_dir=1",
            (user_id, parent_id, part),
        ).fetchone()
        if not row:
            raise HTTPException(404, f"Directorio no encontrado: {part}")
        parent_id = row["id"]
    return parent_id


def _split_path(path: str):
    parts = _parts(path)
    if not parts:
        raise HTTPException(400, "Ruta inválida")
    return parts[-1], "/" + "/".join(parts[:-1])


@app.post("/mkdir")
def mkdir(req: PathReq, user_id: int = Depends(get_current_user_id)):
    name, parent = _split_path(req.path)
    with get_db_connection() as conn:
        cur = conn.cursor()
        parent_id = _resolve_parent(cur, user_id, parent)
        try:
            cur.execute(
                "INSERT INTO files (user_id, parent_id, name, is_dir) VALUES (?, ?, ?, 1)",
                (user_id, parent_id, name),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(400, "El archivo o directorio ya existe")
    return {"message": "Directorio creado"}


@app.post("/ls")
def ls(req: PathReq, user_id: int = Depends(get_current_user_id)):
    with get_db_connection() as conn:
        cur = conn.cursor()
        parent_id = _resolve_parent(cur, user_id, req.path)
        rows = cur.execute(
            "SELECT name, is_dir, size_bytes FROM files WHERE user_id=? AND parent_id IS ? AND status='READY'",
            (user_id, parent_id),
        ).fetchall()
    return [{"name": r["name"], "is_dir": bool(r["is_dir"]), "size": r["size_bytes"]} for r in rows]


@app.post("/rmdir")
def rmdir(req: PathReq, user_id: int = Depends(get_current_user_id)):
    name, parent = _split_path(req.path)
    with get_db_connection() as conn:
        cur = conn.cursor()
        parent_id = _resolve_parent(cur, user_id, parent)
        row = cur.execute(
            "SELECT id FROM files WHERE user_id=? AND parent_id IS ? AND name=? AND is_dir=1",
            (user_id, parent_id, name),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Directorio no encontrado")
        if cur.execute("SELECT 1 FROM files WHERE parent_id=?", (row["id"],)).fetchone():
            raise HTTPException(400, "Directorio no vacío")
        cur.execute("DELETE FROM files WHERE id=?", (row["id"],))
        conn.commit()
    return {"message": "Directorio eliminado"}


@app.post("/files/allocate")
def allocate(req: AllocateReq, user_id: int = Depends(get_current_user_id)):
    if req.file_size < 0 or req.file_size > MAX_FILE_BYTES:
        raise HTTPException(400, "Tamaño inválido")
    name, parent = _split_path(req.path)
    with get_db_connection() as conn:
        cur = conn.cursor()
        parent_id = _resolve_parent(cur, user_id, parent)
        dns = cur.execute(
            f"SELECT datanode_id, ip_address, port FROM datanodes WHERE {LIVE_DN} ORDER BY free_bytes DESC"
        ).fetchall()
        if len(dns) < REPLICATION_FACTOR:
            raise HTTPException(503, f"Se requieren al menos {REPLICATION_FACTOR} DataNodes activos")
        try:
            cur.execute(
                "INSERT INTO files (user_id, parent_id, name, is_dir, size_bytes, status) "
                "VALUES (?, ?, ?, 0, ?, 'UPLOADING')",
                (user_id, parent_id, name, req.file_size),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(400, "El archivo ya existe. Use 'rm' para sobrescribir.")
        file_id = cur.lastrowid

        num_blocks = math.ceil(req.file_size / BLOCK_SIZE_BYTES) if req.file_size > 0 else 0
        allocated = []
        for i in range(num_blocks):
            bid = str(uuid.uuid4())
            size = min(BLOCK_SIZE_BYTES, req.file_size - i * BLOCK_SIZE_BYTES)
            cur.execute(
                "INSERT INTO blocks (file_id, block_index, block_id, size_bytes) VALUES (?, ?, ?, ?)",
                (file_id, i, bid, size),
            )
            chosen = [dns[(i + k) % len(dns)] for k in range(REPLICATION_FACTOR)]
            for d in chosen:
                cur.execute(
                    "INSERT INTO block_locations (block_id, datanode_id) VALUES (?, ?)",
                    (bid, d["datanode_id"]),
                )
            allocated.append({
                "block_id": bid, "block_index": i,
                "replicas": [{"id": d["datanode_id"], "host": d["ip_address"], "port": d["port"]} for d in chosen],
            })
        conn.commit()
    return {"blocks": allocated}


@app.post("/files/commit")
def commit_file(req: CommitReq, user_id: int = Depends(get_current_user_id)):
    name, parent = _split_path(req.path)
    with get_db_connection() as conn:
        cur = conn.cursor()
        parent_id = _resolve_parent(cur, user_id, parent)
        cur.execute(
            "UPDATE files SET status='READY' WHERE user_id=? AND parent_id IS ? "
            "AND name=? AND is_dir=0 AND status='UPLOADING'",
            (user_id, parent_id, name),
        )
        if cur.rowcount == 0:
            raise HTTPException(404, "Archivo no encontrado o ya confirmado")
        for bid, chk in req.checksums.items():
            cur.execute("UPDATE blocks SET checksum=? WHERE block_id=?", (chk, bid))
        conn.commit()
    return {"message": "Archivo confirmado"}


@app.post("/files/locations")
def locations(req: PathReq, user_id: int = Depends(get_current_user_id)):
    name, parent = _split_path(req.path)
    with get_db_connection() as conn:
        cur = conn.cursor()
        parent_id = _resolve_parent(cur, user_id, parent)
        row = cur.execute(
            "SELECT id FROM files WHERE user_id=? AND parent_id IS ? AND name=? AND is_dir=0 AND status='READY'",
            (user_id, parent_id, name),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Archivo no encontrado")
        blocks = cur.execute(
            "SELECT block_id, block_index, checksum FROM blocks WHERE file_id=? ORDER BY block_index",
            (row["id"],),
        ).fetchall()
        result = []
        for b in blocks:
            locs = cur.execute(f"""
                SELECT d.datanode_id, d.ip_address, d.port FROM block_locations bl
                JOIN datanodes d ON bl.datanode_id = d.datanode_id
                WHERE bl.block_id=? AND d.{LIVE_DN}
            """, (b["block_id"],)).fetchall()
            result.append({
                "block_id": b["block_id"], "block_index": b["block_index"], "checksum": b["checksum"],
                "locations": [{"id": l["datanode_id"], "host": l["ip_address"], "port": l["port"]} for l in locs],
            })
    return {"blocks": result}


@app.post("/rm")
def rm(req: PathReq, user_id: int = Depends(get_current_user_id)):
    name, parent = _split_path(req.path)
    with get_db_connection() as conn:
        cur = conn.cursor()
        parent_id = _resolve_parent(cur, user_id, parent)
        row = cur.execute(
            "SELECT id FROM files WHERE user_id=? AND parent_id IS ? AND name=? AND is_dir=0",
            (user_id, parent_id, name),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Archivo no encontrado")
        cur.execute("DELETE FROM files WHERE id=?", (row["id"],))
        conn.commit()
    return {"message": "Archivo eliminado"}


class HeartbeatReq(BaseModel):
    datanode_id: str
    ip_address: str
    port: int
    free_bytes: int


class LocationReq(BaseModel):
    datanode_id: str


@app.post("/blocks/{block_id}/locations")
def register_location(block_id: str, req: LocationReq, _=Depends(verify_cluster_token)):
    with get_db_connection() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO block_locations (block_id, datanode_id)
            SELECT ?, ? WHERE EXISTS (SELECT 1 FROM blocks WHERE block_id=?)
        """, (block_id, req.datanode_id, block_id))
        conn.commit()
    return {"status": "ok"}


@app.post("/datanodes/heartbeat")
def heartbeat(req: HeartbeatReq, _=Depends(verify_cluster_token)):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO datanodes (datanode_id, ip_address, port, last_heartbeat, free_bytes, status)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, 'ACTIVE')
            ON CONFLICT(datanode_id) DO UPDATE SET
                ip_address=excluded.ip_address, port=excluded.port,
                last_heartbeat=CURRENT_TIMESTAMP, free_bytes=excluded.free_bytes, status='ACTIVE'
        """, (req.datanode_id, req.ip_address, req.port, req.free_bytes))
        rep_cmds = [dict(r) for r in cur.execute(
            "SELECT block_id, source_url FROM replicate_cmds WHERE target_datanode=?",
            (req.datanode_id,),
        ).fetchall()]
        cur.execute("DELETE FROM replicate_cmds WHERE target_datanode=?", (req.datanode_id,))
        conn.commit()
    return {"status": "ok", "replicate_blocks": rep_cmds}
