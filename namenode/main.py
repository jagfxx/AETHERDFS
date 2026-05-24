from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import sqlite3
import uuid
import random
import os
import asyncio
from typing import List, Optional, Dict, Any

from database import init_db, get_db_connection
from auth import get_password_hash, verify_password, create_access_token, get_current_user_id

app = FastAPI(title="DFS NameNode")

BLOCK_SIZE_MB = int(os.environ.get("BLOCK_SIZE_MB", 64))
BLOCK_SIZE_BYTES = BLOCK_SIZE_MB * 1024 * 1024

@app.on_event("startup")
def startup_event():
    init_db()
    asyncio.create_task(self_healing_loop())

async def self_healing_loop():
    while True:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT b.block_id, COUNT(bl.datanode_id) as replicas 
                    FROM blocks b 
                    LEFT JOIN block_locations bl ON b.block_id = bl.block_id 
                    LEFT JOIN datanodes d ON bl.datanode_id = d.datanode_id AND d.status='ACTIVE' AND (strftime('%s', 'now') - strftime('%s', d.last_heartbeat)) < 30
                    GROUP BY b.block_id 
                    HAVING replicas < 2
                """)
                under_replicated = cursor.fetchall()
                
                for row in under_replicated:
                    block_id = row["block_id"]
                    cursor.execute("SELECT d.datanode_id, d.ip_address, d.port FROM block_locations bl JOIN datanodes d ON bl.datanode_id = d.datanode_id WHERE bl.block_id=? AND d.status='ACTIVE'", (block_id,))
                    source = cursor.fetchone()
                    if not source: continue
                        
                    cursor.execute("SELECT datanode_id FROM datanodes WHERE status='ACTIVE' AND (strftime('%s', 'now') - strftime('%s', last_heartbeat)) < 30 AND datanode_id NOT IN (SELECT datanode_id FROM block_locations WHERE block_id=?)", (block_id,))
                    target = cursor.fetchone()
                    if target:
                        cursor.execute("CREATE TABLE IF NOT EXISTS replicate_cmds (block_id TEXT, source_url TEXT, target_datanode TEXT, PRIMARY KEY(block_id, target_datanode))")
                        source_url = f"http://{source['ip_address']}:{source['port']}/blocks/{block_id}"
                        cursor.execute("INSERT OR IGNORE INTO replicate_cmds (block_id, source_url, target_datanode) VALUES (?, ?, ?)", (block_id, source_url, target["datanode_id"]))
                conn.commit()
        except Exception as e:
            print(f"Error en self healing: {e}")
        await asyncio.sleep(30)

@app.get("/health")
def health_check():
    return {"status": "ok"}

# ==================== AUTHENTICATION ====================
class UserCreate(BaseModel):
    username: str
    password: str

@app.post("/register")
def register(user: UserCreate):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO users (username, password_hash) VALUES (?, ?)",
                (user.username, get_password_hash(user.password))
            )
            conn.commit()
            return {"message": "User registered successfully"}
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Username already exists")

@app.post("/login")
def login(user: UserCreate):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, password_hash FROM users WHERE username = ?", (user.username,))
        row = cursor.fetchone()
        if not row or not verify_password(user.password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        access_token = create_access_token(data={"sub": row["id"]})
        return {"access_token": access_token, "token_type": "bearer"}

# ==================== DIRECTORY MANAGEMENT ====================
class PathReq(BaseModel):
    path: str

def get_parent_dir_id(cursor, user_id: int, path: str):
    """Resuelve el ID del directorio padre. Retorna None si es la raíz."""
    if path == "/" or path == "":
        return None
    
    parts = [p for p in path.split("/") if p]
    current_parent_id = None
    
    for part in parts:
        if current_parent_id is None:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id IS NULL AND name=? AND is_dir=1", (user_id, part))
        else:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id=? AND name=? AND is_dir=1", (user_id, current_parent_id, part))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Directorio no encontrado: {part}")
        current_parent_id = row["id"]
    return current_parent_id

@app.post("/mkdir")
def make_directory(req: PathReq, user_id: int = Depends(get_current_user_id)):
    if req.path == "/" or req.path == "":
        raise HTTPException(status_code=400, detail="Ruta inválida")
    
    parts = [p for p in req.path.split("/") if p]
    dir_name = parts[-1]
    parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        parent_id = get_parent_dir_id(cursor, user_id, parent_path)
        
        try:
            query = "INSERT INTO files (user_id, parent_id, name, is_dir) VALUES (?, ?, ?, 1)"
            if parent_id is None:
                cursor.execute("INSERT INTO files (user_id, parent_id, name, is_dir) VALUES (?, NULL, ?, 1)", (user_id, dir_name))
            else:
                cursor.execute(query, (user_id, parent_id, dir_name))
            conn.commit()
            return {"message": "Directorio creado exitosamente"}
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="El archivo o directorio ya existe")

@app.post("/ls")
def list_directory(req: PathReq, user_id: int = Depends(get_current_user_id)):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        parent_id = get_parent_dir_id(cursor, user_id, req.path)
        
        if parent_id is None:
            cursor.execute("SELECT name, is_dir, size_bytes FROM files WHERE user_id=? AND parent_id IS NULL", (user_id,))
        else:
            cursor.execute("SELECT name, is_dir, size_bytes FROM files WHERE user_id=? AND parent_id=?", (user_id, parent_id))
        
        return [{"name": r["name"], "is_dir": bool(r["is_dir"]), "size": r["size_bytes"]} for r in cursor.fetchall()]

@app.post("/rmdir")
def remove_directory(req: PathReq, user_id: int = Depends(get_current_user_id)):
    parts = [p for p in req.path.split("/") if p]
    if not parts:
        raise HTTPException(status_code=400, detail="Ruta inválida")
    
    dir_name = parts[-1]
    parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        parent_id = get_parent_dir_id(cursor, user_id, parent_path)
        
        if parent_id is None:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id IS NULL AND name=? AND is_dir=1", (user_id, dir_name))
        else:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id=? AND name=? AND is_dir=1", (user_id, parent_id, dir_name))
            
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Directorio no encontrado")
        
        target_dir_id = row["id"]
        
        # Verificar si está vacío
        cursor.execute("SELECT id FROM files WHERE parent_id=?", (target_dir_id,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El directorio no está vacío")
            
        cursor.execute("DELETE FROM files WHERE id=?", (target_dir_id,))
        conn.commit()
        return {"message": "Directorio eliminado exitosamente"}

# ==================== FILE ALLOCATION (PUT) ====================
class AllocateReq(BaseModel):
    path: str
    file_size: int

@app.post("/files/allocate")
def allocate_blocks(req: AllocateReq, user_id: int = Depends(get_current_user_id)):
    parts = [p for p in req.path.split("/") if p]
    if not parts:
        raise HTTPException(status_code=400, detail="Ruta inválida")
    
    file_name = parts[-1]
    parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        parent_id = get_parent_dir_id(cursor, user_id, parent_path)
        
        # Verificar DataNodes vivos (heartbeat < 30 seg)
        cursor.execute("SELECT datanode_id, ip_address, port FROM datanodes WHERE status='ACTIVE' AND (strftime('%s', 'now') - strftime('%s', last_heartbeat)) < 30")
        active_datanodes = cursor.fetchall()
        
        if len(active_datanodes) < 2:
            raise HTTPException(status_code=503, detail="No hay suficientes DataNodes disponibles (se requieren al menos 2)")
            
        # Crear entrada del archivo
        try:
            if parent_id is None:
                cursor.execute("INSERT INTO files (user_id, parent_id, name, is_dir, size_bytes) VALUES (?, NULL, ?, 0, ?)", (user_id, file_name, req.file_size))
            else:
                cursor.execute("INSERT INTO files (user_id, parent_id, name, is_dir, size_bytes) VALUES (?, ?, ?, 0, ?)", (user_id, parent_id, file_name, req.file_size))
            file_id = cursor.lastrowid
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="El archivo ya existe. Utilice 'rm' primero para sobrescribir.")
            
        # Calcular particiones
        import math
        num_blocks = math.ceil(req.file_size / BLOCK_SIZE_BYTES) if req.file_size > 0 else 1
        
        allocated_blocks = []
        for i in range(num_blocks):
            block_id = str(uuid.uuid4())
            block_size = BLOCK_SIZE_BYTES if (i < num_blocks - 1) else (req.file_size % BLOCK_SIZE_BYTES) or BLOCK_SIZE_BYTES
            
            cursor.execute("INSERT INTO blocks (file_id, block_index, block_id, size_bytes) VALUES (?, ?, ?, ?)", (file_id, i, block_id, block_size))
            
            # Asignar a 2 DataNodes distintos aleatoriamente
            selected_dns = random.sample(active_datanodes, 2)
            for dn in selected_dns:
                cursor.execute("INSERT INTO block_locations (block_id, datanode_id) VALUES (?, ?)", (block_id, dn["datanode_id"]))
                
            allocated_blocks.append({
                "block_id": block_id,
                "block_index": i,
                "primary": {"id": selected_dns[0]["datanode_id"], "host": selected_dns[0]["ip_address"], "port": selected_dns[0]["port"]},
                "replica": {"id": selected_dns[1]["datanode_id"], "host": selected_dns[1]["ip_address"], "port": selected_dns[1]["port"]},
            })
            
        conn.commit()
        return {"blocks": allocated_blocks}

# ==================== FILE DOWNLOAD (GET) ====================
@app.post("/files/locations")
def get_file_locations(req: PathReq, user_id: int = Depends(get_current_user_id)):
    parts = [p for p in req.path.split("/") if p]
    if not parts:
        raise HTTPException(status_code=400, detail="Ruta inválida")
    
    file_name = parts[-1]
    parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        parent_id = get_parent_dir_id(cursor, user_id, parent_path)
        
        if parent_id is None:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id IS NULL AND name=? AND is_dir=0", (user_id, file_name))
        else:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id=? AND name=? AND is_dir=0", (user_id, parent_id, file_name))
            
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
            
        file_id = row["id"]
        
        cursor.execute("SELECT block_id, block_index, checksum FROM blocks WHERE file_id=? ORDER BY block_index", (file_id,))
        blocks = cursor.fetchall()
        
        result = []
        for b in blocks:
            cursor.execute("""
                SELECT d.datanode_id, d.ip_address, d.port 
                FROM block_locations bl 
                JOIN datanodes d ON bl.datanode_id = d.datanode_id 
                WHERE bl.block_id=? AND d.status='ACTIVE' AND (strftime('%s', 'now') - strftime('%s', d.last_heartbeat)) < 30
            """, (b["block_id"],))
            locations = cursor.fetchall()
            result.append({
                "block_id": b["block_id"],
                "block_index": b["block_index"],
                "checksum": b["checksum"],
                "locations": [{"id": l["datanode_id"], "host": l["ip_address"], "port": l["port"]} for l in locations]
            })
            
        return {"blocks": result}

# ==================== FILE DELETE (RM) ====================
@app.post("/rm")
def remove_file(req: PathReq, user_id: int = Depends(get_current_user_id)):
    parts = [p for p in req.path.split("/") if p]
    if not parts:
        raise HTTPException(status_code=400, detail="Ruta inválida")
    
    file_name = parts[-1]
    parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        parent_id = get_parent_dir_id(cursor, user_id, parent_path)
        
        if parent_id is None:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id IS NULL AND name=? AND is_dir=0", (user_id, file_name))
        else:
            cursor.execute("SELECT id FROM files WHERE user_id=? AND parent_id=? AND name=? AND is_dir=0", (user_id, parent_id, file_name))
            
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
            
        file_id = row["id"]
        
        # Recuperar block_ids y datanodes antes de borrar (para notificar la recolección de basura después)
        cursor.execute("""
            SELECT b.block_id, bl.datanode_id 
            FROM blocks b 
            LEFT JOIN block_locations bl ON b.block_id = bl.block_id 
            WHERE b.file_id=?
        """, (file_id,))
        blocks_to_delete = cursor.fetchall()
        
        # Guardar en una tabla de garbage collection para que los datanodes lo recojan en el heartbeat
        cursor.execute("CREATE TABLE IF NOT EXISTS garbage (block_id TEXT, datanode_id TEXT, PRIMARY KEY(block_id, datanode_id))")
        for b in blocks_to_delete:
            if b["datanode_id"]:
                cursor.execute("INSERT OR IGNORE INTO garbage (block_id, datanode_id) VALUES (?, ?)", (b["block_id"], b["datanode_id"]))
                
        # Borrado en cascada (borra records en files, blocks y block_locations)
        cursor.execute("DELETE FROM files WHERE id=?", (file_id,))
        conn.commit()
        return {"message": "Archivo eliminado exitosamente. Los DataNodes borrarán los bloques físicos pronto."}

# ==================== DATANODE HEARTBEAT ====================
class BlockReport(BaseModel):
    id: str
    checksum: str

class HeartbeatReq(BaseModel):
    datanode_id: str
    ip_address: str
    port: int
    free_bytes: int
    blocks: List[BlockReport] = []

@app.post("/datanodes/heartbeat")
def heartbeat(req: HeartbeatReq):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO datanodes (datanode_id, ip_address, port, last_heartbeat, free_bytes, status)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, 'ACTIVE')
            ON CONFLICT(datanode_id) DO UPDATE SET 
            ip_address=excluded.ip_address,
            port=excluded.port,
            last_heartbeat=CURRENT_TIMESTAMP,
            free_bytes=excluded.free_bytes,
            status='ACTIVE'
        """, (req.datanode_id, req.ip_address, req.port, req.free_bytes))
        
        # Procesar Block Report
        for b in req.blocks:
            cursor.execute("INSERT OR IGNORE INTO block_locations (block_id, datanode_id) VALUES (?, ?)", (b.id, req.datanode_id))
            if b.checksum:
                cursor.execute("UPDATE blocks SET checksum=? WHERE block_id=? AND (checksum IS NULL OR checksum='')", (b.checksum, b.id))
                
        # Comandos de replicación
        cursor.execute("CREATE TABLE IF NOT EXISTS replicate_cmds (block_id TEXT, source_url TEXT, target_datanode TEXT, PRIMARY KEY(block_id, target_datanode))")
        cursor.execute("SELECT block_id, source_url FROM replicate_cmds WHERE target_datanode=?", (req.datanode_id,))
        rep_cmds = [{"block_id": r["block_id"], "source_url": r["source_url"]} for r in cursor.fetchall()]
        if rep_cmds:
            cursor.execute("DELETE FROM replicate_cmds WHERE target_datanode=?", (req.datanode_id,))
        
        # Chequear si este DataNode tiene comandos de borrado (Garbage Collection)
        cursor.execute("CREATE TABLE IF NOT EXISTS garbage (block_id TEXT, datanode_id TEXT, PRIMARY KEY(block_id, datanode_id))")
        cursor.execute("SELECT block_id FROM garbage WHERE datanode_id=?", (req.datanode_id,))
        garbage = [r["block_id"] for r in cursor.fetchall()]
        
        if garbage:
            cursor.execute("DELETE FROM garbage WHERE datanode_id=?", (req.datanode_id,))
            
        conn.commit()
        return {"status": "ok", "delete_blocks": garbage, "replicate_blocks": rep_cmds}
