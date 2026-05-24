from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import sqlite3
import os
from typing import List, Optional, Dict, Any

from database import init_db, get_db_connection
from auth import get_password_hash, verify_password, create_access_token, get_current_user_id

app = FastAPI(title="DFS NameNode")

@app.on_event("startup")
def startup_event():
    init_db()

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
>>>>>>> 865f722 (feat(namenode): operaciones de directorios virtuales)
