import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", "/app/data/dfs.db")

def init_db():
    """Initializes the database schema."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Habilitar el modo WAL para mejor concurrencia
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Tabla de usuarios
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )
        """)

        # Tabla de archivos y directorios (modelo jerárquico)
        # Si parent_id es NULL, está en la raíz del usuario
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            parent_id INTEGER,
            name TEXT NOT NULL,
            is_dir BOOLEAN NOT NULL DEFAULT 0,
            size_bytes INTEGER DEFAULT 0,
            status TEXT DEFAULT 'READY',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(parent_id) REFERENCES files(id) ON DELETE CASCADE,
            UNIQUE(user_id, parent_id, name)
        )
        """)

        # Tabla de bloques (solo para archivos, is_dir=0)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            block_index INTEGER NOT NULL,
            block_id TEXT UNIQUE NOT NULL,
            size_bytes INTEGER NOT NULL,
            checksum TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        )
        """)

        # Ubicación física de cada bloque en los DataNodes
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS block_locations (
            block_id TEXT NOT NULL,
            datanode_id TEXT NOT NULL,
            PRIMARY KEY(block_id, datanode_id),
            FOREIGN KEY(block_id) REFERENCES blocks(block_id) ON DELETE CASCADE
        )
        """)

        # Registro de DataNodes (Heartbeats)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS datanodes (
            datanode_id TEXT PRIMARY KEY,
            ip_address TEXT NOT NULL,
            port INTEGER NOT NULL,
            last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            free_bytes INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ACTIVE'
        )
        """)

        conn.commit()

@contextmanager
def get_db_connection():
    """Context manager para conexiones a SQLite."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
