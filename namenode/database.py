import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", "/app/data/dfs.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL
);
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
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_files_root
    ON files(user_id, name) WHERE parent_id IS NULL;
CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    block_index INTEGER NOT NULL,
    block_id TEXT UNIQUE NOT NULL,
    size_bytes INTEGER NOT NULL,
    checksum TEXT,
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS block_locations (
    block_id TEXT NOT NULL,
    datanode_id TEXT NOT NULL,
    PRIMARY KEY(block_id, datanode_id),
    FOREIGN KEY(block_id) REFERENCES blocks(block_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS datanodes (
    datanode_id TEXT PRIMARY KEY,
    ip_address TEXT NOT NULL,
    port INTEGER NOT NULL,
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    free_bytes INTEGER DEFAULT 0,
    status TEXT DEFAULT 'ACTIVE'
);
CREATE TABLE IF NOT EXISTS replicate_cmds (
    block_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    target_datanode TEXT NOT NULL,
    PRIMARY KEY(block_id, target_datanode)
);
"""

def init_db():
    with get_db_connection() as conn:
        conn.executescript("PRAGMA journal_mode=WAL;\n" + SCHEMA)
        conn.commit()

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()
