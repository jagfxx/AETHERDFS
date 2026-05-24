from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3

from database import init_db, get_db_connection
from auth import get_password_hash, verify_password, create_access_token

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
