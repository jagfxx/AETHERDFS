import os
import jwt
from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

SECRET_KEY = os.environ["JWT_SECRET"]
CLUSTER_TOKEN = os.environ["CLUSTER_TOKEN"]
ALGORITHM = "HS256"
TOKEN_TTL_MIN = 60 * 24

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict):
    payload = {**data, "exp": datetime.now(timezone.utc) + timedelta(minutes=TOKEN_TTL_MIN)}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user_id(credentials: HTTPAuthorizationCredentials = Security(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.PyJWTError:
        raise HTTPException(401, "Token inválido o expirado")
    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(401, "Token inválido")
    return user_id

def verify_cluster_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.credentials != CLUSTER_TOKEN:
        raise HTTPException(401, "Token de clúster inválido")
