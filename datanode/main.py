from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Security
from fastapi.responses import FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import asynccontextmanager
import os
import asyncio
import logging
import hashlib
import jwt
import httpx

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("DataNode")

DATANODE_ID = os.environ["DATANODE_ID"]
NAMENODE_URL = os.environ["NAMENODE_URL"]
CLUSTER_TOKEN = os.environ["CLUSTER_TOKEN"]
JWT_SECRET = os.environ["JWT_SECRET"]
MY_HOST = os.environ.get("REPORTED_HOST", DATANODE_ID)
MY_PORT = int(os.environ.get("REPORTED_PORT", 8000))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
os.makedirs(DATA_DIR, exist_ok=True)

security = HTTPBearer()


def require_auth(creds: HTTPAuthorizationCredentials = Security(security)):
    tok = creds.credentials
    if tok == CLUSTER_TOKEN:
        return
    try:
        jwt.decode(tok, JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(401, "Token inválido")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(f"DataNode {DATANODE_ID} → {NAMENODE_URL}")
    task = asyncio.create_task(heartbeat_loop())
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(title="DFS DataNode", lifespan=lifespan)


async def heartbeat_loop():
    hdrs = {"Authorization": f"Bearer {CLUSTER_TOKEN}"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                st = os.statvfs(DATA_DIR)
                payload = {
                    "datanode_id": DATANODE_ID, "ip_address": MY_HOST, "port": MY_PORT,
                    "free_bytes": st.f_bavail * st.f_frsize,
                }
                r = await client.post(f"{NAMENODE_URL}/datanodes/heartbeat", json=payload, headers=hdrs)
                if r.status_code == 200:
                    for rep in r.json().get("replicate_blocks", []):
                        asyncio.create_task(replicate_block(rep["block_id"], rep["source_url"]))
            except Exception as e:
                log.error(f"heartbeat fallo: {e}")
            await asyncio.sleep(5)


async def replicate_block(block_id: str, source_url: str):
    path = os.path.join(DATA_DIR, block_id)
    md5_path = path + ".md5"
    md5 = hashlib.md5()
    hdrs = {"Authorization": f"Bearer {CLUSTER_TOKEN}"}
    try:
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", source_url, headers=hdrs) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    async for chunk in r.aiter_bytes(65536):
                        md5.update(chunk)
                        f.write(chunk)
            with open(md5_path, "w") as f:
                f.write(md5.hexdigest())
            await client.post(
                f"{NAMENODE_URL}/blocks/{block_id}/locations",
                json={"datanode_id": DATANODE_ID}, headers=hdrs,
            )
        log.info(f"Réplica creada: {block_id}")
    except Exception as e:
        for p in (path, md5_path):
            if os.path.exists(p):
                os.remove(p)
        log.error(f"replicación fallida {block_id}: {e}")


@app.get("/health")
def health():
    return {"status": "ok", "datanode_id": DATANODE_ID}


@app.post("/blocks/{block_id}")
async def upload_block(block_id: str, file: UploadFile = File(...), _=Depends(require_auth)):
    path = os.path.join(DATA_DIR, block_id)
    md5_path = path + ".md5"
    md5 = hashlib.md5()
    try:
        with open(path, "wb") as f:
            while True:
                chunk = await file.read(65536)
                if not chunk:
                    break
                md5.update(chunk)
                f.write(chunk)
        with open(md5_path, "w") as f:
            f.write(md5.hexdigest())
        log.info(f"Bloque guardado: {block_id}")
        return {"checksum": md5.hexdigest()}
    except Exception as e:
        for p in (path, md5_path):
            if os.path.exists(p):
                os.remove(p)
        log.error(f"upload fallido {block_id}: {e}")
        raise HTTPException(500, "Error al guardar bloque")


@app.get("/blocks/{block_id}")
def download_block(block_id: str, _=Depends(require_auth)):
    path = os.path.join(DATA_DIR, block_id)
    if not os.path.exists(path):
        raise HTTPException(404, "Bloque no encontrado")
    return FileResponse(path, media_type="application/octet-stream", filename=block_id)
