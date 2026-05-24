from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import os
import shutil
import asyncio
import requests
import logging
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataNode")

app = FastAPI(title="DFS DataNode")

DATANODE_ID = os.environ.get("DATANODE_ID", "dn_dev")
NAMENODE_URL = os.environ.get("NAMENODE_URL", "http://localhost:8000")
DATA_DIR = "/app/data"

os.makedirs(DATA_DIR, exist_ok=True)

# Por defecto asumimos el nombre del contenedor en Docker Compose y puerto 8000 interno
# Pero permitimos variables de entorno para pruebas desde el host local
MY_HOST = os.environ.get("REPORTED_HOST", DATANODE_ID)
MY_PORT = int(os.environ.get("REPORTED_PORT", 8000))

@app.on_event("startup")
async def startup_event():
    logger.info(f"Iniciando DataNode {DATANODE_ID} - Reportando a {NAMENODE_URL}")
    asyncio.create_task(heartbeat_loop())

async def heartbeat_loop():
    while True:
        try:
            # Obtener espacio libre (muy básico para simular en Linux)
            st = os.statvfs(DATA_DIR)
            free_bytes = st.f_bavail * st.f_frsize
            
            # Generar Block Report
            blocks_report = []
            for f_name in os.listdir(DATA_DIR):
                if not f_name.endswith(".md5"):
                    md5_file = os.path.join(DATA_DIR, f_name + ".md5")
                    checksum = ""
                    if os.path.exists(md5_file):
                        with open(md5_file, "r") as f:
                            checksum = f.read().strip()
                    blocks_report.append({"id": f_name, "checksum": checksum})
            
            payload = {
                "datanode_id": DATANODE_ID,
                "ip_address": MY_HOST,
                "port": MY_PORT,
                "free_bytes": free_bytes,
                "blocks": blocks_report
            }
            
            # Bloqueamos un momento en requests, es un hack simple para no usar aiohttp
            # En un entorno real se usaría httpx asíncrono, pero requests sirve para DFS minimalista
            response = requests.post(f"{NAMENODE_URL}/datanodes/heartbeat", json=payload, timeout=2)
            if response.status_code == 200:
                data = response.json()
                if data.get("delete_blocks"):
                    for b_id in data["delete_blocks"]:
                        delete_local_block(b_id)
                if data.get("replicate_blocks"):
                    for rep in data["replicate_blocks"]:
                        asyncio.create_task(replicate_block(rep["block_id"], rep["source_url"]))
        except Exception as e:
            logger.error(f"Fallo al enviar heartbeat al NameNode: {e}")
            
        await asyncio.sleep(5)

def delete_local_block(block_id: str):
    file_path = os.path.join(DATA_DIR, block_id)
    md5_path = file_path + ".md5"
    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info(f"Bloque {block_id} eliminado (Garbage Collection)")
    if os.path.exists(md5_path):
        os.remove(md5_path)

async def replicate_block(block_id: str, source_url: str):
    logger.info(f"Replicando bloque {block_id} desde {source_url}...")
    try:
        file_path = os.path.join(DATA_DIR, block_id)
        md5_path = file_path + ".md5"
        
        md5_hash = hashlib.md5()
        with requests.get(source_url, stream=True, timeout=10) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    md5_hash.update(chunk)
                    
        with open(md5_path, "w") as f:
            f.write(md5_hash.hexdigest())
        logger.info(f"Réplica completada: {block_id}")
    except Exception as e:
        logger.error(f"Fallo replicando {block_id}: {e}")

@app.get("/health")
def health_check():
    return {"status": "ok", "datanode_id": DATANODE_ID}

@app.post("/blocks/{block_id}")
async def upload_block(block_id: str, file: UploadFile = File(...)):
    """Recibe un bloque del cliente y lo guarda físicamente calculando MD5"""
    file_path = os.path.join(DATA_DIR, block_id)
    md5_path = file_path + ".md5"
    try:
        md5_hash = hashlib.md5()
        with open(file_path, "wb") as buffer:
            while True:
                chunk = file.file.read(8192)
                if not chunk:
                    break
                md5_hash.update(chunk)
                buffer.write(chunk)
                
        with open(md5_path, "w") as f:
            f.write(md5_hash.hexdigest())
            
        logger.info(f"Bloque guardado exitosamente: {block_id} (MD5: {md5_hash.hexdigest()})")
        return {"message": "Bloque guardado exitosamente", "checksum": md5_hash.hexdigest()}
    except Exception as e:
        logger.error(f"Error guardando bloque {block_id}: {e}")
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(md5_path):
            os.remove(md5_path)
        raise HTTPException(status_code=500, detail="Error interno al guardar el bloque")

@app.get("/blocks/{block_id}")
def download_block(block_id: str):
    """Sirve el bloque físico al cliente"""
    file_path = os.path.join(DATA_DIR, block_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Bloque no encontrado en este DataNode")
    return FileResponse(file_path, media_type="application/octet-stream", filename=block_id)
