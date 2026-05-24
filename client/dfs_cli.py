import sys
import os
import requests
from typing import Dict, Optional

NAMENODE_URL = os.environ.get("NAMENODE_URL", "http://localhost:8000")
BLOCK_SIZE_MB = int(os.environ.get("BLOCK_SIZE_MB", 64))
BLOCK_SIZE_BYTES = BLOCK_SIZE_MB * 1024 * 1024

TOKEN: Optional[str] = None
CURRENT_DIR = "/"

def headers() -> Dict[str, str]:
    if not TOKEN:
        print("Error: No estás autenticado. Usa 'login' o 'register'.")
        sys.exit(1)
    return {"Authorization": f"Bearer {TOKEN}"}

def resolve_path(path: str) -> str:
    if path.startswith("/"):
        return path
    if CURRENT_DIR == "/":
        return "/" + path
    return CURRENT_DIR + "/" + path

def register(username, password):
    res = requests.post(f"{NAMENODE_URL}/register", json={"username": username, "password": password})
    if res.status_code == 200:
        print("Registro exitoso. Ahora puedes hacer login.")
    else:
        print(f"Error: {res.json().get('detail')}")

def login(username, password):
    global TOKEN
    res = requests.post(f"{NAMENODE_URL}/login", json={"username": username, "password": password})
    if res.status_code == 200:
        TOKEN = res.json()["access_token"]
        print("Autenticación exitosa.")
    else:
        print("Error en credenciales.")

def mkdir(path):
    full_path = resolve_path(path)
    res = requests.post(f"{NAMENODE_URL}/mkdir", json={"path": full_path}, headers=headers())
    if res.status_code == 200:
        print(f"Directorio creado: {full_path}")
    else:
        print(f"Error: {res.json().get('detail')}")

def rmdir(path):
    full_path = resolve_path(path)
    res = requests.post(f"{NAMENODE_URL}/rmdir", json={"path": full_path}, headers=headers())
    if res.status_code == 200:
        print(f"Directorio eliminado: {full_path}")
    else:
        print(f"Error: {res.json().get('detail')}")

def rm(path):
    full_path = resolve_path(path)
    res = requests.post(f"{NAMENODE_URL}/rm", json={"path": full_path}, headers=headers())
    if res.status_code == 200:
        print(f"Archivo eliminado: {full_path}")
    else:
        print(f"Error: {res.json().get('detail')}")

def ls(path=""):
    full_path = resolve_path(path) if path else CURRENT_DIR
    res = requests.post(f"{NAMENODE_URL}/ls", json={"path": full_path}, headers=headers())
    if res.status_code == 200:
        items = res.json()
        if not items:
            print("(vacío)")
        for item in items:
            tipo = "DIR " if item['is_dir'] else "FILE"
            size = item['size'] / (1024*1024)
            print(f"[{tipo}] {item['name']} ({size:.2f} MB)")
    else:
        print(f"Error: {res.json().get('detail')}")

def cd(path):
    global CURRENT_DIR
    if path == "..":
        if CURRENT_DIR != "/":
            CURRENT_DIR = "/" + "/".join([p for p in CURRENT_DIR.split("/") if p][:-1])
            if not CURRENT_DIR: CURRENT_DIR = "/"
        return

    full_path = resolve_path(path)
    if full_path == "/":
        CURRENT_DIR = "/"
        return
        
    res = requests.post(f"{NAMENODE_URL}/ls", json={"path": full_path}, headers=headers())
    if res.status_code == 200:
        CURRENT_DIR = full_path
    else:
        print(f"Directorio no existe: {full_path}")

def put(local_path, remote_path):
    if not os.path.exists(local_path):
        print("Archivo local no existe.")
        return
        
    file_size = os.path.getsize(local_path)
    full_remote_path = resolve_path(remote_path)
    
    # 1. Asignar bloques
    res = requests.post(f"{NAMENODE_URL}/files/allocate", json={"path": full_remote_path, "file_size": file_size}, headers=headers())
    if res.status_code != 200:
        print(f"Error asignando bloques: {res.json().get('detail')}")
        return
        
    blocks = res.json()["blocks"]
    print(f"Asignados {len(blocks)} bloques para {full_remote_path}.")
    
    # 2. Enviar los bloques
    with open(local_path, "rb") as f:
        for b in blocks:
            data_chunk = f.read(BLOCK_SIZE_BYTES)
            if not data_chunk:
                break
                
            block_id = b["block_id"]
            
            # Subir al primario y al secundario directamente (el cliente se asegura de la replicación según el plan)
            for node in [b["primary"], b["replica"]]:
                target_url = f"http://{node['host']}:{node['port']}/blocks/{block_id}"
                print(f"Subiendo bloque {block_id[:8]} a {node['host']}...")
                try:
                    # Usamos requests con files para enviar multipart
                    upload_res = requests.post(target_url, files={"file": (block_id, data_chunk)})
                    if upload_res.status_code != 200:
                        print(f"Advertencia: Falló subida a {node['host']}.")
                except Exception as e:
                    print(f"Advertencia: Nodo {node['host']} inaccesible ({e}).")
                    
    print("Transferencia completada.")

def print_help():
    print("""
Comandos disponibles:
  register <user> <pass>
  login <user> <pass>
  ls [path]
  cd <path>
  mkdir <path>
  rmdir <path>
  rm <path>
  put <local_file> <remote_path>
  help
  exit
""")

def main():
    print("=== DFS Minimalista CLI ===")
    print("Escribe 'help' para ver los comandos.")
    
    while True:
        try:
            cmd_line = input(f"DFS {CURRENT_DIR} > ").strip().split()
            if not cmd_line: continue
            
            cmd = cmd_line[0]
            args = cmd_line[1:]
            
            if cmd == "exit": break
            elif cmd == "help": print_help()
            elif cmd == "register" and len(args) == 2: register(args[0], args[1])
            elif cmd == "login" and len(args) == 2: login(args[0], args[1])
            elif cmd == "ls": ls(args[0] if args else "")
            elif cmd == "cd" and len(args) == 1: cd(args[0])
            elif cmd == "mkdir" and len(args) == 1: mkdir(args[0])
            elif cmd == "rmdir" and len(args) == 1: rmdir(args[0])
            elif cmd == "rm" and len(args) == 1: rm(args[0])
            elif cmd == "put" and len(args) == 2: put(args[0], args[1])
            else:
                print("Comando inválido o argumentos incorrectos. Escribe 'help'.")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        except Exception as e:
            print(f"Error inesperado: {e}")

if __name__ == "__main__":
    main()
