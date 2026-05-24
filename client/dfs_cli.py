import os
import sys
import posixpath
import hashlib
import concurrent.futures
import requests
from typing import Dict, Optional

NAMENODE_URL = os.environ.get("NAMENODE_URL", "http://localhost:8000")
BLOCK_SIZE_BYTES = int(os.environ.get("BLOCK_SIZE_MB", 64)) * 1024 * 1024

TOKEN: Optional[str] = None
CURRENT_DIR = "/"


def headers() -> Dict[str, str]:
    if not TOKEN:
        raise SystemExit("No estás autenticado. Usa 'login' o 'register'.")
    return {"Authorization": f"Bearer {TOKEN}"}


def resolve_path(path: str) -> str:
    if not path.startswith("/"):
        path = posixpath.join(CURRENT_DIR, path)
    return posixpath.normpath(path)


def _post(endpoint: str, payload: dict, auth: bool = True):
    h = headers() if auth else {}
    return requests.post(f"{NAMENODE_URL}{endpoint}", json=payload, headers=h)


def _detail(r):
    try:
        return r.json().get("detail", r.text)
    except Exception:
        return r.text


def register(username, password):
    r = _post("/register", {"username": username, "password": password}, auth=False)
    print("Registro exitoso." if r.ok else f"Error: {_detail(r)}")


def login(username, password):
    global TOKEN
    r = _post("/login", {"username": username, "password": password}, auth=False)
    if r.ok:
        TOKEN = r.json()["access_token"]
        print("Autenticación exitosa.")
    else:
        print("Credenciales inválidas.")


def _cmd(endpoint, path, ok_msg):
    target = resolve_path(path)
    r = _post(endpoint, {"path": target})
    print(f"{ok_msg}: {target}" if r.ok else f"Error: {_detail(r)}")


def mkdir(path): _cmd("/mkdir", path, "Directorio creado")
def rmdir(path): _cmd("/rmdir", path, "Directorio eliminado")
def rm(path):    _cmd("/rm", path, "Archivo eliminado")


def ls(path=""):
    target = resolve_path(path) if path else CURRENT_DIR
    r = _post("/ls", {"path": target})
    if not r.ok:
        print(f"Error: {_detail(r)}")
        return
    items = r.json()
    if not items:
        print("(vacío)")
        return
    for it in items:
        tipo = "DIR " if it["is_dir"] else "FILE"
        print(f"[{tipo}] {it['name']} ({it['size'] / (1024*1024):.2f} MB)")


def cd(path):
    global CURRENT_DIR
    target = resolve_path(path)
    if target == "/":
        CURRENT_DIR = "/"
        return
    r = _post("/ls", {"path": target})
    if r.ok:
        CURRENT_DIR = target
    else:
        print(f"Directorio no existe: {target}")


def _upload_to(node, block_id, data):
    url = f"http://{node['host']}:{node['port']}/blocks/{block_id}"
    try:
        r = requests.post(url, files={"file": (block_id, data)}, headers=headers(), timeout=300)
        return r.json().get("checksum") if r.ok else None
    except Exception as e:
        print(f"  ! fallo en {node['host']}: {e}")
        return None


def put(local_path, remote_path):
    if not os.path.exists(local_path):
        print("Archivo local no existe.")
        return
    file_size = os.path.getsize(local_path)
    remote = resolve_path(remote_path)

    r = _post("/files/allocate", {"path": remote, "file_size": file_size})
    if not r.ok:
        print(f"Error asignando bloques: {_detail(r)}")
        return
    blocks = r.json()["blocks"]
    print(f"Asignados {len(blocks)} bloques.")

    success = True
    checksums = {}
    with open(local_path, "rb") as f:
        for b in blocks:
            data = f.read(BLOCK_SIZE_BYTES)
            block_id = b["block_id"]
            replicas = b["replicas"]
            print(f"  → {block_id[:8]} a {[n['host'] for n in replicas]}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(replicas)) as ex:
                results = list(ex.map(lambda n: _upload_to(n, block_id, data), replicas))
            if not all(results):
                print("  ✗ Falló al menos una réplica (replicación estricta). Abortando.")
                success = False
                break
            checksums[block_id] = results[0]

    if success:
        r = _post("/files/commit", {"path": remote, "checksums": checksums})
        if r.ok:
            print("✓ Subida completa.")
            return
        print(f"Error en commit: {_detail(r)}")
    rm(remote_path)


def get(remote_path, local_path):
    remote = resolve_path(remote_path)
    r = _post("/files/locations", {"path": remote})
    if not r.ok:
        print(f"Error: {_detail(r)}")
        return
    blocks = r.json()["blocks"]
    print(f"Descargando {len(blocks)} bloques...")

    with open(local_path, "wb") as out:
        for b in blocks:
            block_id = b["block_id"]
            expected = b.get("checksum")
            start = out.tell()
            downloaded = False
            for node in b["locations"]:
                url = f"http://{node['host']}:{node['port']}/blocks/{block_id}"
                md5 = hashlib.md5()
                try:
                    with requests.get(url, stream=True, headers=headers(), timeout=300) as resp:
                        resp.raise_for_status()
                        for chunk in resp.iter_content(chunk_size=65536):
                            md5.update(chunk)
                            out.write(chunk)
                    if expected and md5.hexdigest() != expected:
                        print(f"  ! MD5 mismatch en {node['host']}, intentando réplica")
                        out.seek(start)
                        out.truncate()
                        continue
                    downloaded = True
                    break
                except Exception as e:
                    print(f"  ! error en {node['host']}: {e}")
                    out.seek(start)
                    out.truncate()
            if not downloaded:
                print("✗ No se pudo descargar el bloque de ninguna réplica.")
                return
    print(f"✓ Archivo guardado en {local_path}")


HELP = """
Comandos:
  register <user> <pass>
  login <user> <pass>
  ls [path]
  cd <path>
  mkdir <path>
  rmdir <path>
  rm <path>
  put <local> <remoto>
  get <remoto> <local>
  help
  exit
"""


def main():
    print("=== DFS Minimalista CLI ===\nEscribe 'help' para ver los comandos.")
    funcs = {
        "register": register, "login": login, "ls": ls, "cd": cd,
        "mkdir": mkdir, "rmdir": rmdir, "rm": rm, "put": put, "get": get,
        "help": lambda: print(HELP),
    }
    while True:
        try:
            parts = input(f"DFS {CURRENT_DIR} > ").strip().split()
            if not parts:
                continue
            cmd, *args = parts
            if cmd == "exit":
                break
            if cmd not in funcs:
                print("Comando inválido. 'help' para ver opciones.")
                continue
            try:
                funcs[cmd](*args)
            except TypeError:
                print("Argumentos incorrectos.")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
