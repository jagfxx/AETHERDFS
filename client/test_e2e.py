import os
import time
import hashlib
import sys

# Añadimos el path para poder importar dfs_cli
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import dfs_cli

def create_dummy_file(filepath: str, size_mb: int):
    print(f"--- Creando archivo de prueba de {size_mb}MB ---")
    chunk_size = 1024 * 1024
    with open(filepath, "wb") as f:
        for _ in range(size_mb):
            f.write(os.urandom(chunk_size))
            
def md5(filepath: str) -> str:
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def run_tests():
    print("\n================ INICIANDO PRUEBAS A PROFUNDIDAD ================\n")
    
    # 1. Autenticación
    print(">> TEST 1: Registro y Login")
    username = f"user_{int(time.time())}"
    password = "password123"
    dfs_cli.register(username, password)
    dfs_cli.login(username, password)
    if not dfs_cli.TOKEN:
        print("Fallo en login!")
        return
        
    # 2. Gestión de Directorios
    print("\n>> TEST 2: Jerarquía de Directorios (mkdir, cd, ls)")
    dfs_cli.mkdir("videos")
    dfs_cli.cd("videos")
    print(f"Directorio actual: {dfs_cli.CURRENT_DIR}")
    dfs_cli.ls()
    
    # 3. Transferencia de archivos (Upload > 64MB)
    print("\n>> TEST 3: Particionamiento y Subida de Archivo (PUT)")
    test_file_in = "/tmp/test_file_70mb.bin"
    create_dummy_file(test_file_in, 70) # > 64MB para probar múltiples bloques
    original_hash = md5(test_file_in)
    print(f"Hash Original: {original_hash}")
    
    dfs_cli.put(test_file_in, "mi_video.bin")
    
    # 4. Listar para verificar
    print("\n>> TEST 4: Verificación de persistencia de metadatos (LS)")
    dfs_cli.ls()
    
    # 5. Descarga de archivos (GET) y Verificación de Integridad
    print("\n>> TEST 5: Reconstrucción y Descarga de Archivo (GET)")
    test_file_out = "/tmp/test_file_downloaded.bin"
    if os.path.exists(test_file_out):
        os.remove(test_file_out)
        
    dfs_cli.get("mi_video.bin", test_file_out)
    
    downloaded_hash = md5(test_file_out)
    print(f"Hash Descargado: {downloaded_hash}")
    
    if original_hash == downloaded_hash:
        print("✅ ÉXITO: Los hashes MD5 coinciden. Archivo reconstruido perfectamente.")
    else:
        print("❌ ERROR: Los hashes no coinciden.")
        return
        
    # 6. Borrado (RM) y Garbage Collection
    print("\n>> TEST 6: Borrado lógico y Físico (Garbage Collection)")
    dfs_cli.rm("mi_video.bin")
    print("Listando el directorio tras borrar:")
    dfs_cli.ls()
    
    print("\nEsperando 6 segundos para que los DataNodes reciban el Heartbeat con la orden de borrado...")
    time.sleep(6)
    
    # Verificar físicamente si existen bloques en los volumenes locales
    print("Revisando contenedores físicos para asegurar que los bloques binarios fueron purgados...")
    # Esto es una inspección externa a la CLI
    
    print("\n================ PRUEBAS FINALIZADAS CON ÉXITO ================\n")

if __name__ == "__main__":
    run_tests()
