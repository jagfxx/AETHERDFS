#  AetherDFS: Sistema de Archivos Distribuido por Bloques

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

**AetherDFS** es un Sistema de Archivos Distribuido (DFS) minimalista, seguro y tolerante a fallos inspirado en arquitecturas de nivel industrial como **HDFS (Hadoop)** y **Google File System (GFS)**. Diseñado desde cero para garantizar la resiliencia de los datos, fragmentación inteligente y recuperación automática de desastres en clústeres distribuidos.

---

##  Características Principales

*  **Fragmentación Dinámica (Chunking):** Divide de forma automática los archivos masivos en bloques de tamaño configurable (Por defecto: 64MB) para ser distribuidos en múltiples nodos.
*  **Replicación Estricta:** Cada bloque se replica en al menos dos DataNodes distintos. El cliente aborta y revierte la subida si una de las réplicas falla, garantizando el factor de replicación al hacer commit.
*  **Distribución Round-Robin:** El NameNode rota los DataNodes destino bloque por bloque, evitando hot-spots y asegurando que los N nodos del clúster participen activamente.
*  **Self-Healing (Auto-Curación):** El NameNode monitorea constantemente el clúster. Si un DataNode colapsa y se pierde una réplica, el sistema ordena a un nodo sano que duplique el bloque huérfano para restaurar la seguridad.
*  **Autenticación Multi-capa:** JWT para usuarios + cluster token compartido entre NameNode y DataNodes. Los endpoints `/blocks/*` y `/datanodes/heartbeat` rechazan peticiones anónimas.
*  **Integridad Criptográfica (Checksums MD5):** El cliente envía los checksums junto con el commit transaccional. Al descargar audita cada bloque y, ante un mismatch, intenta automáticamente la réplica.
*  **Two-phase Commit:** Los archivos pasan por estado `UPLOADING` → `READY`, evitando metadatos fantasma si una subida falla a la mitad.
*  **Docker-Native:** Completamente dockerizado. Listo para orquestarse en la nube o AWS mediante un simple `docker-compose`.

---

##  Arquitectura del Sistema (Master-Workers)

AetherDFS funciona bajo un modelo `Master-Worker` compuesto por tres actores principales comunicados mediante **API REST** de alta velocidad:

1. **NameNode (Master):** El cerebro central. Almacena todos los metadatos (jerarquías de carpetas, tamaños, propietarios, Hashes MD5 y mapa de bloques). Utiliza SQLite en modo WAL con `foreign_keys=ON` por conexión. El bucle de self-healing corre en thread separado para no bloquear el event loop.
2. **DataNodes (Workers):** Servidores de almacenamiento bruto. Reciben, guardan y envían fragmentos de archivos a los clientes y replican entre sí cuando el NameNode lo ordena. Usan `httpx.AsyncClient` para comunicación no bloqueante.
3. **Cliente (CLI):** Aplicación de consola interactiva en Python que simula una terminal nativa de Unix (`ls`, `cd`, `mkdir`, `put`, `get`, `rm`) para orquestar subidas y bajadas complejas hacia los nodos de forma transparente para el usuario final. Sube un bloque a la vez (RAM acotada) con replicación paralela.

---

##  Despliegue Rápido (Quickstart)

El proyecto está diseñado para levantarse en segundos mediante Docker Compose.

### 1. Clonar e Iniciar el Clúster
```bash
git clone https://github.com/jagfxx/DFS-minimalista.git
cd DFS-minimalista

# (Opcional) Definir secretos. Si se omite, el compose usa defaults de desarrollo
export JWT_SECRET=mi-secreto-jwt
export CLUSTER_TOKEN=mi-token-de-cluster

# Levantar el NameNode central y 3 DataNodes perimetrales
docker compose up -d --build
```
> El NameNode se expondrá en el puerto `8000`, y los DataNodes mapearán sus puertos internamente en `8001`, `8002` y `8003`.

### 2. Iniciar el Cliente Interactivo (CLI)
```bash
cd client
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Ejecutar la Terminal del DFS
python3 dfs_cli.py
```

### 3. Operaciones Soportadas
Dentro de la consola `AetherDFS >`, prueba los siguientes comandos:
- `register <user> <pass>` : Crea un usuario nuevo.
- `login <user> <pass>` : Inicia sesión en el clúster.
- `mkdir <carpeta>` : Crea un directorio virtual.
- `ls` : Lista archivos y directorios (Muestra pesos exactos).
- `cd <carpeta>` : Navega por la jerarquía.
- `put <ruta_local> <nombre_remoto>` : Fragmenta y sube un archivo masivo al clúster de forma distribuida.
- `get <nombre_remoto> <ruta_local>` : Reconstruye y audita un archivo desde los múltiples nodos.
- `rm <nombre_remoto>` : Elimina el archivo y dispara el recolector de basura en los nodos.
- `rmdir <carpeta>` : Elimina un directorio vacío.

---

##  Demostración de Tolerancia a Desastres

AetherDFS brilla cuando las cosas salen mal. Puedes someterlo a pruebas extremas:
1. **Prueba de Fuego (Nodos Caídos):** Mientras descargas un archivo masivo, detén un DataNode (`docker compose stop datanode1`). Verás cómo el cliente intercepta la falla de red y extrae los bloques faltantes de los nodos de réplica sobrevivientes sin interrumpir la descarga.
2. **Prueba de Auto-Curación:** Tras apagar un nodo, espera ~70 segundos (30s para marcar al DN como muerto + 30s del ciclo de self-healing) y observa los logs del NameNode (`docker compose logs -f namenode`). Verás al sistema darse cuenta de que perdió copias e invocar órdenes de replicación entre los nodos sanos.
3. **Prueba de Corrupción Física:** Entra al disco de un DataNode y altera un bloque binario. Al hacer `get`, el cliente reportará `MD5 mismatch en datanodeX, intentando réplica` y automáticamente buscará el bloque intacto en el nodo de respaldo.

##  Pruebas Automatizadas

El repositorio incluye una batería de pruebas end-to-end que valida 12 escenarios contra el cluster Docker, incluyendo:

- Health checks de NameNode y los 3 DataNodes
- Autenticación correcta + rechazo de tokens inválidos
- Operaciones de directorio anidadas y validaciones
- Subida de archivos pequeños (1 bloque) y grandes (3+ bloques)
- Verificación bit-a-bit (MD5) tras descarga
- Subida de archivo vacío (0 bytes)
- Seguridad: rechazo de peticiones anónimas a DataNodes y heartbeat
- **Failover real:** se mata un DataNode mid-flight y se verifica que `get` siga funcionando
- **Self-healing real:** se confirma que un 3er DataNode recibe la réplica perdida automáticamente

Ejecución:
```bash
docker compose up -d --build
python3 -m venv .venv && source .venv/bin/activate
pip install requests
python3 /tmp/dfs_full_test.py    # ver script en /tmp tras correr el test inicial
```

---
*Desarrollado para el proyecto final de Arquitecturas de Nube y Sistemas Distribuidos (2026).*
