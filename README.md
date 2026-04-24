# ERD (GitHub Pages)

Todo lo necesario para el visor ERD quedó agrupado en una sola carpeta:

- `erd/index.html` (visor interactivo)
- `erd/data/rats.schema.json` (datos RATS)
- `erd/data/tml.schema.json` (datos TML)
- `erd/data/airsoft_module4_non_routine.schema.json`
- `erd/data/airsoft_module5_turnover_book.schema.json`
- `erd/data/airsoft_module6_digitalization_docmat.schema.json`
- `erd/data/airsoft_module7_contract_quotation.schema.json`
- `erd/data/airsoft_module8_invoice_systems.schema.json`
- `erd/data/datasets.manifest.json` (pestañas dinámicas del visor)
- `erd/build_erd_data_from_env.py` (genera JSON desde Oracle o DB2 + `.env`)

`index.html` en la raíz solo redirige a `erd/index.html`.

## Generar datos desde .env (auto RATS/TML + módulos)

1. Copia `erd/.env.example` a `erd/.env` y llena credenciales (Oracle y/o DB2).
2. Desde la raíz de este repo:

### Oracle

`python3 erd/build_erd_data_from_env.py --db oracle --with-comments --ai-mode heuristic`

Variables `.env` (Oracle):
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_SERVICE`

### DB2 (LUW)

`python3 erd/build_erd_data_from_env.py --db db2 --with-comments --ai-mode heuristic`

Variables `.env` (DB2):
- `DB2_HOST`, `DB2_PORT` (default 50000), `DB2_DBNAME`, `DB2_USER`, `DB2_PASSWORD`
- opcional: `DB2_SECURITY`

Dependencia:
- `pip install ibm_db ibm_db_dbi`

### Mixed (Oracle para RATS/TML + DB2 para Airsoft)

`python3 erd/build_erd_data_from_env.py --db mixed --with-comments --ai-mode heuristic`

Este modo genera:
- `rats.schema.json`, `tml.schema.json` desde **Oracle**
- `airsoft_full.schema.json` + `airsoft_module*.schema.json` desde **DB2**
- Relaciones se muestran **locales** a cada motor (no hay FKs cross-DB en catálogo).

El script:
- se conecta a Oracle con variables `DB_*` (o DB2 con `DB2_*`)
- extrae esquema completo (según el motor)
- detecta automáticamente qué tablas son de **RATS** y cuáles de **TML**
- separa automáticamente tablas en módulos:
  - Module 4: Non-Routine Data Base
  - Module 5: Turnover Book
  - Module 6: Digitalization (DOCMAT)
  - Module 7: Contract & Quotation
  - Module 8: Invoice Systems
- regenera todos los `*.schema.json` + `datasets.manifest.json`
- genera `ai_table_explanations` por tabla para mostrar tooltip al hacer hover
- en modo `heuristic` la descripción es determinística y basada solo en metadatos (minimiza hallucinations)

## IA local gratis (Ollama)

Si no quieres servicios de pago, puedes usar un modelo local con Ollama:

1. Instala Ollama y descarga un modelo (ejemplo):
   - `ollama pull llama3.1:8b-instruct-q4_K_M`
2. En `erd/.env` agrega:
   - `OLLAMA_URL=http://127.0.0.1:11434`
   - `OLLAMA_MODEL=llama3.1:8b-instruct-q4_K_M`
3. Ejecuta build:
   - `python3 erd/build_erd_data_from_env.py --with-comments --ai-mode ollama`

Opciones de modo IA:
- `--ai-mode heuristic` (default): no llama IA externa/local, solo descripción determinística
- `--ai-mode auto`: usa Ollama si hay `OLLAMA_MODEL`, si no heurístico local
- `--ai-mode ollama`: fuerza Ollama

Si ya tienes `rats.schema.json` y `tml.schema.json` y solo quieres derivar módulos:

`python3 erd/build_erd_data_from_env.py --from-existing --ai-mode heuristic`

Con GitHub Pages en rama **main** y carpeta **/** (root), la URL queda:

`https://<tu-usuario>.github.io/<nombre-del-repo>/`

## Automatización semanal en GitHub (sin correr en laptop)

Este repo incluye workflow:

- `.github/workflows/weekly-erd-refresh.yml`

Para activarlo en el GitHub del cliente:

1. Subir el repo (o zip) a su GitHub.
2. Configurar estos **Repository Secrets**:
   - `DB_HOST`
   - `DB_PORT`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_SERVICE`
3. Habilitar Actions.
4. Ejecutar una vez manualmente `Weekly ERD Refresh` (workflow_dispatch).
5. Luego quedará automático semanalmente (lunes 13:00 UTC).

## Automatización semanal en Mac con FortiClient (VPN)

Si la BD solo es accesible dentro de VPN (FortiClient), lo más simple es correr el refresco **desde un Mac dentro de la VPN** y hacer `git push` de los JSON. GitHub Pages (o Vercel estático) solo sirve los archivos ya generados.

1. En el Mac (con VPN), clona el repo y crea `erd/.env` desde `erd/.env.example`.
2. Instala dependencias:
   - `pip3 install oracledb ibm_db ibm_db_dbi`
3. Instala el job semanal (launchd):

   - `bash tools/install_launchd.sh`

Esto crea `~/Library/LaunchAgents/com.client.erd.weekly.plist` y lo carga.

Ejecutar manualmente para probar:
- `launchctl start com.client.erd.weekly`

Logs:
- `tools/.logs/erd_weekly.out`
- `tools/.logs/erd_weekly.err`

