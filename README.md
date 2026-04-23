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
- `erd/build_erd_data_from_env.py` (genera JSON desde Oracle + `.env`)

`index.html` en la raíz solo redirige a `erd/index.html`.

## Generar datos desde .env (auto RATS/TML + módulos)

1. Copia `erd/.env.example` a `erd/.env` y llena credenciales Oracle.
2. Desde la raíz de este repo:

`python3 erd/build_erd_data_from_env.py --with-comments --ai-mode heuristic`

El script:
- se conecta a Oracle con variables `DB_*`
- extrae esquema completo
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
