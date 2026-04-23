#!/usr/bin/env python3
"""
Generate ERD dataset files from Oracle credentials in .env.

Outputs:
- rats.schema.json
- tml.schema.json
- airsoft_full.schema.json (todos los prefijos Airsoft, vista grande)
- airsoft_module*.schema.json (cada bloque 4..8)
- datasets.manifest.json (navegacion: RATS · TML · Airsoft + subpestanas)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import oracledb
except ImportError:
    oracledb = None

try:
    # Optional dependency for DB2 connectivity (LUW): pip install ibm_db ibm_db_dbi
    import ibm_db_dbi  # type: ignore
except ImportError:
    ibm_db_dbi = None

AIRSOFT_DATASET_ID = "airsoft_full"
AIRSOFT_FILE = f"{AIRSOFT_DATASET_ID}.schema.json"
AIRSOFT_TOP_LABEL = "Airsoft (vista completa)"

# Bump cuando cambia el template de explicacion (invalida .ai_explanations_cache.json antiguo).
AI_EXPL_CACHE_VER = 2

AIRSOFT_MODULES: List[Tuple[str, str, Tuple[str, ...]]] = [
    (
        "airsoft_module4_non_routine",
        "Module 4: Non-Routine Data Base",
        ("PWOS", "AER_PWOS", "PTRD", "TRD", "WOS", "RAT_PWOS"),
    ),
    (
        "airsoft_module5_turnover_book",
        "Module 5: Turnover Book",
        ("TBL", "TBK", "TURN", "BOOK"),
    ),
    (
        "airsoft_module6_digitalization_docmat",
        "Module 6: Digitalization (DOCMAT)",
        ("DOC", "DOCMAT", "DCM", "DMAT", "SCAN"),
    ),
    (
        "airsoft_module7_contract_quotation",
        "Module 7: Contract & Quotation",
        ("CAP", "CERT", "CON", "COT", "QUO", "QUOTE"),
    ),
    (
        "airsoft_module8_invoice_systems",
        "Module 8: Invoice Systems",
        ("FIN", "INV", "MIS", "PMIS", "PFIN", "INVOICE", "FACT"),
    ),
]


def load_env_file(env_path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not env_path.exists():
        raise FileNotFoundError(f"No existe el archivo .env: {env_path}")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def _fmt_type(t, length, precision, scale):
    if t == "NUMBER":
        if precision and scale:
            return f"NUMBER({precision},{scale})"
        if precision:
            return f"NUMBER({precision})"
        return "NUMBER"
    if t in ("VARCHAR2", "CHAR", "NVARCHAR2") and length:
        return f"{t}({length})"
    return t or "?"


def _fmt_type_db2(type_name: str, length: Optional[int], scale: Optional[int]) -> str:
    """
    Format DB2 column types. We keep it simple and deterministic.
    """
    tn = (type_name or "").upper()
    if tn in {"VARCHAR", "CHAR", "GRAPHIC", "VARGRAPHIC"} and length:
        return f"{tn}({length})"
    if tn in {"DECIMAL", "DECFLOAT", "NUMERIC"}:
        if length is not None and scale is not None:
            return f"{tn}({length},{scale})"
        if length is not None:
            return f"{tn}({length})"
        return tn
    if tn in {"TIMESTAMP", "DATE", "TIME"}:
        return tn
    if tn in {"CLOB", "BLOB", "DBCLOB"} and length:
        return f"{tn}({length})"
    return tn or "?"


def get_db_connection_oracle(db_config: Dict[str, Any]):
    if oracledb is None:
        raise RuntimeError("Falta dependencia `oracledb` (pip install oracledb)")
    dsn = f"{db_config['host']}:{db_config['port']}/{db_config['service_name']}"
    return oracledb.connect(
        user=db_config["user"],
        password=db_config["password"],
        dsn=dsn,
        tcp_connect_timeout=120,
    )


def get_db_connection_db2(db2_config: Dict[str, Any]):
    """
    DB2 LUW connection using ibm_db_dbi (DB-API).
    Env mapping:
      - DB2_HOST, DB2_PORT, DB2_DBNAME, DB2_USER, DB2_PASSWORD
      - optional: DB2_SECURITY (e.g. SSL), DB2_PROTOCOL (default TCPIP)
    """
    if ibm_db_dbi is None:
        raise RuntimeError("Falta dependencia DB2 `ibm_db_dbi` (pip install ibm_db ibm_db_dbi)")
    host = db2_config["host"]
    port = int(db2_config.get("port", 50000))
    dbname = db2_config["dbname"]
    user = db2_config["user"]
    password = db2_config["password"]
    protocol = db2_config.get("protocol", "TCPIP")
    security = db2_config.get("security")
    # Minimal DSN; extra parameters can be extended as needed.
    parts = [
        f"DATABASE={dbname}",
        f"HOSTNAME={host}",
        f"PORT={port}",
        f"PROTOCOL={protocol}",
        f"UID={user}",
        f"PWD={password}",
    ]
    if security:
        parts.append(f"SECURITY={security}")
    dsn = ";".join(parts) + ";"
    return ibm_db_dbi.connect(dsn, "", "")


def get_schema_db2(
    connection,
    with_comments: bool = False,
    schemas: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Extract schema metadata from DB2 (LUW) using SYSCAT catalog views.
    Returns the same structure as `get_schema` (Oracle):
      tables: ["SCHEMA.TABLE", ...]
      columns: { "SCHEMA.TABLE": [{"name","type","nullable","comment"}] }
      pks: { "SCHEMA.TABLE": ["COL1", ...] }
      uqs: { "SCHEMA.TABLE": ["COLX", ...] }  # flattened columns across UQ constraints
      refs: [{"from_table","from_col","to_table","to_col"}]
      table_comments: { "SCHEMA.TABLE": "..." }
    """
    cur = connection.cursor()

    def schema_clause(col: str) -> Tuple[str, List[Any]]:
        if schemas:
            sch = [s.upper() for s in schemas]
            placeholders = ",".join(["?"] * len(sch))
            return f"{col} IN ({placeholders})", sch
        # Filter out system schemas by default (keep deterministic but conservative)
        return (
            f"{col} NOT LIKE 'SYS%' AND {col} NOT IN ('NULLID','SQLJ','SYSTOOLS','SYSCAT','SYSIBM','SYSIBMADM','SYSSTAT')",
            [],
        )

    wh, args = schema_clause("TABSCHEMA")
    cur.execute(
        f"SELECT TABSCHEMA, TABNAME, REMARKS FROM SYSCAT.TABLES WHERE TYPE='T' AND {wh} ORDER BY TABSCHEMA, TABNAME",
        args,
    )
    rows = cur.fetchall()
    tables = [f"{r[0].strip()}.{r[1].strip()}" for r in rows]
    table_set: Set[str] = set(tables)

    table_comments: Dict[str, str] = {}
    if with_comments:
        for r in rows:
            key = f"{r[0].strip()}.{r[1].strip()}"
            rem = (r[2] or "").strip()
            if rem:
                table_comments[key] = rem

    wh, args = schema_clause("TABSCHEMA")
    cur.execute(
        f"SELECT TABSCHEMA, TABNAME, COLNAME, TYPENAME, LENGTH, SCALE, NULLS, COLNO, REMARKS "
        f"FROM SYSCAT.COLUMNS WHERE {wh} ORDER BY TABSCHEMA, TABNAME, COLNO",
        args,
    )
    columns: Dict[str, List[Dict[str, Any]]] = {}
    for r in cur.fetchall():
        t = f"{r[0].strip()}.{r[1].strip()}"
        if t not in table_set:
            continue
        comment = (r[8] or "").strip() if with_comments else None
        if comment == "":
            comment = None
        columns.setdefault(t, []).append(
            {
                "name": r[2].strip(),
                "type": _fmt_type_db2(r[3], r[4], r[5]),
                "nullable": (r[6] or "").upper() == "Y",
                "comment": comment,
            }
        )

    # Constraints: PK and UQ
    wh, args = schema_clause("C.TABSCHEMA")
    cur.execute(
        f"SELECT C.TABSCHEMA, C.TABNAME, C.CONSTNAME, C.TYPE "
        f"FROM SYSCAT.TABCONST C WHERE {wh} AND C.TYPE IN ('P','U')",
        args,
    )
    cons = cur.fetchall()
    pk_const: Dict[str, str] = {}
    uq_consts: Dict[str, List[str]] = {}
    for sch, tab, cname, ctype in cons:
        t = f"{sch.strip()}.{tab.strip()}"
        if t not in table_set:
            continue
        if ctype == "P":
            pk_const[t] = cname.strip()
        elif ctype == "U":
            uq_consts.setdefault(t, []).append(cname.strip())

    pks: Dict[str, List[str]] = {}
    uqs: Dict[str, List[str]] = {}

    # PK columns
    for t, cname in pk_const.items():
        sch, tab = t.split(".", 1)
        cur.execute(
            "SELECT COLNAME FROM SYSCAT.KEYCOLUSE "
            "WHERE TABSCHEMA=? AND TABNAME=? AND CONSTNAME=? ORDER BY COLSEQ",
            [sch, tab, cname],
        )
        pks[t] = [rr[0].strip() for rr in cur.fetchall()]

    # UQ columns (flattened across unique constraints, deterministic order by constraint then colseq)
    for t, cnames in uq_consts.items():
        sch, tab = t.split(".", 1)
        cols_acc: List[str] = []
        for cname in sorted(set(cnames)):
            cur.execute(
                "SELECT COLNAME FROM SYSCAT.KEYCOLUSE "
                "WHERE TABSCHEMA=? AND TABNAME=? AND CONSTNAME=? ORDER BY COLSEQ",
                [sch, tab, cname],
            )
            cols_acc.extend([rr[0].strip() for rr in cur.fetchall()])
        if cols_acc:
            uqs[t] = cols_acc

    # Foreign keys
    wh, args = schema_clause("R.TABSCHEMA")
    cur.execute(
        f"SELECT R.TABSCHEMA, R.TABNAME, R.CONSTNAME, R.REFTABSCHEMA, R.REFTABNAME, R.REFKEYNAME "
        f"FROM SYSCAT.REFERENCES R WHERE {wh}",
        args,
    )
    fk_rows = cur.fetchall()
    refs: List[Dict[str, Any]] = []
    for sch, tab, constname, rsch, rtab, refkey in fk_rows:
        from_table = f"{sch.strip()}.{tab.strip()}"
        to_table = f"{rsch.strip()}.{rtab.strip()}"
        if from_table not in table_set or to_table not in table_set:
            continue
        constname = constname.strip()
        refkey = refkey.strip()
        # Child cols
        cur.execute(
            "SELECT COLNAME FROM SYSCAT.KEYCOLUSE "
            "WHERE TABSCHEMA=? AND TABNAME=? AND CONSTNAME=? ORDER BY COLSEQ",
            [sch.strip(), tab.strip(), constname],
        )
        child_cols = [rr[0].strip() for rr in cur.fetchall()]
        # Parent cols (by referenced key name)
        cur.execute(
            "SELECT COLNAME FROM SYSCAT.KEYCOLUSE "
            "WHERE TABSCHEMA=? AND TABNAME=? AND CONSTNAME=? ORDER BY COLSEQ",
            [rsch.strip(), rtab.strip(), refkey],
        )
        parent_cols = [rr[0].strip() for rr in cur.fetchall()]
        for i in range(min(len(child_cols), len(parent_cols))):
            refs.append(
                {
                    "from_table": from_table,
                    "from_col": child_cols[i],
                    "to_table": to_table,
                    "to_col": parent_cols[i],
                }
            )

    cur.close()
    return {
        "tables": tables,
        "columns": columns,
        "pks": pks,
        "uqs": uqs,
        "refs": refs,
        "table_comments": table_comments if with_comments else {},
    }


def get_schema(connection, with_comments: bool = False, owners: Optional[List[str]] = None):
    cursor = connection.cursor()

    system_owners = (
        "SYS", "SYSTEM", "OUTLN", "DBSNMP", "APPQOSSYS", "WMSYS", "EXFSYS",
        "CTXSYS", "XDB", "ORDSYS", "ORDDATA", "MDSYS", "OLAPSYS", "LBACSYS",
        "FLOWS_FILES", "APEX_030200", "APEX_040000", "APEX_040200",
        "DIP", "ORACLE_OCM", "DMSYS", "MDDATA", "SPATIAL_CSW_ADMIN_USR",
        "SPATIAL_WFS_ADMIN_USR", "IX", "OE", "PM", "SH", "BI", "HR", "SCOTT",
    )

    def owner_filter(alias: str = ""):
        col = f"{alias}.owner" if alias else "owner"
        if owners:
            phs = ",".join(f":o{i}" for i in range(len(owners)))
            return f"{col} IN ({phs})", {f"o{i}": o.upper() for i, o in enumerate(owners)}
        phs = ",".join(f"'{o}'" for o in system_owners)
        return f"{col} NOT IN ({phs})", {}

    wh, bind = owner_filter()
    cursor.execute(f"SELECT owner, table_name FROM all_tables WHERE {wh} ORDER BY owner, table_name", bind)
    rows = cursor.fetchall()
    tables = [f"{r[0]}.{r[1]}" for r in rows]
    table_set = set(tables)

    wh, bind = owner_filter()
    cursor.execute(
        f"SELECT owner, table_name, column_name, data_type, data_length, "
        f"data_precision, data_scale, nullable, column_id "
        f"FROM all_tab_columns WHERE {wh} ORDER BY owner, table_name, column_id",
        bind,
    )
    columns: Dict[str, List[Dict[str, Any]]] = {}
    for row in cursor.fetchall():
        t = f"{row[0]}.{row[1]}"
        if t not in table_set:
            continue
        columns.setdefault(t, []).append(
            {
                "name": row[2],
                "type": _fmt_type(row[3], row[4], row[5], row[6]),
                "nullable": row[7] == "Y",
                "comment": None,
            }
        )

    table_comments: Dict[str, str] = {}
    if with_comments:
        try:
            wh, bind = owner_filter()
            cursor.execute(
                f"SELECT owner, table_name, comments FROM all_tab_comments "
                f"WHERE comments IS NOT NULL AND table_type = 'TABLE' AND {wh}",
                bind,
            )
            for row in cursor.fetchall():
                table_comments[f"{row[0]}.{row[1]}"] = (row[2] or "").strip() or None
        except Exception:
            pass
        try:
            wh, bind = owner_filter()
            cursor.execute(
                f"SELECT owner, table_name, column_name, comments "
                f"FROM all_col_comments WHERE comments IS NOT NULL AND {wh}",
                bind,
            )
            for row in cursor.fetchall():
                t, col, comment = f"{row[0]}.{row[1]}", row[2], (row[3] or "").strip() or None
                if t in columns:
                    for c in columns[t]:
                        if c["name"] == col:
                            c["comment"] = comment
                            break
        except Exception:
            pass

    wh, bind = owner_filter("c")
    cursor.execute(
        f"SELECT c.owner, cc.table_name, cc.column_name "
        f"FROM all_constraints c "
        f"JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner "
        f"WHERE c.constraint_type = 'P' AND {wh} ORDER BY cc.table_name, cc.position",
        bind,
    )
    pks: Dict[str, List[str]] = {}
    for row in cursor.fetchall():
        k = f"{row[0]}.{row[1]}"
        pks.setdefault(k, []).append(row[2])

    uqs: Dict[str, List[str]] = {}
    try:
        wh, bind = owner_filter("c")
        cursor.execute(
            f"SELECT c.owner, cc.table_name, cc.column_name "
            f"FROM all_constraints c "
            f"JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner "
            f"WHERE c.constraint_type = 'U' AND {wh} ORDER BY cc.table_name, cc.position",
            bind,
        )
        for row in cursor.fetchall():
            k = f"{row[0]}.{row[1]}"
            uqs.setdefault(k, []).append(row[2])
    except Exception:
        pass

    wh, bind = owner_filter("c")
    cursor.execute(
        f"SELECT c.owner, c.table_name, cc.column_name, "
        f"rc.owner, rc.table_name, rcc.column_name "
        f"FROM all_constraints c "
        f"JOIN all_cons_columns cc  ON c.constraint_name   = cc.constraint_name  AND c.owner  = cc.owner "
        f"JOIN all_constraints rc   ON c.r_constraint_name = rc.constraint_name  AND c.owner  = rc.owner "
        f"JOIN all_cons_columns rcc ON rc.constraint_name  = rcc.constraint_name AND rc.owner = rcc.owner "
        f"                        AND cc.position = rcc.position "
        f"WHERE c.constraint_type = 'R' AND {wh} ORDER BY c.table_name, cc.column_name",
        bind,
    )
    refs: List[Dict[str, Any]] = []
    for row in cursor.fetchall():
        refs.append(
            {
                "from_table": f"{row[0]}.{row[1]}",
                "from_col": row[2],
                "to_table": f"{row[3]}.{row[4]}",
                "to_col": row[5],
            }
        )

    cursor.close()
    return {
        "tables": tables,
        "columns": columns,
        "pks": pks,
        "uqs": uqs,
        "refs": refs,
        "table_comments": table_comments if with_comments else {},
    }


def table_short_name(table_name: str) -> str:
    return table_name.split(".", 1)[1] if "." in table_name else table_name


def classify_group(owner: str, table_name: str) -> Optional[str]:
    ow = owner.upper()
    tn = table_name.upper()
    if ow.startswith("RAT") or ow in {"RATS", "TAMS"} or tn.startswith("RAT_"):
        return "rats"
    if ow.startswith("TML") or tn.startswith("TML_"):
        return "tml"
    return None


def classify_airsoft_module(table_name: str) -> Optional[str]:
    short = table_short_name(table_name).upper()
    for module_id, _label, prefixes in AIRSOFT_MODULES:
        if any(short.startswith(prefix) for prefix in prefixes):
            return module_id
    return None


def generate_ai_table_explanation(
    table_name: str,
    columns: List[Dict[str, Any]],
    pks: List[str],
    refs_out: List[Dict[str, Any]],
    refs_in: List[Dict[str, Any]],
    table_comment: Optional[str],
) -> str:
    """
    Descripcion **determinista** orientada a "para que sirve", usando solo
    nombres de columna, PK, FK, conteos y comentario Oracle (si existe).
    No afirma procesos de negocio no respaldados por comentarios/metadata.
    """
    short = table_short_name(table_name)
    n_cols = len(columns)
    pk_text = ", ".join(pks[:3]) if pks else "sin PK declarada"
    fk_out = len(refs_out)
    fk_in = len(refs_in)
    nullable_count = sum(1 for c in columns if c.get("nullable"))
    required_count = max(0, n_cols - nullable_count)

    hint_cols = []
    for c in columns:
        name = (c.get("name") or "").upper()
        if any(k in name for k in ("ID", "COD", "CODE", "STATUS", "ESTADO", "FECHA", "DATE", "EMP", "USER")):
            hint_cols.append(c.get("name"))
    hint_cols = [c for c in hint_cols if c][:4]
    hint_text = ", ".join(hint_cols) if hint_cols else "sin columnas clave detectadas por nombre"

    to_tables = sorted({table_short_name(r.get("to_table", "")) for r in refs_out if r.get("to_table")})[:4]
    from_tables = sorted({table_short_name(r.get("from_table", "")) for r in refs_in if r.get("from_table")})[:4]
    out_text = ", ".join(to_tables) if to_tables else "ninguna"
    in_text = ", ".join(from_tables) if from_tables else "ninguna"

    if table_comment and table_comment.strip():
        base_purpose = (
            f"Segun comentario Oracle, esta entidad almacena o describe: {table_comment.strip()}. "
        )
    else:
        base_purpose = (
            f"Con los datos disponibles (sin comentario de tabla en Oracle), "
            f"`{short}` parece un repositorio de {n_cols} atributos sobre la entidad nombrada en la tabla, "
            f"con {required_count} de ellos requeridos y el resto opcionales. "
        )

    links = (
        f"Conecta con otras entidades: {fk_out} salida(s) hacia [{out_text}] y "
        f"{fk_in} entrada(s) desde [{in_text}]. "
    )

    identity = (
        f"Identificacion/unicidad: PK [{pk_text}]. "
    )

    hints = (
        f"Nombres de columna sugeridos como relevantes: {hint_text} "
        f"(inferencia heuristica, no un proceso de negocio). "
    )

    disclaimer = (
        "Esto se deduce de metadatos; si falta comentario Oracle, revise PK/FK y nombres de columna en el ERD."
    )

    return " ".join([base_purpose, links, identity, hints, disclaimer])


def generate_ollama_table_explanation(
    table_name: str,
    columns: List[Dict[str, Any]],
    pks: List[str],
    refs: List[Dict[str, Any]],
    table_comment: Optional[str],
    ollama_url: str,
    ollama_model: str,
    timeout_sec: int = 45,
) -> str:
    """
    Generate table description using local Ollama (free/local AI).
    Falls back via caller if request fails.
    """
    short = table_short_name(table_name)
    sample_cols = [
        {"name": c.get("name"), "type": c.get("type"), "nullable": c.get("nullable")}
        for c in columns[:20]
    ]
    ref_targets = [r.get("to_table") for r in refs[:10] if r.get("to_table")]
    prompt = (
        "Eres un arquitecto de datos del ERD. "
        "Redacta una descripcion breve (2 oraciones maximo, espanol) "
        "de la finalidad de esta tabla y sus datos clave. "
        "No inventes procesos fuera de la metadata. "
        f"Tabla: {short}. "
        f"Comentario Oracle: {table_comment or 'N/A'}. "
        f"PK: {pks or []}. "
        f"Columnas ejemplo: {sample_cols}. "
        f"Relaciona hacia: {ref_targets}."
    )
    payload = {
        "model": ollama_model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2},
    }
    req = urllib.request.Request(
        url=f"{ollama_url.rstrip('/')}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as res:
        body = res.read().decode("utf-8", errors="replace")
    parsed = json.loads(body)
    text = (parsed.get("response") or "").strip()
    if not text:
        raise RuntimeError(f"Ollama no devolvio texto para {short}")
    return text


def build_ai_explanations(group_schema: Dict[str, Any]) -> Dict[str, str]:
    refs_by_from: Dict[str, List[Dict[str, Any]]] = {}
    refs_by_to: Dict[str, List[Dict[str, Any]]] = {}
    for r in group_schema.get("refs", []):
        refs_by_from.setdefault(r["from_table"], []).append(r)
        refs_by_to.setdefault(r["to_table"], []).append(r)

    explanations: Dict[str, str] = {}
    for table_name in group_schema.get("tables", []):
        explanations[table_name] = generate_ai_table_explanation(
            table_name=table_name,
            columns=group_schema.get("columns", {}).get(table_name, []),
            pks=group_schema.get("pks", {}).get(table_name, []),
            refs_out=refs_by_from.get(table_name, []),
            refs_in=refs_by_to.get(table_name, []),
            table_comment=group_schema.get("table_comments", {}).get(table_name),
        )
    return explanations


def normalize_names(group_schema: Dict[str, Any]) -> Dict[str, Any]:
    tables = group_schema["tables"]
    short_names = [table_short_name(t) for t in tables]
    use_short = len(short_names) == len(set(short_names))
    if not use_short:
        return group_schema

    name_map = {t: table_short_name(t) for t in tables}
    new_tables = [name_map[t] for t in tables]
    new_columns = {name_map[t]: group_schema["columns"].get(t, []) for t in tables}
    new_pks = {name_map[t]: group_schema["pks"].get(t, []) for t in tables}
    new_uqs = {name_map[t]: group_schema.get("uqs", {}).get(t, []) for t in tables}
    new_uqs = {k: v for k, v in new_uqs.items() if v}
    new_tc = {
        name_map[t]: group_schema.get("table_comments", {}).get(t)
        for t in tables
        if group_schema.get("table_comments", {}).get(t)
    }
    ai_raw = group_schema.get("ai_table_explanations", {})
    new_ai = {
        name_map[t]: ai_raw.get(t)
        for t in tables
        if ai_raw.get(t)
    }

    new_refs = []
    for r in group_schema.get("refs", []):
        ft = name_map.get(r["from_table"])
        tt = name_map.get(r["to_table"])
        if ft and tt:
            new_refs.append(
                {
                    "from_table": ft,
                    "from_col": r["from_col"],
                    "to_table": tt,
                    "to_col": r["to_col"],
                }
            )
    return {
        "tables": new_tables,
        "columns": new_columns,
        "pks": new_pks,
        "uqs": new_uqs,
        "refs": new_refs,
        "table_comments": new_tc,
        "ai_table_explanations": new_ai,
    }


def group_from_tables(schema: Dict[str, Any], group_key: str, tables: List[str]) -> Dict[str, Any]:
    keep: Set[str] = set(tables)
    refs = [
        r
        for r in schema.get("refs", [])
        if r["from_table"] in keep and r["to_table"] in keep
    ]
    group_schema = {
        "name": group_key.upper(),
        "tables": sorted(tables),
        "columns": {t: schema.get("columns", {}).get(t, []) for t in tables},
        "pks": {t: schema.get("pks", {}).get(t, []) for t in tables},
        "uqs": {
            t: schema.get("uqs", {}).get(t, [])
            for t in tables
            if schema.get("uqs", {}).get(t)
        },
        "refs": refs,
        "table_comments": {
            t: schema.get("table_comments", {}).get(t)
            for t in tables
            if schema.get("table_comments", {}).get(t)
        },
    }
    group_schema["ai_table_explanations"] = build_ai_explanations(group_schema)
    normalized = normalize_names(group_schema)
    normalized["name"] = group_key.upper()
    return normalized


def ensure_ai_explanations(
    payload: Dict[str, Any],
    dataset_id: str,
    ai_mode: str,
    ollama_url: str,
    ollama_model: str,
    ai_cache: Dict[str, str],
) -> Dict[str, Any]:
    """
    Ensure each table has an AI-style explanation in payload.
    """
    ai = dict(payload.get("ai_table_explanations", {}))
    refs_by_from: Dict[str, List[Dict[str, Any]]] = {}
    refs_by_to: Dict[str, List[Dict[str, Any]]] = {}
    for r in payload.get("refs", []):
        refs_by_from.setdefault(r["from_table"], []).append(r)
        refs_by_to.setdefault(r["to_table"], []).append(r)

    for table_name in payload.get("tables", []):
        if ai.get(table_name):
            continue
        cache_key = f"v{AI_EXPL_CACHE_VER}:{dataset_id}:{table_name}"
        if ai_cache.get(cache_key):
            ai[table_name] = ai_cache[cache_key]
            continue

        columns = payload.get("columns", {}).get(table_name, [])
        pks = payload.get("pks", {}).get(table_name, [])
        refs = refs_by_from.get(table_name, [])
        refs_in = refs_by_to.get(table_name, [])
        table_comment = payload.get("table_comments", {}).get(table_name)

        text: str
        if ai_mode == "ollama":
            try:
                text = generate_ollama_table_explanation(
                    table_name=table_name,
                    columns=columns,
                    pks=pks,
                    refs=refs,
                    table_comment=table_comment,
                    ollama_url=ollama_url,
                    ollama_model=ollama_model,
                )
            except (urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as err:
                print(f"Aviso: fallback heuristico para {table_name}: {err}")
                text = generate_ai_table_explanation(table_name, columns, pks, refs, refs_in, table_comment)
        else:
            text = generate_ai_table_explanation(table_name, columns, pks, refs, refs_in, table_comment)

        ai[table_name] = text
        ai_cache[cache_key] = text
    payload["ai_table_explanations"] = ai
    return payload


def build_groups(schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    grouped_tables: Dict[str, List[str]] = {"rats": [], "tml": []}
    airsoft_tables: Dict[str, List[str]] = {
        module_id: [] for module_id, _label, _prefixes in AIRSOFT_MODULES
    }

    for table in schema.get("tables", []):
        owner = table.split(".", 1)[0] if "." in table else ""
        base_group = classify_group(owner, table)
        if base_group:
            grouped_tables[base_group].append(table)

        airsoft_group = classify_airsoft_module(table)
        if airsoft_group:
            airsoft_tables[airsoft_group].append(table)

    results: Dict[str, Dict[str, Any]] = {}
    for group_key, tables in grouped_tables.items():
        results[group_key] = group_from_tables(schema, group_key, tables)
    for module_id, _label, _prefixes in AIRSOFT_MODULES:
        results[module_id] = group_from_tables(schema, module_id, airsoft_tables[module_id])
    all_air: Set[str] = set()
    for _mid, tlist in airsoft_tables.items():
        all_air.update(tlist)
    if all_air:
        results[AIRSOFT_DATASET_ID] = group_from_tables(
            schema, AIRSOFT_DATASET_ID, sorted(all_air)
        )
    return results


def build_groups_airsoft_only(schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build only Airsoft datasets (full + modules) from a schema.
    Useful for mixed-mode where Airsoft comes from DB2 and RATS/TML from Oracle.
    """
    airsoft_tables: Dict[str, List[str]] = {
        module_id: [] for module_id, _label, _prefixes in AIRSOFT_MODULES
    }
    for table in schema.get("tables", []):
        airsoft_group = classify_airsoft_module(table)
        if airsoft_group:
            airsoft_tables[airsoft_group].append(table)

    results: Dict[str, Dict[str, Any]] = {}
    for module_id, _label, _prefixes in AIRSOFT_MODULES:
        results[module_id] = group_from_tables(schema, module_id, airsoft_tables[module_id])

    all_air: Set[str] = set()
    for _mid, tlist in airsoft_tables.items():
        all_air.update(tlist)
    results[AIRSOFT_DATASET_ID] = group_from_tables(schema, AIRSOFT_DATASET_ID, sorted(all_air))
    return results


def merge_existing_schema(output_dir: Path) -> Dict[str, Any]:
    merged = {
        "tables": [],
        "columns": {},
        "pks": {},
        "uqs": {},
        "refs": [],
        "table_comments": {},
        "ai_table_explanations": {},
    }
    seen_tables: Set[str] = set()

    for file_name in ("rats.schema.json", "tml.schema.json"):
        src = output_dir / file_name
        if not src.exists():
            continue
        data = json.loads(src.read_text(encoding="utf-8"))
        for t in data.get("tables", []):
            if t not in seen_tables:
                seen_tables.add(t)
                merged["tables"].append(t)
        merged["columns"].update(data.get("columns", {}))
        merged["pks"].update(data.get("pks", {}))
        merged["uqs"].update(data.get("uqs", {}))
        merged["table_comments"].update(data.get("table_comments", {}))
        merged["ai_table_explanations"].update(data.get("ai_table_explanations", {}))
        merged["refs"].extend(data.get("refs", []))

    merged["refs"] = [
        {"from_table": r["from_table"], "from_col": r["from_col"], "to_table": r["to_table"], "to_col": r["to_col"]}
        for r in merged["refs"]
        if r.get("from_table") in seen_tables and r.get("to_table") in seen_tables
    ]
    return merged


def load_existing_base_groups(output_dir: Path) -> Dict[str, Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for dataset_id in ("rats", "tml"):
        src = output_dir / f"{dataset_id}.schema.json"
        if src.exists():
            groups[dataset_id] = json.loads(src.read_text(encoding="utf-8"))
        else:
            groups[dataset_id] = {
                "name": dataset_id.upper(),
                "tables": [],
                "columns": {},
                "pks": {},
                "uqs": {},
                "refs": [],
                "table_comments": {},
                "ai_table_explanations": {},
            }
    return groups


def manifest_label(dataset_id: str) -> str:
    if dataset_id == "rats":
        return "RATS"
    if dataset_id == "tml":
        return "TML"
    if dataset_id == AIRSOFT_DATASET_ID:
        return AIRSOFT_TOP_LABEL
    for module_id, label, _prefixes in AIRSOFT_MODULES:
        if module_id == dataset_id:
            return label
    return dataset_id.upper()


def write_outputs(
    grouped: Dict[str, Dict[str, Any]],
    output_dir: Path,
    include_empty_tabs: bool = False,
    ai_mode: str = "heuristic",
    ollama_url: str = "http://127.0.0.1:11434",
    ollama_model: str = "",
) -> None:
    # Compat: antes filtrabamos pestañas vacias; ahora el manifest v2 siempre lista módulos.
    _ = include_empty_tabs
    ordered_ids = (
        ["rats", "tml", AIRSOFT_DATASET_ID] + [module_id for module_id, _label, _prefixes in AIRSOFT_MODULES]
    )
    cache_path = output_dir / ".ai_explanations_cache.json"
    if cache_path.exists():
        try:
            ai_cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            ai_cache = {}
    else:
        ai_cache = {}

    # Siempre generamos un JSON por id (aunque tenga 0 tablas) para que el visor
    # y el manifest puedan referenciar modulos vacios sin perder el archivo.
    written: Dict[str, Dict[str, Any]] = {}
    for dataset_id in ordered_ids:
        payload = grouped.get(
            dataset_id,
            {
                "name": dataset_id.upper(),
                "tables": [],
                "columns": {},
                "pks": {},
                "uqs": {},
                "refs": [],
                "table_comments": {},
                "ai_table_explanations": {},
            },
        )
        payload = ensure_ai_explanations(
            payload=payload,
            dataset_id=dataset_id,
            ai_mode=ai_mode,
            ollama_url=ollama_url,
            ollama_model=ollama_model,
            ai_cache=ai_cache,
        )
        out_path = output_dir / f"{dataset_id}.schema.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        print(f"OK {out_path.name}: {len(payload['tables'])} tablas, {len(payload['refs'])} relaciones")

        written[dataset_id] = {
            "id": dataset_id,
            "label": manifest_label(dataset_id),
            "file": out_path.name,
            "tables": len(payload["tables"]),
            "relations": len(payload["refs"]),
        }

    by_id: Dict[str, Dict[str, Any]] = dict(written)
    # Publicar los 5 modulos 4..8 aunque alguno venga con 0 tablas: el JSON existe y el manifest
    # conserva el conteo real (0) para que el UI muestre la fila completa.
    airsoft_children: List[Dict[str, Any]] = []
    for module_id, _label, _prefixes in AIRSOFT_MODULES:
        w = by_id.get(module_id)
        if not w:
            continue
        airsoft_children.append(
            {
                "id": w["id"],
                "label": w["label"],
                "file": w["file"],
                "kind": "airsoft_module",
                "tables": w.get("tables"),
                "relations": w.get("relations"),
            }
        )
    top_level: List[Dict[str, Any]] = []
    for key in ("rats", "tml"):
        if key in by_id:
            w = by_id[key]
            top_level.append(
                {
                    "id": w["id"],
                    "label": w["label"],
                    "file": w["file"],
                    "kind": "base",
                    "tables": w.get("tables"),
                    "relations": w.get("relations"),
                }
            )
    if AIRSOFT_DATASET_ID in by_id:
        w = by_id[AIRSOFT_DATASET_ID]
        top_level.append(
            {
                "id": w["id"],
                "label": w["label"],
                "file": w["file"],
                "kind": "airsoft_root",
                "children": airsoft_children,
                "tables": w.get("tables"),
                "relations": w.get("relations"),
            }
        )

    manifest_obj: Dict[str, Any] = {
        "version": 2,
        "topLevel": top_level,
    }

    manifest_path = output_dir / "datasets.manifest.json"
    manifest_path.write_text(json.dumps(manifest_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK {manifest_path.name}: topLevel={len(manifest_obj['topLevel'])} (manifest v2)")
    cache_path.write_text(json.dumps(ai_cache, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Genera data ERD (RATS/TML + módulos) desde Oracle o DB2 usando .env"
    )
    parser.add_argument(
        "--db",
        choices=("oracle", "db2", "mixed"),
        default="oracle",
        help="Motor de BD: oracle, db2 (DB2 LUW via SYSCAT), o mixed (Oracle para RATS/TML + DB2 para Airsoft)",
    )
    parser.add_argument(
        "--env-file",
        default="erd/.env",
        help=(
            "Ruta del .env. Oracle: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_SERVICE. "
            "DB2: DB2_HOST, DB2_PORT, DB2_DBNAME, DB2_USER, DB2_PASSWORD (opc: DB2_SECURITY)"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="erd/data",
        help="Directorio de salida para los JSON",
    )
    parser.add_argument(
        "--with-comments",
        action="store_true",
        help="Incluye comentarios de tablas/columnas (Oracle o DB2 si existen)",
    )
    parser.add_argument(
        "--from-existing",
        action="store_true",
        help="No usa BD: toma rats/tml actuales y genera módulos + manifest",
    )
    parser.add_argument(
        "--include-empty-tabs",
        action="store_true",
        help="Incluye pestañas sin tablas en datasets.manifest.json",
    )
    parser.add_argument(
        "--ai-mode",
        choices=("auto", "heuristic", "ollama"),
        default="heuristic",
        help="Modo de descripciones IA: auto, heuristico local, o Ollama local",
    )
    parser.add_argument(
        "--ollama-url",
        default="",
        help="URL de Ollama local (default env OLLAMA_URL o http://127.0.0.1:11434)",
    )
    parser.add_argument(
        "--ollama-model",
        default="",
        help="Modelo Ollama (default env OLLAMA_MODEL)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    output_dir = (repo_root / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    env_path = (repo_root / args.env_file).resolve()
    env_data: Dict[str, str] = {}
    if env_path.exists():
        env_data = load_env_file(env_path)
    # Allow CI secrets via environment variables
    for key in [
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_PASSWORD",
        "DB_SERVICE",
        "DB2_HOST",
        "DB2_PORT",
        "DB2_DBNAME",
        "DB2_USER",
        "DB2_PASSWORD",
        "DB2_SECURITY",
        "OLLAMA_URL",
        "OLLAMA_MODEL",
    ]:
        val = os.environ.get(key)
        if val:
            env_data[key] = val

    ollama_url = args.ollama_url or env_data.get("OLLAMA_URL", "http://127.0.0.1:11434")
    ollama_model = args.ollama_model or env_data.get("OLLAMA_MODEL", "")
    ai_mode = args.ai_mode
    if ai_mode == "auto":
        ai_mode = "ollama" if ollama_model else "heuristic"
    if ai_mode == "ollama" and not ollama_model:
        raise ValueError("ai-mode=ollama requiere --ollama-model o OLLAMA_MODEL en .env")

    if args.from_existing:
        print("Modo from-existing: usando JSON actuales (rats/tml) para derivar módulos.")
        schema = merge_existing_schema(output_dir)
        grouped = load_existing_base_groups(output_dir)
        derived = build_groups(schema)
        for module_id, _label, _prefixes in AIRSOFT_MODULES:
            grouped[module_id] = derived.get(
                module_id,
                {
                    "name": module_id.upper(),
                    "tables": [],
                    "columns": {},
                    "pks": {},
                    "uqs": {},
                    "refs": [],
                    "table_comments": {},
                    "ai_table_explanations": {},
                },
            )
        grouped[AIRSOFT_DATASET_ID] = derived.get(
            AIRSOFT_DATASET_ID,
            {
                "name": AIRSOFT_DATASET_ID.upper(),
                "tables": [],
                "columns": {},
                "pks": {},
                "uqs": {},
                "refs": [],
                "table_comments": {},
                "ai_table_explanations": {},
            },
        )
    else:
        if not env_data:
            raise FileNotFoundError(f"No existe el archivo .env: {env_path}")
        if args.db == "oracle":
            required = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_SERVICE"]
            missing = [k for k in required if not env_data.get(k)]
            if missing:
                raise ValueError(f"Faltan variables Oracle en {env_path}: {', '.join(missing)}")

            db_config = {
                "host": env_data["DB_HOST"],
                "port": int(env_data.get("DB_PORT", "1521")),
                "user": env_data["DB_USER"],
                "password": env_data["DB_PASSWORD"],
                "service_name": env_data["DB_SERVICE"],
            }

            print("Conectando a Oracle para extraer esquema...")
            conn = get_db_connection_oracle(db_config)
            try:
                schema = get_schema(
                    conn,
                    with_comments=args.with_comments,
                    owners=None,
                )
            finally:
                conn.close()
        elif args.db == "db2":
            required = ["DB2_HOST", "DB2_PORT", "DB2_DBNAME", "DB2_USER", "DB2_PASSWORD"]
            missing = [k for k in required if not env_data.get(k)]
            if missing:
                raise ValueError(f"Faltan variables DB2 en {env_path}: {', '.join(missing)}")

            db2_config = {
                "host": env_data["DB2_HOST"],
                "port": int(env_data.get("DB2_PORT", "50000")),
                "dbname": env_data["DB2_DBNAME"],
                "user": env_data["DB2_USER"],
                "password": env_data["DB2_PASSWORD"],
                "security": env_data.get("DB2_SECURITY") or None,
            }

            print("Conectando a DB2 para extraer esquema...")
            conn = get_db_connection_db2(db2_config)
            try:
                schema = get_schema_db2(
                    conn,
                    with_comments=args.with_comments,
                    schemas=None,
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            required_oracle = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_SERVICE"]
            missing_oracle = [k for k in required_oracle if not env_data.get(k)]
            if missing_oracle:
                raise ValueError(f"Faltan variables Oracle (modo mixed) en {env_path}: {', '.join(missing_oracle)}")

            required_db2 = ["DB2_HOST", "DB2_PORT", "DB2_DBNAME", "DB2_USER", "DB2_PASSWORD"]
            missing_db2 = [k for k in required_db2 if not env_data.get(k)]
            if missing_db2:
                raise ValueError(f"Faltan variables DB2 (modo mixed) en {env_path}: {', '.join(missing_db2)}")

            oracle_cfg = {
                "host": env_data["DB_HOST"],
                "port": int(env_data.get("DB_PORT", "1521")),
                "user": env_data["DB_USER"],
                "password": env_data["DB_PASSWORD"],
                "service_name": env_data["DB_SERVICE"],
            }
            db2_cfg = {
                "host": env_data["DB2_HOST"],
                "port": int(env_data.get("DB2_PORT", "50000")),
                "dbname": env_data["DB2_DBNAME"],
                "user": env_data["DB2_USER"],
                "password": env_data["DB2_PASSWORD"],
                "security": env_data.get("DB2_SECURITY") or None,
            }

            print("Conectando a Oracle (RATS/TML) y DB2 (Airsoft) para extraer esquema...")
            conn_oracle = get_db_connection_oracle(oracle_cfg)
            try:
                oracle_schema = get_schema(
                    conn_oracle,
                    with_comments=args.with_comments,
                    owners=None,
                )
            finally:
                conn_oracle.close()

            conn_db2 = get_db_connection_db2(db2_cfg)
            try:
                db2_schema = get_schema_db2(
                    conn_db2,
                    with_comments=args.with_comments,
                    schemas=None,
                )
            finally:
                try:
                    conn_db2.close()
                except Exception:
                    pass

            grouped = {}
            base = build_groups(oracle_schema)
            grouped["rats"] = base.get("rats", {})
            grouped["tml"] = base.get("tml", {})
            air = build_groups_airsoft_only(db2_schema)
            grouped.update(air)

        if args.db != "mixed":
            grouped = build_groups(schema)
    print(f"Modo descripciones IA: {ai_mode}")
    write_outputs(
        grouped,
        output_dir,
        include_empty_tabs=args.include_empty_tabs,
        ai_mode=ai_mode,
        ollama_url=ollama_url,
        ollama_model=ollama_model,
    )

    if not grouped.get("rats", {}).get("tables"):
        print("Aviso: no se detectaron tablas para RATS.")
    if not grouped.get("tml", {}).get("tables"):
        print("Aviso: no se detectaron tablas para TML.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
