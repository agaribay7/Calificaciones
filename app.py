# app.py
"""
Streamlit app — Calificar Alineaciones — Google Sheets
Versión: ajuste automático del ancho de la barra lateral para que el selector
de evaluador quepa por defecto (sin que el usuario tenga que redimensionar).
Mejoras: normalización de ids, lectura mínima A2:K para index, batching, prevención de duplicados,
descarga sin id/timestamp y Fecha Partido sin hora.
Correcciones: persistencia del selectbox al hacer submit con Enter y permitir editar jornadas ya enviadas.
Optimización: prefetch de A2:K para acelerar comparaciones y CHUNK aumentado.
"""

import base64
import hashlib
import io
import json
import logging
import os
import random
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# -------- CONFIG LOGGER --------
st.set_page_config(page_title="Calificar Alineaciones — Google Sheets", layout="wide")
logger = logging.getLogger("calificaciones_app")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

# -------- CONSTANTS / CONFIG --------
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OQRTq818EXBeibBmWlrcSZm83do264l2mb5jnBmzsu8"
ALIGN_WORKSHEET_NAME = "Alineaciones"
RATINGS_SHEET_NAME = "Calificaciones"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

COLUMN_MAP = {
    "Minutos Jugados": "minutos",
    "Minutos": "minutos",
    "Jugador": "jugador",
    "Nombre": "jugador",
    "Player": "jugador",
    "Fecha Partido": "fecha_partido",
    "Fecha": "fecha_partido",
    "Gol": "gol",
    "Asistencia": "asistencia",
    "Resultado Partido": "resultado_partido",
    "Resultado": "resultado_partido",
    "Jornada": "jornada",
}

EXPECTED_RATINGS_HEADERS = [
    "id",
    "Jornada",
    "Jugador",
    "Evaluador",
    "Calificacion",
    "Minutos",
    "Gol",
    "Asistencia",
    "Resultado",
    "Fecha Partido",
    "timestamp",
]

# -------- Helpers --------
def safe_key(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", str(s or ""))


def normalize_str(s: str) -> str:
    """Normaliza texto para evitar diferencias por mayúsculas, tildes y espacios extras."""
    if s is None:
        return ""
    s2 = str(s).strip().lower()
    s2 = unicodedata.normalize("NFKD", s2)
    s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
    s2 = re.sub(r"\s+", " ", s2)
    return s2


def make_row_id(jornada: str, jugador: str, evaluador: str) -> str:
    base = f"{normalize_str(jornada)}|{normalize_str(jugador)}|{normalize_str(evaluador)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_get(d: dict, k: str, default=None):
    v = d.get(k, default)
    if isinstance(v, str):
        return v.strip()
    return v


# Retry wrapper (con backoff y jitter)
def retry_with_backoff(func, max_retries=5, base=1.0, max_backoff=16.0, *args, **kwargs):
    attempt = 0
    last_exc = None
    while attempt < max_retries:
        try:
            return func(*args, **kwargs)
        except APIError as e:
            attempt += 1
            wait = min(max_backoff, base * (2 ** (attempt - 1)))
            jitter = random.uniform(0, wait * 0.25)
            logger.warning(f"APIError (intento {attempt}/{max_retries}): {e}. Esperando {wait + jitter:.1f}s antes de reintentar.")
            time.sleep(wait + jitter)
            last_exc = e
        except Exception as e:
            logger.exception("Error no recuperable en operación gspread")
            raise
    raise last_exc


def safe_get_all_records(ws):
    try:
        return retry_with_backoff(lambda: ws.get_all_records())
    except Exception:
        logger.exception("Fallo seguro al leer get_all_records()")
        return []


# Credenciales & gspread (cacheado cliente)
def get_credentials_from_st_secrets_or_env(scopes):
    try:
        if "SERVICE_ACCOUNT_JSON" in st.secrets:
            sa_raw = st.secrets["SERVICE_ACCOUNT_JSON"]
            if isinstance(sa_raw, dict):
                sa_info = sa_raw
            else:
                sa_info = json.loads(sa_raw)
            return Credentials.from_service_account_info(sa_info, scopes=scopes)
    except Exception:
        logger.exception("SERVICE_ACCOUNT_JSON presente pero no parseable")

    try:
        if "gcp_service_account" in st.secrets:
            sa_info = st.secrets["gcp_service_account"]
            return Credentials.from_service_account_info(sa_info, scopes=scopes)
    except Exception:
        logger.exception("gcp_service_account presente pero no válido")

    gpath = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gpath:
        try:
            return Credentials.from_service_account_file(gpath, scopes=scopes)
        except Exception:
            logger.exception("Error cargando GOOGLE_APPLICATION_CREDENTIALS")

    return None


@st.cache_resource(hash_funcs={Credentials: lambda _: None})
def get_gspread_client_cached(creds):
    if creds is None:
        return None
    return gspread.authorize(creds)


def connect_gsheets(creds, sheet_url):
    if creds is None:
        return None
    try:
        client = get_gspread_client_cached(creds)
        if client is None:
            return None
        sh = client.open_by_url(sheet_url)
        return sh
    except Exception:
        logger.exception("Error autenticando/abriendo la Google Sheet")
        return None


def get_or_create_ratings_ws(sh, title=RATINGS_SHEET_NAME):
    try:
        try:
            ws = sh.worksheet(title)
        except Exception:
            ws = sh.add_worksheet(title=title, rows="2000", cols="20")
        headers = ws.row_values(1)
        if not headers or len(headers) < len(EXPECTED_RATINGS_HEADERS) or (headers and headers[0].strip().lower() != "id"):
            ws.update("A1", [EXPECTED_RATINGS_HEADERS])
        return ws
    except Exception:
        logger.exception("Error obteniendo/creando la pestaña de Calificaciones")
        return None


# ---------------- Simple in-memory cache (session_state) con TTL ----------------
def _session_cache_get(key: str):
    entry = st.session_state.get("_cache_internal", {}).get(key)
    if not entry:
        return None
    value, ts, ttl = entry.get("value"), entry.get("ts"), entry.get("ttl")
    if (time.time() - ts) > ttl:
        st.session_state["_cache_internal"].pop(key, None)
        return None
    return value


def _session_cache_set(key: str, value, ttl: int = 30):
    if "_cache_internal" not in st.session_state:
        st.session_state["_cache_internal"] = {}
    st.session_state["_cache_internal"][key] = {"value": value, "ts": time.time(), "ttl": ttl}


def _session_cache_invalidate(key_prefix: str):
    if "_cache_internal" not in st.session_state:
        return
    for k in list(st.session_state["_cache_internal"].keys()):
        if k.startswith(key_prefix):
            st.session_state["_cache_internal"].pop(k, None)


# ---------------- Reads (usando cache ligera) ----------------
def load_alignments_cached(sh, worksheet_name=ALIGN_WORKSHEET_NAME, ttl_seconds=30) -> pd.DataFrame:
    cache_key = f"alignments::{worksheet_name}::{SHEET_URL}"
    cached = _session_cache_get(cache_key)
    if cached is not None:
        return cached
    df = load_alignments_uncached(sh, worksheet_name)
    _session_cache_set(cache_key, df, ttl=ttl_seconds)
    return df


def load_ratings_cached(ws, ttl_seconds=15) -> pd.DataFrame:
    cache_key = f"ratings::{ws.id if hasattr(ws,'id') else 'unknown'}::{SHEET_URL}"
    cached = _session_cache_get(cache_key)
    if cached is not None:
        return cached
    df = load_ratings_uncached(ws)
    _session_cache_set(cache_key, df, ttl=ttl_seconds)
    return df


def load_alignments_uncached(sh, worksheet_name=ALIGN_WORKSHEET_NAME) -> pd.DataFrame:
    try:
        try:
            ws_align = sh.worksheet(worksheet_name)
        except Exception:
            ws_align = sh.sheet1
        records = safe_get_all_records(ws_align)
        df = pd.DataFrame(records)
        if df.empty:
            return df
        rename_map = {}
        for col in df.columns:
            if col in COLUMN_MAP:
                rename_map[col] = COLUMN_MAP[col]
            else:
                low = str(col).strip().lower()
                for k, v in COLUMN_MAP.items():
                    if k.lower() == low:
                        rename_map[col] = v
                        break
        if rename_map:
            df = df.rename(columns=rename_map)
        if "fecha_partido" in df.columns:
            df["fecha_partido"] = pd.to_datetime(df["fecha_partido"], errors="coerce")
        for ncol in ["minutos", "gol", "asistencia"]:
            if ncol in df.columns:
                df[ncol] = pd.to_numeric(df[ncol], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        logger.exception("Error cargando la hoja de alineaciones (uncached)")
        return pd.DataFrame()


def load_ratings_uncached(ws) -> pd.DataFrame:
    try:
        records = safe_get_all_records(ws)
        if not records:
            return pd.DataFrame(columns=EXPECTED_RATINGS_HEADERS)
        df = pd.DataFrame(records)
        for col in ["id", "Jornada", "Jugador", "Evaluador", "Calificacion", "timestamp"]:
            if col not in df.columns:
                df[col] = None
        return df
    except Exception:
        logger.exception("Error leyendo calificaciones (uncached)")
        return pd.DataFrame()


# -------- Write helpers (batch update / append, condicional) --------
def append_rows(ws, rows):
    if not rows:
        return
    retry_with_backoff(lambda: ws.append_rows(rows, value_input_option="USER_ENTERED"))


def batch_update_rows(spreadsheet, updates: List[Dict]):
    if not updates:
        return
    body = {"valueInputOption": "USER_ENTERED", "data": updates}
    retry_with_backoff(lambda: spreadsheet.batch_update(body))


def normalize_and_validate_record(r: dict) -> dict:
    out = dict(r)
    try:
        cal = out.get("Calificacion", "")
        if cal is None or cal == "":
            out["Calificacion"] = ""
        else:
            c = float(cal)
            if c < 0:
                c = 0.0
            if c > 10:
                c = 10.0
            out["Calificacion"] = float(round(c * 2) / 2)
    except Exception:
        out["Calificacion"] = ""
    try:
        m = out.get("Minutos", "")
        if pd.isna(m) or m in (None, ""):
            out["Minutos"] = ""
        else:
            out["Minutos"] = int(m)
    except Exception:
        out["Minutos"] = ""
    try:
        out["Gol"] = int(out.get("Gol", 0) or 0)
    except Exception:
        out["Gol"] = 0
    try:
        out["Asistencia"] = int(out.get("Asistencia", 0) or 0)
    except Exception:
        out["Asistencia"] = 0
    return out


# -------- Optimized helpers para index/ts (lectura mínima) --------

def read_ratings_index(ws):
    """Intenta leer solo el rango A2:K (id..timestamp) y construir mapas id->rownum y id->timestamp.
    Si falla, hace fallback a get_all_records().
    Devuelve (id_to_rownum, id_to_timestamp).
    """
    try:
        rows = retry_with_backoff(lambda: ws.get("A2:K"))
        id_to_rownum = {}
        id_to_ts = {}
        for i, row in enumerate(rows, start=2):
            rid = str(row[0]).strip() if len(row) > 0 else ""
            ts = str(row[10]).strip() if len(row) > 10 else ""
            if rid:
                id_to_rownum[rid] = i
                id_to_ts[rid] = ts
        return id_to_rownum, id_to_ts
    except Exception:
        logger.exception("No se pudo leer A2:K; fallback a get_all_records() para indexar ids")
        records = safe_get_all_records(ws)
        id_to_rownum = {}
        id_to_ts = {}
        for idx, r in enumerate(records, start=2):
            rid = str(r.get("id", "")).strip()
            ts = str(r.get("timestamp", "") or "")
            if rid:
                id_to_rownum[rid] = idx
                id_to_ts[rid] = ts
        return id_to_rownum, id_to_ts


# -------- App UI / Logic --------
st.title("Calificar jugadores por jornada")

# Sidebar: logo opcional (queda en la sidebar)
LOGO_FILENAME = "logo_transparent.png"
try:
    app_dir = Path(__file__).parent
except Exception:
    app_dir = Path.cwd()
logo_path = app_dir / LOGO_FILENAME
if logo_path.exists():
    try:
        from PIL import Image

        logo_img = Image.open(logo_path).convert("RGBA")
        w, h = logo_img.size
        new_w = 240
        new_h = int(h * new_w / w)
        logo_img = logo_img.resize((new_w, new_h), Image.LANCZOS)
        buffered = io.BytesIO()
        logo_img.save(buffered, format="PNG")
        img_b64 = base64.b64encode(buffered.getvalue()).decode()
        st.sidebar.markdown(
            f"""<div style="display:flex;justify-content:center;align-items:center;padding:8px 0 4px 0;">
            <img src="data:image/png;base64,{img_b64}" width="{new_w}" style="display:block;">
        </div>""",
            unsafe_allow_html=True,
        )
    except Exception:
        logger.exception("No se pudo cargar logo")

st.sidebar.header("Configuración")

# Credenciales y conexión
creds = get_credentials_from_st_secrets_or_env(SCOPES)
if creds is None:
    st.sidebar.error("No se encontraron credenciales válidas. Añade `gcp_service_account` o `SERVICE_ACCOUNT_JSON` en Streamlit Secrets o define GOOGLE_APPLICATION_CREDENTIALS.")
    st.stop()

sh = connect_gsheets(creds, SHEET_URL)
if sh is None:
    st.error("No se pudo conectar con la Google Sheet. Revisa las credenciales/URL.")
    st.stop()

ratings_ws = get_or_create_ratings_ws(sh)
if ratings_ws is None:
    st.error("No se pudo obtener o crear la pestaña de Calificaciones.")
    st.stop()

# Cargar datos (usando cache ligero)
df_align = load_alignments_cached(sh, ALIGN_WORKSHEET_NAME)
if df_align is None or df_align.empty:
    st.error(f"La hoja '{ALIGN_WORKSHEET_NAME}' está vacía o no tiene registros legibles.")
    st.stop()

# Cargamos las calificaciones en memoria (para UI, promedio y creación de evaluadores)
ratings_df = load_ratings_cached(ratings_ws)

# Session state defaults
st.session_state.setdefault("first_load", True)
st.session_state.setdefault("evaluador", "")
st.session_state.setdefault("pending_new_eval", "")
st.session_state.setdefault("submitted_jornada", None)
st.session_state.setdefault("saving_in_progress", False)  # evita envíos concurrentes
st.session_state.setdefault("created_now", False)  # marca que acabamos de crear un evaluador
st.session_state.setdefault("eval_selectbox", st.session_state.get("evaluador", ""))  # persistencia del selectbox

# Sidebar: evaluador list / create UI (selector en sidebar)
existing_evaluadores = sorted({str(r).strip() for r in ratings_df["Evaluador"].dropna().unique()}) if not ratings_df.empty else []
create_option = "— Crear nuevo evaluador —"
placeholder_option = "— Selecciona evaluador —"

# No limpiar pending_new_eval aquí — lo necesitamos persistente durante reruns
temp_extra = []
pending_candidate = st.session_state.get("pending_new_eval", "")
if pending_candidate:
    pending = pending_candidate.strip()
    if pending and pending not in existing_evaluadores:
        temp_extra.append(pending)

# Asegurar que si ya hay un evaluador en session_state, aparece en las opciones
current_eval = st.session_state.get("evaluador", "")
if current_eval and current_eval not in existing_evaluadores and current_eval not in temp_extra:
    temp_extra.append(current_eval)

# Respectamos un posible valor previamente forzado del selectbox (eval_selectbox)
default_value = st.session_state.get("eval_selectbox", None)
if default_value is None:
    default_value = placeholder_option if st.session_state.get("first_load", False) else (st.session_state.get("evaluador") or placeholder_option)

eval_options = [placeholder_option] + existing_evaluadores + temp_extra + [create_option]
default_index = 0
if default_value in eval_options:
    default_index = eval_options.index(default_value)
else:
    # si el default_value no está (p.ej. valor nuevo), añadimos y usamos ese índice
    eval_options.insert(1, default_value)
    default_index = 1

# --------------------------
# Aquí inyectamos CSS dinámico según el contenido de eval_options
# --------------------------
max_label_len = 0
try:
    max_label_len = max(len(str(o)) for o in eval_options) if eval_options else 30
except Exception:
    max_label_len = 30

CHAR_WIDTH_PX = 8
HORIZONTAL_PADDING_PX = 120  # espacio adicional para padding / iconos / scrollbar
MIN_SIDEBAR_PX = 300
MAX_SIDEBAR_PX = 900

computed_width = max(MIN_SIDEBAR_PX, min(MAX_SIDEBAR_PX, max_label_len * CHAR_WIDTH_PX + HORIZONTAL_PADDING_PX))
listbox_min_width = max(280, computed_width - 40)

st.markdown(
    f"""
    <style>
    /* Target common sidebar container */
    [data-testid="stSidebar"] > div:first-child {{
        min-width: {computed_width}px;
        width: {computed_width}px;
    }}
    /* Fallback selectors used por algunas Streamlit builds */
    .css-1d391kg, .css-1v3fvcr, .css-1lsmgbg {{
        min-width: {computed_width}px !important;
        width: {computed_width}px !important;
    }}
    /* Expand the listbox (dropdown options panel) */
    div[role="listbox"] {{
        min-width: {listbox_min_width}px !important;
        width: {listbox_min_width}px !important;
    }}
    /* Allow selectbox label / content to wrap instead of truncating */
    .stSelectbox div[role="button"], .stSelectbox .css-1tq9d2s {{
        white-space: normal !important;
    }}
    /* Ensure sidebar widgets don't overflow horizontally */
    [data-testid="stSidebar"] .stSelectbox, [data-testid="stSidebar"] .stTextInput {{
        width: 100% !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

# -- Selector en la barra lateral (debería caber por defecto ahora) --
selected_eval = st.sidebar.selectbox("Elige tu evaluador", eval_options, index=default_index, key="eval_selectbox")

# Si acabamos de crear el evaluador (created_now True) permitimos continuar en vez de st.stop()
if selected_eval == placeholder_option and not st.session_state.get("created_now", False):
    st.sidebar.info("Por favor selecciona o crea un evaluador para habilitar la app.")
    st.warning("La app está inactiva hasta que selecciones o crees un evaluador en la barra lateral.")
    st.stop()

if selected_eval == create_option:
    # Usamos un text_input y botón para crear un nuevo evaluador.
    # Si se crea, escribimos directamente la clave del selectbox y CONTINUAMOS en esta ejecución
    # (no forzamos rerun) para evitar que la app vuelva a la pantalla de creación.
    new_eval = st.sidebar.text_input("Nuevo nombre de evaluador", value="", key="new_eval_input")
    create_pressed = st.sidebar.button("Crear y usar este nombre", key="create_eval_btn")
    st.sidebar.info("Escribe un nombre y pulsa 'Crear y usar este nombre' para habilitar la app.")
    if not create_pressed:
        # mostrar mensaje pero no detener si acabamos de crear ahora (permite seguir)
        if not st.session_state.get("created_now", False):
            st.warning("La app está inactiva hasta que confirmes la creación del evaluador.")
            st.stop()
    else:
        # botón fue presionado en este run: validar nombre
        if new_eval.strip() == "":
            st.sidebar.error("El nombre no puede estar vacío.")
            st.stop()

        # Guardamos en session_state y hacemos que el flujo siga usando el nuevo evaluador
        nuevo = new_eval.strip()
        st.session_state["evaluador"] = nuevo
        st.session_state["pending_new_eval"] = nuevo
        st.session_state["first_load"] = False
        st.session_state["created_now"] = True

        # Ajustar selectbox para mostrar el nuevo valor
        try:
            st.session_state["eval_selectbox"] = nuevo
        except Exception:
            logger.exception("No se pudo asignar eval_selectbox en session_state directamente.")

        # IMPORTANTE: en lugar de forzar un rerun, seguimos la ejecución estableciendo selected_eval
        selected_eval = nuevo
        st.sidebar.success(f"Creado evaluador: {nuevo}")

if selected_eval != create_option and selected_eval != placeholder_option:
    st.session_state["evaluador"] = selected_eval
    st.session_state["first_load"] = False

evaluador = st.session_state.get("evaluador", "")
if not evaluador:
    st.sidebar.warning("Evaluador no establecido.")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Evaluando:**  \n`{evaluador}`")
st.sidebar.markdown("")

# ---------------- Validación de esquema de alignments ----------------
missing_cols = []
for exp in ["jornada", "jugador"]:
    if exp not in df_align.columns:
        missing_cols.append(exp)
if missing_cols:
    st.error(f"Columnas esperadas faltantes en '{ALIGN_WORKSHEET_NAME}': {missing_cols}. Revisa la sheet.")
    st.stop()

# ---------------- Selección jornada y etiqueta ----------------
def make_jornada_label(df, j):
    subset = df[df["jornada"] == j]
    found_team_values = []
    resultado_partido = None

    if "resultado_partido" in subset.columns:
        nonnull = subset["resultado_partido"].dropna()
        if not nonnull.empty:
            rp = str(nonnull.iloc[0]).strip()
            if rp:
                score_pat = re.compile(r"(\d+\s*[-–]\s*\d+)")
                m2 = score_pat.search(rp)
                if m2:
                    resultado_partido = m2.group(1).replace("\u2013", "-").strip()

    for col in subset.columns:
        low = str(col).lower()
        if any(k in low for k in ["local", "visit", "equipo", "home", "away", "opponent", "club", "oponente", "team"]):
            vals = subset[col].dropna().unique().tolist()
            for v in vals:
                if v not in (None, "", pd.NA) and str(v).strip() not in found_team_values:
                    found_team_values.append(str(v).strip())
            if found_team_values:
                break

    if not found_team_values:
        pattern_vs = re.compile(r"\b(.+?)\s+vs\.?\s+(.+)", flags=re.IGNORECASE)
        pattern_score = re.compile(r"\s*([^\d\n]+?)\s+(\d+\s*[-–]\s*\d+)\s+([^\d\n]+)\s*", flags=re.IGNORECASE)
        for col in subset.columns:
            for cell in subset[col].dropna().astype(str).unique():
                cell_str = cell.strip()
                if not cell_str:
                    continue
                m_vs = pattern_vs.search(cell_str)
                if m_vs:
                    t1 = m_vs.group(1).strip()
                    t2 = m_vs.group(2).strip()
                    if t1 and t2:
                        found_team_values = [t1, t2]
                        break
                m_sc = pattern_score.search(cell_str)
                if m_sc:
                    t1 = m_sc.group(1).strip()
                    res = m_sc.group(2).strip().replace("\u2013", "-")
                    t2 = m_sc.group(3).strip()
                    if t1 and t2:
                        found_team_values = [t1, t2]
                        if not resultado_partido:
                            resultado_partido = res
                        break
            if found_team_values:
                break

    label = f"{j}."
    if len(found_team_values) >= 2:
        team1, team2 = found_team_values[0], found_team_values[1]
        if resultado_partido:
            res_str = resultado_partido.replace("-", " - ")
            res_str = " ".join(res_str.split())
            label = f"{label} {team1} {res_str} {team2}"
        else:
            label = f"{label} {team1} vs {team2}"
    elif len(found_team_values) == 1:
        label = f"{label} {found_team_values[0]}"
    return label

unique_jornadas = list(pd.unique(df_align["jornada"].dropna().values))
jornada_labels = [make_jornada_label(df_align, j) for j in unique_jornadas]
label_to_jornada = {lab: j for lab, j in zip(jornada_labels, unique_jornadas)}
selected_label = st.selectbox("Selecciona jornada", jornada_labels, key="jornada_select")
selected_jornada = label_to_jornada[st.session_state["jornada_select"]]

j_df = df_align[df_align["jornada"] == selected_jornada].copy()
if j_df.empty:
    st.warning("No hay registros para la jornada seleccionada.")
    st.stop()

player_col = "jugador"

fecha_partido = None
if "fecha_partido" in j_df.columns and not j_df["fecha_partido"].dropna().empty:
    fecha_partido = j_df["fecha_partido"].dropna().iloc[0]
resultado_partido = None
if "resultado_partido" in j_df.columns and not j_df["resultado_partido"].dropna().empty:
    resultado_partido = j_df["resultado_partido"].dropna().iloc[0]

# Construir mapa existing por id (basado en ratings_df cargado al inicio del run)
ratings_df_local = ratings_df.copy()
ratings_df_local["id"] = ratings_df_local.get("id", ratings_df_local.apply(lambda r: make_row_id(r.get("Jornada", ""), r.get("Jugador", ""), r.get("Evaluador", "")), axis=1))
existing_map = {str(r["id"]): idx for idx, r in ratings_df_local.iterrows()}

# Mostrar alineación & minutos (sin goles/asistencias numéricas)
col1, col2, col3 = st.columns([1, 2, 2])
with col1:
    st.metric("Jornada", str(selected_jornada))
    if fecha_partido is not None and not pd.isna(fecha_partido):
        st.write("Fecha:", fecha_partido.date())
with col2:
    st.write("**Resultado**")
    st.write(resultado_partido if resultado_partido is not None else "—")
with col3:
    left_header_col, right_header_col = st.columns([6, 1])
    left_header_col.markdown("**Alineación (jugadores)**")
    right_header_col.markdown("**Minutos Jugados**")

    display_df = j_df.copy()
    if "minutos" in display_df.columns:
        display_df = display_df.sort_values(by="minutos", ascending=False).reset_index(drop=True)
    else:
        display_df = display_df.reset_index(drop=True)

    for idx, row in display_df.iterrows():
        jugador = str(row[player_col])
        minutos = int(row["minutos"]) if "minutos" in row.index and not pd.isna(row["minutos"]) else None
        gol = int(row["gol"]) if "gol" in row.index and not pd.isna(row["gol"]) else 0
        asistencia = int(row["asistencia"]) if "asistencia" in row.index and not pd.isna(row["asistencia"]) else 0

        name_col, min_col = st.columns([6, 1])
        name_col.markdown(f"**{jugador}**")
        min_text = f"{minutos}'" if minutos is not None else ""
        if gol >= 1 or asistencia >= 1:
            min_col.markdown(f"**{min_text}**")
        else:
            min_col.write(min_text)

st.markdown("---")

# Promedio de calificaciones ya guardadas por este evaluador en esta jornada
calificaciones_guardadas = []
for _, rec in ratings_df_local.iterrows():
    try:
        if str(rec.get("Jornada")) == str(selected_jornada) and str(rec.get("Evaluador", "")).strip() == str(evaluador).strip():
            cal = rec.get("Calificacion", "")
            if cal is not None and str(cal).strip() != "":
                calificaciones_guardadas.append(float(cal))
    except Exception:
        continue

st.markdown("### Promedio (esta jornada — tus calificaciones guardadas)")
if calificaciones_guardadas:
    avg_val = sum(calificaciones_guardadas) / len(calificaciones_guardadas)
    st.metric("Promedio", f"{avg_val:.2f}", delta=f"{len(calificaciones_guardadas)} entradas")
else:
    st.info("Aún no hay calificaciones guardadas por este evaluador para esta jornada.")

# Form para ingresar calificaciones (AQUÍ SÍ mostramos Gol/Asistencia para referencia)
st.subheader("Ingresa calificaciones")
visible_df = j_df.copy().reset_index(drop=True)
if "minutos" not in visible_df.columns:
    visible_df["minutos"] = 0
visible_df = visible_df.sort_values(by="minutos", ascending=False).reset_index(drop=True)

# Preparamos mapping id -> timestamp tal como existía al cargar el formulario
initial_timestamps_by_id: Dict[str, str] = {}
for idx, row in ratings_df_local.iterrows():
    rid = str(row.get("id", "")).strip()
    if rid:
        initial_timestamps_by_id[rid] = str(row.get("timestamp", "") or "")

ratings_to_write: List[dict] = []

with st.form("ratings_form_improved"):
    for local_idx, row in visible_df.iterrows():
        jugador = str(row[player_col])
        minutos = int(row["minutos"]) if not pd.isna(row["minutos"]) else None
        gol = int(row["gol"]) if "gol" in row.index and not pd.isna(row["gol"]) else 0
        asistencia = int(row["asistencia"]) if "asistencia" in row.index and not pd.isna(row["asistencia"]) else 0

        key = (str(selected_jornada), jugador, str(evaluador))
        rid = make_row_id(*key)
        default_val = 6.0
        if rid in existing_map:
            try:
                existing_row = ratings_df_local.loc[int(existing_map[rid])]
                if existing_row.get("Calificacion", "") != "":
                    default_val = float(existing_row.get("Calificacion", default_val))
            except Exception:
                pass

        a_col, b_col, c_col = st.columns([6, 2, 1])
        with a_col:
            min_display = f"{minutos}'" if minutos is not None else "—"
            info_rest = f"_min: {min_display}_  \nGol: {gol}  •  Ast: {asistencia}"
            if gol >= 1 or asistencia >= 1:
                st.markdown(f"**{jugador}  \n{info_rest}**")
            else:
                st.markdown(f"**{jugador}**  \n{info_rest}")
        with b_col:
            cal_key = f"cal_{safe_key(selected_jornada)}_{local_idx}_{safe_key(evaluador)}"
            cal_val = st.number_input("", min_value=0.0, max_value=10.0, value=float(default_val), step=0.5, key=cal_key, label_visibility="collapsed")
        with c_col:
            st.write("")

        rec = {
            "id": rid,
            "Jornada": str(selected_jornada),
            "Jugador": jugador,
            "Evaluador": str(evaluador),
            "Calificacion": float(cal_val),
            "Minutos": minutos if (not pd.isna(minutos) and minutos not in (None, "")) else "",
            "Gol": int(gol),
            "Asistencia": int(asistencia),
            "Resultado": str(resultado_partido) if resultado_partido is not None else "",
            "Fecha Partido": fecha_partido.isoformat() if fecha_partido is not None and not pd.isna(fecha_partido) else "",
            "timestamp": now_utc_iso(),
        }
        ratings_to_write.append(rec)

    submitted = st.form_submit_button("Guardar calificaciones")

# Guardado (append/update por id) con writes condicionales y batch_update
if submitted:
    # Ya no bloqueamos la edición si selected_jornada coincide con submitted_jornada;
    # permitimos editar jornadas ya guardadas.
    if st.session_state.get("saving_in_progress", False):
        st.warning("Ya hay un guardado en curso en esta sesión. Espera a que termine.")
    else:
        st.session_state["saving_in_progress"] = True
        saved_ok = False
        try:
            with st.spinner("Guardando calificaciones..."):
                t0_save = time.perf_counter()

                # Leemos solo ids y timestamps para comprobar colisiones y para construir id->rownum
                id_to_rownum, id_to_ts = read_ratings_index(ratings_ws)

                # PREFETCH: leer A2:K UNA sola vez para evitar row_values por fila
                try:
                    existing_rows_raw = retry_with_backoff(lambda: ratings_ws.get("A2:K"))
                except Exception:
                    existing_rows_raw = []
                # construir mapa rownum -> valores (listas de strings)
                rownum_to_values = {}
                for i, row in enumerate(existing_rows_raw, start=2):
                    rownum_to_values[i] = [str(x) for x in row]

                rows_to_append: List[List] = []
                batch_updates: List[Dict] = []
                skipped_due_to_collision: List[str] = []
                updated_count = 0
                appended_count = 0

                for r in ratings_to_write:
                    r_clean = normalize_and_validate_record(r)
                    rid = r_clean["id"]
                    row_values = [
                        rid,
                        r_clean["Jornada"],
                        r_clean["Jugador"],
                        r_clean["Evaluador"],
                        r_clean["Calificacion"],
                        r_clean["Minutos"],
                        r_clean["Gol"],
                        r_clean["Asistencia"],
                        r_clean["Resultado"],
                        r_clean["Fecha Partido"],
                        r_clean["timestamp"],
                    ]
                    if rid in id_to_rownum:
                        try:
                            rownum = id_to_rownum[rid]
                            # Forzar sobrescritura: ignorar el 'continue' por colisión de timestamp.
                            # Registramos en logs si hubo diferencia, pero procederemos a planificar el update.
                            existing_ts = id_to_ts.get(rid, "")
                            initial_ts = initial_timestamps_by_id.get(rid, "")
                            if initial_ts and existing_ts and existing_ts != initial_ts:
                                logger.info(f"Colisión detectada pero se procederá a sobrescribir (id={rid}, jugador={r_clean['Jugador']}). initial_ts={initial_ts}, existing_ts={existing_ts}")

                            # Obtener la fila existente desde el prefetch (si está disponible)
                            existing_row_values = rownum_to_values.get(rownum)
                            if existing_row_values is None:
                                # fallback puntual: leer la fila si no estaba en el prefetch
                                try:
                                    existing_row_values = retry_with_backoff(lambda: ratings_ws.row_values(rownum))
                                except Exception:
                                    existing_row_values = []

                            # Alinear longitud
                            existing_values = [existing_row_values[i] if i < len(existing_row_values) else "" for i in range(len(row_values))]
                            new_values = [str(v) if v is not None else "" for v in row_values]

                            if existing_values != new_values:
                                last_col_letter = chr(ord("A") + len(row_values) - 1)
                                rng = f"A{rownum}:{last_col_letter}{rownum}"
                                batch_updates.append({"range": rng, "values": [row_values]})
                                updated_count += 1
                            else:
                                logger.debug(f"No hay cambios para id={rid}, jugador={r_clean['Jugador']}. Omitiendo update.")
                        except Exception:
                            rownum = id_to_rownum.get(rid)
                            if rownum:
                                last_col_letter = chr(ord("A") + len(row_values) - 1)
                                rng = f"A{rownum}:{last_col_letter}{rownum}"
                                batch_updates.append({"range": rng, "values": [row_values]})
                                updated_count += 1
                    else:
                        rows_to_append.append(row_values)
                        appended_count += 1

                # Ejecutar updates (en batches)
                if batch_updates:
                    try:
                        # Enviar en chunks razonables para evitar batches enormes
                        BATCH_CHUNK = 150
                        for i in range(0, len(batch_updates), BATCH_CHUNK):
                            sub = batch_updates[i : i + BATCH_CHUNK]
                            batch_update_rows(ratings_ws.spreadsheet, sub)
                    except Exception:
                        logger.exception("Error realizando batch updates; intentando updates individuales como fallback")
                        for upd in batch_updates:
                            rng = upd["range"]
                            vals = upd["values"]
                            try:
                                retry_with_backoff(lambda: ratings_ws.update(rng, vals, value_input_option="USER_ENTERED"))
                            except Exception:
                                logger.exception(f"Error actualizando rango {rng} (fallback).")

                # Ejecutar appends en batches para robustez y evitar timeouts
                if rows_to_append:
                    try:
                        # chunk size configurable (aumentado para menos llamadas)
                        CHUNK = 500  # <> Cambio: aumentar CHUNK reduce número de append calls
                        # Nota: si hay concurrencia alta, bajar CHUNK reduce ventana de duplicados.
                        existing_len = len(existing_rows_raw)  # número de filas ya existentes sin cabecera (A2 es fila 2)
                        for i in range(0, len(rows_to_append), CHUNK):
                            chunk = rows_to_append[i : i + CHUNK]
                            append_rows(ratings_ws, chunk)
                            appended_count = appended_count  # solo para consistencia del contador

                            # Actualizar id_to_rownum localmente para minimizar re-lecturas costosas.
                            # Calculamos rownums asignados a las filas que acabamos de agregar.
                            # Nota: esto asume append_rows coloca las filas al final consecutivamente.
                            for j, new_row in enumerate(chunk):
                                new_rownum = existing_len + 2 + i + j  # +2 porque A2 es la fila 2
                                new_rid = str(new_row[0])
                                id_to_rownum[new_rid] = new_rownum
                                id_to_ts[new_rid] = new_row[10] if len(new_row) > 10 else ""
                                # también mantener el mapa de rownum_to_values para posibles comparaciones futuras
                                rownum_to_values[new_rownum] = [str(x) for x in new_row]
                            # incrementar existing_len para el siguiente chunk
                            existing_len += len(chunk)
                    except Exception:
                        logger.exception("Error haciendo append_rows")

                st.session_state["submitted_jornada"] = selected_jornada
                elapsed = time.perf_counter() - t0_save
                msg = f"Guardadas/actualizadas: {updated_count} actualizaciones, {appended_count} nuevas. (guardado en {elapsed:.2f}s)"
                if skipped_due_to_collision:
                    msg += f" {len(skipped_due_to_collision)} filas saltadas por colisión: {', '.join(skipped_due_to_collision)}."
                    st.warning(f"Se saltaron {len(skipped_due_to_collision)} filas porque fueron modificadas por otra sesión (ver detalles).")
                st.success(msg)
                logger.info(msg)

                _session_cache_invalidate("ratings::")
                try:
                    refreshed = safe_get_all_records(ratings_ws)
                    ratings_df = pd.DataFrame(refreshed) if refreshed else pd.DataFrame()
                except Exception:
                    logger.exception("No se pudo refrescar calificaciones tras guardado.")

                saved_ok = True

        except Exception:
            logger.exception("Error procesando guardado por lote")
            st.error("Ocurrió un error al guardar. Revisa logs.")
        finally:
            # liberar el lock siempre
            st.session_state["saving_in_progress"] = False
            # limpiar bandera created_now si venimos de crear usuario
            if st.session_state.get("created_now", False):
                st.session_state["created_now"] = False

        # En lugar de forzar un rerun (que puede resetear el selectbox),
        # aseguramos persistencia del selectbox y recargamos en memoria
        if 'saved_ok' in locals() and saved_ok:
            try:
                st.session_state["eval_selectbox"] = st.session_state.get("evaluador", evaluador)
            except Exception:
                logger.exception("No se pudo asegurar eval_selectbox en session_state tras guardado.")
            try:
                _session_cache_invalidate("ratings::")
                ratings_df = load_ratings_cached(ratings_ws)
                logger.info("Recargadas calificaciones en memoria tras guardado (sin rerun).")
            except Exception:
                logger.exception("No se pudo refrescar calificaciones tras guardado (sin rerun).")

# ======================
# Descarga: todas las calificaciones del evaluador (SIN mostrar tabla)
# ======================
st.markdown("---")
st.subheader("Descargar calificaciones")

try:
    all_records = safe_get_all_records(ratings_ws)
    all_df = pd.DataFrame(all_records) if all_records else pd.DataFrame(columns=EXPECTED_RATINGS_HEADERS)
    if "Evaluador" not in all_df.columns:
        all_df["Evaluador"] = None
    df_user = all_df[all_df["Evaluador"].astype(str).str.strip() == str(evaluador).strip()].copy()
except Exception:
    logger.exception("Error leyendo todas las calificaciones para descarga")
    df_user = pd.DataFrame(columns=EXPECTED_RATINGS_HEADERS)

if df_user.empty:
    st.info("No se encontraron calificaciones guardadas por este evaluador.")
    try:
        st.download_button(
            "Descargar todas mis calificaciones",
            data="",
            file_name=f"calificaciones_{safe_key(evaluador)}.csv",
            mime="text/csv",
            disabled=True,
        )
    except TypeError:
        pass
else:
    try:
        # transformaciones pedidas: eliminar columnas id y timestamp; formatear Fecha Partido a solo fecha
        df_export = df_user.copy()
        for c in ["id", "timestamp"]:
            if c in df_export.columns:
                df_export.drop(columns=[c], inplace=True)

        if "Fecha Partido" in df_export.columns:
            try:
                df_export["Fecha Partido"] = pd.to_datetime(df_export["Fecha Partido"], errors="coerce").dt.date
                # convertir a string YYYY-MM-DD y rellenar vacíos
                df_export["Fecha Partido"] = df_export["Fecha Partido"].apply(lambda d: d.isoformat() if pd.notna(d) else "")
            except Exception:
                logger.exception("No se pudo formatear 'Fecha Partido' al exportar; se deja como estaba.")

        csv_str = df_export.to_csv(index=False)
    except TypeError:
        try:
            import csv

            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=list(df_export.columns))
            writer.writeheader()
            for row in df_export.to_dict(orient="records"):
                safe_row = {k: ("" if v is None else v) for k, v in row.items()}
                writer.writerow(safe_row)
            csv_str = buf.getvalue()
        except Exception:
            rows = [",".join([str(c) for c in df_export.columns])]
            for rec in df_export.to_dict(orient="records"):
                rows.append(",".join([str(rec.get(c, "")) for c in df_export.columns]))
            csv_str = "\n".join(rows) + "\n"

    csv_bytes = csv_str.encode("utf-8-sig")
    st.download_button(
        "Descargar todas mis calificaciones",
        data=csv_bytes,
        file_name=f"calificaciones_{safe_key(evaluador)}.csv",
        mime="text/csv",
    )

# Fin del archivo


