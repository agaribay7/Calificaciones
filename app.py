# app.py
"""
Streamlit app — Calificar Alineaciones — Google Sheets
Versión: mejoras de seguridad y robustez (sin cambiar funcionalidad).
- No se expone información de credenciales en UI.
- Cliente gspread cacheado (st.cache_resource) con hash_funcs para Credentials.
- Lecturas de gspread con retry/backoff centralizado (safe_get_all_records).
- Validaciones mínimas antes de escribir.
- Spinner durante guardado.
- Correcciones: manejo seguro de pd.NA y comportamiento de negritas por gol/asistencia.
- En la PRIMERA SECCIÓN (alineación) no se muestran goles ni asistencias (solo nombre y minutos).
- Tras guardar, se fuerza rerun para recalcular promedios inmediatamente.
Mantiene la lógica original: ids, append/update por id, formularios, etc.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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

# -------- SECURITY NOTE / DEBUG (NON-VERBOSE) --------
# Si estás en desarrollo y guardaste la ruta en st.secrets, el código
# permitirá usarla pero NUNCA imprimirá la ruta ni el contenido en la UI.
# Evita exponer secretos en textos visibles.
if "GOOGLE_APPLICATION_CREDENTIALS" in st.secrets:
    # Solo establecer la env var si no existe (útil en local). No lo mostramos.
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = st.secrets["GOOGLE_APPLICATION_CREDENTIALS"]
            logger.debug("Se estableció GOOGLE_APPLICATION_CREDENTIALS desde st.secrets (no mostrado).")
        except Exception:
            logger.exception("No se pudo asignar GOOGLE_APPLICATION_CREDENTIALS desde st.secrets.")
# No st.write ni prints que muestren rutas/secretos.

# -------- CONSTANTS / CONFIG --------
# Cambia aquí la URL de tu Google Sheet
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OQRTq818EXBeibBmWlrcSZm83do264l2mb5jnBmzsu8"
ALIGN_WORKSHEET_NAME = "Alineaciones"
RATINGS_SHEET_NAME = "Calificaciones"

# Scope reducido: solo spreadsheets (suficiente para leer/escribir)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column mapping para normalizar nombres de columnas de la hoja de alineaciones
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

# ------- Helpers -------

def safe_key(s: str) -> str:
    """Sanitiza valores para usarlos como keys de Streamlit widgets."""
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", str(s or ""))

def make_row_id(jornada: str, jugador: str, evaluador: str) -> str:
    """Genera un id único a partir de jornada+jugador+evaluador."""
    base = f"{jornada}|{jugador}|{evaluador}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_get(d: dict, k: str, default=None):
    v = d.get(k, default)
    if isinstance(v, str):
        return v.strip()
    return v

# -------- Retry wrapper (ya existente, sin cambios funcionales) --------

def retry_with_backoff(func, max_retries=5, base=1.0, max_backoff=16.0, *args, **kwargs):
    """Wrapper simple para retries con backoff y jitter."""
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
            # error no recuperable
            logger.exception("Error no recuperable en operación gspread")
            raise
    # si llegamos aquí, re-lanzar última excepción
    raise last_exc

# safe get_all_records con backoff para lecturas
def safe_get_all_records(ws):
    try:
        return retry_with_backoff(lambda: ws.get_all_records())
    except Exception:
        logger.exception("Fallo seguro al leer get_all_records()")
        # devolver lista vacía para que la lógica superior lo maneje
        return []

# -------- Credenciales & gspread (mejora: cache client) --------

def get_credentials_from_st_secrets_or_env(scopes):
    """
    Orden:
     1) st.secrets["SERVICE_ACCOUNT_JSON"]  (string JSON o dict)
     2) st.secrets["gcp_service_account"]   (map/dict)
     3) GOOGLE_APPLICATION_CREDENTIALS env var (path)
    Nota: No usar fallback a archivos locales en producción.
    """
    # 1) SERVICE_ACCOUNT_JSON
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

    # 2) gcp_service_account
    try:
        if "gcp_service_account" in st.secrets:
            sa_info = st.secrets["gcp_service_account"]
            return Credentials.from_service_account_info(sa_info, scopes=scopes)
    except Exception:
        logger.exception("gcp_service_account presente pero no válido")

    # 3) GOOGLE_APPLICATION_CREDENTIALS (path)
    gpath = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gpath:
        try:
            return Credentials.from_service_account_file(gpath, scopes=scopes)
        except Exception:
            logger.exception("Error cargando GOOGLE_APPLICATION_CREDENTIALS")

    # Si no hay credenciales -> None
    return None

# Cacheado del cliente gspread para rendimiento (no cambia funcionalidad).
# hash_funcs evita intentar hashear Credentials (puede fallar).
@st.cache_resource(hash_funcs={Credentials: lambda _: None})
def get_gspread_client_cached(creds):
    """Devuelve un client gspread autorizado (cacheado para la sesión/app)."""
    if creds is None:
        return None
    return gspread.authorize(creds)

def connect_gsheets(creds, sheet_url):
    """Conectar y devolver el objeto Spreadsheet (usa client cacheado)."""
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
    """Obtiene o crea la worksheet de calificaciones. Añade columna 'id' si es necesario."""
    try:
        try:
            ws = sh.worksheet(title)
        except Exception:
            ws = sh.add_worksheet(title=title, rows="2000", cols="20")
        # Ensure headers exist and include 'id'
        headers = ws.row_values(1)
        if not headers or len(headers) < len(EXPECTED_RATINGS_HEADERS) or headers[0].strip().lower() != "id":
            # reescribimos cabecera en A1
            ws.update("A1", [EXPECTED_RATINGS_HEADERS])
        return ws
    except Exception:
        logger.exception("Error obteniendo/creando la pestaña de Calificaciones")
        return None

# ---------------- Reads (comentados caches previos) ----------------
# NOTA: no estamos usando st.cache_data aquí en depuración; si quieres reactivar, hacerlo con cuidado.
def load_alignments(sh, worksheet_name=ALIGN_WORKSHEET_NAME) -> pd.DataFrame:
    """Carga la hoja de alineaciones y normaliza columnas."""
    try:
        try:
            ws_align = sh.worksheet(worksheet_name)
        except Exception:
            ws_align = sh.sheet1
        records = safe_get_all_records(ws_align)
        df = pd.DataFrame(records)
        if df.empty:
            return df
        # normalizar nombres de columnas
        rename_map = {}
        for col in df.columns:
            if col in COLUMN_MAP:
                rename_map[col] = COLUMN_MAP[col]
            else:
                # try lowercase match
                low = str(col).strip().lower()
                for k, v in COLUMN_MAP.items():
                    if k.lower() == low:
                        rename_map[col] = v
                        break
        if rename_map:
            df = df.rename(columns=rename_map)
        # fecha parsing
        if "fecha_partido" in df.columns:
            df["fecha_partido"] = pd.to_datetime(df["fecha_partido"], errors="coerce")
        # numeric casts
        for ncol in ["minutos", "gol", "asistencia"]:
            if ncol in df.columns:
                df[ncol] = pd.to_numeric(df[ncol], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        logger.exception("Error cargando la hoja de alineaciones")
        return pd.DataFrame()

def load_ratings(ws) -> pd.DataFrame:
    """Carga la worksheet de calificaciones en un DataFrame (sin cache para debug)."""
    try:
        records = safe_get_all_records(ws)
        if not records:
            return pd.DataFrame(columns=EXPECTED_RATINGS_HEADERS)
        df = pd.DataFrame(records)
        # Asegurar columnas mínimas
        for col in ["id", "Jornada", "Jugador", "Evaluador", "Calificacion", "timestamp"]:
            if col not in df.columns:
                df[col] = None
        return df
    except Exception:
        logger.exception("Error leyendo calificaciones")
        return pd.DataFrame()

# -------- Write helpers (append/update por id) --------

def append_rows(ws, rows):
    """Append multiple rows robustamente con retries."""
    if not rows:
        return
    retry_with_backoff(lambda: ws.append_rows(rows, value_input_option="USER_ENTERED"))

def update_row(ws, row_index: int, row_values: list):
    """Actualiza una fila específica (1-based index). row_values debe ser lista de celdas."""
    # construye rango A{row}:K{row} basado en longitud
    last_col_letter = chr(ord("A") + len(row_values) - 1)
    range_a1 = f"A{row_index}:{last_col_letter}{row_index}"
    retry_with_backoff(lambda: ws.update(range_a1, [row_values], value_input_option="USER_ENTERED"))

# -------- Validaciones mínimas (no cambian lógica) --------

def normalize_and_validate_record(r: dict) -> dict:
    """Ajustes mínimos antes de escribir: clamp calificación 0-10, minutos a int o ''."""
    out = dict(r)
    try:
        # Calificacion clamp
        cal = out.get("Calificacion", "")
        if cal is None or cal == "":
            out["Calificacion"] = ""
        else:
            c = float(cal)
            if c < 0:
                c = 0.0
            if c > 10:
                c = 10.0
            out["Calificacion"] = float(round(c * 2) / 2)  # mantener paso 0.5 como en widget
    except Exception:
        out["Calificacion"] = ""
    try:
        m = out.get("Minutos", "")
        # usar pd.isna para cubrir pd.NA / NaN / None
        if pd.isna(m) or m in (None, ""):
            out["Minutos"] = ""
        else:
            out["Minutos"] = int(m)
    except Exception:
        out["Minutos"] = ""
    # Gol / Asistencia to int
    try:
        out["Gol"] = int(out.get("Gol", 0) or 0)
    except Exception:
        out["Gol"] = 0
    try:
        out["Asistencia"] = int(out.get("Asistencia", 0) or 0)
    except Exception:
        out["Asistencia"] = 0
    return out

# -------- App UI / Logic --------

st.title("Calificar jugadores por jornada — (Mejorado)")

# Sidebar: logo opcional
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

# Uso de la función con client cacheado (mejora de rendimiento)
sh = connect_gsheets(creds, SHEET_URL)
if sh is None:
    st.error("No se pudo conectar con la Google Sheet. Revisa las credenciales/URL.")
    st.stop()

ratings_ws = get_or_create_ratings_ws(sh)
if ratings_ws is None:
    st.error("No se pudo obtener o crear la pestaña de Calificaciones.")
    st.stop()

# Cargar datos (sin cache mientras depuramos)
df_align = load_alignments(sh, ALIGN_WORKSHEET_NAME)
if df_align is None or df_align.empty:
    st.error(f"La hoja '{ALIGN_WORKSHEET_NAME}' está vacía o no tiene registros legibles.")
    st.stop()

# Load ratings (sin cache mientras depuramos)
ratings_df = load_ratings(ratings_ws)

# Session state defaults
if "first_load" not in st.session_state:
    st.session_state["first_load"] = True
if "evaluador" not in st.session_state:
    st.session_state["evaluador"] = ""
if "pending_new_eval" not in st.session_state:
    st.session_state["pending_new_eval"] = ""
if "submitted_jornada" not in st.session_state:
    st.session_state["submitted_jornada"] = None  # to prevent double-submits per jornada

# Sidebar: evaluador
existing_evaluadores = sorted({str(r).strip() for r in ratings_df["Evaluador"].dropna().unique()}) if not ratings_df.empty else []
create_option = "— Crear nuevo evaluador —"
placeholder_option = "— Selecciona evaluador —"

temp_extra = []
if st.session_state.get("pending_new_eval"):
    pending = st.session_state["pending_new_eval"].strip()
    if pending and pending not in existing_evaluadores:
        temp_extra = [pending]
    st.session_state["pending_new_eval"] = ""

default_value = placeholder_option if st.session_state.get("first_load", False) else (st.session_state.get("evaluador") or placeholder_option)
eval_options = [placeholder_option] + existing_evaluadores + temp_extra + [create_option]
default_index = 0
if default_value in eval_options:
    default_index = eval_options.index(default_value)

selected_eval = st.sidebar.selectbox("Elige tu evaluador", eval_options, index=default_index, key="eval_selectbox")

if selected_eval == placeholder_option:
    st.sidebar.info("Por favor selecciona o crea un evaluador para habilitar la app.")
    st.warning("La app está inactiva hasta que selecciones o crees un evaluador en la barra lateral.")
    st.stop()

if selected_eval == create_option:
    new_eval = st.sidebar.text_input("Nuevo nombre de evaluador", value="", key="new_eval_input")
    create_pressed = st.sidebar.button("Crear y usar este nombre", key="create_eval_btn")
    st.sidebar.info("Escribe un nombre y pulsa 'Crear y usar este nombre' para habilitar la app.")
    if not create_pressed:
        st.warning("La app está inactiva hasta que confirmes la creación del evaluador.")
        st.stop()
    if new_eval.strip() == "":
        st.sidebar.error("El nombre no puede estar vacío.")
        st.stop()
    # set
    st.session_state["evaluador"] = new_eval.strip()
    st.session_state["pending_new_eval"] = new_eval.strip()
    st.session_state["first_load"] = False
    st.sidebar.success(f"Creado evaluador: {st.session_state['evaluador']}")

if selected_eval != create_option and selected_eval != placeholder_option:
    st.session_state["evaluador"] = selected_eval
    st.session_state["first_load"] = False

evaluador = st.session_state.get("evaluador", "")
if not evaluador:
    st.warning("Evaluador no establecido.")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Evaluando:**  \n`{evaluador}`")
st.sidebar.markdown("")

# ---------------- Selección jornada y etiqueta ----------------
if "jornada" not in df_align.columns:
    st.error('La columna "Jornada" no está presente en la hoja de alineaciones. Actualiza la sheet.')
    st.stop()

def make_jornada_label(df, j):
    subset = df[df["jornada"] == j]
    found_team_values = []
    resultado_partido = None

    # intento de extraer 'resultado_partido' de columnas conocidas
    if "resultado_partido" in subset.columns:
        nonnull = subset["resultado_partido"].dropna()
        if not nonnull.empty:
            rp = str(nonnull.iloc[0]).strip()
            if rp:
                score_pat = re.compile(r"(\d+\s*[-–]\s*\d+)")
                m2 = score_pat.search(rp)
                if m2:
                    resultado_partido = m2.group(1).replace("\u2013", "-").strip()

    # buscar nombres de equipo en columnas que parezcan equipos
    for col in subset.columns:
        low = str(col).lower()
        if any(k in low for k in ["local", "visit", "equipo", "home", "away", "opponent", "club", "oponente", "team"]):
            vals = subset[col].dropna().unique().tolist()
            for v in vals:
                if v not in (None, "", pd.NA) and str(v).strip() not in found_team_values:
                    found_team_values.append(str(v).strip())
            if found_team_values:
                break

    # fallback: buscar patterns en todas las celdas
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
selected_label = st.selectbox("Selecciona jornada", jornada_labels)
selected_jornada = label_to_jornada[selected_label]

j_df = df_align[df_align["jornada"] == selected_jornada].copy()
if j_df.empty:
    st.warning("No hay registros para la jornada seleccionada.")
    st.stop()

# identificar columna jugador (ya renombrada a 'jugador')
player_col = "jugador"

# obtener fecha_partido y resultado_partido si existen
fecha_partido = None
if "fecha_partido" in j_df.columns and not j_df["fecha_partido"].dropna().empty:
    fecha_partido = j_df["fecha_partido"].dropna().iloc[0]
resultado_partido = None
if "resultado_partido" in j_df.columns and not j_df["resultado_partido"].dropna().empty:
    resultado_partido = j_df["resultado_partido"].dropna().iloc[0]

# Construir mapa existing por id (basado en ratings_df)
ratings_df_local = ratings_df.copy()
ratings_df_local["id"] = ratings_df_local.get("id", ratings_df_local.apply(lambda r: make_row_id(r.get("Jornada", ""), r.get("Jugador", ""), r.get("Evaluador", "")), axis=1))
existing_map = {str(r["id"]): idx for idx, r in ratings_df_local.iterrows()}

# Mostrar alineación & minutos (PRIMERA SECCIÓN: sin mostrar goles/asistencias)
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
        # obtener gol/asistencia solo para decidir si aplicamos negritas; NO se mostrarán los números aquí
        gol = int(row["gol"]) if "gol" in row.index and not pd.isna(row["gol"]) else 0
        asistencia = int(row["asistencia"]) if "asistencia" in row.index and not pd.isna(row["asistencia"]) else 0

        name_col, min_col = st.columns([6, 1])

        # Nombre siempre en negrita.
        # Si gol o asistencia >=1 -> también mostramos minutos en negrita (pero NO mostramos los contadores).
        name_col.markdown(f"**{jugador}**")
        min_text = f"{minutos}'" if minutos is not None else ""
        if gol >= 1 or asistencia >= 1:
            min_col.markdown(f"**{min_text}**")
        else:
            min_col.write(min_text)

st.markdown("---")

# Promedio de calificaciones ya guardadas por este evaluador en esta jornada
# Calculamos el promedio a partir de ratings_df_local (cargado al inicio) — se actualizará al hacer rerun tras guardar.
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

# Preparar ratings_to_write
ratings_to_write = []

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
            # intentar obtener calificación existente
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
            # Nombre siempre en negrita; en el formulario mostramos Gol/Asistencia para referencia
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

# Guardado (append/update por id)
if submitted:
    # Prevención doble-submit: solo permitir 1 submit por jornada por sesión
    if st.session_state.get("submitted_jornada") == selected_jornada:
        st.warning("Ya enviaste calificaciones para esta jornada en esta sesión. Evita envíos duplicados.")
    else:
        try:
            with st.spinner("Guardando calificaciones..."):
                # refrescar ratings_df actual (sin usar cache) para evitar usar datos stale
                current_records = safe_get_all_records(ratings_ws)
                current_df = pd.DataFrame(current_records) if current_records else pd.DataFrame()
                # crear map id->row_number (1-based incl header)
                id_to_rownum = {}
                if not current_df.empty:
                    # get_all_records lo devuelve sin indices, la fila 1 es header => rownum = idx+2
                    for idx, r in current_df.iterrows():
                        rid = str(r.get("id", "")).strip()
                        if rid:
                            id_to_rownum[rid] = idx + 2

                # preparar filas nuevas para append y filas a actualizar
                rows_to_append = []
                rows_to_update = []
                for r in ratings_to_write:
                    # normalizar y validar mínimamente antes de escribir
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
                        # update that row
                        rownum = id_to_rownum[rid]
                        rows_to_update.append((rownum, row_values))
                    else:
                        rows_to_append.append(row_values)

                # Ejecutar actualizaciones
                # 1) updates
                for rownum, values in rows_to_update:
                    try:
                        update_row(ratings_ws, rownum, values)
                    except Exception:
                        logger.exception(f"Error actualizando fila {rownum} (id: {values[0]})")

                # 2) appends (batch)
                if rows_to_append:
                    append_rows(ratings_ws, rows_to_append)

                # marcar sesión y notificar
                st.session_state["submitted_jornada"] = selected_jornada
                st.success(f"Guardadas/actualizadas {len(ratings_to_write)} calificaciones en Google Sheets.")

                # Re-lectura inmediata para mostrar resultados y FORZAR recalculo de promedios:
                try:
                    refreshed = safe_get_all_records(ratings_ws)
                    ratings_df = pd.DataFrame(refreshed) if refreshed else pd.DataFrame()
                except Exception:
                    logger.exception("No se pudo refrescar calificaciones tras guardado.")
                # Fuerza rerun para que el bloque que calcula el promedio use los datos actualizados.
                st.experimental_rerun()

        except Exception:
            logger.exception("Error procesando guardado por lote")
            st.error("Ocurrió un error al guardar. Revisa logs.")

# Mostrar calificaciones guardadas (filtrado por jornada y evaluador)
st.subheader("Calificaciones guardadas (esta jornada)")
rows_show = []
# recargar actual (para asegurarnos)
try:
    current_records = safe_get_all_records(ratings_ws)
    current_df = pd.DataFrame(current_records) if current_records else pd.DataFrame()
    if not current_df.empty:
        for idx, rec in current_df.iterrows():
            if str(rec.get("Jornada")) == str(selected_jornada) and str(rec.get("Evaluador", "")).strip() == str(evaluador).strip():
                rows_show.append(
                    {
                        "Jornada": rec.get("Jornada"),
                        "Jugador": rec.get("Jugador"),
                        "Evaluador": rec.get("Evaluador"),
                        "Calificacion": rec.get("Calificacion"),
                        "Minutos": rec.get("Minutos"),
                        "Gol": rec.get("Gol"),
                        "Asistencia": rec.get("Asistencia"),
                        "Resultado": rec.get("Resultado"),
                        "Fecha Partido": rec.get("Fecha Partido"),
                        "timestamp": rec.get("timestamp"),
                    }
                )
except Exception:
    logger.exception("Error al leer calificaciones para mostrar")

if rows_show:
    df_display = pd.DataFrame(rows_show).sort_values(by="Calificacion", ascending=False)
    st.dataframe(df_display.reset_index(drop=True))
    # boton para descargar CSV
    csv = df_display.to_csv(index=False).encode("utf-8")
    st.download_button("Descargar CSV de mis calificaciones (jornada)", data=csv, file_name=f"calificaciones_j{selected_jornada}.csv", mime="text/csv")
else:
    st.info("No hay calificaciones guardadas por este evaluador para esta jornada.")
