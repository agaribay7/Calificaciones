# app.py
"""
Streamlit app — Calificar Alineaciones — Google Sheets
Versión: lee alineaciones desde la Google Sheet (worksheet "Alineaciones").
Mantiene la pestaña "Calificaciones" para leer/guardar las calificaciones.
Credenciales: desde st.secrets (gcp_service_account o SERVICE_ACCOUNT_JSON) o
GOOGLE_APPLICATION_CREDENTIALS / fichero local como fallback (solo para dev).
"""

import json
import os
import io
import base64
import re
import time
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from PIL import Image

# ------------------ CONFIG ------------------
# Fallback local (solo desarrollo). En Cloud no debe usarse.
CRED_FOLDER = Path(r"C:\Users\agaribayda\Documents\Streamlit Calificaciones AP25")
CRED_PREFIX = "credenciales"

SHEET_URL = "https://docs.google.com/spreadsheets/d/1OQRTq818EXBeibBmWlrcSZm83do264l2mb5jnBmzsu8"
ALIGN_WORKSHEET_NAME = "Alineaciones"   # <-- tu nombre de hoja para alineaciones
RATINGS_SHEET_NAME = "Calificaciones"

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

st.set_page_config(page_title="Calificar Alineaciones — Google Sheets", layout="wide")
st.title("Calificar jugadores por jornada")

# ------------------ Sidebar: logo (centrado) ------------------
LOGO_FILENAME = "logo_transparent.png"
LOGO_WIDTH = 240

try:
    app_dir = Path(__file__).parent
except Exception:
    app_dir = Path.cwd()

logo_path = app_dir / LOGO_FILENAME
if logo_path.exists():
    try:
        logo_img = Image.open(logo_path).convert("RGBA")
        w, h = logo_img.size
        if w > 0:
            new_w = LOGO_WIDTH
            new_h = int(h * new_w / w)
            logo_img = logo_img.resize((new_w, new_h), Image.LANCZOS)
        buffered = io.BytesIO()
        logo_img.save(buffered, format="PNG")
        img_b64 = base64.b64encode(buffered.getvalue()).decode()
        html = f"""
        <div style="display:flex;justify-content:center;align-items:center;padding:8px 0 4px 0;">
            <img src="data:image/png;base64,{img_b64}" width="{new_w}" style="display:block;">
        </div>
        """
        st.sidebar.markdown(html, unsafe_allow_html=True)
    except Exception as e:
        st.sidebar.warning("No se pudo cargar el logo de la sidebar.")
        st.sidebar.exception(e)

st.sidebar.header("Configuración")

# ------------------ Helpers credenciales / gspread ------------------
def find_credentials_file(folder: Path, prefix="credenciales"):
    try:
        if not folder.exists():
            return None
        for p in folder.iterdir():
            if p.is_file() and p.name.lower().startswith(prefix.lower()):
                return p
    except Exception:
        return None
    return None

def get_credentials_from_st_secrets_or_env(scopes):
    """
    Order:
      1) st.secrets["SERVICE_ACCOUNT_JSON"]  (string JSON or dict)
      2) st.secrets["gcp_service_account"]   (map/dict)
      3) GOOGLE_APPLICATION_CREDENTIALS env var (path)
      4) local credential file (CRED_FOLDER)  <- only for dev
    """
    # 1) SERVICE_ACCOUNT_JSON
    try:
        if "SERVICE_ACCOUNT_JSON" in st.secrets:
            sa_raw = st.secrets["SERVICE_ACCOUNT_JSON"]
            if isinstance(sa_raw, dict):
                sa_info = sa_raw
            else:
                # si es string, parsear
                sa_info = json.loads(sa_raw)
            return Credentials.from_service_account_info(sa_info, scopes=scopes)
    except Exception as e:
        st.warning("SERVICE_ACCOUNT_JSON presente pero no parseable: " + str(e))

    # 2) gcp_service_account (bloque)
    try:
        if "gcp_service_account" in st.secrets:
            sa_info = st.secrets["gcp_service_account"]
            return Credentials.from_service_account_info(sa_info, scopes=scopes)
    except Exception as e:
        st.warning("gcp_service_account presente pero no válido: " + str(e))

    # 3) GOOGLE_APPLICATION_CREDENTIALS
    try:
        gpath = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if gpath:
            return Credentials.from_service_account_file(gpath, scopes=scopes)
    except Exception as e:
        st.warning("Error cargando GOOGLE_APPLICATION_CREDENTIALS: " + str(e))

    # 4) fallback local file (desarrollo)
    local = find_credentials_file(CRED_FOLDER, CRED_PREFIX)
    if local:
        try:
            return Credentials.from_service_account_file(str(local), scopes=scopes)
        except Exception as e:
            st.warning("Error leyendo fichero local de credenciales: " + str(e))

    return None

def connect_gsheets(creds, sheet_url):
    if creds is None:
        st.error("No se han encontrado credenciales para Google Sheets.")
        return None
    try:
        client = gspread.authorize(creds)
        sh = client.open_by_url(sheet_url)
        return sh
    except Exception as e:
        st.error("Error autenticando/abriendo la Google Sheet con las credenciales proporcionadas.")
        st.exception(e)
        return None

def get_or_create_ratings_ws(sh, title=RATINGS_SHEET_NAME):
    try:
        try:
            ws = sh.worksheet(title)
            return ws
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows="2000", cols="20")
            headers = [
                'Jornada','Jugador','Evaluador','Calificacion','Minutos',
                'Gol','Asistencia','Resultado','Fecha Partido','timestamp'
            ]
            ws.append_row(headers)
            return ws
    except Exception as e:
        st.error("Error obteniendo/creando la pestaña de Calificaciones en Google Sheets.")
        st.exception(e)
        return None

# ------------------ Conexión usando st.secrets / fallback ------------------
creds = get_credentials_from_st_secrets_or_env(SCOPES)
if creds is None:
    st.error("No se encontraron credenciales válidas. Añade `gcp_service_account` o `SERVICE_ACCOUNT_JSON` en Streamlit Secrets.")
    st.stop()

sh = connect_gsheets(creds, SHEET_URL)
if sh is None:
    st.stop()

ratings_ws = get_or_create_ratings_ws(sh)
if ratings_ws is None:
    st.stop()

# ------------------ Cargar alineaciones desde la worksheet "Alineaciones" ------------------
try:
    try:
        ws_align = sh.worksheet(ALIGN_WORKSHEET_NAME)
    except Exception:
        # si no existe la worksheet con ese nombre, usar la primera hoja
        ws_align = sh.sheet1

    align_records = ws_align.get_all_records()
    df = pd.DataFrame(align_records)
    # normalizar columnas y Fecha Partido como en tu código original
    df = df.rename(columns={col: col.strip() if isinstance(col, str) else col for col in df.columns})
    for candidate in ['Fecha Partido','Fecha','FechaPartido','fecha_partido']:
        if candidate in df.columns:
            df['Fecha Partido'] = pd.to_datetime(df[candidate], errors='coerce')
            break

    if df is None or df.empty:
        st.error(f"La hoja '{ALIGN_WORKSHEET_NAME}' está vacía o no tiene registros legibles.")
        st.stop()

except Exception as e:
    st.error("Error cargando la hoja de alineaciones desde Google Sheets.")
    st.exception(e)
    st.stop()

# ------------------ Leer registros existentes desde la pestaña Calificaciones ------------------
records = []
try:
    records = ratings_ws.get_all_records()
except Exception as e:
    st.warning("No se pudieron leer las calificaciones existentes (continuamos, pero la lista de evaluadores puede estar vacía).")
    st.exception(e)
    records = []

# ------------------ Session state defaults ------------------
if 'first_load' not in st.session_state:
    st.session_state['first_load'] = True
if 'evaluador' not in st.session_state:
    st.session_state['evaluador'] = ""
if 'pending_new_eval' not in st.session_state:
    st.session_state['pending_new_eval'] = ""

# ------------------ Sidebar: evaluador (misma lógica que tenías) ------------------
existing_evaluadores = sorted({str(r.get('Evaluador','')).strip() for r in records if str(r.get('Evaluador','')).strip()})
create_option = "— Crear nuevo evaluador —"
placeholder_option = "— Selecciona evaluador —"

temp_extra = []
if st.session_state.get('pending_new_eval'):
    pending = st.session_state['pending_new_eval'].strip()
    if pending and pending not in existing_evaluadores:
        temp_extra = [pending]
    st.session_state['pending_new_eval'] = ""

default_value = placeholder_option if st.session_state.get('first_load', False) else (st.session_state.get('evaluador') or placeholder_option)
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
    if create_pressed:
        if new_eval.strip() == "":
            st.sidebar.error("El nombre no puede estar vacío.")
            st.stop()
        st.session_state['evaluador'] = new_eval.strip()
        st.session_state['pending_new_eval'] = new_eval.strip()
        st.session_state['first_load'] = False
        st.sidebar.success(f"Creado evaluador: {st.session_state['evaluador']}")

if selected_eval != create_option and selected_eval != placeholder_option:
    st.session_state['evaluador'] = selected_eval
    st.session_state['first_load'] = False

evaluador = st.session_state.get('evaluador', '')
if not evaluador:
    st.warning("Evaluador no establecido. Selecciona o crea un evaluador en la barra lateral.")
    st.stop()

# Badge persistente
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Evaluando:**  \n`{evaluador}`")
st.sidebar.markdown("")

# ------------------ Selección de jornada y resto de UI (igual que tu código original) ------------------
if 'Jornada' not in df.columns:
    st.error('La columna "Jornada" no está presente en la hoja de alineaciones. Actualiza la sheet.')
    st.stop()

try:
    unique_jornadas = list(pd.unique(df['Jornada'].dropna().values))

    def make_jornada_label(j):
        subset = df[df['Jornada'] == j]
        found_team_values = []
        resultado_partido = None

        if 'Resultado Partido' in subset.columns:
            nonnull = subset['Resultado Partido'].dropna()
            if not nonnull.empty:
                rp = str(nonnull.iloc[0]).strip()
                if rp:
                    pattern_score = re.compile(r'^\s*([^\d\n]+?)\s+(\d+\s*[-–]\s*\d+)\s+([^\d\n]+)\s*$', flags=re.IGNORECASE)
                    m = pattern_score.match(rp)
                    if m:
                        t1 = m.group(1).strip()
                        res = m.group(2).strip().replace('\u2013', '-')
                        t2 = m.group(3).strip()
                        if t1 and t2:
                            found_team_values = [t1, t2]
                            resultado_partido = res
                    else:
                        score_pat = re.compile(r'(\d+\s*[-–]\s*\d+)')
                        m2 = score_pat.search(rp)
                        if m2:
                            resultado_partido = m2.group(1).replace('\u2013', '-').strip()

        if not found_team_values:
            for col in subset.columns:
                low = str(col).lower()
                if any(k.lower() in low for k in ['local','visit','equipo','home','away','rival','opponent','club','oponente','team']):
                    vals = subset[col].dropna().unique().tolist()
                    for v in vals:
                        if v not in (None, "", pd.NA) and str(v).strip() not in found_team_values:
                            found_team_values.append(str(v).strip())
                    if found_team_values:
                        break

        if not found_team_values:
            pattern_vs = re.compile(r'\b(.+?)\s+vs\.?\s+(.+)', flags=re.IGNORECASE)
            pattern_score = re.compile(r'\s*([^\d\n]+?)\s+(\d+\s*[-–]\s*\d+)\s+([^\d\n]+)\s*', flags=re.IGNORECASE)
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
                        res = m_sc.group(2).strip().replace('\u2013', '-')
                        t2 = m_sc.group(3).strip()
                        if t1 and t2:
                            found_team_values = [t1, t2]
                            if not resultado_partido:
                                resultado_partido = res
                            break
                if found_team_values:
                    break

        if not resultado_partido:
            score_pat = re.compile(r'(\d+\s*[-–]\s*\d+)')
            for col in subset.columns:
                for cell in subset[col].dropna().astype(str).unique():
                    s = cell.strip()
                    m = score_pat.search(s)
                    if m:
                        resultado_partido = m.group(1).replace('\u2013', '-').strip()
                        break
                if resultado_partido:
                    break

        label = f"{j}."
        if len(found_team_values) >= 2:
            team1 = found_team_values[0]
            team2 = found_team_values[1]
            if resultado_partido:
                res_str = resultado_partido.replace('-', ' - ')
                res_str = ' '.join(res_str.split())
                label = f"{label} {team1} {res_str} {team2}"
            else:
                label = f"{label} {team1} vs {team2}"
        elif len(found_team_values) == 1:
            label = f"{label} {found_team_values[0]}"
        else:
            label = label

        return label

    jornada_labels = [make_jornada_label(j) for j in unique_jornadas]
    label_to_jornada = {lab: j for lab, j in zip(jornada_labels, unique_jornadas)}
    selected_label = st.selectbox("Selecciona jornada", jornada_labels)
    selected_jornada = label_to_jornada[selected_label]

except Exception:
    try:
        jornadas = sorted(df['Jornada'].dropna().unique(), key=lambda x: (int(x) if str(x).isdigit() else str(x)))
    except Exception:
        jornadas = sorted(df['Jornada'].dropna().unique())
    selected_jornada = st.selectbox("Selecciona jornada", jornadas)

j_df = df[df['Jornada'] == selected_jornada].copy()
if j_df.empty:
    st.warning("No hay registros para la jornada seleccionada.")
    st.stop()

# Identificar columna de jugador
player_col = None
for candidate in ['Jugador', 'Nombre', 'Player', 'player', 'jugador']:
    if candidate in j_df.columns:
        player_col = candidate
        break
if not player_col:
    st.error('No se encontró la columna de jugador ("Jugador" o "Nombre").')
    st.stop()

fecha_partido = None
if 'Fecha Partido' in j_df.columns and not j_df['Fecha Partido'].dropna().empty:
    fecha_partido = j_df['Fecha Partido'].dropna().iloc[0]
resultado_partido = None
for c in ['Resultado Partido','Resultado','resultado_partido','ResultadoPartido']:
    if c in j_df.columns:
        nonnull = j_df[c].dropna()
        if not nonnull.empty:
            resultado_partido = nonnull.iloc[0]
            break

# Build existing map
existing_map = {}
for idx, rec in enumerate(records):
    key = (str(rec.get('Jornada')), str(rec.get('Jugador')), str(rec.get('Evaluador', '')))
    existing_map[key] = (rec, idx)

# Mostrar alineación y minutos (idéntico a tu código original)
col1, col2, col3 = st.columns([1,2,2])
with col1:
    try:
        st.metric("Jornada", str(selected_jornada))
        if fecha_partido is not None and not pd.isna(fecha_partido):
            st.write("Fecha:", fecha_partido.date())
    except Exception:
        st.metric("Jornada", str(selected_jornada))
with col2:
    st.write("**Resultado**")
    st.write(resultado_partido if resultado_partido is not None else "—")
with col3:
    left_header_col, right_header_col = st.columns([6,1])
    left_header_col.markdown("**Alineación (jugadores)**")
    right_header_col.markdown("**Minutos Jugados**")

    top_df = j_df.copy()
    if 'Minutos Jugados' in top_df.columns:
        top_min_col = 'Minutos Jugados'
    elif 'Minutos' in top_df.columns:
        top_min_col = 'Minutos'
    else:
        top_min_col = None

    if top_min_col:
        top_df['__minutos_num'] = pd.to_numeric(top_df[top_min_col], errors='coerce').fillna(0).astype(int)
    else:
        top_df['__minutos_num'] = 0

    top_df = top_df.sort_values(by='__minutos_num', ascending=False).reset_index(drop=True)

    for idx, row in top_df.iterrows():
        jugador = str(row[player_col])
        minutos = row.get('Minutos Jugados', '') if 'Minutos Jugados' in row.index else row.get('Minutos', '')
        min_text = ""
        try:
            if minutos not in (None, "", pd.NA) and str(minutos).strip() != "":
                min_int = int(float(minutos))
                min_text = f"{min_int}'"
        except Exception:
            min_text = str(minutos)

        gol_val = 0
        ast_val = 0
        try:
            gol_val = int(row.get('Gol', 0)) if 'Gol' in row.index and not pd.isna(row.get('Gol', 0)) else 0
        except Exception:
            gol_val = 0
        try:
            ast_val = int(row.get('Asistencia', 0)) if 'Asistencia' in row.index and not pd.isna(row.get('Asistencia', 0)) else 0
        except Exception:
            ast_val = 0

        name_col, min_col = st.columns([6,1])
        name_col.markdown(f"**{jugador}**")
        if gol_val >= 1 or ast_val >= 1:
            if min_text:
                min_col.markdown(f"**{min_text}**")
            else:
                min_col.write("")
        else:
            if min_text:
                min_col.write(min_text)
            else:
                min_col.write("")

st.markdown("---")

# ------------------ Promedio de calificaciones YA GUARDADAS por este evaluador en esta jornada
calificaciones_guardadas = []
for rec in records:
    try:
        if str(rec.get('Jornada')) == str(selected_jornada) and str(rec.get('Evaluador', '')).strip() == str(evaluador).strip():
            cal = rec.get('Calificacion', '')
            if cal is not None and str(cal).strip() != '':
                calificaciones_guardadas.append(float(cal))
    except Exception:
        continue

if calificaciones_guardadas:
    avg_val = sum(calificaciones_guardadas) / len(calificaciones_guardadas)
    st.markdown("### Promedio (esta jornada — tus calificaciones guardadas)")
    st.metric("Promedio", f"{avg_val:.2f}", delta=f"{len(calificaciones_guardadas)} entradas")
else:
    st.markdown("### Promedio (esta jornada — tus calificaciones guardadas)")
    st.info("Aún no hay calificaciones guardadas por este evaluador para esta jornada.")

# Form para ingresar calificaciones
st.subheader("Ingresa calificaciones")
visible_df = j_df.copy().reset_index(drop=True)

if 'Minutos Jugados' in visible_df.columns:
    min_col_name = 'Minutos Jugados'
elif 'Minutos' in visible_df.columns:
    min_col_name = 'Minutos'
else:
    min_col_name = None

if min_col_name:
    visible_df['__minutos_num'] = pd.to_numeric(visible_df[min_col_name], errors='coerce').fillna(0).astype(int)
else:
    visible_df['__minutos_num'] = 0

visible_df = visible_df.sort_values(by='__minutos_num', ascending=False).reset_index(drop=True)

with st.form("ratings_form_optimized"):
    ratings_to_write = []
    for local_idx, (orig_idx, row) in enumerate(visible_df.iterrows()):
        jugador = str(row[player_col])
        minutos = row.get('Minutos Jugados', '') if 'Minutos Jugados' in row.index else row.get('Minutos', '')
        gol = int(row.get('Gol', 0)) if 'Gol' in row.index and not pd.isna(row.get('Gol', 0)) else 0
        asistencia = int(row.get('Asistencia', 0)) if 'Asistencia' in row.index and not pd.isna(row.get('Asistencia', 0)) else 0

        key = (str(selected_jornada), jugador, str(evaluador))
        default_val = 6.0
        if key in existing_map:
            rec_existing, rec_idx = existing_map[key]
            try:
                if rec_existing.get('Calificacion', '') != '':
                    default_val = float(rec_existing.get('Calificacion', default_val))
            except Exception:
                default_val = default_val

        a_col, b_col, c_col = st.columns([6,2,1])
        with a_col:
            min_display = minutos if minutos not in (None, '') else '—'
            block = f"{jugador}  \n_min: {min_display}_  \nGol: {gol}  •  Ast: {asistencia}"
            st.markdown(f"**{block}**")

        with b_col:
            cal_key = f"cal_{selected_jornada}_{orig_idx}_{evaluador}"
            cal_val = st.number_input("", min_value=0.0, max_value=10.0, value=float(default_val), step=0.5, key=cal_key, label_visibility="collapsed")
        with c_col:
            st.write("")

        rec = {
            'Jornada': str(selected_jornada),
            'Jugador': jugador,
            'Evaluador': str(evaluador),
            'Calificacion': float(cal_val),
            'Minutos': minutos if minutos != '' and minutos is not None else None,
            'Gol': int(gol),
            'Asistencia': int(asistencia),
            'Resultado': str(resultado_partido) if resultado_partido is not None else '',
            'Fecha Partido': fecha_partido.isoformat() if fecha_partido is not None and not pd.isna(fecha_partido) else None,
            'timestamp': datetime.utcnow().isoformat()
        }
        ratings_to_write.append(rec)

    submitted = st.form_submit_button("Guardar calificaciones")

# Guardado en Google Sheets (idéntico a tu lógica original)
if submitted:
    try:
        records = ratings_ws.get_all_records()
        headers = ['Jornada','Jugador','Evaluador','Calificacion','Minutos','Gol','Asistencia','Resultado','Fecha Partido','timestamp']

        all_recs = []
        for r in records:
            rec_row = {
                'Jornada': r.get('Jornada', ''),
                'Jugador': r.get('Jugador', ''),
                'Evaluador': r.get('Evaluador', ''),
                'Calificacion': r.get('Calificacion', ''),
                'Minutos': r.get('Minutos', ''),
                'Gol': r.get('Gol', ''),
                'Asistencia': r.get('Asistencia', ''),
                'Resultado': r.get('Resultado', ''),
                'Fecha Partido': r.get('Fecha Partido', ''),
                'timestamp': r.get('timestamp', '')
            }
            all_recs.append(rec_row)

        key_to_idx = {}
        for i, r in enumerate(all_recs):
            key = (str(r.get('Jornada')), str(r.get('Jugador')), str(r.get('Evaluador', '')))
            key_to_idx[key] = i

        for r in ratings_to_write:
            key = (str(r['Jornada']), str(r['Jugador']), str(r['Evaluador']))
            if key in key_to_idx:
                idx = key_to_idx[key]
                all_recs[idx]['Calificacion'] = r.get('Calificacion')
                all_recs[idx]['Minutos'] = r.get('Minutos')
                all_recs[idx]['Gol'] = r.get('Gol')
                all_recs[idx]['Asistencia'] = r.get('Asistencia')
                all_recs[idx]['Resultado'] = r.get('Resultado')
                all_recs[idx]['Fecha Partido'] = r.get('Fecha Partido')
                all_recs[idx]['timestamp'] = r.get('timestamp')
            else:
                new_rec = {
                    'Jornada': r.get('Jornada'),
                    'Jugador': r.get('Jugador'),
                    'Evaluador': r.get('Evaluador'),
                    'Calificacion': r.get('Calificacion'),
                    'Minutos': r.get('Minutos'),
                    'Gol': r.get('Gol'),
                    'Asistencia': r.get('Asistencia'),
                    'Resultado': r.get('Resultado'),
                    'Fecha Partido': r.get('Fecha Partido'),
                    'timestamp': r.get('timestamp')
                }
                key_to_idx[key] = len(all_recs)
                all_recs.append(new_rec)

        rows_to_write = [headers]
        for r in all_recs:
            rows_to_write.append([
                r.get('Jornada', ''),
                r.get('Jugador', ''),
                r.get('Evaluador', ''),
                r.get('Calificacion', ''),
                r.get('Minutos', ''),
                r.get('Gol', ''),
                r.get('Asistencia', ''),
                r.get('Resultado', ''),
                r.get('Fecha Partido', ''),
                r.get('timestamp', '')
            ])

        max_retries = 5
        backoff = 1.0
        last_err = None
        for attempt in range(max_retries):
            try:
                ratings_ws.update('A1', rows_to_write)
                st.success(f"Guardadas/actualizadas {len(ratings_to_write)} calificaciones en Google Sheets (operación por lote).")
                last_err = None
                break
            except APIError as e:
                last_err = e
                st.warning(f"APIError intentando escribir (intento {attempt+1}/{max_retries}): {e}")
                time.sleep(backoff)
                backoff *= 2.0
            except Exception as e:
                last_err = e
                st.error(f"Error no esperado al actualizar Google Sheets: {e}")
                st.exception(e)
                break

        if last_err:
            st.error("No se pudo guardar las calificaciones después de varios intentos.")
            st.exception(last_err)

        # Refrescar records y mapa local
        records = ratings_ws.get_all_records()
        existing_map = {}
        for idx, rec in enumerate(records):
            key = (str(rec.get('Jornada')), str(rec.get('Jugador')), str(rec.get('Evaluador', '')))
            existing_map[key] = (rec, idx)

    except Exception as e:
        st.error("Error procesando guardado por lote.")
        st.exception(e)

# Mostrar calificaciones guardadas (filtrado por jornada y evaluador)
st.subheader("Calificaciones guardadas (esta jornada)")

rows_show = []
for key, (rec, idx) in existing_map.items():
    j, player, evalr = key
    if str(j) == str(selected_jornada):
        if str(evalr) != str(evaluador):
            continue
        rows_show.append({
            'Jornada': rec.get('Jornada'),
            'Jugador': rec.get('Jugador'),
            'Evaluador': rec.get('Evaluador'),
            'Calificacion': rec.get('Calificacion'),
            'Minutos': rec.get('Minutos'),
            'Gol': rec.get('Gol'),
            'Asistencia': rec.get('Asistencia'),
            'Resultado': rec.get('Resultado'),
            'Fecha Partido': rec.get('Fecha Partido'),
            'timestamp': rec.get('timestamp')
        })

if rows_show:
    df_display = pd.DataFrame(rows_show).sort_values(by='Calificacion', ascending=False)
    st.dataframe(df_display.reset_index(drop=True))
else:
    st.info("No hay calificaciones guardadas por este evaluador para esta jornada.")
