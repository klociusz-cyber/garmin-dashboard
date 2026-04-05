"""
Garmin Dashboard – wersja online
Wgraj pliki .fit aby zobaczyć analizę treningów
"""

import json
import os
import sqlite3
import sys
import tempfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent / "parser"))
from parse_fit import (
    init_db as _init_workout_db,
    parse_fit_bytes,
    get_field,
    fix_encoding,
)
from parse_monitor import (
    init_db as _init_monitor_db,
    parse_monitor_bytes,
    aggregate_daily,
)

st.set_page_config(
    page_title="Garmin Dashboard",
    page_icon="💪",
    layout="wide",
)

# ── session state ──────────────────────────────────────────────────────────

def init_session():
    if "conn" not in st.session_state:
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        _init_workout_db(conn)
        _init_monitor_db(conn)
        st.session_state.conn      = conn
        st.session_state.processed = set()

init_session()

def query(sql, params=()):
    return pd.read_sql_query(sql, st.session_state.conn, params=params)

def has_workouts():
    try:
        return query("SELECT COUNT(*) AS n FROM workouts").iloc[0]["n"] > 0
    except Exception:
        return False


# ── eksport / import bazy ──────────────────────────────────────────────────

def export_db_bytes(conn: sqlite3.Connection) -> bytes:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    dest = sqlite3.connect(tmp.name)
    conn.backup(dest)
    dest.close()
    with open(tmp.name, "rb") as f:
        data = f.read()
    os.unlink(tmp.name)
    return data


def load_db_from_bytes(data: bytes) -> sqlite3.Connection:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.write(data)
    tmp.close()
    src = sqlite3.connect(tmp.name)
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    src.backup(conn)
    src.close()
    os.unlink(tmp.name)
    # upewnij się że wszystkie tabele istnieją (starsze pliki .db)
    _init_workout_db(conn)
    _init_monitor_db(conn)
    return conn

# ── helpers ────────────────────────────────────────────────────────────────

def detect_fit_type(data: bytes) -> str:
    """Rozróżnia plik aktywności od pliku monitoringowego."""
    from fitparse import FitFile
    fit = FitFile(BytesIO(data))
    for msg in fit.get_messages():
        if msg.name == "monitoring":
            return "monitor"
    return "workout"

MONTH_PL = {
    "sty": 1, "lut": 2, "mar": 3, "kwi": 4, "maj": 5, "cze": 6,
    "lip": 7, "sie": 8, "wrz": 9, "paź": 10, "paz": 10,
    "lis": 11, "gru": 12,
}

def parse_fitatu_bytes(data: bytes) -> pd.DataFrame:
    import xlrd
    rows = []
    try:
        book = xlrd.open_workbook(file_contents=data, ignore_workbook_corruption=True)
    except Exception:
        return pd.DataFrame()
    sh = book.sheet_by_index(0)
    for i in range(1, sh.nrows):
        cell = str(sh.cell_value(i, 0)).strip()
        if not cell:
            continue
        parts = cell.split()
        if len(parts) < 4:
            continue
        try:
            day   = int(parts[1])
            month = MONTH_PL.get(parts[2].lower().replace("ź", "z"))
            year  = int(parts[3])
            if not month:
                continue
        except (ValueError, IndexError):
            continue
        def num(col):
            v = sh.cell_value(i, col)
            try:
                return float(v) if v != "" else None
            except Exception:
                return None
        rows.append({
            "date":    pd.Timestamp(year, month, day),
            "kcal":    num(5),
            "protein": num(6),
            "fat":     num(9),
            "carbs":   num(15),
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).dropna(subset=["kcal"]).sort_values("date")

def trend_delta(series: pd.Series):
    if len(series) < 4:
        return None, None
    mid = len(series) // 2
    avg_first  = series.iloc[:mid].mean()
    avg_second = series.iloc[mid:].mean()
    if avg_first == 0:
        return None, None
    pct  = (avg_second - avg_first) / avg_first * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%", "normal" if pct >= 0 else "inverse"

# ── przetwarzanie uploadów ─────────────────────────────────────────────────

def process_uploads(fit_files, xls_files, json_file):
    errors = []
    added_workouts = added_monitors = 0

    for f in (fit_files or []):
        if f.name in st.session_state.processed:
            continue
        data = f.read()
        try:
            if detect_fit_type(data) == "monitor":
                parse_monitor_bytes(data, f.name, st.session_state.conn)
                added_monitors += 1
            else:
                parse_fit_bytes(data, f.name, st.session_state.conn)
                added_workouts += 1
            st.session_state.processed.add(f.name)
        except Exception as e:
            errors.append(f"{f.name}: {e}")

    if added_monitors > 0:
        aggregate_daily(st.session_state.conn)

    for f in (xls_files or []):
        if f.name in st.session_state.processed:
            continue
        df = parse_fitatu_bytes(f.read())
        if not df.empty:
            for _, row in df.iterrows():
                st.session_state.conn.execute(
                    "INSERT OR REPLACE INTO diet (date, kcal, protein, fat, carbs) VALUES (?,?,?,?,?)",
                    (row["date"].strftime("%Y-%m-%d"), row["kcal"], row["protein"], row["fat"], row["carbs"]),
                )
            st.session_state.conn.commit()
        st.session_state.processed.add(f.name)

    if json_file and json_file.name not in st.session_state.processed:
        try:
            raw = json.loads(json_file.read())
            for item in raw.get("pomiary", []):
                date = item.get("data")
                kg   = item.get("pomiar_kg")
                if date and kg:
                    st.session_state.conn.execute(
                        "INSERT OR REPLACE INTO weight (date, weight_kg) VALUES (?,?)",
                        (date, float(kg)),
                    )
            st.session_state.conn.commit()
            st.session_state.processed.add(json_file.name)
        except Exception as e:
            errors.append(f"{json_file.name}: {e}")

    return added_workouts, added_monitors, errors

# ── upload UI ──────────────────────────────────────────────────────────────

def show_upload_page():
    st.title("💪 Garmin Dashboard")
    st.markdown("---")

    # ── opcja A: wgraj zapisaną bazę danych ──
    st.markdown("#### Opcja A — wgraj zapisaną bazę danych")
    db_file = st.file_uploader(
        "Plik `garmin_dashboard.db` z poprzedniej sesji",
        type=["db"],
        key="up_db_main",
    )
    if st.button("▶ Załaduj bazę", type="primary", disabled=not db_file):
        with st.spinner("Ładowanie bazy..."):
            conn = load_db_from_bytes(db_file.read())
        st.session_state.conn = conn
        # odtwórz listę już przetworzonych plików z bazy
        try:
            names = pd.read_sql_query("SELECT file_name FROM workouts", conn)["file_name"].tolist()
            st.session_state.processed = set(names)
        except Exception:
            st.session_state.processed = set()
        st.rerun()

    st.markdown("---")

    # ── opcja B: wgraj pliki od zera ──
    st.markdown("#### Opcja B — wgraj pliki z Garmina")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("##### Treningi / monitoring")
        fit_files = st.file_uploader(
            "Pliki .fit z folderu `Garmin\\Activity\\` i/lub `Garmin\\Monitor\\`",
            type=["fit"],
            accept_multiple_files=True,
            key="up_fit_main",
        )
    with col2:
        st.markdown("##### Dieta — Fitatu (opcjonalnie)")
        xls_files = st.file_uploader(
            "Pliki jadłospisu `.xls` z aplikacji Fitatu",
            type=["xls"],
            accept_multiple_files=True,
            key="up_xls_main",
        )
    with col3:
        st.markdown("##### Waga — Fitatu (opcjonalnie)")
        json_file = st.file_uploader(
            "Plik `pomiary.json` z aplikacji Fitatu",
            type=["json"],
            key="up_json_main",
        )

    st.markdown("")
    if st.button("▶ Analizuj", type="secondary", disabled=not fit_files):
        with st.spinner("Parsowanie plików..."):
            w, m, errs = process_uploads(fit_files, xls_files, json_file)
        for e in errs:
            st.error(e)
        if w > 0 or m > 0:
            st.rerun()
        elif not errs:
            st.warning("Nie znaleziono danych treningowych w wgranych plikach.")

# ── sidebar z opcją dodawania plików ──────────────────────────────────────

def sidebar_upload_extra():
    with st.sidebar.expander("➕ Dodaj więcej plików"):
        fit_files = st.file_uploader("Pliki .fit", type=["fit"],  accept_multiple_files=True, key="up_fit_extra")
        xls_files = st.file_uploader("Fitatu .xls", type=["xls"], accept_multiple_files=True, key="up_xls_extra")
        json_file = st.file_uploader("pomiary.json", type=["json"], key="up_json_extra")
        if st.button("Wgraj", key="btn_extra"):
            with st.spinner("Parsowanie..."):
                w, m, errs = process_uploads(fit_files, xls_files, json_file)
            for e in errs:
                st.sidebar.error(e)
            if w + m > 0:
                st.rerun()

    with st.sidebar.expander("⚖️ Dodaj wagę"):
        w_date = st.date_input("Data", value=pd.Timestamp.today().date(), key="w_date")
        w_kg   = st.number_input("Waga (kg)", min_value=30.0, max_value=250.0,
                                  value=80.0, step=0.1, key="w_kg")
        if st.button("Zapisz wagę", key="w_save"):
            st.session_state.conn.execute(
                "INSERT OR REPLACE INTO weight (date, weight_kg) VALUES (?,?)",
                (str(w_date), w_kg),
            )
            st.session_state.conn.commit()
            st.sidebar.success(f"Zapisano: {w_date} → {w_kg:.1f} kg")
            st.rerun()

    st.sidebar.divider()

    db_bytes = export_db_bytes(st.session_state.conn)
    st.sidebar.download_button(
        "💾 Eksportuj bazę danych",
        data=db_bytes,
        file_name="garmin_dashboard.db",
        mime="application/octet-stream",
        use_container_width=True,
    )

    if st.sidebar.button("Wyczyść dane i zacznij od nowa", type="secondary", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# ── GŁÓWNY WIDOK DASHBOARDU ────────────────────────────────────────────────

if not has_workouts():
    show_upload_page()
    st.stop()

# Sidebar
st.sidebar.title("💪 Garmin Dashboard")
st.sidebar.subheader("Zakres dat")

workouts = query("""
    SELECT id,
           COALESCE(workout_name, 'Trening') AS workout_name,
           DATE(start_time) AS date,
           ROUND(total_elapsed_time_sec / 60.0, 1) AS duration_min,
           total_calories AS calories
    FROM workouts
    ORDER BY start_time
""")
workouts["date"] = pd.to_datetime(workouts["date"])

min_date = workouts["date"].min().date()
max_date = workouts["date"].max().date()

date_from, date_to = st.sidebar.date_input(
    "Od — Do",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

workouts = workouts[
    (workouts["date"].dt.date >= date_from) &
    (workouts["date"].dt.date <= date_to)
]

if workouts.empty:
    st.warning("Brak treningów w wybranym zakresie dat.")
    st.stop()

# Wolumen per trening
all_sets = query("""
    SELECT w.id AS workout_id, DATE(w.start_time) AS date, s.volume_kg
    FROM sets s
    JOIN workouts w ON w.id = s.workout_id
    WHERE s.set_type = 'active' AND s.volume_kg IS NOT NULL
""")

vol_per_workout = (
    all_sets.groupby("workout_id")["volume_kg"]
    .sum().reset_index()
    .rename(columns={"volume_kg": "total_volume_kg"})
)
workouts = workouts.merge(vol_per_workout, left_on="id", right_on="workout_id", how="left")
workouts["total_volume_kg"] = workouts["total_volume_kg"].fillna(0)

# ── metryki ────────────────────────────────────────────────────────────────

total_workouts  = len(workouts)
first_date      = workouts["date"].min()
last_date       = workouts["date"].max()
weeks_span      = max(((last_date - first_date).days / 7), 1)
avg_per_week    = total_workouts / weeks_span
avg_calories    = workouts["calories"].mean()
avg_duration    = workouts["duration_min"].mean()
avg_volume_kg   = workouts[workouts["total_volume_kg"] > 0]["total_volume_kg"].mean()
total_volume_kg = workouts["total_volume_kg"].sum()

by_type = (
    workouts.groupby("workout_name").size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
)

st.title("💪 Podsumowanie treningów")
st.caption(f"Okres: {first_date.date()} — {last_date.date()}  |  Łącznie {total_workouts} treningów")
st.divider()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Treningi / tydzień",      f"{avg_per_week:.1f}")
c2.metric("Śr. czas treningu",       f"{avg_duration:.0f} min")
c3.metric("Śr. kalorie / trening",   f"{avg_calories:.0f} kcal")
c4.metric("Śr. kalorie / tydzień",   f"{avg_calories * avg_per_week:.0f} kcal")
c5.metric("Śr. kg / trening",        f"{avg_volume_kg:,.0f} kg" if avg_volume_kg > 0 else "—")

st.divider()

# ── rodzaje treningów ──────────────────────────────────────────────────────

st.subheader("Rodzaje treningów")
col_a, col_b = st.columns([1, 2])

with col_a:
    st.dataframe(
        by_type.rename(columns={"workout_name": "Typ treningu", "count": "Liczba"}),
        use_container_width=True, hide_index=True,
    )

with col_b:
    fig_types = px.bar(
        by_type, x="workout_name", y="count",
        labels={"workout_name": "", "count": "Liczba treningów"},
        color="count", color_continuous_scale="Blues",
    )
    fig_types.update_layout(coloraxis_showscale=False, height=300)
    st.plotly_chart(fig_types, use_container_width=True)

st.divider()

# ── historia kalorii i czasu ───────────────────────────────────────────────

st.subheader("Historia")
col_h1, col_h2 = st.columns(2)

with col_h1:
    cal_delta, cal_dir = trend_delta(workouts["calories"])
    st.metric("Trend kalorii (1. vs 2. połowa okresu)", "", delta=cal_delta, delta_color=cal_dir or "normal")
    fig_cal = px.line(
        workouts, x="date", y="calories",
        title="Kalorie per trening",
        labels={"date": "Data", "calories": "kcal"},
        markers=True, color_discrete_sequence=["#f39c12"],
    )
    fig_cal.update_layout(height=280)
    st.plotly_chart(fig_cal, use_container_width=True)

with col_h2:
    dur_delta, dur_dir = trend_delta(workouts["duration_min"])
    st.metric("Trend czasu (1. vs 2. połowa okresu)", "", delta=dur_delta, delta_color=dur_dir or "normal")
    fig_dur = px.line(
        workouts, x="date", y="duration_min",
        title="Czas treningu",
        labels={"date": "Data", "duration_min": "min"},
        markers=True, color_discrete_sequence=["#27ae60"],
    )
    fig_dur.update_layout(height=280)
    st.plotly_chart(fig_dur, use_container_width=True)

# ── wolumen ────────────────────────────────────────────────────────────────

vol_data = workouts[workouts["total_volume_kg"] > 0]
if not vol_data.empty:
    st.divider()
    vol_delta, vol_dir = trend_delta(vol_data["total_volume_kg"])
    col_v1, col_v2 = st.columns([3, 1])
    with col_v1:
        st.subheader("Wolumen (kg) per trening")
    with col_v2:
        st.metric("Trend wolumenu", "", delta=vol_delta, delta_color=vol_dir or "normal")
    fig_vol = px.line(
        vol_data, x="date", y="total_volume_kg",
        labels={"date": "Data", "total_volume_kg": "Wolumen (kg)"},
        markers=True, color_discrete_sequence=["#8e44ad"],
    )
    fig_vol.update_layout(height=300)
    st.plotly_chart(fig_vol, use_container_width=True)
    st.caption(f"Łączny wolumen wszystkich treningów: {total_volume_kg:,.0f} kg")

# ── analiza ćwiczeń — link do osobnej strony ──────────────────────────────

st.divider()
st.info("Szczegółowa analiza ćwiczeń (rekordy, 1RM, wskaźnik siły) dostępna na stronie **Ćwiczenia** w menu po lewej.")

# ── dieta (Fitatu) ─────────────────────────────────────────────────────────

st.divider()
st.subheader("Dieta (Fitatu)")

diet = query("SELECT date, kcal, protein, fat, carbs FROM diet ORDER BY date")

if diet.empty:
    st.info("Brak danych diety. Wgraj pliki .xls z Fitatu.")
else:
    diet["date"] = pd.to_datetime(diet["date"])
    diet = diet[
        (diet["date"].dt.date >= date_from) &
        (diet["date"].dt.date <= date_to)
    ]

    if diet.empty:
        st.info("Brak danych diety w wybranym zakresie dat.")
    else:
        avg_kcal    = diet["kcal"].mean()
        avg_protein = diet["protein"].mean()
        avg_fat     = diet["fat"].mean()
        avg_carbs   = diet["carbs"].mean()

        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Śr. kalorie / dzień",  f"{avg_kcal:.0f} kcal")
        d2.metric("Śr. białko / dzień",   f"{avg_protein:.1f} g")
        d3.metric("Śr. tłuszcze / dzień", f"{avg_fat:.1f} g")
        d4.metric("Śr. węgle / dzień",    f"{avg_carbs:.1f} g")

        fig_kcal = px.line(
            diet, x="date", y="kcal", markers=True,
            title="Kalorie dziennie",
            labels={"date": "Data", "kcal": "kcal"},
            color_discrete_sequence=["#e74c3c"],
        )
        fig_kcal.add_hline(
            y=avg_kcal, line_dash="dot",
            annotation_text=f"średnia {avg_kcal:.0f} kcal",
            annotation_position="top left",
            line_color="rgba(231,76,60,0.4)",
        )
        fig_kcal.update_layout(height=300)
        st.plotly_chart(fig_kcal, use_container_width=True)

        macro = diet[["date", "protein", "fat", "carbs"]].melt(
            id_vars="date", var_name="makro", value_name="g"
        )
        macro["makro"] = macro["makro"].map(
            {"protein": "Białko", "fat": "Tłuszcze", "carbs": "Węgle"}
        )
        fig_macro = px.line(
            macro, x="date", y="g", color="makro", markers=True,
            title="Makroskładniki dziennie (g)",
            labels={"date": "Data", "g": "g", "makro": ""},
            color_discrete_map={"Białko": "#3498db", "Tłuszcze": "#f39c12", "Węgle": "#2ecc71"},
        )
        fig_macro.update_layout(height=320, legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_macro, use_container_width=True)

# ── waga ───────────────────────────────────────────────────────────────────

st.divider()
st.subheader("Waga")

waga = query("SELECT date, weight_kg FROM weight ORDER BY date")

if waga.empty:
    st.info("Brak danych wagi. Wgraj plik `pomiary.json` z Fitatu lub dodaj ręcznie (⚖️ w sidebarze).")
else:
    waga["date"] = pd.to_datetime(waga["date"])
    waga = waga[
        (waga["date"].dt.date >= date_from) &
        (waga["date"].dt.date <= date_to)
    ]

    if waga.empty:
        st.info("Brak danych wagi w wybranym zakresie dat.")
    else:
        first_kg = waga["weight_kg"].iloc[0]
        last_kg  = waga["weight_kg"].iloc[-1]
        total_ch = last_kg - first_kg

        w1, w2 = st.columns(2)
        w1.metric("Aktualna waga", f"{last_kg:.1f} kg",
                  delta=f"{total_ch:+.1f} kg od początku", delta_color="inverse")
        w2.metric("Liczba pomiarów", str(len(waga)))

        fig_waga = px.line(
            waga, x="date", y="weight_kg", markers=True,
            title="Historia wagi",
            labels={"date": "Data", "weight_kg": "Waga (kg)"},
            color_discrete_sequence=["#e67e22"],
        )
        fig_waga.update_layout(height=320)
        st.plotly_chart(fig_waga, use_container_width=True)

# ── aktywność dzienna ──────────────────────────────────────────────────────

st.divider()
st.subheader("Aktywność dzienna")

daily = query("""
    SELECT date, steps, distance_km, calories_active, resting_hr, max_hr, avg_hr
    FROM daily_stats ORDER BY date
""")

if daily.empty:
    st.info("Brak danych aktywności. Wgraj pliki `.fit` z folderu `Garmin\\Monitor\\`.")
else:
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily[
        (daily["date"].dt.date >= date_from) &
        (daily["date"].dt.date <= date_to)
    ]

    if not daily.empty:
        avg_steps = daily["steps"].dropna().mean()
        avg_rhr   = daily["resting_hr"].dropna().mean()
        avg_dist  = daily["distance_km"].dropna().mean()
        avg_cal   = daily["calories_active"].dropna().mean()

        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Śr. kroki / dzień",     f"{avg_steps:,.0f}" if avg_steps else "—")
        a2.metric("Śr. tętno spoczynkowe", f"{avg_rhr:.0f} bpm" if avg_rhr else "—")
        a3.metric("Śr. dystans / dzień",   f"{avg_dist:.1f} km" if avg_dist else "—")
        a4.metric("Śr. kalorie aktywne",   f"{avg_cal:.0f} kcal" if avg_cal else "—")

        fig_steps = px.bar(
            daily, x="date", y="steps",
            title="Kroki dziennie",
            labels={"date": "Data", "steps": "Kroki"},
            color_discrete_sequence=["#3498db"],
        )
        fig_steps.add_hline(
            y=avg_steps, line_dash="dot",
            annotation_text=f"średnia {avg_steps:,.0f}",
            annotation_position="top left",
            line_color="rgba(52,152,219,0.5)",
        )
        fig_steps.update_layout(height=280)
        st.plotly_chart(fig_steps, use_container_width=True)

        col_hr, col_hr2 = st.columns(2)
        with col_hr:
            hr_data = daily[daily["resting_hr"].notna()]
            if not hr_data.empty:
                fig_hr = px.line(
                    hr_data, x="date", y="resting_hr", markers=True,
                    title="Tętno spoczynkowe",
                    labels={"date": "Data", "resting_hr": "bpm"},
                    color_discrete_sequence=["#e74c3c"],
                )
                fig_hr.update_layout(height=260)
                st.plotly_chart(fig_hr, use_container_width=True)
        with col_hr2:
            hr_data2 = daily[daily["avg_hr"].notna()]
            if not hr_data2.empty:
                fig_hr2 = px.line(
                    hr_data2, x="date", y="avg_hr", markers=True,
                    title="Śr. tętno dzienne",
                    labels={"date": "Data", "avg_hr": "bpm"},
                    color_discrete_sequence=["#e67e22"],
                )
                fig_hr2.update_layout(height=260)
                st.plotly_chart(fig_hr2, use_container_width=True)

# ── sidebar: dodaj pliki / wyczyść ────────────────────────────────────────

sidebar_upload_extra()
