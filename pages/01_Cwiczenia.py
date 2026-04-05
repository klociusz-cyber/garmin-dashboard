"""
Analiza ćwiczeń — rekordy, trendy, 1RM, wskaźnik siły
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Ćwiczenia", page_icon="🏋️", layout="wide")

# ── guard ──────────────────────────────────────────────────────────────────

if "conn" not in st.session_state:
    st.warning("Najpierw wgraj pliki .fit na stronie głównej.")
    st.page_link("dashboard.py", label="Przejdź do strony głównej")
    st.stop()


def query(sql, params=()):
    return pd.read_sql_query(sql, st.session_state.conn, params=params)


# ── 1RM i formuły ──────────────────────────────────────────────────────────

def epley_1rm(weight: float, reps: int) -> float:
    """Wzór Epley'a: 1RM = w × (1 + r/30)"""
    if reps <= 0:
        return 0.0
    if reps == 1:
        return float(weight)
    return weight * (1 + reps / 30)


def best_1rm_row(df: pd.DataFrame):
    """Zwraca wiersz z najwyższym est. 1RM."""
    if df.empty:
        return None
    df = df.copy()
    df["est_1rm"] = df.apply(lambda r: epley_1rm(r["weight_kg"], r["repetitions"]), axis=1)
    return df.loc[df["est_1rm"].idxmax()]


# ── aktualna waga ciała ────────────────────────────────────────────────────

def current_bodyweight() -> float | None:
    raw = st.session_state.get("pomiary", {})
    pomiary = raw.get("pomiary", [])
    if not pomiary:
        return None
    last = sorted(pomiary, key=lambda x: x.get("data", ""))[-1]
    return last.get("pomiar_kg")


# ── dane ───────────────────────────────────────────────────────────────────

all_sets = query("""
    SELECT COALESCE(s.exercise_name_pl, s.exercise_name) AS exercise,
           s.weight_kg,
           s.repetitions,
           s.volume_kg,
           DATE(w.start_time) AS date
    FROM sets s
    JOIN workouts w ON w.id = s.workout_id
    WHERE s.set_type   = 'active'
      AND s.weight_kg  IS NOT NULL
      AND s.weight_kg  > 0
      AND s.repetitions > 0
    ORDER BY w.start_time
""")

if all_sets.empty:
    st.info("Brak danych o seriach. Wgraj pliki .fit z treningami siłowymi.")
    st.stop()

all_sets["date"] = pd.to_datetime(all_sets["date"])

ex_frequency = (
    all_sets.groupby("exercise")["date"]
    .nunique().reset_index(name="sessions")
    .sort_values("sessions", ascending=False)
)

# ── sidebar ────────────────────────────────────────────────────────────────

st.sidebar.title("🏋️ Analiza ćwiczeń")

min_date = all_sets["date"].min().date()
max_date = all_sets["date"].max().date()

date_from, date_to = st.sidebar.date_input(
    "Zakres dat",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

st.sidebar.divider()

min_sessions = st.sidebar.slider(
    "Min. sesji (odsiewa akcesoryjne)",
    min_value=1,
    max_value=max(int(ex_frequency["sessions"].max()), 2),
    value=max(int(ex_frequency["sessions"].max()) // 3, 2),
)

main_exercises = ex_frequency[ex_frequency["sessions"] >= min_sessions]["exercise"].tolist()

selected = st.sidebar.selectbox(
    "Ćwiczenie",
    options=main_exercises,
    index=0,
)

# ── filtrowanie ────────────────────────────────────────────────────────────

mask = (
    (all_sets["exercise"] == selected) &
    (all_sets["date"].dt.date >= date_from) &
    (all_sets["date"].dt.date <= date_to)
)
ex = all_sets[mask].copy()

if ex.empty:
    st.warning("Brak danych dla wybranego ćwiczenia w tym zakresie dat.")
    st.stop()

ex["est_1rm"] = ex.apply(lambda r: epley_1rm(r["weight_kg"], r["repetitions"]), axis=1)

# Per sesja: maks ciężar, maks est. 1RM, łączny wolumen
per_day = (
    ex.groupby("date")
    .agg(
        max_weight=("weight_kg", "max"),
        max_1rm=("est_1rm", "max"),
        volume=("volume_kg", "sum"),
        best_reps=("repetitions", lambda s: s.loc[ex.loc[s.index, "weight_kg"].idxmax()]),
    )
    .reset_index()
)

# ── rekord ─────────────────────────────────────────────────────────────────

pr_row = best_1rm_row(ex)
pr_weight = pr_row["weight_kg"]
pr_reps   = int(pr_row["repetitions"])
pr_1rm    = pr_row["est_1rm"]
pr_date   = pr_row["date"].date()

bw = current_bodyweight()

# ── nagłówek ───────────────────────────────────────────────────────────────

st.title(f"🏋️ {selected}")
st.caption(f"Zakres: {date_from} — {date_to}  |  {len(per_day)} sesji treningowych")
st.divider()

# ── metryki ────────────────────────────────────────────────────────────────

cols = st.columns(5 if bw else 4)

cols[0].metric(
    "Rekord (PR)",
    f"{pr_weight:.1f} kg × {pr_reps}",
    help="Najlepszy zestaw ciężar × powtórzenia w wybranym okresie",
)
cols[1].metric(
    "Est. 1RM",
    f"{pr_1rm:.1f} kg",
    help="Szacowany maksymalny ciężar na 1 powtórzenie (wzór Epley'a)",
)
cols[2].metric(
    "Data PR",
    str(pr_date),
)
cols[3].metric(
    "Łączny wolumen",
    f"{ex['volume_kg'].sum():,.0f} kg",
    help="Suma (ciężar × powtórzenia) ze wszystkich serii w okresie",
)

if bw:
    ratio = pr_1rm / bw

    if ratio < 0.75:
        poziom = "Początkujący"
    elif ratio < 1.0:
        poziom = "Nowicjusz"
    elif ratio < 1.5:
        poziom = "Średniozaawansowany"
    elif ratio < 2.0:
        poziom = "Zaawansowany"
    else:
        poziom = "Elita"

    cols[4].metric(
        "Wskaźnik siły",
        f"{ratio:.2f}×",
        delta=poziom,
        delta_color="off",
        help=f"Est. 1RM ({pr_1rm:.1f} kg) ÷ masa ciała ({bw:.1f} kg)",
    )

st.divider()

# ── wykres 1: progres ciężaru ──────────────────────────────────────────────

col_l, col_r = st.columns(2)

with col_l:
    st.subheader("Progres ciężaru")

    fig_weight = go.Figure()
    fig_weight.add_trace(go.Scatter(
        x=per_day["date"], y=per_day["max_weight"],
        mode="lines+markers",
        name="Maks. ciężar",
        line=dict(color="#3498db", width=2),
        marker=dict(size=6),
        hovertemplate="%{x|%d.%m.%Y}<br><b>%{y:.1f} kg</b><extra></extra>",
    ))
    fig_weight.add_trace(go.Scatter(
        x=per_day["date"], y=per_day["max_1rm"],
        mode="lines",
        name="Est. 1RM",
        line=dict(color="#9b59b6", width=2, dash="dot"),
        hovertemplate="%{x|%d.%m.%Y}<br>Est. 1RM: <b>%{y:.1f} kg</b><extra></extra>",
    ))

    # Linia PR
    fig_weight.add_hline(
        y=pr_1rm, line_dash="dot",
        annotation_text=f"PR est. 1RM: {pr_1rm:.1f} kg",
        annotation_position="top left",
        line_color="rgba(155,89,182,0.4)",
    )

    fig_weight.update_layout(
        height=350,
        legend=dict(orientation="h", y=-0.2),
        yaxis_title="kg",
        xaxis_title="",
        hovermode="x unified",
    )
    st.plotly_chart(fig_weight, use_container_width=True)

# ── wykres 2: wolumen ──────────────────────────────────────────────────────

with col_r:
    st.subheader("Wolumen per sesja (kg)")

    avg_vol = per_day["volume"].mean()
    vol_delta, vol_dir = None, "normal"
    if len(per_day) >= 4:
        mid = len(per_day) // 2
        a1  = per_day["volume"].iloc[:mid].mean()
        a2  = per_day["volume"].iloc[mid:].mean()
        if a1 > 0:
            pct = (a2 - a1) / a1 * 100
            vol_delta = f"{'+' if pct >= 0 else ''}{pct:.1f}%"
            vol_dir   = "normal" if pct >= 0 else "inverse"

    st.metric("Trend wolumenu (1. vs 2. połowa)", "", delta=vol_delta, delta_color=vol_dir or "normal")

    fig_vol = go.Figure()
    fig_vol.add_trace(go.Bar(
        x=per_day["date"], y=per_day["volume"],
        name="Wolumen",
        marker_color="#27ae60",
        hovertemplate="%{x|%d.%m.%Y}<br><b>%{y:,.0f} kg</b><extra></extra>",
    ))
    fig_vol.add_hline(
        y=avg_vol, line_dash="dot",
        annotation_text=f"średnia {avg_vol:,.0f} kg",
        annotation_position="top left",
        line_color="rgba(39,174,96,0.5)",
    )
    fig_vol.update_layout(
        height=320,
        yaxis_title="kg",
        xaxis_title="",
    )
    st.plotly_chart(fig_vol, use_container_width=True)

# ── wskaźnik siły – kontekst ───────────────────────────────────────────────

if bw:
    st.divider()
    st.subheader("Wskaźnik siły na tle masy ciała")

    ratio_history = per_day.copy()
    ratio_history["wskaznik"] = ratio_history["max_1rm"] / bw

    fig_ratio = go.Figure()
    fig_ratio.add_trace(go.Scatter(
        x=ratio_history["date"], y=ratio_history["wskaznik"],
        mode="lines+markers",
        line=dict(color="#f39c12", width=2),
        marker=dict(size=6),
        hovertemplate="%{x|%d.%m.%Y}<br>Wskaźnik: <b>%{y:.2f}×</b><extra></extra>",
    ))

    # Poziomy referencyjne
    for poziom, val, col in [
        ("Nowicjusz",            1.0, "rgba(52,152,219,0.15)"),
        ("Średniozaawansowany",  1.5, "rgba(46,204,113,0.15)"),
        ("Zaawansowany",         2.0, "rgba(231,76,60,0.15)"),
    ]:
        fig_ratio.add_hline(
            y=val, line_dash="dash", line_color="rgba(255,255,255,0.2)",
            annotation_text=poziom,
            annotation_position="right",
        )

    fig_ratio.update_layout(
        height=300,
        yaxis_title="Est. 1RM / masa ciała",
        xaxis_title="",
        hovermode="x unified",
    )
    st.plotly_chart(fig_ratio, use_container_width=True)
    st.caption(
        f"Masa ciała: **{bw:.1f} kg** (ostatni pomiar z Fitatu). "
        "Poziomy referencyjne dla ruchów wielostawowych (przysiad, martwy ciąg, wyciskanie)."
    )

# ── tabela najlepszych serii ───────────────────────────────────────────────

st.divider()
st.subheader("Top 10 serii (wg est. 1RM)")

top10 = (
    ex.nlargest(10, "est_1rm")[["date", "weight_kg", "repetitions", "est_1rm"]]
    .rename(columns={
        "date":       "Data",
        "weight_kg":  "Ciężar (kg)",
        "repetitions":"Powtórzenia",
        "est_1rm":    "Est. 1RM (kg)",
    })
    .assign(**{"Data": lambda d: d["Data"].dt.date})
)
top10["Est. 1RM (kg)"] = top10["Est. 1RM (kg)"].round(1)

st.dataframe(top10, use_container_width=True, hide_index=True)
