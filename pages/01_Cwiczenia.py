"""
Analiza ćwiczeń — rekordy, trendy, 1RM, Wilks Points, wskaźnik siły
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


# ── formuły siłowe ─────────────────────────────────────────────────────────

def epley_1rm(weight: float, reps: int) -> float:
    """Wzór Epley'a: 1RM = w × (1 + r/30)"""
    if reps <= 0:
        return 0.0
    return float(weight) if reps == 1 else weight * (1 + reps / 30)


def wilks_points(lift_kg: float, bw_kg: float, male: bool = True) -> float:
    """
    Wilks Points — klasyczny standard siłowy normalizowany względem masy ciała.
    Źródło: Robert Wilks, IPF (stosowany do 2019).
    """
    if male:
        a, b, c, d, e, f = (
            -216.0475144, 16.2606339, -0.002388645,
            -0.00113732, 7.01863e-06, -1.291e-08,
        )
    else:
        a, b, c, d, e, f = (
            594.31747775582, -27.23842536447, 0.82112226871,
            -0.00930733913, 4.731582e-05, -9.054e-08,
        )
    x = bw_kg
    denom = a + b*x + c*x**2 + d*x**3 + e*x**4 + f*x**5
    if denom <= 0:
        return 0.0
    return lift_kg * 500 / denom


def wilks_level(pts: float) -> str:
    if pts < 150:   return "Rekreacyjny"
    if pts < 250:   return "Amator"
    if pts < 350:   return "Dobry amator"
    if pts < 450:   return "Zaawansowany"
    if pts < 550:   return "Krajowy poziom"
    return "Elita"


def best_1rm_row(df: pd.DataFrame):
    if df.empty:
        return None
    df = df.copy()
    df["est_1rm"] = df.apply(lambda r: epley_1rm(r["weight_kg"], r["repetitions"]), axis=1)
    return df.loc[df["est_1rm"].idxmax()]


def current_bodyweight() -> float | None:
    raw = st.session_state.get("pomiary", {})
    pomiary = raw.get("pomiary", [])
    if not pomiary:
        return None
    return sorted(pomiary, key=lambda x: x.get("data", ""))[-1].get("pomiar_kg")


# ── dane ───────────────────────────────────────────────────────────────────

all_sets = query("""
    SELECT COALESCE(s.exercise_name_pl, s.exercise_name) AS exercise,
           s.weight_kg, s.repetitions, s.volume_kg,
           DATE(w.start_time) AS date
    FROM sets s
    JOIN workouts w ON w.id = s.workout_id
    WHERE s.set_type    = 'active'
      AND s.weight_kg   IS NOT NULL AND s.weight_kg > 0
      AND s.repetitions > 0
    ORDER BY w.start_time
""")

if all_sets.empty:
    st.info("Brak danych o seriach. Wgraj pliki .fit z treningami siłowymi.")
    st.stop()

all_sets["date"] = pd.to_datetime(all_sets["date"])
all_sets["est_1rm"] = all_sets.apply(
    lambda r: epley_1rm(r["weight_kg"], r["repetitions"]), axis=1
)

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

main_exercises = ex_frequency[
    ex_frequency["sessions"] >= min_sessions
]["exercise"].tolist()

selected = st.sidebar.multiselect(
    "Ćwiczenia",
    options=main_exercises,
    default=main_exercises[:min(3, len(main_exercises))],
    placeholder="Wybierz ćwiczenia...",
)

st.sidebar.divider()
male = st.sidebar.radio("Płeć (Wilks)", ["Mężczyzna", "Kobieta"], index=0) == "Mężczyzna"

if not selected:
    st.info("Wybierz co najmniej jedno ćwiczenie w menu po lewej.")
    st.stop()

# ── filtrowanie ────────────────────────────────────────────────────────────

mask = (
    all_sets["exercise"].isin(selected) &
    (all_sets["date"].dt.date >= date_from) &
    (all_sets["date"].dt.date <= date_to)
)
ex = all_sets[mask].copy()

if ex.empty:
    st.warning("Brak danych dla wybranych ćwiczeń w tym zakresie dat.")
    st.stop()

bw = current_bodyweight()

# ── nagłówek ───────────────────────────────────────────────────────────────

st.title("🏋️ Analiza ćwiczeń")
st.caption(f"Zakres: {date_from} — {date_to}")
st.divider()

# ── metryki per ćwiczenie ──────────────────────────────────────────────────

cols = st.columns(len(selected))

for col, ex_name in zip(cols, selected):
    df_ex = ex[ex["exercise"] == ex_name]
    pr = best_1rm_row(df_ex)
    if pr is None:
        continue

    pr_weight = pr["weight_kg"]
    pr_reps   = int(pr["repetitions"])
    pr_1rm    = pr["est_1rm"]

    col.markdown(f"#### {ex_name}")
    col.metric("Rekord (PR)",  f"{pr_weight:.1f} kg × {pr_reps}")
    col.metric("Est. 1RM",     f"{pr_1rm:.1f} kg",
               help="Wzór Epley'a: w × (1 + r/30)")

    if bw:
        ratio = pr_1rm / bw
        col.metric("Siła / masa ciała", f"{ratio:.2f}×")

        w_pts = wilks_points(pr_1rm, bw, male)
        col.metric(
            "Wilks Points",
            f"{w_pts:.1f}",
            delta=wilks_level(w_pts),
            delta_color="off",
            help="Wilks = 1RM × 500 / W(masa_ciała). Porównuje siłę między różnymi wagami ciała.",
        )

st.divider()

# ── wykres: progres ciężaru (wszystkie wybrane ćwiczenia) ─────────────────

st.subheader("Progres ciężaru")

per_day_all = (
    ex.groupby(["exercise", "date"])
    .agg(max_weight=("weight_kg", "max"), max_1rm=("est_1rm", "max"))
    .reset_index()
)

tab1, tab2 = st.tabs(["Maks. ciężar", "Est. 1RM"])

with tab1:
    fig_w = px.line(
        per_day_all, x="date", y="max_weight", color="exercise",
        markers=True,
        labels={"date": "Data", "max_weight": "Maks. ciężar (kg)", "exercise": "Ćwiczenie"},
    )
    fig_w.update_layout(height=360, legend=dict(orientation="h", y=-0.2), hovermode="x unified")
    st.plotly_chart(fig_w, use_container_width=True)

with tab2:
    fig_1rm = px.line(
        per_day_all, x="date", y="max_1rm", color="exercise",
        markers=True,
        labels={"date": "Data", "max_1rm": "Est. 1RM (kg)", "exercise": "Ćwiczenie"},
    )
    fig_1rm.update_layout(height=360, legend=dict(orientation="h", y=-0.2), hovermode="x unified")
    st.plotly_chart(fig_1rm, use_container_width=True)

# ── Wilks trend (jeśli jest masa ciała) ───────────────────────────────────

if bw:
    st.divider()
    st.subheader(f"Wilks Points w czasie  ·  masa ciała: {bw:.1f} kg")

    per_day_all["wilks"] = per_day_all["max_1rm"].apply(
        lambda x: wilks_points(x, bw, male)
    )

    fig_wilks = px.line(
        per_day_all, x="date", y="wilks", color="exercise",
        markers=True,
        labels={"date": "Data", "wilks": "Wilks Points", "exercise": "Ćwiczenie"},
    )

    # Linie poziomów
    for poziom, val in [("Amator", 250), ("Zaawansowany", 450), ("Elita", 550)]:
        fig_wilks.add_hline(
            y=val, line_dash="dash", line_color="rgba(255,255,255,0.2)",
            annotation_text=poziom, annotation_position="right",
        )

    fig_wilks.update_layout(height=340, legend=dict(orientation="h", y=-0.2), hovermode="x unified")
    st.plotly_chart(fig_wilks, use_container_width=True)

# ── wolumen i szczegóły (tylko jedno ćwiczenie) ───────────────────────────

if len(selected) == 1:
    ex_name = selected[0]
    df_single = ex[ex["exercise"] == ex_name]
    per_day_single = (
        df_single.groupby("date")
        .agg(max_weight=("weight_kg", "max"), max_1rm=("est_1rm", "max"), volume=("volume_kg", "sum"))
        .reset_index()
    )

    st.divider()
    st.subheader("Wolumen per sesja")

    avg_vol = per_day_single["volume"].mean()
    fig_vol = go.Figure()
    fig_vol.add_trace(go.Bar(
        x=per_day_single["date"], y=per_day_single["volume"],
        marker_color="#27ae60",
        hovertemplate="%{x|%d.%m.%Y}<br><b>%{y:,.0f} kg</b><extra></extra>",
    ))
    fig_vol.add_hline(
        y=avg_vol, line_dash="dot",
        annotation_text=f"średnia {avg_vol:,.0f} kg",
        annotation_position="top left",
        line_color="rgba(39,174,96,0.5)",
    )
    fig_vol.update_layout(height=300, yaxis_title="kg (ciężar × powtórzenia)")
    st.plotly_chart(fig_vol, use_container_width=True)

    st.divider()
    st.subheader("Top 10 serii (wg est. 1RM)")

    top10 = (
        df_single.nlargest(10, "est_1rm")[["date", "weight_kg", "repetitions", "est_1rm"]]
        .rename(columns={
            "date": "Data", "weight_kg": "Ciężar (kg)",
            "repetitions": "Powtórzenia", "est_1rm": "Est. 1RM (kg)",
        })
        .assign(Data=lambda d: d["Data"].dt.date)
    )
    top10["Est. 1RM (kg)"] = top10["Est. 1RM (kg)"].round(1)
    if bw:
        top10["Wilks"] = top10["Est. 1RM (kg)"].apply(
            lambda x: round(wilks_points(x, bw, male), 1)
        )
    st.dataframe(top10, use_container_width=True, hide_index=True)
