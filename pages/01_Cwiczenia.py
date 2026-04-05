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
    if reps <= 0:
        return 0.0
    return float(weight) if reps == 1 else weight * (1 + reps / 30)


def wilks_points(lift_kg: float, bw_kg: float, male: bool = True) -> float:
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
    return lift_kg * 500 / denom if denom > 0 else 0.0


def wilks_level(pts: float) -> str:
    if pts < 150:  return "Rekreacyjny"
    if pts < 250:  return "Amator"
    if pts < 350:  return "Dobry amator"
    if pts < 450:  return "Zaawansowany"
    if pts < 550:  return "Krajowy poziom"
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


def bodyweight_series() -> pd.DataFrame | None:
    raw = st.session_state.get("pomiary", {})
    pomiary = raw.get("pomiary", [])
    if not pomiary:
        return None
    df = pd.DataFrame(pomiary)[["data", "pomiar_kg"]].rename(
        columns={"data": "date", "pomiar_kg": "bw_kg"}
    )
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def merge_bodyweight(df: pd.DataFrame, bw_fallback: float, male: bool) -> pd.DataFrame:
    """Przypisuje masę ciała per data i oblicza wilks oraz pct_bw."""
    df = df.copy()
    bw_df = bodyweight_series()
    if bw_df is not None:
        df = pd.merge_asof(
            df.sort_values("date"), bw_df, on="date", direction="nearest"
        ).sort_values("date")
    else:
        df["bw_kg"] = bw_fallback
    df["wilks"]  = df.apply(lambda r: wilks_points(r["max_1rm"], r["bw_kg"], male), axis=1)
    df["pct_bw"] = df["max_1rm"] / df["bw_kg"] * 100
    return df


def period_trend(df_ex: pd.DataFrame, days: int) -> float | None:
    """
    Porównuje max est_1rm z ostatnich `days` dni vs poprzedni taki sam okres.
    Zwraca % zmiany lub None jeśli brak danych w którymś z okresów.
    """
    today = df_ex["date"].max()
    cur = df_ex[df_ex["date"] >= today - pd.Timedelta(days=days)]["est_1rm"].max()
    prv = df_ex[
        (df_ex["date"] >= today - pd.Timedelta(days=2 * days)) &
        (df_ex["date"] <  today - pd.Timedelta(days=days))
    ]["est_1rm"].max()
    if pd.isna(cur) or pd.isna(prv) or prv == 0:
        return None
    return (cur - prv) / prv * 100


def trend_badge(pct: float | None) -> str:
    """Zwraca kolorowy znacznik trendu w markdown."""
    if pct is None:
        return "—"
    color = "green" if pct >= 0 else "red"
    sign  = "+" if pct >= 0 else ""
    return f":{color}[{sign}{pct:.1f}%]"


def zoomed_chart(fig, height=340):
    """Oś Y zaczyna tuż pod minimum — trendy czytelniejsze."""
    fig.update_yaxes(rangemode="normal")
    fig.update_layout(
        height=height,
        legend=dict(orientation="h", y=-0.2),
        hovermode="x unified",
    )
    return fig


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

bw_from_file = current_bodyweight()
if bw_from_file:
    st.sidebar.caption(f"Masa ciała z pomiary.json: **{bw_from_file:.1f} kg**")
bw_manual = st.sidebar.number_input(
    "Masa ciała (kg)",
    min_value=40.0, max_value=200.0,
    value=float(bw_from_file) if bw_from_file else 80.0,
    step=0.5,
    help="Używana gdy brak pomiary.json lub jako nadpisanie",
)

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

bw = bw_manual

# ── dane per sesja ─────────────────────────────────────────────────────────

per_day_all = (
    ex.groupby(["exercise", "date"])
    .agg(max_weight=("weight_kg", "max"), max_1rm=("est_1rm", "max"), volume=("volume_kg", "sum"))
    .reset_index()
)
per_day_all = merge_bodyweight(per_day_all, bw, male)

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
    col.metric("Rekord (PR)",       f"{pr_weight:.1f} kg × {pr_reps}")
    col.metric("Est. 1RM",          f"{pr_1rm:.1f} kg", help="Wzór Epley'a: w × (1 + r/30)")
    col.metric("Siła / masa ciała", f"{pr_1rm / bw * 100:.0f}%",
               help="Est. 1RM jako % aktualnej masy ciała")
    w_pts = wilks_points(pr_1rm, bw, male)
    col.metric("Wilks Points", f"{w_pts:.1f}", delta=wilks_level(w_pts), delta_color="off")

    t1w = period_trend(df_ex, 7)
    t1m = period_trend(df_ex, 30)
    t3m = period_trend(df_ex, 90)
    col.markdown(
        f"**Trend 1RM** (est.)  \n"
        f"&nbsp;&nbsp;1T: {trend_badge(t1w)}&nbsp;&nbsp;"
        f"1M: {trend_badge(t1m)}&nbsp;&nbsp;"
        f"3M: {trend_badge(t3m)}"
    )

st.divider()

# ── wykresy — 5 zakładek ───────────────────────────────────────────────────

tab_w, tab_1rm, tab_pct, tab_wilks, tab_vol = st.tabs([
    "Ciężar (kg)", "Est. 1RM (kg)", "% masy ciała", "Wilks Points", "Wolumen",
])

with tab_w:
    fig = px.line(per_day_all, x="date", y="max_weight", color="exercise", markers=True,
                  labels={"date": "Data", "max_weight": "Maks. ciężar (kg)", "exercise": "Ćwiczenie"})
    st.plotly_chart(zoomed_chart(fig), use_container_width=True)

with tab_1rm:
    fig = px.line(per_day_all, x="date", y="max_1rm", color="exercise", markers=True,
                  labels={"date": "Data", "max_1rm": "Est. 1RM (kg)", "exercise": "Ćwiczenie"})
    st.plotly_chart(zoomed_chart(fig), use_container_width=True)

with tab_pct:
    bw_source = "per data (pomiary.json)" if bodyweight_series() is not None else f"stała {bw:.1f} kg"
    st.caption(f"Est. 1RM jako % masy ciała ({bw_source}). 100% = ciężar równy masie ciała.")
    fig = px.line(per_day_all, x="date", y="pct_bw", color="exercise", markers=True,
                  labels={"date": "Data", "pct_bw": "1RM / masa ciała (%)", "exercise": "Ćwiczenie"})
    for val, label in [(100, "100% = masa ciała"), (150, "150%"), (200, "200%")]:
        fig.add_hline(y=val, line_dash="dash", line_color="rgba(255,255,255,0.15)",
                      annotation_text=label, annotation_position="right")
    st.plotly_chart(zoomed_chart(fig), use_container_width=True)

with tab_wilks:
    bw_source = "per data (pomiary.json)" if bodyweight_series() is not None else f"stała {bw:.1f} kg"
    st.caption(f"Masa ciała: {bw_source}. Wyższa wartość = lepsza siła względna.")
    fig = px.line(per_day_all, x="date", y="wilks", color="exercise", markers=True,
                  labels={"date": "Data", "wilks": "Wilks Points", "exercise": "Ćwiczenie"})
    for poziom, val in [("Amator", 250), ("Zaawansowany", 450), ("Elita", 550)]:
        fig.add_hline(y=val, line_dash="dash", line_color="rgba(255,255,255,0.2)",
                      annotation_text=poziom, annotation_position="right")
    st.plotly_chart(zoomed_chart(fig), use_container_width=True)

with tab_vol:
    st.caption("Suma (ciężar × powtórzenia) ze wszystkich serii w danej sesji.")
    fig = px.bar(per_day_all, x="date", y="volume", color="exercise", barmode="group",
                 labels={"date": "Data", "volume": "Wolumen (kg)", "exercise": "Ćwiczenie"})
    fig.update_layout(height=340, legend=dict(orientation="h", y=-0.2), hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

# ── Top 10 (tylko jedno ćwiczenie) ────────────────────────────────────────

if len(selected) == 1:
    df_single = ex[ex["exercise"] == selected[0]]

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

    bw_s = bodyweight_series()
    top10_dates = pd.to_datetime(top10["Data"]).sort_values()
    if bw_s is not None:
        top10_bw = pd.merge_asof(
            pd.DataFrame({"date": top10_dates}), bw_s,
            on="date", direction="nearest",
        )["bw_kg"].values
    else:
        top10_bw = [bw] * len(top10)

    top10["Wilks"] = [
        round(wilks_points(rm, w, male), 1)
        for rm, w in zip(top10["Est. 1RM (kg)"], top10_bw)
    ]
    st.dataframe(top10, use_container_width=True, hide_index=True)
