"""
Garmin FIT parser → SQLite
Obsługuje pliki z aktywności siłowych (strength_training)
"""

import sqlite3
import os
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path
from fitparse import FitFile


DB_PATH = Path(__file__).parent.parent / "data" / "garmin.db"


def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS workouts (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name                       TEXT NOT NULL,
            workout_name                    TEXT,
            sport                           TEXT,
            sub_sport                       TEXT,
            start_time                      TEXT,
            end_time                        TEXT,
            total_elapsed_time_sec          REAL,
            total_timer_time_sec            REAL,
            total_calories                  INTEGER,
            avg_heart_rate                  INTEGER,
            max_heart_rate                  INTEGER,
            min_heart_rate                  INTEGER,
            total_training_effect           REAL,
            total_anaerobic_training_effect REAL,
            total_cycles                    INTEGER,
            UNIQUE(file_name)
        );

        CREATE TABLE IF NOT EXISTS sets (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id       INTEGER NOT NULL REFERENCES workouts(id),
            set_index        INTEGER,
            set_type         TEXT,
            exercise_name    TEXT,
            exercise_name_pl TEXT,
            weight_kg        REAL,
            repetitions      INTEGER,
            duration_sec     REAL,
            start_time       TEXT,
            end_time         TEXT,
            wkt_step_index   INTEGER,
            volume_kg        REAL  -- weight * reps
        );

        CREATE TABLE IF NOT EXISTS heart_rate (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL REFERENCES workouts(id),
            timestamp  TEXT,
            heart_rate INTEGER,
            distance_m REAL
        );

        CREATE TABLE IF NOT EXISTS weight (
            date      TEXT PRIMARY KEY,
            weight_kg REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS diet (
            date    TEXT PRIMARY KEY,
            kcal    REAL,
            protein REAL,
            fat     REAL,
            carbs   REAL
        );

        CREATE INDEX IF NOT EXISTS idx_sets_workout    ON sets(workout_id);
        CREATE INDEX IF NOT EXISTS idx_sets_exercise   ON sets(exercise_name);
        CREATE INDEX IF NOT EXISTS idx_hr_workout      ON heart_rate(workout_id);
        CREATE INDEX IF NOT EXISTS idx_workouts_start  ON workouts(start_time);
    """)
    conn.commit()


def fix_encoding(value):
    """FIT strings z polskimi znakami są w windows-1250, fitparse zwraca jako latin-1."""
    if isinstance(value, str):
        try:
            return value.encode('latin-1').decode('windows-1250')
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value
    return value


def get_field(msg, name, default=None):
    for f in msg.fields:
        if f.name == name:
            return f.value
    return default


def parse_fit(fit_path: str, conn: sqlite3.Connection) -> int | None:
    file_name = Path(fit_path).name

    # Skip if already imported
    cur = conn.execute("SELECT id FROM workouts WHERE file_name = ?", (file_name,))
    row = cur.fetchone()
    if row:
        print(f"  Pominięto (już w bazie): {file_name}")
        return row[0]

    fit = FitFile(fit_path)

    # --- 1. Collect all messages ---
    sessions       = list(fit.get_messages("session"))
    workouts       = list(fit.get_messages("workout"))
    workout_steps  = list(fit.get_messages("workout_step"))
    exercise_titles= list(fit.get_messages("exercise_title"))
    sets           = list(fit.get_messages("set"))
    records        = list(fit.get_messages("record"))

    if not sessions:
        print(f"  Brak sesji w pliku: {file_name}")
        return None

    # --- 2. Build exercise_title lookup: (category, subtype) → Polish name ---
    title_map: dict[tuple, str] = {}
    for et in exercise_titles:
        cat     = get_field(et, "exercise_category")
        subtype = get_field(et, "exercise_name")  # numeric subtype or None
        name_pl = fix_encoding(get_field(et, "wkt_step_name"))
        if cat is not None:
            title_map[(cat, subtype)] = name_pl
            title_map[(cat, None)]    = name_pl  # fallback without subtype

    # --- 3. Parse session ---
    s = sessions[0]

    def fmt(ts):
        return ts.isoformat() if ts else None

    workout_name = fix_encoding(get_field(workouts[0], "wkt_name")) if workouts else None

    workout_id_row = conn.execute("""
        INSERT INTO workouts (
            file_name, workout_name, sport, sub_sport,
            start_time, end_time,
            total_elapsed_time_sec, total_timer_time_sec,
            total_calories, avg_heart_rate, max_heart_rate, min_heart_rate,
            total_training_effect, total_anaerobic_training_effect,
            total_cycles
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        file_name,
        workout_name,
        get_field(s, "sport"),
        get_field(s, "sub_sport"),
        fmt(get_field(s, "start_time")),
        fmt(get_field(s, "timestamp")),
        get_field(s, "total_elapsed_time"),
        get_field(s, "total_timer_time"),
        get_field(s, "total_calories"),
        get_field(s, "avg_heart_rate"),
        get_field(s, "max_heart_rate"),
        get_field(s, "min_heart_rate"),
        get_field(s, "total_training_effect"),
        get_field(s, "total_anaerobic_training_effect"),
        get_field(s, "total_cycles"),
    ))
    workout_id = workout_id_row.lastrowid

    # --- 4. Parse sets ---
    active_index = 0
    for i, set_msg in enumerate(sets):
        set_type    = get_field(set_msg, "set_type")
        category    = get_field(set_msg, "category")
        subtype     = get_field(set_msg, "category_subtype")
        weight      = get_field(set_msg, "weight")
        reps        = get_field(set_msg, "repetitions")
        duration    = get_field(set_msg, "duration")
        start_time  = fmt(get_field(set_msg, "start_time"))
        end_time    = fmt(get_field(set_msg, "timestamp"))
        step_idx    = get_field(set_msg, "wkt_step_index")

        # Human-readable exercise name
        name_pl = None
        if category:
            name_pl = (
                title_map.get((category, subtype)) or
                title_map.get((category, None))
            )

        volume = (weight * reps) if (weight and reps) else None

        conn.execute("""
            INSERT INTO sets (
                workout_id, set_index, set_type,
                exercise_name, exercise_name_pl,
                weight_kg, repetitions, duration_sec,
                start_time, end_time,
                wkt_step_index, volume_kg
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            workout_id, i, set_type,
            str(category) if category else None, name_pl,
            weight, reps, duration,
            start_time, end_time,
            step_idx, volume,
        ))

    # --- 5. Parse heart rate records ---
    for rec in records:
        hr   = get_field(rec, "heart_rate")
        ts   = fmt(get_field(rec, "timestamp"))
        dist = get_field(rec, "distance")
        if hr:
            conn.execute("""
                INSERT INTO heart_rate (workout_id, timestamp, heart_rate, distance_m)
                VALUES (?,?,?,?)
            """, (workout_id, ts, hr, dist))

    conn.commit()
    print(f"  OK: {file_name} — {len([x for x in sets if get_field(x,'set_type')=='active'])} serii aktywnych, {len(records)} rekordów HR")
    return workout_id


def import_directory(directory: str):
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    fit_files = sorted(Path(directory).glob("*.fit"))
    if not fit_files:
        print(f"Brak plików .fit w: {directory}")
        return

    print(f"Znaleziono {len(fit_files)} plików .fit\n")
    for f in fit_files:
        print(f"Importuję: {f.name}")
        try:
            parse_fit(str(f), conn)
        except Exception as e:
            print(f"  BŁĄD: {e}")

    conn.close()
    print(f"\nBaza danych: {DB_PATH}")


def parse_fit_bytes(data: bytes, file_name: str, conn: sqlite3.Connection) -> int | None:
    """Wersja parse_fit przyjmująca surowe bajty zamiast ścieżki pliku."""
    cur = conn.execute("SELECT id FROM workouts WHERE file_name = ?", (file_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    fit = FitFile(BytesIO(data))

    sessions        = list(fit.get_messages("session"))
    workouts_msg    = list(fit.get_messages("workout"))
    exercise_titles = list(fit.get_messages("exercise_title"))
    sets            = list(fit.get_messages("set"))
    records         = list(fit.get_messages("record"))

    if not sessions:
        return None

    title_map: dict[tuple, str] = {}
    for et in exercise_titles:
        cat     = get_field(et, "exercise_category")
        subtype = get_field(et, "exercise_name")
        name_pl = fix_encoding(get_field(et, "wkt_step_name"))
        if cat is not None:
            title_map[(cat, subtype)] = name_pl
            title_map[(cat, None)]    = name_pl

    s = sessions[0]

    def fmt(ts):
        return ts.isoformat() if ts else None

    workout_name = fix_encoding(get_field(workouts_msg[0], "wkt_name")) if workouts_msg else None

    cur = conn.execute("""
        INSERT INTO workouts (
            file_name, workout_name, sport, sub_sport,
            start_time, end_time,
            total_elapsed_time_sec, total_timer_time_sec,
            total_calories, avg_heart_rate, max_heart_rate, min_heart_rate,
            total_training_effect, total_anaerobic_training_effect,
            total_cycles
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        file_name, workout_name,
        get_field(s, "sport"), get_field(s, "sub_sport"),
        fmt(get_field(s, "start_time")), fmt(get_field(s, "timestamp")),
        get_field(s, "total_elapsed_time"), get_field(s, "total_timer_time"),
        get_field(s, "total_calories"),
        get_field(s, "avg_heart_rate"), get_field(s, "max_heart_rate"), get_field(s, "min_heart_rate"),
        get_field(s, "total_training_effect"), get_field(s, "total_anaerobic_training_effect"),
        get_field(s, "total_cycles"),
    ))
    workout_id = cur.lastrowid

    for i, set_msg in enumerate(sets):
        set_type = get_field(set_msg, "set_type")
        category = get_field(set_msg, "category")
        subtype  = get_field(set_msg, "category_subtype")
        weight   = get_field(set_msg, "weight")
        reps     = get_field(set_msg, "repetitions")
        duration = get_field(set_msg, "duration")
        name_pl  = None
        if category:
            name_pl = title_map.get((category, subtype)) or title_map.get((category, None))
        volume = (weight * reps) if (weight and reps) else None
        conn.execute("""
            INSERT INTO sets (
                workout_id, set_index, set_type,
                exercise_name, exercise_name_pl,
                weight_kg, repetitions, duration_sec,
                start_time, end_time, wkt_step_index, volume_kg
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            workout_id, i, set_type,
            str(category) if category else None, name_pl,
            weight, reps, duration,
            fmt(get_field(set_msg, "start_time")), fmt(get_field(set_msg, "timestamp")),
            get_field(set_msg, "wkt_step_index"), volume,
        ))

    for rec in records:
        hr   = get_field(rec, "heart_rate")
        ts   = fmt(get_field(rec, "timestamp"))
        dist = get_field(rec, "distance")
        if hr:
            conn.execute(
                "INSERT INTO heart_rate (workout_id, timestamp, heart_rate, distance_m) VALUES (?,?,?,?)",
                (workout_id, ts, hr, dist),
            )

    conn.commit()
    return workout_id


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Użycie:")
        print("  python parse_fit.py <plik.fit>           — importuje jeden plik")
        print("  python parse_fit.py <katalog>            — importuje wszystkie .fit z katalogu")
        sys.exit(1)

    target = sys.argv[1]
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    if os.path.isdir(target):
        import_directory(target)
    else:
        print(f"Importuję: {target}")
        parse_fit(target, conn)
        conn.close()
        print(f"\nBaza danych: {DB_PATH}")
