"""
Parser plików monitoringowych Garmina (Garmin/Monitor/*.fit)
Zawierają: kroki, tętno, kalorie, dystans — zbierane przez cały dzień

Użycie:
    python parser/parse_monitor.py <katalog_z_plikami_fit>
"""

import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path

from fitparse import FitFile

DB_PATH = Path(__file__).parent.parent / "data" / "garmin.db"

# Epoka FIT: 1989-12-31 00:00:00 UTC
FIT_EPOCH = datetime(1989, 12, 31, 0, 0, 0, tzinfo=timezone.utc)


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            date            TEXT PRIMARY KEY,
            steps           INTEGER,
            distance_km     REAL,
            calories_active INTEGER,
            resting_hr      INTEGER,
            max_hr          INTEGER,
            avg_hr          INTEGER
        );

        CREATE TABLE IF NOT EXISTS monitoring_raw (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            date        TEXT NOT NULL,
            steps       INTEGER,
            heart_rate  INTEGER,
            distance_m  REAL,
            calories    INTEGER,
            activity_type TEXT,
            UNIQUE(timestamp, activity_type)
        );

        CREATE INDEX IF NOT EXISTS idx_monitoring_date ON monitoring_raw(date);
        CREATE INDEX IF NOT EXISTS idx_daily_date      ON daily_stats(date);
    """)
    conn.commit()


def get_field(msg, name, default=None):
    for f in msg.fields:
        if f.name == name:
            return f.value
    return default


def resolve_ts16(ts16: int, last_full_ts: datetime) -> datetime | None:
    """Rekonstruuje pełny timestamp z 16-bitowej wartości."""
    if last_full_ts is None:
        return None
    # FIT timestamp to sekundy od FIT_EPOCH
    last_sec = int((last_full_ts - FIT_EPOCH).total_seconds())
    # Podmień dolne 16 bitów
    full_sec = (last_sec & ~0xFFFF) | (ts16 & 0xFFFF)
    # Jeśli wynik jest wcześniejszy niż last_sec, dodaj 65536
    if full_sec < last_sec:
        full_sec += 0x10000
    return FIT_EPOCH + timedelta(seconds=full_sec)


def parse_monitor_file(fit_path: str, conn: sqlite3.Connection):
    file_name = Path(fit_path).name
    fit = FitFile(fit_path)

    last_full_ts = None
    hr_values = []     # (datetime, bpm)
    step_records = []  # (date_str, steps, distance_m, calories)

    for msg in fit.get_messages("monitoring"):
        fields = {f.name: f.value for f in msg.fields}

        # Pełny timestamp
        if "timestamp" in fields and isinstance(fields["timestamp"], datetime):
            ts = fields["timestamp"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            last_full_ts = ts
            date_str = ts.strftime("%Y-%m-%d")

            steps      = fields.get("steps")
            distance   = fields.get("distance")
            calories   = fields.get("active_calories")
            act_type   = str(fields.get("activity_type", ""))

            if steps is not None:
                step_records.append((date_str, steps, distance, calories, act_type))

            # HR może być w tym samym rekordzie
            hr = fields.get("heart_rate")
            if hr and hr > 0:
                hr_values.append((ts, hr))

        # 16-bitowy timestamp (rekordy tętna)
        elif "timestamp_16" in fields:
            ts16 = fields["timestamp_16"]
            hr   = fields.get("heart_rate")
            if hr and hr > 0 and last_full_ts is not None:
                full_ts = resolve_ts16(ts16, last_full_ts)
                if full_ts:
                    hr_values.append((full_ts, hr))

    # Zapisz rekordy kroków
    inserted = 0
    for (date_str, steps, distance, calories, act_type) in step_records:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO monitoring_raw
                    (timestamp, date, steps, distance_m, calories, activity_type)
                VALUES (?,?,?,?,?,?)
            """, (date_str + "T" + act_type, date_str, steps, distance, calories, act_type))
            inserted += 1
        except Exception:
            pass

    # Zapisz rekordy tętna
    for (ts, hr) in hr_values:
        date_str = ts.strftime("%Y-%m-%d")
        ts_str   = ts.strftime("%Y-%m-%dT%H:%M:%S")
        try:
            conn.execute("""
                INSERT OR IGNORE INTO monitoring_raw
                    (timestamp, date, heart_rate, activity_type)
                VALUES (?,?,?,?)
            """, (ts_str, date_str, hr, "hr"))
        except Exception:
            pass

    conn.commit()
    print(f"  OK: {file_name} — {len(step_records)} epok kroków, {len(hr_values)} pomiarów HR")


def aggregate_daily(conn: sqlite3.Connection):
    """Agreguje monitoring_raw → daily_stats."""

    # Kroki: max steps z walking (skumulowane dla danego dnia)
    steps_rows = conn.execute("""
        SELECT date,
               MAX(steps)    AS steps,
               MAX(distance_m) / 1000.0 AS distance_km,
               MAX(calories) AS calories_active
        FROM monitoring_raw
        WHERE activity_type = 'walking'
        GROUP BY date
    """).fetchall()

    # Tętno: min/max/avg z rekordów hr
    hr_rows = conn.execute("""
        SELECT date,
               MIN(heart_rate) AS resting_hr,
               MAX(heart_rate) AS max_hr,
               ROUND(AVG(heart_rate)) AS avg_hr
        FROM monitoring_raw
        WHERE activity_type = 'hr'
          AND heart_rate > 30
        GROUP BY date
    """).fetchall()

    # Połącz po dacie
    hr_by_date = {r[0]: r[1:] for r in hr_rows}

    updated = 0
    for (date, steps, distance_km, calories) in steps_rows:
        hr = hr_by_date.get(date, (None, None, None))
        conn.execute("""
            INSERT INTO daily_stats
                (date, steps, distance_km, calories_active, resting_hr, max_hr, avg_hr)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                steps           = excluded.steps,
                distance_km     = excluded.distance_km,
                calories_active = excluded.calories_active,
                resting_hr      = excluded.resting_hr,
                max_hr          = excluded.max_hr,
                avg_hr          = excluded.avg_hr
        """, (date, steps, distance_km, calories, hr[0], hr[1], hr[2]))
        updated += 1

    # Dni z samym HR (bez kroków)
    for date, (rhr, max_hr, avg_hr) in hr_by_date.items():
        conn.execute("""
            INSERT INTO daily_stats (date, resting_hr, max_hr, avg_hr)
            VALUES (?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                resting_hr = COALESCE(daily_stats.resting_hr, excluded.resting_hr),
                max_hr     = COALESCE(daily_stats.max_hr, excluded.max_hr),
                avg_hr     = COALESCE(daily_stats.avg_hr, excluded.avg_hr)
        """, (date, rhr, max_hr, avg_hr))

    conn.commit()
    print(f"  Zsumowano {updated} dni do daily_stats")


def parse_monitor_bytes(data: bytes, file_name: str, conn: sqlite3.Connection):
    """Wersja parse_monitor_file przyjmująca surowe bajty zamiast ścieżki."""
    fit = FitFile(BytesIO(data))
    last_full_ts = None
    hr_values    = []
    step_records = []

    for msg in fit.get_messages("monitoring"):
        fields = {f.name: f.value for f in msg.fields}
        if "timestamp" in fields and isinstance(fields["timestamp"], datetime):
            ts = fields["timestamp"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            last_full_ts = ts
            date_str   = ts.strftime("%Y-%m-%d")
            steps      = fields.get("steps")
            distance   = fields.get("distance")
            calories   = fields.get("active_calories")
            act_type   = str(fields.get("activity_type", ""))
            if steps is not None:
                step_records.append((date_str, steps, distance, calories, act_type))
            hr = fields.get("heart_rate")
            if hr and hr > 0:
                hr_values.append((ts, hr))
        elif "timestamp_16" in fields:
            ts16 = fields["timestamp_16"]
            hr   = fields.get("heart_rate")
            if hr and hr > 0 and last_full_ts is not None:
                full_ts = resolve_ts16(ts16, last_full_ts)
                if full_ts:
                    hr_values.append((full_ts, hr))

    for (date_str, steps, distance, calories, act_type) in step_records:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO monitoring_raw
                    (timestamp, date, steps, distance_m, calories, activity_type)
                VALUES (?,?,?,?,?,?)
            """, (date_str + "T" + act_type, date_str, steps, distance, calories, act_type))
        except Exception:
            pass

    for (ts, hr) in hr_values:
        date_str = ts.strftime("%Y-%m-%d")
        ts_str   = ts.strftime("%Y-%m-%dT%H:%M:%S")
        try:
            conn.execute("""
                INSERT OR IGNORE INTO monitoring_raw
                    (timestamp, date, heart_rate, activity_type)
                VALUES (?,?,?,?)
            """, (ts_str, date_str, hr, "hr"))
        except Exception:
            pass

    conn.commit()


def import_directory(directory: str):
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    fit_files = sorted(Path(directory).glob("*.FIT")) + sorted(Path(directory).glob("*.fit"))
    # usuń duplikaty (różne case)
    seen = set()
    unique_files = []
    for f in fit_files:
        if f.name.upper() not in seen:
            seen.add(f.name.upper())
            unique_files.append(f)

    if not unique_files:
        print(f"Brak plików .fit w: {directory}")
        conn.close()
        return

    print(f"Znaleziono {len(unique_files)} plików monitoringowych\n")
    for f in unique_files:
        print(f"Importuję: {f.name}")
        try:
            parse_monitor_file(str(f), conn)
        except Exception as e:
            print(f"  BŁĄD: {e}")

    print("\nAgreguję dane dzienne...")
    aggregate_daily(conn)
    conn.close()
    print(f"\nBaza danych: {DB_PATH}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Użycie: python parse_monitor.py <katalog_lub_plik.fit>")
        sys.exit(1)

    target = sys.argv[1]
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    if Path(target).is_dir():
        conn.close()
        import_directory(target)
    else:
        print(f"Importuję: {target}")
        parse_monitor_file(target, conn)
        print("\nAgreguję dane dzienne...")
        aggregate_daily(conn)
        conn.close()
