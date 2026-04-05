"""
Garmin Connect → SQLite  (kroki, kalorie, tętno spoczynkowe, sen, HRV)
Użycie:
    python parser/sync_daily.py --login             # pierwsze uruchomienie
    python parser/sync_daily.py                     # synchronizacja (ostatnie 30 dni)
    python parser/sync_daily.py --days 90           # ostatnie 90 dni
    python parser/sync_daily.py --from 2025-01-01   # od konkretnej daty
"""

import argparse
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import garth

DB_PATH      = Path(__file__).parent.parent / "data" / "garmin.db"
SESSION_DIR  = Path.home() / ".garth"


# ── schemat bazy ──────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            date                TEXT PRIMARY KEY,
            steps               INTEGER,
            distance_km         REAL,
            calories_total      INTEGER,
            calories_active     INTEGER,
            intensity_min       INTEGER,
            resting_hr          INTEGER,
            max_hr              INTEGER,
            stress_avg          INTEGER,
            body_battery_high   INTEGER,
            body_battery_low    INTEGER,
            sleep_total_min     INTEGER,
            sleep_deep_min      INTEGER,
            sleep_light_min     INTEGER,
            sleep_rem_min       INTEGER,
            sleep_score         INTEGER,
            hrv_night           INTEGER,
            hrv_7day_avg        INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_stats(date);
    """)
    conn.commit()


# ── logowanie / sesja ─────────────────────────────────────────────────────

def ensure_session():
    if SESSION_DIR.exists():
        try:
            garth.resume(str(SESSION_DIR))
            garth.client.username  # sprawdź czy sesja działa
            print(f"Sesja wczytana: {garth.client.username}")
            return
        except Exception:
            pass
    raise SystemExit(
        "Brak sesji Garmin. Uruchom najpierw:\n"
        "  python parser/sync_daily.py --login"
    )


def do_login():
    import getpass
    email    = input("Email Garmin Connect: ").strip()
    password = getpass.getpass("Hasło: ")
    garth.login(email, password)
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    garth.save(str(SESSION_DIR))
    print(f"Zalogowano jako: {garth.client.username}")
    print("Sesja zapisana. Kolejne uruchomienia nie wymagają hasła.")


# ── pobieranie danych ─────────────────────────────────────────────────────

def fetch_day(date_str: str) -> dict:
    row = {"date": date_str}

    # Kroki, kalorie, tętno, Body Battery, stres
    try:
        summary = garth.connectapi(
            f"/usersummary-service/usersummary/daily/{date_str}",
            params={"calendarDate": date_str},
        )
        if summary:
            row["steps"]             = summary.get("totalSteps")
            row["distance_km"]       = round((summary.get("totalDistanceMeters") or 0) / 1000, 2)
            row["calories_total"]    = summary.get("totalKilocalories")
            row["calories_active"]   = summary.get("activeKilocalories")
            row["intensity_min"]     = (
                (summary.get("moderateIntensityMinutes") or 0) +
                (summary.get("vigorousIntensityMinutes") or 0)
            )
            row["resting_hr"]        = summary.get("restingHeartRate")
            row["max_hr"]            = summary.get("maxHeartRate")
            row["stress_avg"]        = summary.get("averageStressLevel")
            row["body_battery_high"] = summary.get("bodyBatteryHighestValue")
            row["body_battery_low"]  = summary.get("bodyBatteryLowestValue")
    except Exception as e:
        print(f"  [WARN] summary {date_str}: {e}")

    # Sen
    try:
        sleep_data = garth.connectapi(
            f"/wellness-service/wellness/dailySleepData/{garth.client.username}",
            params={"date": date_str, "nonSleepBufferMinutes": 60},
        )
        sleep = (sleep_data or {}).get("dailySleepDTO", {})
        if sleep:
            row["sleep_total_min"] = round((sleep.get("sleepTimeSeconds") or 0) / 60)
            row["sleep_deep_min"]  = round((sleep.get("deepSleepSeconds") or 0) / 60)
            row["sleep_light_min"] = round((sleep.get("lightSleepSeconds") or 0) / 60)
            row["sleep_rem_min"]   = round((sleep.get("remSleepSeconds") or 0) / 60)
            row["sleep_score"]     = (sleep.get("sleepScores") or {}).get("overall", {}).get("value")
    except Exception as e:
        print(f"  [WARN] sleep {date_str}: {e}")

    # HRV
    try:
        hrv_data = garth.connectapi(f"/hrv-service/hrv/{date_str}")
        summary_hrv = (hrv_data or {}).get("hrvSummary", {})
        if summary_hrv:
            row["hrv_night"]    = summary_hrv.get("lastNight")
            row["hrv_7day_avg"] = summary_hrv.get("weeklyAvg")
    except Exception as e:
        print(f"  [WARN] hrv {date_str}: {e}")

    return row


def upsert(conn: sqlite3.Connection, row: dict):
    cols = list(row.keys())
    vals = [row[c] for c in cols]
    placeholders = ", ".join("?" * len(cols))
    update_set = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "date")
    conn.execute(
        f"INSERT INTO daily_stats ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(date) DO UPDATE SET {update_set}",
        vals,
    )


# ── główna pętla ──────────────────────────────────────────────────────────

def sync(date_from: date, date_to: date):
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    current = date_from
    total = (date_to - date_from).days + 1
    print(f"Synchronizuję {total} dni: {date_from} → {date_to}\n")

    while current <= date_to:
        date_str = current.strftime("%Y-%m-%d")
        print(f"  {date_str}...", end=" ", flush=True)
        row = fetch_day(date_str)
        upsert(conn, row)
        conn.commit()
        print("OK")
        current += timedelta(days=1)

    conn.close()
    print(f"\nGotowe. Zsynchronizowano {total} dni.")


# ── CLI ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--login", action="store_true", help="Zaloguj się do Garmin Connect")
    parser.add_argument("--days", type=int, default=30, help="Liczba dni wstecz (domyślnie 30)")
    parser.add_argument("--from", dest="date_from", help="Data początkowa YYYY-MM-DD")
    args = parser.parse_args()

    if args.login:
        do_login()
    else:
        ensure_session()
        date_to   = date.today()
        date_from = (
            date.fromisoformat(args.date_from)
            if args.date_from
            else date_to - timedelta(days=args.days - 1)
        )
        sync(date_from, date_to)
