#!/usr/bin/env python3
"""
load_wbc.py

Reads WBC_GAMES.md, fetches each Complete game from Baseball Savant /gf,
and loads all pitches into wbc_db.pitches with incremental duplicate prevention.

Column type contract (mirrors load_sept_sample.py)
---------------------------------------------------
BOOLEAN  : is_last_pitch, is_barrel, is_strike_swinging, isSword,
           is_bip_out, is_abs_challenge
FLOAT    : start_speed, end_speed, launch_speed, launch_angle,
           hit_distance, spin_rate, breakX, inducedBreakZ, plate_x,
           plate_z, xba, sz_top, sz_bot, extension, plateTime,
           batSpeed, hc_x_ft, hc_y_ft
INTEGER  : game_pk, inning, ab_number, pitch_number, batter, pitcher,
           outs, balls, strikes, pre_balls, pre_strikes, zone,
           player_total_pitches, pitcher_pa_number,
           pitcher_time_thru_order, team_batting_id, team_fielding_id
DATE     : game_date
TEXT     : everything else

Additional WBC columns
----------------------
tournament_round : section header from WBC_GAMES.md (Pool C, Pool A, etc.)
away_team        : away team name from WBC_GAMES.md
home_team        : home team name from WBC_GAMES.md

Usage
-----
  python load_wbc.py
"""

import os
import re
import sys
import time
import urllib.request
import json

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Boolean, Float, Integer, Date, Text

# ── Connection settings ────────────────────────────────────────────────────────
DB_USER = "postgres"
DB_PASS = os.environ.get("PGPASSWORD", "Manipura1")
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "wbc_db"
TABLE   = "pitches"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GAMES_MD   = os.path.join(SCRIPT_DIR, "WBC_GAMES.md")

API_BASE   = "https://baseballsavant.mlb.com/gf?game_pk={}"
DELAY_SECS = 1.5   # polite delay between API calls

# ── Column type lists (same as load_sept_sample.py) ───────────────────────────
BOOL_COLS = [
    "is_last_pitch",
    "is_barrel",
    "is_strike_swinging",
    "isSword",
    "is_bip_out",
    "is_abs_challenge",
]

FLOAT_COLS = [
    "start_speed", "end_speed", "launch_speed", "launch_angle",
    "hit_distance", "spin_rate", "breakX", "inducedBreakZ",
    "plate_x", "plate_z", "xba", "sz_top", "sz_bot",
    "extension", "plateTime", "batSpeed", "hc_x_ft", "hc_y_ft",
]

INT_COLS = [
    "game_pk", "inning", "ab_number", "pitch_number",
    "batter", "pitcher", "outs", "balls", "strikes",
    "pre_balls", "pre_strikes", "zone",
    "player_total_pitches", "pitcher_pa_number",
    "pitcher_time_thru_order", "team_batting_id", "team_fielding_id",
]

DATE_COL = "game_date"

# Columns to keep from the API (plus derived/added columns appended later)
COLUMN_WHITELIST = [
    "type", "play_id", "inning", "ab_number", "outs",
    "batter", "stand", "batter_name", "pitcher", "p_throws",
    "pitcher_name", "team_batting", "team_fielding",
    "team_batting_id", "team_fielding_id",
    "result", "des", "events", "strikes", "balls",
    "pre_strikes", "pre_balls", "call", "call_name",
    "pitch_type", "pitch_name", "description", "result_code",
    "pitch_call", "is_strike_swinging", "balls_and_strikes",
    "start_speed", "end_speed", "sz_top", "sz_bot",
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ", "isSword", "is_bip_out",
    "pitch_number", "is_abs_challenge", "plate_x", "plate_z",
    "player_total_pitches", "pitcher_pa_number",
    "pitcher_time_thru_order", "game_total_pitches",
    "game_pk",
    # BIP / contact fields (present only on balls in play)
    "launch_speed", "hit_distance", "launch_angle",
    "is_barrel", "hc_x_ft", "hc_y_ft", "xba", "batSpeed",
    # Derived / added by this script
    "is_last_pitch", "game_date",
    "tournament_round", "away_team", "home_team",
]


# ── WBC_GAMES.md parser ───────────────────────────────────────────────────────

def parse_games_md(path: str) -> list[dict]:
    """
    Return list of dicts for every Complete game in WBC_GAMES.md:
      {game_pk, away_team, home_team, tournament_round, game_date}
    """
    games = []
    current_round = None

    # Regex for a table row: | date | game_pk | away | home | score | venue | status |
    row_re = re.compile(
        r"\|\s*(\S+)\s*\|\s*(\d+)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|[^|]+\|[^|]+\|\s*(\w+)\s*\|"
    )
    # Regex for section headers: ## Pool C — Tokyo, Japan
    header_re = re.compile(r"^##\s+(.+)")

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip()
            m_hdr = header_re.match(line)
            if m_hdr:
                header_text = m_hdr.group(1).strip()
                # Normalise: "Pool C — Tokyo, Japan" → "Pool C"
                current_round = header_text.split("—")[0].split("–")[0].strip()
                continue

            m_row = row_re.match(line)
            if m_row and current_round:
                date_str = m_row.group(1).strip()
                game_pk  = int(m_row.group(2))
                away     = m_row.group(3).strip()
                home     = m_row.group(4).strip()
                status   = m_row.group(5).strip()

                if status.lower() == "complete":
                    games.append({
                        "game_pk":          game_pk,
                        "away_team":        away,
                        "home_team":        home,
                        "tournament_round": current_round,
                        "game_date_str":    date_str,
                    })

    return games


# ── API fetch ─────────────────────────────────────────────────────────────────

def fetch_game(game_pk: int) -> dict | None:
    """Fetch /gf JSON for one game. Returns parsed dict or None on error."""
    url = API_BASE.format(game_pk)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (WBC-loader/1.0)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        print(f"  ERROR fetching {game_pk}: {exc}")
        return None


# ── Data normalisation ────────────────────────────────────────────────────────

def _to_bool(s: pd.Series) -> pd.Series:
    """Map common truthy/falsy variants to Python bool. Unknown → NaN (NULL)."""
    mapping = {
        True:  True,  False: False,
        "True": True,  "False": False,
        "true": True,  "false": False,
        "TRUE": True,  "FALSE": False,
        "T":   True,   "F":    False,
        "Y":   True,   "N":    False,
        "1":   True,   "0":    False,
        1:     True,   0:      False,
    }
    return s.map(mapping)


def normalize_game(data: dict, meta: dict) -> pd.DataFrame:
    """
    Convert the raw /gf JSON into a clean DataFrame ready for the DB.
    meta keys: game_pk, away_team, home_team, tournament_round, game_date_str
    """
    # Combine both halves of the game
    pitches = data.get("team_home", []) + data.get("team_away", [])
    if not pitches:
        return pd.DataFrame()

    df = pd.DataFrame(pitches)

    # ── Derive is_last_pitch ───────────────────────────────────────────────────
    # The last pitch in each at-bat has the highest pitch_number for that ab.
    # (events is set on ALL pitches in the AB in this API, so we can't use it.)
    if "pitch_number" in df.columns and "ab_number" in df.columns:
        df["pitch_number_int"] = pd.to_numeric(df["pitch_number"], errors="coerce")
        df["ab_number_int"]    = pd.to_numeric(df["ab_number"],    errors="coerce")
        max_pitch = (
            df.groupby("ab_number_int", dropna=False)["pitch_number_int"]
            .transform("max")
        )
        df["is_last_pitch"] = (df["pitch_number_int"] == max_pitch)
        df.drop(columns=["pitch_number_int", "ab_number_int"], inplace=True)
    else:
        df["is_last_pitch"] = False

    # ── Add game_date from game-level metadata ─────────────────────────────────
    game_date_str = data.get("game_date") or meta.get("game_date_str", "")
    df["game_date"] = game_date_str

    # ── Add WBC metadata columns ───────────────────────────────────────────────
    df["tournament_round"] = meta["tournament_round"]
    df["away_team"]        = meta["away_team"]
    df["home_team"]        = meta["home_team"]

    # ── Apply column whitelist ─────────────────────────────────────────────────
    keep = [c for c in COLUMN_WHITELIST if c in df.columns]
    # Add any whitelist columns that are missing (so every game has same schema)
    for c in COLUMN_WHITELIST:
        if c not in df.columns:
            df[c] = None
    df = df[COLUMN_WHITELIST].copy()

    # ── Type coercions ─────────────────────────────────────────────────────────
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = _to_bool(df[col])

    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce").dt.date

    # ── Sort (mirrors load_sept_sample ordering) ───────────────────────────────
    sort_cols = [c for c in ["game_date", "game_pk", "game_total_pitches"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

    return df


def build_dtype_map(df: pd.DataFrame) -> dict:
    """Return SQLAlchemy dtype dict for all columns in df."""
    dtype_map: dict = {}
    for col in BOOL_COLS:
        if col in df.columns:
            dtype_map[col] = Boolean()
    for col in FLOAT_COLS:
        if col in df.columns:
            dtype_map[col] = Float()
    for col in INT_COLS:
        if col in df.columns:
            dtype_map[col] = Integer()
    if DATE_COL in df.columns:
        dtype_map[DATE_COL] = Date()
    for col in df.columns:
        if col not in dtype_map:
            dtype_map[col] = Text()
    return dtype_map


# ── Duplicate prevention ──────────────────────────────────────────────────────

def get_existing_play_ids(engine) -> set:
    """Return the set of play_ids already loaded into the DB (empty set if table absent)."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT play_id FROM {TABLE} WHERE play_id IS NOT NULL"))
            return {row[0] for row in result}
    except Exception:
        return set()


def table_exists(engine) -> bool:
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=:t)"
        ), {"t": TABLE})
        return result.scalar()


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 62)
    print("WBC Pitch Loader")
    print("=" * 62)

    # ── 1. Parse games list ───────────────────────────────────────────────────
    games = parse_games_md(GAMES_MD)
    if not games:
        print("No Complete games found in WBC_GAMES.md. Exiting.")
        sys.exit(0)

    print(f"\nFound {len(games)} Complete game(s) in WBC_GAMES.md:")
    for g in games:
        print(f"  [{g['tournament_round']}] game_pk={g['game_pk']}  "
              f"{g['away_team']} @ {g['home_team']}")

    # ── 2. DB engine ──────────────────────────────────────────────────────────
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        future=True,
    )

    # ── 3. Fetch and load each game ───────────────────────────────────────────
    existing_ids = get_existing_play_ids(engine)
    print(f"\nExisting play_ids in DB: {len(existing_ids):,}")

    total_new_rows  = 0
    total_skip_rows = 0
    games_loaded    = 0
    first_write     = not table_exists(engine)

    for i, meta in enumerate(games):
        game_pk = meta["game_pk"]
        print(f"\n[{i+1}/{len(games)}] Fetching game_pk={game_pk} "
              f"({meta['away_team']} @ {meta['home_team']}) ...")

        raw = fetch_game(game_pk)
        if raw is None:
            print("  Skipping (fetch failed).")
            continue

        df = normalize_game(raw, meta)
        if df.empty:
            print("  No pitch rows returned. Skipping.")
            continue

        print(f"  Raw rows: {len(df):,}")

        # Deduplicate against existing play_ids
        if "play_id" in df.columns and existing_ids:
            before = len(df)
            df = df[~df["play_id"].isin(existing_ids)]
            skipped = before - len(df)
            total_skip_rows += skipped
            if skipped:
                print(f"  Skipped {skipped:,} duplicate play_ids.")

        if df.empty:
            print("  All rows already loaded. Skipping.")
            continue

        dtype_map = build_dtype_map(df)

        if_exists_mode = "fail" if first_write else "append"
        df.to_sql(
            TABLE,
            engine,
            if_exists=if_exists_mode,
            index=False,
            chunksize=500,
            method="multi",
            dtype=dtype_map,
        )

        # Update our seen set so subsequent games in the same run don't re-add
        if "play_id" in df.columns:
            existing_ids.update(df["play_id"].dropna().tolist())

        total_new_rows += len(df)
        games_loaded   += 1
        first_write     = False
        print(f"  Loaded {len(df):,} rows.")

        if i < len(games) - 1:
            print(f"  Sleeping {DELAY_SECS}s ...")
            time.sleep(DELAY_SECS)

    # ── 4. Summary ────────────────────────────────────────────────────────────
    sep = "-" * 62
    print(f"\n{sep}")
    print(f"New rows loaded  : {total_new_rows:,}")
    print(f"Duplicate rows   : {total_skip_rows:,}")
    print(f"Games processed  : {games_loaded}")

    with engine.connect() as conn:
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        distinct_games = conn.execute(
            text(f"SELECT COUNT(DISTINCT game_pk) FROM {TABLE}")
        ).scalar()
        print(f"\nTotal rows in DB : {total_rows:,}")
        print(f"Distinct games   : {distinct_games}")
    print(f"{sep}")
    print("Done.")


if __name__ == "__main__":
    main()
