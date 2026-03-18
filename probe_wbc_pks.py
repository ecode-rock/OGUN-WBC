#!/usr/bin/env python3
"""
probe_wbc_pks.py

Scans a range of game_pk values against the Baseball Savant /gf API
to identify all 2026 WBC games.

Definitive WBC check: home_team_data.league.id == 160 (World Baseball Classic)

Known WBC game_pks (reference): 788114, 788115, 788116, 788120
"""

import json
import time
import urllib.request
import sys

SCAN_START = 788050
SCAN_END   = 788280

API_BASE   = "https://baseballsavant.mlb.com/gf?game_pk={}"
DELAY_SECS = 1.0


def fetch_game(game_pk: int) -> dict | None:
    url = API_BASE.format(game_pk)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (WBC-probe/1.0)"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        return None
    except Exception:
        return None


def is_wbc_game(data: dict) -> bool:
    """Check league.id == 160 from home_team_data or away_team_data."""
    for key in ("home_team_data", "away_team_data"):
        td = data.get(key) or {}
        league = td.get("league") or {}
        if str(league.get("id", "")) == "160":
            return True
        if "World Baseball Classic" in str(league.get("name", "")):
            return True
    return False


def extract_game_info(game_pk: int, data: dict) -> dict:
    sb = data.get("scoreboard", {}) or {}

    htd = data.get("home_team_data", {}) or {}
    atd = data.get("away_team_data", {}) or {}

    home_team = htd.get("name", "")
    away_team = atd.get("name", "")

    dt = sb.get("datetime", {}) or {}
    game_date = dt.get("officialDate", "") or data.get("game_date", "")

    status_obj = sb.get("status", {}) or {}
    status = status_obj.get("detailedState", "")

    teams = sb.get("teams", {}) or {}
    home_score = (teams.get("home", {}) or {}).get("score", "")
    away_score = (teams.get("away", {}) or {}).get("score", "")

    venue = (htd.get("venue", {}) or {}).get("name", "")

    division = (htd.get("division", {}) or {}).get("name", "")

    return {
        "game_pk": game_pk,
        "game_date": game_date,
        "away_team": away_team,
        "home_team": home_team,
        "away_score": away_score,
        "home_score": home_score,
        "venue": venue,
        "status": status,
        "division": division,  # will be Pool A/B/C/D
    }


def main():
    print(f"Scanning game_pk range {SCAN_START}–{SCAN_END} for WBC games (league_id=160)")
    print(f"Total to check: {SCAN_END - SCAN_START + 1}")
    print("=" * 70, flush=True)

    wbc_found = []

    for pk in range(SCAN_START, SCAN_END + 1):
        sys.stdout.write(f"\r  pk={pk}  WBC found: {len(wbc_found)}    ")
        sys.stdout.flush()

        data = fetch_game(pk)

        if data is None:
            time.sleep(0.2)
            continue

        if is_wbc_game(data):
            info = extract_game_info(pk, data)
            wbc_found.append(info)
            score = f"{info['away_score']}-{info['home_score']}" if info['away_score'] != "" else "TBD"
            print(f"\n  ✓ WBC [{pk}] {info['game_date']} | {info['away_team']} @ {info['home_team']} | {score} | {info['venue']} | {info['status']} | {info['division']}", flush=True)
            with open("wbc_probe_results.json", "w") as f:
                json.dump(wbc_found, f, indent=2)

        time.sleep(DELAY_SECS)

    print(f"\n\n{'=' * 70}")
    print(f"Scan complete. WBC games found: {len(wbc_found)}")
    print(f"\nAll game_pks: {[g['game_pk'] for g in wbc_found]}")

    print("\n--- All WBC Games (sorted by date, pk) ---")
    for g in sorted(wbc_found, key=lambda x: (x['game_date'], x['game_pk'])):
        score = f"{g['away_score']}-{g['home_score']}" if g['away_score'] != "" else "TBD"
        print(f"  {g['game_date']} | {g['game_pk']} | {g['away_team']} @ {g['home_team']} | {score} | {g['venue']} | {g['status']}")

    with open("wbc_probe_results.json", "w") as f:
        json.dump(wbc_found, f, indent=2)
    print(f"\nResults saved to wbc_probe_results.json")


if __name__ == "__main__":
    main()
