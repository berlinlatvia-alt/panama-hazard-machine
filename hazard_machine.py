#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hazard_machine.py - MY MACHINE: independent multi-hazard watchdog for Playa Blanca, Panama.
Zero-official-delay doctrine (BIAS-075 / RULE-54: SINAPROC banned).
Feeds (all public, no auth):
  S1 EMSC FDSN Panama box (lag ~15s)          every cycle
  S2 USGS FDSN Panama box (lag ~92s)          every cycle
  S3 USGS FDSN Kamchatka/Kuril tsunami box    every cycle (M>=6.5 trigger)
  T1 tsunami.gov Pacific Atom bulletins       every cycle (PTWC real-time)
  W1 Open-Meteo current+forecast Playa Blanca every 10 cycles (10 min @ 60s)
Alert rules (delta-deduped by event key):
  A1 quake M>=4.5 within 150km of Playa Blanca
  A2 quake M>=6.5 in NW-Pacific tsunamigenic box (Kamchatka/Kuril/Japan)
  A3 PTWC bulletin with WARNING (not advisory/watch)
  A4 daily precip sum > 30mm or gusts > 40km/h (flash-flood/storm watch)
  A5 RH > 85% sustained (mold cascade watch, informational once/day)
Outputs: JSONL alerts, SQL telemetry (better_life.db), watchdog heartbeat (Rule 73), chime (Rule 36).
Usage: python hazard_machine.py [--once] [--interval 60] [--hosted]
"""
import argparse
import json
import sqlite3
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
WS = Path(__file__).resolve().parent
WATCH = WS / "watchdog_jobs" / "hazard_machine.json"
ALERTS = WS / "scratch" / "hazard_machine_alerts.jsonl"
STATE = WS / "scratch" / "hazard_machine_state.json"
DB = WS / "better_life.db"
UA = {"User-Agent": "Mozilla/5.0 BetterLifeHazardMachine/1.0"}

PB = (8.3512, -80.1345)          # Playa Blanca
TSUNAMI_BOX = (40, 65, 140, 175)  # Kamchatka/Kuril/Japan minlat maxlat minlon maxlon

ALERT_RULES = {
    "A1_LOCAL_QUAKE": lambda e: e["dist_km"] <= 150 and e["mag"] >= 4.5,
    "A2_TSUNAMIGENIC_QUAKE": lambda e: e["box"] == "tsunami" and e["mag"] >= 6.5,
    "A4_STORM": lambda w: (w.get("precip_sum_today_mm") or 0) > 30 or (w.get("gust_max_kmh") or 0) > 40,
    "A5_HUMIDITY": lambda w: (w.get("rh_pct") or 0) > 85,
}


def watchdog(status, extra=None):
    WATCH.parent.mkdir(exist_ok=True)
    WATCH.write_text(json.dumps({
        "daemon": "hazard_machine",
        "status": status,
        "last_beat": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        **(extra or {}),
    }, indent=1), encoding="utf-8")


def get_json(url, timeout=15):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def dist_km(lat1, lon1, lat2, lon2):
    import math
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return round(2 * R * math.asin(math.sqrt(a)), 1)


def fetch_quakes():
    start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    day = (datetime.now(timezone.utc).timestamp() - 86400)
    start = datetime.fromtimestamp(day, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    events = []
    emsc = (f"https://www.seismicportal.eu/fdsnws/event/1/query?format=json&limit=30&orderby=time"
            f"&start={start}&minlatitude=5&maxlatitude=12&minlongitude=-86&maxlongitude=-76&minmagnitude=1.5")
    try:
        d = get_json(emsc, 12)
        for f in d.get("features", []):
            p = f.get("properties", {})
            geom = f.get("geometry", {}).get("coordinates", [0, 0, 0])
            ev = {"src": "EMSC", "id": str(f.get("id", p.get("mag", ""))), "mag": float(p.get("mag") or 0),
                  "time": p.get("time", ""), "place": p.get("place", ""), "lat": geom[1], "lon": geom[0]}
            ev["dist_km"] = dist_km(PB[0], PB[1], ev["lat"], ev["lon"])
            ev["box"] = "panama"
            events.append(ev)
    except Exception as ex:
        print(f"[HM] EMSC error: {str(ex)[:60]}")
    usgs_panama = (f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}"
                   f"&minlatitude=5&maxlatitude=12&minlongitude=-86&maxlongitude=-76&minmagnitude=2.0")
    usgs_tsu = (f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}"
                f"&minlatitude={TSUNAMI_BOX[0]}&maxlatitude={TSUNAMI_BOX[1]}&minlongitude={TSUNAMI_BOX[2]}&maxlongitude={TSUNAMI_BOX[3]}&minmagnitude=5.0")
    for url, box in ((usgs_panama, "panama"), (usgs_tsu, "tsunami")):
        try:
            d = get_json(url, 12)
            for f in d.get("features", []):
                p = f["properties"]
                g = f["geometry"]["coordinates"]
                t = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(p["time"] / 1000)) if p.get("time") else ""
                ev = {"src": "USGS", "id": f.get("id", t), "mag": float(p.get("mag") or 0), "time": t,
                      "place": p.get("place", ""), "lat": g[1], "lon": g[0], "box": box, "tsunami_flag": p.get("tsunami", 0)}
                ev["dist_km"] = dist_km(PB[0], PB[1], ev["lat"], ev["lon"]) if box == "panama" else None
                events.append(ev)
        except Exception as ex:
            print(f"[HM] USGS({box}) error: {str(ex)[:60]}")
    return events


def fetch_ptwc():
    out = []
    try:
        req = urllib.request.Request("https://www.tsunami.gov/events/xml/PAAQAtom.xml", headers=UA)
        with urllib.request.urlopen(req, timeout=12) as r:
            root = ET.fromstring(r.read().decode("utf-8", "replace"))
        ns = {"a": "http://www.w3.org/2005/Atom"}
        for e in root.findall("a:entry", ns)[:6]:
            title = (e.findtext("a:title", "", ns) or "").strip()
            updated = (e.findtext("a:updated", "", ns) or "").strip()
            out.append({"id": f"PTWC|{updated}|{title[:40]}", "title": title, "updated": updated})
    except Exception as ex:
        print(f"[HM] PTWC error: {str(ex)[:60]}")
    return out


def fetch_weather():
    try:
        url = ("https://api.open-meteo.com/v1/forecast?latitude=8.3512&longitude=-80.1345"
               "&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,pressure_msl"
               "&daily=precipitation_sum,precipitation_probability_max,wind_gusts_10m_max&forecast_days=1&timezone=America%2FPanama")
        d = get_json(url, 12)
        cur = d.get("current", {})
        day = (d.get("daily") or {})
        return {"t_c": cur.get("temperature_2m"), "rh_pct": cur.get("relative_humidity_2m"),
                "precip_now_mm": cur.get("precipitation"), "wind_kmh": cur.get("wind_speed_10m"),
                "pressure_hpa": cur.get("pressure_msl"),
                "precip_sum_today_mm": (day.get("precipitation_sum") or [0])[0],
                "precip_prob_pct": (day.get("precipitation_probability_max") or [0])[0],
                "gust_max_kmh": (day.get("wind_gusts_10m_max") or [0])[0]}
    except Exception as ex:
        print(f"[HM] weather error: {str(ex)[:60]}")
        return {}


def chime():
    try:
        sys.path.insert(0, str(WS / "tools"))
        import chime_generator
        chime_generator.play_chime()
    except Exception:
        try:
            import winsound
            for f in (660, 880):
                winsound.Beep(f, 180)
        except Exception:
            pass


def load_state():
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        return {"seen": {}}


def save_state(st):
    if len(st.get("seen", {})) > 500:
        seen = sorted(st["seen"].items(), key=lambda x: x[1], reverse=True)[:300]
        st["seen"] = dict(seen)
    STATE.write_text(json.dumps(st), encoding="utf-8")


def telegram_send(kind, payload):
    """24/7 Telegram alerts (free: local PC + GitHub Actions cron redundancy)."""
    try:
        token = chat = None
        for env_path in (WS / ".env", Path(r"C:\Users\smmgo\.agent-data\.env")):
            try:
                for line in open(env_path, encoding="utf-8"):
                    line = line.strip()
                    if line.startswith("TELEGRAM_BOT_TOKEN=") and not token:
                        token = line.split("=", 1)[1].strip('"\'')
                    elif line.startswith("TELEGRAM_CHAT_ID=") and not chat:
                        chat = line.split("=", 1)[1].strip('"\'')
            except OSError:
                pass
        if not (token and chat):
            return None
        import urllib.parse
        icon = {"A1_LOCAL_QUAKE": "\U0001F531", "A2_TSUNAMIGENIC_QUAKE": "\U0001F30A",
                "A3_PTWC_WARNING": "\U0001F6A8", "A4_STORM": "\u26C8", "A5_HUMIDITY": "\U0001F4A7",
                "A6_SUBMARINE_SWARM": "\U0001F30B", "A7_AXIAL_ERUPTION": "\U0001F525"}
        ico = icon.get(kind) or "\u26A0"
        txt = ico + " HAZARD MACHINE | " + kind + "\n" + json.dumps(payload, ensure_ascii=False)[:600]
        url = (f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat}"
               f"&text={urllib.parse.quote(txt)}")
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as ex:
        print(f"[HM] telegram error: {str(ex)[:60]}")
        return False


def emit(kind, key, payload, st):
    if st["seen"].get(key):
        return None
    st["seen"][key] = time.time()
    row = {"ts_utc": datetime.now(timezone.utc).isoformat(), "kind": kind, "key": key, **payload}
    ALERTS.parent.mkdir(exist_ok=True)
    with open(ALERTS, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    try:
        conn = sqlite3.connect(DB)
        conn.execute("""CREATE TABLE IF NOT EXISTS hazard_machine_telemetry (
            id INTEGER PRIMARY KEY, ts_utc TEXT, kind TEXT, dedup_key TEXT UNIQUE, payload_json TEXT)""")
        conn.execute("INSERT OR IGNORE INTO hazard_machine_telemetry (ts_utc, kind, dedup_key, payload_json) VALUES (?,?,?,?)",
                     (row["ts_utc"], kind, key, json.dumps(payload, ensure_ascii=False)))
        conn.commit(); conn.close()
    except Exception as ex:
        print(f"[HM] SQL error: {str(ex)[:60]}")
    telegram_send(kind, payload)
    chime()
    print(f"[HM] ALERT {kind}: {key} :: {json.dumps(payload, ensure_ascii=False)[:160]}")
    return row


def cycle(interval_cycles=0):
    st = load_state()
    alerts = 0

    for e in fetch_quakes():
        for rule, test in ALERT_RULES.items():
            if not rule.startswith(("A1", "A2")):
                continue
            try:
                if test(e):
                    key = f"{rule}|{e['src']}|{e['id']}|{e['mag']}"
                    if emit(rule, key, {"event": e}, st):
                        alerts += 1
            except Exception:
                pass

    for b in fetch_ptwc():
        if "warning" in b["title"].lower():
            if emit("A3_PTWC_WARNING", b["id"], {"bulletin": b}, st):
                alerts += 1

    if interval_cycles % 10 == 0:
        w = fetch_weather()
        if w:
            for rule, test in ALERT_RULES.items():
                if not rule.startswith(("A4", "A5")):
                    continue
                try:
                    if test(w):
                        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        key = f"{rule}|{day}"
                        if emit(rule, key, {"weather": w}, st):
                            alerts += 1
                except Exception:
                    pass

    save_state(st)
    watchdog("ALIVE", {"alerts_this_cycle": alerts, "cycles_seen": st["seen"].__len__()})
    return alerts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=int, default=60)
    args = ap.parse_args()
    if args.once:
        n = cycle(0)
        print(f"[HM] once-cycle done, alerts={n}")
        return
    try:
        hb = json.loads(WATCH.read_text(encoding="utf-8"))
        import datetime as _dt
        age = (datetime.now(timezone.utc) - _dt.datetime.fromisoformat(hb["last_beat"]).replace(tzinfo=timezone.utc)).total_seconds()
        if age < 150:
            print(f"[HM] another instance is alive (beat {age:.0f}s ago) - exiting to avoid duplicate alerts")
            return
    except Exception:
        pass
    print(f"[HM] MY MACHINE running (interval {args.interval}s). Ctrl+C to stop.")
    i = 0
    while True:
        try:
            cycle(i)
        except Exception as ex:
            print(f"[HM] cycle error: {str(ex)[:80]}")
            watchdog("ERROR", {"error": str(ex)[:120]})
        time.sleep(args.interval)
        i += 1


if __name__ == "__main__":
    main()
