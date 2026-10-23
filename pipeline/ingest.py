"""Thu thập dữ liệu từ Open-Meteo (bước 1-3)

Mỗi địa điểm x năm gọi API 1 lần, lưu file json.gz gốc vào data/raw và ghi log vào manifest.jsonl.
Chạy lại thì file nào tải rồi sẽ bỏ qua.

    python pipeline/ingest.py
    python pipeline/ingest.py --core-only
    python pipeline/ingest.py --daily-check   # tải thêm dữ liệu ngày để đối chiếu
"""
import argparse
import gzip
import hashlib
import json
import time
from datetime import datetime, timezone

import requests

if __package__ in (None, ""):
    import sys as _sys
    from pathlib import Path as _P
    _sys.path.insert(0, str(_P(__file__).resolve().parent.parent))

from pipeline import config as C


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest():
    done = {}
    if C.MANIFEST.exists():
        for line in C.MANIFEST.read_text().splitlines():
            if line.strip():
                rec = json.loads(line)
                done[rec["file"]] = rec
    return done


def fetch(params, max_wait_s=3900):
    """Gọi API, bị 429 thì chờ, lỗi mạng thì thử lại"""
    waited, backoff = 0, 5
    while True:
        try:
            r = requests.get(C.API_URL, params=params, timeout=120)
        except requests.RequestException as e:
            print(f"   network error {e!r}, retry in {backoff}s", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
            continue
        if r.status_code == 200:
            # có lúc API trả 200 nhưng body rỗng, gặp thì thử lại
            try:
                body = r.json()
                if "hourly" in body or "daily" in body:
                    return r
                raise ValueError("thiếu trường hourly/daily")
            except ValueError as e:
                print(f"   invalid body ({e}), retry in {backoff}s", flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
        if r.status_code == 429:
            reason = r.json().get("reason", "") if r.headers.get("content-type", "").startswith("application/json") else r.text[:200]
            pause = 600 if "Hourly" in reason else (3600 if "Daily" in reason else 65)
            if waited + pause > max_wait_s * 6:
                raise RuntimeError(f"rate limited too long: {reason}")
            print(f"   429 ({reason}) -> sleep {pause}s", flush=True)
            time.sleep(pause)
            waited += pause
            continue
        if 500 <= r.status_code < 600:
            print(f"   HTTP {r.status_code}, retry in {backoff}s", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
            continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")


def build_tasks(core_only):
    locs = C.locations_as_dicts()
    tasks = []
    # tải phần theo đề trước
    for loc in locs:
        if loc["is_core"]:
            tasks += [(loc, y) for y in range(C.CORE_START, C.CORE_END + 1)]
    if not core_only:
        for loc in locs:
            for y in range(C.START_YEAR, C.END_YEAR + 1):
                if (loc, y) not in tasks:
                    tasks.append((loc, y))
    return tasks


def run_hourly(core_only, pause_s):
    done = load_manifest()
    tasks = build_tasks(core_only)
    print(f"{len(tasks)} tasks, {sum(1 for l, y in tasks if _rel(l, y) in done)} already in manifest", flush=True)
    for i, (loc, year) in enumerate(tasks, 1):
        rel = _rel(loc, year)
        path = C.DATA / rel
        if rel in done and path.exists() and sha256(path) == done[rel]["sha256"]:
            continue
        params = {
            "latitude": loc["latitude"], "longitude": loc["longitude"],
            "start_date": f"{year}-01-01", "end_date": f"{year}-12-31",
            "hourly": ",".join(C.HOURLY_VARS), "timezone": C.TIMEZONE, "models": C.MODEL,
        }
        t0 = time.time()
        r = fetch(params)
        payload = r.json()
        n = len(payload["hourly"]["time"])
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb") as f:
            f.write(r.content)                      # lưu nguyên bytes API trả về
        rec = {
            "file": rel, "kind": "hourly", "location_id": loc["location_id"], "year": year,
            "url": r.url, "params": params, "n_records": n,
            "api_latitude": payload.get("latitude"), "api_longitude": payload.get("longitude"),
            "elevation": payload.get("elevation"), "utc_offset_seconds": payload.get("utc_offset_seconds"),
            "bytes_raw": len(r.content), "bytes_gz": path.stat().st_size, "sha256": sha256(path),
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "elapsed_s": round(time.time() - t0, 2),
        }
        with open(C.MANIFEST, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"[{i}/{len(tasks)}] {loc['location_id']} {year}: {n} rows, {rec['bytes_raw']/1e6:.1f} MB", flush=True)
        time.sleep(pause_s)


def _rel(loc, year):
    return f"raw/{loc['location_id']}/{loc['location_id']}_{year}.json.gz"


def run_daily_check():
    """Tải dữ liệu ngày của Open-Meteo để đối chiếu ở bước kiểm chứng"""
    C.RAW_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    for loc in C.locations_as_dicts():
        if not loc["is_core"]:
            continue
        path = C.RAW_DAILY_DIR / f"{loc['location_id']}_daily.json.gz"
        if path.exists():
            continue
        params = {
            "latitude": loc["latitude"], "longitude": loc["longitude"],
            "start_date": f"{C.CORE_START}-01-01", "end_date": f"{C.CORE_END}-12-31",
            "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": C.TIMEZONE, "models": C.MODEL,
        }
        r = fetch(params)
        with gzip.open(path, "wb") as f:
            f.write(r.content)
        print(f"daily {loc['location_id']}: {len(r.json()['daily']['time'])} days", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--core-only", action="store_true")
    ap.add_argument("--daily-check", action="store_true")
    ap.add_argument("--pause", type=float, default=3.0, help="giây nghỉ giữa các request")
    a = ap.parse_args()
    if a.daily_check:
        run_daily_check()
    else:
        run_hourly(a.core_only, a.pause)
