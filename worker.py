# worker.py
import os
import datetime as dt

# import the functions & constants from your app
from main import _run_kw_backfill, _run_st_backfill, BACKFILL_WAIT_SECS, DAILY_WAIT_SECS

def _d(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

if __name__ == "__main__":
    mode = os.environ.get("JOB_MODE", "daily")  # "daily" or "backfill"
    if mode == "daily":
        # ingest yesterday (IST/UTC doesn’t matter for date-only; Amazon uses YYYY-MM-DD)
        end = dt.date.today() - dt.timedelta(days=1)
        start = end
        wait = int(os.environ.get("DAILY_WAIT_SECS", DAILY_WAIT_SECS))
        chunk = 1
        print(f"[worker] DAILY: {start} → {end}, wait={wait}s, chunk={chunk}", flush=True)
        _run_kw_backfill(start, end, chunk_days=chunk, wait_seconds=wait)
        _run_st_backfill(start, end, chunk_days=chunk, wait_seconds=wait)
        print("[worker] DAILY ✅ done", flush=True)

    elif mode == "backfill":
        # expects BACKFILL_START, BACKFILL_END, optional CHUNK_DAYS
        start = _d(os.environ["BACKFILL_START"])
        end   = _d(os.environ["BACKFILL_END"])
        chunk = int(os.environ.get("CHUNK_DAYS", "7"))
        wait  = int(os.environ.get("BACKFILL_WAIT_SECS", BACKFILL_WAIT_SECS))
        print(f"[worker] BACKFILL: {start} → {end}, chunk={chunk}, wait={wait}s", flush=True)
        _run_kw_backfill(start, end, chunk_days=chunk, wait_seconds=wait)
        _run_st_backfill(start, end, chunk_days=chunk, wait_seconds=wait)
        print("[worker] BACKFILL ✅ done", flush=True)

    else:
        raise SystemExit(f"Unknown JOB_MODE={mode}")
