from datetime import date
from datetime import timedelta
from typing import List
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from fastapi import BackgroundTasks
import uuid
import os
import urllib.parse
import json
import httpx
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
DB_URL = os.environ.get("DATABASE_URL")

# Force SQLAlchemy to use psycopg3 driver if a plain URL was provided
if DB_URL:
    if DB_URL.startswith("postgres://"):
        DB_URL = DB_URL.replace("postgres://", "postgresql+psycopg://", 1)
    elif DB_URL.startswith("postgresql://") and "+psycopg" not in DB_URL:
        DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DB_URL, pool_pre_ping=True) if DB_URL else None

def init_db():
    if not engine:
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS fact_sp_keyword_daily (
      profile_id     text NOT NULL,
      date           date NOT NULL,
      keyword_id     text NOT NULL,

      campaign_id    text NOT NULL,
      campaign_name  text NOT NULL,
      ad_group_id    text NOT NULL,
      ad_group_name  text NOT NULL,
      keyword_text   text NOT NULL,
      match_type     text NOT NULL,

      impressions    integer NOT NULL,
      clicks         integer NOT NULL,
      cost           numeric(18,4) NOT NULL,
      attributed_sales_14d numeric(18,4) NOT NULL,
      attributed_conversions_14d integer NOT NULL,

      cpc            numeric(18,6) NOT NULL,
      ctr            numeric(18,6) NOT NULL,
      acos           numeric(18,6) NOT NULL,
      roas           numeric(18,6) NOT NULL,

      run_id         uuid NOT NULL,
      pulled_at      timestamptz NOT NULL DEFAULT now(),

      CONSTRAINT uq_fact_kw UNIQUE(profile_id, date, keyword_id)
    );
    CREATE INDEX IF NOT EXISTS idx_fact_kw_profile_date ON fact_sp_keyword_daily(profile_id, date);
    CREATE INDEX IF NOT EXISTS idx_fact_kw_keyword_date ON fact_sp_keyword_daily(keyword_id, date);

    CREATE TABLE IF NOT EXISTS fact_sp_search_term_daily (
      profile_id     text NOT NULL,
      date           date NOT NULL,

      campaign_id    text NOT NULL,
      campaign_name  text NOT NULL,
      ad_group_id    text NOT NULL,
      ad_group_name  text NOT NULL,

      search_term    text NOT NULL,
      keyword_id     text,
      keyword_text   text,
      match_type     text NOT NULL,

      impressions    integer NOT NULL,
      clicks         integer NOT NULL,
      cost           numeric(18,4) NOT NULL,
      attributed_sales_14d numeric(18,4) NOT NULL,
      attributed_conversions_14d integer NOT NULL,

      cpc            numeric(18,6) NOT NULL,
      ctr            numeric(18,6) NOT NULL,
      acos           numeric(18,6) NOT NULL,
      roas           numeric(18,6) NOT NULL,

      run_id         uuid NOT NULL,
      pulled_at      timestamptz NOT NULL DEFAULT now(),

      CONSTRAINT uq_fact_st UNIQUE(profile_id, date, ad_group_id, search_term, match_type)
    );
    CREATE INDEX IF NOT EXISTS idx_fact_st_profile_date ON fact_sp_search_term_daily(profile_id, date);
    CREATE INDEX IF NOT EXISTS idx_fact_st_term_date ON fact_sp_search_term_daily(search_term, date);
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)

# ======================================================
# APP SETUP
# ======================================================

app = FastAPI(title="Ads Pull API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

templates = Jinja2Templates(directory="templates")
@app.get("/ui/keywords", response_class=HTMLResponse)
def ui_keywords(request: Request):
    return templates.TemplateResponse("keywords.html", {"request": request})

# --- ultra-safe debug for Search Term table ---

from fastapi.responses import JSONResponse

@app.get("/api/debug/st_counts_safe")
def st_counts_safe():
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")
    pid = _env("AMZN_PROFILE_ID")
    q = text("""
        SELECT date,
               COUNT(*) AS rows,
               COALESCE(SUM(clicks),0) AS clicks,
               COALESCE(SUM(cost),0)   AS cost,
               COALESCE(SUM(attributed_sales_14d),0) AS sales_14d,
               COALESCE(SUM(attributed_conversions_14d),0) AS orders_14d
        FROM fact_sp_search_term_daily
        WHERE profile_id = :pid
        GROUP BY date
        ORDER BY date DESC
        LIMIT 10
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"pid": pid}).mappings().all()
    return [{
        "date": r["date"].isoformat(),
        "rows": int(r["rows"]),
        "clicks": int(r["clicks"]),
        "cost": float(r["cost"]),
        "sales_14d": float(r["sales_14d"]),
        "orders_14d": int(r["orders_14d"]),
    } for r in rows]

@app.get("/api/debug/st_head")
def st_head(limit: int = 20):
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")
    pid = _env("AMZN_PROFILE_ID")
    q = text("""
        SELECT date, campaign_name, ad_group_name, search_term, match_type,
               impressions, clicks, cost, attributed_sales_14d, attributed_conversions_14d
        FROM fact_sp_search_term_daily
        WHERE profile_id = :pid
        ORDER BY date DESC, campaign_name, ad_group_name, search_term
        LIMIT :lim
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"pid": pid, "lim": limit}).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["date"] = r["date"].isoformat()
        d["impressions"] = int(d["impressions"])
        d["clicks"] = int(d["clicks"])
        d["cost"] = float(d["cost"])
        d["attributed_sales_14d"] = float(d["attributed_sales_14d"])
        d["attributed_conversions_14d"] = int(d["attributed_conversions_14d"])
        out.append(d)
    return out

@app.post("/api/debug/migrate_kw_table")
def migrate_kw_table():
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")
    with engine.begin() as conn:
        conn.exec_driver_sql("""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='fact_sp_keywords_daily') AND
             NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='fact_sp_keyword_daily')
          THEN
            EXECUTE 'ALTER TABLE fact_sp_keywords_daily RENAME TO fact_sp_keyword_daily';
          END IF;
        END$$;
        """)
    return {"ok": True}

# ======================================================
# DATA MODELS
# ======================================================

class Metrics(BaseModel):
    impressions: int
    clicks: int
    spend: float
    sales: float
    orders: int
    cpc: float
    ctr: float
    acos: float
    roas: float

class KeywordRow(BaseModel):
    run_id: str
    pulled_at: date
    marketplace: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    entity_type: str
    keyword_id: str
    keyword_text: str
    match_type: str
    bid: float
    lookback_days: int
    buffer_days: int
    metrics: Metrics

# ======================================================
# MOCK DATA FUNCTION (TEMPORARY)
# ======================================================

def _mock_pull_sp_keywords(marketplace: str, lookback_days: int, buffer_days: int, limit: int = 200) -> List[KeywordRow]:
    run_id = str(uuid.uuid4())
    pulled_at = date.today()
    data: List[KeywordRow] = []
    for i in range(limit):
        clicks = (i % 50) + 1
        impressions = clicks * 40
        cpc = round(2 + (i % 7) * 0.5, 2)
        spend = round(clicks * cpc, 2)
        orders = (i % 6)
        sales = round(orders * 200.0, 2)
        ctr = round(clicks / max(1, impressions), 4)
        acos = round(spend / max(0.01, sales), 4) if sales > 0 else 0.0
        roas = round(sales / max(0.01, spend), 4) if spend > 0 else 0.0
        data.append(KeywordRow(
            run_id=run_id,
            pulled_at=pulled_at,
            marketplace=marketplace,
            campaign_id=f"cmp_{i//20}",
            campaign_name=f"Campaign {(i//20)+1}",
            ad_group_id=f"ag_{i//10}",
            ad_group_name=f"AdGroup {(i//10)+1}",
            entity_type="keyword",
            keyword_id=f"kw_{i}",
            keyword_text=f"screen cleaner {(i%10)+1}",
            match_type=["exact", "phrase", "broad"][i % 3],
            bid=round(3.0 + (i % 5) * 0.25, 2),
            lookback_days=lookback_days,
            buffer_days=buffer_days,
            metrics=Metrics(
                impressions=impressions,
                clicks=clicks,
                spend=spend,
                sales=sales,
                orders=orders,
                cpc=cpc,
                ctr=ctr,
                acos=acos,
                roas=roas,
            )
        ))
    return data

# ======================================================
# BASIC ROUTES
# ======================================================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/sp/keywords", response_model=List[KeywordRow])
def get_sp_keywords(
    marketplace: str = "IN",
    lookback_days: int = 14,
    buffer_days: int = 1,
    limit: int = 200,
):
    return _mock_pull_sp_keywords(marketplace, lookback_days, buffer_days, limit)

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
@app.get("/ads/keywords", response_class=HTMLResponse)
def ads_keywords(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/ads/search-terms", response_class=HTMLResponse)
def ads_search_terms(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# ======================================================
# AMAZON ADS API HELPERS
# ======================================================

def _ads_base(region: str) -> str:
    region = (region or "NA").upper()
    if region == "EU":
        return "https://advertising-api-eu.amazon.com"
    if region == "FE":
        return "https://advertising-api-fe.amazon.com"
    return "https://advertising-api.amazon.com"  # NA default

def _env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise HTTPException(status_code=500, detail=f"Missing env var: {name}")
    return v

# ======================================================
# AMAZON OAUTH ROUTES
# ======================================================

@app.get("/api/amzn/oauth/start")
def amzn_oauth_start():
    client_id = _env("AMZN_CLIENT_ID")
    redirect_uri = "https://amazons-ads-backend.onrender.com/api/amzn/oauth/callback"
    scope = "advertising::campaign_management"

    params = {
        "client_id": client_id,
        "scope": scope,
        "response_type": "code",
        "redirect_uri": redirect_uri,
    }
    url = "https://www.amazon.com/ap/oa?" + urllib.parse.urlencode(params)
    return HTMLResponse(f'<a href="{url}">Continue to Amazon consent</a><script>location.href="{url}"</script>')

@app.get("/api/amzn/oauth/callback")
def amzn_oauth_callback(code: str = "", error: str = ""):
    if error:
        raise HTTPException(status_code=400, detail=f"OAuth error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="Missing ?code in callback")

    client_id = _env("AMZN_CLIENT_ID")
    client_secret = _env("AMZN_CLIENT_SECRET")
    redirect_uri = "https://amazons-ads-backend.onrender.com/api/amzn/oauth/callback"

    token_url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }
    with httpx.Client(timeout=60) as client:
        r = client.post(token_url, data=data)
        r.raise_for_status()
        tok = r.json()

    return HTMLResponse(
        "<h2>Copy your refresh token below (keep it secret):</h2>"
        f"<pre style='white-space:pre-wrap'>{json.dumps(tok, indent=2)}</pre>"
        "<p>Set <b>AMZN_REFRESH_TOKEN</b> to the <code>refresh_token</code> value in Render → Environment.</p>"
    )

@app.get("/api/amzn/profiles")
def amzn_profiles():
    client_id = _env("AMZN_CLIENT_ID")
    client_secret = _env("AMZN_CLIENT_SECRET")
    refresh_token = _env("AMZN_REFRESH_TOKEN")
    region = os.environ.get("AMZN_REGION", "NA").upper()

    token_url = "https://api.amazon.com/auth/o2/token"
    tok_data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    with httpx.Client(timeout=60) as client:
        tr = client.post(token_url, data=tok_data)
        tr.raise_for_status()
        access_token = tr.json()["access_token"]

        ads_base = _ads_base(region)
        pr = client.get(
            f"{ads_base}/v2/profiles",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Amazon-Advertising-API-ClientId": client_id,
                "Content-Type": "application/json",
            },
        )
        pr.raise_for_status()
        return pr.json()

import io, gzip, datetime
from fastapi.responses import JSONResponse

def _get_access_token_from_refresh() -> str:
    client_id = _env("AMZN_CLIENT_ID")
    client_secret = _env("AMZN_CLIENT_SECRET")
    refresh_token = _env("AMZN_REFRESH_TOKEN")
    token_url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    with httpx.Client(timeout=60) as client:
        r = client.post(token_url, data=data)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=502, detail={"stage":"refresh_token_exchange","status":e.response.status_code,"body":e.response.text})
        return r.json()["access_token"]

def _ads_headers(access_token: str) -> dict:
    client_id = _env("AMZN_CLIENT_ID")
    profile_id = _env("AMZN_PROFILE_ID")
    return {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope": profile_id,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _ymd(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

@app.get("/api/sp/keywords_live", response_model=List[KeywordRow])
def sp_keywords_live(lookback_days: int = 14, buffer_days: int = 1, limit: int = 1000):
    """
    Pull real Sponsored Products Keyword performance via Reports v3 and map to our table shape.
    """
    import datetime, io, gzip, time, re

    # 1) dates with attribution buffer
    end_date = datetime.date.today() - datetime.timedelta(days=max(0, buffer_days))
    start_date = end_date - datetime.timedelta(days=max(1, lookback_days) - 1)

    # 2) tokens/headers/region
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)

    # helper for dates
    def _ymd(d: datetime.date) -> str:
        return d.strftime("%Y-%m-%d")

    # 3) create report job (Reports v3)
    create_body = {
        "name": f"spKeywords_{_ymd(start_date)}_{_ymd(end_date)}",
        "startDate": _ymd(start_date),
        "endDate": _ymd(end_date),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spKeywords",
            "timeUnit": "DAILY",
            "groupBy": ["adGroup"],  # only adGroup is allowed for this report
            "columns": [
                "campaignId","campaignName",
                "adGroupId","adGroupName",
                "keywordId","keywordText","matchType",
                "impressions","clicks","cost",
                "attributedSales14d","attributedConversions14d"
            ],
            "format": "GZIP_JSON"
        }
    }

    with httpx.Client(timeout=60) as client:
        cr = client.post(f"{ads_base}/reporting/reports", headers=headers, json=create_body)

        # normal: created
        if 200 <= cr.status_code < 300:
            report_id = cr.json().get("reportId")

        # duplicate request: reuse existing report id from error detail (HTTP 425)
        elif cr.status_code == 425:
            try:
                err = cr.json()
                m = re.search(r"([0-9a-fA-F-]{36})", err.get("detail", ""))
                report_id = m.group(1) if m else None
            except Exception:
                report_id = None

        else:
            return JSONResponse(
                status_code=502,
                content={
                    "stage": "create_report",
                    "status": cr.status_code,
                    "body": cr.text,
                    "endpoint": f"{ads_base}/reporting/reports",
                    "payload": create_body,
                },
            )

    if not report_id:
        raise HTTPException(
            status_code=502,
            detail={"stage": "create_report", "error": "No reportId in response", "body": cr.text},
        )

    # 4) poll until status=SUCCESS (configurable wait; default 5 min)
    status_url = f"{ads_base}/reporting/reports/{report_id}"
    wait_seconds = int(os.environ.get("AMZN_REPORT_WAIT_SECONDS", "300"))  # 300s = 5 min
    deadline = time.time() + wait_seconds

    download_url = None
    with httpx.Client(timeout=60) as client:
        while time.time() < deadline:
            sr = client.get(status_url, headers=headers)
            if sr.status_code >= 400:
                return JSONResponse(
                    status_code=502,
                    content={"stage": "check_report", "status": sr.status_code, "body": sr.text, "url": status_url},
                )
            s = sr.json()
            if s.get("status") == "SUCCESS" and s.get("url"):
                download_url = s["url"]
                break
            if s.get("status") in {"FAILURE", "CANCELLED"}:
                return JSONResponse(status_code=502, content={"stage": "check_report", "status": "FAILED", "body": s})
            time.sleep(3)

    if not download_url:
        return JSONResponse(status_code=504, content={"stage": "check_report", "status": "TIMEOUT", "url": status_url})

    # 5) download and parse GZIP JSON lines
    with httpx.Client(timeout=120) as client:
        dr = client.get(download_url, headers=headers)
        if dr.status_code >= 400:
            return JSONResponse(status_code=502, content={"stage": "download", "status": dr.status_code, "body": dr.text})
        buf = io.BytesIO(dr.content)
        with gzip.GzipFile(fileobj=buf) as gz:
            raw = gz.read().decode("utf-8")

    # Each line is a JSON object (NDJSON)
    rows_out: List[KeywordRow] = []
    run_id = str(uuid.uuid4())
    pulled_at = datetime.date.today()
    count = 0
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except Exception:
            continue

        campaign_id = str(rec.get("campaignId", ""))
        campaign_name = rec.get("campaignName", "")
        ad_group_id = str(rec.get("adGroupId", ""))
        ad_group_name = rec.get("adGroupName", "")
        keyword_id = str(rec.get("keywordId", ""))
        keyword_text = rec.get("keywordText", "")
        match_type = rec.get("matchType", "")

        impressions = int(rec.get("impressions", 0) or 0)
        clicks = int(rec.get("clicks", 0) or 0)
        cost = float(rec.get("cost", 0.0) or 0.0)
        sales = float(rec.get("attributedSales14d", 0.0) or 0.0)
        orders = int(rec.get("attributedConversions14d", 0) or 0)

        cpc = round(cost / clicks, 4) if clicks else 0.0
        ctr = round(clicks / impressions, 4) if impressions else 0.0
        acos = round(cost / sales, 4) if sales else 0.0
        roas = round(sales / cost, 4) if cost else 0.0

        rows_out.append(KeywordRow(
            run_id=run_id,
            pulled_at=pulled_at,
            marketplace="",  # can enrich later from profile
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            ad_group_id=ad_group_id,
            ad_group_name=ad_group_name,
            entity_type="keyword",
            keyword_id=keyword_id,
            keyword_text=keyword_text,
            match_type=match_type,
            bid=0.0,  # not in this report; optional enrichment later
            lookback_days=lookback_days,
            buffer_days=buffer_days,
            metrics=Metrics(
                impressions=impressions,
                clicks=clicks,
                spend=round(cost, 4),
                sales=round(sales, 4),
                orders=orders,
                cpc=cpc,
                ctr=ctr,
                acos=acos,
                roas=roas,
            ),
        ))
        count += 1
        if count >= limit:
            break

    return rows_out

from datetime import timedelta

@app.post("/api/sp/keywords_start")
def sp_keywords_start(lookback_days: int = 2):
    """
    Create a Sponsored Products Keywords DAILY report for the last `lookback_days`
    (ending yesterday). Returns a report_id immediately.
    """
    import re

    # 1) date range (ending yesterday, no buffer)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(1, lookback_days) - 1)

    # 2) auth/region
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)

    def _ymd(d: date) -> str:
        return d.strftime("%Y-%m-%d")

    # 3) report payload (includes "date" column)
    create_body = {
        "name": f"spKeywords_{_ymd(start_date)}_{_ymd(end_date)}",
        "startDate": _ymd(start_date),
        "endDate": _ymd(end_date),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spKeywords",
            "timeUnit": "DAILY",
            "groupBy": ["adGroup"],  # only adGroup allowed for this report
            "columns": [
                "date",
                "campaignId","campaignName",
                "adGroupId","adGroupName",
                "keywordId","keywordText","matchType",
                "impressions","clicks","cost",
                "attributedSales14d","attributedConversions14d"
            ],
            "format": "GZIP_JSON"
        }
    }

    # 4) call create; handle duplicate (HTTP 425)
    with httpx.Client(timeout=60) as client:
        cr = client.post(f"{ads_base}/reporting/reports", headers=headers, json=create_body)

    if 200 <= cr.status_code < 300:
        return {"report_id": cr.json().get("reportId")}

    if cr.status_code == 425:
        # detail looks like: "The Request is a duplicate of : <uuid>"
        try:
            rid = re.search(r"([0-9a-fA-F-]{36})", cr.json().get("detail","")).group(1)
            return {"report_id": rid, "duplicate": True}
        except Exception:
            pass

    # otherwise bubble up the error
    raise HTTPException(status_code=cr.status_code, detail=cr.text)

@app.get("/api/sp/report_status")
def sp_report_status(report_id: str):
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)
    url = f"{ads_base}/reporting/reports/{report_id}"

    with httpx.Client(timeout=60) as client:
        r = client.get(url, headers=headers)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
        return r.json()

# ================================
# SP SEARCH TERMS (Reports v3)
# ================================
from fastapi import Query
import io, gzip, json as _json, uuid as _uuid, urllib.request

@app.post("/api/sp/keywords_run")
def sp_keywords_run(lookback_days: int = 2, background_tasks: BackgroundTasks = None):
    """
    Single call:
    - create report for the last `lookback_days` (ending yesterday)
    - start a background task to poll & fetch & store
    - return immediately with report_id
    """
    # 1) compute dates (no buffer)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(1, lookback_days) - 1)

    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)

    def _ymd(d: date) -> str:
        return d.strftime("%Y-%m-%d")

    body = {
        "name": f"spKeywords_{_ymd(start_date)}_{_ymd(end_date)}",
        "startDate": _ymd(start_date),
        "endDate": _ymd(end_date),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spKeywords",
            "timeUnit": "DAILY",
            "groupBy": ["adGroup"],
            "columns": [
                "date",
                "campaignId","campaignName",
                "adGroupId","adGroupName",
                "keywordId","keywordText","matchType",
                "impressions","clicks","cost",
                "attributedSales14d","attributedConversions14d"
            ],
            "format": "GZIP_JSON"
        }
    }

    with httpx.Client(timeout=60) as client:
        cr = client.post(f"{ads_base}/reporting/reports", headers=headers, json=body)

    # handle create + duplicate(425)
    if 200 <= cr.status_code < 300:
        rid = cr.json().get("reportId")
    elif cr.status_code == 425:
        import re
        try:
            rid = re.search(r"([0-9a-fA-F-]{36})", cr.json().get("detail","")).group(1)
        except Exception:
            raise HTTPException(status_code=425, detail=cr.text)
    else:
        raise HTTPException(status_code=cr.status_code, detail=cr.text)

    # kick background processor
    if background_tasks is not None:
        background_tasks.add_task(_process_report_in_bg, rid)

    return {"report_id": rid, "status": "PROCESSING", "start": str(start_date), "end": str(end_date)}

from datetime import date as _date
from fastapi import Query

@app.get("/api/sp/keywords_range", response_model=List[KeywordRow])
def sp_keywords_range(
    start: str,
    end: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    Returns stored keyword-day rows between [start, end] (inclusive).
    Dates must be YYYY-MM-DD.
    Supports pagination via limit & offset.
    """
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")
    profile_id = _env("AMZN_PROFILE_ID")

    try:
        start_d = _date.fromisoformat(start)
        end_d = _date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

    q = """
    SELECT
      profile_id, date, keyword_id,
      campaign_id, campaign_name, ad_group_id, ad_group_name,
      keyword_text, match_type,
      impressions, clicks, cost, attributed_sales_14d, attributed_conversions_14d,
      cpc, ctr, acos, roas,
      run_id, pulled_at
    FROM fact_sp_keyword_daily
    WHERE profile_id = :pid
      AND date >= :start_d
      AND date <= :end_d
    ORDER BY date DESC, campaign_name, ad_group_name, keyword_text
    LIMIT :lim OFFSET :off
    """
    with engine.begin() as conn:
        rows = conn.execute(
            text(q),
            {"pid": profile_id, "start_d": start_d, "end_d": end_d, "lim": limit, "off": offset},
        ).mappings().all()

    out: List[KeywordRow] = []
    for r in rows:
        pa = r["pulled_at"]
        try:
            pulled_at_date = pa.date()
        except AttributeError:
            pulled_at_date = pa

        out.append(KeywordRow(
            run_id=str(r["run_id"]),
            pulled_at=pulled_at_date,
            marketplace="",
            campaign_id=str(r["campaign_id"]),
            campaign_name=r["campaign_name"],
            ad_group_id=str(r["ad_group_id"]),
            ad_group_name=r["ad_group_name"],
            entity_type="keyword",
            keyword_id=str(r["keyword_id"]),
            keyword_text=r["keyword_text"],
            match_type=r["match_type"],
            bid=0.0,
            lookback_days=0,
            buffer_days=0,
            metrics=Metrics(
                impressions=int(r["impressions"]),
                clicks=int(r["clicks"]),
                spend=float(r["cost"]),
                sales=float(r["attributed_sales_14d"]),
                orders=int(r["attributed_conversions_14d"]),
                cpc=float(r["cpc"]),
                ctr=float(r["ctr"]),
                acos=float(r["acos"]),
                roas=float(r["roas"]),
            )
        ))
    return out

@app.get("/api/debug/sp_counts")
def sp_counts():
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")
    pid = _env("AMZN_PROFILE_ID")
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT date, COUNT(*) AS rows, SUM(clicks) AS clicks, SUM(cost) AS cost
            FROM fact_sp_keyword_daily
            WHERE profile_id = :pid
            GROUP BY date
            ORDER BY date DESC
            LIMIT 10
        """), {"pid": pid}).mappings().all()
    out = []
    for r in rows:
        out.append({
            "date": r["date"].isoformat(),
            "rows": int(r["rows"]),
            "clicks": int(r["clicks"]) if r["clicks"] else 0,
            "cost": float(r["cost"]) if r["cost"] else 0.0,
        })
    return out

@app.post("/api/debug/create_st_table")
def create_st_table():
    """
    Create the Sponsored Products Search Term daily fact table if it doesn't exist.
    """
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")

    ddl = """
    CREATE TABLE IF NOT EXISTS fact_sp_search_term_daily (
      profile_id     text NOT NULL,
      date           date NOT NULL,
      -- grain: a search term performance row is scoped by campaign/ad_group/day
      search_term    text NOT NULL,
      keyword_id     text,
      keyword_text   text,
      match_type     text NOT NULL,

      campaign_id    text NOT NULL,
      campaign_name  text NOT NULL,
      ad_group_id    text NOT NULL,
      ad_group_name  text NOT NULL,

      impressions    integer NOT NULL,
      clicks         integer NOT NULL,
      cost           numeric(18,4) NOT NULL,
      attributed_sales_14d        numeric(18,4) NOT NULL,
      attributed_conversions_14d  integer NOT NULL,

      -- derived
      cpc            numeric(18,6) NOT NULL,
      ctr            numeric(18,6) NOT NULL,
      acos           numeric(18,6) NOT NULL,
      roas           numeric(18,6) NOT NULL,

      run_id         uuid NOT NULL,
      pulled_at      timestamptz NOT NULL DEFAULT now(),

      CONSTRAINT uq_st_fact UNIQUE (profile_id, date, campaign_id, ad_group_id, search_term, match_type)
    );
    CREATE INDEX IF NOT EXISTS idx_st_profile_date ON fact_sp_search_term_daily(profile_id, date);
    CREATE INDEX IF NOT EXISTS idx_st_term_date ON fact_sp_search_term_daily(search_term, date);
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)

    return {"ok": True, "table": "fact_sp_search_term_daily"}

# --- DEBUG: list tables
@app.get("/api/debug/tables")
def list_tables():
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT schemaname, tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname NOT IN ('pg_catalog','information_schema')
            ORDER BY schemaname, tablename
        """)).mappings().all()
    return [{"schema": r["schemaname"], "table": r["tablename"]} for r in rows]

# --- DEBUG: safer st_counts (returns error details instead of 500)

from fastapi.responses import JSONResponse

@app.get("/api/debug/report_head")
def debug_report_head(report_id: str):
    """Quick test: fetch first few rows from Amazon report without DB insert."""
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)

    # 1) Get report status & presigned URL
    status_url = f"{ads_base}/reporting/reports/{report_id}"
    with httpx.Client(timeout=60) as client:
        r = client.get(status_url, headers=headers)
        r.raise_for_status()
        meta = r.json()
        url = meta.get("url")
        if not url:
            return {"stage": "check_report", "meta": meta}

    # 2) Download with no headers
    import urllib.request, gzip, io, json
    with urllib.request.urlopen(url, timeout=60) as resp:
        raw_bytes = resp.read()

    # 3) Gunzip if needed
    try:
        buf = io.BytesIO(raw_bytes)
        with gzip.GzipFile(fileobj=buf) as gz:
            raw_text = gz.read().decode("utf-8")
    except OSError:
        raw_text = raw_bytes.decode("utf-8", errors="ignore")

    # 4) Return just first 5 lines/objects
    sample = []
    for line in raw_text.splitlines():
        if not line.strip():
            continue
        try:
            sample.append(json.loads(line))
        except Exception:
            pass
        if len(sample) >= 5:
            break

    return {"stage": "ok", "sample": sample}

# PERMANENT INGEST: fetch & upsert (headerless S3 download, robust row mapping)
from fastapi import Query
import io, gzip, json as _json, uuid as _uuid, urllib.request

@app.post("/api/sp/keywords_fetch")
def sp_keywords_fetch(
    report_id: str = Query(..., description="Amazon Reports v3 reportId"),
    limit: int = Query(5000, ge=1, le=200000)
):
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")

    # 1) get report meta from Ads API (needs auth)
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)

    status_url = f"{ads_base}/reporting/reports/{report_id}"
    with httpx.Client(timeout=120) as client:
        sr = client.get(status_url, headers=headers)
        try:
            sr.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=502,
                detail={"stage": "check_report", "status": e.response.status_code, "body": e.response.text},
            )
        meta = sr.json()
        st = meta.get("status")
        presigned_url = meta.get("url")
        if st not in ("SUCCESS", "COMPLETED") or not presigned_url:
            return JSONResponse(status_code=409, content={"stage": "check_report", "status": st, "meta": meta})

    # 2) download presigned S3 URL with ZERO headers
    try:
        with urllib.request.urlopen(presigned_url, timeout=120) as resp:
            raw_bytes = resp.read()
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail={"stage": "download", "status": 400, "body": f"urllib error: {e!r}"},
        )

        # 3) gunzip (or fallback to plain)
    try:
        buf = io.BytesIO(raw_bytes)
        with gzip.GzipFile(fileobj=buf) as gz:
            raw_text = gz.read().decode("utf-8")
    except OSError:
        raw_text = raw_bytes.decode("utf-8", errors="ignore")

    # --- DEBUG: log a small prefix so we know the shape ---
    try:
        print("[fetch_debug] size_bytes=", len(raw_text), "prefix=", raw_text[:600].replace("\n","\\n")[:600])
    except Exception:
        pass

    # 4) iterate records from multiple possible shapes
    import json as _json

    def extract_records(obj):
        """Yield dict records from a loaded JSON object that might be:
           - a list of dicts
           - a dict with a key that holds a list of dicts
           - a single dict record
        """
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    yield item
        elif isinstance(obj, dict):
            # common wrappers
            for k in ("records", "rows", "data", "report", "result", "items"):
                v = obj.get(k)
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            yield item
                    return
                # sometimes nested like {"report": {"records":[...]}}
                if isinstance(v, dict):
                    for kk in ("records", "rows", "data", "items"):
                        vv = v.get(kk)
                        if isinstance(vv, list):
                            for item in vv:
                                if isinstance(item, dict):
                                    yield item
                            return
            # if it's just a single record dict
            yield obj

    def iter_records(text: str):
        # Try NDJSON first (fast path)
        out = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = _json.loads(line)
            except Exception:
                # if any line is not JSON, bail from NDJSON path
                out = []
                break
            # Expand possible wrappers per line
            out.extend(list(extract_records(obj)))
        if out:
            return out

        # Fallback: whole-text JSON
        try:
            obj = _json.loads(text)
            return list(extract_records(obj))
        except Exception:
            return []

    records = iter_records(raw_text)
    if not records:
        raise HTTPException(
            status_code=502,
            detail={"stage": "parse", "body_prefix": raw_text[:600]}
        )

    # 5) upsert
    pid = _env("AMZN_PROFILE_ID")
    run_id = str(_uuid.uuid4())
    inserted = updated = processed = 0

    upsert_sql = text("""
        INSERT INTO fact_sp_keyword_daily (
            profile_id, date, keyword_id,
            campaign_id, campaign_name, ad_group_id, ad_group_name,
            keyword_text, match_type,
            impressions, clicks, cost, attributed_sales_14d, attributed_conversions_14d,
            cpc, ctr, acos, roas,
            run_id, pulled_at
        )
        VALUES (
            :profile_id, :date, :keyword_id,
            :campaign_id, :campaign_name, :ad_group_id, :ad_group_name,
            :keyword_text, :match_type,
            :impressions, :clicks, :cost, :sales, :orders,
            :cpc, :ctr, :acos, :roas,
            :run_id, now()
        )
        ON CONFLICT (profile_id, date, keyword_id) DO UPDATE SET
            campaign_id = EXCLUDED.campaign_id,
            campaign_name = EXCLUDED.campaign_name,
            ad_group_id = EXCLUDED.ad_group_id,
            ad_group_name = EXCLUDED.ad_group_name,
            keyword_text = EXCLUDED.keyword_text,
            match_type = EXCLUDED.match_type,
            impressions = EXCLUDED.impressions,
            clicks = EXCLUDED.clicks,
            cost = EXCLUDED.cost,
            attributed_sales_14d = EXCLUDED.attributed_sales_14d,
            attributed_conversions_14d = EXCLUDED.attributed_conversions_14d,
            cpc = EXCLUDED.cpc,
            ctr = EXCLUDED.ctr,
            acos = EXCLUDED.acos,
            roas = EXCLUDED.roas,
            run_id = EXCLUDED.run_id,
            pulled_at = now()
        RETURNING xmax = 0 AS inserted_flag
    """)

    with engine.begin() as conn:
        for rec in records:
            # map fields
            date_str = (rec.get("date") or rec.get("reportDate") or "")[:10]
            if not date_str:
                continue

            d = {
                "profile_id": pid,
                "date": date_str,
                "campaign_id": str(rec.get("campaignId") or ""),
                "campaign_name": rec.get("campaignName") or "",
                "ad_group_id": str(rec.get("adGroupId") or ""),
                "ad_group_name": rec.get("adGroupName") or "",
                "keyword_id": str(rec.get("keywordId") or "0"),
                "keyword_text": rec.get("keywordText") or "",
                "match_type": rec.get("matchType") or "",
                "impressions": int(rec.get("impressions") or 0),
                "clicks": int(rec.get("clicks") or 0),
                "cost": float(rec.get("cost") or 0.0),
                "sales": float(rec.get("attributedSales14d") or 0.0),
                "orders": int(rec.get("attributedConversions14d") or 0),
            }
            d["cpc"]  = round(d["cost"] / d["clicks"], 6) if d["clicks"] else 0.0
            d["ctr"]  = round(d["clicks"] / d["impressions"], 6) if d["impressions"] else 0.0
            d["acos"] = round(d["cost"] / d["sales"], 6) if d["sales"] else 0.0
            d["roas"] = round(d["sales"] / d["cost"], 6) if d["cost"] else 0.0
            d["run_id"] = run_id

            res = conn.execute(upsert_sql, d).first()
            if res and res[0] is True:
                inserted += 1
            else:
                updated += 1

            processed += 1
            if processed >= limit:
                break

    return {"report_id": report_id, "processed": processed, "inserted": inserted, "updated": updated}

# ===============================
# SP Search Terms: create & run
# ===============================

@app.post("/api/sp/st_start")
def sp_search_terms_start(lookback_days: int = 2):
    """
    Create a Sponsored Products Search Terms DAILY report (ending yesterday).
    Returns a report_id immediately (or an existing one if it's a duplicate request).
    """
    import re
    from datetime import date, timedelta
    import httpx

    # 1) date range (ending yesterday)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(1, lookback_days) - 1)

    # 2) auth/region/headers
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)

    def _ymd(d: date) -> str:
        return d.strftime("%Y-%m-%d")

    # 3) correct configuration for SP Search Terms
    create_body = {
        "name": f"spSearchTerm_{_ymd(start_date)}_{_ymd(end_date)}",
        "startDate": _ymd(start_date),
        "endDate": _ymd(end_date),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spSearchTerm",
            "timeUnit": "DAILY",
            "groupBy": ["searchTerm"],
            "columns": [
                "date",
                "campaignId", "campaignName",
                "adGroupId", "adGroupName",
                "searchTerm", "matchType",
                "impressions", "clicks", "cost",
                "attributedsales14d", "attributedconversions14d"
            ],
            "format": "GZIP_JSON"
        }
    }

    # 4) create report; handle duplicate (HTTP 425) gracefully
    with httpx.Client(timeout=60) as client:
        cr = client.post(f"{ads_base}/reporting/reports", headers=headers, json=create_body)

    if 200 <= cr.status_code < 300:
        return {"report_id": cr.json().get("reportId")}

    if cr.status_code == 425:
        try:
            rid = re.search(r"([0-9a-fA-F-]{36})", cr.json().get("detail", "")).group(1)
            return {"report_id": rid, "duplicate": True}
        except Exception:
            pass

    raise HTTPException(status_code=cr.status_code, detail=cr.text)

@app.post("/api/sp/st_run")
def sp_search_terms_run(lookback_days: int = 2, background_tasks: BackgroundTasks = None):
    """
    One-click: create report and process in background until stored.
    """
    r = sp_search_terms_start(lookback_days=lookback_days)
    rid = r["report_id"]
    if background_tasks is not None:
        background_tasks.add_task(_process_st_report_in_bg, rid)
    return {"report_id": rid, "status": "PROCESSING"}

def _process_st_report_in_bg(report_id: str):
    import io, gzip, time, datetime as dt, json as _json

    try:
        access = _get_access_token_from_refresh()
        headers = _ads_headers(access)
        region = os.environ.get("AMZN_REGION", "NA").upper()
        ads_base = _ads_base(region)

        status_url = f"{ads_base}/reporting/reports/{report_id}"
        deadline = time.time() + int(os.environ.get("AMZN_REPORT_BG_MAX_SECONDS", "900"))

        download_url = None
        with httpx.Client(timeout=60) as client:
            while time.time() < deadline:
                sr = client.get(status_url, headers=headers)
                if sr.status_code >= 400:
                    print("[st_status_error]", sr.status_code, sr.text); return
                meta = sr.json()
                st = meta.get("status")
                if st in ("SUCCESS", "COMPLETED") and meta.get("url"):
                    download_url = meta["url"]; break
                if st in {"FAILURE", "CANCELLED"}:
                    print("[st_failed]", meta); return
                time.sleep(20)

            if not download_url:
                print("[st_timeout]", status_url); return

            # download with ZERO headers (presigned S3)
            dr = httpx.get(download_url, headers={}, timeout=120)
            if dr.status_code >= 400:
                print("[st_download_error]", dr.status_code, dr.text); return

            # gunzip
            try:
                buf = io.BytesIO(dr.content)
                with gzip.GzipFile(fileobj=buf) as gz:
                    raw_text = gz.read().decode("utf-8")
            except OSError:
                raw_text = dr.content.decode("utf-8", errors="ignore")

        # parse lines
        pid = _env("AMZN_PROFILE_ID")
        run_id = str(uuid.uuid4())
        rows = []

        for line in raw_text.splitlines():
            line = line.strip()
            if not line:
                continue
            rec = _json.loads(line)
            ds = (rec.get("date") or rec.get("reportDate") or "")[:10]
            if not ds:
                continue

            campaign_id = str(rec.get("campaignId","") or "")
            campaign_name = rec.get("campaignName","") or ""
            ad_group_id = str(rec.get("adGroupId","") or "")
            ad_group_name = rec.get("adGroupName","") or ""
            search_term = rec.get("searchTerm","") or ""
            keyword_id = str(rec.get("keywordId","") or "")
            keyword_text = rec.get("keywordText","") or ""
            match_type = rec.get("matchType","") or ""

            impressions = int(rec.get("impressions",0) or 0)
            clicks = int(rec.get("clicks",0) or 0)
            cost = float(rec.get("cost",0.0) or 0.0)
            sales = float(rec.get("attributedSales14d",0.0) or 0.0)
            orders = int(rec.get("attributedConversions14d",0) or 0)

            cpc  = round(cost / clicks, 6) if clicks else 0.0
            ctr  = round(clicks / impressions, 6) if impressions else 0.0
            acos = round(cost / sales, 6) if sales else 0.0
            roas = round(sales / cost, 6) if cost else 0.0

            rows.append({
                "profile_id": pid, "date": ds,
                "campaign_id": campaign_id, "campaign_name": campaign_name,
                "ad_group_id": ad_group_id, "ad_group_name": ad_group_name,
                "search_term": search_term,
                "keyword_id": keyword_id or None,
                "keyword_text": keyword_text or None,
                "match_type": match_type,
                "impressions": impressions, "clicks": clicks, "cost": cost,
                "attributed_sales_14d": sales, "attributed_conversions_14d": orders,
                "cpc": cpc, "ctr": ctr, "acos": acos, "roas": roas,
                "run_id": run_id,
            })

        if not rows or not engine:
            print("[st_no_rows_or_db]", len(rows)); return

        upsert_sql = text("""
            INSERT INTO fact_sp_search_term_daily (
                profile_id, date,
                campaign_id, campaign_name, ad_group_id, ad_group_name,
                search_term, keyword_id, keyword_text, match_type,
                impressions, clicks, cost, attributed_sales_14d, attributed_conversions_14d,
                cpc, ctr, acos, roas, run_id
            )
            VALUES (
                :profile_id, :date,
                :campaign_id, :campaign_name, :ad_group_id, :ad_group_name,
                :search_term, :keyword_id, :keyword_text, :match_type,
                :impressions, :clicks, :cost, :attributed_sales_14d, :attributed_conversions_14d,
                :cpc, :ctr, :acos, :roas, :run_id
            )
            ON CONFLICT (profile_id, date, ad_group_id, search_term, match_type) DO UPDATE SET
                campaign_id = EXCLUDED.campaign_id,
                campaign_name = EXCLUDED.campaign_name,
                ad_group_id = EXCLUDED.ad_group_id,
                ad_group_name = EXCLUDED.ad_group_name,
                keyword_id = EXCLUDED.keyword_id,
                keyword_text = EXCLUDED.keyword_text,
                match_type = EXCLUDED.match_type,
                impressions = EXCLUDED.impressions,
                clicks = EXCLUDED.clicks,
                cost = EXCLUDED.cost,
                attributed_sales_14d = EXCLUDED.attributed_sales_14d,
                attributed_conversions_14d = EXCLUDED.attributed_conversions_14d,
                cpc = EXCLUDED.cpc,
                ctr = EXCLUDED.ctr,
                acos = EXCLUDED.acos,
                roas = EXCLUDED.roas,
                run_id = EXCLUDED.run_id,
                pulled_at = now()
        """)

        with engine.begin() as conn:
            conn.execute(upsert_sql, rows)

        print(f"[st_report_done] {report_id} rows={len(rows)}")

    except Exception as e:
        import traceback
        print("[st_bg_error]", e)
        traceback.print_exc()

@app.get("/api/sp/st_range")
def sp_search_terms_range(start: str, end: str, limit: int = 1000):
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")
    pid = _env("AMZN_PROFILE_ID")

    from datetime import date as _date
    try:
        start_d = _date.fromisoformat(start)
        end_d = _date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

    q = text("""
        SELECT
            date, campaign_name, ad_group_name,
            search_term, keyword_text, match_type,
            impressions, clicks, cost, attributed_sales_14d, attributed_conversions_14d,
            cpc, ctr, acos, roas
        FROM fact_sp_search_term_daily
        WHERE profile_id = :pid
          AND date BETWEEN :start_d AND :end_d
        ORDER BY date DESC, campaign_name, ad_group_name, search_term
        LIMIT :lim
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"pid": pid, "start_d": start_d, "end_d": end_d, "lim": limit}).mappings().all()
    return [dict(r) for r in rows]

# ---- SP SEARCH TERMS: fetch & upsert (sync) ----
from fastapi import Query
import io, gzip, json as _json, uuid as _uuid, urllib.request
from sqlalchemy import text as _text

@app.post("/api/sp/st_fetch")
def sp_search_terms_fetch(
    report_id: str = Query(..., description="Amazon Reports v3 reportId"),
    limit: int = Query(50000, ge=1, le=200000)
):
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")

    # 1) get meta (auth required)
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)

    status_url = f"{ads_base}/reporting/reports/{report_id}"
    with httpx.Client(timeout=120) as client:
        r = client.get(status_url, headers=headers)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=502, detail={"stage":"check_report","status":e.response.status_code,"body":e.response.text})
        meta = r.json()
        st = meta.get("status")
        url = meta.get("url")
        if st not in ("SUCCESS", "COMPLETED") or not url:
            # not ready yet
            return JSONResponse(status_code=409, content={"stage":"check_report","status":st,"meta":meta})

    # 2) download presigned S3 URL with ZERO headers
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            raw_bytes = resp.read()
    except Exception as e:
        raise HTTPException(status_code=502, detail={"stage":"download","status":400,"body":f"urllib error: {e!r}"})

    # 3) gunzip (or fallback)
    try:
        buf = io.BytesIO(raw_bytes)
        with gzip.GzipFile(fileobj=buf) as gz:
            raw_text = gz.read().decode("utf-8")
    except OSError:
        raw_text = raw_bytes.decode("utf-8", errors="ignore")

    # 4) iterate records (NDJSON or JSON array)
    def iter_records(text: str):
        any_yield = False
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            any_yield = True
            yield _json.loads(line)
        if not any_yield:
            try:
                arr = _json.loads(text)
                if isinstance(arr, list):
                    for obj in arr:
                        yield obj
            except Exception:
                pass

    # 5) upsert   <-- from here down, everything was missing one indent level
    pid = _env("AMZN_PROFILE_ID")
    run_id = str(_uuid.uuid4())
    inserted = updated = processed = 0

    upsert_sql = _text("""
    INSERT INTO fact_sp_search_term_daily (
        profile_id, date,
        campaign_id, campaign_name,
        ad_group_id, ad_group_name,
        search_term, keyword_id, keyword_text, match_type,
        impressions, clicks, cost, attributed_sales_14d, attributed_conversions_14d,
        cpc, ctr, acos, roas,
        run_id, pulled_at
    )
    VALUES (
        :profile_id, :date,
        :campaign_id, :campaign_name,
        :ad_group_id, :ad_group_name,
        :search_term, :keyword_id, :keyword_text, :match_type,
        :impressions, :clicks, :cost, :attributed_sales_14d, :attributed_conversions_14d,
        :cpc, :ctr, :acos, :roas,
        :run_id, now()
    )
    ON CONFLICT (profile_id, date, search_term, ad_group_id, match_type) DO UPDATE SET
        campaign_id = EXCLUDED.campaign_id,
        campaign_name = EXCLUDED.campaign_name,
        ad_group_id = EXCLUDED.ad_group_id,
        ad_group_name = EXCLUDED.ad_group_name,
        keyword_id = EXCLUDED.keyword_id,
        keyword_text = EXCLUDED.keyword_text,
        match_type = EXCLUDED.match_type,
        impressions = EXCLUDED.impressions,
        clicks = EXCLUDED.clicks,
        cost = EXCLUDED.cost,
        attributed_sales_14d = EXCLUDED.attributed_sales_14d,
        attributed_conversions_14d = EXCLUDED.attributed_conversions_14d,
        cpc = EXCLUDED.cpc,
        ctr = EXCLUDED.ctr,
        acos = EXCLUDED.acos,
        roas = EXCLUDED.roas,
        run_id = EXCLUDED.run_id,
        pulled_at = now()
    RETURNING xmax = 0 AS inserted_flag
    """)

    with engine.begin() as conn:
    for rec in iter_records(raw_text):
        items = rec if isinstance(rec, list) else [rec]
        for obj in items:
            if not isinstance(obj, dict):
                continue
            d = {
                "profile_id": pid,
                "date": (obj.get("date") or "")[:10],
                "campaign_id": str(obj.get("campaignId") or ""),
                "campaign_name": obj.get("campaignName") or "",
                "ad_group_id": str(obj.get("adGroupId") or ""),
                "ad_group_name": obj.get("adGroupName") or "",
                "search_term": obj.get("searchTerm") or "",
                "keyword_id": (str(obj.get("keywordId") or "") or None),
                "keyword_text": obj.get("keywordText") or None,
                "match_type": obj.get("matchType") or "",
                "impressions": int(obj.get("impressions") or 0),
                "clicks": int(obj.get("clicks") or 0),
                "cost": float(obj.get("cost") or 0.0),
                "attributed_sales_14d": float(obj.get("attributedSales14d") or 0.0),
                "attributed_conversions_14d": int(obj.get("attributedConversions14d") or 0),
                "run_id": run_id,
            }
            d["cpc"]  = round(d["cost"] / d["clicks"], 6) if d["clicks"] else 0.0
            d["ctr"]  = round(d["clicks"] / d["impressions"], 6) if d["impressions"] else 0.0
            d["acos"] = round(d["cost"] / d["attributed_sales_14d"], 6) if d["attributed_sales_14d"] else 0.0
            d["roas"] = round(d["attributed_sales_14d"] / d["cost"], 6) if d["cost"] else 0.0

            res = conn.execute(upsert_sql, d).first()
            if res and res[0] is True:
                inserted += 1
            else:
                updated += 1

            processed += 1
            if processed >= limit:
                break
    
    return {"report_id": report_id, "processed": processed, "inserted": inserted, "updated": updated}

@app.on_event("startup")
def _startup():
    init_db()
