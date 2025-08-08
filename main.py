from datetime import date
from typing import List
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uuid
import os
import urllib.parse
import json
import httpx
from fastapi.templating import Jinja2Templates

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
    import datetime, io, gzip, time

    # 1) dates with attribution buffer
    end_date = datetime.date.today() - datetime.timedelta(days=max(0, buffer_days))
    start_date = end_date - datetime.timedelta(days=max(1, lookback_days) - 1)

    # 2) tokens/headers/region
    region = os.environ.get("AMZN_REGION", "NA").upper()
    ads_base = _ads_base(region)
    access = _get_access_token_from_refresh()
    headers = _ads_headers(access)

    # 3) create report job (Reports v3)
    def _ymd(d: datetime.date) -> str:
        return d.strftime("%Y-%m-%d")

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
                # detail looks like: "The Request is a duplicate of : <uuid>"
                import re
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
            bid=0.0,  # not provided by this report; optional enrichment later
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
