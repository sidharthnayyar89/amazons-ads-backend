from datetime import date
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uuid

app = FastAPI(title="Ads Pull API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

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

from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

import os, urllib.parse, json
from fastapi import Request, HTTPException
import httpx

# Helper to pick region base
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

@app.get("/api/amzn/oauth/start")
def amzn_oauth_start():
    client_id = _env("AMZN_CLIENT_ID")
    redirect_uri = "https://amazons-ads-backend.onrender.com/api/amzn/oauth/callback"
   scope = "cpc_advertising:campaign_management"
    # LWA authorize URL
    params = {
        "client_id": client_id,
       "scope": scope,
        "response_type": "code",
        "redirect_uri": redirect_uri,
    }
    url = "https://www.amazon.com/ap/oa?" + urllib.parse.urlencode(params)
    # Redirect by returning a tiny HTML with a link (Render sometimes blocks 307s)
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

    # Exchange code for access + refresh token
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

    # Show you the refresh token so you can copy it into Render env vars
    # WARNING: keep this route only temporarily, then remove it.
    return HTMLResponse(
        "<h2>Copy your refresh token below (keep it secret):</h2>"
        f"<pre style='white-space:pre-wrap'>{json.dumps(tok, indent=2)}</pre>"
        "<p>Set <b>AMZN_REFRESH_TOKEN</b> to the <code>refresh_token</code> value in Render → Environment.</p>"
    )

@app.get("/api/amzn/profiles")
def amzn_profiles():
    # Use your stored refresh token to get an access token, then list profiles
    client_id = _env("AMZN_CLIENT_ID")
    client_secret = _env("AMZN_CLIENT_SECRET")
    refresh_token = _env("AMZN_REFRESH_TOKEN")
    region = os.environ.get("AMZN_REGION", "NA").upper()

    # 1) exchange refresh token → access token
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

        # 2) call profiles endpoint
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

