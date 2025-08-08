import os, urllib.parse, json
from fastapi import HTTPException
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
    # Return an HTML that redirects (some hosts block 307s from APIs)
    return HTMLResponse(
        f'<a href="{url}">Continue to Amazon consent</a>'
        f'<script>location.href="{url}"</script>'
    )

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

    # Show the refresh token so you can store it in Render
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
