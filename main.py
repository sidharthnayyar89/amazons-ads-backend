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
