"""
quarter-corro / quarter_pipeline.py
=====================================
Pulls ALL data Ceci needs for the Quarterly Report from Shopify API
and writes to Google Sheets (one workbook per quarter).

Sheets written:
  SUMMARY            KPI snapshot: Total Sales, Net Sales, GP, Margin, Units, Orders
  top100_all         Top 100 products by Total Sales (all)
  top100_gross_profit  Top 100 by Gross Profit (sorted by GP desc)
  top100_dropship    Top 100 Dropship products by Total Sales
  top100_best_margin Top 100 by Gross Margin % (min 5 units)
  monthly_breakdown  Monthly split: Jan / Feb / Mar totals per product
  cost_zero_items    Products with COGS=0 that customers DID pay for (flag for Shopify fix)
  zero_sales         SKUs with zero revenue this quarter (review for de-listing)
  product_types      All unique Product Types → mapped to 7 main categories
  by_category        Revenue aggregated by main category (GP, NS, GS)
  by_channel         Revenue by channel (Online Ecom, Concierge, Wellington POS)

Product Type mapping (Ceci confirmed):
  The raw Shopify Product Type field uses "Parent > Sub > Sub2" format.
  We take the FIRST segment as the main category and map to:
    Horse Wear, Rider, Horse Care, Stable, Tack & Equipment, Pharmacy, Dog, Other

Usage:
  python quarter_pipeline.py --quarter q1 --year 2026
  python quarter_pipeline.py --quarter q2 --year 2026
  python quarter_pipeline.py --quarter q1 --year 2025

GitHub Actions secrets required:
  SHOPIFY_TOKEN_CORRO   SHOPIFY_STORE   GOOGLE_CREDENTIALS   SHEET_ID_QUARTER

Note on Product Types:
  Shopify has ~20 unique product types but with duplication issues
  (e.g. "English Saddle Pads" appears in multiple formats).
  This script normalizes them all to 7 parent categories.
"""

import os
import json
import time
import requests
import gspread
import argparse
import calendar
from collections import defaultdict
from datetime import date, datetime
from google.oauth2.service_account import Credentials
import pytz

# ── CONFIG ────────────────────────────────────────────────────────
TIMEZONE    = pytz.timezone("America/Bogota")
API_VERSION = "2024-10"
STORE_URL   = os.environ.get("SHOPIFY_STORE", "equestrian-labs.myshopify.com")
TOKEN       = os.environ.get("SHOPIFY_TOKEN_CORRO", "")
SHEET_ID    = os.environ.get("SHEET_ID_QUARTER", "")
SCOPES      = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── PRODUCT TYPE → MAIN CATEGORY ─────────────────────────────────
# Ceci confirmed these 7 categories from Shopify Product Type field.
# Shopify's Product Type uses "Parent > Sub" hierarchy.
# We take the FIRST segment and normalize to one of these 7.
# NOTE: Shopify has duplication issues (e.g. "English Saddle Pads" repeated).
# This mapping handles all known variants from equestrian-labs.myshopify.com

PRODUCT_TYPE_MAP = {
    # Horse Wear — anything wearable on the horse body
    "Horse Wear":        ["horse wear", "horsewear", "blanket", "rug", "fly sheet",
                          "fly mask", "cooler", "fleece", "neck cover", "hood"],
    # Rider — human apparel and accessories
    "Rider":             ["rider", "apparel", "breeches", "shirt", "jacket", "boot",
                          "helmet", "glove", "vest", "coat", "hat", "belt", "schooling",
                          "leather handbag", "accessories", "bags"],
    # Horse Care — health, grooming, supplements, therapy
    "Horse Care":        ["horse care", "supplement", "grooming", "detangler",
                          "hoof care", "poultice", "liniment", "shampoo", "skin",
                          "pharmacy" , "equine therapy", "first aid", "wound",
                          "fly & insect", "fungal", "treats", "toys", "clippers"],
    # Stable — stable equipment, stall supplies, barn tools
    "Stable":            ["stable", "stall", "barn", "feed", "hay", "bedding",
                          "manure", "fork", "broom", "feeder", "bucket", "mat",
                          "cross tie", "jump", "cleaning", "organizer"],
    # Tack & Equipment — saddles, bridles, boots, pads, girths
    "Tack & Equipment":  ["tack & equipment", "tack and equipment", "bridle",
                          "saddle", "pad", "girth", "stirrup", "bit", "rein",
                          "boot", "wrap", "martingale", "breastplate", "halter",
                          "lead", "spur", "english"],
    # Pharmacy — prescription/medical products
    "Pharmacy":          ["pharmacy", "veterinary", "prescription", "dewormer",
                          "anti-inflammatory", "ulcer", "joint & ligament"],
    # Dog — dog products
    "Dog":               ["dog"],
}

def get_main_category(product_type: str) -> str:
    """
    Maps a Shopify Product Type string to one of 7 main categories.
    Handles both flat ("Rider") and hierarchical ("Rider > Accessories > Shirts") formats.
    """
    if not product_type or product_type.strip() == "":
        return "Other"

    pt = product_type.strip()
    # Take first segment of hierarchy
    first = pt.split(" > ")[0].strip().lower()
    full  = pt.lower()

    for category, keywords in PRODUCT_TYPE_MAP.items():
        # Check first segment match first (most reliable)
        if any(first.startswith(kw) or kw in first for kw in keywords):
            return category
        # Fallback: check full string
        if any(kw in full for kw in keywords):
            return category

    return "Other"


# ── QUARTER DATES ─────────────────────────────────────────────────
def get_quarter_dates(quarter: int, year: int):
    """Returns (start_date, end_date, label) for a given quarter."""
    today = datetime.now(TIMEZONE).date()
    month_start = (quarter - 1) * 3 + 1
    month_end   = quarter * 3
    q_start = date(year, month_start, 1)
    last_day = calendar.monthrange(year, month_end)[1]
    q_end   = date(year, month_end, last_day)
    # Don't go past today
    if q_end > today:
        q_end = today
    label = f"Q{quarter}_{year}"
    return q_start, q_end, label


# ── SHOPIFY REST ──────────────────────────────────────────────────
def shopify_get(endpoint: str, params: dict = None) -> list:
    """
    Paginated Shopify REST call with retry on 429 / 5xx / network errors.
    Returns list of all records across all pages.
    """
    if params is None:
        params = {}
    base_url = f"https://{STORE_URL}/admin/api/{API_VERSION}/{endpoint}"
    headers  = {"X-Shopify-Access-Token": TOKEN}
    results  = []
    url      = base_url
    cur_params = params

    while url:
        for attempt in range(8):
            try:
                r = requests.get(url, headers=headers, params=cur_params, timeout=60)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = min(10 * (attempt + 1), 60)
                print(f"    network error (attempt {attempt+1}/8), retry {wait}s: {e}")
                time.sleep(wait)
                continue

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 2 ** attempt) or 2)
                print(f"    rate-limited — waiting {wait}s...")
                time.sleep(max(wait, 1))
                continue

            if r.status_code >= 500:
                wait = min(10 * (attempt + 1), 90)
                print(f"    Shopify {r.status_code} — retry in {wait}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()
            break
        else:
            raise RuntimeError(f"shopify_get failed after 8 attempts: {endpoint}")

        data     = r.json()
        data_key = next((k for k in data if k != "errors"), None)
        if not data_key:
            break

        batch = data.get(data_key, [])
        if isinstance(batch, list):
            results.extend(batch)

        link = r.headers.get("Link", "")
        url  = None
        cur_params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")

        time.sleep(0.35)

    return results


# ── FETCH ORDERS ──────────────────────────────────────────────────
def fetch_orders(start: date, end: date) -> list:
    """
    Fetches all paid orders for the quarter.
    Uses two requests (paid + refunded statuses) and deduplicates by order ID
    to avoid the double-count bug from v1.0 (which gave 18K instead of 8.8K orders).
    """
    print(f"  Fetching orders {start} → {end}...")
    seen = set()
    all_orders = []

    for fin_status in ["paid,partially_paid", "partially_refunded,refunded"]:
        batch = shopify_get("orders.json", {
            "status":           "any",
            "financial_status": fin_status,
            "created_at_min":   f"{start}T00:00:00-05:00",
            "created_at_max":   f"{end}T23:59:59-05:00",
            "limit":            250,
            "fields":           "id,name,created_at,subtotal_price,total_discounts,"
                                "source_name,tags,line_items,customer",
        })
        for o in batch:
            oid = o.get("id")
            if oid and oid not in seen:
                seen.add(oid)
                all_orders.append(o)

    print(f"  → {len(all_orders)} unique orders")
    return all_orders


def fetch_products_meta() -> dict:
    """
    Returns a map: variant_id → {product_id, product_title, product_type,
                                   vendor, sku, inventory_item_id, dropship_tag}
    Dropship is detected from product tags containing 'dropship'.
    """
    print("  Fetching product catalogue...")
    products = shopify_get("products.json", {
        "limit": 250,
        "fields": "id,title,product_type,vendor,tags,variants",
    })
    vmap = {}
    for p in products:
        is_drop = "dropship" in (p.get("tags") or "").lower()
        for v in p.get("variants", []):
            vmap[str(v["id"])] = {
                "product_id":        str(p["id"]),
                "product_title":     p.get("title", ""),
                "product_type":      p.get("product_type", ""),
                "main_category":     get_main_category(p.get("product_type", "")),
                "vendor":            p.get("vendor", ""),
                "sku":               v.get("sku", ""),
                "inventory_item_id": str(v.get("inventory_item_id", "")),
                "dropship":          is_drop,
            }
    print(f"  → {len(products)} products / {len(vmap)} variants")
    return vmap


def fetch_cogs(vmap: dict) -> dict:
    """
    Returns variant_id → cost (float).
    Pulls from Shopify inventory_items endpoint in batches of 100.
    Products with no cost entered in Shopify will return 0.0 (flagged in cost_zero sheet).
    """
    print("  Fetching COGS from inventory items...")
    inv_ids = [m["inventory_item_id"] for m in vmap.values() if m["inventory_item_id"]]
    cost_map = {}  # inventory_item_id → cost

    for i in range(0, len(inv_ids), 100):
        batch_ids = ",".join(inv_ids[i:i + 100])
        items = shopify_get("inventory_items.json", {
            "ids": batch_ids, "limit": 100, "fields": "id,cost",
        })
        for item in items:
            cost_map[str(item["id"])] = float(item.get("cost") or 0)

    # Map back to variant_id
    vid_cost = {}
    for vid, meta in vmap.items():
        iid = meta.get("inventory_item_id", "")
        vid_cost[vid] = cost_map.get(iid, 0.0)

    filled = sum(1 for c in vid_cost.values() if c > 0)
    print(f"  → {len(vid_cost)} variants mapped, {filled} with COGS > $0")
    return vid_cost


# ── CHANNEL DETECTION ─────────────────────────────────────────────
def detect_channel(order: dict) -> str:
    """
    Detects sales channel from order source_name and tags.
    Concierge orders are tagged 'concierge' (case-insensitive).
    Wellington = Point of Sale terminal at Wellington Equestrian Festival.
    """
    src  = (order.get("source_name") or "").lower().strip()
    tags = (order.get("tags") or "").lower()
    if "concierge" in tags or "concierge" in src:
        return "Concierge"
    if src == "pos" or "wellington" in tags:
        return "Wellington (POS)"
    return "Online (Ecom)"


# ── AGGREGATION ───────────────────────────────────────────────────
def aggregate_products(orders: list, vmap: dict, vid_cost: dict, quarter: int, year: int) -> dict:
    """
    Aggregates line items by product_id.
    Returns: product_id → {all fields needed for all 4 product sheets}
    """
    # Monthly buckets Jan/Feb/Mar
    q_months = [(quarter - 1) * 3 + 1 + i for i in range(3)]

    agg = defaultdict(lambda: {
        "product_id": "", "product_title": "", "product_type": "",
        "main_category": "", "vendor": "", "sku": "", "dropship": False,
        "gross_sales": 0.0, "discounts": 0.0, "net_sales": 0.0,
        "cogs": 0.0, "gross_profit": 0.0,
        "units": 0, "orders": set(),
        "channels": defaultdict(float),
        "monthly": defaultdict(lambda: {"units": 0, "gross_sales": 0.0, "net_sales": 0.0, "cogs": 0.0}),
    })

    for order in orders:
        disc_total  = float(order.get("total_discounts", 0) or 0)
        li_list     = order.get("line_items", [])
        li_count    = len(li_list) or 1
        disc_per_li = disc_total / li_count
        channel     = detect_channel(order)
        order_month = int((order.get("created_at") or "")[:7].split("-")[1] or 0)

        for li in li_list:
            vid   = str(li.get("variant_id") or "")
            meta  = vmap.get(vid, {
                "product_id": str(li.get("product_id", "")),
                "product_title": li.get("title", "Unknown"),
                "product_type": "", "main_category": "Other",
                "vendor": "", "sku": li.get("sku", ""),
                "dropship": False,
            })
            pid   = meta["product_id"]
            qty   = int(li.get("quantity", 0) or 0)
            price = float(li.get("price", 0) or 0) * qty
            net   = max(price - disc_per_li, 0)
            cost  = vid_cost.get(vid, 0.0) * qty
            gp    = max(net - cost, 0)

            row = agg[pid]
            row["product_id"]    = pid
            row["product_title"] = meta["product_title"]
            row["product_type"]  = meta["product_type"]
            row["main_category"] = meta["main_category"]
            row["vendor"]        = meta["vendor"]
            row["sku"]           = row["sku"] or meta["sku"]
            row["dropship"]      = row["dropship"] or meta["dropship"]
            row["gross_sales"]  += price
            row["discounts"]    += disc_per_li
            row["net_sales"]    += net
            row["cogs"]         += cost
            row["gross_profit"] += gp
            row["units"]        += qty
            row["orders"].add(order["id"])
            row["channels"][channel] += net

            # Monthly breakdown
            mo_key = f"m{order_month}"
            row["monthly"][mo_key]["units"]      += qty
            row["monthly"][mo_key]["gross_sales"] += price
            row["monthly"][mo_key]["net_sales"]   += net
            row["monthly"][mo_key]["cogs"]        += cost

    return dict(agg)


def compute_product_rows(agg: dict, sort_key: str = "gross_sales") -> list:
    """
    Converts aggregation dict to sorted list with Pareto zone assignment.
    sort_key: 'gross_sales' | 'gross_profit' | 'gp_pct' | 'net_sales'
    Pareto zone always based on CUMULATIVE GROSS PROFIT (Ceci's v2 requirement).
    """
    rows = []
    for pid, d in agg.items():
        ns  = d["net_sales"]
        gp  = d["gross_profit"]
        gs  = d["gross_sales"]
        cos = d["cogs"]
        gpm = round(gp / ns * 100, 2) if ns > 0 else 0.0
        top_ch = max(d["channels"], key=d["channels"].get) if d["channels"] else ""
        rows.append({
            **d,
            "orders":    len(d["orders"]),
            "gross_sales":   round(gs, 2),
            "net_sales":     round(ns, 2),
            "cogs":          round(cos, 2),
            "gross_profit":  round(gp, 2),
            "gp_pct":        gpm,
            "top_channel":   top_ch,
        })

    rows.sort(key=lambda r: r.get(sort_key, 0), reverse=True)

    # Assign Pareto zones by cumulative GP
    total_gp = sum(r["gross_profit"] for r in rows) or 1
    cum_gp = 0.0
    for i, r in enumerate(rows):
        cum_gp += r["gross_profit"]
        cum_pct = cum_gp / total_gp * 100
        r["rank"]      = i + 1
        r["cum_gp_pct"] = round(cum_pct, 2)
        r["pareto_zone"] = "A" if cum_pct <= 80 else ("B" if cum_pct <= 95 else "C")

    return rows


# ── CHANNEL AGGREGATION ───────────────────────────────────────────
def aggregate_channels(orders: list) -> list:
    ch_agg = defaultdict(lambda: {"gross_sales": 0.0, "net_sales": 0.0,
                                   "units": 0, "orders": 0})
    for order in orders:
        ch   = detect_channel(order)
        sub  = float(order.get("subtotal_price", 0) or 0)
        disc = float(order.get("total_discounts", 0) or 0)
        units = sum(int(li.get("quantity", 0) or 0) for li in order.get("line_items", []))
        ch_agg[ch]["gross_sales"] += sub + disc
        ch_agg[ch]["net_sales"]  += sub
        ch_agg[ch]["units"]      += units
        ch_agg[ch]["orders"]     += 1

    total_ns = sum(d["net_sales"] for d in ch_agg.values()) or 1
    result = []
    for ch, d in sorted(ch_agg.items(), key=lambda x: -x[1]["net_sales"]):
        result.append({
            "channel":    ch,
            "gross_sales": round(d["gross_sales"], 2),
            "net_sales":   round(d["net_sales"], 2),
            "pct_revenue": round(d["net_sales"] / total_ns * 100, 1),
            "units":       d["units"],
            "orders":      d["orders"],
        })
    return result


# ── CATEGORY AGGREGATION ──────────────────────────────────────────
def aggregate_categories(product_rows: list) -> list:
    """Groups product rows by main_category."""
    cat_agg = defaultdict(lambda: {"gross_sales": 0.0, "net_sales": 0.0,
                                    "gross_profit": 0.0, "cogs": 0.0,
                                    "units": 0, "orders": 0})
    for r in product_rows:
        cat = r.get("main_category", "Other")
        cat_agg[cat]["gross_sales"]  += r["gross_sales"]
        cat_agg[cat]["net_sales"]    += r["net_sales"]
        cat_agg[cat]["gross_profit"] += r["gross_profit"]
        cat_agg[cat]["cogs"]         += r["cogs"]
        cat_agg[cat]["units"]        += r["units"]
        cat_agg[cat]["orders"]       += r["orders"]

    total_gp = sum(d["gross_profit"] for d in cat_agg.values()) or 1
    result = []
    for cat, d in sorted(cat_agg.items(), key=lambda x: -x[1]["gross_profit"]):
        ns = d["net_sales"] or 1
        result.append({
            "category":    cat,
            "gross_sales": round(d["gross_sales"], 2),
            "net_sales":   round(d["net_sales"], 2),
            "gross_profit":round(d["gross_profit"], 2),
            "cogs":        round(d["cogs"], 2),
            "gp_pct":      round(d["gross_profit"] / ns * 100, 1),
            "pct_of_total_gp": round(d["gross_profit"] / total_gp * 100, 1),
            "units":       d["units"],
            "orders":      d["orders"],
        })
    return result


# ── PRODUCT TYPE AUDIT ────────────────────────────────────────────
def audit_product_types(vmap: dict) -> list:
    """
    Returns all unique Product Types with their main_category mapping.
    Useful for verifying the mapping is correct and spotting duplicates.
    Shopify note: equestrian-labs has ~20 types but with duplication issues.
    """
    pt_map = {}
    for meta in vmap.values():
        pt  = meta.get("product_type", "") or "(none)"
        cat = meta.get("main_category", "Other")
        if pt not in pt_map:
            pt_map[pt] = {"product_type": pt, "mapped_to": cat, "variant_count": 0}
        pt_map[pt]["variant_count"] += 1

    return sorted(pt_map.values(), key=lambda x: x["mapped_to"])


# ── SUMMARY ───────────────────────────────────────────────────────
def compute_summary(orders: list, product_rows: list, quarter: int, year: int,
                    start: date, end: date) -> dict:
    total_gross_sales = sum(r["gross_sales"] for r in product_rows)
    total_net_sales   = sum(r["net_sales"] for r in product_rows)
    total_discounts   = sum(r["discounts"] for r in product_rows)
    total_gp          = sum(r["gross_profit"] for r in product_rows)
    total_cogs        = sum(r["cogs"] for r in product_rows)
    total_units       = sum(r["units"] for r in product_rows)
    gp_margin         = round(total_gp / total_net_sales * 100, 1) if total_net_sales else 0
    za_count          = sum(1 for r in product_rows if r["pareto_zone"] == "A")
    returns           = total_gross_sales - total_discounts - total_net_sales

    return {
        "quarter":       f"Q{quarter}_{year}",
        "period":        f"{start} → {end}",
        "total_orders":  len(orders),
        "total_units":   total_units,
        "gross_sales":   round(total_gross_sales, 2),
        "total_discounts": round(total_discounts, 2),
        "returns_est":   round(returns, 2),
        "net_sales":     round(total_net_sales, 2),
        "cogs":          round(total_cogs, 2),
        "gross_profit":  round(total_gp, 2),
        "gross_margin_pct": gp_margin,
        "pareto_zone_a_products": za_count,
    }


# ── GOOGLE SHEETS ─────────────────────────────────────────────────
def get_gc():
    raw = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS env var missing")
    creds = Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    return gspread.authorize(creds)


def write_sheet(sh, tab_name: str, headers: list, rows: list):
    """Creates or clears a worksheet and writes headers + rows."""
    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(tab_name, rows=max(3000, len(rows) + 50), cols=len(headers) + 2)
        time.sleep(2)

    ws.clear()
    time.sleep(0.5)

    all_data = [headers]
    for row in rows:
        clean = []
        for v in row:
            if v is None:
                clean.append("")
            elif isinstance(v, float) and v != v:  # NaN
                clean.append("")
            elif isinstance(v, bool):
                clean.append("YES" if v else "NO")
            elif isinstance(v, (int, float)):
                clean.append(v)
            else:
                clean.append(str(v))
        all_data.append(clean)

    # Write in batches
    BATCH = 500
    for i in range(0, len(all_data), BATCH):
        ws.append_rows(all_data[i:i + BATCH], value_input_option="RAW",
                       insert_data_option="INSERT_ROWS")
        if i + BATCH < len(all_data):
            time.sleep(1.5)

    time.sleep(4)  # avoid Sheets rate limit between tabs
    print(f"    ✓ {tab_name}: {len(rows)} rows")


# ── MAIN ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="quarter-corro pipeline")
    parser.add_argument("--quarter", type=int, default=1, choices=[1, 2, 3, 4])
    parser.add_argument("--year",    type=int, default=2026)
    args = parser.parse_args()

    start, end, label = get_quarter_dates(args.quarter, args.year)
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    print(f"\n{'='*65}")
    print(f"  quarter-corro — {label}")
    print(f"  Store  : {STORE_URL}")
    print(f"  Period : {start} → {end}")
    print(f"  Sheet  : {SHEET_ID or '(not set)'}")
    print(f"{'='*65}\n")

    if not TOKEN:
        raise RuntimeError("SHOPIFY_TOKEN_CORRO env var missing")
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID_QUARTER env var missing")

    # ── Pull data ─────────────────────────────────────────────────
    orders   = fetch_orders(start, end)
    vmap     = fetch_products_meta()
    vid_cost = fetch_cogs(vmap)
    agg      = aggregate_products(orders, vmap, vid_cost, args.quarter, args.year)

    # ── Product variants ──────────────────────────────────────────
    # All products sorted by Total Sales (Gross Sales proxy)
    all_rows = compute_product_rows(agg, sort_key="gross_sales")
    # Top by Gross Profit (Ceci's v2 requirement)
    gp_rows  = compute_product_rows(agg, sort_key="gross_profit")
    # Dropship only
    drop_rows = [r for r in all_rows if r["dropship"]]
    # Best margin (min 5 units, sorted by GP%)
    margin_rows = sorted(
        [r for r in all_rows if r["units"] >= 5 and r["gp_pct"] > 0],
        key=lambda r: r["gp_pct"], reverse=True
    )
    # Cost zero items (COGS = 0 but customer DID pay — need Shopify cost fix)
    cost_zero = [r for r in all_rows if r["cogs"] == 0 and r["net_sales"] > 0]
    # Zero sales SKUs
    zero_sales = [r for r in all_rows if r["net_sales"] == 0]

    channels   = aggregate_channels(orders)
    categories = aggregate_categories(all_rows)
    summary    = compute_summary(orders, all_rows, args.quarter, args.year, start, end)
    pt_audit   = audit_product_types(vmap)

    # Monthly breakdown (Jan/Feb/Mar for Q1)
    q_months = [(args.quarter - 1) * 3 + 1 + i for i in range(3)]

    # ── Write to Google Sheets ────────────────────────────────────
    gc = get_gc()
    sh = gc.open_by_key(SHEET_ID)
    print(f"\n  Writing to Google Sheets → {SHEET_ID}")

    # SUMMARY
    write_sheet(sh, "SUMMARY", [
        "updated_at", "quarter", "period",
        "total_orders", "total_units",
        "gross_sales", "total_discounts", "returns_est", "net_sales",
        "cogs", "gross_profit", "gross_margin_pct",
        "pareto_zone_a_products",
    ], [[
        now_str, summary["quarter"], summary["period"],
        summary["total_orders"], summary["total_units"],
        summary["gross_sales"], summary["total_discounts"], summary["returns_est"],
        summary["net_sales"], summary["cogs"], summary["gross_profit"],
        summary["gross_margin_pct"], summary["pareto_zone_a_products"],
    ]])

    # PRODUCT HEADERS
    H_PROD = [
        "rank", "pareto_zone", "product_title", "product_type", "main_category",
        "vendor", "sku", "dropship",
        "gross_sales", "discounts", "net_sales", "cogs", "gross_profit", "gp_pct",
        "cum_gp_pct", "units", "orders", "top_channel",
    ]

    def prod_row(r):
        return [
            r["rank"], r["pareto_zone"], r["product_title"], r["product_type"],
            r["main_category"], r["vendor"], r["sku"], r["dropship"],
            r["gross_sales"], round(r["discounts"], 2), r["net_sales"],
            r["cogs"], r["gross_profit"], r["gp_pct"], r["cum_gp_pct"],
            r["units"], r["orders"], r["top_channel"],
        ]

    # TOP 100 — All Products by Total Sales
    write_sheet(sh, "top100_all",      H_PROD, [prod_row(r) for r in all_rows[:100]])
    # TOP 100 — By Gross Profit (Ceci v2: sorted by GP, zonas por GP acumulado)
    write_sheet(sh, "top100_gross_profit", H_PROD, [prod_row(r) for r in gp_rows[:100]])
    # TOP 100 — Dropship only
    write_sheet(sh, "top100_dropship", H_PROD, [prod_row(r) for r in drop_rows[:100]])
    # TOP 100 — Best Margin (min 5 units)
    write_sheet(sh, "top100_best_margin", H_PROD, [prod_row(r) for r in margin_rows[:100]])

    # MONTHLY BREAKDOWN
    m_headers = [
        "rank", "product_title", "vendor", "sku", "main_category",
    ]
    for m in q_months:
        m_headers += [f"m{m}_units", f"m{m}_gross_sales", f"m{m}_net_sales", f"m{m}_gp_pct"]
    m_headers += ["q_units", "q_gross_sales", "q_net_sales", "q_gross_profit"]

    m_rows = []
    for i, r in enumerate(all_rows[:100], 1):
        row = [i, r["product_title"], r["vendor"], r["sku"], r["main_category"]]
        for m in q_months:
            mk = f"m{m}"
            md = r.get("monthly", {}).get(mk, {})
            m_ns = md.get("net_sales", 0)
            m_cg = md.get("cogs", 0)
            m_gp = round((m_ns - m_cg) / m_ns * 100, 1) if m_ns > 0 else 0
            row += [md.get("units", 0), round(md.get("gross_sales", 0), 2),
                    round(m_ns, 2), m_gp]
        row += [r["units"], r["gross_sales"], r["net_sales"], r["gross_profit"]]
        m_rows.append(row)
    write_sheet(sh, "monthly_breakdown", m_headers, m_rows)

    # COST ZERO ITEMS (COGS=0, customer paid > $0 → needs Shopify cost update)
    write_sheet(sh, "cost_zero_items", [
        "rank", "product_title", "vendor", "sku", "product_type",
        "net_sales", "gross_profit_inflated", "units", "orders",
        "note",
    ], [[
        i+1, r["product_title"], r["vendor"], r["sku"], r["product_type"],
        r["net_sales"], r["gross_profit"], r["units"], r["orders"],
        "COGS=0 in Shopify — GP is overstated. Update cost in Admin > Products > Variant",
    ] for i, r in enumerate(cost_zero[:50])])

    # ZERO SALES (SKUs with no revenue this quarter)
    write_sheet(sh, "zero_sales", [
        "product_title", "vendor", "sku", "product_type", "main_category",
        "note",
    ], [[
        r["product_title"], r["vendor"], r["sku"], r["product_type"],
        r["main_category"], "Review for de-listing or inventory clearance",
    ] for r in zero_sales[:200]])

    # PRODUCT TYPE AUDIT
    write_sheet(sh, "product_types", [
        "product_type_raw", "mapped_to_category", "variant_count", "note",
    ], [[
        pt["product_type"], pt["mapped_to"], pt["variant_count"],
        "Duplicate/blank — review in Shopify Admin" if not pt["product_type"] or pt["product_type"] == "(none)" else "",
    ] for pt in pt_audit])

    # BY CATEGORY
    write_sheet(sh, "by_category", [
        "category", "gross_sales", "net_sales", "cogs",
        "gross_profit", "gp_pct", "pct_of_total_gp", "units", "orders",
    ], [[
        c["category"], c["gross_sales"], c["net_sales"], c["cogs"],
        c["gross_profit"], c["gp_pct"], c["pct_of_total_gp"], c["units"], c["orders"],
    ] for c in categories])

    # BY CHANNEL
    write_sheet(sh, "by_channel", [
        "channel", "gross_sales", "net_sales", "pct_revenue", "units", "orders",
    ], [[
        c["channel"], c["gross_sales"], c["net_sales"],
        c["pct_revenue"], c["units"], c["orders"],
    ] for c in channels])

    # ── Print summary ─────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  ✓ DONE — {label}")
    print(f"  Orders         : {len(orders):,}")
    print(f"  Gross Sales    : ${summary['gross_sales']:,.2f}")
    print(f"  Net Sales      : ${summary['net_sales']:,.2f}")
    print(f"  Gross Profit   : ${summary['gross_profit']:,.2f} ({summary['gross_margin_pct']}% margin)")
    print(f"  Units Sold     : {summary['total_units']:,}")
    print(f"  Pareto Zone A  : {summary['pareto_zone_a_products']} products")
    print(f"  COGS=0 items   : {len(cost_zero)} (need Shopify cost update)")
    print(f"  Zero sales SKUs: {len(zero_sales)}")
    print(f"  Sheet          : {SHEET_ID}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
