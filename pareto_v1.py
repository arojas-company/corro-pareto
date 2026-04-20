"""
PARETO PIPELINE v1 — Equestrian Labs / Corro
=============================================
Pulls order data directly from Shopify REST API and writes
four Pareto analysis tabs to Google Sheets:

  pareto_products   – revenue by PARENT product (groups all variants/colors)
  pareto_categories – revenue by Shopify collection (main category)
  pareto_channels   – revenue by sales channel
  pareto_customers  – top customers by revenue

Each tab stores ALL periods — every full year and every quarter from
START_YEAR through today. The dashboard filters client-side by period key.

Period keys written:
  full_year_2024, full_year_2025, full_year_2026  (Jan 1 → Dec 31 or today)
  q1_2024 … q4_2026                               (Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec)

Designed to answer Ceci's question:
  "Why does a product with 14 sales in 90 days not appear in top sellers?"
  → Because the old report split by variant. Now grouped by parent product.

Run modes:
  python pareto_v1.py                    # all periods from START_YEAR → today
  python pareto_v1.py --only q4_2025    # single period (for quick re-run)
  python pareto_v1.py --only custom --start 2025-01-01 --end 2025-12-31

Environment variables required:
  SHOPIFY_TOKEN_CORRO   – Shopify Admin API token
  GOOGLE_CREDENTIALS    – Google service account JSON string
  SHEET_ID_CORRO        – Google Sheet ID for Corro

GitHub Actions: set as repository secrets.
"""

import os, json, requests, gspread, argparse, calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
from collections import defaultdict
import pytz

# ── CONFIG ────────────────────────────────────────────────────────
TIMEZONE    = pytz.timezone("America/Bogota")
API_VERSION = "2024-10"
STORE_URL   = os.environ.get("SHOPIFY_STORE", "equestrian-labs.myshopify.com")
TOKEN       = os.environ.get("SHOPIFY_TOKEN_CORRO", "")
SHEET_ID    = os.environ.get("SHEET_ID_CORRO", "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU")
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets",
               "https://www.googleapis.com/auth/drive"]

# First year to include in full backfill
START_YEAR  = 2024

# Channel detection — same logic as pipeline_v3
CHANNEL_MAP = {
    "pos":          "Wellington (POS)",
    "wellington":   "Wellington (POS)",
    "concierge":    "Concierge",
    "web":          "Online (Ecom)",
    "shopify":      "Online (Ecom)",
    "online_store": "Online (Ecom)",
    "":             "Online (Ecom)",
}

# ── PERIOD BUILDER ────────────────────────────────────────────────
def last_day(y, m):
    return date(y, m, calendar.monthrange(y, m)[1])

def build_all_periods():
    """
    Returns list of (start, end, period_key) for:
      - Every full year from START_YEAR to current year
      - Every quarter from Q1 START_YEAR to the current (partial) quarter
    Sorted chronologically, most recent first.
    """
    today = datetime.now(TIMEZONE).date()
    periods = []

    for y in range(START_YEAR, today.year + 1):
        # Full year
        ys = date(y, 1, 1)
        ye = date(y, 12, 31) if y < today.year else today
        periods.append((ys, ye, f"full_year_{y}"))

        # Quarters
        max_q = 4 if y < today.year else ((today.month - 1) // 3 + 1)
        for q in range(1, max_q + 1):
            qs = date(y, (q - 1) * 3 + 1, 1)
            qe = last_day(y, q * 3) if (y < today.year or q < max_q) else today
            periods.append((qs, qe, f"q{q}_{y}"))

    # Most recent first so progress output is intuitive
    periods.sort(key=lambda x: x[0], reverse=True)
    return periods

def resolve_single(key, start_override=None, end_override=None):
    """Parse a single --only period key into (start, end, key)."""
    today = datetime.now(TIMEZONE).date()
    if key == "custom" and start_override and end_override:
        return date.fromisoformat(start_override), date.fromisoformat(end_override), "custom"
    # full_year_YYYY or full_year (current year)
    if key.startswith("full_year_"):
        y = int(key.split("_")[-1])
        return date(y, 1, 1), (date(y, 12, 31) if y < today.year else today), key
    if key == "full_year":
        return date(today.year, 1, 1), today, f"full_year_{today.year}"
    # qN_YYYY
    if key.startswith("q") and "_" in key:
        parts = key[1:].split("_"); q, y = int(parts[0]), int(parts[1])
        qs = date(y, (q - 1) * 3 + 1, 1)
        cur_q = (today.month - 1) // 3 + 1
        qe = last_day(y, q * 3) if (y < today.year or q < cur_q) else today
        return qs, qe, key
    raise ValueError(f"Unknown period key: {key}. Use full_year_YYYY or qN_YYYY or custom.")

# ── SHOPIFY REST ───────────────────────────────────────────────────
def shopify_get(endpoint, params):
    url = f"https://{STORE_URL}/admin/api/{API_VERSION}/{endpoint}"
    headers = {"X-Shopify-Access-Token": TOKEN}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        key = [k for k in data if k != "errors"][0]
        results.extend(data[key])
        link = r.headers.get("Link", ""); url = None; params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results

def fetch_orders(start: date, end: date):
    """Fetch all paid orders in date range with full line item detail."""
    print(f"  Fetching orders {start} → {end}...")
    orders = shopify_get("orders.json", {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start}T00:00:00-05:00",
        "created_at_max": f"{end}T23:59:59-05:00",
        "limit": 250,
        "fields": (
            "id,name,created_at,subtotal_price,total_discounts,"
            "source_name,tags,line_items,customer"
        ),
    })
    print(f"  → {len(orders)} orders retrieved")
    return orders

def fetch_cogs_map():
    """
    Fetch cost per variant via inventory_items API.
    Shopify stores 'Cost per item' (COGS) in inventory_item.cost.
    Returns: {inventory_item_id: cost_float}
    Called in batches of 100 IDs (API limit).
    """
    print("  Fetching COGS from inventory items...")
    # First get all inventory_item_ids from variants
    all_items = shopify_get("variants.json", {
        "limit": 250,
        "fields": "id,inventory_item_id,sku",
    })
    iids = [str(v["inventory_item_id"]) for v in all_items if v.get("inventory_item_id")]
    vid_to_iid = {str(v["id"]): str(v["inventory_item_id"]) for v in all_items if v.get("inventory_item_id")}

    cost_map = {}  # inventory_item_id → cost
    # Fetch in batches of 100
    for i in range(0, len(iids), 100):
        batch = iids[i:i+100]
        items = shopify_get("inventory_items.json", {
            "ids": ",".join(batch),
            "limit": 100,
            "fields": "id,cost,sku",
        })
        for item in items:
            cost_map[str(item["id"])] = float(item.get("cost") or 0)

    # Map variant_id → cost
    variant_cost = {}
    for v in all_items:
        vid  = str(v["id"])
        iid  = str(v.get("inventory_item_id", ""))
        variant_cost[vid] = cost_map.get(iid, 0.0)

    filled = sum(1 for c in variant_cost.values() if c > 0)
    print(f"  → {len(variant_cost)} variants, {filled} with cost > 0")
    return variant_cost

def fetch_products_map():
    """
    Build a map: variant_id → {product_id, product_title, sku, vendor, product_type}
    Used to group variants under their parent product.
    """
    print("  Fetching product catalogue...")
    products = shopify_get("products.json", {
        "limit": 250,
        "fields": "id,title,vendor,product_type,variants",
    })
    vmap = {}
    for p in products:
        for v in p.get("variants", []):
            vmap[str(v["id"])] = {
                "product_id":         str(p["id"]),
                "product_title":      p.get("title", ""),
                "sku_parent":         v.get("sku", "").split("-")[0] if v.get("sku") else "",
                "sku_full":           v.get("sku", ""),
                "vendor":             p.get("vendor", ""),
                "product_type":       p.get("product_type", "Uncategorized"),
                "inventory_item_id":  str(v.get("inventory_item_id", "")),
            }
    print(f"  → {len(products)} products / {len(vmap)} variants mapped")
    return vmap

def fetch_collections_map():
    """
    Build a map: product_id → [collection_title, ...]
    Uses Shopify GraphQL Admin API (collects.json was deprecated).
    First fetches all collection IDs+titles via REST, then fetches
    products per collection via GraphQL using the collection GID.
    """
    print("  Fetching collections via GraphQL...")
    url_gql = f"https://{STORE_URL}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }

    # Step 1: get all collections (id + title) via REST — still works
    colls = {}
    for c in shopify_get("custom_collections.json", {"limit": 250, "fields": "id,title"}):
        colls[str(c["id"])] = c["title"]
    for c in shopify_get("smart_collections.json", {"limit": 250, "fields": "id,title"}):
        colls[str(c["id"])] = c["title"]

    # Step 2: for each collection, fetch its products via GraphQL (handles pagination)
    prod_collections = defaultdict(list)
    for cid, ctitle in colls.items():
        gid = f"gid://shopify/Collection/{cid}"
        prod_cursor = None
        while True:
            after = f', after: "{prod_cursor}"' if prod_cursor else ""
            query = f"""
            {{
              collection(id: "{gid}") {{
                products(first: 250{after}) {{
                  pageInfo {{ hasNextPage endCursor }}
                  edges {{ node {{ id }} }}
                }}
              }}
            }}
            """
            r = requests.post(url_gql, headers=headers, json={"query": query}, timeout=60)
            r.raise_for_status()
            data = r.json()
            coll_node = (data.get("data") or {}).get("collection")
            if not coll_node:
                break
            products_page = coll_node["products"]
            for pe in products_page["edges"]:
                pid = pe["node"]["id"].split("/")[-1]
                prod_collections[pid].append(ctitle)
            if not products_page["pageInfo"]["hasNextPage"]:
                break
            prod_cursor = products_page["pageInfo"]["endCursor"]

    print(f"  → {len(colls)} collections mapped")
    return prod_collections

# ── ANALYSIS ──────────────────────────────────────────────────────
def detect_channel(order):
    src  = (order.get("source_name") or "").lower().strip()
    tags = (order.get("tags") or "").lower()
    if src == "pos" or "wellington" in tags or "pos" in tags:
        return "Wellington (POS)"
    if "concierge" in tags or "concierge" in src:
        return "Concierge"
    if src in ("web", "shopify", "", "online_store") or not src:
        return "Online (Ecom)"
    return "Others"

def audit_concierge_tags(orders, sample=30):
    """
    Print every unique tag combination found in orders.
    Use this to discover the exact tag Shopify is using for Concierge orders.
    Run once and check the output to confirm.
    """
    tag_counts = defaultdict(int)
    concierge_found = []
    for o in orders:
        tags = (o.get("tags") or "").strip()
        src  = (o.get("source_name") or "").strip()
        key  = f"source={src!r:20} | tags={tags[:80]!r}" if tags or src else "(no tags, no source)"
        tag_counts[key] += 1
        if "concierge" in tags.lower() or "concierge" in src.lower():
            concierge_found.append(key)

    print("\n  ── CONCIERGE TAG AUDIT ──────────────────────────────")
    print(f"  Total unique source+tag combos: {len(tag_counts)}")
    print(f"  Orders matching 'concierge': {len(concierge_found)}")
    print("\n  All source+tag combinations (sorted by count):")
    for k, cnt in sorted(tag_counts.items(), key=lambda x: -x[1])[:sample]:
        print(f"    {cnt:>5}x  {k}")
    print("  ────────────────────────────────────────────────────\n")

def build_pareto_products(orders, vmap, variant_cost):
    """Group by parent product, summing revenue + COGS + gross profit."""
    agg = defaultdict(lambda: {
        "product_id": "", "product_title": "", "product_type": "",
        "vendor": "", "sku_parent": "",
        "gross_sales": 0.0, "discounts": 0.0, "net_sales": 0.0,
        "cogs": 0.0, "gross_profit": 0.0,
        "units": 0, "orders": set(), "channels": defaultdict(float),
    })
    for order in orders:
        disc_total  = float(order.get("total_discounts", 0) or 0)
        li_count    = len(order.get("line_items", [])) or 1
        disc_per_li = disc_total / li_count
        for li in order.get("line_items", []):
            vid   = str(li.get("variant_id") or "")
            info  = vmap.get(vid, {
                "product_id": str(li.get("product_id", "")),
                "product_title": li.get("title", "Unknown"),
                "product_type": "Uncategorized",
                "vendor": "", "sku_parent": "",
            })
            pid   = info["product_id"]
            qty   = int(li.get("quantity", 0) or 0)
            price = float(li.get("price", 0) or 0) * qty
            net   = max(price - disc_per_li, 0)
            cost  = variant_cost.get(vid, 0.0) * qty
            row = agg[pid]
            row["product_id"]    = pid
            row["product_title"] = info["product_title"]
            row["product_type"]  = info["product_type"]
            row["vendor"]        = info["vendor"]
            row["sku_parent"]    = info["sku_parent"]
            row["gross_sales"]  += price
            row["discounts"]    += disc_per_li
            row["net_sales"]    += net
            row["cogs"]         += cost
            row["gross_profit"] += max(net - cost, 0)
            row["units"]        += qty
            row["orders"].add(order["id"])
            row["channels"][detect_channel(order)] += net
    return agg

def build_pareto_categories(orders, vmap, prod_colls, variant_cost):
    """Group by main Shopify collection/product_type, including COGS."""
    agg = defaultdict(lambda: {
        "gross_sales": 0.0, "discounts": 0.0, "net_sales": 0.0,
        "cogs": 0.0, "gross_profit": 0.0,
        "units": 0, "orders": set(),
    })
    for order in orders:
        disc_total  = float(order.get("total_discounts", 0) or 0)
        li_count    = len(order.get("line_items", [])) or 1
        disc_per_li = disc_total / li_count
        for li in order.get("line_items", []):
            vid   = str(li.get("variant_id") or "")
            info  = vmap.get(vid, {"product_id": "", "product_type": "Uncategorized"})
            pid   = info["product_id"]
            cats  = prod_colls.get(pid, [])
            cat   = cats[0] if cats else (info.get("product_type") or "Uncategorized")
            qty   = int(li.get("quantity", 0) or 0)
            price = float(li.get("price", 0) or 0) * qty
            net   = max(price - disc_per_li, 0)
            cost  = variant_cost.get(vid, 0.0) * qty
            agg[cat]["gross_sales"]  += price
            agg[cat]["discounts"]    += disc_per_li
            agg[cat]["net_sales"]    += net
            agg[cat]["cogs"]         += cost
            agg[cat]["gross_profit"] += max(net - cost, 0)
            agg[cat]["units"]        += qty
            agg[cat]["orders"].add(order["id"])
    return agg

def build_pareto_channels(orders):
    """Group by sales channel."""
    agg = defaultdict(lambda: {
        "gross_sales": 0.0, "discounts": 0.0, "net_sales": 0.0,
        "units": 0, "orders": set(),
    })
    for order in orders:
        ch    = detect_channel(order)
        disc  = float(order.get("total_discounts", 0) or 0)
        sub   = float(order.get("subtotal_price", 0) or 0)
        units = sum(int(li.get("quantity", 0) or 0) for li in order.get("line_items", []))
        agg[ch]["gross_sales"] += sub + disc
        agg[ch]["discounts"]   += disc
        agg[ch]["net_sales"]   += sub
        agg[ch]["units"]       += units
        agg[ch]["orders"].add(order["id"])
    return agg

def build_pareto_customers(orders, period_start):
    """
    Top customers by net sales.
    Classifies each customer as NEW (first ever order within this period)
    or RETURNING (had orders before period_start).
    Also builds a new_vs_returning summary.
    """
    agg = defaultdict(lambda: {
        "customer_id": "", "name": "", "email": "",
        "gross_sales": 0.0, "net_sales": 0.0, "orders": 0, "units": 0,
        "first_order": "9999-99-99", "last_order": "0000-00-00",
        "is_new": True,
    })
    for order in orders:
        c    = order.get("customer") or {}
        cid  = str(c.get("id", f"guest_{order['id']}"))
        fn   = c.get("first_name", ""); ln = c.get("last_name", "")
        sub  = float(order.get("subtotal_price", 0) or 0)
        disc = float(order.get("total_discounts", 0) or 0)
        units = sum(int(li.get("quantity", 0) or 0) for li in order.get("line_items", []))
        d    = order.get("created_at", "")[:10]
        # Customer is NEW if their Shopify account was created on/after period_start
        cust_created = (c.get("created_at") or d)[:10]
        is_new = cust_created >= str(period_start)

        row = agg[cid]
        row["customer_id"]  = cid
        row["name"]         = f"{fn} {ln}".strip() or "Guest"
        row["email"]        = c.get("email", "")
        row["gross_sales"] += sub + disc
        row["net_sales"]   += sub
        row["orders"]      += 1
        row["units"]       += units
        row["is_new"]       = is_new
        if d < row["first_order"]: row["first_order"] = d
        if d > row["last_order"]:  row["last_order"]  = d
    return agg

def build_new_vs_returning(agg_customers):
    """
    Returns a simple summary dict:
      {new: {count, revenue, orders}, returning: {count, revenue, orders}}
    This is the table the meeting asked for.
    """
    summary = {
        "new":       {"count": 0, "net_sales": 0.0, "orders": 0},
        "returning": {"count": 0, "net_sales": 0.0, "orders": 0},
    }
    for d in agg_customers.values():
        key = "new" if d["is_new"] else "returning"
        summary[key]["count"]     += 1
        summary[key]["net_sales"] += d["net_sales"]
        summary[key]["orders"]    += d["orders"]
    return summary

# ── PARETO MATH ───────────────────────────────────────────────────
def add_pareto_cols(rows_sorted, revenue_key="net_sales"):
    """Add cumulative % and pareto zone (A/B/C) columns."""
    total = sum(r[revenue_key] for r in rows_sorted)
    cum = 0.0
    for i, r in enumerate(rows_sorted):
        cum += r[revenue_key]
        r["rank"]        = i + 1
        r["pct_revenue"] = round(r[revenue_key] / total * 100, 2) if total else 0
        r["cum_pct"]     = round(cum / total * 100, 2) if total else 0
        r["pareto_zone"] = "A" if r["cum_pct"] <= 80 else ("B" if r["cum_pct"] <= 95 else "C")
    return rows_sorted, total

# ── GOOGLE SHEETS WRITER ──────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

# ── UPSERT WRITER (keeps all periods, updates existing ones) ──────
def upsert_tab(sh, tab_name, headers, new_rows, key_cols):
    """
    Reads existing tab, merges new_rows by composite key (key_cols),
    then rewrites the full tab. Preserves historical periods.
    """
    try:    ws = sh.worksheet(tab_name)
    except: ws = sh.add_worksheet(tab_name, rows=5000, cols=len(headers)+2)

    existing_vals = ws.get_all_values()
    existing = {}
    if len(existing_vals) >= 2:
        ex_h = existing_vals[0]
        for r in existing_vals[1:]:
            m = {ex_h[i]: (r[i] if i < len(r) else "") for i in range(len(ex_h))}
            k = tuple(str(m.get(c,"")).strip() for c in key_cols)
            if any(k): existing[k] = [m.get(h,"") for h in headers]

    for row in new_rows:
        # row is a list aligned to headers
        m = {headers[i]: row[i] for i in range(len(headers))}
        k = tuple(str(m.get(c,"")).strip() for c in key_cols)
        existing[k] = row

    merged = list(existing.values())
    # Sort by period_start desc, then rank asc
    def sort_key(r):
        m = {headers[i]: r[i] for i in range(len(headers))}
        return (str(m.get("period_start","")) or "0000", int(m.get("rank",0) or 0))
    merged.sort(key=sort_key, reverse=False)
    merged.sort(key=lambda r: {headers[i]: r[i] for i in range(len(headers))}.get("period_start",""), reverse=True)

    ws.clear()
    ws.append_row(headers)
    if merged:
        # Write in batches to avoid timeout
        for i in range(0, len(merged), 100):
            ws.append_rows(merged[i:i+100], value_input_option="USER_ENTERED")
    print(f"    ✓ {tab_name}: {len(new_rows)} new rows, {len(merged)} total stored")
    return ws


# ── MAIN ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default=None,
        help="Run a single period key, e.g. q4_2025 or full_year_2025 or custom")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (with --only custom)")
    parser.add_argument("--end",   help="End date YYYY-MM-DD (with --only custom)")
    args = parser.parse_args()

    today   = datetime.now(TIMEZONE).date()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    # Decide which periods to run
    if args.only:
        periods = [resolve_single(args.only, args.start, args.end)]
        mode = f"single ({args.only})"
    else:
        periods = build_all_periods()
        mode = f"full backfill — {len(periods)} periods"

    print(f"\n{'='*60}")
    print(f"  PARETO PIPELINE — Corro ({STORE_URL})")
    print(f"  Mode: {mode}")
    print(f"{'='*60}\n")

    # Fetch catalogue once — reused across all periods
    vmap         = fetch_products_map()
    variant_cost = fetch_cogs_map()
    prod_colls   = fetch_collections_map()

    # Open sheet once
    gc = get_gc()
    sh = gc.open_by_key(SHEET_ID)

    # Headers for each tab
    h_prod = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "product_title","product_type","vendor","sku_parent","product_id",
              "gross_sales","discounts","net_sales","cogs","gross_profit",
              "gp_pct","pct_revenue","cum_pct","units","orders","top_channel"]
    h_cat  = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "category","gross_sales","discounts","net_sales","cogs","gross_profit",
              "gp_pct","pct_revenue","cum_pct","units","orders"]
    h_ch   = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "channel","gross_sales","discounts","net_sales","pct_revenue","cum_pct",
              "units","orders"]
    h_cust = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "customer_id","name","email","segment","net_sales","pct_revenue","cum_pct",
              "orders","units","first_order","last_order"]
    h_nvr  = ["updated_at","period","period_start","period_end",
              "segment","customers","net_sales","orders","pct_customers","pct_revenue"]

    all_prod, all_cat, all_ch, all_cust, all_nvr = [], [], [], [], []

    # Run audit on first period only to discover concierge tags
    first_run = True

    for idx, (start, end, period_label) in enumerate(periods):
        print(f"\n[{idx+1}/{len(periods)}] {period_label}  ({start} → {end})")

        orders = fetch_orders(start, end)
        if not orders:
            print(f"  ⚠ No orders — skipping")
            continue

        # Print concierge tag audit on first run so you can verify the tags
        if first_run:
            audit_concierge_tags(orders)
            first_run = False

        agg_products   = build_pareto_products(orders, vmap, variant_cost)
        agg_categories = build_pareto_categories(orders, vmap, prod_colls, variant_cost)
        agg_channels   = build_pareto_channels(orders)
        agg_customers  = build_pareto_customers(orders, start)
        nvr_summary    = build_new_vs_returning(agg_customers)

        # Products
        rows_raw = []
        for pid, d in agg_products.items():
            gp_pct = round(d["gross_profit"] / d["net_sales"] * 100, 1) if d["net_sales"] else 0
            rows_raw.append({
                "net_sales": d["net_sales"], "gross_sales": d["gross_sales"],
                "discounts": d["discounts"], "units": d["units"],
                "orders": len(d["orders"]), "product_id": d["product_id"],
                "product_title": d["product_title"], "product_type": d["product_type"],
                "vendor": d["vendor"], "sku_parent": d["sku_parent"],
                "cogs": d["cogs"], "gross_profit": d["gross_profit"], "gp_pct": gp_pct,
                "top_channel": max(d["channels"], key=d["channels"].get) if d["channels"] else "",
            })
        rows_sorted, _ = add_pareto_cols(sorted(rows_raw, key=lambda x: x["net_sales"], reverse=True))
        all_prod += [[now_str, period_label, str(start), str(end),
            r["rank"], r["pareto_zone"], r["product_title"], r["product_type"],
            r["vendor"], r["sku_parent"], r["product_id"],
            round(r["gross_sales"],2), round(r["discounts"],2), round(r["net_sales"],2),
            round(r["cogs"],2), round(r["gross_profit"],2), r["gp_pct"],
            r["pct_revenue"], r["cum_pct"], r["units"], r["orders"], r["top_channel"],
        ] for r in rows_sorted]

        # Categories
        rows_raw = []
        for cat, d in agg_categories.items():
            gp_pct = round(d["gross_profit"] / d["net_sales"] * 100, 1) if d["net_sales"] else 0
            rows_raw.append({
                "category": cat, "net_sales": d["net_sales"], "gross_sales": d["gross_sales"],
                "discounts": d["discounts"], "units": d["units"], "orders": len(d["orders"]),
                "cogs": d["cogs"], "gross_profit": d["gross_profit"], "gp_pct": gp_pct,
            })
        rows_sorted, _ = add_pareto_cols(sorted(rows_raw, key=lambda x: x["net_sales"], reverse=True))
        all_cat += [[now_str, period_label, str(start), str(end),
            r["rank"], r["pareto_zone"], r["category"],
            round(r["gross_sales"],2), round(r["discounts"],2), round(r["net_sales"],2),
            round(r["cogs"],2), round(r["gross_profit"],2), r["gp_pct"],
            r["pct_revenue"], r["cum_pct"], r["units"], r["orders"],
        ] for r in rows_sorted]

        # Channels
        rows_raw = [{"channel": ch, "net_sales": d["net_sales"], "gross_sales": d["gross_sales"],
                     "discounts": d["discounts"], "units": d["units"], "orders": len(d["orders"])}
                    for ch, d in agg_channels.items()]
        rows_sorted, _ = add_pareto_cols(sorted(rows_raw, key=lambda x: x["net_sales"], reverse=True))
        all_ch += [[now_str, period_label, str(start), str(end),
            r["rank"], r["pareto_zone"], r["channel"],
            round(r["gross_sales"],2), round(r["discounts"],2), round(r["net_sales"],2),
            r["pct_revenue"], r["cum_pct"], r["units"], r["orders"],
        ] for r in rows_sorted]

        # Customers
        rows_raw = [dict(d, net_sales=d["net_sales"], segment="New" if d["is_new"] else "Returning")
                    for d in agg_customers.values()]
        rows_sorted, _ = add_pareto_cols(sorted(rows_raw, key=lambda x: x["net_sales"], reverse=True))
        all_cust += [[now_str, period_label, str(start), str(end),
            r["rank"], r["pareto_zone"], r["customer_id"], r["name"], r["email"],
            r["segment"], round(r["net_sales"],2), r["pct_revenue"], r["cum_pct"],
            r["orders"], r["units"], r["first_order"], r["last_order"],
        ] for r in rows_sorted]

        # New vs Returning summary — the simple table the meeting asked for
        total_cust = sum(v["count"] for v in nvr_summary.values()) or 1
        total_rev  = sum(v["net_sales"] for v in nvr_summary.values()) or 1
        for seg, v in nvr_summary.items():
            all_nvr.append([
                now_str, period_label, str(start), str(end),
                seg.capitalize(),
                v["count"], round(v["net_sales"],2), v["orders"],
                round(v["count"]/total_cust*100,1),
                round(v["net_sales"]/total_rev*100,1),
            ])

        print(f"  → {len(orders)} orders | "
              f"New: {nvr_summary['new']['count']} cust ${nvr_summary['new']['net_sales']:,.0f} | "
              f"Returning: {nvr_summary['returning']['count']} cust ${nvr_summary['returning']['net_sales']:,.0f}")

    # Write all to Sheets
    print(f"\n  Writing to Google Sheets...")
    upsert_tab(sh, "pareto_products",    h_prod, all_prod, ["period","product_id"])
    upsert_tab(sh, "pareto_categories",  h_cat,  all_cat,  ["period","category"])
    upsert_tab(sh, "pareto_channels",    h_ch,   all_ch,   ["period","channel"])
    upsert_tab(sh, "pareto_customers",   h_cust, all_cust, ["period","customer_id"])
    upsert_tab(sh, "pareto_new_vs_ret",  h_nvr,  all_nvr,  ["period","segment"])

    print(f"\n{'='*60}")
    print(f"  ✓ Done — {len(periods)} periods processed")
    print(f"  Sheets: pareto_products (+ COGS + gross_profit),")
    print(f"          pareto_categories (+ COGS + gross_profit),")
    print(f"          pareto_channels, pareto_customers (+ segment),")
    print(f"          pareto_new_vs_ret (New vs Returning summary)")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()

