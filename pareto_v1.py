"""
PARETO PIPELINE v1.1 — Equestrian Labs / Corro
===============================================
Cambios v1.1 vs v1.0:
- FIXED: duplicación de órdenes. fetch_orders usaba financial_status como
  lista separada por comas — Shopify REST no soporta eso directamente,
  hacía múltiples requests y duplicaba. Ahora usa dos requests (paid/refunded)
  y deduplica por order id. Esperado ~8,800 ordenes en 2025 (no 18.2K).
- CHANGED: Pareto Zone A ahora se basa en Gross Profit (cum_gp_pct ≤ 80%)
  en lugar de Net Sales. Así los 403 productos que producen el 80% del GP
  se marcan como Zone A.
- ADDED: columna cum_gp_pct (acumulado de Gross Profit %) en pareto_products
  y pareto_categories.
- ADDED: columna cum_gs_pct (acumulado de Gross Sales %) — el frontend
  necesita ambos acumulados.
- NOTE: los productos se siguen ORDENANDO por net_sales (descendente),
  pero la zona se asigna por GP acumulado. Esto es lo que pidió Ceci:
  "ordenarlo por Net Sales para llegar al 80% del Gross Profit."

Run modes (sin cambios):
  python pareto_v1.py                     # all periods 2024 → today
  python pareto_v1.py --only q4_2025     # single quarter
  python pareto_v1.py --only full_year_2025
  python pareto_v1.py --only custom --start 2025-01-01 --end 2025-12-31

GitHub Actions secrets required:
  SHOPIFY_TOKEN_CORRO   GOOGLE_CREDENTIALS   SHEET_ID_CORRO
"""

import os, json, requests, gspread, argparse, calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
from collections import defaultdict
import pytz

TIMEZONE    = pytz.timezone("America/Bogota")
API_VERSION = "2024-10"
STORE_URL   = os.environ.get("SHOPIFY_STORE", "equestrian-labs.myshopify.com")
TOKEN       = os.environ.get("SHOPIFY_TOKEN_CORRO", "")
SHEET_ID    = os.environ.get("SHEET_ID_CORRO",
              "1NnH7Ln3HP9AuJ5ohxgvVk6A5BnG9_iz9WPC9SxaaidI")
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets",
               "https://www.googleapis.com/auth/drive"]
START_YEAR  = 2024

# ── PERIODS ───────────────────────────────────────────────────────
def last_day(y, m):
    return date(y, m, calendar.monthrange(y, m)[1])

def build_all_periods():
    today = datetime.now(TIMEZONE).date()
    periods = []
    for y in range(START_YEAR, today.year + 1):
        ys = date(y, 1, 1)
        ye = date(y, 12, 31) if y < today.year else today
        periods.append((ys, ye, f"full_year_{y}"))
        max_q = 4 if y < today.year else ((today.month - 1) // 3 + 1)
        for q in range(1, max_q + 1):
            qs = date(y, (q - 1) * 3 + 1, 1)
            qe = last_day(y, q * 3) if (y < today.year or q < max_q) else today
            periods.append((qs, qe, f"q{q}_{y}"))
    periods.sort(key=lambda x: x[0], reverse=True)
    return periods

def resolve_single(key, start_override=None, end_override=None):
    today = datetime.now(TIMEZONE).date()
    if key == "custom" and start_override and end_override:
        return date.fromisoformat(start_override), date.fromisoformat(end_override), "custom"
    if key.startswith("full_year_"):
        y = int(key.split("_")[-1])
        return date(y, 1, 1), (date(y, 12, 31) if y < today.year else today), key
    if key == "full_year":
        return date(today.year, 1, 1), today, f"full_year_{today.year}"
    if key.startswith("q") and "_" in key:
        parts = key[1:].split("_"); q, y = int(parts[0]), int(parts[1])
        qs = date(y, (q - 1) * 3 + 1, 1)
        cur_q = (today.month - 1) // 3 + 1
        qe = last_day(y, q * 3) if (y < today.year or q < cur_q) else today
        return qs, qe, key
    raise ValueError(f"Unknown period: {key}. Use full_year_YYYY, qN_YYYY, or custom.")

# ── SHOPIFY REST ──────────────────────────────────────────────────
import time

def shopify_get(endpoint, params):
    url = f"https://{STORE_URL}/admin/api/{API_VERSION}/{endpoint}"
    headers = {"X-Shopify-Access-Token": TOKEN}
    results = []
    while url:
        for attempt in range(8):
            try:
                r = requests.get(url, headers=headers, params=params, timeout=60)
            except requests.exceptions.ConnectionError as e:
                wait = min(2 ** attempt, 60)
                print(f"    connection error — retrying in {wait}s: {e}")
                time.sleep(wait)
                continue
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 2 ** attempt))
                print(f"    rate-limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code in (502, 503, 504):
                wait = min(2 ** attempt, 60)
                print(f"    {r.status_code} — retrying in {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            break
        else:
            r.raise_for_status()
        data = r.json()
        key = [k for k in data if k != "errors"][0]
        results.extend(data[key])
        link = r.headers.get("Link", ""); url = None; params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
        time.sleep(0.3)
    return results

# ── FIX v1.1: fetch_orders sin duplicación ────────────────────────
def fetch_orders(start, end):
    """
    FIXED v1.1: La versión anterior pasaba financial_status como lista
    separada por comas en un solo parámetro. Shopify REST acepta eso en
    algunos endpoints pero con paginación puede duplicar. Ahora hacemos
    DOS requests separados (paid+partially_paid y partially_refunded+refunded)
    y deduplicamos por order id.

    Resultado esperado: ~8,800 órdenes en 2025 (la mitad de los 18.2K anteriores).
    """
    print(f"  Fetching orders {start} → {end} (deduplication fix v1.1)...")

    seen_ids = set()
    all_orders = []

    # Batch 1: paid + partially_paid
    for status in ["paid,partially_paid", "partially_refunded,refunded"]:
        batch = shopify_get("orders.json", {
            "status": "any",
            "financial_status": status,
            "created_at_min": f"{start}T00:00:00-05:00",
            "created_at_max": f"{end}T23:59:59-05:00",
            "limit": 250,
            "fields": "id,name,created_at,subtotal_price,total_discounts,"
                      "source_name,tags,line_items,customer",
        })
        for o in batch:
            oid = o.get("id")
            if oid and oid not in seen_ids:
                seen_ids.add(oid)
                all_orders.append(o)

    print(f"  → {len(all_orders)} unique orders (after dedup)")
    return all_orders

def fetch_products_map():
    print("  Fetching product catalogue...")
    products = shopify_get("products.json", {"limit": 250, "fields": "id,title,vendor,product_type,variants"})
    vmap = {}
    for p in products:
        for v in p.get("variants", []):
            vmap[str(v["id"])] = {
                "product_id":        str(p["id"]),
                "product_title":     p.get("title", ""),
                "sku_parent":        v.get("sku", "").split("-")[0] if v.get("sku") else "",
                "vendor":            p.get("vendor", ""),
                "product_type":      p.get("product_type", "Uncategorized"),
                "inventory_item_id": str(v.get("inventory_item_id", "")),
            }
    print(f"  → {len(products)} products / {len(vmap)} variants")
    return vmap

def fetch_cogs_map():
    print("  Fetching COGS from inventory items...")
    all_variants = shopify_get("variants.json", {"limit": 250, "fields": "id,inventory_item_id"})
    iids = [str(v["inventory_item_id"]) for v in all_variants if v.get("inventory_item_id")]
    cost_map = {}
    for i in range(0, len(iids), 100):
        items = shopify_get("inventory_items.json", {
            "ids": ",".join(iids[i:i+100]), "limit": 100, "fields": "id,cost"
        })
        for item in items:
            cost_map[str(item["id"])] = float(item.get("cost") or 0)
    variant_cost = {}
    for v in all_variants:
        variant_cost[str(v["id"])] = cost_map.get(str(v.get("inventory_item_id", "")), 0.0)
    filled = sum(1 for c in variant_cost.values() if c > 0)
    print(f"  → {len(variant_cost)} variants, {filled} with COGS > 0")
    return variant_cost

def fetch_collections_map():
    print("  Fetching collections (titles)...")
    colls = {}
    for c in shopify_get("custom_collections.json", {"limit": 250, "fields": "id,title"}):
        colls[str(c["id"])] = c["title"]
    for c in shopify_get("smart_collections.json", {"limit": 250, "fields": "id,title"}):
        colls[str(c["id"])] = c["title"]
    print(f"  → {len(colls)} collection titles loaded")
    print("  Fetching all collects...")
    prod_colls = defaultdict(list)
    all_collects = shopify_get("collects.json", {"limit": 250, "fields": "collection_id,product_id"})
    for co in all_collects:
        cid = str(co.get("collection_id", ""))
        pid = str(co.get("product_id", ""))
        title = colls.get(cid)
        if title and pid and title not in prod_colls[pid]:
            prod_colls[pid].append(title)
    print(f"  → {len(prod_colls)} products mapped to collections")
    return prod_colls

# ── CHANNEL DETECTION ─────────────────────────────────────────────
def detect_channel(order):
    src  = (order.get("source_name") or "").lower().strip()
    tags = (order.get("tags") or "").lower()
    if "concierge" in tags or "concierge" in src:
        return "Concierge"
    if src == "pos" or "wellington" in tags:
        return "Wellington (POS)"
    return "Online (Ecom)"

def audit_concierge_tags(orders, sample=30):
    tag_counts = defaultdict(int)
    concierge_count = 0
    for o in orders:
        tags = (o.get("tags") or "").strip()
        src  = (o.get("source_name") or "").strip()
        key  = f"source={src!r:20} | tags={tags[:80]!r}" if (tags or src) else "(no tags)"
        tag_counts[key] += 1
        if "concierge" in tags.lower() or "concierge" in src.lower():
            concierge_count += 1
    print("\n  ── CONCIERGE TAG AUDIT ──────────────────────────────")
    print(f"  Orders matching 'concierge': {concierge_count} / {len(orders)}")
    for k, cnt in sorted(tag_counts.items(), key=lambda x: -x[1])[:sample]:
        print(f"    {cnt:>5}x  {k}")
    print("  ────────────────────────────────────────────────────\n")

# ── AGGREGATION ───────────────────────────────────────────────────
def build_pareto_products(orders, vmap, variant_cost):
    agg = defaultdict(lambda: {
        "product_id":"","product_title":"","product_type":"","vendor":"","sku_parent":"",
        "gross_sales":0.0,"discounts":0.0,"net_sales":0.0,"cogs":0.0,"gross_profit":0.0,
        "units":0,"orders":set(),"channels":defaultdict(float),
    })
    for order in orders:
        disc_total  = float(order.get("total_discounts",0) or 0)
        li_count    = len(order.get("line_items",[])) or 1
        disc_per_li = disc_total / li_count
        for li in order.get("line_items",[]):
            vid   = str(li.get("variant_id") or "")
            info  = vmap.get(vid,{"product_id":str(li.get("product_id","")),"product_title":li.get("title","Unknown"),"product_type":"Uncategorized","vendor":"","sku_parent":""})
            pid   = info["product_id"]
            qty   = int(li.get("quantity",0) or 0)
            price = float(li.get("price",0) or 0) * qty
            net   = max(price - disc_per_li, 0)
            cost  = variant_cost.get(vid, 0.0) * qty
            row   = agg[pid]
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
    agg = defaultdict(lambda: {"gross_sales":0.0,"discounts":0.0,"net_sales":0.0,"cogs":0.0,"gross_profit":0.0,"units":0,"orders":set()})
    for order in orders:
        disc_total  = float(order.get("total_discounts",0) or 0)
        li_count    = len(order.get("line_items",[])) or 1
        disc_per_li = disc_total / li_count
        for li in order.get("line_items",[]):
            vid   = str(li.get("variant_id") or "")
            info  = vmap.get(vid,{"product_id":"","product_type":"Uncategorized"})
            pid   = info["product_id"]
            cats  = prod_colls.get(pid,[])
            cat   = cats[0] if cats else (info.get("product_type") or "Uncategorized")
            qty   = int(li.get("quantity",0) or 0)
            price = float(li.get("price",0) or 0) * qty
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
    agg = defaultdict(lambda: {"gross_sales":0.0,"discounts":0.0,"net_sales":0.0,"units":0,"orders":set()})
    for order in orders:
        ch   = detect_channel(order)
        disc = float(order.get("total_discounts",0) or 0)
        sub  = float(order.get("subtotal_price",0) or 0)
        units = sum(int(li.get("quantity",0) or 0) for li in order.get("line_items",[]))
        agg[ch]["gross_sales"] += sub + disc
        agg[ch]["discounts"]   += disc
        agg[ch]["net_sales"]   += sub
        agg[ch]["units"]       += units
        agg[ch]["orders"].add(order["id"])
    return agg

def build_pareto_customers(orders, period_start):
    agg = defaultdict(lambda: {"customer_id":"","name":"","email":"","gross_sales":0.0,"net_sales":0.0,"orders":0,"units":0,"first_order":"9999-99-99","last_order":"0000-00-00","is_new":True})
    for order in orders:
        c    = order.get("customer") or {}
        cid  = str(c.get("id", f"guest_{order['id']}"))
        sub  = float(order.get("subtotal_price",0) or 0)
        disc = float(order.get("total_discounts",0) or 0)
        units = sum(int(li.get("quantity",0) or 0) for li in order.get("line_items",[]))
        d    = order.get("created_at","")[:10]
        is_new = (c.get("created_at") or d)[:10] >= str(period_start)
        row  = agg[cid]
        row["customer_id"]  = cid
        row["name"]         = f"{c.get('first_name','')} {c.get('last_name','')}".strip() or "Guest"
        row["email"]        = c.get("email","")
        row["gross_sales"] += sub + disc
        row["net_sales"]   += sub
        row["orders"]      += 1
        row["units"]       += units
        row["is_new"]       = is_new
        if d < row["first_order"]: row["first_order"] = d
        if d > row["last_order"]:  row["last_order"]  = d
    return agg

def build_new_vs_returning(agg_cust):
    s = {"new":{"count":0,"net_sales":0.0,"orders":0},"returning":{"count":0,"net_sales":0.0,"orders":0}}
    for d in agg_cust.values():
        k = "new" if d["is_new"] else "returning"
        s[k]["count"] += 1; s[k]["net_sales"] += d["net_sales"]; s[k]["orders"] += d["orders"]
    return s

# ── PARETO COLUMNS — v1.1 ─────────────────────────────────────────
def add_pareto_cols(rows, revenue_key="net_sales", gp_key="gross_profit"):
    """
    v1.1 CHANGES:
    - pct_revenue y cum_pct siguen siendo sobre net_sales (para ranking)
    - ADDED: pct_gs y cum_gs_pct sobre gross_sales
    - ADDED: pct_gp y cum_gp_pct sobre gross_profit
    - pareto_zone ahora se asigna por cum_gp_pct (Ceci quiere 80% GP):
        Zone A: cum_gp_pct <= 80%
        Zone B: cum_gp_pct <= 95%
        Zone C: resto

    Los productos se ordenan por net_sales (desc) antes de llamar esta función.
    Así "ordenamos por Net Sales para llegar al 80% del Gross Profit."
    """
    total_rev = sum(r.get(revenue_key, 0) for r in rows) or 1
    total_gs  = sum(r.get("gross_sales", 0) for r in rows) or 1
    total_gp  = sum(r.get(gp_key, 0) for r in rows) or 1

    cum_rev = 0.0
    cum_gs  = 0.0
    cum_gp  = 0.0

    for i, r in enumerate(rows):
        cum_rev += r.get(revenue_key, 0)
        cum_gs  += r.get("gross_sales", 0)
        cum_gp  += r.get(gp_key, 0)

        r["rank"]        = i + 1
        # net_sales %
        r["pct_revenue"] = round(r.get(revenue_key, 0) / total_rev * 100, 2)
        r["cum_pct"]     = round(cum_rev / total_rev * 100, 2)
        # gross_sales % — ADDED
        r["pct_gs"]      = round(r.get("gross_sales", 0) / total_gs * 100, 2)
        r["cum_gs_pct"]  = round(cum_gs / total_gs * 100, 2)
        # gross_profit % — ADDED
        r["pct_gp"]      = round(r.get(gp_key, 0) / total_gp * 100, 2)
        r["cum_gp_pct"]  = round(cum_gp / total_gp * 100, 2)
        # CHANGED: zone based on GP cumulative, not revenue cumulative
        r["pareto_zone"] = "A" if r["cum_gp_pct"] <= 80 else ("B" if r["cum_gp_pct"] <= 95 else "C")

    return rows, total_rev

# ── SHEETS ────────────────────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

def sheets_call(fn, *args, **kwargs):
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, "response") else 0
            if status == 429 or status >= 500:
                wait = min(15 * (attempt + 1), 90)
                print(f"    Sheets API {status} — waiting {wait}s...")
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            if attempt < 3:
                print(f"    Sheets error, retrying: {e}")
                time.sleep(10)
                continue
            raise
    raise RuntimeError("Sheets call failed after 8 attempts")

def upsert_tab(sh, tab_name, headers, new_rows, key_cols):
    BATCH_SIZE   = 500
    SLEEP_BATCH  = 2.0
    SLEEP_TAB    = 5.0

    try:    ws = sh.worksheet(tab_name)
    except: ws = sheets_call(sh.add_worksheet, tab_name, rows=max(5000, len(new_rows)+100), cols=len(headers)+2)
    time.sleep(2)

    existing = {}
    try:
        vals = sheets_call(ws.get_all_values)
        if len(vals) >= 2:
            ex_h = vals[0]
            for r in vals[1:]:
                m = {ex_h[i]: (r[i] if i < len(r) else "") for i in range(len(ex_h))}
                k = tuple(str(m.get(c,"")).strip() for c in key_cols)
                if any(k): existing[k] = [m.get(h,"") for h in headers]
    except Exception as e:
        print(f"    Warning reading {tab_name}: {e}")

    for row in new_rows:
        m = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        k = tuple(str(m.get(c,"")).strip() for c in key_cols)
        existing[k] = row

    def sort_key(r):
        m = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        return (str(m.get("period_start","") or ""), str(m.get("period","") or ""), int(m.get("rank",0) or 0))

    merged = sorted(existing.values(), key=sort_key)
    all_data = [headers]
    for r in merged:
        clean = []
        for v in r:
            if v is None: clean.append("")
            elif isinstance(v, float) and (v != v): clean.append("")
            elif hasattr(v, 'strftime'): clean.append(v.strftime("%Y-%m-%d"))
            else: clean.append(str(v) if not isinstance(v,(int,float,bool)) else v)
        all_data.append(clean)

    total_rows = len(all_data)
    if total_rows > ws.row_count or len(headers) > ws.col_count:
        sheets_call(ws.resize, rows=total_rows+50, cols=len(headers)+2)
        time.sleep(1)

    sheets_call(ws.clear)
    time.sleep(1)

    written = 0
    for i in range(0, len(all_data), BATCH_SIZE):
        batch = all_data[i:i+BATCH_SIZE]
        sheets_call(ws.append_rows, batch, value_input_option="RAW", insert_data_option="INSERT_ROWS")
        written += len(batch)
        print(f"    {tab_name}: {written}/{total_rows} rows written...")
        if i+BATCH_SIZE < len(all_data): time.sleep(SLEEP_BATCH)

    time.sleep(SLEEP_TAB)
    print(f"    ok {tab_name}: {len(new_rows)} new rows, {len(merged)} total")

# ── MAIN ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default=None)
    parser.add_argument("--start"); parser.add_argument("--end")
    args = parser.parse_args()

    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    periods = [resolve_single(args.only, args.start, args.end)] if args.only else build_all_periods()

    print(f"\n{'='*60}")
    print(f"  PARETO PIPELINE v1.1 — Corro ({STORE_URL})")
    print(f"  Mode  : {'single ('+args.only+')' if args.only else 'full backfill — '+str(len(periods))+' periods'}")
    print(f"  Sheet : {SHEET_ID}")
    print(f"  Fix   : order deduplication + GP-based zones")
    print(f"{'='*60}\n")

    vmap         = fetch_products_map()
    variant_cost = fetch_cogs_map()
    prod_colls   = fetch_collections_map()
    sh           = get_gc().open_by_key(SHEET_ID)

    # CHANGED: headers include cum_gs_pct and cum_gp_pct
    h_prod = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "product_title","product_type","vendor","sku_parent","product_id",
              "gross_sales","pct_gs","cum_gs_pct",
              "net_sales","pct_revenue","cum_pct",
              "cogs","gross_profit","gp_pct","pct_gp","cum_gp_pct",
              "units","orders","top_channel"]
    h_cat  = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "category",
              "gross_sales","pct_gs","cum_gs_pct",
              "net_sales","pct_revenue","cum_pct",
              "cogs","gross_profit","gp_pct","pct_gp","cum_gp_pct",
              "units","orders"]
    h_ch   = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "channel","gross_sales","discounts","net_sales","pct_revenue","cum_pct","units","orders"]
    h_cust = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "customer_id","name","email","segment","net_sales","pct_revenue","cum_pct",
              "orders","units","first_order","last_order"]
    h_nvr  = ["updated_at","period","period_start","period_end",
              "segment","customers","net_sales","orders","pct_customers","pct_revenue"]

    all_prod, all_cat, all_ch, all_cust, all_nvr = [], [], [], [], []
    first_run = True

    for idx, (start, end, pk) in enumerate(periods):
        print(f"\n[{idx+1}/{len(periods)}] {pk}  ({start} → {end})")
        orders = fetch_orders(start, end)
        if not orders:
            print("  ⚠ No orders — skipping"); continue

        if first_run:
            audit_concierge_tags(orders)
            first_run = False

        ap = build_pareto_products(orders, vmap, variant_cost)
        ac = build_pareto_categories(orders, vmap, prod_colls, variant_cost)
        ah = build_pareto_channels(orders)
        au = build_pareto_customers(orders, start)
        nvr = build_new_vs_returning(au)

        # Products — sorted by net_sales desc, zones by GP cumulative
        raws = []
        for pid, d in ap.items():
            gp = round(d["gross_profit"]/d["net_sales"]*100,1) if d["net_sales"] else 0
            raws.append({
                "net_sales":d["net_sales"],"gross_sales":d["gross_sales"],"discounts":d["discounts"],
                "units":d["units"],"orders":len(d["orders"]),"product_id":pid,
                "product_title":d["product_title"],"product_type":d["product_type"],
                "vendor":d["vendor"],"sku_parent":d["sku_parent"],
                "cogs":d["cogs"],"gross_profit":d["gross_profit"],"gp_pct":gp,
                "top_channel":max(d["channels"],key=d["channels"].get) if d["channels"] else ""
            })
        # CHANGED: sorted by net_sales, zones assigned by GP cumulative
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
        all_prod += [[
            now_str, pk, str(start), str(end),
            r["rank"], r["pareto_zone"],
            r["product_title"], r["product_type"], r["vendor"], r["sku_parent"], r["product_id"],
            round(r["gross_sales"],2), r["pct_gs"], r["cum_gs_pct"],
            round(r["net_sales"],2),   r["pct_revenue"], r["cum_pct"],
            round(r["cogs"],2), round(r["gross_profit"],2), r["gp_pct"], r["pct_gp"], r["cum_gp_pct"],
            r["units"], r["orders"], r["top_channel"]
        ] for r in rs]

        # Categories
        raws = []
        for cat, d in ac.items():
            gp = round(d["gross_profit"]/d["net_sales"]*100,1) if d["net_sales"] else 0
            raws.append({
                "category":cat,"net_sales":d["net_sales"],"gross_sales":d["gross_sales"],
                "discounts":d["discounts"],"units":d["units"],"orders":len(d["orders"]),
                "cogs":d["cogs"],"gross_profit":d["gross_profit"],"gp_pct":gp
            })
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
        all_cat += [[
            now_str, pk, str(start), str(end),
            r["rank"], r["pareto_zone"], r["category"],
            round(r["gross_sales"],2), r["pct_gs"], r["cum_gs_pct"],
            round(r["net_sales"],2),   r["pct_revenue"], r["cum_pct"],
            round(r["cogs"],2), round(r["gross_profit"],2), r["gp_pct"], r["pct_gp"], r["cum_gp_pct"],
            r["units"], r["orders"]
        ] for r in rs]

        # Channels
        raws = [{"channel":ch,"net_sales":d["net_sales"],"gross_sales":d["gross_sales"],
                 "discounts":d["discounts"],"units":d["units"],"orders":len(d["orders"])} for ch,d in ah.items()]
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
        all_ch += [[
            now_str, pk, str(start), str(end),
            r["rank"], r["pareto_zone"], r["channel"],
            round(r["gross_sales"],2), round(r["discounts"],2), round(r["net_sales"],2),
            r["pct_revenue"], r["cum_pct"], r["units"], r["orders"]
        ] for r in rs]

        # Customers
        raws = [dict(d, segment="New" if d["is_new"] else "Returning") for d in au.values()]
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
        all_cust += [[
            now_str, pk, str(start), str(end),
            r["rank"], r["pareto_zone"],
            r["customer_id"], r["name"], r["email"], r["segment"],
            round(r["net_sales"],2), r["pct_revenue"], r["cum_pct"],
            r["orders"], r["units"], r["first_order"], r["last_order"]
        ] for r in rs]

        # New vs Returning
        tc = sum(v["count"] for v in nvr.values()) or 1
        tr = sum(v["net_sales"] for v in nvr.values()) or 1
        for seg, v in nvr.items():
            all_nvr.append([
                now_str, pk, str(start), str(end), seg.capitalize(),
                v["count"], round(v["net_sales"],2), v["orders"],
                round(v["count"]/tc*100,1), round(v["net_sales"]/tr*100,1)
            ])

        za_count = sum(1 for r in rs if r["pareto_zone"]=="A")
        print(f"  → {len(orders)} orders | Zone A: {za_count} products (GP-based) | "
              f"New: {nvr['new']['count']} (${nvr['new']['net_sales']:,.0f}) | "
              f"Returning: {nvr['returning']['count']} (${nvr['returning']['net_sales']:,.0f})")

    print(f"\n  Writing to Google Sheets...")
    upsert_tab(sh, "pareto_products",   h_prod, all_prod, ["period","product_id"])
    upsert_tab(sh, "pareto_categories", h_cat,  all_cat,  ["period","category"])
    upsert_tab(sh, "pareto_channels",   h_ch,   all_ch,   ["period","channel"])
    upsert_tab(sh, "pareto_customers",  h_cust, all_cust, ["period","customer_id"])
    upsert_tab(sh, "pareto_new_vs_ret", h_nvr,  all_nvr,  ["period","segment"])

    print(f"\n{'='*60}")
    print(f"  ✓ Done v1.1 — {len(periods)} periods | Sheet: {SHEET_ID}")
    print(f"  Orders deduplication fix applied")
    print(f"  Pareto zones based on Gross Profit cumulative")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
