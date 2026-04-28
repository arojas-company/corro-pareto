"""
PARETO PIPELINE v1.5.1 — Equestrian Labs / Corro
=================================================
FIX v1.5.1: upsert_tab reescrita con estrategia append-only.
  - Ya NO hace clear + rewrite de todo el sheet en cada run.
  - Lee las filas existentes, actualiza solo las del período actual in-place.
  - Append solo filas nuevas (claves que no existían).
  - Evita el crash "10M cells limit" en backfills de 13 períodos.
  - Primer run (sheet vacío): inicializa normalmente con clear+write.

Cambios v1.3: map_product_type_to_category() → 7 categorías = Shopify order tags.
Cambios v1.2: ordenamiento por GP DESC, categorías por product_type.
Cambios v1.1: fix duplicación órdenes, Zone A basado en cum_gp_pct ≤ 80%.

Run modes:
  python pareto_v1.py                       # full backfill 2024 → today
  python pareto_v1.py --only q4_2025        # single quarter
  python pareto_v1.py --only full_year_2025
  python pareto_v1.py --only custom --start 2025-01-01 --end 2025-12-31

GitHub Actions secrets: SHOPIFY_TOKEN_CORRO  GOOGLE_CREDENTIALS  SHEET_ID_CORRO
"""

import os, json, requests, gspread, argparse, calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from collections import defaultdict
import pytz, time

TIMEZONE    = pytz.timezone("America/Bogota")
API_VERSION = "2024-10"
STORE_URL   = os.environ.get("SHOPIFY_STORE", "equestrian-labs.myshopify.com")
TOKEN       = os.environ.get("SHOPIFY_TOKEN_CORRO", "")
SHEET_ID    = os.environ.get("SHEET_ID_CORRO",
              "1NnH7Ln3HP9AuJ5ohxgvVk6A5BnG9_iz9WPC9SxaaidI")
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets",
               "https://www.googleapis.com/auth/drive"]
START_YEAR  = 2024

# ── PERIODS ──────────────────────────────────────────────────────
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

# ── SHOPIFY REST ─────────────────────────────────────────────────
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
                time.sleep(wait); continue
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 2 ** attempt))
                print(f"    rate-limited — waiting {wait}s..."); time.sleep(wait); continue
            if r.status_code in (502, 503, 504):
                wait = min(2 ** attempt, 60)
                print(f"    {r.status_code} — retrying in {wait}s..."); time.sleep(wait); continue
            r.raise_for_status(); break
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

def fetch_orders(start, end):
    print(f"  Fetching orders {start} → {end} (deduplication fix v1.1)...")
    seen_ids = set(); all_orders = []
    for status in ["paid,partially_paid", "partially_refunded,refunded"]:
        batch = shopify_get("orders.json", {
            "status": "any", "financial_status": status,
            "created_at_min": f"{start}T00:00:00-05:00",
            "created_at_max": f"{end}T23:59:59-05:00",
            "limit": 250,
            "fields": "id,name,created_at,subtotal_price,total_discounts,source_name,tags,line_items,customer",
        })
        for o in batch:
            oid = o.get("id")
            if oid and oid not in seen_ids:
                seen_ids.add(oid); all_orders.append(o)
    print(f"  → {len(all_orders)} unique orders (after dedup)")
    return all_orders

def shopify_graphql(query, variables=None):
    url = f"https://{STORE_URL}/admin/api/{API_VERSION}/graphql.json"
    headers = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}
    payload = {"query": query}
    if variables: payload["variables"] = variables
    for attempt in range(8):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=60)
        except requests.exceptions.ConnectionError as e:
            time.sleep(min(2**attempt, 60)); continue
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 2**attempt))); continue
        if r.status_code in (502, 503, 504):
            time.sleep(min(2**attempt, 60)); continue
        r.raise_for_status()
        data = r.json()
        errors = data.get("errors") or []
        if errors and any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors):
            time.sleep(min(2**attempt, 60)); continue
        return data
    raise RuntimeError("GraphQL call failed after 8 attempts")

def fetch_products_map():
    print("  Fetching product catalogue via GraphQL...")
    QUERY = """
    query fetchVariants($cursor: String) {
      productVariants(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges { node { id sku product { id title vendor productType } } }
      }
    }"""
    vmap = {}; title_map = {}; cursor = None
    while True:
        data  = shopify_graphql(QUERY, {"cursor": cursor})
        pv    = data.get("data", {}).get("productVariants", {})
        for edge in pv.get("edges", []):
            node    = edge["node"]
            vid     = str(node["id"].split("/")[-1])
            product = node.get("product") or {}
            pid     = str((product.get("id") or "").split("/")[-1])
            sku     = node.get("sku") or ""
            title   = product.get("title", "")
            ptype   = product.get("productType") or "Uncategorized"
            vendor  = product.get("vendor", "")
            vmap[vid] = {"product_id": pid, "product_title": title,
                         "sku_parent": sku.split("-")[0] if sku else "",
                         "vendor": vendor, "product_type": ptype}
            title_map[title.lower()] = {"product_id": pid, "product_type": ptype, "vendor": vendor}
        page_info = pv.get("pageInfo", {})
        if not page_info.get("hasNextPage"): break
        cursor = page_info["endCursor"]; time.sleep(0.3)
    print(f"  → {len(vmap)} variants  |  {len(title_map)} unique product titles")
    return vmap, title_map

def fetch_shopifyql_sales(start, end):
    print(f"  Fetching ShopifyQL sales {start} → {end}...")
    GQL = """
    query shopifyqlSales($q: String!) {
      shopifyqlQuery(query: $q) {
        tableData { columns { name dataType } rows }
        parseErrors
      }
    }"""

    def run_shopifyql(q):
        url = f"https://{STORE_URL}/admin/api/2026-01/graphql.json"
        headers = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}
        for attempt in range(8):
            try:
                r = requests.post(url, headers=headers, json={"query": GQL, "variables": {"q": q}}, timeout=60)
            except requests.exceptions.ConnectionError:
                time.sleep(min(2**attempt, 60)); continue
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 2**attempt))); continue
            if r.status_code in (502, 503, 504):
                time.sleep(min(2**attempt, 60)); continue
            r.raise_for_status(); d = r.json()
            if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in (d.get("errors") or [])):
                time.sleep(min(2**attempt, 60)); continue
            break
        else:
            return [], [], [{"code": "FAILED", "message": "max retries"}]
        raw_errors  = d.get("errors")
        shopifyql_q = (d.get("data") or {}).get("shopifyqlQuery")
        if raw_errors: print(f"  ✗ GraphQL errors: {raw_errors}")
        if shopifyql_q is None:
            print(f"  ✗ shopifyqlQuery null — missing read_reports scope")
            return [], [], [{"code": "NULL_RESPONSE", "message": "null"}]
        raw_pe = shopifyql_q.get("parseErrors")
        errs   = [{"code": "PARSE_ERROR", "message": str(raw_pe)}] if raw_pe else []
        table  = shopifyql_q.get("tableData") or {}
        cols   = [c["name"] for c in (table.get("columns") or [])]
        rows   = table.get("rows") or []
        if errs: print(f"  ⚠ parseErrors: {raw_pe}")
        print(f"    → cols={cols}  rows={len(rows)}")
        return cols, rows, errs

    q_main = (f"FROM sales SHOW gross_profit, gross_sales, net_sales, orders, net_items_sold "
              f"GROUP BY product_title, product_vendor SINCE {start} UNTIL {end} ORDER BY gross_profit DESC")
    cols, rows, errs = run_shopifyql(q_main)
    if errs:
        print(f"  ⚠ retrying without product_vendor:")
        for e in errs: print(f"    {e.get('code')} — {e.get('message')}")
        q_fallback = (f"FROM sales SHOW gross_profit, gross_sales, net_sales, orders, net_items_sold "
                      f"GROUP BY product_title SINCE {start} UNTIL {end} ORDER BY gross_profit DESC")
        cols, rows, errs2 = run_shopifyql(q_fallback)
        if errs2:
            for e in errs2: print(f"  ✗ fallback failed: {e.get('code')} — {e.get('message')}")
            return []
    if not cols:
        print("  ⚠ ShopifyQL returned no columns"); return []

    top_channel_map = {}
    try:
        q_ch = (f"FROM sales SHOW net_sales GROUP BY product_title, sales_channel "
                f"SINCE {start} UNTIL {end} ORDER BY net_sales DESC")
        ch_cols, ch_rows, ch_errs = run_shopifyql(q_ch)
        if not ch_errs and ch_cols:
            ch_agg = defaultdict(dict)
            for row in ch_rows:
                if not isinstance(row, dict): row = dict(zip(ch_cols, row)) if isinstance(row, list) else {}
                t = (row.get("product_title") or "").strip()
                ch = (row.get("sales_channel") or "").strip()
                ns = float(row.get("net_sales") or 0)
                if t and ch: ch_agg[t][ch] = ch_agg[t].get(ch, 0) + ns
            for t, channels in ch_agg.items(): top_channel_map[t] = max(channels, key=channels.get)
            print(f"  → channel query: {len(top_channel_map)} products mapped")
        elif ch_errs:
            print(f"  ⚠ channel query error (non-fatal): {ch_errs[0].get('message')}")
    except Exception as e:
        print(f"  ⚠ channel query failed (non-fatal): {e}")

    results = []; seen = set()
    for row in rows:
        if not isinstance(row, dict): row = dict(zip(cols, row)) if isinstance(row, list) else {}
        title  = (row.get("product_title") or "").strip()
        vendor = (row.get("product_vendor") or "").strip()
        if not title: continue
        if title in seen:
            for r in results:
                if r["product_title"] == title:
                    r["gross_profit"] += float(row.get("gross_profit") or 0)
                    r["gross_sales"]  += float(row.get("gross_sales")  or 0)
                    r["net_sales"]    += float(row.get("net_sales")    or 0)
                    r["orders"]       += int(row.get("orders")         or 0)
                    r["units"]        += int(row.get("net_items_sold") or 0)
                    break
            continue
        seen.add(title)
        ns = float(row.get("net_sales") or 0)
        gp = float(row.get("gross_profit") or 0)
        gs = float(row.get("gross_sales") or 0) or ns
        results.append({"product_title": title, "vendor": vendor, "gross_profit": gp,
            "gross_sales": gs, "discounts": max(gs - ns, 0), "net_sales": ns,
            "orders": int(row.get("orders") or 0), "units": int(row.get("net_items_sold") or 0),
            "top_channel": top_channel_map.get(title, "")})
    print(f"  → {len(results)} products from ShopifyQL")
    return results

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
    for co in shopify_get("collects.json", {"limit": 250, "fields": "collection_id,product_id"}):
        cid = str(co.get("collection_id", "")); pid = str(co.get("product_id", ""))
        title = colls.get(cid)
        if title and pid and title not in prod_colls[pid]: prod_colls[pid].append(title)
    print(f"  → {len(prod_colls)} products mapped to collections")
    return prod_colls

def detect_channel(order):
    src  = (order.get("source_name") or "").lower().strip()
    tags = (order.get("tags") or "").lower()
    if "concierge" in tags or "concierge" in src: return "Concierge"
    if src == "pos" or "wellington" in tags: return "Wellington (POS)"
    return "Online (Ecom)"

def audit_concierge_tags(orders, sample=30):
    tag_counts = defaultdict(int); concierge_count = 0
    for o in orders:
        tags = (o.get("tags") or "").strip(); src = (o.get("source_name") or "").strip()
        key  = f"source={src!r:20} | tags={tags[:80]!r}" if (tags or src) else "(no tags)"
        tag_counts[key] += 1
        if "concierge" in tags.lower() or "concierge" in src.lower(): concierge_count += 1
    print("\n  ── CONCIERGE TAG AUDIT ──────────────────────────────")
    print(f"  Orders matching 'concierge': {concierge_count} / {len(orders)}")
    for k, cnt in sorted(tag_counts.items(), key=lambda x: -x[1])[:sample]:
        print(f"    {cnt:>5}x  {k}")
    print("  ────────────────────────────────────────────────────\n")

def map_product_type_to_category(raw_type):
    if not raw_type: return "Uncategorized"
    t = raw_type.strip().lower()
    if any(k in t for k in ["apparel","breeches","hat","shirt","schooling","leather handbag","handbag","boot bag","rider","belt"]): return "Rider"
    if any(k in t for k in ["bridle","rein","bridle accessories","saddle pad","all purpose","hunter/jumper","dressage saddle","half pad","stirrup","dressage bridle","girth","breastplate","martingale","bit"]): return "Tack_Equipment"
    if any(k in t for k in ["blanket","sheet","fly wrap","fly rug","cooler","neck cover","hood","turnout","stable rug","horsewear"]): return "Horsewear"
    if any(k in t for k in ["detangler","shampoo","grooming","brush","comb","body brace","skin & hair","fungal","spray"]): return "Grooming_Tools"
    if any(k in t for k in ["supplement","electrolyte","vitamin","probiotic","omega","joint","performance"]): return "Supplements"
    if any(k in t for k in ["hoof care","poultice","liniment","relief","wound","ointment","pharmaceutical","therapeutic","ulcer","gastric","back on track","dog","personal care"]): return "Horse_Health"
    if any(k in t for k in ["stall mat","stall & trailer","trailer cleaning","stall","stable supply","cleaning","bag","cavali club","spring 2024","special edition","_elite_gift","elite_gift"]): return "StableSupplies_Collection"
    if "accessories" in t: return "Rider"
    return "Uncategorized"

def build_pareto_products(shopifyql_rows, title_map, orders):
    channel_agg = defaultdict(lambda: defaultdict(float))
    for order in orders:
        ch = detect_channel(order)
        disc_total  = float(order.get("total_discounts", 0) or 0)
        li_count    = len(order.get("line_items", [])) or 1
        disc_per_li = disc_total / li_count
        for li in order.get("line_items", []):
            pid   = str(li.get("product_id") or "")
            price = float(li.get("price", 0) or 0) * int(li.get("quantity", 0) or 0)
            net   = max(price - disc_per_li, 0)
            if pid: channel_agg[pid][ch] += net
    agg = {}
    for row in shopifyql_rows:
        title  = row["product_title"]
        meta   = title_map.get(title.lower(), {})
        pid    = meta.get("product_id", "")
        ptype  = meta.get("product_type", "Uncategorized")
        vendor = row.get("vendor") or meta.get("vendor", "")
        top_ch = row.get("top_channel") or (max(channel_agg[pid], key=channel_agg[pid].get) if channel_agg.get(pid) else "")
        gp = row["gross_profit"]; ns = row["net_sales"]
        agg[title] = {"product_id": pid, "product_title": title, "product_type": ptype,
            "vendor": vendor, "sku_parent": "",
            "gross_sales": row.get("gross_sales", ns),
            "discounts": row.get("discounts") or max(row.get("gross_sales", ns) - ns, 0),
            "net_sales": ns, "cogs": max(ns - gp, 0), "gross_profit": gp,
            "gp_pct": round(gp / ns * 100, 1) if ns else 0.0,
            "units": row["units"], "orders": row["orders"], "top_channel": top_ch}
    return agg

def build_pareto_categories(shopifyql_rows, title_map):
    agg = defaultdict(lambda: {"gross_sales":0.0,"discounts":0.0,"net_sales":0.0,"cogs":0.0,"gross_profit":0.0,"units":0,"orders":0})
    for row in shopifyql_rows:
        meta = title_map.get(row["product_title"].lower(), {})
        cat  = map_product_type_to_category(meta.get("product_type", "Uncategorized"))
        ns = row["net_sales"]; gp = row["gross_profit"]
        agg[cat]["gross_sales"]  += row["gross_sales"]
        agg[cat]["discounts"]    += max(row["gross_sales"] - ns, 0)
        agg[cat]["net_sales"]    += ns
        agg[cat]["cogs"]         += max(ns - gp, 0)
        agg[cat]["gross_profit"] += gp
        agg[cat]["units"]        += row["units"]
        agg[cat]["orders"]       += row["orders"]
    return agg

def build_pareto_channels(orders):
    agg = defaultdict(lambda: {"gross_sales":0.0,"discounts":0.0,"net_sales":0.0,"units":0,"orders":set()})
    for order in orders:
        ch = detect_channel(order)
        disc = float(order.get("total_discounts",0) or 0)
        sub  = float(order.get("subtotal_price",0) or 0)
        units = sum(int(li.get("quantity",0) or 0) for li in order.get("line_items",[]))
        agg[ch]["gross_sales"] += sub + disc; agg[ch]["discounts"] += disc
        agg[ch]["net_sales"] += sub; agg[ch]["units"] += units; agg[ch]["orders"].add(order["id"])
    return agg

def build_pareto_customers(orders, period_start):
    agg = defaultdict(lambda: {"customer_id":"","name":"","email":"","gross_sales":0.0,"net_sales":0.0,"orders":0,"units":0,"first_order":"9999-99-99","last_order":"0000-00-00","is_new":True})
    for order in orders:
        c = order.get("customer") or {}; cid = str(c.get("id", f"guest_{order['id']}"))
        sub = float(order.get("subtotal_price",0) or 0); disc = float(order.get("total_discounts",0) or 0)
        units = sum(int(li.get("quantity",0) or 0) for li in order.get("line_items",[]))
        d = order.get("created_at","")[:10]; is_new = (c.get("created_at") or d)[:10] >= str(period_start)
        row = agg[cid]
        row["customer_id"] = cid; row["name"] = f"{c.get('first_name','')} {c.get('last_name','')}".strip() or "Guest"
        row["email"] = c.get("email",""); row["gross_sales"] += sub + disc; row["net_sales"] += sub
        row["orders"] += 1; row["units"] += units; row["is_new"] = is_new
        if d < row["first_order"]: row["first_order"] = d
        if d > row["last_order"]:  row["last_order"]  = d
    return agg

def build_new_vs_returning(agg_cust):
    s = {"new":{"count":0,"net_sales":0.0,"orders":0},"returning":{"count":0,"net_sales":0.0,"orders":0}}
    for d in agg_cust.values():
        k = "new" if d["is_new"] else "returning"
        s[k]["count"] += 1; s[k]["net_sales"] += d["net_sales"]; s[k]["orders"] += d["orders"]
    return s

def add_pareto_cols(rows, revenue_key="net_sales", gp_key="gross_profit"):
    total_rev = sum(r.get(revenue_key, 0) for r in rows) or 1
    total_gs  = sum(r.get("gross_sales", 0) for r in rows) or 1
    total_gp  = sum(r.get(gp_key, 0) for r in rows) or 1
    cum_rev = cum_gs = cum_gp = 0.0
    for i, r in enumerate(rows):
        cum_rev += r.get(revenue_key, 0); cum_gs += r.get("gross_sales", 0); cum_gp += r.get(gp_key, 0)
        r["rank"]        = i + 1
        r["pct_revenue"] = round(r.get(revenue_key, 0) / total_rev * 100, 2)
        r["cum_pct"]     = round(cum_rev / total_rev * 100, 2)
        r["pct_gs"]      = round(r.get("gross_sales", 0) / total_gs * 100, 2)
        r["cum_gs_pct"]  = round(cum_gs / total_gs * 100, 2)
        r["pct_gp"]      = round(r.get(gp_key, 0) / total_gp * 100, 2)
        r["cum_gp_pct"]  = round(cum_gp / total_gp * 100, 2)
        r["pareto_zone"] = "A" if r["cum_gp_pct"] <= 80 else ("B" if r["cum_gp_pct"] <= 95 else "C")
    return rows, total_rev

# ── SHEETS ───────────────────────────────────────────────────────
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
                print(f"    Sheets API {status} — waiting {wait}s..."); time.sleep(wait); continue
            raise
        except Exception as e:
            if attempt < 3: print(f"    Sheets error, retrying: {e}"); time.sleep(10); continue
            raise
    raise RuntimeError("Sheets call failed after 8 attempts")

def _clean_row(row):
    out = []
    for v in row:
        if v is None: out.append("")
        elif isinstance(v, float) and v != v: out.append("")
        elif hasattr(v, "strftime"): out.append(v.strftime("%Y-%m-%d"))
        elif not isinstance(v, (int, float, bool)): out.append(str(v))
        else: out.append(v)
    return out

def _batch_append(ws, rows, batch_size, sleep_batch, tab_name):
    written = 0; total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        sheets_call(ws.append_rows, batch, value_input_option="RAW", insert_data_option="INSERT_ROWS")
        written += len(batch)
        print(f"    {tab_name}: {written}/{total} rows appended...")
        if i + batch_size < total: time.sleep(sleep_batch)


# ── upsert_tab v1.5.1 — APPEND-ONLY (no full clear+rewrite) ──────
#
# HOW IT WORKS:
#   1. Read the full sheet once.
#   2. For rows whose period is in the current batch:
#        - If the composite key (e.g. period+product_id) already exists → batch_update that row.
#        - If the key is new → append.
#   3. Rows from OTHER periods are never touched.
#
# RESULT: each run writes at most ~1000 rows × 25 cols per tab per period.
# No more 10M-cell explosion regardless of how many historical periods exist.
#
def upsert_tab(sh, tab_name, headers, new_rows, key_cols):
    BATCH_SIZE  = 500
    SLEEP_BATCH = 2.0
    SLEEP_TAB   = 4.0

    # Get or create worksheet
    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheets_call(sh.add_worksheet, tab_name,
                         rows=max(2000, len(new_rows) + 100), cols=len(headers) + 2)
    time.sleep(2)

    # Read existing sheet
    try:
        existing_vals = sheets_call(ws.get_all_values)
    except Exception as e:
        print(f"    Warning reading {tab_name}: {e}"); existing_vals = []

    ex_h     = existing_vals[0] if existing_vals else []
    ex_h_idx = {h: i for i, h in enumerate(ex_h)}

    # Collect which periods we are writing in this batch
    period_col_idx = headers.index("period") if "period" in headers else None
    periods_in_batch = set()
    if period_col_idx is not None:
        for row in new_rows:
            if period_col_idx < len(row):
                periods_in_batch.add(str(row[period_col_idx]).strip())

    # Index new rows by composite key
    def make_key(row, h):
        return tuple(str(row[h.index(c)] if c in h else "").strip() for c in key_cols)

    new_index = {make_key(row, headers): row for row in new_rows}

    # ── FIRST RUN: sheet empty or wrong headers → clear + full write ──
    if not existing_vals or ex_h != headers:
        print(f"    {tab_name}: initializing (first run or header change)")
        all_data = [headers] + [_clean_row(r) for r in new_rows]
        needed   = len(all_data) + 200
        if ws.row_count < needed or ws.col_count < len(headers) + 2:
            sheets_call(ws.resize, rows=max(ws.row_count, needed),
                        cols=max(ws.col_count, len(headers) + 2)); time.sleep(1)
        sheets_call(ws.clear); time.sleep(1)
        _batch_append(ws, all_data, BATCH_SIZE, SLEEP_BATCH, tab_name)
        time.sleep(SLEEP_TAB)
        print(f"    ok {tab_name}: {len(new_rows)} rows written (init)")
        return

    # ── INCREMENTAL: find existing rows for our periods ───────────────
    period_col_ex      = ex_h_idx.get("period")
    key_col_ex_indices = [ex_h_idx.get(c) for c in key_cols]

    existing_key_to_sheetrow = {}
    if period_col_ex is not None:
        for data_idx, data_row in enumerate(existing_vals[1:], start=1):
            period_val = (data_row[period_col_ex] if period_col_ex < len(data_row) else "").strip()
            if period_val not in periods_in_batch: continue
            k = tuple(
                str(data_row[i]).strip() if (i is not None and i < len(data_row)) else ""
                for i in key_col_ex_indices
            )
            # data_idx is 1-based within existing_vals[1:], so sheet row = data_idx + 1
            existing_key_to_sheetrow[k] = data_idx + 1

    rows_to_update = []
    rows_to_append = []
    for k, row in new_index.items():
        clean = _clean_row(row)
        if k in existing_key_to_sheetrow:
            rows_to_update.append((existing_key_to_sheetrow[k], clean))
        else:
            rows_to_append.append(clean)

    # Batch-update existing rows via batch_update (no clear needed)
    if rows_to_update:
        for i in range(0, len(rows_to_update), BATCH_SIZE):
            batch = rows_to_update[i:i + BATCH_SIZE]
            data  = []
            for sheet_row_num, clean_row in batch:
                r1 = gspread.utils.rowcol_to_a1(sheet_row_num, 1)
                r2 = gspread.utils.rowcol_to_a1(sheet_row_num, len(headers))
                data.append({"range": f"{r1}:{r2}", "values": [clean_row]})
            sheets_call(ws.batch_update, data, value_input_option="RAW")
            print(f"    {tab_name}: updated {min(i+BATCH_SIZE, len(rows_to_update))}/{len(rows_to_update)} rows...")
            if i + BATCH_SIZE < len(rows_to_update): time.sleep(SLEEP_BATCH)

    # Append brand-new rows
    if rows_to_append:
        current_total = len(existing_vals) + len(rows_to_append) + 50
        if current_total > ws.row_count:
            sheets_call(ws.resize, rows=current_total + 200,
                        cols=max(ws.col_count, len(headers) + 2)); time.sleep(1)
        _batch_append(ws, rows_to_append, BATCH_SIZE, SLEEP_BATCH, tab_name)

    time.sleep(SLEEP_TAB)
    print(f"    ok {tab_name}: {len(rows_to_update)} updated + {len(rows_to_append)} appended")


# ── MAIN ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default=None)
    parser.add_argument("--start"); parser.add_argument("--end")
    args = parser.parse_args()

    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    periods = [resolve_single(args.only, args.start, args.end)] if args.only else build_all_periods()

    print(f"\n{'='*60}")
    print(f"  PARETO PIPELINE v1.5.1 — Corro ({STORE_URL})")
    print(f"  Mode  : {'single ('+args.only+')' if args.only else 'full backfill — '+str(len(periods))+' periods'}")
    print(f"  Sheet : {SHEET_ID}")
    print(f"  v1.5.1: append-only upsert · no 10M cell crash")
    print(f"{'='*60}\n")

    vmap, title_map = fetch_products_map()
    prod_colls      = fetch_collections_map()
    sh = get_gc().open_by_key(SHEET_ID)

    h_prod = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "product_title","product_type","vendor","sku_parent","product_id",
              "gross_sales","pct_gs","cum_gs_pct",
              "net_sales","pct_revenue","cum_pct",
              "cogs","gross_profit","gp_pct","pct_gp","cum_gp_pct",
              "units","orders","top_channel"]
    h_cat  = ["updated_at","period","period_start","period_end","rank","pareto_zone",
              "category","gross_sales","pct_gs","cum_gs_pct",
              "net_sales","pct_revenue","cum_pct",
              "cogs","gross_profit","gp_pct","pct_gp","cum_gp_pct","units","orders"]
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
        if not orders: print("  ⚠ No orders — skipping"); continue

        if first_run: audit_concierge_tags(orders); first_run = False

        sq_rows = fetch_shopifyql_sales(start, end)
        if not sq_rows:
            print("  ⚠ ShopifyQL returned 0 rows — skipping products/categories")
            ah = build_pareto_channels(orders); au = build_pareto_customers(orders, start)
            nvr = build_new_vs_returning(au)
            raws = [{"channel":ch,"net_sales":d["net_sales"],"gross_sales":d["gross_sales"],
                     "discounts":d["discounts"],"units":d["units"],"orders":len(d["orders"])} for ch,d in ah.items()]
            rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
            all_ch += [[now_str,pk,str(start),str(end),r["rank"],r["pareto_zone"],r["channel"],
                round(r["gross_sales"],2),round(r["discounts"],2),round(r["net_sales"],2),
                r["pct_revenue"],r["cum_pct"],r["units"],r["orders"]] for r in rs]
            raws = [dict(d, segment="New" if d["is_new"] else "Returning") for d in au.values()]
            rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
            all_cust += [[now_str,pk,str(start),str(end),r["rank"],r["pareto_zone"],
                r["customer_id"],r["name"],r["email"],r["segment"],round(r["net_sales"],2),
                r["pct_revenue"],r["cum_pct"],r["orders"],r["units"],r["first_order"],r["last_order"]] for r in rs]
            tc = sum(v["count"] for v in nvr.values()) or 1; tr = sum(v["net_sales"] for v in nvr.values()) or 1
            for seg, v in nvr.items():
                all_nvr.append([now_str,pk,str(start),str(end),seg.capitalize(),
                    v["count"],round(v["net_sales"],2),v["orders"],
                    round(v["count"]/tc*100,1),round(v["net_sales"]/tr*100,1)])
            continue

        ap  = build_pareto_products(sq_rows, title_map, orders)
        ac  = build_pareto_categories(sq_rows, title_map)
        ah  = build_pareto_channels(orders)
        au  = build_pareto_customers(orders, start)
        nvr = build_new_vs_returning(au)

        raws = list(ap.values())
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["gross_profit"], reverse=True))
        all_prod += [[now_str,pk,str(start),str(end),r["rank"],r["pareto_zone"],
            r["product_title"],r["product_type"],r["vendor"],r["sku_parent"],r["product_id"],
            round(r["gross_sales"],2),r["pct_gs"],r["cum_gs_pct"],
            round(r["net_sales"],2),r["pct_revenue"],r["cum_pct"],
            round(r["cogs"],2),round(r["gross_profit"],2),r["gp_pct"],r["pct_gp"],r["cum_gp_pct"],
            r["units"],r["orders"],r["top_channel"]] for r in rs]

        raws = []
        for cat, d in ac.items():
            gp = round(d["gross_profit"]/d["net_sales"]*100,1) if d["net_sales"] else 0
            raws.append({"category":cat,"net_sales":d["net_sales"],"gross_sales":d["gross_sales"],
                "discounts":d["discounts"],"units":d["units"],"orders":d["orders"],
                "cogs":d["cogs"],"gross_profit":d["gross_profit"],"gp_pct":gp})
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["gross_profit"], reverse=True))
        all_cat += [[now_str,pk,str(start),str(end),r["rank"],r["pareto_zone"],r["category"],
            round(r["gross_sales"],2),r["pct_gs"],r["cum_gs_pct"],
            round(r["net_sales"],2),r["pct_revenue"],r["cum_pct"],
            round(r["cogs"],2),round(r["gross_profit"],2),r["gp_pct"],r["pct_gp"],r["cum_gp_pct"],
            r["units"],r["orders"]] for r in rs]

        raws = [{"channel":ch,"net_sales":d["net_sales"],"gross_sales":d["gross_sales"],
                 "discounts":d["discounts"],"units":d["units"],"orders":len(d["orders"])} for ch,d in ah.items()]
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
        all_ch += [[now_str,pk,str(start),str(end),r["rank"],r["pareto_zone"],r["channel"],
            round(r["gross_sales"],2),round(r["discounts"],2),round(r["net_sales"],2),
            r["pct_revenue"],r["cum_pct"],r["units"],r["orders"]] for r in rs]

        raws = [dict(d, segment="New" if d["is_new"] else "Returning") for d in au.values()]
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
        all_cust += [[now_str,pk,str(start),str(end),r["rank"],r["pareto_zone"],
            r["customer_id"],r["name"],r["email"],r["segment"],round(r["net_sales"],2),
            r["pct_revenue"],r["cum_pct"],r["orders"],r["units"],r["first_order"],r["last_order"]] for r in rs]

        tc = sum(v["count"] for v in nvr.values()) or 1; tr = sum(v["net_sales"] for v in nvr.values()) or 1
        for seg, v in nvr.items():
            all_nvr.append([now_str,pk,str(start),str(end),seg.capitalize(),
                v["count"],round(v["net_sales"],2),v["orders"],
                round(v["count"]/tc*100,1),round(v["net_sales"]/tr*100,1)])

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
    print(f"  ✓ Done v1.5.1 — {len(periods)} periods | Sheet: {SHEET_ID}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
