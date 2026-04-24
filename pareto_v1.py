"""
PARETO PIPELINE v1.3 — Equestrian Labs / Corro
===============================================
Cambios v1.3 vs v1.2:
- CHANGED: map_product_type_to_category() ahora usa las 7 categorías que
  corresponden exactamente a los order tags de Shopify:
    Horse_Health, Grooming_Tools, Horsewear, Supplements,
    Tack_Equipment, StableSupplies_Collection, Rider, Uncategorized.
  Estos tags son asignados automáticamente por Shopify a las órdenes
  según los tipos de productos que contienen.
- CHANGED: Dogs ya no existe como categoría separada — los productos de
  perros caen en Horse_Health o Supplements según su product_type.
- NOTE: el ordenamiento por Gross Profit DESC se aplica consistentemente
  tanto en products como en categories (ya introducido en v1.2).
- NOTE: Los 7 order tags de Shopify son los mismos labels que se usan en
  el frontend HTML para la sección de categorías y el chart.

Cambios v1.2 vs v1.1:
- CHANGED: Products and categories now sorted by Gross Profit DESC (previously Net Sales).
  Zone A still = 80% cumulative GP.
- CHANGED: build_pareto_categories now uses product_type (mapped to 7 main categories
  matching Shopify order tags) instead of Shopify collections.
- ADDED: map_product_type_to_category() helper with confirmed product_type keywords.

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

Run modes:
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
    FIXED v1.1: fetch_orders sin duplicación — dos requests separados
    (paid+partially_paid y partially_refunded+refunded) y deduplicamos por order id.
    Resultado esperado: ~8,800 órdenes en 2025.
    """
    print(f"  Fetching orders {start} → {end} (deduplication fix v1.1)...")

    seen_ids = set()
    all_orders = []

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

def shopify_graphql(query, variables=None):
    """
    Execute a single Shopify Admin GraphQL query with retry/backoff.
    """
    url     = f"https://{STORE_URL}/admin/api/{API_VERSION}/graphql.json"
    headers = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(8):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=60)
        except requests.exceptions.ConnectionError as e:
            wait = min(2 ** attempt, 60)
            print(f"    GQL connection error — retrying in {wait}s: {e}")
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 2 ** attempt))
            print(f"    GQL rate-limited — waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code in (502, 503, 504):
            wait = min(2 ** attempt, 60)
            print(f"    GQL {r.status_code} — retrying in {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        data = r.json()
        errors = data.get("errors") or []
        if errors and any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors
        ):
            wait = min(2 ** attempt, 60)
            print(f"    GQL throttled — retrying in {wait}s...")
            time.sleep(wait)
            continue
        return data
    raise RuntimeError("GraphQL call failed after 8 attempts")


def fetch_products_map():
    """
    v1.5: Fetches product/variant metadata via GraphQL for product_type,
    vendor, and product_id lookups. COGS/gross_profit now come directly
    from ShopifyQL — this function no longer needs to fetch costs.
    Returns:
        vmap  dict[variant_id_str] → {product_id, product_title, sku_parent,
                                       vendor, product_type}
        title_map  dict[product_title_lower] → {product_type, vendor, product_id}
    """
    print("  Fetching product catalogue via GraphQL...")

    QUERY = """
    query fetchVariants($cursor: String) {
      productVariants(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            sku
            product {
              id
              title
              vendor
              productType
            }
          }
        }
      }
    }
    """

    vmap      = {}
    title_map = {}  # product_title.lower() → product metadata for ShopifyQL join
    cursor    = None

    while True:
        data  = shopify_graphql(QUERY, {"cursor": cursor})
        pv    = data.get("data", {}).get("productVariants", {})
        edges = pv.get("edges", [])

        for edge in edges:
            node    = edge["node"]
            vid     = str(node["id"].split("/")[-1])
            product = node.get("product") or {}
            pid     = str((product.get("id") or "").split("/")[-1])
            sku     = node.get("sku") or ""
            title   = product.get("title", "")
            ptype   = product.get("productType") or "Uncategorized"
            vendor  = product.get("vendor", "")

            vmap[vid] = {
                "product_id":    pid,
                "product_title": title,
                "sku_parent":    sku.split("-")[0] if sku else "",
                "vendor":        vendor,
                "product_type":  ptype,
            }
            # title_map keyed by lowercase title for case-insensitive join with ShopifyQL rows
            title_map[title.lower()] = {
                "product_id":   pid,
                "product_type": ptype,
                "vendor":       vendor,
            }

        page_info = pv.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info["endCursor"]
        time.sleep(0.3)

    print(f"  → {len(vmap)} variants  |  {len(title_map)} unique product titles")
    return vmap, title_map


def fetch_shopifyql_sales(start, end):
    """
    v1.5: Uses ShopifyQL (shopifyqlQuery) to fetch gross_profit, gross_sales,
    net_sales, orders, net_items_sold per product_title from Shopify Analytics.

    Query matches exactly the confirmed Shopify AI script.
    Requires scope: read_reports

    Two separate queries:
      1. Main: product financials grouped by product_title + product_vendor
      2. Channel: top sales_channel per product_title (non-fatal if fails)

    Returns list of dicts per product title.
    """
    print(f"  Fetching ShopifyQL sales {start} → {end}...")

    GQL = """
    query shopifyqlSales($q: String!) {
      shopifyqlQuery(query: $q) {
        tableData {
          columns { name dataType }
          rows
        }
        parseErrors { code message range { start end } }
      }
    }
    """

    def run_shopifyql(q):
        """Run a ShopifyQL query, return (cols, rows, errors)."""
        d      = shopify_graphql(GQL, {"q": q})
        errs   = (d.get("data") or {}).get("shopifyqlQuery", {}).get("parseErrors") or []
        table  = (d.get("data") or {}).get("shopifyqlQuery", {}).get("tableData") or {}
        cols   = [c["name"] for c in table.get("columns", [])]
        rows   = table.get("rows") or []
        return cols, rows, errs

    # ── Query 1: product financials ───────────────────────────────────
    # GROUP BY product_title, product_vendor (no GROUP_TOTALS — simpler, no parse errors)
    # ORDER BY gross_profit DESC — straightforward, matches Shopify AI guidance
    q_main = (
        f"FROM sales "
        f"SHOW gross_profit, gross_sales, net_sales, orders, net_items_sold "
        f"GROUP BY product_title, product_vendor "
        f"SINCE {start} UNTIL {end} "
        f"ORDER BY gross_profit DESC"
    )
    cols, rows, errs = run_shopifyql(q_main)

    if errs:
        # Fallback: drop product_vendor, group by product_title only
        print(f"  ⚠ ShopifyQL main query error — retrying without product_vendor:")
        for e in errs:
            print(f"    {e.get('code')} — {e.get('message')}")
        q_fallback = (
            f"FROM sales "
            f"SHOW gross_profit, gross_sales, net_sales, orders, net_items_sold "
            f"GROUP BY product_title "
            f"SINCE {start} UNTIL {end} "
            f"ORDER BY gross_profit DESC"
        )
        cols, rows, errs2 = run_shopifyql(q_fallback)
        if errs2:
            for e in errs2:
                print(f"  ✗ ShopifyQL fallback failed: {e.get('code')} — {e.get('message')}")
            return []

    if not cols:
        print("  ⚠ ShopifyQL returned no columns — check read_reports scope on token")
        return []

    # ── Query 2: top channel per product (non-fatal) ──────────────────
    top_channel_map = {}
    try:
        q_ch = (
            f"FROM sales "
            f"SHOW net_sales "
            f"GROUP BY product_title, sales_channel "
            f"SINCE {start} UNTIL {end} "
            f"ORDER BY net_sales DESC"
        )
        ch_cols, ch_rows, ch_errs = run_shopifyql(q_ch)
        if not ch_errs and ch_cols:
            ch_agg = defaultdict(dict)
            for row in ch_rows:
                if not isinstance(row, dict):
                    row = dict(zip(ch_cols, row)) if isinstance(row, list) else {}
                t  = (row.get("product_title") or "").strip()
                ch = (row.get("sales_channel") or "").strip()
                ns = float(row.get("net_sales") or 0)
                if t and ch:
                    ch_agg[t][ch] = ch_agg[t].get(ch, 0) + ns
            for t, channels in ch_agg.items():
                top_channel_map[t] = max(channels, key=channels.get)
            print(f"  → channel query: {len(top_channel_map)} products mapped")
        else:
            if ch_errs:
                print(f"  ⚠ channel query error (non-fatal): {ch_errs[0].get('message')}")
    except Exception as e:
        print(f"  ⚠ channel query failed (non-fatal): {e}")

    # ── Parse rows ────────────────────────────────────────────────────
    results = []
    seen    = set()
    for row in rows:
        if not isinstance(row, dict):
            row = dict(zip(cols, row)) if isinstance(row, list) else {}
        title  = (row.get("product_title")  or "").strip()
        vendor = (row.get("product_vendor") or "").strip()
        if not title:
            continue
        # Deduplicate: if product_title appears multiple times (can happen when
        # product has variants from multiple vendors), aggregate into one row
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
        ns = float(row.get("net_sales")    or 0)
        gp = float(row.get("gross_profit") or 0)
        gs = float(row.get("gross_sales")  or 0)
        if gs == 0:
            gs = ns
        results.append({
            "product_title": title,
            "vendor":        vendor,
            "gross_profit":  gp,
            "gross_sales":   gs,
            "discounts":     max(gs - ns, 0),
            "net_sales":     ns,
            "orders":        int(row.get("orders")         or 0),
            "units":         int(row.get("net_items_sold") or 0),
            "top_channel":   top_channel_map.get(title, ""),
        })

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

# ── CATEGORY MAPPING v1.3 ─────────────────────────────────────────
def map_product_type_to_category(raw_type):
    """
    v1.3: Maps Shopify product_type values to the 7 categories that match
    exactly the Shopify order tags assigned automatically to orders.

    Order tag → category meaning:
      Horse_Health           → Farmacia equina, terapéuticos (GastroGard, UlcerGard, Back on Track)
      Grooming_Tools         → Herramientas de grooming (cajas, cepillos, detanglers)
      Horsewear              → Ropa para caballos (mantas, sheets, fly wraps, coolers, neck covers)
      Supplements            → Suplementos nutricionales (Cavalor, Nupafeed, Kentucky Performance, EO-3)
      Tack_Equipment         → Equipo de tack (riendas, cinchas, breastplates, estribos, bridones)
      StableSupplies_Collection → Suministros de establo (stall mats, bags, limpieza)
      Rider                  → Productos para el jinete (ropa, shirts, breeches, boot bags, accesorios)

    Keywords confirmed against Shopify product_type export (April 2026, 3,021 products).
    Order matters — checked top to bottom, first match wins.
    Dogs products map into Horse_Health (therapeutic) or Supplements per their product_type.
    """
    if not raw_type:
        return "Uncategorized"
    t = raw_type.strip().lower()

    # ── Rider — jinete products checked FIRST ─────────────────────
    # Must be before Tack_Equipment because "breeches" could otherwise
    # fall through to accessories-related keywords.
    rider_kw = [
        "apparel", "breeches", "hat", "shirt", "schooling",
        "leather handbag", "handbag", "boot bag",
        "rider", "belt",
    ]
    if any(k in t for k in rider_kw):
        return "Rider"

    # ── Tack & Equipment ─────────────────────────────────────────
    tack_kw = [
        "bridle", "rein", "bridle accessories",
        "saddle pad", "all purpose", "hunter/jumper", "dressage saddle",
        "half pad", "stirrup", "dressage bridle",
        "girth", "breastplate", "martingale", "bit",
    ]
    if any(k in t for k in tack_kw):
        return "Tack_Equipment"

    # ── Horsewear — mantas, sheets, fly wraps, coolers ───────────
    horsewear_kw = [
        "blanket", "sheet", "fly wrap", "fly rug", "cooler",
        "neck cover", "hood", "turnout", "stable rug",
        "horsewear",
    ]
    if any(k in t for k in horsewear_kw):
        return "Horsewear"

    # ── Grooming Tools ───────────────────────────────────────────
    grooming_kw = [
        "detangler", "shampoo", "grooming", "brush", "comb",
        "body brace", "skin & hair", "fungal", "spray",
    ]
    if any(k in t for k in grooming_kw):
        return "Grooming_Tools"

    # ── Supplements — nutricionales ──────────────────────────────
    supplement_kw = [
        "supplement", "electrolyte", "vitamin", "probiotic",
        "omega", "joint", "performance",
    ]
    if any(k in t for k in supplement_kw):
        return "Supplements"

    # ── Horse Health — farmacia y terapéuticos ───────────────────
    # Includes dog therapeutic/pharma products too
    health_kw = [
        "hoof care", "poultice", "liniment", "relief",
        "wound", "ointment", "pharmaceutical", "therapeutic",
        "ulcer", "gastric", "back on track",
        "dog",  # dog products map here if not supplement
        "personal care",
    ]
    if any(k in t for k in health_kw):
        return "Horse_Health"

    # ── Stable Supplies ──────────────────────────────────────────
    stable_kw = [
        "stall mat", "stall & trailer", "trailer cleaning",
        "stall", "stable supply", "cleaning", "bag",
        "cavali club", "spring 2024", "special edition",
        "_elite_gift", "elite_gift",
    ]
    if any(k in t for k in stable_kw):
        return "StableSupplies_Collection"

    # ── Rider catch-all: accessories ────────────────────────────
    # 'accessories' is checked late to avoid false positives
    if "accessories" in t:
        return "Rider"

    return "Uncategorized"


# ── AGGREGATION ─────────────────────────────────────────────────────
def build_pareto_products(shopifyql_rows, title_map, orders):
    """
    v1.5: All financials (gross_profit, gross_sales, net_sales, orders, units,
    vendor, top_channel) come directly from ShopifyQL — exact match to
    Shopify Analytics.

    product_type comes from title_map (product catalogue via GraphQL) since
    ShopifyQL FROM sales does not expose product_type.
    top_channel comes from ShopifyQL channel query if available, otherwise
    falls back to order-level detection.

    shopifyql_rows: list of dicts from fetch_shopifyql_sales()
    title_map:      dict[product_title.lower()] → {product_id, product_type, vendor}
    orders:         raw order list — fallback for top_channel if ShopifyQL channel
                    query was unavailable
    """
    # Fallback channel map from orders — only used if ShopifyQL channel query failed
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
            if pid:
                channel_agg[pid][ch] += net

    agg = {}
    for row in shopifyql_rows:
        title  = row["product_title"]
        meta   = title_map.get(title.lower(), {})
        pid    = meta.get("product_id", "")
        ptype  = meta.get("product_type", "Uncategorized")
        # vendor: prefer ShopifyQL (comes from product_vendor in GROUP BY),
        # fall back to title_map if blank
        vendor = row.get("vendor") or meta.get("vendor", "")
        # top_channel: prefer ShopifyQL channel query result (already in row),
        # fall back to order-level detection
        top_ch = row.get("top_channel") or (
            max(channel_agg[pid], key=channel_agg[pid].get)
            if channel_agg.get(pid) else ""
        )
        gp        = row["gross_profit"]
        ns        = row["net_sales"]
        gp_pct    = round(gp / ns * 100, 1) if ns else 0.0
        cogs      = max(ns - gp, 0)
        discounts = row.get("discounts") or max(row.get("gross_sales", ns) - ns, 0)

        agg[title] = {
            "product_id":    pid,
            "product_title": title,
            "product_type":  ptype,
            "vendor":        vendor,
            "sku_parent":    "",
            "gross_sales":   row.get("gross_sales", ns),
            "discounts":     discounts,
            "net_sales":     ns,
            "cogs":          cogs,
            "gross_profit":  gp,
            "gp_pct":        gp_pct,
            "units":         row["units"],
            "orders":        row["orders"],
            "top_channel":   top_ch,
        }
    return agg


def build_pareto_categories(shopifyql_rows, title_map):
    """
    v1.5: Aggregates ShopifyQL product rows into 7 order-tag categories.
    gross_profit and all financials come from ShopifyQL — no COGS calculation.
    """
    agg = defaultdict(lambda: {
        "gross_sales": 0.0, "discounts": 0.0, "net_sales": 0.0,
        "cogs": 0.0, "gross_profit": 0.0, "units": 0, "orders": 0,
    })
    for row in shopifyql_rows:
        title  = row["product_title"]
        meta   = title_map.get(title.lower(), {})
        ptype  = meta.get("product_type", "Uncategorized")
        cat    = map_product_type_to_category(ptype)
        ns     = row["net_sales"]
        gp     = row["gross_profit"]
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

# ── PARETO COLUMNS — v1.1 / v1.2 ─────────────────────────────────
def add_pareto_cols(rows, revenue_key="net_sales", gp_key="gross_profit"):
    """
    - pct_revenue y cum_pct sobre net_sales (ranking)
    - pct_gs y cum_gs_pct sobre gross_sales
    - pct_gp y cum_gp_pct sobre gross_profit
    - pareto_zone asignado por cum_gp_pct:
        Zone A: cum_gp_pct <= 80%
        Zone B: cum_gp_pct <= 95%
        Zone C: resto
    Rows deben estar pre-ordenados por gross_profit DESC antes de llamar esta función.
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
        r["pct_revenue"] = round(r.get(revenue_key, 0) / total_rev * 100, 2)
        r["cum_pct"]     = round(cum_rev / total_rev * 100, 2)
        r["pct_gs"]      = round(r.get("gross_sales", 0) / total_gs * 100, 2)
        r["cum_gs_pct"]  = round(cum_gs / total_gs * 100, 2)
        r["pct_gp"]      = round(r.get(gp_key, 0) / total_gp * 100, 2)
        r["cum_gp_pct"]  = round(cum_gp / total_gp * 100, 2)
        # Zone based on cumulative GP
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
    print(f"  PARETO PIPELINE v1.5 — Corro ({STORE_URL})")
    print(f"  Mode  : {'single ('+args.only+')' if args.only else 'full backfill — '+str(len(periods))+' periods'}")
    print(f"  Sheet : {SHEET_ID}")
    print(f"  v1.5  : ShopifyQL gross_profit · 7 order-tag categories · GP-sorted")
    print(f"{'='*60}\n")

    vmap, title_map = fetch_products_map()
    prod_colls      = fetch_collections_map()
    sh           = get_gc().open_by_key(SHEET_ID)

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

        # ── ShopifyQL: gross_profit, gross_sales, net_sales, orders, units per product
        #    Same source as Shopify Analytics — gross_profit matches exactly.
        sq_rows = fetch_shopifyql_sales(start, end)
        if not sq_rows:
            print("  ⚠ ShopifyQL returned 0 rows — skipping products/categories for this period")
            print("    Check that token has read_reports scope.")
            # Skip products + categories but still write channels, customers, nvr
            ah  = build_pareto_channels(orders)
            au  = build_pareto_customers(orders, start)
            nvr = build_new_vs_returning(au)
            # channels
            raws = [{"channel":ch,"net_sales":d["net_sales"],"gross_sales":d["gross_sales"],
                     "discounts":d["discounts"],"units":d["units"],"orders":len(d["orders"])} for ch,d in ah.items()]
            rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
            all_ch += [[
                now_str, pk, str(start), str(end),
                r["rank"], r["pareto_zone"], r["channel"],
                round(r["gross_sales"],2), round(r["discounts"],2), round(r["net_sales"],2),
                r["pct_revenue"], r["cum_pct"], r["units"], r["orders"]
            ] for r in rs]
            # customers
            raws = [dict(d, segment="New" if d["is_new"] else "Returning") for d in au.values()]
            rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["net_sales"], reverse=True))
            all_cust += [[
                now_str, pk, str(start), str(end),
                r["rank"], r["pareto_zone"],
                r["customer_id"], r["name"], r["email"], r["segment"],
                round(r["net_sales"],2), r["pct_revenue"], r["cum_pct"],
                r["orders"], r["units"], r["first_order"], r["last_order"]
            ] for r in rs]
            tc = sum(v["count"] for v in nvr.values()) or 1
            tr = sum(v["net_sales"] for v in nvr.values()) or 1
            for seg, v in nvr.items():
                all_nvr.append([now_str, pk, str(start), str(end), seg.capitalize(),
                    v["count"], round(v["net_sales"],2), v["orders"],
                    round(v["count"]/tc*100,1), round(v["net_sales"]/tr*100,1)])
            print(f"  → {len(orders)} orders | channels/customers written | products SKIPPED (ShopifyQL empty)")
            continue

        ap  = build_pareto_products(sq_rows, title_map, orders)
        ac  = build_pareto_categories(sq_rows, title_map)
        ah  = build_pareto_channels(orders)
        au  = build_pareto_customers(orders, start)
        nvr = build_new_vs_returning(au)

        # Products — sorted by gross_profit DESC, zones by cumulative GP
        raws = list(ap.values())
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["gross_profit"], reverse=True))
        all_prod += [[
            now_str, pk, str(start), str(end),
            r["rank"], r["pareto_zone"],
            r["product_title"], r["product_type"], r["vendor"], r["sku_parent"], r["product_id"],
            round(r["gross_sales"],2), r["pct_gs"], r["cum_gs_pct"],
            round(r["net_sales"],2),   r["pct_revenue"], r["cum_pct"],
            round(r["cogs"],2), round(r["gross_profit"],2), r["gp_pct"], r["pct_gp"], r["cum_gp_pct"],
            r["units"], r["orders"], r["top_channel"]
        ] for r in rs]

        # Categories — sorted by gross_profit DESC, zones by cumulative GP
        raws = []
        for cat, d in ac.items():
            gp = round(d["gross_profit"]/d["net_sales"]*100,1) if d["net_sales"] else 0
            raws.append({
                "category":cat,"net_sales":d["net_sales"],"gross_sales":d["gross_sales"],
                "discounts":d["discounts"],"units":d["units"],"orders":d["orders"],
                "cogs":d["cogs"],"gross_profit":d["gross_profit"],"gp_pct":gp
            })
        rs, _ = add_pareto_cols(sorted(raws, key=lambda x: x["gross_profit"], reverse=True))
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
    print(f"  ✓ Done v1.5 — {len(periods)} periods | Sheet: {SHEET_ID}")
    print(f"  Categories: 7 Shopify order tags (Horse_Health, Grooming_Tools,")
    print(f"              Horsewear, Supplements, Tack_Equipment,")
    print(f"              StableSupplies_Collection, Rider)")
    print(f"  Sorted and zoned by Gross Profit (cumulative)")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
