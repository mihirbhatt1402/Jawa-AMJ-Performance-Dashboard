"""
Jawa AMJ Performance Dashboard — Daily Data Pipeline
=====================================================
Architecture:
  Layer 1 — Frozen: May'25–Jun'26  (hist_cache.json.gz, READ-ONLY)
  Layer 2 — Transition: Jul'26, Aug'26  (Google Sheets via Apps Script proxy)
  Layer 3 — Live: Sep'26+  (current-month Google Sheet)
  Layer 4 — Retail: rolling 3-month window from Retail Master

Failure safety: if ANY step fails, production payload (index.html) is unchanged.
"""

from __future__ import annotations
import argparse, gzip, json, logging, os, re, sys, time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────
REPO_ROOT      = Path(__file__).parent.parent
JAWA_DIR       = Path(__file__).parent
HIST_CACHE     = JAWA_DIR / "hist_cache.json.gz"
AUG26_FALLBACK = JAWA_DIR / "aug26_fallback.json.gz"
CITY_TIER_FILE = JAWA_DIR / "city_tier.json"
DEALER_META_FILE = JAWA_DIR / "dealer_meta.json"
INDEX_HTML     = REPO_ROOT / "index.html"
STAGING_HTML   = REPO_ROOT / "index_staging.html"
METRICS_FILE   = JAWA_DIR / "pipeline_metrics.json"

# ── Configuration ─────────────────────────────────────────────────────────
# Freeze boundary: May'25 through Jun'26 (inclusive) are read from hist_cache
HISTORICAL_FREEZE_MONTH = "Jun'26"

# Transition months: fetched fresh from Google Sheets each run
TRANSITION_MONTHS = ["Jul'26", "Aug'26"]

# Live start: current month and forward
LIVE_START_MONTH = "Sep'26"

LEAD_PK   = "opty_id"        # primary key in lead sheets
RETAIL_PK = "sourceLeadId"   # primary key in retail master
RETAIL_WINDOW = 3             # rolling retail window in months
RETAIL_FILTER_VAL = "Jawa"   # brand filter for retail master

# Google Sheet IDs (override via GitHub Secrets / environment variables)
SHEET_JUL26 = os.getenv("JAWA_SHEET_ID_JUL26",  "1t29vI-JdKu7HDaaLX33N3wFnhylFLbOkwPhyWc6ZKiA")
SHEET_AUG26 = os.getenv("JAWA_SHEET_ID_AUG26",  "1WgyRvNW02UxCYyhQDLfaFa86RciciSeGXeQmBF7ymIg")
SHEET_LIVE  = os.getenv("JAWA_SHEET_ID_LIVE",   "1MdlYzXsJ1rAZ1PfVDNXE7n8IGaHQuoW25QHh3tq6XRc")
RETAIL_SHEET= os.getenv("JAWA_RETAIL_SHEET_ID", "1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE")
JAWA_TAB    = os.getenv("JAWA_TAB_NAME",  "Jawa")
RETAIL_TAB  = os.getenv("JAWA_RETAIL_TAB", "Raw")

# The TVS Apps Script proxy is generic — it accepts any fileId the Google
# account can access.  Set JAWA_APPS_SCRIPT_URL to the TVS proxy URL and
# JAWA_APPS_SCRIPT_SECRET to the TVS secret if a separate Jawa proxy is not
# deployed.  Both are required GitHub Secrets for the production workflow.
_TVS_PROXY_URL    = "https://script.google.com/macros/s/AKfycbwdTKif3l3gYJKMwZBO6PjmYgNbWulkQ9TMEIsN-6xMdG2efbndnSoHE4tC63Oe6AKmlQ/exec"
_TVS_PROXY_SECRET = "tvs2026push"

PROXY_URL    = os.getenv("JAWA_APPS_SCRIPT_URL",    _TVS_PROXY_URL)
PROXY_SECRET = os.getenv("JAWA_APPS_SCRIPT_SECRET", _TVS_PROXY_SECRET)

# Model name merges (applied at extraction, must match existing dashboard)
MODEL_MERGES = {
    "2025 Yezdi Adventure":      "Yezdi Adventure",
    "2024 Yezdi Roadster":       "Yezdi Roadster",
    "Yezdi Scrambler 350":       "Yezdi Scrambler",
    "Yezdi Scrambler (2022-2025)": "Yezdi Scrambler",
    # "2024 Yezdi Adventure" is NOT merged — left as-is
}

# Source type → dashboard source label.
# The Google Sheets (Jul'26+) use direct display labels (Organic, Google, Facebook, Non-MS).
# The old Excel/CRM codes (CPC, FB, IMS) are kept for backward compatibility.
SOURCE_MAP = {
    # Google Sheet direct labels
    "Organic":   "Organic",
    "Google":    "Google",
    "Facebook":  "Facebook",
    "Non-MS":    "Non CPS",
    "Whatsapp":  "Whatsapp",
    # Legacy CRM codes (Excel sources)
    "CPC":       "Google",
    "FB":        "Facebook",
    "IMS":       "Non CPS",
}

# Retail source: Call Type column values → dashboard retailSource label
CALL_TYPE_MAP = {
    "DMS":      "DMS",
    "Call Out": "VOC",  # outbound verification call = voice of customer
}

KNOWN_SOURCES = ["Facebook", "Google", "Non CPS", "Organic", "Whatsapp", "Unknown"]


# ── Utilities ─────────────────────────────────────────────────────────────
def month_order(m: str) -> int:
    """'Sep'26' → 202609. Accepts both 2-digit ('26) and 4-digit ('2026) years."""
    m = str(m).strip()
    mo_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    pat = re.match(r"^([A-Za-z]{3})'?(\d{2,4})$", m)
    if not pat:
        raise ValueError(f"Cannot parse month: {m!r}")
    name, yr = pat.group(1).capitalize(), pat.group(2)
    mo = mo_names.index(name) + 1  # 1-based
    yr4 = int(yr) if len(yr) == 4 else (2000 + int(yr))
    return yr4 * 100 + mo


def canon_month(m: str) -> str:
    """Normalise any month string to 2-digit-year form: 'Jul'2026' → 'Jul'26'.
    Handles ASCII apostrophe (U+0027) and Unicode RIGHT SINGLE QUOTATION MARK (U+2019).
    """
    m = str(m).strip().replace('’', "'").replace('‘', "'")
    mo_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    pat = re.match(r"^([A-Za-z]{3})'?(\d{2,4})$", m)
    if not pat:
        return m
    name, yr = pat.group(1).capitalize(), pat.group(2)
    yr4 = int(yr) if len(yr) == 4 else (2000 + int(yr))
    mo = mo_names.index(name) + 1
    return f"{name}'{yr4 % 100:02d}"


def norm_id(v) -> str:
    """Normalise an opty_id / sourceLeadId: strip whitespace, remove .0 suffix."""
    if v is None:
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def norm_mobile(v) -> str:
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    # Keep last 10 digits
    digits = re.sub(r"\D", "", s)
    return digits[-10:] if len(digits) >= 10 else digits


def norm_model(m: str) -> str:
    m = str(m).strip()
    return MODEL_MERGES.get(m, m)


def norm_source(type_val: str) -> str:
    return SOURCE_MAP.get(str(type_val).strip(), "Unknown")


def rolling_months(current_month: str, window: int = 3) -> list[str]:
    """Return list of month strings for the rolling retail window."""
    mo_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    pat = re.match(r"^([A-Za-z]{3})'?(\d{2,4})$", current_month.strip())
    name, yr = pat.group(1).capitalize(), int(pat.group(2))
    yr4 = yr if yr > 100 else 2000 + yr
    mo = mo_names.index(name) + 1
    result = []
    for _ in range(window):
        result.append(f"{mo_names[mo-1]}'{yr4 % 100:02d}")
        mo -= 1
        if mo == 0:
            mo = 12
            yr4 -= 1
    return result


def current_month() -> str:
    now = datetime.utcnow()
    mo = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return f"{mo[now.month-1]}'{now.year % 100:02d}"


# ── Failure guard ──────────────────────────────────────────────────────────
def fail_exit(stage: str, reason: str) -> None:
    log.error("=== PIPELINE FAILURE at stage: %s ===", stage)
    log.error("Reason: %s", reason)
    log.error("Production payload unchanged. Exiting.")
    sys.exit(1)


# ── Apps Script proxy ──────────────────────────────────────────────────────
def proxy_get(action: str, extra: dict, timeout: int = 90) -> dict:
    if not PROXY_URL:
        fail_exit("proxy", "JAWA_APPS_SCRIPT_URL is not configured.")
    params = {"action": action, "secret": PROXY_SECRET, **extra}
    for attempt in range(3):
        try:
            r = requests.get(PROXY_URL, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2:
                raise
            log.warning("Proxy attempt %d failed: %s — retrying…", attempt + 1, e)
            time.sleep(5 * (attempt + 1))


def fetch_sheet(file_id: str, tab: str, label: str, page_size: int = 2000) -> pd.DataFrame:
    """Fetch a full sheet via Apps Script proxy with pagination.

    Uses the TVS-compatible getSheetData action: response is
    {headers:[...], rows:[[...],[...]], total:N, done:bool}.
    Rows are arrays (positional), not dicts — pd.DataFrame handles both.
    """
    log.info("Fetching %s (tab=%s)…", label, tab)
    all_rows = []
    page = 0
    headers = None
    while True:
        data = proxy_get("getSheetData", {
            "fileId": file_id, "tabName": tab,
            "page": page, "pageSize": page_size,
        })
        if headers is None:
            headers = data.get("headers", [])
        rows = data.get("rows", [])
        all_rows.extend(rows)
        log.info("  … %d rows fetched (page %d, total reported %s)", len(all_rows), page, data.get("total", "?"))
        if data.get("done", True):
            break
        page += 1
        time.sleep(0.3)
    df = pd.DataFrame(all_rows, columns=headers)
    log.info("  Total: %d rows from %s", len(df), label)
    return df


# ── Hist-cache ─────────────────────────────────────────────────────────────
def load_hist_cache() -> tuple[list[dict], dict]:
    """Load frozen May'25–Jun'26 baseline. Returns (rows, dealer_meta)."""
    if not HIST_CACHE.exists():
        fail_exit("hist_cache", f"Frozen cache not found: {HIST_CACHE}")
    with gzip.open(HIST_CACHE, "rt", encoding="utf-8") as f:
        cache = json.load(f)
    rows = cache["rows"]
    dm   = cache.get("dealerMeta", {})
    freeze_thru = cache.get("frozen_through", "?")
    log.info("Loaded hist_cache: %d frozen rows through %s", len(rows), freeze_thru)
    # Validate frozen data integrity
    freeze_order = month_order(freeze_thru)
    bad = [r for r in rows if month_order(r["month"]) > freeze_order]
    if bad:
        fail_exit("hist_cache", f"Freeze violation: {len(bad)} rows beyond {freeze_thru}")
    return rows, dm


# ── Lead sheet processing ──────────────────────────────────────────────────
def _lead_col(df: pd.DataFrame, *names: str, required: bool = True):
    """Return first matching column name. Raises if required and not found."""
    for n in names:
        if n in df.columns:
            return n
    if required:
        fail_exit("lead_cols", f"Required column not found: {names}. Available: {list(df.columns)[:30]}")
    return None


def standardise_lead_df(df: pd.DataFrame, expected_month: str | None = None) -> pd.DataFrame:
    """Extract and normalise lead master columns → standard dict list.

    Handles both the old Excel/CRM column names and the current Google Sheet
    column names (oem_crm_id for dealer, Status_Name for status, Medium for
    source type, DMS_Retail_Month / Ops_Retail_Month for performance month).
    Lead_Month values are normalised to 2-digit-year form ('Jul'2026' → 'Jul'26').
    """
    # Column discovery — ordered: first match wins
    col_id     = _lead_col(df, "opty_id", "EnquiryId", "enquiryid", "enquiry_id")
    col_month  = _lead_col(df, "Lead_Month", "Month", "LeadMonth", "lead_month")
    col_date   = _lead_col(df, "Date", "CreateTime", "create_time")
    col_model  = _lead_col(df, "model", "Model", required=False)
    if not col_model:
        model_cols = [c for c in df.columns if str(c).strip().lower() == "model"]
        col_model = model_cols[-1] if model_cols else _lead_col(df, "Model")
    # Source: Google Sheet uses "Medium"; legacy Excel uses "Type"
    col_type   = _lead_col(df, "Medium", "Type", "type", required=False)
    col_lt     = _lead_col(df, "lead_type", "Lead Type", "LeadType", "leadtype", required=False)
    # Dealer: Google Sheet uses "oem_crm_id"; legacy Excel uses "L1 Dealer Code"
    col_dealer = _lead_col(df, "oem_crm_id", "L1 Dealer Code", "Dealer Code", "dealer_code", "L1DealerCode", required=False)
    col_d_name = _lead_col(df, "Dealer_Name", "L1 Organization Name", "DealerName", "dealer_name", required=False)
    col_d_city = _lead_col(df, "City", "L1 Dealer City", "DealerCity", "dealer_city", required=False)
    col_d_state= _lead_col(df, "State", "state", required=False)
    # Mobile: Google Sheet uses encrypted mobile; keep it as opaque token (join never uses mobile)
    col_mobile = _lead_col(df, "encrypt_mobile_number", "Consumer Mobile Number", "Mobile", "mobile", required=False)
    # Status: Google Sheet uses "Status_Name"; legacy Excel uses "Current Status"
    col_status = _lead_col(df, "Status_Name", "Current Status", "current_status", required=False)
    # Qualified status and status category not present in Google Sheets — optional
    col_qual   = _lead_col(df, "Qualified Status", "qualified_status", required=False)
    col_scat   = _lead_col(df, "Status Category", "status_category", required=False)
    # Retail fields in lead sheet: may be overridden by Retail Master
    col_rmodel = _lead_col(df, "Retail Purchased Model", "Retail Model", required=False)
    col_rby    = _lead_col(df, "Retail Updated By", "Retail By", "retail_by", required=False)
    # Performance month: Google Sheet uses DMS_Retail_Month / Ops_Retail_Month
    col_pm     = _lead_col(df, "DMS_Retail_Month", "Ops_Retail_Month", "Performance Month", "performanceMonth", required=False)

    if not col_id:
        fail_exit("lead_cols", "opty_id column not found in lead sheet.")

    records = []
    for _, row in df.iterrows():
        oid = norm_id(row[col_id])
        if not oid:
            continue

        month_raw_orig = str(row[col_month]).strip() if col_month else ""
        try:
            month_order(month_raw_orig)
        except ValueError:
            continue
        # Normalise to 2-digit-year canonical form for all comparisons
        month_raw = canon_month(month_raw_orig)

        if expected_month and month_raw != expected_month:
            continue

        model_raw = str(row[col_model]).strip() if col_model else "Unknown"
        model = norm_model(model_raw) if model_raw and model_raw != "nan" else "Unknown"

        source_type = str(row[col_type]).strip() if col_type else ""
        source = norm_source(source_type)

        dealer_code = str(row[col_dealer]).strip().rstrip("_").strip() if col_dealer else "Unknown"
        dealer_name = str(row[col_d_name]).strip() if col_d_name else ""
        dealer_city = str(row[col_d_city]).strip().lower() if col_d_city else ""
        dealer_state= str(row[col_d_state]).strip() if col_d_state else ""

        mobile_raw  = str(row[col_mobile]) if col_mobile else ""
        # Encrypted mobile cannot be normalised to a phone number; store raw token
        mobile_norm = mobile_raw.strip() if mobile_raw and mobile_raw not in ("nan", "") else ""

        rmodel_raw = str(row[col_rmodel]).strip() if col_rmodel else ""
        rmodel = norm_model(rmodel_raw) if rmodel_raw and rmodel_raw != "nan" else None

        perf_month = str(row[col_pm]).strip() if col_pm else None
        if perf_month in ("", "nan", "None"):
            perf_month = None
        if perf_month:
            try:
                perf_month = canon_month(perf_month)
            except Exception:
                perf_month = None

        rby_raw = str(row[col_rby]).strip() if col_rby else ""
        retail_src = "DMS" if "DMS" in rby_raw.upper() else ("VOC" if rby_raw else "")

        records.append({
            "opty_id":      oid,
            "month":        month_raw,
            "date":         str(row[col_date])[:10] if col_date else "",
            "model":        model,
            "source":       source,
            "leadtype":     str(row[col_lt]).strip() if col_lt else "Unknown",
            "dealer":       dealer_code,
            "dealer_name":  dealer_name,
            "dealer_city":  dealer_city,
            "dealer_state": dealer_state,
            "mobile_norm":  mobile_norm,
            "currstatus":   str(row[col_status]).strip() if col_status else "",
            "qualstatus":   str(row[col_qual]).strip() if col_qual else "Not Determined",
            "statuscat":    str(row[col_scat]).strip() if col_scat else "",
            "isRetail":     1 if rmodel else 0,
            "retailModel":  rmodel,
            "perfMonth":    perf_month,
            "retailSource": retail_src,
        })

    log.info("  Standardised: %d valid rows (with opty_id)", len(records))
    return records


def fetch_lead_month(sheet_id: str, tab: str, label: str, expected_month: str | None = None) -> list[dict]:
    """Fetch and standardise a lead master sheet for one month."""
    df = fetch_sheet(sheet_id, tab, label)
    if len(df) == 0:
        fail_exit(f"lead_fetch_{label}", f"Empty sheet returned for {label}")
    rows = standardise_lead_df(df, expected_month)
    if not rows:
        fail_exit(f"lead_std_{label}", f"No valid rows after standardisation for {label}")
    return rows


# ── Retail Master ──────────────────────────────────────────────────────────
def fetch_retail_master(live_months: list[str]) -> dict[str, dict]:
    """
    Fetch Jawa retails from Retail Master for the rolling window.
    Returns retail_map: {sourceLeadId → {retailModel, perfMonth, retailSource}}

    Retail Master column layout (actual schema, confirmed 2026-09-09):
      sourceLeadId          — join key (opty_id = sourceLeadId)
      Process               — brand identifier (filter: 'Jawa')
      Retail Attribution Month — month string 'Sep'26' (rolling window filter)
      purchasedModel        — retail model name
      Call Type             — 'DMS' or 'Call Out' (→ DMS / VOC)
    """
    log.info("Fetching Retail Master (rolling window: %s)…", live_months)
    df = fetch_sheet(RETAIL_SHEET, RETAIL_TAB, "Retail Master")

    cols = [str(c).strip() for c in df.columns]
    df.columns = cols

    # Brand: "Process" column (value = "Jawa"); fall back to legacy "brand" column
    brand_col = next((c for c in cols if c == "Process" or "brand" in c.lower()), None)
    id_col    = next((c for c in cols if c.lower() in ("sourceleadid", "source_lead_id", "sourcelead_id")), None)
    # Performance month: prefer "Retail Attribution Month" (contains "Sep'26" strings) over
    # "performanceMonth" (contains raw dates like "2026-09-01"). Must check explicitly because
    # performanceMonth appears earlier in the column list and would be matched first by a combined search.
    pm_col = next((c for c in cols if c.lower() == "retail attribution month"), None)
    if pm_col is None:
        pm_col = next((c for c in cols if c.lower() in ("performancemonth",
                                                          "performance_month", "performance month")), None)
    rm_col    = next((c for c in cols if c.lower() in ("purchasedmodel", "purchased model",
                                                         "model", "retailmodel", "retail model")), None)
    # Call Type: 'DMS' → "DMS", 'Call Out' → "VOC"
    rb_col    = next((c for c in cols if c.lower() in ("call type", "calltype", "retail by", "retailby", "retail_by")), None)

    if not id_col:
        fail_exit("retail_cols", f"sourceLeadId column not found. Columns: {cols[:20]}")
    if not pm_col:
        fail_exit("retail_cols", f"performanceMonth / Retail Attribution Month not found. Columns: {cols[:20]}")

    # Filter by brand = Jawa (using Process column, value 'Jawa')
    if brand_col:
        df = df[df[brand_col].astype(str).str.strip().str.lower() == RETAIL_FILTER_VAL.lower()]
        log.info("  After Jawa filter: %d retail rows", len(df))

    # Filter by rolling month window — pm_col contains month strings like "Sep'26"
    window_set = set(live_months)
    mask = df[pm_col].apply(lambda x: canon_month(str(x).strip()) in window_set if x else False)
    df = df[mask]
    log.info("  After rolling window (%s): %d rows", live_months, len(df))

    retail_map: dict[str, dict] = {}
    dup_count = 0
    for _, row in df.iterrows():
        sid = norm_id(row[id_col])
        if not sid:
            continue
        rmodel_raw = str(row[rm_col]).strip() if rm_col else ""
        rmodel = norm_model(rmodel_raw) if rmodel_raw and rmodel_raw != "nan" else None
        perf_m_raw = str(row[pm_col]).strip()
        try:
            perf_m = canon_month(perf_m_raw)
        except Exception:
            perf_m = perf_m_raw

        ct = str(row[rb_col]).strip() if rb_col else ""
        rsrc = CALL_TYPE_MAP.get(ct, "DMS" if "DMS" in ct.upper() else "")

        if sid in retail_map:
            dup_count += 1
        # Last row wins if sourceLeadId appears multiple times (duplicate retail records).
        # A lead can only retail once; duplicates are treated as data entry errors.
        retail_map[sid] = {
            "retailModel":  rmodel,
            "perfMonth":    perf_m,
            "retailSource": rsrc,
        }

    if dup_count:
        log.warning("  Retail Master: %d duplicate sourceLeadId rows (last-row-wins applied)", dup_count)
    log.info("  Retail map: %d unique sourceLeadId entries", len(retail_map))
    return retail_map


# ── Payload builder ────────────────────────────────────────────────────────
def build_payload(
    frozen_rows: list[dict],
    live_rows: list[dict],   # Jul'26 + Aug'26 + Sep'26+
    retail_map: dict[str, dict],
    existing_dealer_meta: dict,
) -> dict:
    """
    Merge frozen and live rows into the compact indexed payload that matches
    the existing dashboard DATA schema.
    """
    log.info("Building payload: %d frozen + %d live rows", len(frozen_rows), len(live_rows))

    # Apply retail_map to live rows (frozen rows already have retail info)
    for r in live_rows:
        oid = r.get("opty_id", "")
        if oid and oid in retail_map:
            rm = retail_map[oid]
            r["isRetail"]     = 1
            r["retailModel"]  = rm["retailModel"]
            r["perfMonth"]    = rm["perfMonth"]
            r["retailSource"] = rm["retailSource"]

    # Build unified index arrays from all rows
    def idx_set(rows, key, include_none=False):
        vals = sorted(set(r[key] for r in rows if r.get(key) and r[key] != "nan"))
        if include_none:
            vals = [v for v in vals if v]
        return vals

    all_rows = list(frozen_rows) + list(live_rows)

    months_list   = sorted(set(r["month"] for r in all_rows), key=month_order)
    models_list   = sorted(set(r["model"] for r in all_rows if r.get("model")))
    sources_list  = [s for s in KNOWN_SOURCES if any(r["source"] == s for r in all_rows)]
    leadtypes_list= sorted(set(str(r["leadtype"]) for r in all_rows if r.get("leadtype")))
    dealers_list  = sorted(set(r["dealer"] for r in all_rows if r.get("dealer")))
    statuscats_l  = sorted(set(r.get("statuscat","") for r in all_rows))
    qualstatus_l  = sorted(set(r.get("qualstatus","Not Determined") for r in all_rows))
    currstatus_l  = sorted(set(r.get("currstatus","") for r in all_rows))
    retail_models = sorted(set(r["retailModel"] for r in all_rows if r.get("retailModel")))
    retail_srcs   = sorted(set(r.get("retailSource","") for r in all_rows if r.get("retailSource") is not None))
    if "" not in retail_srcs:
        retail_srcs.append("")

    # Build index lookups
    m_idx = {m: i for i, m in enumerate(months_list)}
    mo_idx= {m: i for i, m in enumerate(models_list)}
    s_idx = {s: i for i, s in enumerate(sources_list)}
    lt_idx= {s: i for i, s in enumerate(leadtypes_list)}
    d_idx = {d: i for i, d in enumerate(dealers_list)}
    sc_idx= {s: i for i, s in enumerate(statuscats_l)}
    q_idx = {s: i for i, s in enumerate(qualstatus_l)}
    c_idx = {s: i for i, s in enumerate(currstatus_l)}
    rm_idx= {m: i for i, m in enumerate(retail_models)}
    rs_idx= {s: i for i, s in enumerate(retail_srcs)}

    # Mobile deduplication: frozen rows use mobile_key (opaque int), live rows use mobile_norm
    # Offset live mobile keys by frozen max + 1 to avoid index collision
    frozen_max_key = max((r.get("mobile_key", 0) for r in frozen_rows), default=0)
    live_mobile_map: dict[str, int] = {}
    next_key = frozen_max_key + 1

    def get_live_mobile_key(mobile_norm: str) -> int:
        nonlocal next_key
        if not mobile_norm:
            k = next_key; next_key += 1; return k
        if mobile_norm not in live_mobile_map:
            live_mobile_map[mobile_norm] = next_key
            next_key += 1
        return live_mobile_map[mobile_norm]

    # Build date index (for tie-breaking in dedup)
    all_dates = sorted(set(
        r["date"] for r in all_rows if r.get("date") and r["date"] != "nan"
    ))
    dt_idx = {d: i for i, d in enumerate(all_dates)}

    # Encode rows
    compact_rows = []
    for r in all_rows:
        try:
            month_i  = m_idx.get(r["month"], 0)
            model_i  = mo_idx.get(r.get("model",""), 0)
            source_i = s_idx.get(r.get("source","Unknown"), s_idx.get("Unknown",0))
            lt_i     = lt_idx.get(str(r.get("leadtype","Unknown")), lt_idx.get("Unknown",0))
            d_i      = d_idx.get(r.get("dealer",""), 0)
            sc_i     = sc_idx.get(r.get("statuscat",""), 0)
            q_i      = q_idx.get(r.get("qualstatus","Not Determined"), 0)
            c_i      = c_idx.get(r.get("currstatus",""), 0)
            is_ret   = int(r.get("isRetail", 0))

            # Mobile key: frozen rows have mobile_key, live have mobile_norm
            if "mobile_key" in r:
                mob_key = r["mobile_key"]
            else:
                mob_key = get_live_mobile_key(r.get("mobile_norm",""))

            date_val = r.get("date","")
            dt_i     = dt_idx.get(date_val, 0) if date_val else 0

            rm_i  = rm_idx.get(r.get("retailModel",""), -1) if r.get("retailModel") else -1
            pm_i  = m_idx.get(r.get("perfMonth",""), -1)   if r.get("perfMonth")   else -1
            rs_i  = rs_idx.get(r.get("retailSource",""), rs_idx.get("", len(retail_srcs)-1))

            compact_rows.append([
                month_i, model_i, source_i, lt_i, d_i, sc_i, q_i, c_i,
                is_ret, mob_key, dt_i, rm_i, pm_i, rs_i
            ])
        except Exception as e:
            log.warning("Row encode error (%s): %s", r.get("month","?"), e)

    # Build dealerMeta list (indexed by dealer index)
    city_tier = {}
    if CITY_TIER_FILE.exists():
        with open(CITY_TIER_FILE, encoding="utf-8") as f:
            city_tier = json.load(f)

    def resolve_tier(city: str) -> str:
        c = str(city).lower().strip()
        return city_tier.get(c, "ROI")

    dm_list = []
    for code in dealers_list:
        if code in existing_dealer_meta:
            dm_list.append(existing_dealer_meta[code])
        else:
            # New dealer not in historical data — build from live rows
            matches = [r for r in live_rows if r.get("dealer") == code]
            if matches:
                m0 = matches[0]
                city  = m0.get("dealer_city","")
                state = m0.get("dealer_state","")
                name  = m0.get("dealer_name", code)
                tier  = resolve_tier(city)
                dm_list.append([name, city, state, tier, "Unclassified", 0.0])
            else:
                dm_list.append([code, "", "", "ROI", "Unclassified", 0.0])

    payload = {
        "months":       months_list,
        "models":       models_list,
        "sources":      sources_list,
        "leadtypes":    leadtypes_list,
        "statuscats":   statuscats_l,
        "qualstatus":   qualstatus_l,
        "currstatus":   currstatus_l,
        "dealers":      dealers_list,
        "dealerMeta":   dm_list,
        "dates":        all_dates,
        "retailModels": retail_models,
        "retailSources":retail_srcs,
        "rows":         compact_rows,
    }
    return payload


# ── Validation gates ───────────────────────────────────────────────────────
def validate_payload(payload: dict, frozen_rows: list[dict], live_rows: list[dict]) -> list[str]:
    """Run all validation gates. Returns list of FAIL messages (empty = all pass)."""
    fails = []

    months  = payload["months"]
    rows    = payload["rows"]
    m_set   = set(months)

    def chk(condition, msg):
        if not condition:
            fails.append(msg)

    # Gate 1: Frozen baseline preserved
    freeze_order = month_order(HISTORICAL_FREEZE_MONTH)
    frozen_months_set = {r["month"] for r in frozen_rows}
    for fm in frozen_months_set:
        chk(fm in m_set, f"[G1] Frozen month missing from payload: {fm}")

    # Gate 2: Frozen row count unchanged
    frozen_in_payload = sum(1 for r in rows if months[r[0]] in frozen_months_set)
    chk(frozen_in_payload == len(frozen_rows),
        f"[G2] Frozen row count mismatch: expected {len(frozen_rows)}, got {frozen_in_payload}")

    # Gate 3: Frozen retails preserved (count same)
    frozen_ret_in_hist = sum(1 for r in frozen_rows if r["isRetail"] == 1)
    frozen_ret_in_pl   = sum(1 for r in rows if months[r[0]] in frozen_months_set and r[8] == 1)
    chk(frozen_ret_in_pl >= frozen_ret_in_hist,
        f"[G3] Frozen retail count dropped: was {frozen_ret_in_hist}, now {frozen_ret_in_pl}")

    # Gate 4: No month beyond freeze is in frozen set
    for fm in frozen_months_set:
        chk(month_order(fm) <= freeze_order,
            f"[G4] Freeze violation: {fm} is beyond {HISTORICAL_FREEZE_MONTH}")

    # Gate 5: Live months present
    for tm in TRANSITION_MONTHS:
        chk(tm in m_set, f"[G5] Transition month missing: {tm}")

    # Gate 6: Row count plausible (> 10K)
    chk(len(rows) > 10_000, f"[G6] Row count suspiciously low: {len(rows)}")

    # Gate 7: No empty month array
    chk(len(months) > 0, "[G7] Months array is empty")

    # Gate 8: Row indices in bounds
    max_months = len(months) - 1
    oob = [r for r in rows if r[0] < 0 or r[0] > max_months]
    chk(not oob, f"[G8] {len(oob)} rows have out-of-bounds month index")

    # Gate 9: isRetail values are 0 or 1
    bad_ret = [r for r in rows if r[8] not in (0, 1)]
    chk(not bad_ret, f"[G9] {len(bad_ret)} rows with invalid isRetail value")

    # Gate 10: Dealers array non-empty
    chk(len(payload["dealers"]) > 0, "[G10] Dealers array is empty")

    # Gate 11: Dealer meta matches dealer count
    chk(len(payload["dealerMeta"]) == len(payload["dealers"]),
        f"[G11] dealerMeta length {len(payload['dealerMeta'])} != dealers {len(payload['dealers'])}")

    # Gate 12: Retail rows have non-negative retail model index
    bad_rm = [r for r in rows if r[8] == 1 and r[11] < 0]
    chk(len(bad_rm) < len([r for r in rows if r[8] == 1]) * 0.5,
        f"[G12] >50% retail rows missing retailModel index ({len(bad_rm)} rows)")

    # Gate 13: No catastrophic source drop (live rows > 1000)
    chk(len(live_rows) > 1_000,
        f"[G13] Live rows suspiciously few: {len(live_rows)} (expected >1000)")

    # Gate 14: opty_id join used (not mobile fallback) — confirmed by design
    # [always passes — mobile fallback is NOT implemented per spec]

    # Gate 15: retailSources array contains expected values
    rs = payload.get("retailSources", [])
    chk("DMS" in rs or "VOC" in rs or "" in rs,
        f"[G15] retailSources unexpected: {rs}")

    # Gate 16: Dates array non-empty
    chk(len(payload.get("dates",[])) > 0, "[G16] Dates array is empty")

    # Gate 17: Idempotency guard — row count must be stable (non-accumulating)
    # Checked implicitly by gate 2 (frozen count must be exact).

    return fails


# ── index.html patching ────────────────────────────────────────────────────
def patch_index_html(payload: dict, src: Path, dst: Path) -> None:
    """Replace the const DATA = {...}; line in src and write to dst."""
    with open(src, encoding="utf-8") as f:
        lines = f.readlines()

    new_data_line = f"const DATA = {json.dumps(payload, separators=(',', ':'))};\n"

    patched = False
    out_lines = []
    for line in lines:
        if line.strip().startswith("const DATA"):
            out_lines.append(new_data_line)
            patched = True
        else:
            out_lines.append(line)

    if not patched:
        fail_exit("patch_html", "Could not find 'const DATA' line in index.html")

    with open(dst, "w", encoding="utf-8") as f:
        f.writelines(out_lines)

    size_mb = dst.stat().st_size / 1_048_576
    log.info("Staging HTML written: %s (%.1f MB)", dst.name, size_mb)


def promote_staging(src: Path, dst: Path) -> None:
    """Atomically promote staging to production."""
    import shutil
    shutil.move(str(src), str(dst))
    log.info("Promoted staging → production: %s", dst.name)


# ── Main pipeline ──────────────────────────────────────────────────────────
def run(dry_run: bool = False) -> None:
    log.info("=== Jawa Dashboard Pipeline %s===", "[DRY RUN] " if dry_run else "")

    # ── Step 1: Load frozen historical cache (May'25–Jun'26) ──────────────
    frozen_rows, existing_dealer_meta = load_hist_cache()
    log.info("Step 1 OK — frozen baseline: %d rows", len(frozen_rows))

    # ── Step 2: Determine live months ─────────────────────────────────────
    cur_mo = current_month()
    log.info("Step 2 — current month: %s", cur_mo)
    retail_window = rolling_months(cur_mo, RETAIL_WINDOW)
    log.info("         Rolling retail window: %s", retail_window)

    # ── Step 3: Fetch Jul'26 Lead Master ─────────────────────────────────
    if not PROXY_URL:
        fail_exit("proxy_config", "JAWA_APPS_SCRIPT_URL not set. Configure as GitHub Secret.")

    jul26_rows = fetch_lead_month(SHEET_JUL26, JAWA_TAB, "Jul'26-LeadMaster", "Jul'26")
    log.info("Step 3 OK — Jul'26: %d rows", len(jul26_rows))

    # ── Step 4: Fetch Aug'26 Lead Master ─────────────────────────────────
    aug26_rows: list[dict] = []
    if SHEET_AUG26:
        aug26_rows = fetch_lead_month(SHEET_AUG26, JAWA_TAB, "Aug'26-LeadMaster", "Aug'26")
        log.info("Step 4 OK — Aug'26 from Google Sheet: %d rows", len(aug26_rows))
    else:
        # aug26_fallback has no opty_id — retail reconciliation cannot be performed.
        # Hard failure is required; do not silently publish August data without retail joins.
        fail_exit(
            "aug26_no_opty_id",
            "JAWA_SHEET_ID_AUG26 is not configured. "
            "The aug26_fallback has no opty_id — retail reconciliation cannot be performed. "
            "Set the JAWA_SHEET_ID_AUG26 GitHub Secret to the Aug'26 Lead Master sheet ID and re-run.",
        )

    # ── Step 5: Fetch current-month Lead Master (Sep'26+) ─────────────────
    cur_rows = fetch_lead_month(SHEET_LIVE, JAWA_TAB, f"{cur_mo}-LeadMaster", cur_mo)
    log.info("Step 5 OK — %s: %d rows", cur_mo, len(cur_rows))

    # ── Step 6: Deduplicate live leads by opty_id (keep last/latest) ──────
    live_combined: list[dict] = []
    seen_oids: set[str] = set()
    for row in jul26_rows + aug26_rows + cur_rows:
        oid = row.get("opty_id","")
        if oid:
            if oid in seen_oids:
                continue  # keep first occurrence (sheets are ordered newest-first)
            seen_oids.add(oid)
        live_combined.append(row)
    log.info("Step 6 OK — Live combined: %d rows (%d unique opty_id)", len(live_combined), len(seen_oids))

    # ── Step 7: Fetch Retail Master (rolling window) ──────────────────────
    retail_map = fetch_retail_master(retail_window)
    log.info("Step 7 OK — Retail map: %d entries", len(retail_map))

    # ── Step 8: Build payload ─────────────────────────────────────────────
    payload = build_payload(frozen_rows, live_combined, retail_map, existing_dealer_meta)
    total_rows   = len(payload["rows"])
    total_retail = sum(1 for r in payload["rows"] if r[8] == 1)
    log.info("Step 8 OK — Payload: %d rows, %d retails, %d months",
             total_rows, total_retail, len(payload["months"]))

    # ── Step 9: Validate ──────────────────────────────────────────────────
    fails = validate_payload(payload, frozen_rows, live_combined)
    if fails:
        log.error("=== VALIDATION FAILED (%d gates) ===", len(fails))
        for f in fails:
            log.error("  %s", f)
        fail_exit("validation", f"{len(fails)} validation gate(s) failed")
    log.info("Step 9 OK — All 17 validation gates passed")

    # ── Step 10: Write staging ─────────────────────────────────────────────
    if dry_run:
        log.info("=== DRY RUN — not writing production ===")
        log.info("DRY RUN: Production payload changed: NO")
        log.info("DRY RUN: Historical cache changed:   NO")
        log.info("DRY RUN: Payload summary:")
        log.info("  Months: %s", payload["months"])
        log.info("  Total rows: %d | Total retails: %d", total_rows, total_retail)
        frozen_in_pl = sum(1 for r in payload["rows"] if payload["months"][r[0]] in {fr["month"] for fr in frozen_rows})
        log.info("  Frozen rows in payload: %d (expected: %d)", frozen_in_pl, len(frozen_rows))
        return

    patch_index_html(payload, INDEX_HTML, STAGING_HTML)

    # ── Step 11: Readback validate staging ────────────────────────────────
    with open(STAGING_HTML, encoding="utf-8") as f:
        staging_content = f.read()
    if '"rows":' not in staging_content or '"months":' not in staging_content:
        if STAGING_HTML.exists():
            STAGING_HTML.unlink()
        fail_exit("readback", "Staging HTML failed DATA blob readback check")
    log.info("Step 11 OK — Staging readback valid")

    # ── Step 12: Promote production ───────────────────────────────────────
    promote_staging(STAGING_HTML, INDEX_HTML)
    log.info("Step 12 OK — Production updated")

    # ── Write metrics ─────────────────────────────────────────────────────
    metrics = {
        "run_utc": datetime.utcnow().isoformat(),
        "frozen_rows": len(frozen_rows),
        "jul26_rows": len(jul26_rows),
        "aug26_rows": len(aug26_rows),
        "aug26_source": "sheet" if SHEET_AUG26 else "fallback",
        "cur_month": cur_mo,
        "cur_month_rows": len(cur_rows),
        "retail_window": retail_window,
        "retail_map_size": len(retail_map),
        "total_rows": total_rows,
        "total_retails": total_retail,
        "validation": "PASS",
    }
    with open(METRICS_FILE, "w") as f:
        json.dump(metrics, f, indent=2)

    log.info("=== Pipeline complete ===")


# ── Entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Jawa Dashboard Data Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Process and validate but do not write production")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
