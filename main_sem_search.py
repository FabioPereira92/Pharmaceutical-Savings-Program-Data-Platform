import openpyxl
import sqlite3
from datetime import datetime, timezone
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
# FIX: selenium.webdriver.common.exceptions import error in newer selenium
import selenium.common.exceptions as selenium_exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
from typing import Optional, List, Dict, Any, Tuple
import logging
import os
import re
import json
import ast
from dotenv import load_dotenv
from urllib.parse import (
    urljoin,
    quote_plus,
    urlparse,
    urlunparse,
    parse_qsl,
    urlencode,
)

# REFACTORING NOTE: crawl4ai is now used for HTML page fetching in ai_extract_full_schema_from_page
# and ai_extract_full_schema_two_pass functions. Selenium is ONLY used for:
# 1. GoodRx manufacturer modal scraping (get_goodrx_manufacturer_modal)
# 2. co-pay.com fallback extraction (co_pay_search_and_extract)
# 3. Bot-walled / JS-shell fallback extraction (selenium_rendered_fetch)
# 4. PDF URLs still route to ai_extract_from_pdf (PyMuPDF/pdfplumber)
from crawl4ai_fetch import crawl4ai_fetch

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

try:
    import openai
except Exception:
    openai = None

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    import requests
except Exception:
    requests = None


# =========================
# Constants
# =========================
GENERIC_ENGINE_PATHS = (
    "/discount-card",
    "/discountcard",
    "/savings-card",
    "/savingscard",
    "/drug-coupons",
    "/coupons",
    "/coupon",
    "/card",
    "/cards",
)

DENY_ALWAYS_HOSTS = {
    "wikipedia.org",
    "www.wikipedia.org",
    "rxlist.com",
    "www.rxlist.com",
    "webmd.com",
    "www.webmd.com",
    "healthline.com",
    "www.healthline.com",
    "verywellhealth.com",
    "www.verywellhealth.com",
    "nicelocal.com",
    "www.nicelocal.com",
}

DISCOUNT_ENGINE_HOSTS = {
    "drugs.com",
    "www.drugs.com",
    "insiderx.com",
    "www.insiderx.com",
    "singlecare.com",
    "www.singlecare.com",
    "rxsaver.com",
    "www.rxsaver.com",
    "wellrx.com",
    "www.wellrx.com",
    "drugmart.com",
    "www.drugmart.com",
    "nowpatient.com",
    "www.nowpatient.com",
    "benefitsexplorer.com",
    "www.benefitsexplorer.com",
    "prescriptionbliss.com",
    "www.prescriptionbliss.com",
}

GOODRX_HOSTS = {"goodrx.com", "www.goodrx.com"}

AGGREGATOR_HOSTS = {
    "drugs.com",
    "www.drugs.com",
    "goodrx.com",
    "www.goodrx.com",
    "insiderx.com",
    "www.insiderx.com",
    "singlecare.com",
    "www.singlecare.com",
    "webmd.com",
    "www.webmd.com",
    "rxlist.com",
    "www.rxlist.com",
    "healthline.com",
    "www.healthline.com",
    "verywellhealth.com",
    "www.verywellhealth.com",
    "wikipedia.org",
    "www.wikipedia.org",
    "nowpatient.com",
    "www.nowpatient.com",
    "benefitsexplorer.com",
    "www.benefitsexplorer.com",
    "prescriptionbliss.com",
    "www.prescriptionbliss.com",
    "rxsaver.com",
    "www.rxsaver.com",
    "wellrx.com",
    "www.wellrx.com",
    "nicelocal.com",
    "www.nicelocal.com",
    "drugmart.com",
    "www.drugmart.com",
}

BAD_PATH_KEYWORDS = [
    "privacy",
    "cookie",
    "cookies",
    "terms-of-use",
    "terms",
    "legal",
    "sitemap",
    "careers",
    "jobs",
    "press",
    "newsroom",
    "investors",
    "about",
    "contact-us",
    "facebook",
    "twitter",
    "instagram",
    "linkedin",
    "youtube",
    "signup-newsletter",
]

GOOD_PATH_KEYWORDS = [
    "copay",
    "co-pay",
    "savings",
    "saving",
    "card",
    "coupon",
    "discount",
    "support",
    "assistance",
    "patient",
    "enroll",
    "enrollment",
    "register",
    "activate",
    "download",
    "pdf",
    "terms-and-conditions",
    "eligibility",
    "get-started",
    "apply",
]

GENERIC_BAD_PATHS = [
    "/discount-card",
    "/discount-card/",
    "/savings-card",
    "/savings-card/",
    "/coupons",
    "/coupons/",
]

# For program picking / ranking (single source of truth)
TIER_PRIORITY = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "": 5}
TYPE_PRIORITY = {
    "copay": 0,
    "pap": 1,
    "bridge_fill": 2,
    "rebate": 3,
    "foundation": 4,
    "support": 5,
    "discount_card": 6,
}


# =========================
# Small shared helpers (MUST be defined before reduce_to_single_program)
# =========================
def _text(x) -> str:
    return (x or "").strip() if isinstance(x, str) else ""


def _sanitize_url_http(u: str) -> str:
    u = _text(u)
    if not u:
        return ""
    try:
        p = urlparse(u)
        if (p.scheme or "").lower() in {"http", "https"} and (p.netloc or ""):
            return u
    except Exception:
        pass
    return ""


def _program_completeness(p: dict) -> int:
    """
    DROP-IN REPLACEMENT (NEW SCHEMA):
    Scores how complete/useful a program looks using the new full schema.
    """
    if not isinstance(p, dict):
        return 0

    score = 0

    if _text(p.get("name")):
        score += 2
    if _text(p.get("type")):
        score += 1

    # Benefit TLDRs (any branch)
    bl = p.get("benefit_logic") if isinstance(p.get("benefit_logic"), dict) else {}
    if isinstance(bl, dict):
        found_tldr = False
        for branch in (
            "covered",
            "not_covered",
            "uninsured",
            "restrictions_not_met",
            "bridge_fill",
        ):
            node = bl.get(branch) if isinstance(bl.get(branch), dict) else {}
            summ = node.get("summary") if isinstance(node.get("summary"), dict) else {}
            if _text(summ.get("tldr")):
                found_tldr = True
                break
        if found_tldr:
            score += 3

    # Program summary
    summ = p.get("summary") if isinstance(p.get("summary"), dict) else {}
    if _text(summ.get("tldr")):
        score += 2
    if _text(summ.get("ui_blurb")):
        score += 1

    # Eligibility signals (insurance tokens etc.)
    elig = p.get("eligibility") if isinstance(p.get("eligibility"), dict) else {}
    if isinstance(elig, dict):
        inc = elig.get("insurance_included")
        exc = elig.get("insurance_excluded")
        if isinstance(inc, list) and len(inc) > 0:
            score += 1
        if isinstance(exc, list) and len(exc) > 0:
            score += 1
        if elig.get("cash_payers_excluded") is not None:
            score += 1
        if elig.get("requires_valid_rx") is not None:
            score += 1
        if elig.get("requires_fda_label_use") is not None:
            score += 1
        if elig.get("state_limitations") is not None:
            score += 1

    # Contact + CTA
    contact = p.get("contact") if isinstance(p.get("contact"), dict) else {}
    if _text(contact.get("phone")):
        score += 1
    if _text(contact.get("email")):
        score += 1

    cta = p.get("cta") if isinstance(p.get("cta"), dict) else {}
    if _sanitize_url_http(cta.get("url")):
        score += 2
    if isinstance(cta.get("enrollment_required"), bool):
        score += 1

    return score


def _actionability(p: dict) -> int:
    """
    DROP-IN REPLACEMENT (NEW SCHEMA):
    Scores how "actionable" the program is (can the user do something now?).
    New schema has no cta.label, so this is URL-driven.
    """
    if not isinstance(p, dict):
        return 0

    cta = p.get("cta") if isinstance(p.get("cta"), dict) else {}
    url = _sanitize_url_http(cta.get("url"))
    if not url:
        return 0

    u = url.lower()
    score = 10

    # PDFs are often directly usable (print/download card, terms, enrollment form)
    if u.split("?")[0].endswith(".pdf"):
        score += 25

    # Strong intent keywords in URL
    strong = (
        "download",
        "print",
        "get",
        "card",
        "activate",
        "enroll",
        "enrollment",
        "register",
        "apply",
        "copay",
        "savings",
    )
    if any(k in u for k in strong):
        score += 15

    # Weak/ informational destinations
    weak = ("faq", "resources", "learn-more", "learn_more", "about", "support", "contact", "privacy", "terms")
    if any(k in u for k in weak):
        score -= 8

    # enrollment_required present (not necessarily better, but indicates specificity)
    if isinstance(cta.get("enrollment_required"), bool):
        score += 2

    return score


def reduce_to_single_program(obj: dict) -> dict:
    """
    Keep exactly one program per drug.
    Prefer copay; remove noise. Deterministic best-pick.
    """
    if not isinstance(obj, dict):
        return obj

    programs = obj.get("programs")
    if not isinstance(programs, list) or not programs:
        obj["programs"] = []
        return obj

    # Sanitize cta URLs (no tel:, no garbage)
    for p in programs:
        if isinstance(p, dict):
            cta = p.get("cta")
            if isinstance(cta, dict):
                cta["url"] = _sanitize_url_http(cta.get("url"))

    # If there's any copay program, drop pure "support" noise unless it's the only thing
    has_copay = any(isinstance(p, dict) and _text(p.get("type")).lower() == "copay" for p in programs)
    if has_copay:
        programs = [p for p in programs if isinstance(p, dict) and _text(p.get("type")).lower() != "support"] or programs

    def key(p: dict):
        tier = _text(p.get("confidence_tier")).upper()
        tp = TIER_PRIORITY.get(tier, 5)
        typ = _text(p.get("type")).lower()
        ty = TYPE_PRIORITY.get(typ, 99)
        act = _actionability(p)
        comp = _program_completeness(p)
        has_url = 0 if _sanitize_url_http((p.get("cta") or {}).get("url")) else 1
        # Sort ascending: type priority, tier, then prefer higher actionability & completeness
        return (ty, tp, -act, -comp, has_url)

    best = None
    best_k = None
    for p in programs:
        if not isinstance(p, dict):
            continue
        k = key(p)
        if best is None or k < best_k:
            best = p
            best_k = k

    obj["programs"] = [best] if best else []
    obj.pop("primary_program", None)
    obj.pop("other_programs", None)
    return obj


# =========================
# FINAL AI schema post-processor (central enforcement)
# =========================
def postprocess_ai_extraction(
    ai_extraction_json: Optional[str],
    *,
    drop_if_no_programs: bool = True,
    drop_if_only_discount_card: bool = False,
) -> Optional[str]:
    """
    Central enforcement:
    - Always reduce to a single program if programs exist
    - Optionally drop schema if programs is empty
    - Optional: drop discount_card-only records
    """
    if not ai_extraction_json:
        return None
    try:
        data = json.loads(ai_extraction_json)
    except Exception:
        return None
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return None

    obj = data[0]
    programs = obj.get("programs")
    if not isinstance(programs, list):
        programs = []
        obj["programs"] = programs

    if not programs:
        return None if drop_if_no_programs else json.dumps([obj], ensure_ascii=False)

    obj = reduce_to_single_program(obj)

    progs = obj.get("programs") or []
    if drop_if_only_discount_card and progs:
        t = _text(progs[0].get("type")).lower()
        if t == "discount_card":
            return None

    if drop_if_no_programs and not (obj.get("programs") or []):
        return None

    return json.dumps([obj], ensure_ascii=False)


# =========================
# URL selection / ranking
# =========================
def _norm_host(u: str) -> str:
    try:
        return (urlparse(u).netloc or "").lower()
    except Exception:
        return ""


def _norm_path(u: str) -> str:
    try:
        return (urlparse(u).path or "").lower()
    except Exception:
        return ""


def _looks_like_pdf(u: str) -> bool:
    return (u or "").lower().split("?")[0].endswith(".pdf")


def _contains_drug_token(u: str, drug_name: str) -> bool:
    if not u or not drug_name:
        return False
    path = _norm_path(u)
    tokens = [t for t in re.split(r"[^a-z0-9]+", drug_name.lower()) if t and len(t) >= 4]
    return any(t in path for t in tokens)


def score_candidate_url(
    u: str,
    drug_name: str,
    preferred_domain: Optional[str] = None,
) -> float:
    if not u or not u.startswith("http"):
        return -999.0

    host = _norm_host(u)
    path = _norm_path(u)

    score = 0.0

    DENY_HOSTS = {
        "wemanufacturerdrugcoupons.com",
        "www.wemanufacturerdrugcoupons.com",
        "rxpharmacycoupons.com",
        "www.rxpharmacycoupons.com",
        "rxpharmacydiscount.com",
        "www.rxpharmacydiscount.com",
        "manufacturerdrugcoupons.com",
        "www.manufacturerdrugcoupons.com",
    }
    if host in DENY_HOSTS:
        return -999.0

    token_match = _contains_drug_token(u, drug_name)
    is_pdf = _looks_like_pdf(u)

    # Block generic discount-engine landing pages unless drug-specific
    if host in DISCOUNT_ENGINE_HOSTS:
        is_generic = any(path == p or path.startswith(p + "/") for p in GENERIC_ENGINE_PATHS)
        if is_generic and not token_match:
            return -999.0

    SAVINGS_KWS = ("savings", "save", "copay", "co-pay", "coupon", "rebate", "card", "voucher", "offer", "afford", "assistance")
    SUPPORT_KWS = ("support", "access", "coverage", "cost", "pricing", "pay", "patient", "financial", "enroll", "eligib")
    BAD_INTENT_KWS = ("faq", "blog", "news", "article", "press", "story", "how-to", "using-my-fsa", "hsa", "fsa")

    GENERIC_AGGREGATOR_PATH_PREFIXES = (
        "/discount-card",
        "/discountcard",
        "/savings-card",
        "/savingscard",
        "/coupon",
        "/coupons",
        "/card",
        "/cards",
        "/pharmacy",
        "/drug-coupons",
    )
    GENERIC_NAV_PREFIXES = ("/search", "/find", "/results", "/category", "/tags", "/tag", "/drugs", "/medications")
    GENERIC_ROOTS = ("/", "")

    # Hard rejects
    if path in GENERIC_ROOTS and not token_match:
        return -999.0

    if host in AGGREGATOR_HOSTS:
        if any(path == p or path.startswith(p + "/") for p in GENERIC_AGGREGATOR_PATH_PREFIXES):
            if not token_match:
                return -999.0

    if any(path == p or path.startswith(p + "/") for p in GENERIC_NAV_PREFIXES):
        if not token_match:
            return -999.0

    if preferred_domain:
        pd = preferred_domain.lower()
        if host == pd or host.endswith("." + pd):
            score += 45.0

    MANUFACTURER_HINTS = [
        "pfizer",
        "bms",
        "bristol",
        "myers",
        "squibb",
        "abbvie",
        "otsuka",
        "gsk",
        "merck",
        "novartis",
        "roche",
        "genentech",
        "sanofi",
        "astrazeneca",
        "lilly",
        "jnj",
        "janssen",
        "takeda",
        "amgen",
        "biogen",
        "gilead",
        "teva",
        "viatris",
        "bayer",
        "encompass",
        "accesssupport",
        "rxpathways",
    ]
    if any(h in host for h in MANUFACTURER_HINTS):
        score += 18.0

    for kw in BAD_PATH_KEYWORDS:
        if kw in path:
            score -= 25.0

    for bad in GENERIC_BAD_PATHS:
        if path == bad or path.startswith(bad + "/"):
            score -= 35.0

    has_savings_kw = any(k in path for k in GOOD_PATH_KEYWORDS) or any(k in path for k in SAVINGS_KWS)
    has_support_kw = any(k in path for k in SUPPORT_KWS)

    if any(k in path for k in BAD_INTENT_KWS) and not (has_savings_kw or has_support_kw):
        score -= 35.0

    if has_savings_kw:
        score += 18.0
    if has_support_kw:
        score += 8.0

    if token_match:
        score += 35.0
    else:
        score -= 55.0
        if any(h in host for h in MANUFACTURER_HINTS) and (has_savings_kw or has_support_kw):
            score += 25.0

    if is_pdf:
        if not any(k in path for k in ("savings", "copay", "card", "rebate", "voucher", "offer")):
            score -= 70.0
        else:
            score -= 10.0

    if host in AGGREGATOR_HOSTS:
        score -= 35.0

    if u.startswith("https://"):
        score += 1.0

    if 8 <= len(path) <= 80:
        score += 2.0

    if token_match and host.endswith(".com") and host.count(".") <= 2:
        score += 10.0

    return score


def pick_best_url(
    urls: List[str],
    drug_name: str,
    preferred_domain: Optional[str] = None,
    top_k: int = 8,
) -> List[str]:
    uniq = []
    seen = set()
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(u)

    scored = [(score_candidate_url(u, drug_name, preferred_domain), u) for u in uniq]
    scored.sort(key=lambda x: x[0], reverse=True)
    return [u for _, u in scored[:top_k]]


# =========================
# OpenAI compatibility wrapper
# =========================
def _openai_chat_create(messages, model="gpt-4.1", max_tokens=600, temperature=0.0):
    if openai is None:
        raise RuntimeError("openai module not available")

    # legacy 0.x
    try:
        if hasattr(openai, "ChatCompletion") and hasattr(openai.ChatCompletion, "create"):
            return openai.ChatCompletion.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
    except Exception:
        pass

    # new 1.x+
    try:
        if hasattr(openai, "OpenAI"):
            client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            return client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
    except Exception:
        raise

    # fallback namespace
    try:
        if hasattr(openai, "chat"):
            return openai.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
    except Exception:
        pass

    raise RuntimeError("No compatible openai chat completion method found")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =========================
# PDF detection and extraction
# =========================
def is_pdf_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url.lower())
    return (parsed.path or "").endswith(".pdf")


def extract_pdf_structured_text(pdf_path_or_url: str) -> Dict[str, Any]:
    result = {
        "title": "",
        "headings": [],
        "bullet_points": [],
        "tables": [],
        "raw_text": "",
        "phone_numbers": [],
        "dollar_amounts": [],
    }

    pdf_bytes = None
    filename = ""
    if pdf_path_or_url.startswith("http://") or pdf_path_or_url.startswith("https://"):
        if requests is None:
            return result
        try:
            response = requests.get(pdf_path_or_url, timeout=30)
            response.raise_for_status()
            pdf_bytes = response.content
            filename = pdf_path_or_url.split("/")[-1].replace(".pdf", "")
        except Exception as e:
            logging.warning(f"Failed to download PDF from {pdf_path_or_url}: {e}")
            return result
    else:
        filename = os.path.basename(pdf_path_or_url).replace(".pdf", "")
        try:
            with open(pdf_path_or_url, "rb") as f:
                pdf_bytes = f.read()
        except Exception as e:
            logging.warning(f"Failed to read local PDF {pdf_path_or_url}: {e}")
            return result

    # Try PyMuPDF (fitz) first
    if fitz is not None and pdf_bytes:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            result["title"] = doc.metadata.get("title", "") or filename

            full_text = ""
            for page in doc:
                text = page.get_text()
                full_text += text + "\n"

                # headings
                blocks = page.get_text("dict")["blocks"]
                for block in blocks:
                    if "lines" in block:
                        for line in block["lines"]:
                            for span in line["spans"]:
                                text_content = (span.get("text") or "").strip()
                                font_size = span.get("size", 0)
                                if text_content and (text_content.isupper() or font_size > 14):
                                    if text_content not in result["headings"]:
                                        result["headings"].append(text_content)

            result["raw_text"] = full_text
            doc.close()
        except Exception as e:
            logging.warning(f"PyMuPDF extraction failed: {e}")

    # Fallback to pdfplumber
    elif pdfplumber is not None and pdf_bytes:
        try:
            import io

            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                result["title"] = filename
                full_text = ""
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    full_text += text + "\n"
                    tables = page.extract_tables()
                    for table in tables:
                        if table:
                            result["tables"].append(table)
                result["raw_text"] = full_text
        except Exception as e:
            logging.warning(f"pdfplumber extraction failed: {e}")

    phone_pattern = r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}"
    phones = re.findall(phone_pattern, result["raw_text"])
    result["phone_numbers"] = list(set(phones))[:5]

    dollar_pattern = r"\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?"
    amounts = re.findall(dollar_pattern, result["raw_text"])
    result["dollar_amounts"] = list(set(amounts))[:10]

    lines = result["raw_text"].split("\n")
    for line in lines:
        stripped = line.strip()
        if stripped and (
            stripped[0] in ["•", "-", "*"]
            or (len(stripped) > 2 and stripped[0].isdigit() and stripped[1] in [".", ")"])
        ):
            result["bullet_points"].append(stripped)

    return result


def _extract_braced_json(text: str) -> Optional[str]:
    if not text:
        return None
    start_positions = [i for i, ch in enumerate(text) if ch == "{"]

    for start in start_positions:
        depth = 0
        for i in range(start, len(text)):
            ch = text[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start : i + 1]
                    if ":" in candidate and '"' in candidate:
                        return candidate
                    break
    return None


def _extract_balanced_json(text: str) -> Optional[str]:
    if not text:
        return None

    first_obj = text.find("{")
    first_arr = text.find("[")

    if first_obj == -1 and first_arr == -1:
        return None

    if first_obj == -1 or (first_arr != -1 and first_arr < first_obj):
        start = first_arr
        open_ch, close_ch = "[", "]"
    else:
        start = first_obj
        open_ch, close_ch = "{", "}"

    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def ai_extract_from_pdf(url: str, drug_name: str, model: str = "gpt-4.1") -> Tuple[Optional[str], str]:
    log_parts = []

    if openai is None:
        return None, "openai_not_installed"
    if not os.environ.get("OPENAI_API_KEY"):
        return None, "openai_api_key_missing"
    if fitz is None and pdfplumber is None:
        return None, "no_pdf_library_available"
    if requests is None:
        return None, "requests_library_missing"

    try:
        pdf_data = extract_pdf_structured_text(url)
        log_parts.append(f"pdf_text_len={len(pdf_data.get('raw_text') or '')}")
    except Exception as e:
        return None, f"pdf_extraction_failed:{e}"

    if not pdf_data.get("raw_text"):
        return None, "pdf_empty"

    hints = {
        "phone_numbers": pdf_data.get("phone_numbers") or [],
        "dollar_amounts": pdf_data.get("dollar_amounts") or [],
        "title": pdf_data.get("title") or "",
    }

    raw_text = pdf_data["raw_text"]
    MAX_TEXT = 200000
    if len(raw_text) > MAX_TEXT:
        raw_text = raw_text[:MAX_TEXT]
        log_parts.append("pdf_text_truncated")

    schema = r"""
    [
      {
        "drug": {
          "name": null,
          "generic": null,
          "manufacturer": null,
          "therapeutic_area": null,
          "route_form": null,
          "ndc_variants": null,
          "rxnorm": null
        },
        "programs": [
          {
            "program_id": null,
            "type": "copay | discount_card | pap | foundation | rebate | bridge_fill | support",
            "name": null,
            "eligibility": {
              "insurance_included": [],
              "insurance_excluded": [],
              "cash_payers_excluded": null,
              "requires_valid_rx": null,
              "requires_fda_label_use": null,
              "state_limitations": null
            },
            "benefit_logic": {
              "covered": {
                "pay_as_low_as": null,
                "max_monthly_savings": null,
                "max_annual_savings": null,
                "max_fills_per_year": null,
                "month_definition": null,
                "redemption_cadence": null,
                "summary": { "tldr": null }
              },
              "not_covered": {
                "cash_price": null,
                "patient_price_floor": null,
                "max_fills_lifetime": null,
                "days_supply": null,
                "max_savings_per_fill": null,
                "max_annual_savings": null,
                "pricing_details": null,
                "outside_insurance_note": null,
                "notes": null,
                "summary": { "tldr": null }
              },
              "restrictions_not_met": {
                "cash_price": null,
                "patient_price_floor": null,
                "max_fills_lifetime": null,
                "days_supply": null,
                "summary": { "tldr": null }
              },
              "uninsured": {
                "cash_price": null,
                "patient_price_floor": null,
                "max_fills_lifetime": null,
                "days_supply": null,
                "pricing_details": null,
                "outside_insurance_note": null,
                "notes": null,
                "summary": { "tldr": null }
              },
              "bridge_fill": {
                "one_time_fill": null,
                "max_tablets": null,
                "trigger": null,
                "summary": { "tldr": null }
              }
            },
            "compliance": {
              "not_insurance": null,
              "reimbursement_prohibited": null,
              "subject_to_change": null,
              "copay_maximizer_handling": null,
              "lab_test_support": null,
              "offer_validity": null,
              "rems_required": null,
              "notes": null
            },
            "contact": {
              "phone": null,
              "email": null
            },
            "cta": {
              "enrollment_required": null,
              "url": null
            },
            "summary": {
              "tldr": null,
              "operational_notes": null,
              "ui_blurb": null
            },
            "source_urls": [],
            "retrieved_at": null
          }
        ],
        "disclaimers": [],
        "sources": [
          {
            "url": null,
            "retrieved_at": null,
            "content_type": "pdf",
            "notes": null,
            "fields_supported": []
          }
        ],
        "summary": {
          "tldr": null,
          "recommended_next_step": null,
          "display_message": null
        }
      }
    ]
    """.strip()

    system = """
    You are a PDF information extraction engine for drug copay cards, rebate forms, and patient assistance documents.

    Extract ONLY explicitly stated information from the provided PDF TEXT and output STRICTLY valid JSON that conforms EXACTLY to the SCHEMA.

    HARD RULES (STRICT):
    - Output JSON ONLY. No markdown, no commentary, no trailing text.
    - The output MUST be a JSON array of objects (even if only one object).
    - The JSON MUST match the schema exactly: same keys, same nesting, no extra keys.
    - DO NOT GUESS. If not explicitly stated, use null (or [] for arrays).
    - Do not omit fields. If unknown, keep the field present with null / [].
    - Money fields are numbers (e.g., 25 not "$25"). Do not include currency symbols.
    - For any field named "url", use the PDF URL unless the PDF text explicitly provides a different enrollment URL.
    - If a timestamp is provided in the input, use ISO 8601 UTC; otherwise null.

    NORMALIZATION:
    - programs[].type must be exactly one of: copay, discount_card, pap, foundation, rebate, bridge_fill, support.
    - eligibility.insurance_included / insurance_excluded MUST contain ONLY these tokens (lowercase):
      commercial, uninsured, medicare, medicaid, tricare, va, state, other_government, state_pharmaceutical_assistance, medicare_part_d
      Include a token ONLY if the PDF explicitly mentions it.
    - eligibility.state_limitations is either null or an array of 2-letter state abbreviations (e.g., ["MA","CA"]).

    PROGRAM ID:
    - programs[].program_id: create a stable slug if possible using drug + program name/type (lowercase, underscores).
      Example: "zepbound_savings_card". If you cannot confidently name it, use null.

    BENEFIT LOGIC:
    Populate ALL benefit_logic branches. Each branch must include summary.tldr (1–2 sentences).
    - covered: use for commercially insured + covered by insurance.
    - not_covered: use for commercially insured + NOT covered by insurance.
    - restrictions_not_met: use for criteria not met / PA denial / step therapy not satisfied.
    - uninsured: use for uninsured/self-pay pathways or published self-pay price floors.
    - bridge_fill: use for temporary/free fills during verification/appeal.

    SOURCES / AUDIT:
    - programs[].source_urls MUST include the PDF URL.
    - sources[] MUST include one entry for the PDF URL with content_type="pdf".
    - sources[].fields_supported: list dotted JSON paths supported by the PDF (e.g., "programs[0].benefit_logic.covered.max_annual_savings").
    - If a value is inferred (only allowed inside summary fields), mark it with "(inferred)" in fields_supported.

    CONSISTENCY:
    - If the PDF implies a requirement (e.g., “must have a prescription”), set requires_valid_rx=true only if explicitly stated; otherwise null.
    """.strip()

    user = (
        f"Drug context: {drug_name}\n"
        f"PDF URL: {url}\n"
        f"PDF Title: {hints['title']}\n"
        f"Detected phone numbers (hints): {', '.join(hints['phone_numbers']) if hints['phone_numbers'] else 'none'}\n"
        f"Detected dollar amounts (hints): {', '.join(hints['dollar_amounts']) if hints['dollar_amounts'] else 'none'}\n\n"
        f"SCHEMA:\n{schema}\n\n"
        f"PDF TEXT:\n{raw_text}\n\n"
        "Return ONLY the JSON array matching the schema."
    )

    try:
        resp = _openai_chat_create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=3000,
            temperature=0.0,
        )
        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception:
            content = getattr(resp.choices[0].message, "content", "") if hasattr(resp, "choices") else str(resp)
        content = (content or "").strip()

        json_text = _extract_balanced_json(content) or _extract_braced_json(content)
        if not json_text:
            return None, "pdf_ai_no_json_found"

        try:
            obj = json.loads(json_text)
        except Exception:
            obj = ast.literal_eval(json_text)

        data = [obj] if isinstance(obj, dict) else (obj if isinstance(obj, list) else [])
        if not data:
            return None, "pdf_ai_not_object_or_array"

        # Ensure cta.url fallback
        for item in data:
            if isinstance(item, dict) and isinstance(item.get("programs"), list):
                for prog in item["programs"]:
                    if isinstance(prog, dict) and isinstance(prog.get("cta"), dict):
                        if not (prog["cta"].get("url") or "").strip():
                            prog["cta"]["url"] = url

        # Reduce to single program + optionally drop empties (via postprocess)
        normalized = json.dumps(data, ensure_ascii=False)
        normalized = postprocess_ai_extraction(normalized, drop_if_no_programs=True, drop_if_only_discount_card=False)
        return normalized, "pdf_ai_parse_ok"

    except Exception as e:
        return None, f"pdf_ai_exception:{e}"


# =========================
# Selenium: safer driver creation
# =========================
def create_chrome_driver(retries: int = 2):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            opts = Options()
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--disable-extensions")
            opts.add_argument("--start-maximized")
            # opts.add_argument("--headless=new")  # enable if desired

            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(45)
            driver.set_script_timeout(45)
            return driver
        except Exception as e:
            last_exc = e
            logging.warning("Chrome driver creation failed (attempt %s/%s): %s", attempt + 1, retries + 1, e)
            time.sleep(2.0)
    raise last_exc


# =========================
# DB migration helpers
# =========================
def ensure_table_columns(conn: sqlite3.Connection, table: str, required_cols: dict):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for col, col_type in required_cols.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
    conn.commit()


# =========================
# URL helpers (for robust 2-pass)
# =========================
def _normalize_url_for_compare(u: str) -> str:
    if not u or not isinstance(u, str):
        return ""
    u = u.strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        if path.endswith("/") and path != "/":
            path = path[:-1]
        fragment = ""
        q = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kl = (k or "").lower()
            if kl in {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid", "msclkid"}:
                continue
            if "utm_" in kl:
                continue
            q.append((k, v))
        query = urlencode(q, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, fragment))
    except Exception:
        return u


def _is_probably_relevant_link(label: str, href: str) -> bool:
    s = (label or "").lower() + " " + (href or "").lower()
    bad = [
        "privacy",
        "cookie",
        "terms of use",
        "careers",
        "investor",
        "press",
        "newsroom",
        "accessibility",
        "site map",
        "sitemap",
        "contact-us",
        "contact us",
        "adchoices",
        "preferences",
    ]
    if any(b in s for b in bad):
        return False
    good = [
        "savings",
        "copay",
        "co-pay",
        "co pay",
        "card",
        "download",
        "pdf",
        "enroll",
        "enrollment",
        "register",
        "activate",
        "get card",
        "print",
        "terms",
        "conditions",
        "eligibility",
        "assistance",
        "support",
        "program",
    ]
    return any(g in s for g in good)


# =========================
# Selenium rendered fetch (MISSING IN YOUR ORIGINAL CODE)  ✅ FIXED
# =========================
def selenium_rendered_fetch(browser, url: str, wait_s: float = 2.0, max_links: int = 350) -> Dict[str, Any]:
    """
    Render a page in Selenium, then return:
      - final_url
      - text (visible body innerText)
      - links (list of {href,label})
      - forms (list of {action,method})
      - blocked (best-effort heuristic)
      - block_reason
    """
    out: Dict[str, Any] = {
        "final_url": url,
        "text": "",
        "links": [],
        "forms": [],
        "blocked": False,
        "block_reason": None,
    }

    try:
        browser.get(url)
        time.sleep(max(0.2, float(wait_s)))

        try:
            final_url = browser.current_url or url
        except Exception:
            final_url = url
        out["final_url"] = final_url

        try:
            text = browser.execute_script("return document.body ? (document.body.innerText || '') : '';")
        except Exception:
            text = browser.page_source or ""
        out["text"] = text or ""

        # Collect links/forms
        try:
            links = _collect_dom_links_structured(browser, max_items=max_links)
        except Exception:
            links = []
        out["links"] = links or []

        # forms
        try:
            forms_data = browser.execute_script(
                """
                const out = [];
                const forms = Array.from(document.querySelectorAll('form[action]'));
                for (const f of forms) {
                  const action = f.action || f.getAttribute('action') || '';
                  if (!action) continue;
                  out.push({ action: action, method: (f.method || '').toUpperCase() });
                }
                return out;
                """
            )
            forms = []
            seen = set()
            for it in forms_data or []:
                action = (it.get("action") or "").strip()
                if not action or action in seen:
                    continue
                seen.add(action)
                forms.append({"action": action, "method": (it.get("method") or "").upper()})
            out["forms"] = forms
        except Exception:
            out["forms"] = []

        # Heuristic blocked detection (best-effort)
        low = (out["text"] or "").lower()
        blocked_markers = [
            "access denied",
            "forbidden",
            "not authorized",
            "verify you are human",
            "captcha",
            "cloudflare",
            "akamai",
            "incapsula",
            "datadome",
            "unusual traffic",
            "we are checking your browser",
            "please enable cookies",
        ]
        if any(m in low for m in blocked_markers):
            out["blocked"] = True
            out["block_reason"] = "selenium_detected_block_markers"

        return out
    except Exception as e:
        out["blocked"] = True
        out["block_reason"] = f"selenium_exception:{type(e).__name__}:{e}"
        return out


def try_extract_schema_from_candidate_urls(
    browser,
    drug_name: str,
    candidate_urls: List[str],
    *,
    model: str = "gpt-4.1",
    max_tries: int = 8,
) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """
    Tries URLs in order, returning the first non-empty schema.
    Returns: (ai_extraction_json, ai_extraction_url, ai_extraction_log, debug_log)
    """
    tried = []
    logs = []

    # dedupe while preserving order
    seen = set()
    ordered = []
    for u in candidate_urls or []:
        u = _sanitize_url_http(u)
        nu = _normalize_url_for_compare(u)
        if not u or not nu or nu in seen:
            continue
        seen.add(nu)
        ordered.append(u)

    for u in ordered[:max_tries]:
        tried.append(u)
        try:
            j, l = ai_extract_full_schema_two_pass(browser, u, drug_name, model=model)

            # IMPORTANT: check emptiness BEFORE/AFTER postprocess
            # two_pass already postprocesses at the end, so "empty" usually means j is None.
            if j and not schema_is_effectively_empty(j):
                logs.append(f"OK:{u}")
                return j, u, l, ";".join(logs)

            logs.append(f"EMPTY:{u}:{l}")

        except Exception as e:
            logs.append(f"EXC:{u}:{type(e).__name__}:{e}")

    return None, None, None, ";".join(logs)


def looks_like_shell_page(text: str, *, min_chars: int = 600) -> bool:
    """
    Heuristic: determines if a page is likely a JS shell / bot wall / blank portal step.
    We want to trigger Selenium fallback when crawl fetch doesn't give usable content.
    """
    t = (text or "").strip()
    if len(t) < min_chars:
        return True

    low = t.lower()

    # Common JS-shell / blocked / consent patterns
    shell_markers = [
        "enable javascript",
        "javascript is required",
        "please enable cookies",
        "access denied",
        "forbidden",
        "not authorized",
        "request blocked",
        "verify you are human",
        "captcha",
        "cloudflare",
        "akamai",
        "incapsula",
        "datadome",
        "bot detection",
        "attention required",
        "unusual traffic",
        "we are checking your browser",
        "please wait while we check",
        "cookie settings",
        "consent preferences",
    ]

    if any(m in low for m in shell_markers):
        return True

    return False


# =========================
# DOM helpers
# =========================
def find_label_value(modal, label: str) -> Optional[str]:
    base = label.strip().rstrip(":?").strip()
    xpaths = [
        f".//*[normalize-space()='{base}:']",
        f".//*[normalize-space()='{base}?']",
        f".//*[normalize-space()='{base}']",
        f".//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{base.lower()}')]",
    ]
    for xp in xpaths:
        try:
            label_el = modal.find_element(By.XPATH, xp)
            try:
                value_el = label_el.find_element(By.XPATH, "following::*[normalize-space()!=''][1]")
                text = value_el.text.strip()
                if text:
                    return text
            except Exception:
                txt = label_el.text.strip()
                if txt:
                    return txt
        except Exception:
            continue
    return None


def href_after_label(modal, label: str) -> Optional[str]:
    base = label.strip().rstrip(":?").strip()
    xpaths = [
        f".//*[normalize-space()='{base}:']",
        f".//*[normalize-space()='{base}?']",
        f".//*[normalize-space()='{base}']",
        f".//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{base.lower()}')]",
    ]
    for xp in xpaths:
        try:
            label_el = modal.find_element(By.XPATH, xp)
            try:
                link = label_el.find_element(By.XPATH, "following::*[1]//a[@href]")
                href = link.get_attribute("href")
                if href:
                    return href.strip()
            except Exception:
                try:
                    value_el = label_el.find_element(By.XPATH, "following::*[normalize-space()!=''][1]")
                    val = value_el.text.strip()
                    if val:
                        return val
                except Exception:
                    pass
        except Exception:
            continue
    return None


def get_goodrx_display_drug_name(browser) -> str:
    try:
        h1 = browser.find_element(By.XPATH, "//h1[normalize-space()]")
        txt = (h1.text or "").strip()
        if "\n" in txt:
            txt = txt.split("\n", 1)[0].strip()
        return txt
    except Exception:
        return ""


def looks_like_goodrx_manufacturer_modal(el) -> bool:
    try:
        t = (el.text or "").lower()
    except Exception:
        t = ""
    signals = ["program name", "website", "phone", "how much can i save", "how much can i save?"]
    return any(s in t for s in signals)


def get_goodrx_manufacturer_modal(browser, timeout_seconds=10):
    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        try:
            dialogs = browser.find_elements(By.XPATH, "//*[@role='dialog' or @aria-modal='true']")
        except Exception:
            dialogs = []

        for d in dialogs:
            try:
                if d.is_displayed() and looks_like_goodrx_manufacturer_modal(d):
                    return d
            except Exception:
                continue

        time.sleep(0.25)
    return None


def _collect_dom_links_structured(browser, max_items: int = 350) -> List[Dict[str, str]]:
    try:
        data = browser.execute_script(
            """
            const out = [];
            const links = Array.from(document.querySelectorAll('a[href]'));
            for (const a of links) {
              const href = a.href || a.getAttribute('href') || '';
              if (!href) continue;

              const text = (a.innerText || a.textContent || '').trim().replace(/\\s+/g,' ');
              const aria = (a.getAttribute('aria-label') || '').trim().replace(/\\s+/g,' ');
              const title = (a.getAttribute('title') || '').trim().replace(/\\s+/g,' ');

              out.push({ href, label: (text || aria || title || '').trim() });
            }
            return out;
            """
        )
        out: List[Dict[str, str]] = []
        seen = set()
        for item in data or []:
            href = (item.get("href") or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            label = (item.get("label") or "").strip() or "(no visible text)"
            if len(label) > 160:
                label = label[:160] + "…"
            out.append({"href": href, "label": label})
            if len(out) >= max_items:
                break
        return out
    except Exception:
        return []


def fetch_page_best_effort(browser, url: str, timeout_s: int = 30) -> Dict[str, Any]:
    """
    Attempts crawl4ai_fetch first (fast, low block rate).
    Falls back to Selenium-rendered fetch if:
      - crawl4ai says blocked, OR
      - text looks like a shell, OR
      - no links/forms found and text is weak
    Returns a dict with: final_url, text, links, forms, blocked, block_reason, used
    """
    # 1) crawl4ai
    r = crawl4ai_fetch(url, timeout_s=timeout_s) or {}
    used = "crawl4ai"

    blocked = bool(r.get("blocked", False))
    text = (r.get("text") or "")
    links = r.get("links") or []
    forms = r.get("forms") or []

    weak_links = len(links) < 3 and len(forms) == 0
    shell = looks_like_shell_page(text)

    # 2) fallback to selenium if needed
    if blocked or shell or weak_links:
        sr = selenium_rendered_fetch(browser, url, wait_s=2.5)
        # If Selenium yields *better* content, prefer it
        s_text = (sr.get("text") or "")
        s_links = sr.get("links") or []
        s_forms = sr.get("forms") or []

        if len(s_text.strip()) > len(text.strip()) or len(s_links) > len(links) or len(s_forms) > len(forms):
            r = sr
            used = "selenium"

    r["used"] = used
    return r


# =========================
# Phone helpers
# =========================
def _extract_phone_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(\+?\d{1,3}[\s.-]?)?(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})", text)
    return m.group(0) if m else None


def _normalize_phone(raw: str) -> str:
    if not raw:
        return raw
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 10:
        return f"+1-{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
    return raw.strip()


# =========================
# Schema guards
# =========================
def schema_is_effectively_empty(ai_extraction_json: Optional[str]) -> bool:
    if not ai_extraction_json:
        return True
    try:
        data = json.loads(ai_extraction_json)
    except Exception:
        return True
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return True

    obj = data[0]
    programs = obj.get("programs") if isinstance(obj.get("programs"), list) else []
    if not programs:
        return True

    def program_has_substance(p: Dict[str, Any]) -> bool:
        if not isinstance(p, dict):
            return False
        if _text(p.get("name")) or _text(p.get("type")):
            return True

        cta = p.get("cta") if isinstance(p.get("cta"), dict) else {}
        if _sanitize_url_http(cta.get("url")):
            return True

        contact = p.get("contact") if isinstance(p.get("contact"), dict) else {}
        if _text(contact.get("phone")) or _text(contact.get("email")):
            return True

        summ = p.get("summary") if isinstance(p.get("summary"), dict) else {}
        if _text(summ.get("tldr")) or _text(summ.get("ui_blurb")):
            return True

        bl = p.get("benefit_logic") if isinstance(p.get("benefit_logic"), dict) else {}
        for branch in ("covered", "not_covered", "uninsured", "restrictions_not_met", "bridge_fill"):
            node = bl.get(branch) if isinstance(bl.get(branch), dict) else {}
            s = node.get("summary") if isinstance(node.get("summary"), dict) else {}
            if _text(s.get("tldr")):
                return True

        return False

    return not any(program_has_substance(p) for p in programs)


def url_open_failed(ai_log: Optional[str]) -> bool:
    if not ai_log:
        return False
    markers = ["nav_failed_about_blank", "page_blocked:", "exception:"]
    return any(m in ai_log for m in markers)


# =========================
# Schema fallback builder (GoodRx modal)
# =========================
def build_schema_from_goodrx_modal(
    drug_name: str,
    display_drug_name: Optional[str],
    program_name: Optional[str],
    website: Optional[str],
    offer_text: Optional[str],
    phone_number: Optional[str],
) -> str:
    """
    DROP-IN REPLACEMENT (NEW SCHEMA):
    Builds a minimal-but-valid full-schema JSON extraction using GoodRx modal fields.
    """
    drug_display = (display_drug_name or "").strip() or (drug_name or "").strip() or None
    program = (program_name or "").strip() or drug_display or None
    offer = (offer_text or "").strip() or None
    phone = _normalize_phone(phone_number).strip() if phone_number else None
    url = (website or "").strip() or None

    covered_tldr = offer
    ui_blurb = offer

    obj = [
        {
            "drug": {
                "name": drug_display,
                "generic": None,
                "manufacturer": None,
                "therapeutic_area": None,
                "route_form": None,
                "ndc_variants": None,
                "rxnorm": None,
            },
            "programs": [
                {
                    "program_id": None,
                    "type": "copay",
                    "name": program,
                    "eligibility": {
                        "insurance_included": [],
                        "insurance_excluded": [],
                        "cash_payers_excluded": None,
                        "requires_valid_rx": None,
                        "requires_fda_label_use": None,
                        "state_limitations": None,
                    },
                    "benefit_logic": {
                        "covered": {
                            "pay_as_low_as": None,
                            "max_monthly_savings": None,
                            "max_annual_savings": None,
                            "max_fills_per_year": None,
                            "month_definition": None,
                            "redemption_cadence": None,
                            "summary": {"tldr": covered_tldr},
                        },
                        "not_covered": {
                            "cash_price": None,
                            "patient_price_floor": None,
                            "max_fills_lifetime": None,
                            "days_supply": None,
                            "max_savings_per_fill": None,
                            "max_annual_savings": None,
                            "pricing_details": None,
                            "outside_insurance_note": None,
                            "notes": None,
                            "summary": {"tldr": None},
                        },
                        "restrictions_not_met": {
                            "cash_price": None,
                            "patient_price_floor": None,
                            "max_fills_lifetime": None,
                            "days_supply": None,
                            "summary": {"tldr": None},
                        },
                        "uninsured": {
                            "cash_price": None,
                            "patient_price_floor": None,
                            "max_fills_lifetime": None,
                            "days_supply": None,
                            "pricing_details": None,
                            "outside_insurance_note": None,
                            "notes": None,
                            "summary": {"tldr": None},
                        },
                        "bridge_fill": {
                            "one_time_fill": None,
                            "max_tablets": None,
                            "trigger": None,
                            "summary": {"tldr": None},
                        },
                    },
                    "compliance": {
                        "not_insurance": None,
                        "reimbursement_prohibited": None,
                        "subject_to_change": None,
                        "copay_maximizer_handling": None,
                        "lab_test_support": None,
                        "offer_validity": None,
                        "rems_required": None,
                        "notes": None,
                    },
                    "contact": {"phone": phone, "email": None},
                    "cta": {"enrollment_required": None, "url": url},
                    "summary": {"tldr": offer, "operational_notes": None, "ui_blurb": ui_blurb},
                    "source_urls": [u for u in [url] if u],
                    "retrieved_at": now_utc_iso(),
                }
            ],
            "disclaimers": [],
            "sources": [
                {
                    "url": url,
                    "retrieved_at": now_utc_iso(),
                    "content_type": "text/html",
                    "notes": "Fallback schema built from GoodRx manufacturer modal fields (program name/offer/phone/website).",
                    "fields_supported": [
                        "drug.name",
                        "programs[0].type",
                        "programs[0].name",
                        "programs[0].contact.phone",
                        "programs[0].cta.url",
                        "programs[0].benefit_logic.covered.summary.tldr",
                        "programs[0].summary.tldr",
                    ],
                }
            ],
            "summary": {
                "tldr": offer,
                "recommended_next_step": ("Use programs[0].cta.url to enroll/activate/download if available." if url else None),
                "display_message": offer,
            },
        }
    ]

    return json.dumps(obj, ensure_ascii=False)


# =========================
# Small/basic AI extractor (program_name/offer/phone)
# =========================
def ai_extract_from_page(browser, url: str, drug_name: str, model: str = "gpt-4.1"):
    log_parts = []
    program_name = None
    offer_text = None
    phone_number = None

    if openai is None:
        return None, None, None, "openai_not_installed"
    if not os.environ.get("OPENAI_API_KEY"):
        return None, None, None, "openai_api_key_missing"

    try:
        try:
            openai.api_key = os.environ.get("OPENAI_API_KEY")
        except Exception:
            pass

        browser.get(url)
        time.sleep(1.0)

        try:
            page_text = browser.execute_script("return document.body.innerText || ''")
        except Exception:
            page_text = browser.page_source or ""

        MAX_CHARS = 250000
        if len(page_text) > MAX_CHARS:
            page_text = page_text[:MAX_CHARS]
            log_parts.append("page_text_truncated")

        system = (
            "You are a structured data extractor. Given visible page text, return JSON with keys "
            '"program_name", "offer_text", and "phone_number". If a field cannot be found, set it to null. '
            "Output strictly valid JSON and nothing else."
        )
        user = (
            f"Page text (for drug '{drug_name}'):\n\n{page_text}\n\n"
            "Extract the program or coupon name if present, a short relevant offer text (one sentence or a short phrase), "
            "and a contact phone number if present. Return JSON."
        )

        resp = _openai_chat_create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=350,
            temperature=0.0,
        )

        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception:
            content = getattr(resp.choices[0].message, "content", "") if hasattr(resp, "choices") else str(resp)
        content = str(content).strip()

        raw_trunc = content if len(content) <= 600 else content[:600] + "...[truncated]"
        log_parts.append("ai_raw=" + raw_trunc.replace("\n", " "))

        json_text = _extract_balanced_json(content) or _extract_braced_json(content) or content
        data = {}
        parse_success = False

        try:
            data = json.loads(json_text)
            parse_success = True
            log_parts.append("ai_parse_ok")
        except Exception:
            try:
                data = ast.literal_eval(json_text)
                parse_success = True
                log_parts.append("ai_parsed_literal_eval")
            except Exception:
                pass

        if parse_success and isinstance(data, dict):
            program_name = data.get("program_name") if data.get("program_name") else None
            offer_text = data.get("offer_text") if data.get("offer_text") else None
            phone_number = data.get("phone_number") if data.get("phone_number") else None
        else:
            pn = _extract_phone_from_text(page_text)
            if pn:
                phone_number = pn
                log_parts.append("phone_heuristic_ok")

    except Exception as e:
        log_parts.append(f"exception:{e}")

    if phone_number:
        phone_number = _normalize_phone(phone_number)

    return program_name, offer_text, phone_number, ";".join(log_parts)


# =========================
# PASS-1: Link selection (INDEX-BASED to ensure pass-2 works)
# =========================
def ai_select_followup_link_indexes(
    drug_name: str,
    base_url: str,
    links: List[Dict[str, str]],
    page_text: str,
    model: str = "gpt-4.1",
    max_links: int = 3,
) -> Tuple[List[int], str]:
    if openai is None:
        return [], "openai_not_installed"
    if not os.environ.get("OPENAI_API_KEY"):
        return [], "openai_api_key_missing"
    if not links:
        return [], "no_links"

    links_trim = links[:220]
    lines = []
    for i, it in enumerate(links_trim, start=1):
        href = (it.get("href") or "").strip()
        if not href:
            continue
        label = (it.get("label") or "").strip() or "(no visible text)"
        lines.append(f'{i}. "{label}" => {href}')

    sys = f"""
You are a link-selection engine for drug savings/copay assistance extraction.

Goal: pick up to {max_links} FOLLOW-UP LINKS (by index) that are most likely to contain:
- eligibility criteria
- terms & conditions
- savings card PDF
- enrollment portal / get card / download
- phone/contact details

Rules:
- Only choose from the numbered LINKS below.
- Prefer labels/URLs containing: savings, copay, card, enroll, register, activate, download, pdf, terms, eligibility, program.
- Avoid: privacy policy, cookie policy, careers, investor relations, press/news.
- Output STRICT JSON only:
{{ "selected_indexes": [1, 5, 9], "reasons": ["..", "..", ".."] }}
If none are relevant: {{ "selected_indexes": [] }}
""".strip()

    ctx = (page_text or "")[:5000]
    user = (
        f"Drug: {drug_name}\nBase URL: {base_url}\n\n"
        "PAGE TEXT (truncated):\n"
        f"{ctx}\n\n"
        "LINKS:\n" + "\n".join(lines) + "\n\nReturn JSON."
    )

    try:
        resp = _openai_chat_create(
            model=model,
            messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}],
            max_tokens=450,
            temperature=0.0,
        )
        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception:
            content = getattr(resp.choices[0].message, "content", "") if hasattr(resp, "choices") else str(resp)
        content = (content or "").strip()

        json_text = _extract_balanced_json(content) or _extract_braced_json(content) or content
        data = json.loads(json_text) if json_text else {}
        idxs = data.get("selected_indexes") or []
        if not isinstance(idxs, list):
            idxs = []

        clean: List[int] = []
        for x in idxs:
            try:
                ix = int(x)
                if 1 <= ix <= len(links_trim):
                    clean.append(ix)
            except Exception:
                continue

        seen = set()
        out = []
        for ix in clean:
            if ix not in seen:
                seen.add(ix)
                out.append(ix)
        return out[:max_links], "ai_link_select_ok"
    except Exception as e:
        return [], f"ai_link_select_error:{e}"


# =========================
# Full-schema AI extractor (single page)
# =========================
def ai_extract_full_schema_from_page(browser, url: str, drug_name: str, model: str = "gpt-4.1"):
    log_parts = []
    if openai is None:
        return None, "openai_not_installed"
    if not os.environ.get("OPENAI_API_KEY"):
        return None, "openai_api_key_missing"

    try:
        try:
            openai.api_key = os.environ.get("OPENAI_API_KEY")
        except Exception:
            pass

        # Use crawl4ai_fetch instead of Selenium
        fetch_result = fetch_page_best_effort(browser, url, timeout_s=30)
        log_parts.append(f"fetch_used={fetch_result.get('used')}")

        # If still blocked after fallback, bail
        if fetch_result.get("blocked", False) and fetch_result.get("used") != "selenium":
            reason = fetch_result.get("block_reason", "unknown")
            log_parts.append(f"page_blocked:{reason}")
            return None, ";".join(log_parts)

        current = fetch_result.get("final_url", url)
        if not current or current.startswith("about:"):
            log_parts.append("nav_failed_about_blank")
            return None, ";".join(log_parts)

        page_text = fetch_result.get("text", "")
        if len(page_text.strip()) < 30:
            log_parts.append("page_too_short")
            return None, ";".join(log_parts)

        links = fetch_result.get("links", []) or []
        forms = fetch_result.get("forms", []) or []
        link_map_parts = []
        for lnk in links[:250]:
            label = (lnk.get("label") or "").strip()
            href = (lnk.get("href") or "").strip()
            if label and href:
                link_map_parts.append(f'  "{label}" => {href}')
        for frm in forms[:50]:
            action = (frm.get("action") or "").strip()
            method = (frm.get("method") or "GET").upper()
            if action:
                link_map_parts.append(f'  <form {method}> => {action}')
        link_map_text = "\n".join(link_map_parts) if link_map_parts else ""
        if not link_map_text:
            log_parts.append("no_links_collected")

        MAX_TEXT = 260000
        if len(page_text) > MAX_TEXT:
            page_text = page_text[:MAX_TEXT]
            log_parts.append("page_text_truncated")
        if link_map_text and len(link_map_text) > 80000:
            link_map_text = link_map_text[:80000] + "\n...[truncated]"
            log_parts.append("link_map_truncated")

        schema = r"""
        [
          {
            "drug": {
              "name": null,
              "generic": null,
              "manufacturer": null,
              "therapeutic_area": null,
              "route_form": null,
              "ndc_variants": null,
              "rxnorm": null
            },
            "programs": [
              {
                "program_id": null,
                "type": "copay | discount_card | pap | foundation | rebate | bridge_fill | support",
                "name": null,
                "eligibility": {
                  "insurance_included": [],
                  "insurance_excluded": [],
                  "cash_payers_excluded": null,
                  "requires_valid_rx": null,
                  "requires_fda_label_use": null,
                  "state_limitations": null
                },
                "benefit_logic": {
                  "covered": {
                    "pay_as_low_as": null,
                    "max_monthly_savings": null,
                    "max_annual_savings": null,
                    "max_fills_per_year": null,
                    "month_definition": null,
                    "redemption_cadence": null,
                    "summary": { "tldr": null }
                  },
                  "not_covered": {
                    "cash_price": null,
                    "patient_price_floor": null,
                    "max_fills_lifetime": null,
                    "days_supply": null,
                    "max_savings_per_fill": null,
                    "max_annual_savings": null,
                    "pricing_details": null,
                    "outside_insurance_note": null,
                    "notes": null,
                    "summary": { "tldr": null }
                  },
                  "restrictions_not_met": {
                    "cash_price": null,
                    "patient_price_floor": null,
                    "max_fills_lifetime": null,
                    "days_supply": null,
                    "summary": { "tldr": null }
                  },
                  "uninsured": {
                    "cash_price": null,
                    "patient_price_floor": null,
                    "max_fills_lifetime": null,
                    "days_supply": null,
                    "pricing_details": null,
                    "outside_insurance_note": null,
                    "notes": null,
                    "summary": { "tldr": null }
                  },
                  "bridge_fill": {
                    "one_time_fill": null,
                    "max_tablets": null,
                    "trigger": null,
                    "summary": { "tldr": null }
                  }
                },
                "compliance": {
                  "not_insurance": null,
                  "reimbursement_prohibited": null,
                  "subject_to_change": null,
                  "copay_maximizer_handling": null,
                  "lab_test_support": null,
                  "offer_validity": null,
                  "rems_required": null,
                  "notes": null
                },
                "contact": {
                  "phone": null,
                  "email": null
                },
                "cta": {
                  "enrollment_required": null,
                  "url": null
                },
                "summary": {
                  "tldr": null,
                  "operational_notes": null,
                  "ui_blurb": null
                },
                "source_urls": [],
                "retrieved_at": null
              }
            ],
            "disclaimers": [],
            "sources": [
              {
                "url": null,
                "retrieved_at": null,
                "content_type": null,
                "notes": null,
                "fields_supported": []
              }
            ],
            "summary": {
              "tldr": null,
              "recommended_next_step": null,
              "display_message": null
            }
          }
        ]
        """.strip()

        system = """
        You are an information extraction engine for drug cost, savings, and assistance programs.

        Extract ONLY explicitly stated information from the provided PAGE TEXT and LINKS and output STRICTLY valid JSON that conforms EXACTLY to the SCHEMA.

        HARD RULES (STRICT):
        - Output JSON ONLY. No markdown, no commentary, no trailing text.
        - The output MUST be a JSON array of objects (even if only one object).
        - The JSON MUST match the schema exactly: same keys, same nesting, no extra keys.
        - DO NOT GUESS. If not explicitly stated, use null (or [] for arrays).
        - Do not omit fields. If unknown, keep the field present with null / [].
        - Money fields are numbers (e.g., 25 not "$25"). Do not include currency symbols.
        - Use booleans only where the schema uses booleans; otherwise null.
        - For any field named "url", use an absolute URL from LINKS if available; otherwise use the Page URL loaded.
        - Use ISO 8601 UTC for retrieved_at if a timestamp is provided in the input; otherwise null.

        NORMALIZATION:
        - programs[].type must be exactly one of: copay, discount_card, pap, foundation, rebate, bridge_fill, support.
        - eligibility.insurance_included / insurance_excluded MUST contain ONLY these tokens (lowercase):
          commercial, uninsured, medicare, medicaid, tricare, va, other_government, state_pharmaceutical_assistance, medicare_part_d
          Include a token ONLY if the terms explicitly mention it.
        - eligibility.state_limitations is either null or an array of 2-letter state abbreviations (e.g., ["MA","CA"]).

        PROGRAM ID:
        - programs[].program_id: create a stable slug if possible using drug + program name/type (lowercase, underscores).
          Example: "zepbound_savings_card". If you cannot confidently name it, use null.

        BENEFIT LOGIC:
        Populate ALL benefit_logic branches. Each branch must include summary.tldr (1–2 sentences).
        - covered: use for commercially insured + covered by insurance.
        - not_covered: use for commercially insured + NOT covered by insurance.
        - restrictions_not_met: use for criteria not met / PA denial / step therapy not satisfied.
        - uninsured: use for uninsured/self-pay pathways or published self-pay price floors.
        - bridge_fill: use for temporary/free fills during verification/appeal.

        AUDIT / SOURCES:
        - programs[].source_urls: include all URLs (from LINKS or page URL) that directly support that program.
        - sources[]: include one entry per URL used. For each entry:
          - content_type: "text/html" for web pages unless explicitly a PDF; use "pdf" for PDFs.
          - notes: what the source contained that you used.
          - fields_supported: list dotted JSON paths you could support from that source (e.g., "drug.name", "programs[0].eligibility").
        - If a value is inferred (only allowed for summary text), mark it with "(inferred)" in fields_supported.

        CONSISTENCY:
        - If the page implies a requirement (e.g., "must have a prescription"), set requires_valid_rx=true only if the text clearly states it; otherwise null.
        - If caps/maximums are mentioned but not quantified, keep numeric fields null and explain in the relevant summary.tldr.
        """.strip()

        user = (
            f"Drug context: {drug_name}\n"
            f"Page URL loaded: {current}\n\n"
            f"SCHEMA:\n{schema}\n\n"
            "LINKS (anchor text/label => ACTUAL href, and forms => action):\n"
            f"{link_map_text}\n\n"
            "PAGE TEXT:\n"
            f"{page_text}\n\n"
            "Return ONLY the JSON array matching the schema."
        )

        resp = _openai_chat_create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=3000,
            temperature=0.0,
        )

        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception:
            content = getattr(resp.choices[0].message, "content", "") if hasattr(resp, "choices") else str(resp)
        content = (content or "").strip()

        raw_trunc = content if len(content) <= 1000 else content[:1000] + "...[truncated]"
        log_parts.append("ai_raw=" + raw_trunc.replace("\n", " "))

        json_text = _extract_balanced_json(content) or _extract_braced_json(content)
        if not json_text:
            log_parts.append("ai_no_json_found")
            return None, ";".join(log_parts)

        try:
            obj = json.loads(json_text)
        except Exception:
            obj = ast.literal_eval(json_text)
            log_parts.append("ai_parsed_literal_eval")

        data = [obj] if isinstance(obj, dict) else (obj if isinstance(obj, list) else [])
        if not data:
            log_parts.append("ai_not_object_or_array")
            return None, ";".join(log_parts)

        normalized = json.dumps(data, ensure_ascii=False)
        log_parts.append("ai_parse_ok")
        return normalized, ";".join(log_parts)

    except Exception as e:
        log_parts.append(f"exception:{e}")
        return None, ";".join(log_parts)


# =========================
# Merge helpers (fill-only)
# =========================
def _merge_program_fill_only(dp: Dict[str, Any], sp: Dict[str, Any]) -> Dict[str, Any]:
    """
    DROP-IN REPLACEMENT (NEW SCHEMA):
    Fill-only merge of a single program dict using the new schema fields.
    Never overwrites a non-empty destination value.
    """

    def is_empty(v):
        if v is None:
            return True
        if isinstance(v, str):
            return v.strip() == ""
        if isinstance(v, list):
            return len(v) == 0
        if isinstance(v, dict):
            return len(v) == 0
        return False

    def fill(dstv, srcv):
        return srcv if (not is_empty(srcv) and is_empty(dstv)) else dstv

    # Simple scalar fields
    for k in ("program_id", "type", "name", "retrieved_at"):
        dp[k] = fill(dp.get(k), sp.get(k))

    # eligibility (fill scalars; union arrays)
    de = dp.get("eligibility") if isinstance(dp.get("eligibility"), dict) else {}
    se = sp.get("eligibility") if isinstance(sp.get("eligibility"), dict) else {}
    if isinstance(de, dict) and isinstance(se, dict):
        for k in ("cash_payers_excluded", "requires_valid_rx", "requires_fda_label_use", "state_limitations"):
            de[k] = fill(de.get(k), se.get(k))

        for arr_key in ("insurance_included", "insurance_excluded"):
            da = de.get(arr_key) if isinstance(de.get(arr_key), list) else []
            sa = se.get(arr_key) if isinstance(se.get(arr_key), list) else []
            merged = []
            seen = set()
            for x in da + sa:
                if isinstance(x, str):
                    xx = x.strip().lower()
                    if xx and xx not in seen:
                        seen.add(xx)
                        merged.append(xx)
            de[arr_key] = merged

        dp["eligibility"] = de

    # contact
    dc = dp.get("contact") if isinstance(dp.get("contact"), dict) else {}
    sc = sp.get("contact") if isinstance(sp.get("contact"), dict) else {}
    if isinstance(dc, dict) and isinstance(sc, dict):
        for k in ("phone", "email"):
            dc[k] = fill(dc.get(k), sc.get(k))
        dp["contact"] = dc

    # cta
    dcta = dp.get("cta") if isinstance(dp.get("cta"), dict) else {}
    scta = sp.get("cta") if isinstance(sp.get("cta"), dict) else {}
    if isinstance(dcta, dict) and isinstance(scta, dict):
        dcta["url"] = fill(dcta.get("url"), scta.get("url"))
        dcta["enrollment_required"] = fill(dcta.get("enrollment_required"), scta.get("enrollment_required"))
        dp["cta"] = dcta

    # compliance (fill-only on scalar keys)
    dco = dp.get("compliance") if isinstance(dp.get("compliance"), dict) else {}
    sco = sp.get("compliance") if isinstance(sp.get("compliance"), dict) else {}
    if isinstance(dco, dict) and isinstance(sco, dict):
        for k in (
            "not_insurance",
            "reimbursement_prohibited",
            "subject_to_change",
            "copay_maximizer_handling",
            "lab_test_support",
            "offer_validity",
            "rems_required",
            "notes",
        ):
            dco[k] = fill(dco.get(k), sco.get(k))
        dp["compliance"] = dco

    # summary
    ds = dp.get("summary") if isinstance(dp.get("summary"), dict) else {}
    ss = sp.get("summary") if isinstance(sp.get("summary"), dict) else {}
    if isinstance(ds, dict) and isinstance(ss, dict):
        for k in ("tldr", "operational_notes", "ui_blurb"):
            ds[k] = fill(ds.get(k), ss.get(k))
        dp["summary"] = ds

    # benefit_logic: for each branch, fill-only fields, including summary.tldr
    dbl = dp.get("benefit_logic") if isinstance(dp.get("benefit_logic"), dict) else {}
    sbl = sp.get("benefit_logic") if isinstance(sp.get("benefit_logic"), dict) else {}
    if isinstance(dbl, dict) and isinstance(sbl, dict):
        branches = ("covered", "not_covered", "restrictions_not_met", "uninsured", "bridge_fill")
        for b in branches:
            dnode = dbl.get(b) if isinstance(dbl.get(b), dict) else {}
            snode = sbl.get(b) if isinstance(sbl.get(b), dict) else {}

            if isinstance(dnode, dict) and isinstance(snode, dict):
                dsum = dnode.get("summary") if isinstance(dnode.get("summary"), dict) else {}
                ssum = snode.get("summary") if isinstance(snode.get("summary"), dict) else {}
                if isinstance(dsum, dict) and isinstance(ssum, dict):
                    dsum["tldr"] = fill(dsum.get("tldr"), ssum.get("tldr"))
                    dnode["summary"] = dsum

                for k, sv in snode.items():
                    if k == "summary":
                        continue
                    dnode[k] = fill(dnode.get(k), sv)

                dbl[b] = dnode

        dp["benefit_logic"] = dbl

    # source_urls union
    dsu = dp.get("source_urls") if isinstance(dp.get("source_urls"), list) else []
    ssu = sp.get("source_urls") if isinstance(sp.get("source_urls"), list) else []
    seen = set()
    out = []
    for u in dsu + ssu:
        uu = _sanitize_url_http(u) if isinstance(u, str) else ""
        if uu and uu not in seen:
            seen.add(uu)
            out.append(uu)
    dp["source_urls"] = out

    return dp


def _merge_fill_only(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """
    DROP-IN REPLACEMENT (NEW SCHEMA):
    Fill-only merge of two root objects for the new full schema.
    """

    def is_empty(v):
        if v is None:
            return True
        if isinstance(v, str):
            return v.strip() == ""
        if isinstance(v, list):
            return len(v) == 0
        if isinstance(v, dict):
            return len(v) == 0
        return False

    def fill(dstv, srcv):
        return srcv if (not is_empty(srcv) and is_empty(dstv)) else dstv

    # drug
    dd = dst.get("drug") if isinstance(dst.get("drug"), dict) else {}
    sd = src.get("drug") if isinstance(src.get("drug"), dict) else {}
    if isinstance(dd, dict) and isinstance(sd, dict):
        for k, sv in sd.items():
            dd[k] = fill(dd.get(k), sv)
        dst["drug"] = dd

    # summary
    dsum = dst.get("summary") if isinstance(dst.get("summary"), dict) else {}
    ssum = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    if isinstance(dsum, dict) and isinstance(ssum, dict):
        for k in ("tldr", "recommended_next_step", "display_message"):
            dsum[k] = fill(dsum.get(k), ssum.get(k))
        dst["summary"] = dsum

    # disclaimers union
    ddisc = dst.get("disclaimers") if isinstance(dst.get("disclaimers"), list) else []
    sdisc = src.get("disclaimers") if isinstance(src.get("disclaimers"), list) else []
    disc_seen = set()
    disc_out = []
    for x in ddisc + sdisc:
        if isinstance(x, str):
            xx = x.strip()
            if xx and xx not in disc_seen:
                disc_seen.add(xx)
                disc_out.append(xx)
    dst["disclaimers"] = disc_out

    # sources union by url
    dsources = dst.get("sources") if isinstance(dst.get("sources"), list) else []
    ssources = src.get("sources") if isinstance(src.get("sources"), list) else []

    def _src_url(sobj) -> str:
        if isinstance(sobj, dict):
            return _sanitize_url_http(sobj.get("url"))
        return ""

    src_index = {}
    out_sources = []
    for s in dsources:
        u = _src_url(s)
        if u:
            src_index[u] = s
        if isinstance(s, dict):
            out_sources.append(s)

    for s in ssources:
        if not isinstance(s, dict):
            continue
        u = _src_url(s)
        if not u:
            continue
        if u in src_index and isinstance(src_index[u], dict):
            dso = src_index[u]
            for k, sv in s.items():
                dso[k] = fill(dso.get(k), sv)
            df = dso.get("fields_supported") if isinstance(dso.get("fields_supported"), list) else []
            sf = s.get("fields_supported") if isinstance(s.get("fields_supported"), list) else []
            seen = set()
            merged = []
            for x in df + sf:
                if isinstance(x, str):
                    xx = x.strip()
                    if xx and xx not in seen:
                        seen.add(xx)
                        merged.append(xx)
            dso["fields_supported"] = merged
        else:
            out_sources.append(s)
            src_index[u] = s

    dst["sources"] = out_sources

    # programs merge
    dprogs = dst.get("programs") if isinstance(dst.get("programs"), list) else []
    sprogs = src.get("programs") if isinstance(src.get("programs"), list) else []

    def prog_key(p: Dict[str, Any]) -> Tuple[str, str, str]:
        t = _text(p.get("type")).lower()
        n = _text(p.get("name")).lower()
        cta = p.get("cta") if isinstance(p.get("cta"), dict) else {}
        u = _sanitize_url_http(cta.get("url")).lower()
        return (t, n, u)

    index = {}
    for i, p in enumerate(dprogs):
        if isinstance(p, dict):
            index[prog_key(p)] = i

    for sp in sprogs:
        if not isinstance(sp, dict):
            continue
        k = prog_key(sp)
        if k in index and isinstance(dprogs[index[k]], dict):
            dprogs[index[k]] = _merge_program_fill_only(dprogs[index[k]], sp)
        else:
            if _program_completeness(sp) >= 2 or _actionability(sp) > 0:
                dprogs.append(sp)
                index[k] = len(dprogs) - 1

    dst["programs"] = dprogs
    return dst


# =========================
# PASS-2: multi-page extraction (2-pass)
# =========================
def ai_extract_full_schema_two_pass(browser, url: str, drug_name: str, model: str = "gpt-4.1") -> Tuple[Optional[str], str]:
    logs = []

    # PDF routing
    if is_pdf_url(url):
        logs.append("pdf_detected")
        pdf_json, pdf_log = ai_extract_from_pdf(url, drug_name, model=model)
        logs.append(f"pdf:{pdf_log}")
        return pdf_json, ";".join(logs)

    # Base extraction
    base_json, base_log = ai_extract_full_schema_from_page(browser, url, drug_name, model=model)
    logs.append(f"base:{base_log}")

    # Link selection context
    selected_urls: List[str] = []
    try:
        fetch_result = fetch_page_best_effort(browser, url, timeout_s=30)
        logs.append(f"link_fetch_used={fetch_result.get('used')}")

        if fetch_result.get("blocked", False) and fetch_result.get("used") != "selenium":
            logs.append(f"link_page_blocked:{fetch_result.get('block_reason', 'unknown')}")
            selected_urls = []
        else:
            page_text = fetch_result.get("text", "") or ""

            links_from_fetch = fetch_result.get("links", []) or []
            links_struct = []
            for idx, lnk in enumerate(links_from_fetch[:350], start=1):
                label = (lnk.get("label") or "").strip()
                href = (lnk.get("href") or "").strip()
                if href:
                    links_struct.append({"index": idx, "label": label, "href": href})

            logs.append(f"links_collected={len(links_struct)}")

            idxs, sel_log = ai_select_followup_link_indexes(drug_name, url, links_struct, page_text, model=model, max_links=3)
            logs.append(f"link_select:{sel_log}; idxs={idxs}")

            links_trim = links_struct[:220]
            for ix in idxs:
                try:
                    item = links_trim[ix - 1]
                    href = (item.get("href") or "").strip()
                    if href:
                        selected_urls.append(href)
                except Exception:
                    continue

            if not selected_urls and links_struct:
                heur = []
                for it in links_struct:
                    href = (it.get("href") or "").strip()
                    label = (it.get("label") or "").strip()
                    if href and _is_probably_relevant_link(label, href):
                        heur.append(href)
                    if len(heur) >= 3:
                        break
                selected_urls = heur
                logs.append(f"heuristic_links_used={len(selected_urls)}")

        seen = set()
        final = []
        for u in selected_urls:
            nu = _normalize_url_for_compare(u)
            if nu and nu not in seen:
                seen.add(nu)
                final.append(u)
        selected_urls = final[:3]
        logs.append(f"selected_urls_n={len(selected_urls)}")

    except Exception as e:
        logs.append(f"link_select_exception:{e}")
        selected_urls = []

    # Parse base object
    merged_obj = None
    if base_json:
        try:
            base_data = json.loads(base_json)
            if isinstance(base_data, list) and base_data and isinstance(base_data[0], dict):
                merged_obj = base_data[0]
        except Exception:
            merged_obj = None

    # Follow-up extractions
    followup_objs: List[Dict[str, Any]] = []
    for fu in selected_urls:
        try:
            fu_json, fu_log = ai_extract_full_schema_from_page(browser, fu, drug_name, model=model)
            logs.append(f"follow:{fu[:120]}:{fu_log}")
            if fu_json:
                fu_data = json.loads(fu_json)
                if isinstance(fu_data, list) and fu_data and isinstance(fu_data[0], dict):
                    followup_objs.append(fu_data[0])
        except Exception as e:
            logs.append(f"follow_exception:{fu[:120]}:{e}")

    if merged_obj is None and followup_objs:
        merged_obj = followup_objs[0]

    if merged_obj is not None:
        for fo in followup_objs:
            merged_obj = _merge_fill_only(merged_obj, fo)

        # Always reduce to one program here (early normalization)
        merged_obj = reduce_to_single_program(merged_obj)
        merged_json = json.dumps([merged_obj], ensure_ascii=False)

        merged_json = postprocess_ai_extraction(merged_json, drop_if_no_programs=True, drop_if_only_discount_card=False)
        logs.append("two_pass_ok")
        return merged_json, ";".join(logs)

    # Base-only
    if base_json:
        base_json = postprocess_ai_extraction(base_json, drop_if_no_programs=True, drop_if_only_discount_card=False)
        logs.append("base_only_postprocessed")
    return base_json, ";".join(logs)


# =========================
# Extract key fields from schema for manufacturer_coupons
# =========================
def derive_manufacturer_fields_from_schema(ai_extraction_json: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    DROP-IN REPLACEMENT (NEW SCHEMA):
    Extract program_name, offer_text, phone_number from the new full-schema JSON.
    """
    if not ai_extraction_json:
        return None, None, None

    try:
        data = json.loads(ai_extraction_json)
    except Exception:
        return None, None, None

    if not isinstance(data, list) or not data:
        return None, None, None

    obj = data[0] if isinstance(data[0], dict) else None
    if not obj:
        return None, None, None

    programs = obj.get("programs") if isinstance(obj.get("programs"), list) else []
    if not programs:
        return None, None, None

    def _get_offer_from_program(p: Dict[str, Any]) -> Optional[str]:
        bl = p.get("benefit_logic") if isinstance(p.get("benefit_logic"), dict) else {}
        for branch in ("covered", "not_covered", "uninsured", "restrictions_not_met", "bridge_fill"):
            node = bl.get(branch) if isinstance(bl.get(branch), dict) else {}
            summ = node.get("summary") if isinstance(node.get("summary"), dict) else {}
            tldr = _text(summ.get("tldr"))
            if tldr:
                return tldr

        summ = p.get("summary") if isinstance(p.get("summary"), dict) else {}
        tldr2 = _text(summ.get("tldr"))
        if tldr2:
            return tldr2
        ui_blurb = _text(summ.get("ui_blurb"))
        if ui_blurb:
            return ui_blurb
        return None

    def _rank(p: Dict[str, Any]) -> Tuple[int, int]:
        t = _text(p.get("type")).lower()
        if t == "copay":
            type_rank = 0
        elif t == "discount_card":
            type_rank = 1
        else:
            type_rank = 2

        name = _text(p.get("name"))
        offer = _get_offer_from_program(p) or ""
        contact = p.get("contact") if isinstance(p.get("contact"), dict) else {}
        phone = _text(contact.get("phone"))

        substance_score = int(bool(name)) + int(bool(offer)) + int(bool(phone))
        return (type_rank, -substance_score)

    best = None
    best_key = None
    for p in programs:
        if not isinstance(p, dict):
            continue
        k = _rank(p)
        if best is None or k < best_key:
            best = p
            best_key = k

    if not best:
        return None, None, None

    program_name = _text(best.get("name")) or None
    offer_text = _get_offer_from_program(best)
    contact = best.get("contact") if isinstance(best.get("contact"), dict) else {}
    phone_number = _text(contact.get("phone")) or None

    if phone_number:
        phone_number = _normalize_phone(phone_number)

    return program_name, offer_text, phone_number


# =========================
# co-pay.com extraction
# =========================
def extract_activate_link(browser, activate_el, timeout=6):
    try:
        href = activate_el.get_attribute("href")
        if href and href.strip():
            return urljoin(browser.current_url, href.strip())
    except Exception:
        pass

    try:
        prev_handles = list(browser.window_handles)
        prev_url = browser.current_url
    except Exception:
        prev_handles = []
        prev_url = None

    try:
        try:
            activate_el.click()
        except Exception:
            browser.execute_script("arguments[0].click();", activate_el)
    except Exception:
        return None

    end_time = time.time() + timeout
    new_window = None
    while time.time() < end_time:
        try:
            handles = list(browser.window_handles)
            if len(handles) > len(prev_handles):
                new_handles = [h for h in handles if h not in prev_handles]
                if new_handles:
                    new_window = new_handles[0]
                    break
            if browser.current_url and prev_url and browser.current_url != prev_url:
                break
        except Exception:
            pass
        time.sleep(0.25)

    if new_window:
        try:
            original = browser.current_window_handle
            browser.switch_to.window(new_window)
            time.sleep(0.3)
            current = browser.current_url
            result = current if current and current.startswith("http") else None
            try:
                browser.close()
            except Exception:
                pass
            try:
                browser.switch_to.window(original)
            except Exception:
                if browser.window_handles:
                    browser.switch_to.window(browser.window_handles[0])
            return result
        except Exception:
            return None
    else:
        try:
            current = browser.current_url
            if current and prev_url and current != prev_url and current.startswith("http"):
                return current
        except Exception:
            pass
    return None


def co_pay_search_and_extract(browser, drug_name, wait_seconds=5):
    try:
        browser.get("https://co-pay.com")
        wait = WebDriverWait(browser, wait_seconds)

        search_xpaths = [
            "//input[@placeholder='Enter drug']",
            "//input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'enter drug')]",
            "//input[@type='search']",
            "//input[contains(@id,'search') or contains(@name,'search') or contains(@name,'q')]",
        ]
        search_el = None
        for xp in search_xpaths:
            try:
                search_el = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
                if search_el:
                    break
            except TimeoutException:
                continue

        if not search_el:
            return None, None, None, "co-pay: search input not found"

        try:
            try:
                search_el.click()
            except Exception:
                pass
            try:
                search_el.clear()
            except Exception:
                pass
            try:
                search_el.send_keys(drug_name)
                time.sleep(0.4)
            except Exception:
                browser.execute_script("arguments[0].value = arguments[1];", search_el, drug_name)
                time.sleep(0.25)
            search_el.send_keys(Keys.RETURN)
        except Exception as type_exc:
            return None, None, None, f"co-pay typing failed: {type_exc}"

        time.sleep(1.2)

        extracted_offer = None
        offer_xpaths = [
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'save up to') and normalize-space()][1]",
            "//div[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'discountstyles') and normalize-space()]",
        ]
        for xp in offer_xpaths:
            try:
                el = wait.until(EC.presence_of_element_located((By.XPATH, xp)))
                txt = el.text.strip()
                if txt:
                    extracted_offer = txt[:200].strip()
                    break
            except TimeoutException:
                continue
            except Exception:
                continue

        extracted_link = None
        activate_xpaths = [
            "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate') and (@href)]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate')]",
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate your coupon')]",
        ]
        activate_el = None
        for xp in activate_xpaths:
            try:
                activate_el = browser.find_element(By.XPATH, xp)
                if activate_el:
                    break
            except Exception:
                continue

        if activate_el:
            extracted_link = extract_activate_link(browser, activate_el)

        try:
            page_url = browser.current_url
        except Exception:
            page_url = None

        log = f"co-pay: offer_extracted={bool(extracted_offer)} link_extracted={bool(extracted_link)}"
        return extracted_offer, extracted_link, page_url, log

    except Exception as e:
        try:
            page_url = browser.current_url
        except Exception:
            page_url = None
        return None, None, page_url, f"co-pay error: {e}"


# =========================
# DuckDuckGo helpers
# =========================
def _is_bad_tracking_url(u: str) -> bool:
    if not u:
        return True
    try:
        p = urlparse(u)
    except Exception:
        return True
    host = (p.netloc or "").lower()
    path = (p.path or "").lower()
    if host.endswith("duckduckgo.com"):
        return True
    if "y.js" in path:
        return True
    if any(x in u.lower() for x in ["aclick", "ad_domain", "ad_provider", "click_metadata", "msclkid="]):
        return True
    return False


def search_duckduckgo_candidates_with_meta(browser, query, wait_seconds=0.8, max_results=8):
    from urllib.parse import parse_qs, unquote_plus

    results = []
    try:
        browser.get("https://duckduckgo.com/html/?q=" + quote_plus(query))
        time.sleep(wait_seconds)
        anchors = browser.find_elements(
            By.XPATH,
            "//a[contains(@class,'result__a') or starts-with(@href,'http') or starts-with(@href,'/l/') or contains(@href,'uddg=')]",
        )
    except Exception:
        anchors = []

    seen = set()
    for a in anchors:
        if len(results) >= max_results:
            break
        try:
            href = a.get_attribute("href")
            if not href:
                continue
            txt = (a.text or "").strip()
            parsed = urlparse(href)

            if parsed.netloc.endswith("duckduckgo.com") or parsed.path.startswith("/l/"):
                qs = parse_qs(parsed.query)
                uddg_vals = qs.get("uddg") or []
                if uddg_vals:
                    target = uddg_vals[0]
                    for _ in range(3):
                        new = unquote_plus(target)
                        if new == target:
                            break
                        target = new
                    if target.startswith("//"):
                        target = "https:" + target
                    if not urlparse(target).scheme:
                        target = "https://" + target
                    candidate = target
                else:
                    continue
            else:
                candidate = href
                if not urlparse(candidate).scheme:
                    candidate = "https://" + candidate

            if _is_bad_tracking_url(candidate):
                continue

            if candidate not in seen:
                seen.add(candidate)
                results.append({"url": candidate, "text": txt})
        except Exception:
            continue
    return results


def ai_select_candidate_from_search(candidates, drug_name, model="gpt-4.1"):
    if not candidates:
        return None, "no_candidates"

    items = []
    for i, c in enumerate(candidates, start=1):
        snippet = (c.get("text") or "").replace("\n", " ").strip()
        items.append(f"{i}. URL: {c['url']}\n   Snippet: {snippet}")

    system = 'Return JSON like {"index": 3, "url": "https://..."} or {"index": null, "url": null}.'
    user = f"Drug name: {drug_name}\n\nResults:\n" + "\n".join(items) + "\n\nReturn JSON."

    try:
        resp = _openai_chat_create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=200,
            temperature=0.0,
        )
    except Exception as e:
        return None, f"openai_error:{e}"

    try:
        content = resp["choices"][0]["message"]["content"]
    except Exception:
        content = getattr(resp.choices[0].message, "content", "") if hasattr(resp, "choices") else str(resp)

    chosen_url = None
    try:
        json_text = _extract_balanced_json(content) or _extract_braced_json(content) or content
        data = json.loads(json_text) if json_text else {}
        if data.get("url"):
            chosen_url = data["url"]
        elif data.get("index"):
            idx = int(data["index"])
            if 1 <= idx <= len(candidates):
                chosen_url = candidates[idx - 1]["url"]
    except Exception:
        pass

    return chosen_url, str(content)


# =========================
# DB init + migration
# =========================
conn = sqlite3.connect("goodrx_coupons.db")
cursor = conn.cursor()

cursor.execute(
    """
CREATE TABLE IF NOT EXISTS manufacturer_coupons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drug_name TEXT,
    program_name TEXT,
    manufacturer_url TEXT,
    offer_text TEXT,
    phone_number TEXT,
    confidence TEXT,
    has_copay_program INTEGER,
    last_extracted_at TEXT,
    extraction_log TEXT
)
"""
)

cursor.execute(
    """
CREATE TABLE IF NOT EXISTS ai_page_extractions (
    drug_name TEXT PRIMARY KEY,
    ai_extraction TEXT
)
"""
)

required_manufacturer_cols = {
    "drug_name": "TEXT",
    "program_name": "TEXT",
    "manufacturer_url": "TEXT",
    "offer_text": "TEXT",
    "phone_number": "TEXT",
    "confidence": "TEXT",
    "has_copay_program": "INTEGER",
    "last_extracted_at": "TEXT",
    "extraction_log": "TEXT",
}
ensure_table_columns(conn, "manufacturer_coupons", required_manufacturer_cols)
conn.close()


# =========================
# Main
# =========================
wb = openpyxl.load_workbook("Database_Send (2).xlsx")
sheet = wb.active

for row in sheet.iter_rows(min_row=2, values_only=True):
    if row[1] != "brand":
        continue

    drug_name = row[0]
    browser = None

    program_name = None
    website = None
    how_much_can_i_save = None
    phone_number = None
    confidence = "fallback"
    has_copay_program = 0
    extraction_log = None
    last_extracted_at = now_utc_iso()

    ai_extraction = None
    ai_extraction_log = None
    ai_extraction_url = None

    try:
        browser = create_chrome_driver()
        wait = WebDriverWait(browser, 10)

        # -------------------------
        # GoodRx path
        # -------------------------
        try:
            browser.get(f"https://www.goodrx.com/{str(drug_name).replace(' ', '-')}")
            time.sleep(1.0)

            display_drug_name = get_goodrx_display_drug_name(browser)

            manufacturer_button_xpaths = [
                "//button[contains(., 'Manufacturer')]",
                "//a[contains(., 'Manufacturer')]",
                "//*[self::button or self::a][contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'manufacturer')]",
            ]
            coupon_button = None
            for xp in manufacturer_button_xpaths:
                try:
                    coupon_button = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
                    if coupon_button:
                        break
                except TimeoutException:
                    continue

            if not coupon_button:
                raise TimeoutException("Manufacturer button not found")

            try:
                browser.execute_script("arguments[0].scrollIntoView({block:'center'});", coupon_button)
                time.sleep(0.2)
                coupon_button.click()
            except Exception:
                browser.execute_script("arguments[0].click();", coupon_button)

            modal = get_goodrx_manufacturer_modal(browser, timeout_seconds=10)
            if not modal:
                raise TimeoutException("Manufacturer modal not found")

            # Wait/retry for modal fields to populate
            end = time.time() + 6.0
            program_name = phone_number = website = how_much_can_i_save = None
            while time.time() < end:
                program_name = find_label_value(modal, "Program Name") or find_label_value(modal, "Program name")
                phone_number = (
                    find_label_value(modal, "Phone Number")
                    or find_label_value(modal, "Phone number")
                    or find_label_value(modal, "Phone")
                    or find_label_value(modal, "Contact")
                )
                website = (
                    href_after_label(modal, "Website")
                    or href_after_label(modal, "Web site")
                    or href_after_label(modal, "Site")
                )
                how_much_can_i_save = (
                    find_label_value(modal, "How much can I save")
                    or find_label_value(modal, "How much can I save?")
                    or find_label_value(modal, "Offer")
                    or find_label_value(modal, "Savings")
                )

                if any([(program_name or "").strip(), (phone_number or "").strip(), (website or "").strip(), (how_much_can_i_save or "").strip()]):
                    break
                time.sleep(0.25)

            if not any([(program_name or "").strip(), (phone_number or "").strip(), (website or "").strip(), (how_much_can_i_save or "").strip()]):
                raise TimeoutException("GoodRx manufacturer modal fields empty after wait")

            has_copay_program = 1
            confidence = "GoodRx"
            extraction_log = (
                f"GoodRx modal: program_name={'present' if program_name else 'missing'}; "
                f"phone_extracted={'yes' if phone_number else 'no'}; "
                f"website={'present' if website else 'missing'}; "
                f"offer_extracted={'yes' if how_much_can_i_save else 'no'}"
            )
            last_extracted_at = now_utc_iso()

            # 2-PASS schema extraction from modal website
            if website:
                ai_extraction_url = website
                try:
                    # ✅ FIX: 'link' was undefined in your original code.
                    # Start with the modal website, then add DDG-ranked URLs.
                    candidate_urls = [website]

                    candidates = search_duckduckgo_candidates_with_meta(browser, f"{drug_name} patient copay card")
                    raw_urls = [c.get("url") for c in candidates if c.get("url")]

                    chosen_url, choose_raw = ai_select_candidate_from_search(candidates, drug_name)
                    if chosen_url:
                        raw_urls = [chosen_url] + raw_urls

                    urls_to_try = pick_best_url(raw_urls, drug_name, preferred_domain=None, top_k=8)
                    candidate_urls.extend(urls_to_try)

                    ai_extraction, ai_extraction_url, ai_extraction_log, try_log = try_extract_schema_from_candidate_urls(
                        browser,
                        drug_name,
                        candidate_urls,
                    )

                    # ✅ FIX: 'log' was undefined; append to extraction_log instead.
                    extraction_log = (extraction_log + f"; schema_try_log={try_log}") if extraction_log else f"schema_try_log={try_log}"

                    if ai_extraction:
                        website = ai_extraction_url
                        has_copay_program = 1
                        confidence = "SE - ai-extracted"
                except Exception as ex:
                    ai_extraction = None
                    ai_extraction_log = f"exception:{ex}"

                if url_open_failed(ai_extraction_log) or schema_is_effectively_empty(ai_extraction):
                    ai_extraction = build_schema_from_goodrx_modal(
                        drug_name=drug_name,
                        display_drug_name=display_drug_name,
                        program_name=program_name,
                        website=website,
                        offer_text=how_much_can_i_save,
                        phone_number=phone_number,
                    )
                    ai_extraction_log = (ai_extraction_log or "") + "; forced_fallback=goodrx_modal"

                extraction_log += (
                    f"; ai_schema={'yes' if ai_extraction else 'no'}"
                    f"; ai_schema_url={ai_extraction_url}"
                    f"; ai_schema_log={ai_extraction_log}"
                )
            else:
                ai_extraction_url = ""
                ai_extraction = build_schema_from_goodrx_modal(
                    drug_name=drug_name,
                    display_drug_name=display_drug_name,
                    program_name=program_name,
                    website=website,
                    offer_text=how_much_can_i_save,
                    phone_number=phone_number,
                )
                ai_extraction_log = "forced_fallback=goodrx_modal_no_website"
                extraction_log += f"; ai_schema=yes; ai_schema_log={ai_extraction_log}"

        except TimeoutException as e:
            # -------------------------
            # Fallback path: co-pay + DuckDuckGo
            # -------------------------
            log = ""
            offer, link, page_url, log = co_pay_search_and_extract(browser, drug_name)

            if offer:
                how_much_can_i_save = offer
                has_copay_program = 1
                confidence = "fallback-copay"

            if link:
                website = link
                ai_extraction_url = link

                ai_extraction, ai_extraction_log = ai_extract_full_schema_two_pass(browser, ai_extraction_url, drug_name)

                # Fill manufacturer_coupons fields
                try:
                    ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, website, drug_name)
                    if ai_prog and not program_name:
                        program_name = ai_prog
                    if ai_offer and not how_much_can_i_save:
                        how_much_can_i_save = ai_offer
                    if ai_phone and not phone_number:
                        phone_number = _normalize_phone(ai_phone)
                    if any([ai_prog, ai_offer, ai_phone]):
                        confidence = "copay - ai-extracted"
                        has_copay_program = 1
                    log = (log + "; ai_log=" + ai_log) if log else ("ai_log=" + ai_log)
                except Exception as ai_exc:
                    log = (log + f"; ai_error={ai_exc}") if log else f"ai_error={ai_exc}"

                log = (
                    (log + f"; ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}")
                    if log
                    else f"ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}"
                )
            else:
                try:
                    candidates = search_duckduckgo_candidates_with_meta(browser, f"{drug_name} patient copay card")
                    raw_urls = [c.get("url") for c in candidates if c.get("url")]

                    chosen_url, choose_raw = ai_select_candidate_from_search(candidates, drug_name)
                    if chosen_url:
                        raw_urls = [chosen_url] + raw_urls

                    urls_to_try = pick_best_url(raw_urls, drug_name, preferred_domain=None, top_k=8)

                    ai_extraction, ai_extraction_url, ai_extraction_log, try_log = try_extract_schema_from_candidate_urls(
                        browser,
                        drug_name,
                        urls_to_try,
                    )

                    if ai_extraction_url:
                        website = ai_extraction_url
                        has_copay_program = 1
                        confidence = "SE - ai-extracted"

                    log = (log + f"; schema_try_log={try_log}") if log else f"schema_try_log={try_log}"

                except Exception as ddg_exc:
                    log = (log + f"; ddg_exception={ddg_exc}") if log else f"ddg_exception={ddg_exc}"

            if ai_extraction and (not program_name or not how_much_can_i_save or not phone_number):
                sp, so, sph = derive_manufacturer_fields_from_schema(ai_extraction)
                if sp and not program_name:
                    program_name = sp
                if so and not how_much_can_i_save:
                    how_much_can_i_save = so
                if sph and not phone_number:
                    phone_number = sph

            extraction_log = f"GoodRx path not usable; fallback_log={log}; website={website}; original_error={e}"
            last_extracted_at = now_utc_iso()

        except Exception as e:
            extraction_log = f"GoodRx path error: {type(e).__name__}: {e}"
            confidence = "fallback"
            has_copay_program = 0
            last_extracted_at = now_utc_iso()

    except Exception as outer_exc:
        extraction_log = f"outer_error: {type(outer_exc).__name__}: {outer_exc}"
        confidence = "fallback"
        has_copay_program = 0
        last_extracted_at = now_utc_iso()

    finally:
        conn = None
        try:
            if phone_number:
                phone_number = _normalize_phone(phone_number)

            # If we have a website but missing key fields, try derive from schema, then lightweight extraction
            if website and (not program_name or not how_much_can_i_save or not phone_number):
                if ai_extraction:
                    sp, so, sph = derive_manufacturer_fields_from_schema(ai_extraction)
                    if sp and not program_name:
                        program_name = sp
                    if so and not how_much_can_i_save:
                        how_much_can_i_save = so
                    if sph and not phone_number:
                        phone_number = sph

                if (not program_name or not how_much_can_i_save or not phone_number) and browser:
                    try:
                        ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, website, drug_name)
                        if ai_prog and not program_name:
                            program_name = ai_prog
                        if ai_offer and not how_much_can_i_save:
                            how_much_can_i_save = ai_offer
                        if ai_phone and not phone_number:
                            phone_number = _normalize_phone(ai_phone)
                        extraction_log = (extraction_log or "") + f"; final_lightweight_ai={ai_log}"
                    except Exception:
                        pass

            if extraction_log is None:
                extraction_log = f"no_extraction_log; confidence={confidence}; website={website}"
            if not last_extracted_at:
                last_extracted_at = now_utc_iso()

            # >>> FINAL ENFORCEMENT (single program + drop junk) <<<
            ai_extraction = postprocess_ai_extraction(
                ai_extraction,
                drop_if_no_programs=True,
                drop_if_only_discount_card=False,
            )
            if not ai_extraction:
                extraction_log = (extraction_log or "") + "; ai_extraction_dropped_postprocess"

            logging.info(
                "Inserting/updating row: drug=%s website=%s offer=%s confidence=%s log=%s",
                drug_name,
                website,
                how_much_can_i_save,
                confidence,
                extraction_log,
            )

            conn = sqlite3.connect("goodrx_coupons.db")
            cursor = conn.cursor()

            existing = None
            if website:
                try:
                    cursor.execute(
                        "SELECT id FROM manufacturer_coupons WHERE drug_name=? AND manufacturer_url=? LIMIT 1",
                        (drug_name, website),
                    )
                    existing = cursor.fetchone()
                except Exception:
                    existing = None

            if existing:
                record_id = existing[0]
                cursor.execute(
                    """
                    UPDATE manufacturer_coupons SET
                        program_name = ?,
                        offer_text = ?,
                        phone_number = ?,
                        confidence = ?,
                        has_copay_program = ?,
                        last_extracted_at = ?,
                        extraction_log = ?
                    WHERE id = ?
                    """,
                    (
                        program_name,
                        how_much_can_i_save,
                        phone_number,
                        confidence,
                        has_copay_program,
                        last_extracted_at,
                        extraction_log,
                        record_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO manufacturer_coupons (
                        drug_name,
                        program_name,
                        manufacturer_url,
                        offer_text,
                        phone_number,
                        confidence,
                        has_copay_program,
                        last_extracted_at,
                        extraction_log
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        drug_name,
                        program_name,
                        website,
                        how_much_can_i_save,
                        phone_number,
                        confidence,
                        has_copay_program,
                        last_extracted_at,
                        extraction_log,
                    ),
                )

            if ai_extraction:
                cursor.execute(
                    """
                    INSERT INTO ai_page_extractions (drug_name, ai_extraction)
                    VALUES (?, ?)
                    ON CONFLICT(drug_name) DO UPDATE SET
                        ai_extraction = excluded.ai_extraction
                    """,
                    (drug_name, ai_extraction),
                )

            conn.commit()

        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            if browser:
                try:
                    browser.quit()
                except Exception:
                    pass