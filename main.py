import openpyxl
import sqlite3
from datetime import datetime, timezone
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common import exceptions as selenium_exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin, quote_plus, urlparse
import time
from typing import Optional
import logging
import os
import re
import json
import ast
from dotenv import load_dotenv

load_dotenv()

# Configure logging so debug/info messages are visible. Set to DEBUG to see raw AI responses.
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

try:
    import openai
except Exception:
    openai = None


# Compatibility wrapper for different openai-python versions
def _openai_chat_create(messages, model='gpt-3.5-turbo', max_tokens=600, temperature=0.0):
    """Try to send a chat completion request using installed openai package.
    Supports both pre-1.0 (openai.ChatCompletion.create) and 1.0+ (openai.OpenAI().chat.completions.create).
    Returns the raw response object. Raises exception on failure.
    """
    if openai is None:
        raise RuntimeError('openai module not available')

    version = getattr(openai, '__version__', None)
    logging.debug('openai module version: %s', version)

    # Try old interface first (0.x)
    try:
        if hasattr(openai, 'ChatCompletion') and hasattr(openai.ChatCompletion, 'create'):
            logging.debug('Using openai.ChatCompletion.create (legacy)')
            return openai.ChatCompletion.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
    except Exception as e:
        logging.debug('legacy ChatCompletion.create failed: %s', e)

    # Try new interface (1.0+)
    try:
        if hasattr(openai, 'OpenAI'):
            logging.debug('Using openai.OpenAI.chat.completions.create (new)')
            client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
            return client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
    except Exception as e:
        logging.debug('new OpenAI client chat.completions.create failed: %s', e)
        raise

    # Last resort: try calling a generic attribute
    try:
        if hasattr(openai, 'chat'):
            # Some versions expose a chat namespace
            logging.debug('Using openai.chat.completions.create fallback')
            return openai.chat.completions.create(
                model=model, messages=messages, max_tokens=max_tokens, temperature=temperature
            )
    except Exception as e:
        logging.debug('fallback chat create failed: %s', e)
        raise

    raise RuntimeError('No compatible openai chat completion method found')


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- DOM helpers ---
def schema_is_effectively_empty(ai_extraction_json: Optional[str]) -> bool:
    """
    Treat schema as empty if:
    - missing/unparseable
    - empty list
    - OR only contains boilerplate like a disclaimer while all actionable fields are empty.

    IMPORTANT: disclaimer by itself does NOT count as a successful extraction.
    """
    if not ai_extraction_json:
        return True

    try:
        data = json.loads(ai_extraction_json)
    except Exception:
        return True

    if not isinstance(data, list) or len(data) == 0:
        return True

    obj = data[0] if isinstance(data[0], dict) else None
    if not obj:
        return True

    drug = obj.get("drug") or {}
    pricing = obj.get("pricing") or {}
    discount_card = (pricing.get("discount_card") or {}) if isinstance(pricing, dict) else {}
    offers = obj.get("offers") or []
    assistance = obj.get("assistance") or []

    # Signals that indicate we actually extracted something useful/actionable
    actionable_signals = []

    def add_actionable(val):
        if isinstance(val, str) and val.strip():
            actionable_signals.append(val.strip())

    # drug fields
    add_actionable(drug.get("name", ""))
    add_actionable(drug.get("generic", ""))
    add_actionable(drug.get("indication", ""))

    # pricing fields
    if isinstance(pricing, dict):
        add_actionable(pricing.get("cash_price", ""))
    add_actionable(discount_card.get("name", ""))
    add_actionable(discount_card.get("benefit", ""))
    add_actionable(discount_card.get("details", ""))

    # offers fields
    if isinstance(offers, list):
        for off in offers[:5]:
            if not isinstance(off, dict):
                continue
            add_actionable(off.get("title", ""))
            add_actionable(off.get("type", ""))      # counts if present
            add_actionable(off.get("benefit", ""))
            add_actionable(off.get("eligibility", ""))
            add_actionable(off.get("contact", ""))
            add_actionable(off.get("url", ""))       # important

    # assistance fields
    if isinstance(assistance, list):
        for a in assistance[:5]:
            if not isinstance(a, dict):
                continue
            add_actionable(a.get("provider", ""))
            add_actionable(a.get("benefit", ""))
            add_actionable(a.get("eligibility", ""))
            add_actionable(a.get("contact", ""))
            add_actionable(a.get("url", ""))

    # disclaimer is tracked but does NOT count as actionable by itself
    disclaimer = obj.get("disclaimer", "")
    has_disclaimer = isinstance(disclaimer, str) and bool(disclaimer.strip())

    # If there are no actionable signals, it's empty — even if disclaimer exists.
    if len(actionable_signals) == 0:
        return True

    # If the *only* signal is a generic offer.type or something minimal but no real content,
    # you can tighten further by requiring at least one of these "strong" fields:
    strong_fields_present = False

    strong_candidates = [
        drug.get("name", ""),
        (pricing.get("cash_price", "") if isinstance(pricing, dict) else ""),
        discount_card.get("benefit", ""),
    ]

    # also check offer strong fields
    if isinstance(offers, list):
        for off in offers[:5]:
            if isinstance(off, dict):
                strong_candidates.extend([
                    off.get("title", ""),
                    off.get("benefit", ""),
                    off.get("contact", ""),
                    off.get("url", ""),
                ])

    # assistance strong fields
    if isinstance(assistance, list):
        for a in assistance[:5]:
            if isinstance(a, dict):
                strong_candidates.extend([
                    a.get("provider", ""),
                    a.get("benefit", ""),
                    a.get("contact", ""),
                    a.get("url", ""),
                ])

    for v in strong_candidates:
        if isinstance(v, str) and v.strip():
            strong_fields_present = True
            break

    # If we only have weak signals but no strong fields, treat as empty.
    if not strong_fields_present:
        return True

    return False

def build_schema_from_goodrx_modal(
    drug_name: str,
    program_name: Optional[str],
    website: Optional[str],
    offer_text: Optional[str],
    phone_number: Optional[str],
) -> str:
    """
    Build the required JSON schema using ONLY the GoodRx manufacturer coupon popup fields.
    Returns a JSON string (list with one object).
    """
    # Prefer the program name shown in the popup; otherwise fall back to the drug_name
    primary_name = (program_name or "").strip() or (drug_name or "").strip()

    obj = [
        {
            "drug": {
                "name": primary_name,
                "generic": "",
                "indication": "",
            },
            "pricing": {
                "cash_price": "",
                "discount_card": {
                    "name": primary_name if primary_name else "",
                    "benefit": (offer_text or "").strip(),
                    "details": "",
                },
            },
            "offers": [
                {
                    "title": primary_name if primary_name else "Manufacturer Coupon",
                    "type": "copay",
                    "benefit": (offer_text or "").strip(),
                    "eligibility": "",
                    "contact": (phone_number or "").strip(),
                    "url": (website or "").strip(),  # IMPORTANT: actual URL string, not hyperlink text
                }
            ],
            "assistance": [],
            "disclaimer": "",
        }
    ]
    return json.dumps(obj, ensure_ascii=False)

def find_label_value(modal, label: str) -> Optional[str]:
    """Try multiple label matching strategies and return the following non-empty node text."""
    base = label.strip().rstrip(':?').strip()
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
                # sometimes the label itself contains the value or the structure differs
                txt = label_el.text.strip()
                if txt:
                    return txt
        except Exception:
            continue
    return None


def href_after_label(modal, label: str) -> Optional[str]:
    """Try to find an href following a label, or the following textual value."""
    base = label.strip().rstrip(':?').strip()
    xpaths = [
        f".//*[normalize-space()='{base}:']",
        f".//*[normalize-space()='{base}?']",
        f".//*[normalize-space()='{base}']",
        f".//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{base.lower()}')]",
    ]
    for xp in xpaths:
        try:
            label_el = modal.find_element(By.XPATH, xp)
            # prefer an <a> following the label
            try:
                link = label_el.find_element(By.XPATH, "following::*[1]//a[starts-with(@href,'http')]")
                href = link.get_attribute('href')
                if href:
                    return href
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

def _collect_dom_links_and_forms(browser, max_items: int = 250) -> str:
    """
    Collect anchor hrefs + their visible text/aria-label/title, plus form action URLs.
    Returns a compact text block the LLM can use to choose actual URLs.
    """
    try:
        data = browser.execute_script("""
            const out = {links: [], forms: []};

            // Links
            const links = Array.from(document.querySelectorAll('a[href]'));
            for (const a of links) {
              const href = a.href || a.getAttribute('href') || '';
              if (!href) continue;

              const text = (a.innerText || a.textContent || '').trim().replace(/\\s+/g,' ');
              const aria = (a.getAttribute('aria-label') || '').trim().replace(/\\s+/g,' ');
              const title = (a.getAttribute('title') || '').trim().replace(/\\s+/g,' ');

              out.links.push({
                text: text,
                aria: aria,
                title: title,
                href: href
              });
            }

            // Forms (sometimes enroll/activate are POST forms)
            const forms = Array.from(document.querySelectorAll('form[action]'));
            for (const f of forms) {
              const action = f.action || f.getAttribute('action') || '';
              if (!action) continue;
              out.forms.push({
                action: action,
                method: (f.method || '').toUpperCase()
              });
            }

            return out;
        """)

        lines = []
        links = (data or {}).get("links") or []
        forms = (data or {}).get("forms") or []

        # De-dupe by href/action but keep first label we see
        seen = set()

        for item in links:
            href = (item.get("href") or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)

            label = (item.get("text") or "").strip()
            if not label:
                # fallback to aria/title when innerText is empty
                label = (item.get("aria") or "").strip() or (item.get("title") or "").strip()
            if not label:
                label = "(no visible text)"

            # keep it compact
            if len(label) > 140:
                label = label[:140] + "…"

            lines.append(f'- "{label}" => {href}')
            if len(lines) >= max_items:
                break

        # Include forms after links
        for f in forms:
            action = (f.get("action") or "").strip()
            if not action or action in seen:
                continue
            seen.add(action)
            method = (f.get("method") or "").strip() or "GET/POST?"
            lines.append(f'- [FORM {method}] => {action}')
            if len(lines) >= max_items:
                break

        return "\n".join(lines)
    except Exception:
        return ""

# --- AI & phone helpers ---

def _extract_phone_from_text(text: str) -> Optional[str]:
    """Simple heuristic to find a phone-like string in page text."""
    if not text:
        return None
    # allow spaces, dots, or dashes between groups
    m = re.search(r'(\+?\d{1,3}[\s.-]?)?(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})', text)
    return m.group(0) if m else None


def _normalize_phone(raw: str) -> str:
    if not raw:
        return raw
    digits = re.sub(r'\D', '', raw or '')
    if len(digits) == 10:
        return f"+1-{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    if len(digits) == 11 and digits.startswith('1'):
        return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
    return raw.strip()


# Helper: extract the first balanced {...} substring from text.
def _extract_braced_json(text: str) -> Optional[str]:
    if not text:
        return None
    start_positions = [i for i, ch in enumerate(text) if ch == '{']
    for start in start_positions:
        depth = 0
        for i in range(start, len(text)):
            ch = text[i]
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    candidate = text[start:i+1]
                    # quick sanity: must contain a colon and a quote
                    if ':' in candidate and '"' in candidate:
                        return candidate
                    break
    return None


def _extract_balanced_json(text: str) -> Optional[str]:
    """Extract the first balanced JSON object {...} or array [...] from text."""
    if not text:
        return None

    first_obj = text.find('{')
    first_arr = text.find('[')

    if first_obj == -1 and first_arr == -1:
        return None

    if first_obj == -1 or (first_arr != -1 and first_arr < first_obj):
        start = first_arr
        open_ch, close_ch = '[', ']'
    else:
        start = first_obj
        open_ch, close_ch = '{', '}'

    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return text[start:i+1]
    return None


def ai_extract_from_page(browser, url: str, drug_name: str, model: str = 'gpt-3.5-turbo', timeout: int = 6):
    """Navigate to url, extract visible text, call OpenAI to parse program_name, offer_text, phone_number.
    Returns (program_name, offer_text, phone_number, log)
    If OpenAI is unavailable or API key missing, returns (None, None, None, 'ai_unavailable').
    """
    log_parts = []
    program_name = None
    offer_text = None
    phone_number = None

    if openai is None:
        log_parts.append('openai_not_installed')
        return None, None, None, ';'.join(log_parts)

    openai_api_key = os.environ.get('OPENAI_API_KEY')
    if not openai_api_key:
        log_parts.append('openai_api_key_missing')
        return None, None, None, ';'.join(log_parts)

    try:
        openai.api_key = openai_api_key
    except Exception:
        pass

    try:
        browser.get(url)
        time.sleep(1.0)
        try:
            page_text = browser.execute_script("return document.body.innerText || ''")
        except Exception:
            page_text = browser.page_source or ''

        MAX_CHARS = 1000000
        if len(page_text) > MAX_CHARS:
            page_text = page_text[:MAX_CHARS]

        system = (
            "You are a structured data extractor. Given visible page text, return JSON with keys "
            "\"program_name\", \"offer_text\", and \"phone_number\". If a field cannot be found, set it to null. "
            "Output strictly valid JSON and nothing else."
        )
        user = (
            f"Page text (for drug '{drug_name}'):\n\n{page_text}\n\n"
            "Extract the program or coupon name if present, a short relevant offer text (one sentence or a short phrase), "
            "and a contact phone number if present. Return: {\"program_name\":..., \"offer_text\":..., \"phone_number\":...}."
        )

        resp = _openai_chat_create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=600,
            temperature=0.0,
        )

        content = ''
        try:
            try:
                content = resp['choices'][0]['message']['content']
            except Exception:
                pass
            if not content:
                try:
                    content = getattr(resp.choices[0].message, 'content', '')
                except Exception:
                    pass
            if not content:
                try:
                    content = resp['choices'][0]['message']['content']
                except Exception:
                    pass
            if not content:
                try:
                    content = resp['choices'][0]['text']
                except Exception:
                    pass
            if not content:
                content = str(resp)
            if content is None:
                content = ''
            content = str(content).strip()
        except Exception:
            content = str(resp)

        raw_trunc = content if len(content) <= 1000 else content[:1000] + '...[truncated]'
        logging.debug('AI raw content (truncated): %s', raw_trunc.replace('\n', ' '))
        log_parts.append('ai_raw=' + raw_trunc.replace('\n', ' '))

        json_text = _extract_braced_json(content)
        if json_text is None:
            try:
                first = content.index('{')
                last = content.rindex('}')
                json_text = content[first:last+1]
            except Exception:
                json_text = content

        data = {}
        parse_success = False

        try:
            data = json.loads(json_text)
            parse_success = True
            log_parts.append('ai_parse_ok')
        except Exception as e1:
            log_parts.append(f'ai_json_parse_error:{e1}')
            try:
                data = ast.literal_eval(json_text)
                parse_success = True
                log_parts.append('ai_parsed_literal_eval')
            except Exception as e2:
                log_parts.append(f'ai_literal_eval_error:{e2}')
                try:
                    repaired = re.sub(r"(?<!\\)'", '"', json_text)
                    data = json.loads(repaired)
                    parse_success = True
                    log_parts.append('ai_parsed_repaired_json')
                except Exception as e3:
                    log_parts.append(f'ai_repair_error:{e3}')

        if parse_success:
            try:
                program_name = data.get('program_name') if data.get('program_name') else None
                offer_text = data.get('offer_text') if data.get('offer_text') else None
                phone_number = data.get('phone_number') if data.get('phone_number') else None
            except Exception as e:
                log_parts.append(f'ai_extract_fields_error:{e}')
        else:
            pn = _extract_phone_from_text(page_text)
            if pn:
                phone_number = pn
                log_parts.append('phone_heuristic_ok')

    except Exception as e:
        log_parts.append(f'exception:{e}')

    if phone_number:
        phone_number = _normalize_phone(phone_number)

    return program_name, offer_text, phone_number, ';'.join(log_parts)


def ai_extract_full_schema_from_page(browser, url: str, drug_name: str, model: str = 'gpt-3.5-turbo'):
    """
    Navigate to url, extract visible text + DOM link hrefs, call OpenAI to output STRICT JSON matching the required schema.
    Returns (ai_extraction_json_string_or_None, log_string).
    """
    log_parts = []

    if openai is None:
        return None, 'openai_not_installed'

    openai_api_key = os.environ.get('OPENAI_API_KEY')
    if not openai_api_key:
        return None, 'openai_api_key_missing'

    try:
        try:
            openai.api_key = openai_api_key
        except Exception:
            pass

        browser.get(url)
        time.sleep(1.0)

        try:
            page_text = browser.execute_script("return document.body.innerText || ''")
        except Exception:
            page_text = browser.page_source or ''

        # NEW: collect actual href URLs
        link_map_text = _collect_dom_links_and_forms(browser, max_items=250)

        MAX_CHARS = 280000
        if len(page_text) > MAX_CHARS:
            page_text = page_text[:MAX_CHARS]
            log_parts.append('page_text_truncated')

        if link_map_text:
            # keep this bounded too
            if len(link_map_text) > 80000:
                link_map_text = link_map_text[:80000] + "\n...[truncated]"
                log_parts.append('link_map_truncated')
        else:
            log_parts.append('no_links_collected')

        schema = r'''
[
  {
    "drug": {
      "name": "<Primary drug/program name shown on the page (string). Use brand name casing as displayed. Empty if not mentioned.>",
      "generic": "<Generic/active ingredient name if explicitly mentioned (string). Empty if not mentioned or not applicable.>",
      "indication": "<Primary condition/use this page is about (string). Prefer the simplest phrase used on-page. Empty if not mentioned.>"
    },
    "pricing": {
      "cash_price": "<Any explicit cash price, list price, or per-fill price shown (string). Preserve symbols like $ and commas. Empty if not mentioned.>",
      "discount_card": {
        "name": "<Name of the discount card/savings card/discount program (string). Empty if not mentioned.>",
        "benefit": "<One-line summary of savings benefit (string), e.g., 'Pay as little as $0' or 'Save up to 80%'. Empty if not mentioned.>",
        "details": "<Short extra details/terms (string). Include key caps (annual/monthly), 'not insurance', participating pharmacies count, frequency assumptions. Empty if not mentioned.>"
      }
    },
    "offers": [
      {
        "title": "<Name/title of an actionable offer/program on the page (string), e.g., 'Savings Card', 'Copay Program', 'Patient Support Program'.>",
        "type": "<Exactly one: copay | discount | rebate | free_trial | patient_support | other (string). Choose best match from page context.>",
        "benefit": "<Offer benefit summary (string). Include $0 language, max benefit amounts, etc. Empty if not mentioned.>",
        "eligibility": "<High-level eligibility rules (string). Include 'commercial insurance', age limits, exclusions (Medicare/Medicaid), residency, etc. Empty if not mentioned.>",
        "contact": "<Primary contact info for this offer (string). Prefer phone number if present; otherwise email or empty.>",
        "url": "<Primary URL to enroll/learn more for this offer (string). Use the most direct CTA link. Empty if not present.>"
      }
    ],
    "assistance": [
      {
        "provider": "<Name of an assistance organization/program (string), e.g., foundation or manufacturer assistance program.>",
        "benefit": "<What assistance they provide (string), e.g., 'financial assistance for copays', 'free drug for uninsured', 'reimbursement support'. Empty if not mentioned.>",
        "eligibility": "<Eligibility summary (string). Include income/FPL, insured/uninsured, diagnosis requirements, residency, etc. Empty if not mentioned.>",
        "contact": "<Contact phone/email (string). Empty if not present.>",
        "url": "<Website/CTA link for the assistance provider (string). Empty if not present.>"
      }
    ],
    "disclaimer": "<Short disclaimer text (string). Prefer the page's own wording about variability, eligibility, 'not insurance', terms apply. Empty if not present.>"
  }
]
'''.strip()

        system = (
            "You are a structured data extractor.\n"
            "You MUST output strictly valid JSON and nothing else.\n"
            "Your output MUST be a JSON array with exactly one object that matches the schema.\n\n"
            "CRITICAL URL RULE:\n"
            "- For any field named 'url', you MUST output the ACTUAL URL (href/action), not the hyperlink text.\n"
            "- Use the LINKS section to choose the correct href.\n"
            "- If you cannot find a real href/action for the CTA, leave url as an empty string.\n\n"
            "Other rules:\n"
            "- If a field is not explicitly mentioned, use an empty string.\n"
            "- offers[].type MUST be exactly one of: copay | discount | rebate | free_trial | patient_support | other\n"
            "- Keep strings short and faithful to the page wording.\n"
            "- offers and assistance must be arrays (can be empty arrays, but include the keys).\n"
        )

        user = (
            f"Drug context: {drug_name}\n"
            f"Page URL loaded: {url}\n\n"
            f"SCHEMA:\n{schema}\n\n"
            "LINKS (anchor text/label => ACTUAL href, and forms => action):\n"
            f"{link_map_text}\n\n"
            "PAGE TEXT:\n"
            f"{page_text}\n\n"
            "Return ONLY the JSON array."
        )

        resp = _openai_chat_create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=1600,
            temperature=0.0,
        )

        # Extract assistant content
        content = ''
        try:
            try:
                content = resp['choices'][0]['message']['content']
            except Exception:
                pass
            if not content:
                try:
                    content = getattr(resp.choices[0].message, 'content', '')
                except Exception:
                    pass
            if not content:
                try:
                    content = resp['choices'][0]['text']
                except Exception:
                    pass
            if not content:
                content = str(resp)
            content = str(content).strip()
        except Exception:
            content = str(resp).strip()

        raw_trunc = content if len(content) <= 1000 else content[:1000] + '...[truncated]'
        log_parts.append('ai_raw=' + raw_trunc.replace('\n', ' '))

        json_text = _extract_balanced_json(content)
        if not json_text:
            log_parts.append('ai_no_json_found')
            return None, ';'.join(log_parts)

        try:
            data = json.loads(json_text)
            if not isinstance(data, list):
                log_parts.append('ai_not_array')
                return None, ';'.join(log_parts)

            # Hard post-check: ensure url fields aren't obviously hyperlink text
            # (Optional but helpful: blank them if they aren't URLs)
            def _fix_url(v):
                if not isinstance(v, str):
                    return ""
                vv = v.strip()
                if not vv:
                    return ""
                if vv.startswith("http://") or vv.startswith("https://"):
                    return vv
                return ""  # not an actual URL -> empty

            for obj in data:
                offers = obj.get("offers") or []
                for off in offers:
                    off["url"] = _fix_url(off.get("url", ""))
                assists = obj.get("assistance") or []
                for a in assists:
                    a["url"] = _fix_url(a.get("url", ""))

            normalized = json.dumps(data, ensure_ascii=False)
            log_parts.append('ai_parse_ok')
            return normalized, ';'.join(log_parts)

        except Exception as e:
            log_parts.append(f'ai_json_parse_error:{e}')
            return None, ';'.join(log_parts)

    except Exception as e:
        log_parts.append(f'exception:{e}')
        return None, ';'.join(log_parts)


# --- fallback helpers ---

def search_google_for_copay(browser, query, wait_seconds=5):
    """(DEPRECATED) Placeholder kept for backward compatibility. Calls DuckDuckGo fallback."""
    return search_duckduckgo_for_copay(browser, query, wait_seconds)


def search_duckduckgo_for_copay(browser, query, wait_seconds=5):
    """Return the first external URL from a DuckDuckGo HTML search for `query`, or None."""
    from urllib.parse import parse_qs, unquote_plus

    try:
        browser.get("https://duckduckgo.com/html/?q=" + quote_plus(query))
        time.sleep(0.8)
        candidates = browser.find_elements(
            By.XPATH,
            "//a[contains(@class,'result__a') or contains(@class,'result__url') or starts-with(@href,'http') "
            "or starts-with(@href,'/l/') or contains(@href,'uddg=')]"
        )
    except Exception as exc:
        logging.debug('duckduckgo fetch error: %s', exc)
        candidates = []

    for a in candidates:
        try:
            href = a.get_attribute('href')
            if not href:
                continue

            parsed = urlparse(href)
            if parsed.netloc.endswith('duckduckgo.com') or parsed.path.startswith('/l/'):
                qs = parse_qs(parsed.query)
                uddg_vals = qs.get('uddg') or qs.get('u') or []
                target = None
                if uddg_vals:
                    target = uddg_vals[0]
                else:
                    if 'uddg=' in href:
                        try:
                            target = href.split('uddg=')[1].split('&')[0]
                        except Exception:
                            target = None
                if target:
                    for _ in range(3):
                        new = unquote_plus(target)
                        if new == target:
                            break
                        target = new
                    if target.startswith('//'):
                        target = 'https:' + target
                    if not urlparse(target).scheme:
                        target = 'https://' + target
                    logging.info('duckduckgo decoded uddg target: %s', target)
                    return target
                continue

            if href.startswith('/'):
                candidate = urljoin(browser.current_url or 'https://duckduckgo.com', href)
            else:
                candidate = href
            p = urlparse(candidate)
            if not p.scheme:
                candidate = 'https://' + candidate
            logging.info('duckduckgo candidate: %s', candidate)
            return candidate
        except Exception as e:
            logging.debug('duckduckgo candidate parse error: %s', e)
            continue
    return None


def search_duckduckgo_candidates_with_meta(browser, query, wait_seconds=0.8, max_results=8):
    """Return a list of candidate dicts: {'url':..., 'text':...} from DuckDuckGo search results."""
    from urllib.parse import parse_qs, unquote_plus

    results = []
    try:
        browser.get("https://duckduckgo.com/html/?q=" + quote_plus(query))
        time.sleep(wait_seconds)
        anchors = browser.find_elements(
            By.XPATH,
            "//a[contains(@class,'result__a') or contains(@class,'result__url') or starts-with(@href,'http') "
            "or starts-with(@href,'/l/') or contains(@href,'uddg=')]"
        )
    except Exception as exc:
        logging.debug('duckduckgo fetch error (meta): %s', exc)
        anchors = []

    seen = set()
    for a in anchors:
        if len(results) >= max_results:
            break
        try:
            href = a.get_attribute('href')
            if not href:
                continue
            txt = (a.text or '').strip()
            parsed = urlparse(href)

            if parsed.netloc.endswith('duckduckgo.com') or parsed.path.startswith('/l/'):
                qs = parse_qs(parsed.query)
                uddg_vals = qs.get('uddg') or qs.get('u') or []
                target = None
                if uddg_vals:
                    target = uddg_vals[0]
                else:
                    if 'uddg=' in href:
                        try:
                            target = href.split('uddg=')[1].split('&')[0]
                        except Exception:
                            target = None
                if target:
                    for _ in range(3):
                        new = unquote_plus(target)
                        if new == target:
                            break
                        target = new
                    if target.startswith('//'):
                        target = 'https:' + target
                    if not urlparse(target).scheme:
                        target = 'https://' + target
                    candidate = target
                else:
                    continue
            else:
                if href.startswith('/'):
                    candidate = urljoin(browser.current_url or 'https://duckduckgo.com', href)
                else:
                    candidate = href
                p = urlparse(candidate)
                if not p.scheme:
                    candidate = 'https://' + candidate

            if candidate and candidate not in seen:
                seen.add(candidate)
                results.append({'url': candidate, 'text': txt})
        except Exception as e:
            logging.debug('duckduckgo candidate meta parse error: %s', e)
            continue

    logging.debug('duckduckgo candidates with meta: %s', results)
    return results


def ai_select_candidate_from_search(candidates, drug_name, model='gpt-3.5-turbo'):
    """Ask the AI to pick the single most likely candidate (URL) to contain a copay program given search snippets.
    Returns (selected_url_or_None, ai_choice_raw)
    """
    if not candidates:
        return None, 'no_candidates'

    items = []
    for i, c in enumerate(candidates, start=1):
        text = c.get('text') or ''
        snippet = text.replace('\n', ' ').strip()
        items.append(f"{i}. URL: {c['url']}\n   Snippet: {snippet}")

    system = (
        "You are a helpful assistant that chooses which search result is most likely to contain a manufacturer's patient copay program or copay card for a prescription drug.\n"
        "Given a numbered list of search results (URL + snippet/title), pick the single result that is most likely to contain a copay program, coupon, or activation link for the drug.\n"
        "Only return the number of the chosen result and the chosen URL on a single line in JSON like: {\"index\": 3, \"url\": \"https://...\"}. If none look relevant, return {\"index\": null, \"url\": null}."
    )

    user = f"Drug name: {drug_name}\n\nResults:\n" + "\n".join(items) + "\n\nReturn JSON: {\"index\":..., \"url\":...}."

    try:
        resp = _openai_chat_create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_tokens=200,
            temperature=0.0,
        )
    except Exception as e:
        logging.debug('AI selection call failed: %s', e)
        return None, f'openai_error:{e}'

    content = ''
    try:
        try:
            content = resp['choices'][0]['message']['content']
        except Exception:
            pass
        if not content:
            try:
                content = getattr(resp.choices[0].message, 'content', '')
            except Exception:
                pass
        if not content:
            try:
                content = resp['choices'][0]['text']
            except Exception:
                pass
        if not content:
            content = str(resp)
        content = str(content).strip()
    except Exception:
        content = str(resp)

    chosen_url = None
    try:
        json_text = _extract_braced_json(content)
        if json_text:
            data = json.loads(json_text)
            url = data.get('url')
            idx = data.get('index')
            if url:
                chosen_url = url
            elif idx:
                try:
                    idx = int(idx)
                    if 1 <= idx <= len(candidates):
                        chosen_url = candidates[idx - 1]['url']
                except Exception:
                    chosen_url = None
    except Exception:
        chosen_url = None

    if not chosen_url:
        m = re.search(r"https?://[\w./?&=%#@-]+", content)
        if m:
            chosen_url = m.group(0)

    return chosen_url, content


def extract_activate_link(browser, activate_el, timeout=6):
    """Given an element (anchor/button) try to resolve the activation target URL."""
    try:
        href = activate_el.get_attribute('href')
        if href and href.strip():
            return urljoin(browser.current_url, href)
    except Exception:
        href = None

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
            browser.execute_script('arguments[0].click();', activate_el)
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
            if current and current.startswith('http'):
                result = current
            else:
                try:
                    a = browser.find_element(By.XPATH, "//a[starts-with(@href,'http')]")
                    result = a.get_attribute('href')
                except Exception:
                    result = None
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
            if current and prev_url and current != prev_url and current.startswith('http'):
                return current
        except Exception:
            pass
        try:
            a = browser.find_element(
                By.XPATH,
                "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate') and starts-with(@href,'http')]"
            )
            return a.get_attribute('href')
        except Exception:
            try:
                a2 = browser.find_element(By.XPATH, "//a[starts-with(@href,'http')]")
                return a2.get_attribute('href')
            except Exception:
                return None


def co_pay_search_and_extract(browser, drug_name, wait_seconds=5):
    """Search co-pay.com for drug_name, extract offer text and activation link if present.
    Returns (offer_text or None, link or None, page_url, log string)
    """
    try:
        browser.get('https://co-pay.com')
        wait = WebDriverWait(browser, wait_seconds)
        search_xpaths = [
            "//input[@placeholder='Enter drug']",
            "//input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'enter drug')]",
            "//input[@type='search']",
            "//input[@type='text' and (contains(@placeholder,'Search') or contains(@placeholder,'search'))]",
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
            return None, None, None, 'co-pay: search input not found'

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
                current_val = browser.execute_script(
                    "return arguments[0].value || arguments[0].textContent || '';",
                    search_el
                )
                if not current_val or drug_name.lower() not in current_val.lower():
                    raise Exception('send_keys did not set input')
            except Exception:
                browser.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input')); arguments[0].dispatchEvent(new Event('change'));",
                    search_el,
                    drug_name,
                )
                time.sleep(0.25)
            search_el.send_keys(Keys.RETURN)
        except Exception as type_exc:
            return None, None, None, f'co-pay typing failed: {type_exc}'

        time.sleep(1.2)

        extracted_offer = None
        offer_xpaths = [
            "//div[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'discountstyles') and normalize-space()]",
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'save up to') and normalize-space()][1]",
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'save up to')]/ancestor-or-self::*[normalize-space()!=''][1]",
        ]
        for xp in offer_xpaths:
            try:
                el = wait.until(EC.presence_of_element_located((By.XPATH, xp)))
                text = el.text.strip()
                if text:
                    lowered = text.lower()
                    idx = lowered.find('save up to')
                    if idx != -1:
                        extracted_offer = text[idx: idx + 200].strip()
                    else:
                        extracted_offer = text
                    break
            except TimeoutException:
                continue
            except Exception:
                continue

        if not extracted_offer:
            try:
                price_label = wait.until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'price')][1]"
                    ))
                )
                try:
                    maybe_offer = price_label.find_element(By.XPATH, "following::*[normalize-space()!=''][1]")
                    if maybe_offer and maybe_offer.text.strip():
                        extracted_offer = maybe_offer.text.strip()
                except Exception:
                    pass
            except TimeoutException:
                pass

        extracted_link = None
        try:
            activate_xpaths = [
                "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate') and (contains(@href,'http') or starts-with(@href,'/'))]",
                "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate')]",
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
        except Exception:
            extracted_link = None

        try:
            page_url = browser.current_url
        except Exception:
            page_url = None

        log = f'co-pay: offer_extracted={bool(extracted_offer)} link_extracted={bool(extracted_link)}'
        return extracted_offer, extracted_link, page_url, log

    except Exception as e:
        try:
            page_url = browser.current_url
        except Exception:
            page_url = None
        return None, None, page_url, f'co-pay error: {e}'


# ===== initialize DB schema (creates DB if it doesn't exist) =====
conn = sqlite3.connect("goodrx_coupons.db")
cursor = conn.cursor()
cursor.execute("""
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
""")

# NEW: two-column table for ai extraction
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_page_extractions (
    drug_name TEXT PRIMARY KEY,
    ai_extraction TEXT
)
""")

# Add any missing columns (no-op if already present)
additional_columns = {
    "manufacturer_url": "TEXT",
    "offer_text": "TEXT",
    "confidence": "TEXT",
    "has_copay_program": "INTEGER",
    "last_extracted_at": "TEXT",
    "extraction_log": "TEXT"
}
for col, col_type in additional_columns.items():
    try:
        cursor.execute(f"ALTER TABLE manufacturer_coupons ADD COLUMN {col} {col_type}")
        conn.commit()
    except sqlite3.OperationalError:
        pass

conn.close()

wb = openpyxl.load_workbook("Database_Send (2).xlsx")
sheet = wb.active

for row in sheet.iter_rows(min_row=2, values_only=True):
    if row[1] != "brand":
        continue

    drug_name = row[0]
    browser = None

    # default values
    program_name = None
    website = None
    how_much_can_i_save = None
    phone_number = None
    confidence = "fallback"
    has_copay_program = 0
    extraction_log = None
    last_extracted_at = now_utc_iso()

    # NEW: ai extraction storage (and what URL it came from)
    ai_extraction = None
    ai_extraction_log = None
    ai_extraction_url = None  # the URL we *actually* extracted from (per your rules)

    try:
        browser = webdriver.Chrome()
        wait = WebDriverWait(browser, 2)
        browser.get(f"https://www.goodrx.com/{drug_name.replace(' ', '-')}")

        coupon_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Manufacturer')]"))
        )
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", coupon_button)
        coupon_button.click()

        modal = wait.until(
            EC.visibility_of_element_located((
                By.XPATH,
                "//*[contains(., 'Program Name') and contains(., 'Phone Number') and contains(., 'Website')]"
            ))
        )

        program_name = find_label_value(modal, "Program Name")
        phone_number = find_label_value(modal, "Phone Number")
        website = href_after_label(modal, "Website")
        how_much_can_i_save = find_label_value(modal, "How much can I save")

        # Your original behavior
        has_copay_program = 1
        confidence = 'GoodRx'

        extraction_log = (
            f"GoodRx modal: program_name={'present' if program_name else 'missing'}; "
            f"phone_extracted={'yes' if phone_number else 'no'}; "
            f"website={'present' if website else 'missing'}; "
            f"offer_extracted={'yes' if how_much_can_i_save else 'no'}"
        )
        last_extracted_at = now_utc_iso()

        # NEW: GoodRx path AI extraction must use the URL shown as "Website"
        if website:
            ai_extraction_url = website
            ai_extraction = None
            ai_extraction_log = None

            try:
                ai_extraction, ai_extraction_log = ai_extract_full_schema_from_page(browser, ai_extraction_url,
                                                                                    drug_name)
            except Exception as ex:
                ai_extraction = None
                ai_extraction_log = f"ai_schema_exception:{ex}"

            # IMPORTANT: treat empty/blank schema as failure
            if schema_is_effectively_empty(ai_extraction):
                ai_extraction = build_schema_from_goodrx_modal(
                    drug_name=drug_name,
                    program_name=program_name,
                    website=website,
                    offer_text=how_much_can_i_save,
                    phone_number=phone_number,
                )
                ai_extraction_log = (ai_extraction_log or "") + "; fallback=goodrx_modal_schema"

            extraction_log += (
                f"; ai_schema={'yes' if ai_extraction else 'no'}"
                f"; ai_schema_url={ai_extraction_url}"
                f"; ai_schema_log={ai_extraction_log}"
            )

        else:
            # No website URL at all: always use popup fallback
            ai_extraction_url = ""
            ai_extraction = build_schema_from_goodrx_modal(
                drug_name=drug_name,
                program_name=program_name,
                website=website,
                offer_text=how_much_can_i_save,
                phone_number=phone_number,
            )
            ai_extraction_log = "fallback=goodrx_modal_schema_no_website"

            extraction_log += (
                f"; ai_schema=yes"
                f"; ai_schema_log={ai_extraction_log}"
            )

    except TimeoutException as e:
        # co-pay fallback: ensure we have a browser instance
        try:
            if browser is None:
                browser = webdriver.Chrome()
        except Exception as be:
            extraction_log = f"TimeoutException on GoodRx; couldn't create browser for fallback: {be}; original_error={e}"
            last_extracted_at = now_utc_iso()
        else:
            log = ''
            try:
                offer, link, page_url, log = co_pay_search_and_extract(browser, drug_name)
            except Exception as coe:
                offer = None
                link = None
                page_url = None
                log = f"co-pay exception: {coe}"

            if offer:
                how_much_can_i_save = offer
                has_copay_program = 1
                confidence = 'fallback-copay'

            # IMPORTANT: per your rules, co-pay AI extraction must use the activation URL (from Activate button)
            if link:
                website = link
                ai_extraction_url = link
                ai_extraction, ai_extraction_log = ai_extract_full_schema_from_page(browser, ai_extraction_url, drug_name)
                log = (log + f"; ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}") if log else f"ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}"

                # (Optional) keep your existing smaller extraction too
                try:
                    ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, website, drug_name)
                    if ai_prog and (not program_name or str(program_name).strip() == ''):
                        program_name = ai_prog
                    if ai_offer and (not how_much_can_i_save or str(how_much_can_i_save).strip() == ''):
                        how_much_can_i_save = ai_offer
                    if ai_phone and (not phone_number or str(phone_number).strip() == ''):
                        phone_number = _normalize_phone(ai_phone)
                    confidence = 'copay - ai-extracted'
                    log = (log + '; ai_log=' + ai_log) if log else ('ai_log=' + ai_log)
                except Exception as ai_exc:
                    log = (log + f'; ai_error={ai_exc}') if log else f'ai_error={ai_exc}'

            else:
                # No activation link found: DuckDuckGo fallback
                try:
                    candidates = search_duckduckgo_candidates_with_meta(browser, f"{drug_name} patient copay card")
                    dd_success = False

                    # AI choose best candidate first
                    try:
                        chosen_url, choose_raw = ai_select_candidate_from_search(candidates, drug_name)
                        log = (log + "; duckduckgo_ai_choice=" + (chosen_url or 'none')) if log else f'duckduckgo_ai_choice={(chosen_url or "none")}'

                        if chosen_url:
                            # NEW: DuckDuckGo path AI extraction must use the URL accessed
                            ai_extraction_url = chosen_url
                            ai_extraction, ai_extraction_log = ai_extract_full_schema_from_page(browser, ai_extraction_url, drug_name)
                            log = (log + f"; ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}") if log else f"ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}"

                            # keep existing small extraction to fill basic fields
                            ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, chosen_url, drug_name)
                            parsed_ok = ('ai_parse_ok' in (ai_log or '')) or ai_prog or ai_offer or ai_phone
                            if parsed_ok:
                                website = chosen_url
                                has_copay_program = 1
                                confidence = 'SE - ai-extracted'
                                if ai_prog:
                                    program_name = ai_prog
                                if ai_offer:
                                    how_much_can_i_save = ai_offer
                                if ai_phone:
                                    phone_number = _normalize_phone(ai_phone)
                                log = (log + '; ai_log=' + ai_log) if log else ('ai_log=' + ai_log)
                                dd_success = True

                    except Exception as sel_exc:
                        log = (log + f'; duckduckgo_ai_select_error={sel_exc}') if log else f'duckduckgo_ai_select_error={sel_exc}'

                    # If AI didn't pick a working candidate, probe sequentially
                    if not dd_success and candidates:
                        for cand in candidates:
                            try:
                                cand_url = cand['url']

                                # NEW: store schema extraction from the URL accessed (first one that works)
                                ai_extraction_url = cand_url
                                ai_extraction, ai_extraction_log = ai_extract_full_schema_from_page(browser, ai_extraction_url, drug_name)
                                log = (log + f"; ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}") if log else f"ai_schema={'yes' if ai_extraction else 'no'}; ai_schema_url={ai_extraction_url}; ai_schema_log={ai_extraction_log}"

                                ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, cand_url, drug_name)
                                parsed_ok = ('ai_parse_ok' in (ai_log or '')) or ai_prog or ai_offer or ai_phone
                                if parsed_ok:
                                    website = cand_url
                                    has_copay_program = 1
                                    confidence = 'SE - ai-extracted'
                                    if ai_prog:
                                        program_name = ai_prog
                                    if ai_offer:
                                        how_much_can_i_save = ai_offer
                                    if ai_phone:
                                        phone_number = _normalize_phone(ai_phone)
                                    log = (log + '; ai_log=' + ai_log) if log else ('ai_log=' + ai_log)
                                    dd_success = True
                                    break
                                else:
                                    log = (log + '; ai_log_probe=' + ai_log) if log else ('ai_log_probe=' + ai_log)
                            except Exception as probe_exc:
                                log = (log + f'; duckduckgo_probe_error={probe_exc}') if log else f'duckduckgo_probe_error={probe_exc}'
                                continue

                except Exception as dd_exc:
                    log = (log + f'; duckduckgo_error={dd_exc}') if log else f'duckduckgo_error={dd_exc}'
                    # NOTE: per your rule, DuckDuckGo path extraction must be from the URL accessed.
                    # If DuckDuckGo fails entirely, we do not fabricate an extraction URL.
                    try:
                        copay_search_url = 'https://co-pay.com/?q=' + quote_plus(drug_name)
                        website = copay_search_url
                        confidence = 'copay-search'
                        log = (log + "; copay_search_url=" + website) if log else f'copay_search_url={website}'
                    except Exception as dd_exc2:
                        log = (log + f'; copay_url_error={dd_exc2}') if log else f'copay_url_error={dd_exc2}'

            # Compose final extraction_log for this fallback path
            try:
                extraction_log = f"TimeoutException on GoodRx; fallback_log={log}; website={website}; original_error={e}"
            except Exception:
                extraction_log = f"TimeoutException on GoodRx; fallback_log={log}; website={website}"
            last_extracted_at = now_utc_iso()

    except (selenium_exceptions.NoSuchElementException, selenium_exceptions.StaleElementReferenceException) as e:
        extraction_log = f"DOM error: {type(e).__name__}: {e}"
        confidence = 'fallback'
        has_copay_program = 0
        last_extracted_at = now_utc_iso()

    finally:
        # insert a row regardless of success/failure for audit
        conn = None
        try:
            if phone_number:
                phone_number = _normalize_phone(phone_number)

            if extraction_log is None:
                extraction_log = f'no_extraction_log; confidence={confidence}; website={website}'
            if not last_extracted_at:
                last_extracted_at = now_utc_iso()

            logging.info(
                'Inserting/updating row: drug=%s website=%s offer=%s confidence=%s log=%s',
                drug_name, website, how_much_can_i_save, confidence, extraction_log
            )

            conn = sqlite3.connect("goodrx_coupons.db")
            cursor = conn.cursor()

            # Upsert manufacturer_coupons: try update if same drug+website exists
            if website:
                try:
                    cursor.execute(
                        "SELECT id FROM manufacturer_coupons WHERE drug_name=? AND manufacturer_url=? LIMIT 1",
                        (drug_name, website)
                    )
                    existing = cursor.fetchone()
                except Exception:
                    existing = None

                if existing:
                    record_id = existing[0]
                    try:
                        cursor.execute("""
                            UPDATE manufacturer_coupons SET
                                program_name = ?,
                                offer_text = ?,
                                phone_number = ?,
                                confidence = ?,
                                has_copay_program = ?,
                                last_extracted_at = ?,
                                extraction_log = ?
                            WHERE id = ?
                        """, (
                            program_name,
                            how_much_can_i_save,
                            phone_number,
                            confidence,
                            has_copay_program,
                            last_extracted_at,
                            extraction_log,
                            record_id
                        ))
                        conn.commit()
                    except Exception as upd_e:
                        logging.debug('Failed to update existing record: %s', upd_e)
                        cursor.execute("""
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
                        """, (
                            drug_name,
                            program_name,
                            website,
                            how_much_can_i_save,
                            phone_number,
                            confidence,
                            has_copay_program,
                            last_extracted_at,
                            extraction_log
                        ))
                        conn.commit()
                else:
                    cursor.execute("""
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
                    """, (
                        drug_name,
                        program_name,
                        website,
                        how_much_can_i_save,
                        phone_number,
                        confidence,
                        has_copay_program,
                        last_extracted_at,
                        extraction_log
                    ))
                    conn.commit()
            else:
                cursor.execute("""
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
                """, (
                    drug_name,
                    program_name,
                    website,
                    how_much_can_i_save,
                    phone_number,
                    confidence,
                    has_copay_program,
                    last_extracted_at,
                    extraction_log
                ))
                conn.commit()

            # NEW: Upsert into ai_page_extractions (two columns only)
            # Only write if we actually performed the required extraction from the required URL.
            if ai_extraction:
                try:
                    cursor.execute("""
                        INSERT INTO ai_page_extractions (drug_name, ai_extraction)
                        VALUES (?, ?)
                        ON CONFLICT(drug_name) DO UPDATE SET
                            ai_extraction = excluded.ai_extraction
                    """, (drug_name, ai_extraction))
                    conn.commit()
                except Exception as ex_ai_db:
                    logging.debug("Failed to upsert ai_page_extractions for %s: %s", drug_name, ex_ai_db)

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


