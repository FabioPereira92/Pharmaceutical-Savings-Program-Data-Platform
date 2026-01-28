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
            return openai.chat.completions.create(model=model, messages=messages, max_tokens=max_tokens, temperature=temperature)
    except Exception as e:
        logging.debug('fallback chat create failed: %s', e)
        raise

    raise RuntimeError('No compatible openai chat completion method found')


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- DOM helpers ---
def find_label_value(modal, label: str) -> Optional[str]:
    """Try multiple label matching strategies and return the following non-empty node text."""
    base = label.strip().rstrip(':?').strip()
    xpaths = [
        f".//*[normalize-space()='{base}:']",
        f".//*[normalize-space()='{base}?']",
        f".//*[normalize-space()='{base}']",
        f".//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{base.lower()}')]"]
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
        f".//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{base.lower()}')]"]
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

    # set key (safe to set repeatedly)
    try:
        openai.api_key = openai_api_key
    except Exception:
        # older clients use different config, but try to proceed
        pass

    try:
        browser.get(url)
        time.sleep(1.0)
        try:
            page_text = browser.execute_script("return document.body.innerText || ''")
        except Exception:
            page_text = browser.page_source or ''

        MAX_CHARS = 100000
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
        # Robust extraction of assistant content for many openai client shapes
        content = ''
        try:
            # dict-like access
            try:
                content = resp['choices'][0]['message']['content']
            except Exception:
                pass
            # attribute access
            if not content:
                try:
                    # new client may expose choices as list of objects
                    content = getattr(resp.choices[0].message, 'content', '')
                except Exception:
                    pass
            # alternative attribute/dict shapes
            if not content:
                try:
                    content = resp.choices[0]['message']['content']
                except Exception:
                    pass
            if not content:
                try:
                    content = resp['choices'][0]['message']['content']
                except Exception:
                    pass
            if not content:
                # older completions used 'text'
                try:
                    content = resp['choices'][0]['text']
                except Exception:
                    pass
            if not content:
                # fallback to stringifying the response object
                content = str(resp)
            if content is None:
                content = ''
            content = str(content).strip()
        except Exception:
            content = str(resp)

        # Truncate the raw content for logs/DB to avoid overly large fields
        raw_trunc = content if len(content) <= 1000 else content[:1000] + '...[truncated]'
        logging.debug('AI raw content (truncated): %s', raw_trunc.replace('\n', ' '))
        log_parts.append('ai_raw=' + raw_trunc.replace('\n', ' '))

        # try to extract a balanced JSON object from content
        json_text = _extract_braced_json(content)
        if json_text is None:
            # fallback to naive first/last brace substring
            try:
                first = content.index('{')
                last = content.rindex('}')
                json_text = content[first:last+1]
            except Exception:
                json_text = content

        # ensure 'data' is always defined for static analysis
        data = {}

        parse_success = False
        # Attempt 1: strict JSON
        try:
            data = json.loads(json_text)
            parse_success = True
            log_parts.append('ai_parse_ok')
        except Exception as e1:
            log_parts.append(f'ai_json_parse_error:{e1}')
            # Attempt 2: try ast.literal_eval (handles python literals)
            try:
                data = ast.literal_eval(json_text)
                parse_success = True
                log_parts.append('ai_parsed_literal_eval')
            except Exception as e2:
                log_parts.append(f'ai_literal_eval_error:{e2}')
                # Attempt 3: replace single quotes around keys/strings carefully
                try:
                    # conservative fallback: replace unescaped single quotes with double quotes
                    repaired = re.sub(r"(?<!\\)'", '"', json_text)
                    # last resort: ensure quote consistency
                    repaired = repaired
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
            # fallback: try simple phone heuristic
            pn = _extract_phone_from_text(page_text)
            if pn:
                phone_number = pn
                log_parts.append('phone_heuristic_ok')

    except Exception as e:
        log_parts.append(f'exception:{e}')

    if phone_number:
        phone_number = _normalize_phone(phone_number)

    return program_name, offer_text, phone_number, ';'.join(log_parts)


# --- fallback helpers ---

def search_google_for_copay(browser, query, wait_seconds=5):
    """(DEPRECATED) Placeholder kept for backward compatibility. Calls DuckDuckGo fallback."""
    return search_duckduckgo_for_copay(browser, query, wait_seconds)


def search_duckduckgo_for_copay(browser, query, wait_seconds=5):
    """Return the first external URL from a DuckDuckGo HTML search for `query`, or None.

    Improved heuristics: search common result anchors (including result__a), handle uddg and /l/
    redirects, and normalize returned URLs to include a scheme. Repeatedly unquote encoded uddg.
    """
    from urllib.parse import parse_qs, unquote_plus

    try:
        browser.get("https://duckduckgo.com/html/?q=" + quote_plus(query))
        time.sleep(0.8)
        # broaden candidate anchors to include common result link classes
        candidates = browser.find_elements(By.XPATH, "//a[contains(@class,'result__a') or contains(@class,'result__url') or starts-with(@href,'http') or starts-with(@href,'/l/') or contains(@href,'uddg=')]")
    except Exception as exc:
        logging.debug('duckduckgo fetch error: %s', exc)
        candidates = []

    for a in candidates:
        try:
            href = a.get_attribute('href')
            if not href:
                continue

            parsed = urlparse(href)
            # handle duckduckgo redirect: /l/?uddg=encoded or https://duckduckgo.com/l/?uddg=
            if parsed.netloc.endswith('duckduckgo.com') or parsed.path.startswith('/l/'):
                qs = parse_qs(parsed.query)
                uddg_vals = qs.get('uddg') or qs.get('u') or []
                target = None
                if uddg_vals:
                    target = uddg_vals[0]
                else:
                    # sometimes the encoded target is in the path or fragment; try to extract after 'uddg=' substring
                    if 'uddg=' in href:
                        try:
                            target = href.split('uddg=')[1].split('&')[0]
                        except Exception:
                            target = None
                if target:
                    # repeatedly unquote to handle double-encoding
                    for _ in range(3):
                        new = unquote_plus(target)
                        if new == target:
                            break
                        target = new
                    # normalize scheme
                    if target.startswith('//'):
                        target = 'https:' + target
                    if not urlparse(target).scheme:
                        target = 'https://' + target
                    logging.info('duckduckgo decoded uddg target: %s', target)
                    return target
                # skip other duckduckgo internal links
                continue

            # skip other duckduckgo internal links
            if 'duckduckgo.com' in href and not href.startswith('http'):
                continue

            # normalize: if relative, resolve; if missing scheme, add https
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


def search_duckduckgo_candidates(browser, query, wait_seconds=0.8, max_results=6):
    """Return a list of normalized candidate URLs from DuckDuckGo search results (order preserved).
    This collects multiple anchors (including uddg redirect targets) so callers can try them in sequence.
    """
    from urllib.parse import parse_qs, unquote_plus

    results = []
    try:
        browser.get("https://duckduckgo.com/html/?q=" + quote_plus(query))
        time.sleep(wait_seconds)
        anchors = browser.find_elements(By.XPATH, "//a[contains(@class,'result__a') or contains(@class,'result__url') or starts-with(@href,'http') or starts-with(@href,'/l/') or contains(@href,'uddg=')]")
    except Exception as exc:
        logging.debug('duckduckgo fetch error (candidates): %s', exc)
        anchors = []

    seen = set()
    for a in anchors:
        if len(results) >= max_results:
            break
        try:
            href = a.get_attribute('href')
            if not href:
                continue

            parsed = urlparse(href)
            # uddg redirect handling
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
                results.append(candidate)
        except Exception as e:
            logging.debug('duckduckgo candidate parse error: %s', e)
            continue

    logging.debug('duckduckgo candidates: %s', results)
    return results


def search_duckduckgo_candidates_with_meta(browser, query, wait_seconds=0.8, max_results=8):
    """Return a list of candidate dicts: {'url':..., 'text':...} from DuckDuckGo search results.
    This collects anchor href and visible text/snippet so the AI can decide which result is most likely to contain a copay program.
    """
    from urllib.parse import parse_qs, unquote_plus

    results = []
    try:
        browser.get("https://duckduckgo.com/html/?q=" + quote_plus(query))
        time.sleep(wait_seconds)
        anchors = browser.find_elements(By.XPATH, "//a[contains(@class,'result__a') or contains(@class,'result__url') or starts-with(@href,'http') or starts-with(@href,'/l/') or contains(@href,'uddg=')]")
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
            # uddg redirect handling
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

    # Build a compact prompt with numbered candidates
    items = []
    for i, c in enumerate(candidates, start=1):
        text = c.get('text') or ''
        # keep each item short
        snippet = text.replace('\n', ' ').strip()
        items.append(f"{i}. URL: {c['url']}\n   Snippet: {snippet}")

    system = (
        "You are a helpful assistant that chooses which search result is most likely to contain a manufacturer's patient copay program or copay card for a prescription drug.\n"
        "Given a numbered list of search results (URL + snippet/title), pick the single result that is most likely to contain a copay program, coupon, or activation link for the drug.\n"
        "Only return the number of the chosen result and the chosen URL on a single line in JSON like: {\"index\": 3, \"url\": \"https://...\"}. If none look relevant, return {\"index\": null, \"url\": null}."
    )

    user = (
        f"Drug name: {drug_name}\n\nResults:\n" + "\n".join(items) + "\n\nReturn JSON: {\"index\":..., \"url\":...}."
    )

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

    # extract assistant content robustly
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

    # Try to parse JSON from content
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
                        chosen_url = candidates[idx-1]['url']
                except Exception:
                    chosen_url = None
    except Exception:
        chosen_url = None

    # Fallback: try to find first http in content
    if not chosen_url:
        m = re.search(r"https?://[\w./?&=%#@-]+", content)
        if m:
            chosen_url = m.group(0)

    return chosen_url, content


def extract_activate_link(browser, activate_el, timeout=6):
    """Given an element (anchor/button) try to resolve the activation target URL.
    Returns resolved URL or None.
    """
    try:
        href = activate_el.get_attribute('href')
        if href and href.strip():
            return urljoin(browser.current_url, href)
    except Exception:
        href = None

    # Click and detect new tab or navigation
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
            # JS click fallback
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
            # or URL changed in same tab
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
        # fallback: find any http anchor mentioning 'activate' or first http anchor
        try:
            a = browser.find_element(By.XPATH, "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate') and starts-with(@href,'http')]")
            return a.get_attribute('href')
        except Exception:
            try:
                a2 = browser.find_element(By.XPATH, "//a[starts-with(@href,'http')]")
                return a2.get_attribute('href')
            except Exception:
                return None


def co_pay_search_and_extract(browser, drug_name, wait_seconds=5):
    """Search co-pay.com for drug_name, extract offer text and activation link if present.
    Returns (offer_text or None, link or None, log string)
    """
    try:
        browser.get('https://co-pay.com')
        wait = WebDriverWait(browser, wait_seconds)
        search_xpaths = [
            "//input[@placeholder='Enter drug']",
            "//input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'enter drug')]",
            "//input[@type='search']",
            "//input[@type='text' and (contains(@placeholder,'Search') or contains(@placeholder,'search'))]",
            "//input[contains(@id,'search') or contains(@name,'search') or contains(@name,'q')]"
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
            return None, None, 'co-pay: search input not found'

        # type with JS fallback
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
                current_val = browser.execute_script("return arguments[0].value || arguments[0].textContent || '';", search_el)
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
            return None, None, f'co-pay typing failed: {type_exc}'

        time.sleep(1.2)

        # extract offer
        extracted_offer = None
        offer_xpaths = [
            "//div[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'discountstyles') and normalize-space()]",
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'save up to') and normalize-space()][1]",
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'save up to')]/ancestor-or-self::*[normalize-space()!=''][1]"
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
                price_label = wait.until(EC.presence_of_element_located((
                    By.XPATH,
                    "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'price')][1]"
                )))
                try:
                    maybe_offer = price_label.find_element(By.XPATH, "following::*[normalize-space()!=''][1]")
                    if maybe_offer and maybe_offer.text.strip():
                        extracted_offer = maybe_offer.text.strip()
                except Exception:
                    pass
            except TimeoutException:
                pass

        # find activate/activation link
        extracted_link = None
        try:
            activate_xpaths = [
                "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate') and (contains(@href,'http') or starts-with(@href,'/'))]",
                "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate')]",
                "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate')]",
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'activate your coupon')]"
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
            else:
                extracted_link = None
        except Exception:
            extracted_link = None

        # also return the current page URL where the offer/link was observed
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
                    # otherwise continue searching
                    break
    return None

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

    try:
        browser = webdriver.Chrome()
        wait = WebDriverWait(browser, 2)
        browser.get(f"https://www.goodrx.com/{drug_name.replace(' ', '-')}")

        coupon_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Manufacturer')]") )
        )
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", coupon_button)
        coupon_button.click()

        # Wait for modal anchored on stable labels
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
        if how_much_can_i_save:
            has_copay_program = 1
            confidence = 'GoodRx'
        else:
            has_copay_program = 1
            confidence = 'GoodRx'
        # Record a concise extraction log for successful GoodRx modal extraction so DB rows are auditable
        extraction_log = (
            f"GoodRx modal: program_name={'present' if program_name else 'missing'}; "
            f"phone_extracted={'yes' if phone_number else 'no'}; "
            f"website={'present' if website else 'missing'}; "
            f"offer_extracted={'yes' if how_much_can_i_save else 'no'}"
        )
        last_extracted_at = now_utc_iso()

    except TimeoutException as e:
        # co-pay fallback: ensure we have a browser instance
        try:
            if browser is None:
                browser = webdriver.Chrome()
        except Exception as be:
            # If we can't create a browser, record and continue to finally/insert (don't re-raise)
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

            # decide website and whether to run AI
            should_run_ai = False
            ai_overwrite = False
            ai_source = None
            # prefer any link we found from co-pay
            if link:
                website = link
                should_run_ai = True
                ai_source = 'copay'
                logging.info('co-pay provided link: %s for %s', website, drug_name)
            else:
                # No activation link found on co-pay: use DuckDuckGo fallback to find a site and use AI to fill all fields
                try:
                    # Collect several DuckDuckGo result candidates and try them in order until AI extraction yields useful data
                    candidates = search_duckduckgo_candidates_with_meta(browser, f"{drug_name} patient copay card")
                    dd_success = False
                    chosen = None
                    try:
                        chosen_url, choose_raw = ai_select_candidate_from_search(candidates, drug_name)
                        log = (log + "; duckduckgo_ai_choice=" + (chosen_url or 'none')) if log else f'duckduckgo_ai_choice={(chosen_url or "none")}'
                        if chosen_url:
                            # try the AI-selected candidate first
                            try:
                                ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, chosen_url, drug_name)
                                log = (log + "; duckduckgo_candidate=" + chosen_url) if log else f'duckduckgo_candidate={chosen_url}'
                                parsed_ok = ('ai_parse_ok' in (ai_log or '')) or ai_prog or ai_offer or ai_phone
                                if parsed_ok:
                                    website = chosen_url
                                    confidence = 'duckduckgo-fallback'
                                    should_run_ai = False
                                    ai_overwrite = True
                                    ai_source = 'SE'
                                    try:
                                        if ai_prog:
                                            program_name = ai_prog
                                        if ai_offer:
                                            how_much_can_i_save = ai_offer
                                        if ai_phone:
                                            phone_number = _normalize_phone(ai_phone)
                                        has_copay_program = 1
                                    except Exception as persist_exc:
                                        log = (log + f'; ai_persist_error={persist_exc}') if log else f'ai_persist_error={persist_exc}'
                                    log = (log + '; ai_log=' + ai_log) if log else ('ai_log=' + ai_log)
                                    if ai_prog or ai_offer or ai_phone:
                                        confidence = 'SE - ai-extracted'
                                    logging.info('duckduckgo AI-selected candidate succeeded %s for %s', website, drug_name)
                                    dd_success = True
                                else:
                                    log = (log + '; ai_log_probe=' + ai_log) if log else ('ai_log_probe=' + ai_log)
                            except Exception as probe_exc:
                                log = (log + f'; duckduckgo_probe_error={probe_exc}') if log else f'duckduckgo_probe_error={probe_exc}'
                    except Exception as sel_exc:
                        log = (log + f'; duckduckgo_ai_select_error={sel_exc}') if log else f'duckduckgo_ai_select_error={sel_exc}'

                    # If AI didn't pick a working candidate, fall back to probing candidates sequentially
                    if not dd_success and candidates:
                        for cand in candidates:
                            try:
                                ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, cand['url'], drug_name)
                                log = (log + "; duckduckgo_candidate=" + cand['url']) if log else f'duckduckgo_candidate={cand["url"]}'
                                parsed_ok = ('ai_parse_ok' in (ai_log or '')) or ai_prog or ai_offer or ai_phone
                                if parsed_ok:
                                    website = cand['url']
                                    confidence = 'duckduckgo-fallback'
                                    should_run_ai = False
                                    ai_overwrite = True
                                    ai_source = 'SE'
                                    try:
                                        if ai_prog:
                                            program_name = ai_prog
                                        if ai_offer:
                                            how_much_can_i_save = ai_offer
                                        if ai_phone:
                                            phone_number = _normalize_phone(ai_phone)
                                        has_copay_program = 1
                                    except Exception as persist_exc:
                                        log = (log + f'; ai_persist_error={persist_exc}') if log else f'ai_persist_error={persist_exc}'
                                    log = (log + '; ai_log=' + ai_log) if log else ('ai_log=' + ai_log)
                                    if ai_prog or ai_offer or ai_phone:
                                        confidence = 'SE - ai-extracted'
                                    logging.info('duckduckgo candidate succeeded %s for %s', website, drug_name)
                                    dd_success = True
                                    break
                                else:
                                    log = (log + '; ai_log_probe=' + ai_log) if log else ('ai_log_probe=' + ai_log)
                                    continue
                            except Exception as probe_exc:
                                log = (log + f'; duckduckgo_probe_error={probe_exc}') if log else f'duckduckgo_probe_error={probe_exc}'
                                continue
                except Exception as dd_exc:
                    # an error during duckduckgo processing: fallback to co-pay search URL
                    log = (log + f'; duckduckgo_error={dd_exc}') if log else f'duckduckgo_error={dd_exc}'
                    try:
                        copay_search_url = 'https://co-pay.com/?q=' + quote_plus(drug_name)
                        website = copay_search_url
                        confidence = 'copay-search'
                        log = (log + "; copay_search_url=" + website) if log else f'copay_search_url={website}'
                    except Exception as dd_exc2:
                        log = (log + f'; copay_url_error={dd_exc2}') if log else f'copay_url_error={dd_exc2}'

            # AI extraction: run AI only when an activation link has been found or when DuckDuckGo provided a result
            if should_run_ai:
                try:
                    ai_prog, ai_offer, ai_phone, ai_log = ai_extract_from_page(browser, website, drug_name)
                    # If this AI run came from DuckDuckGo, allow it to overwrite existing fields to "fill all fields"
                    if ai_overwrite:
                        if ai_prog:
                            program_name = ai_prog
                            # AI extracted a program; set confidence by source
                            if ai_source == 'copay':
                                confidence = 'copay - ai-extracted'
                            elif ai_source == 'SE':
                                confidence = 'SE - ai-extracted'
                            else:
                                confidence = 'ai-extracted'
                        if ai_offer:
                            how_much_can_i_save = ai_offer
                            if ai_source == 'copay':
                                confidence = 'copay - ai-extracted'
                            elif ai_source == 'SE':
                                confidence = 'SE - ai-extracted'
                            else:
                                confidence = 'ai-extracted'
                        if ai_phone:
                            phone_number = _normalize_phone(ai_phone)
                            if ai_source == 'copay':
                                confidence = 'copay - ai-extracted'
                            elif ai_source == 'SE':
                                confidence = 'SE - ai-extracted'
                            else:
                                confidence = 'ai-extracted'
                    else:
                        if ai_prog and (not program_name or str(program_name).strip() == ''):
                            program_name = ai_prog
                            if ai_source == 'copay':
                                confidence = 'copay - ai-extracted'
                            elif ai_source == 'SE':
                                confidence = 'SE - ai-extracted'
                            else:
                                confidence = 'ai-extracted'
                        if ai_offer and (not how_much_can_i_save or str(how_much_can_i_save).strip() == ''):
                            how_much_can_i_save = ai_offer
                            if ai_source == 'copay':
                                confidence = 'copay - ai-extracted'
                            elif ai_source == 'SE':
                                confidence = 'SE - ai-extracted'
                            else:
                                confidence = 'ai-extracted'
                        if ai_phone and (not phone_number or str(phone_number).strip() == ''):
                            phone_number = _normalize_phone(ai_phone)
                            if ai_source == 'copay':
                                confidence = 'copay - ai-extracted'
                            elif ai_source == 'SE':
                                confidence = 'SE - ai-extracted'
                            else:
                                confidence = 'ai-extracted'
                    log = (log + '; ai_log=' + ai_log) if log else ('ai_log=' + ai_log)
                except Exception as ai_exc:
                    log = (log + f'; ai_error={ai_exc}') if log else f'ai_error={ai_exc}'

            # Compose a final extraction_log for this TimeoutException fallback path so it is saved in DB
            try:
                extraction_log = f"TimeoutException on GoodRx; co-pay_log={log}; website={website}; original_error={e}"
            except Exception:
                # fallback to include what we have
                extraction_log = f"TimeoutException on GoodRx; co-pay_log={log}; website={website}"
            last_extracted_at = now_utc_iso()

    except (selenium_exceptions.NoSuchElementException, selenium_exceptions.StaleElementReferenceException) as e:
        extraction_log = f"DOM error: {type(e).__name__}: {e}"
        confidence = 'fallback'
        has_copay_program = 0
        last_extracted_at = now_utc_iso()

    finally:
        # insert a row regardless of success/failure for audit
        try:
            # normalize phone before insert
            if phone_number:
                phone_number = _normalize_phone(phone_number)

            # Ensure extraction_log and last_extracted_at are set so DB rows are auditable
            if extraction_log is None:
                extraction_log = f'no_extraction_log; confidence={confidence}; website={website}'
            if not last_extracted_at:
                last_extracted_at = now_utc_iso()

            logging.info('Inserting/updating row: drug=%s website=%s offer=%s confidence=%s log=%s', drug_name, website, how_much_can_i_save, confidence, extraction_log)
            conn = sqlite3.connect("goodrx_coupons.db")
            cursor = conn.cursor()

            # If we discovered a website, avoid duplicate rows: try to update existing record for same drug + website
            if website:
                try:
                    cursor.execute("SELECT id FROM manufacturer_coupons WHERE drug_name=? AND manufacturer_url=? LIMIT 1", (drug_name, website))
                    row = cursor.fetchone()
                except Exception:
                    row = None

                if row:
                    record_id = row[0]
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
                        # fallback to insert
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
                    # no existing row: insert new
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
                # No website found: still insert an audit row for traceability
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
        finally:
            conn.close()
            if browser:
                try:
                    browser.quit()
                except Exception:
                    pass

