import openpyxl, time, re, sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def value_after_exact_label(modal, label: str) -> str:
    label_el = modal.find_element(By.XPATH, f".//*[normalize-space()='{label}:']")
    value_el = label_el.find_element(By.XPATH, "following::*[normalize-space()!=''][1]")
    return value_el.text.strip()

def href_after_exact_label(modal, label: str) -> str:
    label_el = modal.find_element(By.XPATH, f".//*[normalize-space()='{label}:']")
    try:
        link = label_el.find_element(By.XPATH, "following::*[1]//a[starts-with(@href,'http')]")
        return link.get_attribute("href")
    except Exception:
        value_el = label_el.find_element(By.XPATH, "following::*[normalize-space()!=''][1]")
        return value_el.text.strip()

def extract_savings(modal) -> str:
    """
    Try to extract the savings answer from the modal.
    1) Find the section containing 'How much can I save' and look for a $ amount inside it.
    2) Fallback: find the first $ amount anywhere in the modal.
    3) Fallback: return empty string.
    """
    # 1) find the node containing the phrase
    q = modal.find_element(
        By.XPATH,
        ".//*[contains(translate(normalize-space(.),"
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),"
        "'how much can i save')]"
    )

    # climb to a nearby container and search within it for something with a $
    # (try a few ancestors; UI structures vary)
    for up in range(1, 6):
        try:
            container = q.find_element(By.XPATH, f"./ancestor::*[{up}]")
            txt = container.get_attribute("innerText") or ""
            m = re.search(r"\$\s?\d+(?:,\d{3})*(?:\.\d{2})?", txt)
            if m:
                return m.group(0).replace(" ", "")
        except Exception:
            pass

    # 2) fallback: search whole modal for a $ amount
    modal_txt = modal.get_attribute("innerText") or ""
    m = re.search(r"\$\s?\d+(?:,\d{3})*(?:\.\d{2})?", modal_txt)
    if m:
        return m.group(0).replace(" ", "")

    return ""

# ===== connect (creates DB if it doesn't exist) =====
conn = sqlite3.connect("goodrx_coupons.db")
cursor = conn.cursor()

# ===== create table =====
cursor.execute("""
CREATE TABLE IF NOT EXISTS manufacturer_coupons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drug_name TEXT,
    program_name TEXT,
    phone_number TEXT,
    website TEXT,
    how_much_can_i_save TEXT,
    has_copay_program INTEGER
)
""")

conn.close()

wb = openpyxl.load_workbook("Database_Send (2).xlsx")
sheet = wb.active

for row in sheet.iter_rows(min_row=2, values_only=True):
    if row[1] == "brand":
        drug_name = row[0]

        browser = webdriver.Chrome()
        wait = WebDriverWait(browser, 30)
        browser.get("https://www.goodrx.com/")

        # wait for DOM ready
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

        # get BOTH, choose the visible/enabled one
        boxes = wait.until(lambda d: d.find_elements(By.ID, "hero-drug-search-input"))
        box = next(b for b in boxes if b.is_displayed() and b.is_enabled())

        # interact
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", box)
        browser.execute_script("arguments[0].focus();", box)
        box.click()
        box.send_keys(drug_name)
        time.sleep(2)
        box.send_keys(Keys.ENTER)

        coupon_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Manufacturer')]"))
        )
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", coupon_button)
        coupon_button.click()

        # Wait for modal by anchoring on stable labels in the popup
        modal = wait.until(
            EC.visibility_of_element_located((
                By.XPATH,
                "//*[contains(., 'Program Name') and contains(., 'Phone Number') and contains(., 'Website')]"
            ))
        )

        program_name = value_after_exact_label(modal, "Program Name")
        phone_number = value_after_exact_label(modal, "Phone Number")
        website = href_after_exact_label(modal, "Website")
        how_much_can_i_save = extract_savings(modal)
        has_copay_program = 1

        # ===== connect (creates DB if it doesn't exist) =====
        conn = sqlite3.connect("goodrx_coupons.db")
        cursor = conn.cursor()

        # ===== insert data =====
        cursor.execute("""
        INSERT INTO manufacturer_coupons (
            drug_name,
            program_name,
            phone_number,
            website,
            how_much_can_i_save,
            has_copay_program
        ) VALUES (?, ?, ?, ?, ?)
        """, (
            drug_name,
            program_name,
            phone_number,
            website,
            how_much_can_i_save,
            has_copay_program
        ))

        # ===== commit & close =====
        conn.commit()
        conn.close()