# Pharmaceutical Savings Data Platform

AI-powered data extraction platform that discovers, ranks, and normalizes manufacturer copay and patient assistance programs from GoodRx, manufacturer websites, and PDFs using hybrid crawling, Selenium rendering, AI schema extraction, and deterministic post-processing.

---

## Overview

This project is a production-style data pipeline built to solve a real-world problem:

> How do you reliably extract structured pharmaceutical savings program data from inconsistent, bot-protected, and multi-format sources?

Manufacturer copay programs are distributed across:
- GoodRx manufacturer modals
- JS-heavy manufacturer websites
- Enrollment portals
- Terms & conditions pages
- PDF savings cards
- Assistance program documents

This platform handles all of it — automatically.

---

## Architecture

### High-Level Flow

1. **Input Source**
   - Excel file containing brand drug names
2. **Primary Path**
   - Scrape GoodRx manufacturer modal
3. **Multi-Source Expansion**
   - Manufacturer website
   - DuckDuckGo ranked candidates
4. **Hybrid Fetch Strategy**
   - `crawl4ai_fetch` (fast HTML fetch)
   - Selenium fallback for:
     - JS-rendered pages
     - Bot walls
     - Shell pages
5. **PDF Intelligence**
   - PyMuPDF / pdfplumber extraction
   - AI-driven structured parsing
6. **AI Schema Extraction**
   - Strict JSON schema enforcement
   - No guessing, no hallucination rules
7. **Deterministic Post-Processing**
   - Single-program enforcement
   - Ranking by:
     - Program type priority
     - Confidence tier
     - Actionability
     - Completeness
8. **Persistence**
   - SQLite storage
   - Structured program JSON
   - Human-readable summary fields

---

## Key Engineering Features

### Hybrid Crawling Strategy

- Uses lightweight crawler first
- Falls back to Selenium only when necessary
- Detects:
  - Bot walls
  - JS-only shells
  - Cookie walls
  - Blocked responses

Minimizes automation footprint while maximizing reliability.

---

### Intelligent URL Ranking

Candidate URLs are scored using:

- Drug token path matching
- Savings/copay keyword signals
- Manufacturer domain hints
- Aggregator penalties
- PDF intent detection
- Generic landing page rejection

Ensures extraction attempts focus on high-probability sources.

---

### Full Structured Schema

Each drug produces a normalized JSON object:

- `drug`
- `programs[]`
- `benefit_logic`
- `eligibility`
- `compliance`
- `contact`
- `cta`
- `sources`
- `audit fields`

Schema enforcement rules:

- No missing keys
- Explicit nulls where data is absent
- Strict enum normalization
- Monetary values normalized to numbers
- ISO8601 timestamps
- Deterministic reduction to one best program

---

### PDF Intelligence

PDF documents are:

- Downloaded (if remote)
- Parsed via:
  - PyMuPDF (preferred)
  - pdfplumber (fallback)
- Structured text extracted
- Phone numbers and dollar amounts detected
- AI extracts structured program logic from raw PDF text

PDF pages often contain the *real* terms — this system captures them.

---

### Two-Pass Extraction

Pass 1:
- Extract from base page

Pass 2:
- AI selects likely follow-up links (terms, enroll, eligibility)
- Extract from those pages
- Merge using fill-only logic
- Never overwrite non-empty fields
- Enforce single best program

This mimics how a human researcher navigates a site.

---

## Database Output

### SQLite: `goodrx_coupons.db`

### Table: `manufacturer_coupons`

Human-facing fields:

- `drug_name`
- `program_name`
- `manufacturer_url`
- `offer_text`
- `phone_number`
- `confidence`
- `has_copay_program`
- `last_extracted_at`
- `extraction_log`

---

### Table: `ai_page_extractions`

Stores full normalized schema JSON per drug.

This allows:
- Downstream API use
- Structured analysis
- Program logic interpretation
- Auditable source tracking

---

## Technologies Used

- Python 3
- Selenium
- OpenAI API
- SQLite
- PyMuPDF
- pdfplumber
- requests
- openpyxl
- dotenv

---

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install openpyxl selenium python-dotenv requests openai PyMuPDF pdfplumber
```

## Configuration

Create a `.env` file in the project root with your OpenAI API key:

OPENAI_API_KEY=your_key_here

---

## Input Format

Excel file: Database_Send (2).xlsx

Expected structure:

| drug_name | type    |
|-----------|---------|
| Zepbound  | brand   |
| Humira    | brand   |
| Metformin | generic |

Only rows marked "brand" are processed.

---

## Running

Run the script:

python main.py

---

## Outputs

The script generates:

- goodrx_coupons.db (SQLite database)
- Structured schema JSON per processed drug
- Logged extraction trails stored in the database

---

## Failure Handling & Edge Cases

The system explicitly handles:

- Missing manufacturer modals  
- Blocked pages  
- Cookie walls  
- JS-rendered shells  
- Dead links  
- PDF-only programs  
- Aggregator-only results  
- Discount-card noise  
- Multi-program conflicts  

Every extraction is logged for traceability and auditing.

---

## Why This Project Matters

This is not a simple scraper.

It demonstrates:

- Multi-source orchestration  
- AI integration with strict schema control  
- Deterministic ranking systems  
- Failure-aware automation  
- Structured data modeling  
- Real-world edge case handling  
- Production-style persistence  
- Defensive programming patterns  

It reflects backend engineering and data platform thinking — not toy automation.

---

## Potential Extensions

- REST API layer  
- Dockerized deployment  
- PostgreSQL migration  
- Scheduled job execution  
- Program versioning  
- Cost analysis modeling  
- Automated monitoring  

---

## License

Add your preferred license (MIT / Apache 2.0 / Proprietary).
