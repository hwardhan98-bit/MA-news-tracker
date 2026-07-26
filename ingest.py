#!/usr/bin/env python3
"""
M&A tracker — ingest job.

Pulls deal news from free, key-less public sources, extracts structured deal
records with pattern rules (no LLM, no API cost), merges them into deals.json.

Run locally:   python3 ingest.py
Run scheduled: see .github/workflows/ingest.yml

Standard library only. No pip install required.
"""

import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

OUT_FILE = os.environ.get("DEALS_FILE", "deals.json")
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "1825"))  # 5 years

# SEC requires a descriptive User-Agent with contact details. Set this or your
# requests will be throttled or blocked.
UA = os.environ.get(
    "INGEST_USER_AGENT",
    "MA-Tracker/1.0 (contact: you@example.com)"
)


# --------------------------------------------------------------------------
# 1. Sources
# --------------------------------------------------------------------------

# Google News caps each query at roughly 100 items, so volume comes from running
# many narrow queries across several editions rather than one broad query.
WINDOW = os.environ.get("WINDOW", "30d")   # set to 90d or 1y for a one-off backfill

GLOBAL_QUERIES = [
    '"to acquire"', '"agrees to acquire"', '"agrees to buy"', '"has acquired"',
    '"merger agreement"', '"completes acquisition"', '"completes the acquisition"',
    '"closes acquisition"', '"acquisition of" billion', '"to buy" billion deal',
    '"take-private" OR "take private"', '"all-cash deal"', '"all-stock deal"',
    '"definitive agreement" acquire', 'private equity acquires',
    '"acquires majority stake"', '"buys stake in"', '"to merge with"',
    '"carve-out" OR "divestiture" sale', '"terminates merger" OR "scraps acquisition"',
]

INDIA_QUERIES = [
    'acquisition crore', '"to acquire" crore', '"acquires" stake crore',
    'CCI approves acquisition', 'CCI approves merger', 'NCLT approves merger',
    '"open offer" acquisition SEBI', 'India acquisition deal', 'Indian startup acquired',
    '"to acquire" India company', 'merger Indian company crore',
]

# (label, hl, gl, ceid, queries)
NEWS_EDITIONS = [
    ("US", "en-US", "US", "US:en", GLOBAL_QUERIES),
    ("IN", "en-IN", "IN", "IN:en", GLOBAL_QUERIES + INDIA_QUERIES),
    ("GB", "en-GB", "GB", "GB:en", GLOBAL_QUERIES[:10]),
]

# Regulator feeds: authoritative, structured, and free.
REGULATOR_FEEDS = [
    ("UK CMA", "https://www.gov.uk/cma-cases.atom?case_type%5B%5D=mergers"),
]

SEC_FULLTEXT = "https://efts.sec.gov/LATEST/search-index"
SEC_QUERIES = ['"merger agreement"', '"agreement and plan of merger"']


def fetch(url, headers=None, retries=3):
    req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except Exception as e:
            if attempt == retries - 1:
                print(f"  ! failed {url}: {e}", file=sys.stderr)
                return None
            time.sleep(2 ** attempt)
    return None


def parse_feed(xml_bytes):
    """Parse RSS or Atom into a list of {title, link, date, source}."""
    items = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return items

    ns = {"atom": "http://www.w3.org/2005/Atom"}

    for it in root.iter():
        tag = it.tag.split("}")[-1]
        if tag not in ("item", "entry"):
            continue

        def txt(*names):
            for n in names:
                el = it.find(n) if "}" not in n else it.find(n, ns)
                if el is None:
                    el = it.find(f"{{http://www.w3.org/2005/Atom}}{n}")
                if el is not None and el.text:
                    return el.text.strip()
            return ""

        title = txt("title")
        if not title:
            continue

        link = txt("link")
        if not link:
            le = it.find("{http://www.w3.org/2005/Atom}link")
            if le is not None:
                link = le.get("href", "")

        date_raw = txt("pubDate", "updated", "published")
        source = txt("source") or ""

        src_el = it.find("source")
        publisher_url = src_el.get("url", "") if src_el is not None else ""

        items.append({
            "title": _unescape(title),
            "link": link,
            "date": normalise_date(date_raw),
            "source": source,
            "publisherUrl": publisher_url,
        })
    return items


def _unescape(s):
    return (s.replace("&amp;", "&").replace("&lt;", "<")
             .replace("&gt;", ">").replace("&quot;", '"')
             .replace("&#39;", "'").replace("&nbsp;", " ")).strip()


def normalise_date(raw):
    if not raw:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw = raw.strip()
    formats = [
        "%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d",
    ]
    for f in formats:
        try:
            return datetime.strptime(raw, f).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return m.group(0)
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def pull_google_news():
    out, seen = [], set()
    for label, hl, gl, ceid, queries in NEWS_EDITIONS:
        print(f"  google news [{label}]: {len(queries)} queries")
        for q in queries:
            url = ("https://news.google.com/rss/search?q="
                   + urllib.parse.quote(f"{q} when:{WINDOW}")
                   + f"&hl={hl}&gl={gl}&ceid={ceid}")
            body = fetch(url)
            if not body:
                continue
            for item in parse_feed(body):
                sig = (item["title"] or "").lower()[:90]
                if sig in seen:
                    continue
                seen.add(sig)
                item["feed"] = "Google News"
                item["edition"] = label
                out.append(item)
            time.sleep(0.6)
    return out


def pull_regulators():
    out = []
    for name, url in REGULATOR_FEEDS:
        print(f"  regulator: {name}")
        body = fetch(url)
        if not body:
            continue
        for item in parse_feed(body):
            item["feed"] = name
            item["source"] = name
            out.append(item)
        time.sleep(1)
    return out


def pull_sec():
    """SEC EDGAR full-text search. Structured, authoritative, US-only."""
    out = []
    for q in SEC_QUERIES:
        params = urllib.parse.urlencode({"q": q, "forms": "8-K", "dateRange": "custom"})
        print(f"  sec edgar: {q}")
        body = fetch(f"{SEC_FULLTEXT}?{params}",
                     headers={"Accept": "application/json"})
        if not body:
            continue
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            continue
        for hit in data.get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            names = src.get("display_names") or []
            if not names:
                continue
            adsh = (hit.get("_id", "").split(":")[0] or "").replace("-", "")
            cik = (src.get("ciks") or [""])[0].lstrip("0")
            out.append({
                "title": names[0],
                "link": (f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh}/"
                         if cik and adsh else "https://www.sec.gov/edgar/search/"),
                "date": normalise_date(src.get("file_date", "")),
                "source": "SEC EDGAR",
                "feed": "SEC EDGAR",
                "sec_filer": names[0],
            })
        time.sleep(1)
    return out


# --------------------------------------------------------------------------
# 2. Extraction
# --------------------------------------------------------------------------

RUMOUR = re.compile(
    r"\b(in talks|exploring|explores|considering|considers|weighs|mulls|"
    r"reportedly|rumou?r|could acquire|may acquire|might acquire|eyeing|"
    r"eyes |approach(?:es|ed)? |bid interest|takeover interest)\b", re.I)

VERBS = r"(?:to acquire|acquires|acquired|to buy|buys|to purchase|purchases|" \
        r"agrees to acquire|agrees to buy|has agreed to acquire|will acquire|" \
        r"completes acquisition of|completes purchase of|closes acquisition of|" \
        r"to take over|takes over|snaps up|to merge with|merges with|" \
        r"completes merger with|completes its merger with|" \
        r"(?:scraps|abandons|terminates|calls off|ends|drops)\s+" \
        r"(?:its\s+|the\s+|planned\s+|proposed\s+|\$?[\d.]+\s?\w*\s+)*" \
        r"(?:acquisition|takeover|merger|purchase|bid)\s+(?:of|for|with))"

PATTERNS = [
    re.compile(rf"^(?P<a>.{{2,60}}?)\s+{VERBS}\s+(?P<t>.{{2,60}}?)"
               rf"(?:\s+(?:for|in a|in an|at)\s+(?P<v>.{{2,30}}?))?"
               rf"(?:\s*[-–—|,]\s*.*)?$", re.I),
    re.compile(r"^(?P<t>.{2,60}?)\s+(?:to be acquired by|acquired by|"
               r"agrees to be acquired by|agrees to sell itself to)\s+"
               r"(?P<a>.{2,60}?)(?:\s+(?:for|in)\s+(?P<v>.{2,30}?))?"
               r"(?:\s*[-–—|,]\s*.*)?$", re.I),
    # "CCI approves Zomato acquisition of Blinkit" / "EU clears Microsoft takeover of X"
    re.compile(r"^(?:cci|sebi|nclt|rbi|eu|ec|cma|ftc|doj|regulators?|"
               r"[\w .&']{2,28}?)\s+(?:approves|approved|clears|cleared|greenlights)\s+"
               r"(?P<a>.{2,50}?)(?:'s)?\s+"
               r"(?:proposed\s+)?(?:acquisition|takeover|purchase|buyout)\s+of\s+"
               r"(?P<t>.{2,60}?)(?:\s+(?:for|in|at)\s+(?P<v>.{2,30}))?"
               r"(?:\s*[-–—|]\s*.*)?$", re.I),
]

VALUE = re.compile(
    r"(?P<cur>US\$|\$|€|£|₹|Rs\.?|INR|EUR|GBP)\s?(?P<num>[\d,]+(?:\.\d+)?)\s*"
    r"(?P<mag>trillion|billion|million|crore|lakh|tn|bn|mn|b\b|m\b)?", re.I)

FX = {"$": 1.0, "us$": 1.0, "€": 1.08, "eur": 1.08, "£": 1.27, "gbp": 1.27,
      "₹": 0.012, "rs": 0.012, "rs.": 0.012, "inr": 0.012}

MAG = {"trillion": 1_000_000, "tn": 1_000_000, "billion": 1000, "bn": 1000,
       "b": 1000, "million": 1, "mn": 1, "m": 1, "crore": 10, "lakh": 0.1}

INDUSTRIES = {
    "Technology & Software": ["software", "saas", "cloud", "cyber", "ai ", "data platform",
                              "app", "digital", "tech", "platform", "it services"],
    "Semiconductors": ["semiconductor", "chip", "chipmaker", "foundry", "wafer", "silicon"],
    "Healthcare & Pharmaceuticals": ["pharma", "biotech", "drug", "therapeutic", "hospital",
                                     "medical", "health", "clinical", "vaccine", "diagnostic"],
    "Financial Services": ["bank", "fintech", "lender", "asset manager", "brokerage",
                           "payments", "nbfc", "wealth", "exchange"],
    "Insurance": ["insurer", "insurance", "reinsur", "underwrit"],
    "Energy & Utilities": ["oil", "gas", "energy", "solar", "wind", "renewable",
                           "utility", "power", "pipeline", "drilling"],
    "Industrials & Manufacturing": ["manufactur", "industrial", "machinery", "factory",
                                    "equipment", "engineering"],
    "Consumer & Retail": ["retail", "consumer", "brand", "food", "beverage", "restaurant",
                          "apparel", "grocery", "e-commerce"],
    "Media & Telecom": ["media", "telecom", "broadcast", "studio", "publisher",
                        "streaming", "wireless", "advertis"],
    "Real Estate": ["real estate", "reit", "property", "developer", "warehouse"],
    "Transport & Logistics": ["logistics", "shipping", "airline", "freight", "rail",
                              "trucking", "port"],
    "Materials & Chemicals": ["chemical", "mining", "steel", "cement", "materials", "metals"],
    "Aerospace & Defence": ["aerospace", "defence", "defense", "satellite", "aviation"],
}

# Fallback when the headline names no sector — extend this list freely, it is
# just a lookup on the party names.
COMPANY_SECTORS = {
    "Technology & Software": ["vmware", "salesforce", "oracle", "adobe", "splunk", "hubspot",
                              "workday", "atlassian", "zoom", "dropbox", "shopify", "sap",
                              "microsoft", "google", "alphabet", "meta", "ibm", "servicenow",
                              "infosys", "tcs", "wipro", "hcl", "cognizant", "accenture"],
    "Semiconductors": ["broadcom", "nvidia", "intel", "amd", "qualcomm", "arm ", "tsmc",
                       "micron", "analog devices", "marvell", "nxp", "infineon", "asml"],
    "Healthcare & Pharmaceuticals": ["pfizer", "merck", "seagen", "moderna", "astrazeneca",
                                     "novartis", "roche", "gsk", "sanofi", "amgen", "abbvie",
                                     "biogen", "eli lilly", "bristol myers", "cipla",
                                     "sun pharma", "dr reddy", "medtronic", "baxter"],
    "Financial Services": ["jpmorgan", "goldman", "morgan stanley", "citigroup", "hsbc",
                           "barclays", "blackrock", "blackstone", "kkr", "carlyle",
                           "apollo global", "hdfc", "icici", "axis bank", "visa",
                           "mastercard", "paypal", "stripe"],
    "Energy & Utilities": ["exxon", "chevron", "shell", "bp ", "totalenergies", "conocophillips",
                           "pioneer natural", "hess", "occidental", "nextera", "adani green",
                           "ntpc", "reliance industries"],
    "Consumer & Retail": ["walmart", "target", "kroger", "unilever", "nestle", "pepsico",
                          "coca-cola", "mondelez", "kraft", "nike", "starbucks", "mcdonald",
                          "reliance retail", "metro cash"],
    "Media & Telecom": ["disney", "warner", "paramount", "netflix", "comcast", "verizon",
                        "at&t", "t-mobile", "vodafone", "airtel", "jio", "endeavor", "wpp"],
    "Aerospace & Defence": ["boeing", "airbus", "lockheed", "raytheon", "rtx", "northrop",
                            "general dynamics", "bae systems", "spirit aero"],
}

REGIONS = {
    "United Kingdom": ["uk ", " uk", "britain", "british", "london", "ftse", "plc"],
    "Europe": ["germany", "german", "france", "french", "european", "eu ",
               "netherlands", "dutch", "spain", "spanish", "italy", "italian",
               "nordic", "swiss", "switzerland", "sweden", "denmark", "norway"],
    "Asia-Pacific": ["china", "chinese", "japan", "japanese", "korea", "korean",
                     "singapore", "australia", "australian", "hong kong", "taiwan",
                     "indonesia", "malaysia", "vietnam", "thailand", "new zealand"],
    "Middle East & Africa": ["saudi", "uae", "dubai", "abu dhabi", "qatar", "kuwait",
                             "israel", "israeli", "africa", "african", "nigeria",
                             "egypt", "south africa"],
    "Latin America": ["brazil", "brazilian", "mexico", "mexican", "chile",
                      "colombia", "argentina", "latin america", "peru"],
    "North America": ["us ", " u.s.", "american", "wall street", "nasdaq", "nyse",
                      "canada", "canadian", "toronto", "silicon valley", "sec filing"],
}

# --- India signals -------------------------------------------------------
# India was previously only detected when a headline literally said "india",
# which almost never happens. These three independent signals fix that.

INDIA_TERMS = [
    "india", "indian", "mumbai", "bengaluru", "bangalore", "delhi", "chennai",
    "hyderabad", "kolkata", "pune", "gurugram", "gurgaon", "noida", "ahmedabad",
    "crore", "lakh", "rupee", "₹", " sebi", " cci ", "nclt", "nse ", "bse ",
    "reserve bank of india", "dalal street", "nifty", "sensex", "irdai",
    "competition commission of india", "open offer",
]

INDIA_DOMAINS = [
    "economictimes.indiatimes.com", "indiatimes.com", "livemint.com", "mint.com",
    "business-standard.com", "moneycontrol.com", "thehindubusinessline.com",
    "thehindu.com", "financialexpress.com", "businesstoday.in", "ndtvprofit.com",
    "cnbctv18.com", "zeebiz.com", "bqprime.com", "vccircle.com", "inc42.com",
    "entrackr.com", "yourstory.com", "medianama.com", "hindustantimes.com",
    "timesofindia.indiatimes.com", "deccanherald.com", "thewire.in", "scroll.in",
]

INDIA_COMPANIES = [
    "reliance", "tata ", "adani", "birla", "mahindra", "bajaj", "godrej",
    "wipro", "infosys", "hcl", "tcs", "hdfc", "icici", "axis bank", "kotak",
    "sbi ", "state bank of india", "yes bank", "idfc", "bandhan",
    "jio", "airtel", "bharti", "vodafone idea", "zomato", "swiggy", "paytm",
    "flipkart", "ola ", "oyo", "byju", "phonepe", "nykaa", "policybazaar",
    "dr reddy", "cipla", "sun pharma", "lupin", "torrent pharma", "zydus",
    "biocon", "glenmark", "aurobindo", "divis", "piramal",
    "l&t", "larsen & toubro", "ultratech", "ambuja", "acc ", "jsw", "vedanta",
    "hindalco", "sail ", "ntpc", "ongc", "indian oil", "bpcl", "hpcl",
    "asian paints", "britannia", "dabur", "marico", "itc ", "hul ",
    "hindustan unilever", "titan", "dmart", "avenue supermarts", "jubilant",
]

DOMAIN_REGIONS = {
    "United Kingdom": ["bbc.co.uk", "theguardian.com", "telegraph.co.uk", "ft.com",
                       "thetimes.co.uk", "cityam.com", "sky.com"],
    "Europe": ["handelsblatt.com", "lesechos.fr", "elpais.com", "dw.com",
               "euronews.com", "politico.eu"],
    "Asia-Pacific": ["scmp.com", "nikkei.com", "japantimes.co.jp", "straitstimes.com",
                     "afr.com", "koreaherald.com", "channelnewsasia.com"],
    "Middle East & Africa": ["thenationalnews.com", "arabnews.com", "gulfnews.com",
                             "businesslive.co.za"],
    "Latin America": ["valor.globo.com", "eluniversal.com.mx", "batimes.com.ar"],
    "North America": ["wsj.com", "bloomberg.com", "cnbc.com", "nytimes.com",
                      "prnewswire.com", "businesswire.com", "globenewswire.com",
                      "sec.gov", "axios.com", "forbes.com", "barrons.com"],
}


def _urls_of(item):
    return " ".join(filter(None, [
        (item or {}).get("link", ""),
        (item or {}).get("publisherUrl", ""),
        (item or {}).get("source", ""),
    ])).lower()


def detect_region(title, item=None, parties=""):
    """India first, then domain of the publisher, then headline keywords."""
    hay = f" {title.lower()} {parties.lower()} "
    urls = _urls_of(item)

    if any(t in hay for t in INDIA_TERMS):
        return "India"
    if any(d in urls for d in INDIA_DOMAINS):
        return "India"
    if any(c in hay for c in INDIA_COMPANIES):
        return "India"
    if (item or {}).get("edition") == "IN" and "crore" in hay:
        return "India"

    for label, domains in DOMAIN_REGIONS.items():
        if any(d in urls for d in domains):
            return label

    for label, keys in REGIONS.items():
        if any(k in hay for k in keys):
            return label

    return "Unspecified"

NOISE = re.compile(r"^(exclusive|update \d|breaking|analysis|opinion|report)[:\-–—]\s*", re.I)
TRAIL = re.compile(r"\s*[-–—|]\s*[A-Z][\w .&']{2,30}$")  # " - Reuters"


TAIL_VALUE = re.compile(
    r"\s+(?:in|for|at|worth|valued at)\s+(?:an?\s+)?[$€£₹]?[\d.,].*$", re.I)
TAIL_WORDS = re.compile(
    r"\s+(?:deal|transaction|all-cash deal|cash deal|stock deal)$", re.I)


def clean_party(s):
    s = _unescape(s or "").strip(" ,.:;\"'“”")
    s = TAIL_VALUE.sub("", s)
    s = TAIL_WORDS.sub("", s)
    s = re.sub(r"^the\s+", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def parse_value(raw):
    """Return (display string, USD millions) or (None, None)."""
    if not raw:
        return None, None
    m = VALUE.search(raw)
    if not m:
        return None, None
    cur = (m.group("cur") or "$").lower()
    try:
        num = float(m.group("num").replace(",", ""))
    except ValueError:
        return None, None
    mag = (m.group("mag") or "").lower().strip()
    mult = MAG.get(mag, 1 if num > 1000 else 1000)  # bare "$61" almost always billions
    usd_m = num * mult * FX.get(cur, 1.0)
    if usd_m < 1 or usd_m > 5_000_000:
        return None, None
    if usd_m >= 1000:
        disp = f"${usd_m/1000:.1f}B"
    else:
        disp = f"${usd_m:.0f}M"
    return disp, round(usd_m, 1)


def detect_status(title):
    t = title.lower()
    if re.search(r"\b(completes|completed|closes|closed|finalis|finaliz)\b", t):
        return "Completed"
    if re.search(r"\b(terminat|scraps|scrapped|abandons|calls off|walks away|"
                 r"collapses|drops bid)\b", t):
        return "Terminated"
    if re.search(r"\b(approv\w*|clear(?:s|ed)|greenlight\w*|regulatory nod)\b", t):
        return "Pending"
    return "Announced"


def detect_type(title):
    t = title.lower()
    if "merge" in t:
        return "Merger"
    if "take-private" in t or "take private" in t:
        return "Take-private / LBO"
    if "joint venture" in t:
        return "Joint venture"
    if "stake" in t:
        return "Minority stake" if re.search(r"\b([1-4]?\d)%", t) else "Majority stake"
    if "divest" in t or "carve-out" in t or "carve out" in t or "unit" in t:
        return "Divestiture / carve-out"
    if "assets" in t:
        return "Asset purchase"
    return "Acquisition"


def classify(text, table, default=""):
    t = " " + text.lower() + " "
    for label, keys in table.items():
        if any(k in t for k in keys):
            return label
    return default


def extract(item):
    title = NOISE.sub("", _unescape(item["title"]))
    title = TRAIL.sub("", title).strip()

    if RUMOUR.search(title):
        return None

    for pat in PATTERNS:
        m = pat.match(title)
        if not m:
            continue
        acquirer = clean_party(m.group("a"))
        target = clean_party(m.group("t"))
        if not acquirer or not target:
            continue
        if len(acquirer) < 2 or len(target) < 2:
            continue
        if acquirer.lower() == target.lower():
            continue
        if len(acquirer.split()) > 8 or len(target.split()) > 8:
            continue

        # Parse from the whole headline: the capture group stops early on the
        # thousands comma in figures like "Rs 2,850 crore".
        disp, usd_m = parse_value(title)
        return {
            "acquirer": acquirer,
            "target": target,
            "dealValue": disp,
            "valueUsdM": usd_m,
            "announcedDate": item["date"],
            "status": detect_status(title),
            "dealType": detect_type(title),
            "industry": (classify(title, INDUSTRIES)
                         or classify(f"{acquirer} {target}", COMPANY_SECTORS, "Other")),
            "region": detect_region(title, item, f"{acquirer} {target}"),
            "headline": title,
            "sourceName": item.get("source") or item.get("feed") or "News",
            "sourceUrl": item.get("link", ""),
            "feed": item.get("feed", ""),
        }
    return None


# --------------------------------------------------------------------------
# 3. Merge and write
# --------------------------------------------------------------------------

def key_of(d):
    norm = lambda s: re.sub(r"[^a-z0-9]", "", (s or "").lower())[:22]
    return f"{norm(d['acquirer'])}|{norm(d['target'])}"


STATUS_RANK = {"Announced": 0, "Pending": 1, "Completed": 2, "Terminated": 3}


def merge(existing, fresh):
    by_key = {key_of(d): d for d in existing if d.get("acquirer") and d.get("target")}

    for d in fresh:
        k = key_of(d)
        if k not in by_key:
            by_key[k] = d
            continue
        old = by_key[k]
        # keep the earliest announcement date, the latest status, the best value
        if d["announcedDate"] < old["announcedDate"]:
            old["announcedDate"] = d["announcedDate"]
        if STATUS_RANK.get(d["status"], 0) > STATUS_RANK.get(old["status"], 0):
            old["status"] = d["status"]
            old["sourceUrl"] = d["sourceUrl"]
            old["sourceName"] = d["sourceName"]
        if old.get("valueUsdM") is None and d.get("valueUsdM") is not None:
            old["valueUsdM"] = d["valueUsdM"]
            old["dealValue"] = d["dealValue"]
        if old.get("industry") in ("", "Other") and d.get("industry") not in ("", "Other"):
            old["industry"] = d["industry"]

    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    kept = [d for d in by_key.values() if d.get("announcedDate", "") >= cutoff]
    kept.sort(key=lambda d: d.get("announcedDate", ""), reverse=True)
    return kept


def main():
    print("Pulling sources...")
    items = []
    items += pull_google_news()
    items += pull_regulators()
    try:
        items += pull_sec()
    except Exception as e:
        print(f"  ! sec step skipped: {e}", file=sys.stderr)

    print(f"Fetched {len(items)} headlines.")

    fresh = []
    for it in items:
        try:
            d = extract(it)
        except Exception:
            d = None
        if d:
            fresh.append(d)
    print(f"Extracted {len(fresh)} deals.")

    existing = []
    if os.path.exists(OUT_FILE):
        try:
            with open(OUT_FILE) as f:
                existing = json.load(f).get("deals", [])
        except Exception:
            existing = []

    merged = merge(existing, fresh)
    added = len(merged) - len(existing)

    payload = {
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dealCount": len(merged),
        "sources": ["Google News RSS"] + [n for n, _ in REGULATOR_FEEDS] + ["SEC EDGAR"],
        "deals": merged,
    }
    with open(OUT_FILE, "w") as f:
        json.dump(payload, f, indent=1, ensure_ascii=False)

    print(f"Wrote {OUT_FILE}: {len(merged)} deals ({added:+d} this run).")


if __name__ == "__main__":
    main()
