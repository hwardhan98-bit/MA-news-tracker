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
from collections import Counter
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

# --- Region --------------------------------------------------------------
# Region describes WHERE THE DEAL IS, not who reported it. An Economic Times
# story about Broadcom/VMware is a North America deal, not an Indian one, so
# the publisher's domain is deliberately NOT used as a signal here.
#
# Convention: region follows the TARGET's home market where known, because
# that is what deal league tables segment on. If the target is unknown we fall
# back to jurisdiction signals in the headline (currency, regulator, country
# adjectives), then to the acquirer, then to "Unspecified".

COUNTRY_COMPANIES = {
    "India": [
        "reliance", "reliance industries", "reliance retail", "tata", "tata sons",
        "tata motors", "tata steel", "tata consultancy", "adani", "aditya birla",
        "birla", "mahindra", "bajaj", "godrej", "wipro", "infosys", "hcl",
        "hcltech", "tcs", "tech mahindra", "hdfc", "hdfc bank", "icici",
        "icici bank", "axis bank", "kotak", "kotak mahindra", "sbi",
        "state bank of india", "yes bank", "idfc", "bandhan", "indusind",
        "jio", "reliance jio", "airtel", "bharti", "bharti airtel",
        "vodafone idea", "zomato", "swiggy", "paytm", "one97", "flipkart",
        "myntra", "ola", "ola electric", "oyo", "byju", "byjus", "blinkit",
        "phonepe", "nykaa", "policybazaar", "lenskart", "meesho", "razorpay",
        "cred", "zepto", "urban company", "delhivery", "pine labs",
        "dr reddy", "cipla", "sun pharma", "lupin", "torrent pharma", "zydus",
        "biocon", "glenmark", "aurobindo", "divis", "piramal", "mankind pharma",
        "apollo hospitals", "fortis healthcare", "max healthcare",
        "larsen & toubro", "ultratech", "ambuja cements", "acc cements",
        "shree cement", "jsw", "jsw steel", "vedanta", "hindalco", "sail",
        "ntpc", "ongc", "indian oil", "bpcl", "hpcl", "gail", "power grid",
        "asian paints", "britannia", "dabur", "marico", "godrej consumer",
        "hindustan unilever", "titan", "dmart", "avenue supermarts",
        "jubilant foodworks", "varun beverages", "havells", "voltas",
        "maruti suzuki", "hero motocorp", "tvs motor", "eicher motors",
        "air india", "indigo", "interglobe aviation", "spicejet", "vistara",
        "zee entertainment", "sony pictures networks india", "pvr", "inox",
    ],
    "North America": [
        "microsoft", "google", "alphabet", "amazon", "apple", "meta", "facebook",
        "nvidia", "intel", "amd", "qualcomm", "broadcom", "cisco", "oracle",
        "ibm", "salesforce", "adobe", "vmware", "splunk", "servicenow",
        "workday", "snowflake", "datadog", "hubspot", "zoom", "dropbox",
        "activision", "activision blizzard", "electronic arts", "roblox",
        "twitter", "x corp", "uber", "lyft", "doordash", "airbnb", "stripe",
        "paypal", "visa", "mastercard", "block", "square", "coinbase",
        "jpmorgan", "goldman sachs", "morgan stanley", "citigroup", "wells fargo",
        "bank of america", "blackrock", "blackstone", "kkr", "carlyle",
        "apollo global", "tpg", "silver lake", "thoma bravo", "vista equity",
        "berkshire hathaway", "charles schwab", "nasdaq", "adenza",
        "pfizer", "merck", "moderna", "johnson & johnson", "abbvie", "amgen",
        "bristol myers", "eli lilly", "gilead", "biogen", "seagen", "medtronic",
        "exxon", "exxon mobil", "chevron", "conocophillips", "occidental",
        "pioneer natural resources", "hess", "marathon oil", "devon energy",
        "nextera", "duke energy", "us steel", "united states steel",
        "walmart", "target", "kroger", "costco", "home depot", "nike",
        "starbucks", "mcdonald", "pepsico", "coca-cola", "mondelez", "kraft",
        "disney", "warner bros", "paramount", "netflix", "comcast", "nbcuniversal",
        "verizon", "at&t", "t-mobile", "charter communications",
        "boeing", "lockheed martin", "raytheon", "rtx", "northrop grumman",
        "general dynamics", "spirit aerosystems", "general electric", "honeywell",
        "3m", "caterpillar", "deere", "ford", "general motors", "tesla",
        "irobot", "shopify", "brookfield", "onex", "cgi",
    ],
    "United Kingdom": [
        "hsbc", "barclays", "natwest", "lloyds banking", "standard chartered",
        "aviva", "legal & general", "prudential plc", "m&g", "schroders",
        "bp", "shell", "rio tinto", "anglo american", "glencore", "bhp",
        "unilever", "diageo", "reckitt", "tesco", "tesco bank", "sainsbury",
        "asda", "morrisons", "marks & spencer", "ocado", "b&q", "kingfisher",
        "vodafone group", "bt group", "sky", "itv", "wpp", "pearson",
        "rolls-royce", "bae systems", "gsk", "glaxosmithkline", "astrazeneca",
        "smith & nephew", "arm holdings", "sage group", "darktrace",
        "national grid", "sse", "centrica", "easyjet", "iag", "whitbread",
    ],
    "Europe": [
        "sap", "siemens", "bosch", "volkswagen", "bmw", "mercedes-benz", "daimler",
        "porsche", "continental ag", "bayer", "basf", "merck kgaa", "allianz",
        "deutsche bank", "commerzbank", "deutsche telekom", "eon", "rwe",
        "infineon", "zalando", "delivery hero", "n26", "adidas", "puma",
        "lvmh", "kering", "hermes", "loreal", "danone", "carrefour", "totalenergies",
        "airbus", "thales", "safran", "schneider electric", "capgemini", "atos",
        "bnp paribas", "societe generale", "axa", "credit agricole", "sanofi",
        "asml", "philips", "ing group", "rabobank", "heineken", "ahold delhaize",
        "shell plc", "stellantis", "ferrari", "enel", "eni", "intesa sanpaolo",
        "unicredit", "generali", "telefonica", "santander", "bbva", "iberdrola",
        "inditex", "repsol", "nestle", "novartis", "roche", "ubs", "credit suisse",
        "zurich insurance", "abb", "nokia", "ericsson", "spotify", "klarna",
        "volvo", "ikea", "maersk", "novo nordisk", "carlsberg", "equinor",
    ],
    "Asia-Pacific": [
        "alibaba", "tencent", "baidu", "bytedance", "jd.com", "meituan", "didi",
        "xiaomi", "huawei", "byd", "catl", "smic", "china mobile", "icbc",
        "toyota", "honda", "nissan", "sony", "panasonic", "hitachi", "toshiba",
        "softbank", "nintendo", "rakuten", "nippon steel", "mitsubishi",
        "mitsui", "sumitomo", "mizuho", "nomura", "fast retailing", "shiseido",
        "samsung", "sk hynix", "lg electronics", "hyundai", "kia", "naver",
        "kakao", "coupang", "tsmc", "foxconn", "mediatek", "asus", "acer",
        "grab", "sea limited", "shopee", "dbs", "ocbc", "singtel", "temasek",
        "gic", "capitaland", "keppel", "bhp group", "woodside", "telstra",
        "commonwealth bank", "westpac", "anz", "macquarie", "qantas", "wesfarmers",
    ],
    "Middle East & Africa": [
        "aramco", "saudi aramco", "sabic", "pif", "public investment fund",
        "mubadala", "adnoc", "adia", "emaar", "emirates", "etisalat", "e&",
        "dp world", "qatar investment authority", "qatar national bank",
        "teva", "check point", "wiz", "mobileye", "nice ltd", "cyberark",
        "naspers", "prosus", "mtn group", "sasol", "standard bank",
        "safaricom", "dangote", "attijariwafa",
    ],
    "Latin America": [
        "petrobras", "vale", "itau", "bradesco", "ambev", "jbs", "nubank",
        "mercadolibre", "b3", "braskem", "gerdau", "natura", "magazine luiza",
        "america movil", "grupo bimbo", "cemex", "femsa", "grupo mexico",
        "falabella", "lan", "latam airlines", "ecopetrol", "bancolombia",
    ],
}

# Signals that describe the DEAL's jurisdiction. Currency and regulator
# mentions are safe because they refer to the transaction, not the outlet.
JURISDICTION_SIGNALS = {
    "India": ["crore", "lakh", "rupee", "rupees", "\u20b9", "sebi", "nclt", "irdai",
              "competition commission of india", "reserve bank of india",
              "india", "indian", "mumbai", "bengaluru", "bangalore", "new delhi",
              "chennai", "hyderabad", "kolkata", "gurugram", "noida", "dalal street"],
    "United Kingdom": ["\u00a3", "pence", "takeover panel", "competition and markets authority",
                       "britain", "british", "uk-based", "uk based", "london-listed",
                       "london-based", "ftse", "scotland", "wales"],
    "Europe": ["\u20ac", "european commission", "brussels", "eurozone", "germany", "german",
               "france", "french", "netherlands", "dutch", "spain", "spanish",
               "italy", "italian", "sweden", "swedish", "denmark", "danish",
               "norway", "norwegian", "switzerland", "swiss", "belgium", "austria",
               "poland", "portugal", "finland", "ireland", "irish"],
    "Asia-Pacific": ["china", "chinese", "beijing", "shanghai", "shenzhen", "hong kong",
                     "japan", "japanese", "tokyo", "yen", "south korea", "korean",
                     "seoul", "singapore", "taiwan", "taipei", "australia",
                     "australian", "sydney", "melbourne", "new zealand", "indonesia",
                     "malaysia", "vietnam", "thailand", "philippines"],
    "Middle East & Africa": ["saudi", "riyadh", "uae", "dubai", "abu dhabi", "qatar",
                             "doha", "kuwait", "bahrain", "oman", "israel", "israeli",
                             "tel aviv", "egypt", "nigeria", "kenya", "south africa",
                             "johannesburg", "morocco", "africa", "african"],
    "Latin America": ["brazil", "brazilian", "sao paulo", "mexico", "mexican",
                      "chile", "chilean", "colombia", "argentina", "peru",
                      "latin america", "latam", "real", "peso"],
    "North America": ["united states", "u.s.", "us-based", "us based", "american",
                      "wall street", "nasdaq-listed", "nyse-listed", "new york",
                      "california", "texas", "silicon valley", "canada", "canadian",
                      "toronto", "ontario", "sec filing", "ftc", "doj"],
}


def _compile_lexicon(table):
    out = {}
    for label, names in table.items():
        alts = sorted((re.escape(n) for n in names), key=len, reverse=True)
        out[label] = re.compile(r"(?<![a-z0-9])(?:" + "|".join(alts) + r")(?![a-z0-9])", re.I)
    return out


_COMPANY_RX = _compile_lexicon(COUNTRY_COMPANIES)
_JURIS_RX = _compile_lexicon(JURISDICTION_SIGNALS)


def company_region(name):
    if not name:
        return None
    n = name.lower()
    for label, rx in _COMPANY_RX.items():
        if rx.search(n):
            return label
    return None


def jurisdiction_region(text):
    t = (text or "").lower()
    hits = [(label, len(rx.findall(t))) for label, rx in _JURIS_RX.items()]
    hits = [h for h in hits if h[1] > 0]
    if not hits:
        return None
    hits.sort(key=lambda h: -h[1])
    return hits[0][0]


def detect_region(title, item=None, acquirer="", target=""):
    """Where the deal is, in priority order. The publisher is never a signal."""
    return (company_region(target)
            or jurisdiction_region(title)
            or company_region(acquirer)
            or "Unspecified")


NOISE = re.compile(r"^(exclusive|update \d|breaking|analysis|opinion|report)[:\-–—]\s*", re.I)
TRAIL = re.compile(r"\s*[-–—|]\s*[A-Z][\w .&']{2,30}$")  # " - Reuters"


TAIL_VALUE = re.compile(
    r"\s+(?:in|for|at|worth|valued at)\s+(?:an?\s+)?[$€£₹]?[\d.,].*$", re.I)
TAIL_WORDS = re.compile(
    r"\s+(?:deal|transaction|all-cash deal|cash deal|stock deal)$", re.I)
TAIL_FROM = re.compile(
    r"\s+from\s+(?:the\s+)?[\w .&'-]{2,40}$", re.I)


# A trade word is always garnish ("chipmaker Broadcom"). A country word is
# only garnish when possessive ("Japan's Nippon Steel") or when it modifies a
# trade word ("US chipmaker Broadcom") — otherwise it belongs to the name,
# as in "US Steel" or "Air India".
TRADE_WORDS = {
    "chipmaker", "drugmaker", "carmaker", "automaker", "steelmaker", "lender",
    "insurer", "retailer", "miner", "brewer", "telco", "conglomerate",
    "startup", "unicorn", "fintech", "biotech", "giant", "major", "firm",
    "billionaire", "tycoon", "group", "maker", "producer", "operator",
}
COUNTRY_WORDS = {
    "us", "usa", "american", "uk", "british", "indian", "india", "chinese",
    "china", "japanese", "japan", "german", "germany", "french", "france",
    "dutch", "swiss", "korean", "saudi", "canadian", "australian", "european",
    "singapore", "israeli", "brazilian", "mexican", "russian", "spanish",
}


def _strip_descriptors(s):
    """Drop press garnish while protecting names like 'US Steel'."""
    tokens = s.split()
    while tokens:
        raw = tokens[0].lower().strip(".,-")
        possessive = bool(re.search(r"['\u2019]s$", raw))
        word = re.sub(r"['\u2019]s$", "", raw)
        nxt = re.sub(r"['\u2019]s$", "", tokens[1].lower().strip(".,-")) if len(tokens) > 1 else ""

        if word in TRADE_WORDS:
            tokens.pop(0)
        elif word in COUNTRY_WORDS and (possessive or nxt in TRADE_WORDS):
            tokens.pop(0)
        else:
            break
    return " ".join(tokens)


def clean_party(s):
    s = _unescape(s or "").strip(" ,.:;\"'“”")
    s = _strip_descriptors(s)
    s = TAIL_VALUE.sub("", s)
    s = TAIL_FROM.sub("", s)
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
            "region": detect_region(title, item, acquirer, target),
            "headline": title,
            "sourceName": item.get("source") or item.get("feed") or "News",
            "sourceUrl": item.get("link", ""),
            "feed": item.get("feed", ""),
        }
    return None


# --------------------------------------------------------------------------
# 3. Merge and write
# --------------------------------------------------------------------------

LEGAL_SUFFIXES = {
    "inc", "incorporated", "corp", "corporation", "co", "company", "ltd",
    "limited", "llc", "llp", "lp", "plc", "pvt", "private", "group", "holding",
    "holdings", "international", "intl", "sa", "nv", "ag", "gmbh", "ab", "as",
    "oyj", "spa", "bv", "kk", "pte", "pty", "berhad", "bhd", "sas", "se",
    "technologies", "technology", "solutions", "systems", "enterprises",
    "industries", "ventures", "partners", "capital", "the",
}

# Words the press bolts onto a company name. These vary story to story and are
# the main reason the same deal used to be stored several times over.
DESCRIPTORS = {
    "chipmaker", "drugmaker", "carmaker", "automaker", "steelmaker", "lender",
    "insurer", "retailer", "miner", "brewer", "airline", "telco", "conglomerate",
    "startup", "unicorn", "fintech", "biotech", "pharma", "giant", "major",
    "tech", "retail", "software", "energy", "oil", "gas", "media",
    "us", "usa", "american", "uk", "british", "indian", "india", "chinese",
    "china", "japanese", "japan", "german", "germany", "french", "france",
    "dutch", "swiss", "korean", "saudi", "canadian", "australian", "european",
    "billionaire", "state", "run", "owned", "backed", "based", "led",
    "buyout", "equity", "hedge", "fund", "firm", "pe", "vc",
}


def core(name):
    """Canonical form of a company name for matching across headlines."""
    s = re.sub(r"\([^)]*\)", " ", (name or "").lower())
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\bunited states\b", "us", s)
    s = re.sub(r"\bunited kingdom\b", "uk", s)
    s = re.sub(r"\bgreat britain\b", "uk", s)
    tokens = [t for t in s.split() if t and t != "and"]
    while tokens and tokens[0] in DESCRIPTORS:
        tokens.pop(0)
    tokens = [t for t in tokens if t not in LEGAL_SUFFIXES]
    while tokens and tokens[-1] in DESCRIPTORS:
        tokens.pop()
    return " ".join(tokens[:4])


def key_of(d):
    return core(d.get("acquirer")) + "|" + core(d.get("target"))


STATUS_RANK = {"Announced": 0, "Pending": 1, "Completed": 2, "Terminated": 3}

# Filings and wire services beat aggregators when two records disagree.
SOURCE_RANK = ["sec.gov", "prnewswire", "businesswire", "globenewswire",
               "reuters", "bloomberg", "ft.com", "wsj.com"]


def _source_score(d):
    u = (d.get("sourceUrl") or "").lower()
    for i, s in enumerate(reversed(SOURCE_RANK)):
        if s in u:
            return i + 1
    return 0


def fold(items):
    """Collapse several records of the same transaction into one."""
    best = max(items, key=_source_score)
    out = dict(best)

    out["announcedDate"] = min(i.get("announcedDate") or "9999" for i in items)
    out["status"] = max((i.get("status", "Announced") for i in items),
                        key=lambda s: STATUS_RANK.get(s, 0))

    valued = [i for i in items if i.get("valueUsdM")]
    if valued:
        v = max(valued, key=lambda i: i["valueUsdM"])
        out["valueUsdM"], out["dealValue"] = v["valueUsdM"], v["dealValue"]

    for field, blank in (("industry", ("", "Other")), ("region", ("", "Unspecified"))):
        if out.get(field) in blank:
            for i in items:
                if i.get(field) not in blank:
                    out[field] = i[field]
                    break

    # Prefer the spelling most outlets used; length only breaks ties.
    def consensus(field):
        names = [i.get(field, "") for i in items if i.get(field)]
        counts = Counter(names)
        return max(names, key=lambda n: (counts[n], -len(n))) if names else ""

    out["acquirer"] = consensus("acquirer")
    out["target"] = consensus("target")

    others = [i.get("sourceUrl") for i in items if i.get("sourceUrl") and i.get("sourceUrl") != out.get("sourceUrl")]
    out["alsoReported"] = len(others)
    return out


def _same_party(a, b):
    """True when two acquirer names plausibly refer to one company."""
    if not a or not b:
        return False
    if a == b:
        return True
    if a.startswith(b + " ") or b.startswith(a + " "):
        return True
    at, bt = a.split(), b.split()
    return len(at[0]) > 3 and at[0] == bt[0]


def dedupe(deals):
    """Cluster on the target, then on the acquirer within each target group."""
    by_target = {}
    for d in deals:
        if not d.get("acquirer") or not d.get("target"):
            continue
        by_target.setdefault(core(d["target"]), []).append(d)

    out = []
    for group in by_target.values():
        clusters = []
        for d in group:
            a = core(d["acquirer"])
            for c in clusters:
                if _same_party(a, c["a"]):
                    c["items"].append(d)
                    break
            else:
                clusters.append({"a": a, "items": [d]})
        out.extend(fold(c["items"]) for c in clusters)
    return out


def merge(existing, fresh):
    combined = dedupe(list(existing) + list(fresh))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    kept = [d for d in combined if d.get("announcedDate", "") >= cutoff]
    kept.sort(key=lambda d: d.get("announcedDate", ""), reverse=True)
    return kept


def rebuild(path):
    """Re-run dedupe and region tagging on an existing file. No network."""
    with open(path) as f:
        book = json.load(f)
    deals = book.get("deals", [])
    before = len(deals)

    for d in deals:
        d["region"] = detect_region(d.get("headline") or "",
                                    acquirer=d.get("acquirer", ""),
                                    target=d.get("target", ""))
    deals = merge(deals, [])

    book["deals"] = deals
    book["dealCount"] = len(deals)
    book["updatedAt"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    book.pop("sample", None)
    with open(path, "w") as f:
        json.dump(book, f, indent=1, ensure_ascii=False)

    print(f"Rebuilt {path}: {before} -> {len(deals)} deals ({before - len(deals)} duplicates removed)")
    for r, n in Counter(d.get("region") for d in deals).most_common():
        print(f"  {r:22} {n}")


def main():
    if "--rebuild" in sys.argv:
        rebuild(OUT_FILE)
        return

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
