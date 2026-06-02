"""
Phase 3 & 4 — Full RSS Financial News Pipeline
===============================================
Phase 3 : Fetch & parse all 52 feeds  →  raw article records
Enrich  : Parallel og:description fetch for articles with no RSS summary
Phase 4 : Clean & normalize           →  rich JSON with entities + sentiment

Many feeds (Yahoo Finance, Fortune, Business Insider …) publish RSS with only
a title + link — no description field at all.  We fix this with a parallel
meta-description fetch (30 workers, 5 s timeout each) so every article gets
real content before Phase 4 runs.

Outputs (workspace root):
  pipeline_output.json   — full run with metadata + all articles
  pipeline_output.jsonl  — one article per line (vector-store ready)
"""

import feedparser
import hashlib
import json
import os
import re
import tempfile
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import bleach
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from langdetect import detect, LangDetectException

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────────────────────
# FEED REGISTRY — 52 feeds
# ──────────────────────────────────────────────────────────────────────────────
FEEDS = [
    {"id":  1, "name": "Reuters – Business News",      "url": "https://feeds.reuters.com/reuters/businessNews",                       "region": "Global",          "category": "News & Analysis",       "subcategory": "General business / markets"},
    {"id":  2, "name": "Reuters – Markets",            "url": "https://feeds.reuters.com/reuters/financialsNews",                     "region": "Global",          "category": "News & Analysis",       "subcategory": "Markets & equities"},
    {"id":  3, "name": "Yahoo Finance – News",         "url": "https://finance.yahoo.com/news/rss",                                   "region": "Global",          "category": "News & Analysis",       "subcategory": "General financial news"},
    {"id":  4, "name": "MarketWatch – Top Stories",    "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories",           "region": "Global",          "category": "News & Analysis",       "subcategory": "Equities & markets"},
    {"id":  5, "name": "MarketWatch – Markets",        "url": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",          "region": "Global",          "category": "News & Analysis",       "subcategory": "Markets"},
    {"id":  6, "name": "WSJ – Markets Main",           "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",                        "region": "Global",          "category": "News & Analysis",       "subcategory": "Global markets",       "paywall": True},
    {"id":  7, "name": "WSJ – World News",             "url": "https://feeds.content.dowjones.io/public/rss/RSSWSJD",                 "region": "Global",          "category": "News & Analysis",       "subcategory": "World / economy",      "paywall": True},
    {"id":  8, "name": "Financial Times – Home",       "url": "https://www.ft.com/rss/home",                                          "region": "Global",          "category": "News & Analysis",       "subcategory": "Global financial news","paywall": True},
    {"id":  9, "name": "CNBC – Finance",               "url": "https://www.cnbc.com/id/10001147/device/rss/rss.html",                  "region": "Global",          "category": "News & Analysis",       "subcategory": "Finance"},
    {"id": 10, "name": "CNBC – World News",            "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html",                 "region": "Global",          "category": "News & Analysis",       "subcategory": "World markets"},
    {"id": 11, "name": "CNBC – Economy",               "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html",                  "region": "Global",          "category": "News & Analysis",       "subcategory": "Macro economy"},
    {"id": 12, "name": "Fox Business – Latest",        "url": "https://moxie.foxbusiness.com/google-publisher/latest.xml",            "region": "Global",          "category": "News & Analysis",       "subcategory": "Economy / markets"},
    {"id": 13, "name": "Nasdaq – Original Articles",   "url": "https://www.nasdaq.com/feed/nasdaq-original/rss.xml",                  "region": "Global",          "category": "News & Analysis",       "subcategory": "Equities & trading signals"},
    {"id": 14, "name": "Fortune – Business",           "url": "https://fortune.com/feed/fortune-feeds/?id=3230629",                   "region": "Global",          "category": "News & Analysis",       "subcategory": "Business & economy"},
    {"id": 15, "name": "Seeking Alpha",                "url": "https://seekingalpha.com/feed.xml",                                    "region": "Global",          "category": "News & Analysis",       "subcategory": "Investment analysis",  "paywall": True},
    {"id": 16, "name": "Investing.com – All News",     "url": "https://www.investing.com/rss/news.rss",                               "region": "Global",          "category": "News & Analysis",       "subcategory": "All markets"},
    {"id": 17, "name": "Investing.com – Forex",        "url": "https://www.investing.com/rss/news_25.rss",                            "region": "Global",          "category": "News & Analysis",       "subcategory": "Forex"},
    {"id": 18, "name": "Investing.com – Commodities",  "url": "https://www.investing.com/rss/news_11.rss",                            "region": "Global",          "category": "News & Analysis",       "subcategory": "Commodities"},
    {"id": 19, "name": "Investing.com – Macro",        "url": "https://www.investing.com/rss/news_14.rss",                            "region": "Global",          "category": "News & Analysis",       "subcategory": "Macro economy"},
    {"id": 20, "name": "Investing.com – Stocks",       "url": "https://www.investing.com/rss/news_25.rss",                            "region": "Global",          "category": "News & Analysis",       "subcategory": "Equities"},
    {"id": 21, "name": "The Economist – Latest",       "url": "https://www.economist.com/latest/rss.xml",                             "region": "Global",          "category": "News & Analysis",       "subcategory": "Global macro / politics","paywall": True},
    {"id": 22, "name": "Benzinga – All News",          "url": "https://feeds.benzinga.com/benzinga",                                  "region": "Global",          "category": "News & Analysis",       "subcategory": "Stocks & trading signals"},
    {"id": 23, "name": "TheStreet – Full Feed",        "url": "https://www.thestreet.com/.rss/full",                                  "region": "Global",          "category": "News & Analysis",       "subcategory": "Investment / stock analysis"},
    {"id": 24, "name": "Motley Fool – Investing",      "url": "https://fool.com/a/feeds/partner/google-news/rss.aspx",                "region": "Global",          "category": "News & Analysis",       "subcategory": "Investment analysis"},
    {"id": 25, "name": "Business Insider – Finance",   "url": "https://www.businessinsider.com/rss",                                  "region": "Global",          "category": "News & Analysis",       "subcategory": "Finance / economy"},
    {"id": 26, "name": "Investopedia – News",          "url": "https://feeds-api.dotdashmeredith.com/api/v1/feeds/investopedia_latest","region": "Global",          "category": "News & Analysis",       "subcategory": "Financial education / news"},
    {"id": 27, "name": "Forbes – Business",            "url": "https://www.forbes.com/business/feed/",                                "region": "Global",          "category": "News & Analysis",       "subcategory": "Business / wealth"},
    {"id": 28, "name": "Barron's – Latest",            "url": "https://www.barrons.com/xml/rss/3_7551.xml",                           "region": "Global",          "category": "News & Analysis",       "subcategory": "Investment analysis",  "paywall": True},
    {"id": 29, "name": "US Federal Reserve – Data",    "url": "https://www.federalreserve.gov/feeds/data_releases.xml",               "region": "Global",          "category": "Official / Regulatory", "subcategory": "Monetary policy / econ data"},
    {"id": 30, "name": "St. Louis Fed (FRED)",         "url": "https://research.stlouisfed.org/publications/review/rss.xml",          "region": "Global",          "category": "Official / Regulatory", "subcategory": "Macro research & banking"},
    {"id": 31, "name": "IMF – News Releases",          "url": "https://www.imf.org/en/News/rss?language=eng",                         "region": "Global",          "category": "Official / Regulatory", "subcategory": "Global macro / policy"},
    {"id": 32, "name": "Zawya – Business",             "url": "https://www.zawya.com/sitemaps/en/rss",                                "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "MENA business & markets"},
    {"id": 33, "name": "Arab News – Business",         "url": "https://www.arabnews.com/rss.xml?pid=3",                               "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "GCC business"},
    {"id": 34, "name": "Gulf News – Business",         "url": "https://gulfnews.com/rss/business",                                    "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "UAE business & economy"},
    {"id": 35, "name": "The National – Business",      "url": "https://www.thenationalnews.com/business/rss",                         "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "UAE / GCC markets"},
    {"id": 36, "name": "Khaleej Times – Business",     "url": "https://www.khaleejtimes.com/rss/business",                            "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "UAE economy"},
    {"id": 37, "name": "Arabian Business",             "url": "https://www.arabianbusiness.com/feed",                                  "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "MENA business & industry"},
    {"id": 38, "name": "Al-Monitor – Economy",         "url": "https://www.al-monitor.com/rss/economy.xml",                           "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "MENA political economy"},
    {"id": 39, "name": "Trade Arabia – Business",      "url": "https://www.tradearabia.com/rss/NEWS.xml",                             "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "GCC trade & industry"},
    {"id": 40, "name": "Mubasher – Market News",       "url": "https://english.mubasher.info/rss/news",                               "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "MENA stock markets"},
    {"id": 41, "name": "MENA FN – Financial News",     "url": "https://www.menafn.com/rss/rss.aspx",                                  "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "MENA & global financial wire"},
    {"id": 42, "name": "Business Today Egypt",         "url": "https://www.businesstodayegypt.com/rss",                               "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "Egypt / North Africa business"},
    {"id": 43, "name": "Ammon News – Economy",         "url": "https://en.ammonnews.net/rss/1",                                       "region": "Regional (MENA)", "category": "News & Analysis",       "subcategory": "Jordan / Levant economy"},
    {"id": 44, "name": "ADX – Market Releases",        "url": "https://www.adx.ae/English/News/Pages/ADX-RSS.aspx",                   "region": "Regional (MENA)", "category": "Official / Regulatory", "subcategory": "Abu Dhabi exchange"},
    {"id": 45, "name": "DFM – Announcements",          "url": "https://www.dfm.ae/rss/news",                                          "region": "Regional (MENA)", "category": "Official / Regulatory", "subcategory": "Dubai exchange listings"},
    {"id": 46, "name": "Saudi Exchange (Tadawul)",      "url": "https://www.saudiexchange.sa/wps/portal/saudiexchange/newsandmedia/news-releases/rss", "region": "Regional (MENA)", "category": "Official / Regulatory", "subcategory": "Saudi equities / Tadawul"},
    {"id": 47, "name": "Qatar Stock Exchange",         "url": "https://www.qe.com.qa/rss/news",                                       "region": "Regional (MENA)", "category": "Official / Regulatory", "subcategory": "Qatar equities"},
    {"id": 48, "name": "Boursa Kuwait",                "url": "https://www.boursakuwait.com.kw/rss/news",                             "region": "Regional (MENA)", "category": "Official / Regulatory", "subcategory": "Kuwait equities"},
    {"id": 49, "name": "CBUAE – Publications",         "url": "https://www.centralbank.ae/en/rss",                                    "region": "Regional (MENA)", "category": "Official / Regulatory", "subcategory": "UAE monetary policy"},
    {"id": 50, "name": "SAMA – News",                  "url": "https://www.sama.gov.sa/en-US/rss/News.aspx",                          "region": "Regional (MENA)", "category": "Official / Regulatory", "subcategory": "Saudi monetary policy"},
    {"id": 51, "name": "SEC EDGAR – 8-K Filings",      "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom",       "region": "Global", "category": "Official / Regulatory", "subcategory": "US corporate filings"},
    {"id": 52, "name": "SEC EDGAR – Press Releases",   "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=PX14A6G&dateb=&owner=include&count=40&output=atom",   "region": "Global", "category": "Official / Regulatory", "subcategory": "US regulatory announcements"},
]

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
ARTICLE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
}
FETCH_TIMEOUT   = 12   # RSS feed fetch timeout (s)
META_TIMEOUT    = 6    # og:description / article meta fetch timeout (s)
META_WORKERS    = 30   # parallel threads for meta enrichment

TRACKING_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "ref","cmpid","ncid","yptr","guccounter","mbid",
}
BOILERPLATE_RE = [
    r"subscribe\s+(now|for\s+full\s+access|to\s+read)[^.]*\.",
    r"sign\s+up\s+for.*?newsletter[^.]*\.",
    r"cookie\s+policy[^.]*\.",
    r"all\s+rights\s+reserved[^.]*\.",
    r"terms\s+of\s+(use|service)[^.]*\.",
    r"follow\s+us\s+on\s+(twitter|linkedin|facebook|instagram)[^.]*\.",
    r"advertisement\b.*",
    r"©\s*\d{4}[^.]*\.",
    r"click\s+here\s+to\s+(read|subscribe)[^.]*\.",
    r"already\s+a\s+subscriber[^.]*\.",
]

# ──────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ──────────────────────────────────────────────────────────────────────────────
def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def strip_html(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", bleach.clean(str(text), tags=[], strip=True)).strip()

def remove_boilerplate(text: str) -> str:
    for p in BOILERPLATE_RE:
        text = re.sub(p, "", text, flags=re.IGNORECASE | re.DOTALL)
    return re.sub(r"\s{3,}", " ", text).strip()

def normalize_ts(raw: str) -> str:
    if not raw:
        return utc_now()
    try:
        dt = dateparser.parse(raw, ignoretz=False)
        if not dt:
            return utc_now()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return utc_now()

def clean_url(u: str) -> str:
    if not u:
        return ""
    try:
        p = urlparse(u)
        qs = {k: v for k, v in parse_qs(p.query).items() if k not in TRACKING_PARAMS}
        return urlunparse(p._replace(query=urlencode({k: v[0] for k, v in qs.items()})))
    except Exception:
        return u

def detect_lang(text: str) -> str:
    try:
        return detect(text[:800]) if text and len(text) > 30 else "unknown"
    except LangDetectException:
        return "unknown"

# ──────────────────────────────────────────────────────────────────────────────
# PHASE 3 — FETCH RSS FEEDS
# ──────────────────────────────────────────────────────────────────────────────
def fetch_feed(feed_cfg: dict) -> tuple[list, str]:
    """
    Download RSS/Atom feed bytes, save to /tmp file, parse with feedparser.
    Parsing from a local file avoids a Replit sandbox MAX_PATH crash that
    occurs when feedparser resolves long in-feed URLs during bytes parsing.
    """
    url = feed_cfg["url"]
    tmp_path = None
    try:
        resp = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT, allow_redirects=False)
        if resp.status_code in (301, 302, 303, 307, 308):
            loc = resp.headers.get("Location", "")
            if loc and len(loc) < 400:
                resp = requests.get(loc, headers=HEADERS, timeout=FETCH_TIMEOUT, allow_redirects=False)
        if resp.status_code in (401, 403, 429):
            return [], f"http_{resp.status_code}"
        if resp.status_code != 200:
            return [], f"http_{resp.status_code}"

        fd, tmp_path = tempfile.mkstemp(suffix=".xml", dir="/tmp")
        with os.fdopen(fd, "wb") as f:
            f.write(resp.content)

        parsed = feedparser.parse(tmp_path)
        return parsed.get("entries", []), "ok"
    except Exception as e:
        return [], f"error: {str(e)[:80]}"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

def parse_entry(entry, feed_cfg: dict, ingested_at: str) -> dict:
    """
    PHASE 3 — Extract all raw fields from a feedparser entry.
    Captures every field in the Phase 3 schema.
    """
    title   = strip_html(getattr(entry, "title", "") or "")
    link    = clean_url(getattr(entry, "link", "") or "")
    pub_raw = (getattr(entry, "published", None) or
               getattr(entry, "updated",   None) or
               getattr(entry, "dc_date",   None) or "")
    author  = strip_html(getattr(entry, "author", None) or
                         getattr(entry, "dc_creator", None) or "")

    # Summary: try multiple feedparser attribute names
    summary_raw = ""
    for attr in ("summary", "description"):
        val = getattr(entry, attr, None)
        if val:
            summary_raw = val
            break

    # Full content block (some feeds embed full text in <content:encoded>)
    full_in_feed = ""
    try:
        if hasattr(entry, "content") and entry.content:
            full_in_feed = entry.content[0].get("value", "")
    except Exception:
        pass

    tags = []
    try:
        tags = [t.term for t in getattr(entry, "tags", []) if hasattr(t, "term")]
    except Exception:
        pass

    dedup_key  = link if link else (title + pub_raw)
    article_id = sha256(dedup_key)

    return {
        # Phase 3 raw fields
        "id":            article_id,
        "title_raw":     title,
        "published_raw": pub_raw,
        "ingested_at":   ingested_at,
        "source_name":   feed_cfg["name"],
        "source_url":    feed_cfg["url"],
        "article_url":   link,
        "region":        feed_cfg["region"],
        "category":      feed_cfg["category"],
        "subcategory":   feed_cfg.get("subcategory", ""),
        "is_paywall":    feed_cfg.get("paywall", False),
        "tags_raw":      tags,
        "author":        author,
        "summary_raw":   summary_raw,
        "full_in_feed":  full_in_feed,
    }

# ──────────────────────────────────────────────────────────────────────────────
# ENRICHMENT — parallel og:description fetch for articles with no RSS summary
# ──────────────────────────────────────────────────────────────────────────────
def fetch_meta_description(article_url: str) -> str:
    """
    Fetch the article page and extract og:description (or meta description).
    Falls back to first meaningful <p> paragraph if no meta tag found.
    Fast and lightweight — reads only the <head> portion of the page.
    """
    if not article_url:
        return ""
    try:
        resp = requests.get(
            article_url, headers=ARTICLE_HEADERS, timeout=META_TIMEOUT,
            allow_redirects=False
        )
        if resp.status_code not in (200,):
            return ""
        soup = BeautifulSoup(resp.text[:80000], "lxml")

        # 1. og:description (most reliable)
        tag = soup.find("meta", attrs={"property": "og:description"})
        if tag and tag.get("content", "").strip():
            return strip_html(tag["content"].strip())

        # 2. Standard meta description
        tag = soup.find("meta", attrs={"name": "description"})
        if tag and tag.get("content", "").strip():
            return strip_html(tag["content"].strip())

        # 3. twitter:description
        tag = soup.find("meta", attrs={"name": "twitter:description"})
        if tag and tag.get("content", "").strip():
            return strip_html(tag["content"].strip())

        # 4. First substantive <p> paragraph in <body>
        for p in soup.find_all("p"):
            txt = p.get_text(strip=True)
            if len(txt) > 60:
                return remove_boilerplate(txt[:500])

        return ""
    except Exception:
        return ""

def enrich_summaries(raw_articles: list) -> list:
    """
    For every article whose summary_raw is empty, fetch og:description in
    parallel (META_WORKERS threads).  Updates each dict in place.
    """
    need_fetch = [a for a in raw_articles if not a["summary_raw"] and a["article_url"]]
    if not need_fetch:
        return raw_articles

    print(f"\n  → Enriching {len(need_fetch)} articles with no RSS summary "
          f"(parallel meta fetch, {META_WORKERS} workers) …")

    def fetch_one(art):
        desc = fetch_meta_description(art["article_url"])
        return art["id"], desc

    id_to_desc = {}
    with ThreadPoolExecutor(max_workers=META_WORKERS) as pool:
        futures = {pool.submit(fetch_one, a): a for a in need_fetch}
        done = 0
        for fut in as_completed(futures):
            try:
                aid, desc = fut.result()
                if desc:
                    id_to_desc[aid] = desc
            except Exception:
                pass
            done += 1
            if done % 50 == 0:
                print(f"    {done}/{len(need_fetch)} fetched …")

    enriched = 0
    for art in raw_articles:
        if art["id"] in id_to_desc:
            art["summary_raw"] = id_to_desc[art["id"]]
            art["summary_source"] = "og:description"
            enriched += 1
        else:
            art.setdefault("summary_source", "rss")

    print(f"  → Enriched {enriched}/{len(need_fetch)} articles with meta descriptions")
    return raw_articles

# ──────────────────────────────────────────────────────────────────────────────
# PHASE 4 — CLEAN & NORMALIZE
# ──────────────────────────────────────────────────────────────────────────────
def extract_entities(text: str) -> dict:
    STOPWORDS = {
        "THE","AND","FOR","ARE","BUT","NOT","YOU","ALL","CAN","HER","WAS","ONE",
        "OUR","OUT","HAS","HIM","HIS","HOW","ITS","WHO","DID","TOP","NEW","MAY",
        "NOW","WAY","USE","TWO","SET","END","WHY","LET","DAY","FEW","FAR","YET",
        "OLD","BIG","OWN","OFF","CEO","CFO","COO","IPO","ETF","YTD","QTD","MOM",
        "YOY","GDP","CPI","PMI","PPI","ECB","IMF","WHO","WTO","NATO","SEC","FTC",
        "FDA","DOJ","EPA","IRS","FED","NYC","USA","UAE","GCC","KSA","MENA","OPEC",
        "CNN","BBC","FOX","NBC","CBS","ABC","AP","AI","ML","US","UK","EU","UN",
    }
    ticker_re    = r"\b([A-Z]{2,5}(?:\.[A-Z]{1,2})?)\b"
    currency_re  = r"\b(USD|EUR|GBP|AED|SAR|QAR|KWD|BHD|OMR|EGP|JPY|CNY|CHF|DXY|CAD|AUD|INR|TRY|BRL)\b"
    commodity_re = r"\b(gold|silver|oil|crude|brent|WTI|copper|gas|wheat|corn|platinum|palladium|nickel|coal|LNG)\b"
    org_re       = (r"\b(Federal Reserve|Fed|IMF|World Bank|OPEC|OPEC\+|Tadawul|ADX|DFM|SEC|CBUAE|SAMA|"
                    r"Aramco|Saudi Aramco|Apple|Microsoft|Google|Amazon|Tesla|JPMorgan|Goldman Sachs|"
                    r"Morgan Stanley|BlackRock|Berkshire|Alphabet|Meta|Nvidia|OpenAI|Anthropic|"
                    r"Emirates NBD|First Abu Dhabi Bank|Mashreq|Cisco|Cerebras|SpaceX|Boeing)\b")
    rate_re      = r"(\d+\.?\d*)\s*(?:per cent|percent|%)"
    price_re     = r"(?:USD|EUR|GBP|\$|€|£)\s*(\d[\d,\.]+)"

    raw_tickers = re.findall(ticker_re, text)
    tickers  = sorted({t for t in raw_tickers if t not in STOPWORDS})[:15]
    currencies  = sorted(set(re.findall(currency_re, text)))
    commodities = sorted({c.lower() for c in re.findall(commodity_re, text, re.IGNORECASE)})
    orgs        = sorted(set(re.findall(org_re, text)))
    rates       = [float(r.replace(",","")) for r in re.findall(rate_re, text)][:8]
    prices      = [p.replace(",","") for p in re.findall(price_re, text)][:8]

    return {
        "tickers":          tickers,
        "currencies":       currencies,
        "commodities":      commodities,
        "organizations":    orgs,
        "rates_pct":        rates,
        "prices_mentioned": prices,
    }

def simple_sentiment(text: str) -> dict:
    POS = {
        "rise","rises","risen","gain","gains","gained","growth","grew","profit","profits",
        "surge","surged","rally","rallied","beat","record","high","strong","stronger",
        "climbed","up","rose","boost","boosted","bullish","positive","soared","jumped",
        "added","supported","robust","improved","increased","outperform","upgraded",
        "recovery","exceeded","optimism","accelerate","expansion","advance","milestone",
    }
    NEG = {
        "fall","falls","fell","drop","drops","dropped","loss","losses","decline","declined",
        "slump","crash","miss","missed","weak","weaker","low","lower","cut","risk",
        "tumble","slide","negative","bearish","pressure","concern","warning","deficit",
        "debt","recession","contraction","slowdown","layoffs","downgrade","uncertainty",
        "volatile","volatility","inflation","tariff","sanction","default","bankruptcy",
    }
    tokens = set(re.findall(r"\b\w+\b", text.lower()))
    pos = len(tokens & POS)
    neg = len(tokens & NEG)
    total = pos + neg or 1
    if pos > neg:
        return {"label": "positive", "score": round(pos/total, 2), "pos_hits": pos, "neg_hits": neg}
    if neg > pos:
        return {"label": "negative", "score": round(neg/total, 2), "pos_hits": pos, "neg_hits": neg}
    return {"label": "neutral", "score": 0.50, "pos_hits": pos, "neg_hits": neg}

def clean_article(raw: dict) -> dict:
    """PHASE 4 — full cleaning, enrichment, and normalization pipeline."""

    # Step 1+3: Strip HTML + remove boilerplate from all text sources
    summary    = remove_boilerplate(strip_html(raw["summary_raw"]))[:3000]
    feed_body  = remove_boilerplate(strip_html(raw["full_in_feed"]))
    title_clean = strip_html(raw["title_raw"])

    # Best available corpus for analysis (prefer feed body > summary > title)
    corpus = feed_body or summary or title_clean

    # Step 2: Normalize timestamp to UTC ISO 8601
    published_at = normalize_ts(raw["published_raw"])

    # Step 4: Language detection
    language = detect_lang(corpus)

    # Step 5: Entity extraction
    entities  = extract_entities(corpus) if corpus else {}

    # Step 6: Sentiment tagging
    sentiment = simple_sentiment(corpus) if corpus else {}

    return {
        # ── Identity ────────────────────────────────────────────────────────
        "id":            raw["id"],
        # ── Source ──────────────────────────────────────────────────────────
        "source_name":   raw["source_name"],
        "source_url":    raw["source_url"],
        "article_url":   raw["article_url"],
        "region":        raw["region"],
        "category":      raw["category"],
        "subcategory":   raw["subcategory"],
        "is_paywall":    raw["is_paywall"],
        # ── Content ─────────────────────────────────────────────────────────
        "title":         title_clean,
        "author":        raw["author"],
        "published_at":  published_at,
        "ingested_at":   raw["ingested_at"],
        "tags":          [t.lower().strip() for t in raw["tags_raw"]],
        "summary":       summary,
        "full_text":     feed_body,
        "summary_source":raw.get("summary_source", "rss"),
        "word_count":    len(corpus.split()) if corpus else 0,
        # ── Enrichment ──────────────────────────────────────────────────────
        "language":      language,
        "entities":      entities,
        "sentiment":     sentiment,
        # ── Pipeline flags ───────────────────────────────────────────────────
        "chunk_ready":   bool(corpus.strip()),
    }

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────
def run():
    ingested_at = utc_now()
    DIVIDER = "═" * 72

    print(DIVIDER)
    print(f"  RSS Financial News Pipeline  —  Phase 3 & 4")
    print(f"  Run time (UTC): {ingested_at}")
    print(f"  Feeds configured: {len(FEEDS)}")
    print(DIVIDER)

    raw_articles = []
    feed_log     = []
    seen_ids     = set()

    # ── PHASE 3: Fetch all feeds ─────────────────────────────────────────────
    print("\n▶  PHASE 3 — Fetching feeds …\n")
    for feed_cfg in FEEDS:
        name = feed_cfg["name"]
        print(f"  [{feed_cfg['id']:02d}/52] {name}")
        entries, status = fetch_feed(feed_cfg)

        if status != "ok":
            print(f"         SKIP — {status}")
            feed_log.append({"feed": name, "url": feed_cfg["url"], "status": status,
                              "entries": 0, "new": 0})
            continue

        new_count = 0
        for entry in entries:
            try:
                raw = parse_entry(entry, feed_cfg, ingested_at)
                if not raw["title_raw"] or raw["id"] in seen_ids:
                    continue
                seen_ids.add(raw["id"])
                raw_articles.append(raw)
                new_count += 1
            except Exception:
                pass

        has_summary = sum(1 for a in raw_articles[-new_count:] if a["summary_raw"])
        print(f"         {new_count} articles  |  {has_summary} have RSS summary  |  "
              f"{new_count - has_summary} need meta fetch")
        feed_log.append({"feed": name, "url": feed_cfg["url"], "region": feed_cfg["region"],
                          "status": status, "entries": len(entries), "new": new_count})

    print(f"\n  Phase 3 complete: {len(raw_articles)} articles from "
          f"{sum(1 for f in feed_log if f['status']=='ok')} feeds")

    # ── ENRICHMENT: parallel og:description for empty summaries ──────────────
    print("\n▶  ENRICHMENT — parallel meta description fetch …")
    raw_articles = enrich_summaries(raw_articles)

    # ── PHASE 4: Clean & normalize ───────────────────────────────────────────
    print("\n▶  PHASE 4 — Cleaning & normalizing …\n")
    all_articles = []
    for raw in raw_articles:
        try:
            article = clean_article(raw)
            all_articles.append(article)
        except Exception as e:
            pass

    # ── Summary stats ────────────────────────────────────────────────────────
    total         = len(all_articles)
    chunk_ready   = sum(1 for a in all_articles if a["chunk_ready"])
    has_summary   = sum(1 for a in all_articles if a["summary"])
    from_og       = sum(1 for a in all_articles if a.get("summary_source") == "og:description")
    by_sentiment  = {"positive": 0, "negative": 0, "neutral": 0}
    by_region     = {}
    by_lang       = {}
    all_ents      = {"tickers": set(), "currencies": set(), "commodities": set(), "organizations": set()}

    for a in all_articles:
        lbl = a.get("sentiment", {}).get("label", "neutral")
        by_sentiment[lbl] = by_sentiment.get(lbl, 0) + 1
        by_region[a["region"]] = by_region.get(a["region"], 0) + 1
        by_lang[a["language"]] = by_lang.get(a["language"], 0) + 1
        for k in ("tickers","currencies","commodities","organizations"):
            all_ents[k].update(a.get("entities", {}).get(k, []))

    print(DIVIDER)
    print("  PIPELINE SUMMARY")
    print(DIVIDER)
    print(f"  Feeds attempted         : {len(FEEDS)}")
    print(f"  Feeds successful        : {sum(1 for f in feed_log if f['status']=='ok')}")
    print(f"  Total articles          : {total}")
    print(f"  Articles with summary   : {has_summary}  "
          f"({from_og} from meta fetch, {has_summary - from_og} from RSS)")
    print(f"  Chunk-ready             : {chunk_ready}/{total}")
    print(f"  By region               : {by_region}")
    print(f"  By language             : {by_lang}")
    print(f"  Sentiment               : {by_sentiment}")
    print(f"  Unique tickers found    : {len(all_ents['tickers'])}")
    print(f"  Currencies mentioned    : {sorted(all_ents['currencies'])}")
    print(f"  Commodities mentioned   : {sorted(all_ents['commodities'])}")
    print(f"  Organizations mentioned : {sorted(all_ents['organizations'])[:25]}")

    # Sample — show 3 full normalized articles
    print(f"\n{'─'*72}")
    print("  SAMPLE ARTICLES (3 of {total})")
    print(f"{'─'*72}\n")
    for a in all_articles[:3]:
        print(json.dumps(a, indent=2, ensure_ascii=False))
        print()

    # ── Save ─────────────────────────────────────────────────────────────────
    output = {
        "pipeline_run": {
            "ingested_at":            ingested_at,
            "feeds_configured":       len(FEEDS),
            "feeds_successful":       sum(1 for f in feed_log if f["status"] == "ok"),
            "total_articles":         total,
            "articles_with_summary":  has_summary,
            "summaries_from_rss":     has_summary - from_og,
            "summaries_from_og_meta": from_og,
            "chunk_ready":            chunk_ready,
            "by_region":              by_region,
            "by_language":            by_lang,
            "sentiment_breakdown":    by_sentiment,
            "all_entities":           {k: sorted(v) for k, v in all_ents.items()},
        },
        "feed_log":  feed_log,
        "articles":  all_articles,
    }
    with open("pipeline_output.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    with open("pipeline_output.jsonl", "w", encoding="utf-8") as f:
        for art in all_articles:
            f.write(json.dumps(art, ensure_ascii=False) + "\n")

    json_kb  = os.path.getsize("pipeline_output.json")  // 1024
    jsonl_kb = os.path.getsize("pipeline_output.jsonl") // 1024

    print(DIVIDER)
    print(f"  Saved: pipeline_output.json  — {json_kb} KB  ({total} articles)")
    print(f"  Saved: pipeline_output.jsonl — {jsonl_kb} KB  ({total} lines, vector-store ready)")
    print(DIVIDER)

if __name__ == "__main__":
    run()
