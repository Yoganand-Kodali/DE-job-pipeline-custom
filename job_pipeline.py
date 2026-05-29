"""
╔══════════════════════════════════════════════════════════╗
║   DATA ENGINEER JOB PIPELINE — FINAL                   ║
║   Sources: LinkedIn · Indeed · Dice                    ║
║   Filters: Last 24h · YOE < 8 · Resume match · US     ║
╚══════════════════════════════════════════════════════════╝

Setup:
  1. pip install -r requirements.txt
     (requirements: requests cloudscraper pandas pdfminer.six beautifulsoup4 python-dotenv)
  2. Create .env file  (see bottom of this file)
  3. Place resume.pdf  in the same folder
  4. python job_pipeline.py
  5. Cron: 0 9,21 * * * cd /path/to/folder && python3 job_pipeline.py
"""

import os, re, json, time, random, smtplib, traceback
from collections import Counter
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import requests
import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════

RESUME_FILE    = "resume.pdf"
JOB_HISTORY    = "job_history.csv"
MAX_PER_SOURCE = 20
FINAL_TOP      = 50
MIN_SCORE      = 3

EMAIL_SENDER   = os.getenv("EMAIL_SENDER",   "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO       = os.getenv("EMAIL_TO",       "")
SMTP_HOST      = os.getenv("SMTP_HOST",      "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT",  "587"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ══════════════════════════════════════════════════════
# SKILLS
# ══════════════════════════════════════════════════════

SKILL_WEIGHTS = {
    "pyspark": 3, "databricks": 3, "snowflake": 3, "bigquery": 3,
    "kafka": 3,   "airflow": 3,   "delta lake": 3,
    "spark": 2,   "aws": 2,       "azure": 2,      "gcp": 2,
    "etl": 2,     "data pipeline": 2, "data warehouse": 2, "dbt": 2,
    "python": 1,  "sql": 1,
}

ROLE_BONUS = {"staff": 5, "principal": 4, "lead": 4, "senior": 3, "sr.": 3, "sr ": 3}

BANNED_ROLES = [
    "intern", "internship", "junior", "entry level", "entry-level",
    "student", "graduate", "new grad", "fresh grad", "associate", "apprentice",
]

OPT_POS_SIGNALS = [
    "visa sponsorship", "sponsor visa", "h1b", "opt eligible", "cpt",
    "work authorization", "authorized to work", "visa support",
    "staffing", "consulting", "c2c", "corp to corp",
]
OPT_NEG_SIGNALS = [
    "us citizens only", "no sponsorship", "must be authorized",
    "no visa", "citizen or permanent resident only", "will not sponsor",
]

# Ordered — most specific first so multi-word tags match before substrings
ALL_TECH = [
    "pyspark", "delta lake", "databricks", "snowflake", "bigquery", "airflow",
    "kafka", "spark", "redshift", "dbt", "terraform", "kubernetes", "docker",
    "mlflow", "flink", "hadoop", "hive", "looker", "tableau", "powerbi",
    "aws", "azure", "gcp", "python", "scala", "java", "sql", "go", "etl", "elt",
]

# Greenhouse company boards to query
GREENHOUSE_BOARDS = [
    "anthropic", "databricks", "snowflake", "stripe", "life360", "figma",
    "notion", "rippling", "brex", "plaid", "3cloud", "velir", "flowcode",
    "attentive", "cockroachlabs", "samsara", "retool", "dbt-labs",
    "canva", "intercom", "benchling", "hex-technologies",
]

# ══════════════════════════════════════════════════════
# DATE HELPERS
# ══════════════════════════════════════════════════════

def is_within_24h(posted_str):
    """True if posted today or yesterday.
    Uses a 48h window because scrapers store date-only (no time),
    so a job posted yesterday evening appears as ~36h ago with hours=24 cutoff.
    """
    s = str(posted_str or "").strip().lower()
    if s in ("", "unknown"):
        return False
    if s == "active":
        return True    # same-day posts from LinkedIn/Glassdoor
    try:
        posted = datetime.strptime(s[:10], "%Y-%m-%d")
        return (datetime.today() - posted) <= timedelta(hours=48)
    except Exception:
        return False

def is_us_location(location_str):
    """Return True only if the location is in the United States (or remote without a country)."""
    loc = str(location_str or "").strip().lower()
    if not loc or loc in ("united states", "us", "usa", "remote"):
        return True
    # Explicitly non-US country patterns
    non_us = [
        "canada", "united kingdom", "uk", " india", "australia", "germany",
        "france", "netherlands", "spain", "poland", "ireland", "singapore",
        "brazil", "mexico", "argentina", "colombia", "philippines", "remote - ca",
        "toronto", "london", "berlin", "bangalore", "mumbai", "sydney", "paris",
    ]
    return not any(c in loc for c in non_us)

def _parse_relative_date(text):
    """'3 hours ago' / '1 day ago' / 'Today' → YYYY-MM-DD string."""
    t = (text or "").lower()
    today = datetime.today()
    if any(k in t for k in ("just", "hour", "minute", "today", "active", "moment", "now")):
        return today.strftime("%Y-%m-%d")
    m = re.search(r'(\d+)\s*day', t)
    if m:
        return (today - timedelta(days=int(m.group(1)))).strftime("%Y-%m-%d")
    return today.strftime("%Y-%m-%d")

# ══════════════════════════════════════════════════════
# RESUME SKILL EXTRACTION
# ══════════════════════════════════════════════════════

def extract_resume_skills():
    try:
        text = extract_text(RESUME_FILE).lower()
        found = [s for s in SKILL_WEIGHTS if re.search(r'\b' + re.escape(s) + r'\b', text)]
        print(f"  Skills from resume: {found}")
        return found if found else list(SKILL_WEIGHTS.keys())
    except Exception as e:
        print(f"  [WARN] Resume read failed ({e}). Using full skill list.")
        return list(SKILL_WEIGHTS.keys())

# ══════════════════════════════════════════════════════
# SCORING + CLASSIFICATION
# ══════════════════════════════════════════════════════

def score_job(title, description, resume_skills):
    text = (title + " " + description).lower()
    matched, pts = [], 0
    for skill in resume_skills:
        if re.search(r'\b' + re.escape(skill) + r'\b', text):
            matched.append(skill)
            pts += SKILL_WEIGHTS.get(skill, 1)
    bonus = max((v for k, v in ROLE_BONUS.items() if k in title.lower()), default=0)
    opt = ("positive" if any(k in text for k in OPT_POS_SIGNALS) else
           "negative" if any(k in text for k in OPT_NEG_SIGNALS) else "unknown")
    return {"total_score": pts + bonus, "matched_skills": matched, "opt_signal": opt}

def valid_role(title):
    t = title.lower()
    if any(b in t for b in BANNED_ROLES):
        return False
    return any(k in t for k in [
        "data engineer", "data platform", "analytics engineer", "etl",
        "pipeline engineer", "data infrastructure", "spark engineer",
        "databricks", "staff engineer", "lead engineer", "principal engineer",
    ])

def yoe_ok(text):
    """False if job explicitly requires 8+ years."""
    hits = re.findall(
        r'(\d+)\+?\s*(?:to\s*\d+\s*)?years?\s*(?:of\s*)?(?:experience|exp\b)',
        text.lower()
    )
    return all(int(y) < 8 for y in hits) if hits else True

def detect_work_mode(location_str, description_str):
    """
    Returns 'remote', 'hybrid', or 'onsite'.
    Priority: hybrid > remote > onsite.
    Scans both location field AND full description text.
    """
    loc  = str(location_str  or "").lower().strip()
    desc = str(description_str or "").lower().strip()
    combined = loc + " " + desc

    hybrid_kw = [
        "hybrid", "hybrid work", "hybrid schedule", "hybrid model",
        "hybrid remote", "partially remote", "2 days remote", "3 days remote",
        "2 days in office", "3 days in office", "2 days onsite", "3 days onsite",
        "in-office 2", "in-office 3", "flexible / hybrid", "mix of remote",
    ]
    remote_kw = [
        "remote", "work from home", "wfh", "fully remote", "100% remote",
        "telecommute", "distributed team", "remote-first", "remote only",
        "remote anywhere", "work remotely", "remote position", "remote, usa",
        "remote - usa", "remote (usa)", "remote us", "home office",
    ]
    onsite_kw = [
        "on-site", "onsite", "in office", "in-office", "on site",
        "must be local", "must be on-site", "no remote",
    ]

    # Explicit onsite language beats everything
    if any(kw in combined for kw in onsite_kw):
        # But if hybrid is also mentioned, it's hybrid
        if any(kw in combined for kw in hybrid_kw):
            return "hybrid"
        return "onsite"

    if any(kw in combined for kw in hybrid_kw):
        return "hybrid"
    if any(kw in combined for kw in remote_kw):
        return "remote"

    # Location field heuristics
    if "remote" in loc:
        return "remote"
    if any(city in loc for city in [", ca", ", ny", ", tx", ", wa", ", il", ", ga", ", ma"]):
        return "onsite"  # specific city = likely onsite unless description says remote

    return "onsite"

# ══════════════════════════════════════════════════════
# HTTP HELPER
# ══════════════════════════════════════════════════════

def safe_get(url, is_api=False, retries=2):
    for _ in range(retries):
        try:
            time.sleep(random.uniform(0.8, 2.2))
            r = requests.get(url, headers={} if is_api else HEADERS, timeout=15)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                print("  [RATE LIMIT] sleeping 12s …")
                time.sleep(12)
        except Exception as e:
            print(f"  [ERR] {url[:60]}: {e}")
    return None

def make_job(company, title, platform, location, job_type, salary,
             posted, link, description, work_mode=None):
    if work_mode is None:
        work_mode = detect_work_mode(location, description)
    return dict(
        company=company, title=title, platform=platform, location=location,
        type=job_type, salary=salary, posted=posted, link=link,
        description=description, work_mode=work_mode,
    )

# ══════════════════════════════════════════════════════
# SCRAPERS
# ══════════════════════════════════════════════════════

def greenhouse_jobs():
    """Scrape Greenhouse public API for known tech company boards."""
    jobs = []
    for board in GREENHOUSE_BOARDS:
        r = safe_get(
            f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs",
            is_api=True,
        )
        if not r:
            continue
        try:
            for job in r.json().get("jobs", []):
                t   = job.get("title", "")
                loc = job.get("location", {}).get("name", "")
                if not valid_role(t) or not yoe_ok(t + " " + loc):
                    continue
                link = job.get("absolute_url", "")
                desc = t + " " + loc
                mode = detect_work_mode(loc, desc)
                jobs.append(make_job(
                    board.replace("-", " ").title(), t, "Greenhouse",
                    loc, "Full-Time", "Not Listed", "Active", link, desc, mode,
                ))
        except Exception as e:
            print(f"  [Greenhouse/{board}] {e}")
    print(f"  Greenhouse: {len(jobs)}")
    return jobs


def linkedin_jobs():
    """Scrape LinkedIn guest job search API (last 24h, US)."""
    jobs = []
    queries = [
        "senior+data+engineer", "staff+data+engineer",
        "lead+data+engineer",   "data+platform+engineer",
        "senior+analytics+engineer",
    ]
    for q in queries:
        r = safe_get(
            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            f"?keywords={q}&location=United%20States&f_TPR=r86400"
        )
        if not r:
            continue
        try:
            soup = BeautifulSoup(r.text, "html.parser")
            for card in soup.find_all("li"):
                tt = card.find("h3", class_="base-search-card__title")
                ct = card.find("h4", class_="base-search-card__subtitle")
                lt = card.find("span", class_="job-search-card__location")
                la = card.find("a", class_="base-card__full-link")
                sa = card.find("span", class_=re.compile(r"job-search-card__salary"))
                if not tt:
                    continue
                title = tt.text.strip()
                if not valid_role(title) or not yoe_ok(title):
                    continue
                loc  = lt.text.strip() if lt else "United States"
                if not is_us_location(loc):
                    continue
                sal  = sa.text.strip() if sa else "Not Listed"
                link = la["href"].split("?")[0] if la else ""
                desc = title + " " + q.replace("+", " ") + " " + loc
                mode = detect_work_mode(loc, desc)
                jobs.append(make_job(
                    ct.text.strip() if ct else "Unknown", title, "LinkedIn",
                    loc, "Full-Time", sal,
                    datetime.today().strftime("%Y-%m-%d"), link, desc, mode,
                ))
        except Exception as e:
            print(f"  [LinkedIn/{q}] {e}")
    print(f"  LinkedIn: {len(jobs)}")
    return jobs


def indeed_jobs():
    """
    Scrape Indeed for DE jobs posted in last 48h.
    Indeed properly supports fromage=1&sort=date — most reliable recent-jobs source.
    Uses cloudscraper to bypass bot detection.
    """
    jobs = []
    try:
        cs = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    except Exception as e:
        print(f"  [Indeed] cloudscraper init failed: {e}")
        return jobs

    queries = [
        "senior data engineer",
        "staff data engineer",
        "lead data engineer",
        "data platform engineer",
        "analytics engineer",
    ]
    today = datetime.today().strftime("%Y-%m-%d")

    for q in queries:
        url = (
            "https://www.indeed.com/jobs"
            f"?q={requests.utils.quote(q)}&l=United+States"
            "&fromage=1&sort=date&radius=25"
        )
        try:
            time.sleep(random.uniform(2.0, 4.0))
            r = cs.get(url, timeout=25)
            if r.status_code != 200:
                print(f"  [Indeed/{q}] HTTP {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            # Try to get jobs from embedded JSON first (more reliable)
            for script in soup.find_all("script"):
                src = script.string or ""
                if "jobKeysWithInfo" not in src and "mosaic-provider-jobcards" not in src:
                    continue
                m = re.search(r'"jobKeysWithInfo"\s*:\s*(\{[^<]{100,}\})', src)
                if not m:
                    continue
                try:
                    blob = json.loads(m.group(1))
                    for jk, jdata in blob.items():
                        t       = jdata.get("title", "")
                        company = jdata.get("company", "Unknown")
                        loc     = jdata.get("formattedLocation", jdata.get("location", "United States"))
                        if not t or not valid_role(t) or not yoe_ok(t) or not is_us_location(loc):
                            continue
                        link    = f"https://www.indeed.com/viewjob?jk={jk}"
                        # date: relative text like "1 day ago" or epoch ms
                        raw_dt  = jdata.get("pubDate", jdata.get("formattedRelativeTime", ""))
                        if isinstance(raw_dt, (int, float)):
                            pd_ = datetime.fromtimestamp(raw_dt / 1000).strftime("%Y-%m-%d")
                        else:
                            pd_ = _parse_relative_date(str(raw_dt))
                        if not is_within_24h(pd_):
                            continue
                        mode = detect_work_mode(loc, t + " " + loc)
                        jobs.append(make_job(company, t, "Indeed", loc, "Full-Time", "—", pd_, link, t + " " + loc, mode))
                except Exception:
                    pass
                break

            # HTML card fallback
            cards = soup.find_all("div", class_=re.compile(r"job_seen_beacon|cardOutline|tapItem"))
            for card in cards:
                t_el  = card.find("h2",   class_=re.compile(r"jobTitle"))
                co_el = card.find("span", class_=re.compile(r"companyName"))
                l_el  = card.find("div",  class_=re.compile(r"companyLocation"))
                a_el  = card.find("a", href=True)
                d_el  = card.find("span", class_=re.compile(r"^date|posted"))
                if not t_el:
                    continue
                t = t_el.get_text(strip=True).replace("new", "").strip()
                if not valid_role(t) or not yoe_ok(t):
                    continue
                company    = co_el.get_text(strip=True) if co_el else "Unknown"
                loc        = l_el.get_text(strip=True)  if l_el  else "United States"
                if not is_us_location(loc):
                    continue
                posted_txt = d_el.get_text(strip=True)  if d_el  else "Today"
                href       = a_el["href"]               if a_el  else ""
                if href.startswith("/"):
                    href = "https://www.indeed.com" + href
                pd_  = _parse_relative_date(posted_txt)
                if not is_within_24h(pd_):
                    continue
                mode = detect_work_mode(loc, t + " " + loc)
                jobs.append(make_job(company, t, "Indeed", loc, "Full-Time", "—", pd_, href, t + " " + loc, mode))

        except Exception as e:
            print(f"  [Indeed/{q}] {e}")

    print(f"  Indeed: {len(jobs)}")
    return jobs


def dice_jobs():
    """
    Scrape Dice.com using cloudscraper.
    Dice is a Next.js SPA — the search page embeds results in window.APP_INITIAL_STATE
    or __NEXT_DATA__. We fetch that with cloudscraper, then parse the JSON.
    Falls back to HTML card selectors.
    """
    jobs  = []
    today = datetime.today().strftime("%Y-%m-%d")

    try:
        cs = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
        # Warm up — get a valid Cloudflare cookie
        cs.get("https://www.dice.com/", timeout=15)
        time.sleep(random.uniform(2.0, 3.5))
    except Exception as e:
        print(f"  [Dice] init failed: {e}")
        return jobs

    queries = [
        "senior data engineer",
        "staff data engineer",
        "lead data engineer",
        "analytics engineer",
        "data platform engineer",
    ]

    for q in queries:
        found = 0
        url   = (
            f"https://www.dice.com/jobs?q={requests.utils.quote(q)}"
            "&countryCode=US&radius=30&radiusUnit=mi&pageSize=20"
            "&filters.postedDate=ONE&language=en&eid=S2Q_"
        )
        try:
            time.sleep(random.uniform(3.0, 5.0))
            r = cs.get(url, timeout=25)
            if r.status_code != 200:
                print(f"  [Dice/{q}] HTTP {r.status_code}")
                continue

            text = r.text
            soup = BeautifulSoup(text, "html.parser")

            # ── A: __NEXT_DATA__ embedded JSON ──────────────────────────────
            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if nd_tag and nd_tag.string:
                try:
                    nd       = json.loads(nd_tag.string)
                    pp       = nd.get("props", {}).get("pageProps", {})
                    # Try every known location for the jobs array
                    job_list = (
                        pp.get("jobs") or
                        pp.get("initialState", {}).get("search", {}).get("jobs") or
                        pp.get("searchResults", {}).get("hits") or
                        pp.get("searchState", {}).get("results") or []
                    )
                    for job in job_list:
                        t = job.get("title", "")
                        if not valid_role(t) or not yoe_ok(t):
                            continue
                        co_raw  = job.get("company", job.get("hiringCompanyName", {}))
                        company = co_raw.get("name", str(co_raw)) if isinstance(co_raw, dict) else str(co_raw or "Unknown")
                        loc_raw = job.get("location", job.get("jobLocation", {}))
                        loc     = loc_raw.get("displayName", str(loc_raw)) if isinstance(loc_raw, dict) else str(loc_raw or "United States")
                        if not is_us_location(loc):
                            continue
                        pd_ = str(job.get("postedDate", job.get("date", today)))[:10]
                        if not is_within_24h(pd_):
                            continue
                        link = (job.get("applyUrl") or job.get("url") or
                                f"https://www.dice.com/job-detail/{job.get('id','')}")
                        emp  = job.get("employmentType", "Full-Time") or "Full-Time"
                        mode = detect_work_mode(loc, t)
                        jobs.append(make_job(company, t, "Dice", loc, emp, "—", pd_, link, t, mode))
                        found += 1
                except Exception as e:
                    print(f"  [Dice/__NEXT_DATA__/{q}] {e}")

            # ── B: window.APP_INITIAL_STATE or similar inline JSON ───────────
            if not found:
                for pattern in [
                    r'window\.__APP_INITIAL_STATE__\s*=\s*(\{.+?\});\s*</script>',
                    r'window\.APP_INITIAL_STATE\s*=\s*(\{.+?\});\s*</script>',
                    r'"hits"\s*:\s*(\[(?:[^[\]]|\[(?:[^[\]]|\[[^\]]*\])*\])*\])',
                ]:
                    m = re.search(pattern, text, re.DOTALL)
                    if not m:
                        continue
                    try:
                        data     = json.loads(m.group(1))
                        # hits array is top-level or nested
                        hits     = data if isinstance(data, list) else data.get("hits", data.get("results", []))
                        for job in hits:
                            if not isinstance(job, dict):
                                continue
                            t = job.get("title", "")
                            if not valid_role(t) or not yoe_ok(t):
                                continue
                            company = job.get("company", {}).get("name", "Unknown") if isinstance(job.get("company"), dict) else job.get("company", "Unknown")
                            loc     = job.get("location", {}).get("displayName", "United States") if isinstance(job.get("location"), dict) else job.get("location", "United States")
                            if not is_us_location(loc):
                                continue
                            pd_ = str(job.get("postedDate", today))[:10]
                            if not is_within_24h(pd_):
                                continue
                            link = job.get("applyUrl") or f"https://www.dice.com/job-detail/{job.get('id','')}"
                            mode = detect_work_mode(loc, t)
                            jobs.append(make_job(job.get("company","Unknown") if not isinstance(job.get("company"),dict) else company,
                                                 t, "Dice", loc, "Full-Time", "—", pd_, link, t, mode))
                            found += 1
                        if found:
                            break
                    except Exception:
                        pass

            # ── C: HTML card selectors (last resort) ─────────────────────────
            if not found:
                for card in soup.find_all(["div","article"],
                                          attrs={"data-testid": re.compile(r"job-card|search-result|jobCard")}):
                    t_el  = card.find(["h5","h4","a"], class_=re.compile(r"title|jobTitle"))
                    co_el = card.find(class_=re.compile(r"company|employer"))
                    l_el  = card.find(class_=re.compile(r"location"))
                    a_el  = card.find("a", href=True)
                    d_el  = card.find(class_=re.compile(r"date|posted|age"))
                    if not t_el:
                        continue
                    t = t_el.get_text(strip=True)
                    if not valid_role(t) or not yoe_ok(t):
                        continue
                    company    = co_el.get_text(strip=True) if co_el else "Unknown"
                    loc        = l_el.get_text(strip=True)  if l_el  else "United States"
                    if not is_us_location(loc):
                        continue
                    posted_txt = d_el.get_text(strip=True)  if d_el  else "Today"
                    pd_  = _parse_relative_date(posted_txt)
                    if not is_within_24h(pd_):
                        continue
                    href = a_el["href"] if a_el else url
                    if href.startswith("/"):
                        href = "https://www.dice.com" + href
                    mode = detect_work_mode(loc, t)
                    jobs.append(make_job(company, t, "Dice", loc, "Full-Time", "—", pd_, href, t, mode))
                    found += 1

            if not found:
                print(f"  [Dice/{q}] 0 jobs parsed")

        except Exception as e:
            print(f"  [Dice/{q}] {e}")

    print(f"  Dice: {len(jobs)}")
    return jobs


# ══════════════════════════════════════════════════════
# DEDUP + HISTORY
# ══════════════════════════════════════════════════════

def remove_duplicates(df):
    if df.empty:
        return df
    df = df[df["link"].astype(str).str.strip().ne("")].drop_duplicates(subset=["link"])
    df = df.copy()
    norm = lambda s: re.sub(r'[^a-z0-9]', '', str(s).lower())
    df["_k"] = df["company"].apply(norm) + "|" + df["title"].apply(norm)
    return df.drop_duplicates(subset=["_k"]).drop(columns=["_k"])

def remove_seen_jobs(df):
    if not os.path.exists(JOB_HISTORY):
        return df
    try:
        seen = set(pd.read_csv(JOB_HISTORY)["link"].dropna())
        return df[~df["link"].isin(seen)]
    except Exception:
        return df

def save_history(df):
    out = df[["company", "title", "link"]].copy()
    out["date_seen"] = datetime.today().strftime("%Y-%m-%d")
    if os.path.exists(JOB_HISTORY):
        existing = pd.read_csv(JOB_HISTORY)
        out = pd.concat([existing, out]).drop_duplicates(subset=["link"])
    out.to_csv(JOB_HISTORY, index=False)

def top_jobs(df):
    """Sort all qualifying jobs by match score, return top 50 — no per-source cap."""
    return df.sort_values("total_score", ascending=False).head(FINAL_TOP).reset_index(drop=True)

# ══════════════════════════════════════════════════════
# DASHBOARD HELPERS
# ══════════════════════════════════════════════════════

def extract_chips(description, matched_skills):
    """Return up to 8 tech chip dicts {name, hi}."""
    text = description.lower()
    seen, chips = set(), []
    for tag in ALL_TECH:
        if re.search(r'\b' + re.escape(tag) + r'\b', text) and tag not in seen:
            # Nice display name
            display = {
                "pyspark": "PySpark", "delta lake": "Delta Lake",
                "databricks": "Databricks", "bigquery": "BigQuery",
                "aws": "AWS", "gcp": "GCP", "sql": "SQL",
                "etl": "ETL", "elt": "ELT",
            }.get(tag, tag.title())
            chips.append({"name": display, "hi": tag in matched_skills})
            seen.add(tag)
        if len(chips) >= 8:
            break
    return chips

def opt_label(signal, job_type, company="", platform=""):
    t = (job_type or "").lower()
    c = (company  or "").lower()

    if signal == "positive":
        return "✓ Sponsors Visas", "opt-pos"
    if signal == "negative":
        return "✗ No Sponsorship", "opt-neg"

    # Contract/staffing type → OPT friendly by nature
    if any(k in t for k in ["contract", "c2c", "consulting", "staffing", "w2"]):
        return "⚡ Contract / W2 Friendly", "opt-neu"

    # Company-name heuristics for interesting labels
    known = {
        "databricks": ("✦ Leading Data & AI Platform", "opt-neu"),
        "snowflake":  ("✦ Cloud Data Warehouse Leader", "opt-neu"),
        "amazon":     ("✦ Fortune 1 · Large Sponsor",   "opt-neu"),
        "google":     ("✦ Top Tech Employer",            "opt-neu"),
        "microsoft":  ("✦ Known H-1B Sponsor",           "opt-neu"),
        "meta":       ("✦ Top Tech Employer",            "opt-neu"),
        "apple":      ("✦ Top Tech Employer",            "opt-neu"),
        "netflix":    ("✦ Top Streaming Tech",           "opt-neu"),
        "airbnb":     ("✦ Remote-First Culture",         "opt-neu"),
        "stripe":     ("✦ Fintech Unicorn",              "opt-neu"),
        "uber":       ("✦ Global Tech Platform",         "opt-neu"),
        "lyft":       ("✦ Tech-Driven Mobility",         "opt-neu"),
        "linkedin":   ("✦ Microsoft-Owned Platform",     "opt-neu"),
        "oracle":     ("✦ Enterprise Data Giant",        "opt-neu"),
        "ibm":        ("✦ Known Visa Sponsor",           "opt-neu"),
        "accenture":  ("✓ Consulting Firm · OPT OK",     "opt-pos"),
        "deloitte":   ("✓ Big 4 · Sponsors Visas",       "opt-pos"),
        "infosys":    ("✓ Global IT · Sponsors OPT",     "opt-pos"),
        "tata":       ("✓ TCS · Sponsors OPT/H1B",       "opt-pos"),
        "cognizant":  ("✓ Consulting · OPT Friendly",    "opt-pos"),
        "capgemini":  ("✓ Consulting · OPT Friendly",    "opt-pos"),
        "wipro":      ("✓ Global IT · Sponsors OPT",     "opt-pos"),
        "randstad":   ("✓ Staffing W2 Sponsor",          "opt-pos"),
        "bcforward":  ("✓ W2 / Consulting Friendly",     "opt-pos"),
        "jobot":      ("⚡ Recruiter – Ask OPT",         "opt-neu"),
        "robert half":("⚡ Staffing – Ask OPT",          "opt-neu"),
        "dice":       ("⚡ Tech-Focused Job Board",       "opt-neu"),
    }
    for kw, label in known.items():
        if kw in c:
            return label

    # Platform-based fallbacks
    if platform == "Dice":
        return "⚡ Tech Role · Verify OPT",  "opt-neu"
    if platform == "LinkedIn":
        return "⚡ Active Hiring · Ask OPT", "opt-neu"
    if platform == "Glassdoor":
        return "⚡ Check Employer Profile",  "opt-neu"

    return "⚡ Verify OPT Directly", "opt-neu"

def _extract_sal_nums(s):
    vals = []
    for raw in re.findall(r'[\d,]+', str(s).replace(",","")):
        try:
            n = int(raw)
            if n > 500:          vals.append(n)
            elif 30 <= n <= 500: vals.append(n * 1000)
        except Exception:
            pass
    return vals

# Platform CSS tag class (matches reference HTML classes exactly)
PTAG_CLS = {
    "Greenhouse": "pg",
    "Lever":      "pl",
    "LinkedIn":   "pli",
    "Indeed":     "pbi",
    "Indeed":     "pbi",
    "Dice":       "pd",
    "Remotive":   "pr",
    "BuiltIn":    "pj",
}

# Donut chart colours per platform
PLAT_COLORS = {
    "Greenhouse": ("rgba(16,185,129,.25)",  "#10b981"),
    "LinkedIn":   ("rgba(99,102,241,.25)",  "#6366f1"),
    "Indeed":     ("rgba(56,189,248,.25)",   "#7dd3fc"),
    "Indeed":     ("rgba(56,189,248,.25)",  "#7dd3fc"),
    "Dice":       ("rgba(245,158,11,.25)",  "#f59e0b"),
}

# ══════════════════════════════════════════════════════
# DASHBOARD GENERATOR — replicates reference HTML exactly
# ══════════════════════════════════════════════════════

def generate_dashboard(df, resume_skills=None):
    now   = datetime.today()
    fname = f"jobs_{now.strftime('%Y-%m-%d_%H-%M')}.html"
    total = len(df)
    opt_n = len(df[df["opt_signal"] == "positive"])
    rem_n = int(df["work_mode"].isin(["remote", "hybrid"]).sum())
    con_n = len(df[df["type"].str.lower().str.contains("contract", na=False)])

    # Normalise scores to 0–100. Bar width = score_100, label = "XX/100"
    max_raw = df["total_score"].max() if not df.empty and df["total_score"].max() > 0 else 1
    df = df.copy()
    df["score_100"] = (df["total_score"] / max_raw * 100).round().astype(int).clip(0, 100)

    # ── Skill demand bars ────────────────────────────
    skill_counts = Counter()
    for _, row in df.iterrows():
        for s in (row.get("matched_skills") or []):
            skill_counts[s] += 1
    top_skills = skill_counts.most_common(10)
    max_sc     = top_skills[0][1] if top_skills else 1
    skill_bars = "".join(
        f'<div class="skill-row">'
        f'<span class="skill-lbl">{s.title()}</span>'
        f'<div class="skill-bg"><div class="skill-fill" data-w="{round(c/max_sc*100)}%"></div></div>'
        f'<span class="skill-n">{c}</span></div>'
        for s, c in top_skills
    ) or '<p style="color:var(--muted);font-size:12px">No skill data yet.</p>'

    # ── Platform donut ───────────────────────────────
    plat_counts = df["platform"].value_counts().to_dict()
    plat_labels = json.dumps(list(plat_counts.keys()))
    plat_bg     = json.dumps([PLAT_COLORS.get(p, ("rgba(99,102,241,.25)", "#6366f1"))[0] for p in plat_counts])
    plat_border = json.dumps([PLAT_COLORS.get(p, ("rgba(99,102,241,.25)", "#6366f1"))[1] for p in plat_counts])
    plat_values = json.dumps(list(plat_counts.values()))

    # ── Header lines ─────────────────────────────────
    skills_line  = " · ".join(s.title() for s in (resume_skills or [])[:14]) or "–"
    sources_line = "LinkedIn · Indeed · Dice"

    # ── Table rows + CSV data ────────────────────────
    rows_html, csv_rows = "", []
    today_str = now.strftime("%Y-%m-%d")

    for i, row in df.reset_index(drop=True).iterrows():
        rank      = i + 1
        score_100 = int(row.get("score_100", 0))
        posted    = str(row.get("posted", "Active") or "Active")
        posted_lbl = "Active" if (posted == today_str or posted.lower() in ("active", "")) else posted
        fresh_cls  = "posted fresh" if (posted == today_str or posted.lower() in ("active", "")) else "posted"
        typ        = str(row.get("type", "Full-Time") or "Full-Time")
        loc        = str(row.get("location", "") or "")
        work_mode  = str(row.get("work_mode", "onsite") or "onsite")
        opt_txt, opt_cls = opt_label(
            row.get("opt_signal", "unknown"), typ,
            company=row.get("company", ""), platform=row.get("platform", ""),
        )

        if work_mode == "hybrid":
            loc_badge = '<span class="badge-remote badge-hybrid">Hybrid</span>'
        elif work_mode == "remote":
            loc_badge = '<span class="badge-remote">Remote</span>'
        else:
            loc_badge = ""

        ptag_cls    = PTAG_CLS.get(row["platform"], "pli")
        row_cls     = "row-top"      if rank <= 5 else \
                      "row-contract" if "contract" in typ.lower() else ""
        data_remote = "yes" if work_mode in ("remote", "hybrid") else "no"
        data_type   = "contract" if "contract" in typ.lower() else "fulltime"
        data_opt    = "pos" if opt_cls == "opt-pos" else "no"

        rows_html += f"""
<tr class="{row_cls}" data-remote="{data_remote}" data-type="{data_type}" data-score="{score_100}" data-rank="{rank}" data-opt="{data_opt}">
  <td><div class="rank-cell"><span class="rank-num">{rank}</span><span class="score-badge">{score_100}<span class="score-denom">/100</span></span></div></td>
  <td><div class="company-name">{row['company']}</div></td>
  <td><div class="role-title">{row['title']}</div></td>
  <td><span class="ptag {ptag_cls}">{row['platform']}</span></td>
  <td><div class="loc-text">{loc}</div>{loc_badge}</td>
  <td><span class="type-tag">{typ}</span></td>
  <td><span class="{fresh_cls}">{posted_lbl}</span></td>
  <td><span class="opt {opt_cls}">{opt_txt}</span></td>
  <td><a class="apply-btn" href="{row['link']}" target="_blank">Apply →</a></td>
</tr>"""

        csv_rows.append([
            rank, row["company"], row["title"], row["platform"],
            loc, typ, posted_lbl, opt_txt, row["link"],
        ])

    csv_js = json.dumps(csv_rows)

    # ══════════════════════════════════════════════════
    # HTML — pixel-perfect match to attached reference
    # ══════════════════════════════════════════════════
    HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Engineer Jobs — OPT Radar · {now.strftime('%B %Y')}</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
:root {{
  --bg:      #080c12;
  --surface: #0e1420;
  --card:    #111827;
  --border:  #1a2235;
  --border2: #243047;
  --accent:  #22d3ee;
  --accent2: #6366f1;
  --accent3: #f59e0b;
  --green:   #10b981;
  --red:     #ef4444;
  --text:    #e2e8f0;
  --muted:   #64748b;
  --muted2:  #94a3b8;
  --font:    'Space Grotesk', sans-serif;
  --mono:    'JetBrains Mono', monospace;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: var(--bg); color: var(--text); font-family: var(--font); font-size: 13px; min-height: 100vh; }}

/* HEADER */
header {{
  background: linear-gradient(160deg, #0a101e 0%, #0e1830 60%, #080e1a 100%);
  border-bottom: 1px solid var(--border);
  padding: 36px 48px 28px;
  position: relative; overflow: hidden;
}}
header::after {{
  content: ''; position: absolute; inset: 0;
  background: radial-gradient(ellipse at 80% 0%, rgba(34,211,238,.07) 0%, transparent 60%),
              radial-gradient(ellipse at 20% 100%, rgba(99,102,241,.06) 0%, transparent 60%);
  pointer-events: none;
}}
.hi {{ position: relative; z-index: 1; }}
.live-badge {{
  display: inline-flex; align-items: center; gap: 8px;
  background: rgba(34,211,238,.08); border: 1px solid rgba(34,211,238,.25);
  color: var(--accent); font-family: var(--mono); font-size: 10px; letter-spacing: .08em;
  padding: 5px 14px; border-radius: 99px; margin-bottom: 16px;
}}
.pulse {{ width: 7px; height: 7px; border-radius: 50%; background: var(--accent); animation: pulse 1.8s ease-in-out infinite; }}
@keyframes pulse {{ 0%,100% {{ transform:scale(1); opacity:1; }} 50% {{ transform:scale(1.6); opacity:.4; }} }}
h1 {{ font-size: 30px; font-weight: 700; color: #fff; letter-spacing: -.5px; margin-bottom: 8px; }}
h1 em {{ color: var(--accent); font-style: normal; }}
.header-sub {{ color: var(--muted2); font-family: var(--mono); font-size: 11px; line-height: 1.7; }}
.stats-grid {{ display: flex; gap: 14px; margin-top: 24px; flex-wrap: wrap; }}
.stat-card {{
  background: rgba(255,255,255,.03); border: 1px solid var(--border2);
  border-radius: 10px; padding: 14px 22px; min-width: 130px;
  transition: border-color .2s, transform .2s;
}}
.stat-card:hover {{ border-color: var(--accent); transform: translateY(-2px); }}
.stat-num {{ font-size: 26px; font-weight: 700; line-height: 1; }}
.c1 {{ color: var(--accent); }} .c2 {{ color: var(--green); }} .c3 {{ color: var(--accent2); }} .c4 {{ color: var(--accent3); }}
.stat-label {{ color: var(--muted); font-size: 11px; margin-top: 4px; }}

/* TOOLBAR */
.toolbar {{
  background: var(--surface); border-bottom: 1px solid var(--border);
  padding: 11px 48px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  position: sticky; top: 0; z-index: 100; backdrop-filter: blur(14px);
}}
.search-box {{
  background: var(--bg); border: 1px solid var(--border2); border-radius: 8px;
  padding: 7px 14px; color: var(--text); font-family: var(--font); font-size: 12px;
  width: 240px; outline: none; transition: border-color .2s, box-shadow .2s;
}}
.search-box:focus {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(34,211,238,.1); }}
.search-box::placeholder {{ color: var(--muted); }}
.filter-btn {{
  background: transparent; border: 1px solid var(--border2); color: var(--muted2);
  padding: 6px 13px; border-radius: 6px; cursor: pointer; font-family: var(--mono);
  font-size: 10px; letter-spacing: .05em; transition: all .2s; white-space: nowrap;
}}
.filter-btn:hover {{ border-color: var(--accent); color: var(--accent); }}
.filter-btn.active {{ background: var(--accent); border-color: var(--accent); color: #000; font-weight: 700; }}
.spacer {{ flex: 1; }}
.sort-select {{
  background: var(--bg); border: 1px solid var(--border2); color: var(--muted2);
  padding: 6px 10px; border-radius: 6px; font-family: var(--mono); font-size: 10px; cursor: pointer; outline: none;
}}
.export-btn {{
  background: var(--green); border: none; color: #000; padding: 7px 16px;
  border-radius: 6px; cursor: pointer; font-family: var(--mono); font-size: 10px;
  font-weight: 700; letter-spacing: .05em; transition: opacity .2s;
}}
.export-btn:hover {{ opacity: .85; }}

/* MAIN */
main {{ padding: 32px 48px; }}
.sec-hdr {{ display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }}
.sec-title {{ font-family: var(--mono); font-size: 10px; letter-spacing: .12em; color: var(--muted); text-transform: uppercase; }}
.sec-line {{ flex: 1; height: 1px; background: var(--border); }}
.sec-count {{ font-family: var(--mono); font-size: 10px; color: var(--muted); background: var(--card); border: 1px solid var(--border); padding: 2px 8px; border-radius: 4px; }}

/* TABLE */
.table-wrap {{ overflow-x: auto; border-radius: 12px; border: 1px solid var(--border); }}
table {{ width: 100%; border-collapse: collapse; }}
thead th {{
  background: var(--surface); padding: 10px 14px; text-align: left;
  font-family: var(--mono); font-size: 9px; letter-spacing: .1em; color: var(--muted);
  text-transform: uppercase; border-bottom: 1px solid var(--border);
  white-space: nowrap; cursor: pointer; user-select: none; transition: color .2s;
}}
thead th:hover {{ color: var(--accent); }}
thead th.sorted {{ color: var(--accent); }}
tbody tr {{ border-bottom: 1px solid var(--border); transition: background .15s; animation: fadeRow .4s ease both; }}
tbody tr:last-child {{ border-bottom: none; }}
tbody tr:hover {{ background: rgba(255,255,255,.025); }}
tbody tr.row-top {{ background: rgba(34,211,238,.025); }}
tbody tr.row-contract {{ background: rgba(245,158,11,.018); }}
@keyframes fadeRow {{ from {{ opacity:0; transform: translateY(4px); }} to {{ opacity:1; transform: translateY(0); }} }}
tbody td {{ padding: 11px 14px; vertical-align: middle; }}

/* CELLS */
.rank-cell  {{ display: flex; align-items: center; gap: 10px; }}
.rank-num   {{ font-family: var(--mono); font-size: 12px; font-weight: 700; color: var(--muted2); min-width: 22px; }}
.score-badge {{ font-family: var(--mono); font-size: 13px; font-weight: 700; color: var(--accent); }}
.score-denom {{ font-size: 10px; color: var(--muted); font-weight: 400; }}

.company-name {{ font-weight: 700; color: #fff; font-size: 13px; }}
.company-size {{ font-size: 10px; color: var(--muted); margin-top: 2px; }}
.role-title {{ color: var(--muted2); font-size: 12px; max-width: 280px; line-height: 1.4; }}

.ptag {{ font-family: var(--mono); font-size: 9px; letter-spacing: .05em; padding: 3px 8px; border-radius: 4px; white-space: nowrap; border: 1px solid; }}
.pg  {{ background: rgba(16,185,129,.12); color: var(--green);   border-color: rgba(16,185,129,.25); }}
.pl  {{ background: rgba(34,211,238,.12); color: var(--accent);  border-color: rgba(34,211,238,.25); }}
.pli {{ background: rgba(99,102,241,.12); color: var(--accent2); border-color: rgba(99,102,241,.25); }}
.pd  {{ background: rgba(245,158,11,.12); color: var(--accent3); border-color: rgba(245,158,11,.25); }}
.pr  {{ background: rgba(239,68,68,.12);  color: #f87171;        border-color: rgba(239,68,68,.25);  }}
.pj  {{ background: rgba(168,85,247,.12); color: #c084fc;        border-color: rgba(168,85,247,.25); }}
.pbi {{ background: rgba(56,189,248,.12); color: #7dd3fc;        border-color: rgba(56,189,248,.25); }}
.pgl {{ background: rgba(14,165,233,.12); color: #38bdf8;        border-color: rgba(14,165,233,.25); }}

.loc-text {{ color: var(--muted2); font-size: 12px; }}
.badge-remote {{
  display: inline-block; margin-top: 3px; font-family: var(--mono); font-size: 9px;
  padding: 1px 7px; border-radius: 3px;
  background: rgba(16,185,129,.12); border: 1px solid rgba(16,185,129,.25); color: var(--green);
}}
.badge-hybrid {{
  background: rgba(245,158,11,.1) !important; border-color: rgba(245,158,11,.25) !important; color: var(--accent3) !important;
}}

.type-tag {{ font-size: 11px; color: var(--muted2); }}

.chips {{ display: flex; flex-wrap: wrap; gap: 4px; max-width: 260px; }}
.chip {{ font-family: var(--mono); font-size: 9px; padding: 2px 7px; border-radius: 4px; background: rgba(255,255,255,.04); border: 1px solid var(--border2); color: var(--muted2); }}
.chip-hi {{ background: rgba(34,211,238,.1); border-color: rgba(34,211,238,.3); color: var(--accent); font-weight: 700; }}
.chip-na {{ color: var(--muted); border-color: var(--border); font-style: italic; }}

.salary    {{ color: var(--green); font-family: var(--mono); font-size: 11px; font-weight: 700; }}
.salary-na {{ color: var(--muted); font-size: 11px; }}
.posted    {{ color: var(--muted); font-size: 11px; }}
.fresh     {{ color: var(--green); }}

.opt {{ font-size: 10px; font-weight: 600; padding: 3px 9px; border-radius: 5px; white-space: nowrap; border: 1px solid; }}
.opt-pos {{ background: rgba(16,185,129,.12); color: var(--green);   border-color: rgba(16,185,129,.25); }}
.opt-neg {{ background: rgba(239,68,68,.1);   color: #f87171;        border-color: rgba(239,68,68,.2);  }}
.opt-neu {{ background: rgba(245,158,11,.1);  color: var(--accent3); border-color: rgba(245,158,11,.2); }}
.opt-unk {{ background: rgba(100,116,139,.1); color: var(--muted2);  border-color: rgba(100,116,139,.2);}}

.apply-btn {{
  display: inline-block; background: var(--accent2); color: #fff; text-decoration: none;
  padding: 6px 14px; border-radius: 6px; font-size: 11px; font-weight: 600;
  transition: opacity .2s, transform .1s; white-space: nowrap;
}}
.apply-btn:hover {{ opacity: .85; transform: translateY(-1px); }}

/* INSIGHTS */
.insights-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 48px; }}
.insight-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 22px 24px; }}
.insight-title {{ font-family: var(--mono); font-size: 10px; letter-spacing: .1em; color: var(--muted); text-transform: uppercase; margin-bottom: 18px; }}

.skill-row {{ display: flex; align-items: center; gap: 10px; margin-bottom: 9px; }}
.skill-lbl {{ font-size: 11px; color: var(--muted2); min-width: 96px; font-family: var(--mono); }}
.skill-bg  {{ flex: 1; height: 5px; background: var(--border2); border-radius: 3px; overflow: hidden; }}
.skill-fill {{ height: 100%; background: linear-gradient(90deg, var(--accent), var(--accent2)); border-radius: 3px; transition: width .7s cubic-bezier(.23,1,.32,1); width: 0%; }}
.skill-n   {{ font-family: var(--mono); font-size: 10px; color: var(--muted); min-width: 22px; text-align: right; }}

/* BOTTOM BAR */
.bottom-bar {{
  display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 16px;
  padding: 20px 24px; background: var(--card); border: 1px solid var(--border);
  border-radius: 12px; margin-top: 24px;
}}
.bb-text h3 {{ font-size: 14px; color: #fff; margin-bottom: 3px; }}
.bb-text p  {{ font-size: 11px; color: var(--muted); }}
.btn-csv {{ background: var(--green); color: #000; border: none; padding: 9px 20px; border-radius: 7px; cursor: pointer; font-family: var(--mono); font-size: 11px; font-weight: 700; transition: opacity .2s; }}
.btn-csv:hover {{ opacity: .85; }}
.disclaimer {{ margin-top: 16px; padding: 14px 18px; background: rgba(239,68,68,.05); border: 1px solid rgba(239,68,68,.15); border-radius: 8px; font-size: 11px; color: var(--muted); line-height: 1.8; }}
.disclaimer strong {{ color: #f87171; }}
#emptyState {{ display: none; text-align: center; padding: 60px 0; color: var(--muted); }}

::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: var(--bg); }}
::-webkit-scrollbar-thumb {{ background: var(--border2); border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: var(--muted); }}

@media (max-width: 768px) {{
  header, .toolbar, main {{ padding-left: 18px; padding-right: 18px; }}
  h1 {{ font-size: 22px; }}
  .insights-grid {{ grid-template-columns: 1fr; }}
  .search-box {{ width: 160px; }}
}}
</style>
</head>
<body>

<header>
<div class="hi">
  <div class="live-badge"><div class="pulse"></div>PIPELINE RUN · {now.strftime('%B %d, %Y').upper()}</div>
  <h1>Data Engineer Jobs — <em>OPT Radar</em></h1>
  <div class="header-sub">
    Resume skills matched: {skills_line}<br>
    Sources: {sources_line}
  </div>
  <div class="stats-grid">
    <div class="stat-card"><div class="stat-num c1" id="sTotal">{total}</div><div class="stat-label">Jobs Found</div></div>
    <div class="stat-card"><div class="stat-num c2" id="sOpt">{opt_n}</div><div class="stat-label">OPT / Visa Friendly</div></div>
    <div class="stat-card"><div class="stat-num c3" id="sRemote">{rem_n}</div><div class="stat-label">Remote Roles</div></div>
    <div class="stat-card"><div class="stat-num c4" id="sContract">{con_n}</div><div class="stat-label">Contract Roles</div></div>
  </div>
</div>
</header>

<div class="toolbar">
  <input class="search-box" id="searchBox" type="text" placeholder="🔍  Search company, role, tech..." oninput="applyFilters()">
  <button class="filter-btn active" onclick="setFilter(this,'all')">ALL</button>
  <button class="filter-btn" onclick="setFilter(this,'remote')">REMOTE</button>
  <button class="filter-btn" onclick="setFilter(this,'contract')">CONTRACT</button>
  <button class="filter-btn" onclick="setFilter(this,'fulltime')">FULL-TIME</button>
  <button class="filter-btn" onclick="setFilter(this,'opt')">OPT FRIENDLY</button>
  <div class="spacer"></div>
  <select class="sort-select" id="sortSelect" onchange="sortBy(this.value)">
    <option value="rank">Sort: Best Match</option>
    <option value="score">Sort: Score ↓</option>
    <option value="company">Sort: Company A–Z</option>
  </select>
  <button class="export-btn" onclick="exportCSV()">⬇ CSV</button>
</div>

<main>

<div class="sec-hdr">
  <div class="sec-title">⭐ Ranked Job Results</div>
  <div class="sec-line"></div>
  <div class="sec-count" id="visibleCount">{total} jobs</div>
</div>

<div class="table-wrap">
<table id="jobTable">
<thead>
  <tr>
    <th onclick="sortBy('rank')" class="sorted">RANK ↕</th>
    <th onclick="sortBy('company')">COMPANY ↕</th>
    <th>ROLE</th>
    <th>SOURCE</th>
    <th>LOCATION</th>
    <th>TYPE</th>
    <th>POSTED</th>
    <th>OPT SIGNAL</th>
    <th>APPLY</th>
  </tr>
</thead>
<tbody id="jobBody">
{rows_html}
</tbody>
</table>
<div id="emptyState">
  <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"
       style="opacity:.3;margin-bottom:12px;display:block;margin-left:auto;margin-right:auto">
    <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
  </svg>
  <p style="color:var(--muted)">No jobs match your filters.</p>
</div>
</div>

<!-- INSIGHTS PANEL -->
<div class="insights-grid" style="margin-top:48px;">
  <div class="insight-card">
    <div class="insight-title">📊 Skill Demand in Results</div>
    {skill_bars}
  </div>
  <div class="insight-card">
    <div class="insight-title">🗂 Jobs by Platform</div>
    <canvas id="platformChart" style="max-height:200px"></canvas>
  </div>
</div>

<!-- EXPORT -->
<div class="bottom-bar">
  <div class="bb-text"><h3>📤 Export Job Data</h3><p>Download as CSV — import into Notion, Google Sheets, or your tracker</p></div>
  <button class="btn-csv" onclick="exportCSV()">⬇ Download CSV</button>
</div>

<div class="disclaimer">
  <strong>⚠ OPT / Visa Notice:</strong> OPT signals are inferred from job posting language and employer type. Always confirm visa sponsorship policy directly with the employer before applying. Consulting and staffing firms are generally more OPT-friendly via W2/C2C arrangements. Positions marked ✓ have explicit positive signals or are known sponsoring firms.
</div>

</main>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script>
const CSV_DATA = {csv_js};
let activeFilter = 'all';

function setFilter(btn, type) {{
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  activeFilter = type;
  applyFilters();
}}

function applyFilters() {{
  const q    = document.getElementById('searchBox').value.toLowerCase();
  const rows = document.querySelectorAll('#jobBody tr');
  let vis = 0;
  rows.forEach(row => {{
    const remote = row.dataset.remote === 'yes';
    const type   = row.dataset.type;
    const opt    = row.dataset.opt === 'pos';
    const text   = row.textContent.toLowerCase();
    let show = true;
    if (activeFilter === 'remote'   && !remote)               show = false;
    if (activeFilter === 'contract' && type !== 'contract')   show = false;
    if (activeFilter === 'fulltime' && type !== 'fulltime')   show = false;
    if (activeFilter === 'opt'      && !opt)                  show = false;
    if (q && !text.includes(q))                               show = false;
    row.style.display = show ? '' : 'none';
    if (show) vis++;
  }});
  document.getElementById('visibleCount').textContent = vis + ' jobs';
  document.getElementById('emptyState').style.display = vis === 0 ? 'block' : 'none';
  updateStats();
}}

function updateStats() {{
  const rows = Array.from(document.querySelectorAll('#jobBody tr')).filter(r => r.style.display !== 'none');
  document.getElementById('sTotal').textContent    = rows.length;
  document.getElementById('sOpt').textContent      = rows.filter(r => r.dataset.opt      === 'pos').length;
  document.getElementById('sRemote').textContent   = rows.filter(r => r.dataset.remote   === 'yes').length;
  document.getElementById('sContract').textContent = rows.filter(r => r.dataset.type     === 'contract').length;
}}

let sortState = {{}};
function sortBy(key) {{
  const tbody = document.getElementById('jobBody');
  const rows  = Array.from(tbody.querySelectorAll('tr'));
  sortState[key] = !sortState[key];
  const asc = sortState[key];
  rows.sort((a, b) => {{
    if (key === 'rank')    return asc ? +a.dataset.rank  - +b.dataset.rank  : +b.dataset.rank  - +a.dataset.rank;
    if (key === 'score')   return asc ? +a.dataset.score - +b.dataset.score : +b.dataset.score - +a.dataset.score;
    if (key === 'company') {{
      const av = a.querySelector('.company-name')?.textContent || '';
      const bv = b.querySelector('.company-name')?.textContent || '';
      return asc ? av.localeCompare(bv) : bv.localeCompare(av);
    }}
    if (key === 'salary') {{
      const parse = el => {{
        const s = el?.querySelector('.salary')?.textContent || '0';
        const m = s.match(/[$][\\d,]+/);
        return m ? parseInt(m[0].replace(/[$,]/g,'')) : 0;
      }};
      return asc ? parse(a) - parse(b) : parse(b) - parse(a);
    }}
    return 0;
  }});
  rows.forEach(r => tbody.appendChild(r));
  document.querySelectorAll('thead th').forEach(th => th.classList.remove('sorted'));
  document.getElementById('sortSelect').value = key;
}}

function exportCSV() {{
  const headers = ['#','Company','Role','Source','Location','Type','Posted','OPT Signal','Apply Link'];
  const rows = Array.from(document.querySelectorAll('#jobBody tr')).filter(r => r.style.display !== 'none');
  const data = rows.map(row => {{
    const cells = row.querySelectorAll('td');
    return [
      cells[0]?.querySelector('.rank-num')?.textContent?.trim() || '',
      cells[1]?.querySelector('.company-name')?.textContent?.trim() || '',
      cells[2]?.querySelector('.role-title')?.textContent?.trim() || '',
      cells[3]?.textContent?.trim() || '',
      cells[4]?.querySelector('.loc-text')?.textContent?.trim() || '',
      cells[5]?.textContent?.trim() || '',
      cells[6]?.textContent?.trim() || '',
      cells[7]?.textContent?.trim() || '',
      cells[8]?.querySelector('a')?.href || '',
    ];
  }});
  const csv = [headers, ...data].map(r => r.map(c => `"${{String(c).replace(/"/g,'""')}}"`).join(',')).join('\\n');
  const a = document.createElement('a');
  a.href     = URL.createObjectURL(new Blob([csv], {{ type: 'text/csv' }}));
  a.download = 'de_jobs_{now.strftime("%Y-%m-%d_%H-%M")}.csv';
  a.click();
}}

// PLATFORM CHART
const ctx = document.getElementById('platformChart').getContext('2d');
new Chart(ctx, {{
  type: 'doughnut',
  data: {{
    labels: {plat_labels},
    datasets: [{{
      data: {plat_values},
      backgroundColor: {plat_bg},
      borderColor:     {plat_border},
      borderWidth: 2, hoverOffset: 6,
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{
      legend: {{ position: 'right', labels: {{ color: '#94a3b8', font: {{ family: 'JetBrains Mono', size: 10 }}, padding: 10, boxWidth: 10 }} }},
      tooltip: {{ callbacks: {{ label: ctx => ` ${{ctx.label}}: ${{ctx.parsed}} jobs` }} }}
    }},
    cutout: '60%',
  }}
}});

// ANIMATE SKILL BARS
window.addEventListener('load', () => {{
  document.querySelectorAll('.skill-fill').forEach((bar, i) => {{
    const target = bar.dataset.w;
    bar.style.width = '0%';
    setTimeout(() => {{ bar.style.width = target; }}, i * 90 + 300);
  }});
}});
</script>
</body>
</html>"""

    path = os.path.join(os.getcwd(), fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(HTML)
    return path, fname

# ══════════════════════════════════════════════════════
# EMAIL SENDER
# ══════════════════════════════════════════════════════

def send_email(df, dashboard_path, fname):
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_TO]):
        print("  [SKIP] Email credentials not configured — skipping email")
        return

    now   = datetime.today()
    total = len(df)
    opt_n = len(df[df["opt_signal"] == "positive"])
    rem_n = int(df["work_mode"].isin(["remote","hybrid"]).sum())

    top5_rows = ""
    for _, row in df.head(5).iterrows():
        opt_txt, _ = opt_label(row.get("opt_signal", "unknown"), row.get("type", ""),
                               company=row.get("company",""), platform=row.get("platform",""))
        sal = str(row.get("salary", "Not Listed") or "Not Listed")
        top5_rows += f"""<tr>
          <td style="padding:10px 14px;border-bottom:1px solid #1a1a1a;color:#fff;font-weight:600">{row['company']}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1a1a1a;color:#94a3b8;font-size:12px">{row['title']}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1a1a1a;color:#10b981;font-family:monospace;font-size:11px">{sal}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1a1a1a;color:#22d3ee;font-family:monospace">{row.get('total_score',0)}pts</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1a1a1a">
            <a href="{row['link']}" style="background:#6366f1;color:#fff;text-decoration:none;padding:5px 12px;border-radius:5px;font-size:11px">Apply →</a>
          </td>
        </tr>"""

    email_html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="background:#000;color:#e2e8f0;font-family:'Segoe UI',sans-serif;margin:0;padding:0">
<div style="max-width:660px;margin:0 auto;padding:40px 24px">
  <div style="background:linear-gradient(135deg,#050a10,#0a0a0a);border:1px solid #1a1a1a;border-radius:14px;padding:32px;margin-bottom:24px">
    <div style="display:inline-flex;align-items:center;gap:8px;background:rgba(34,211,238,.08);border:1px solid rgba(34,211,238,.2);color:#22d3ee;font-family:monospace;font-size:10px;letter-spacing:.08em;padding:4px 12px;border-radius:99px;margin-bottom:16px">
      <span style="width:6px;height:6px;border-radius:50%;background:#22d3ee;display:inline-block"></span>
      PIPELINE RUN · {now.strftime('%B %d, %Y — %I:%M %p').upper()}
    </div>
    <h1 style="font-size:22px;font-weight:700;color:#fff;margin:0 0 6px">Data Engineer Jobs — OPT Radar</h1>
    <p style="color:#4b5563;font-size:12px;font-family:monospace;margin:0">Last 24 h · YOE &lt; 8 · US only · LinkedIn · Indeed · Dice</p>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:24px">
    <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;padding:16px 20px">
      <div style="font-size:26px;font-weight:700;color:#22d3ee">{total}</div>
      <div style="font-size:11px;color:#4b5563;margin-top:3px">Jobs Found</div>
    </div>
    <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;padding:16px 20px">
      <div style="font-size:26px;font-weight:700;color:#10b981">{opt_n}</div>
      <div style="font-size:11px;color:#4b5563;margin-top:3px">OPT Friendly</div>
    </div>
    <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;padding:16px 20px">
      <div style="font-size:26px;font-weight:700;color:#6366f1">{rem_n}</div>
      <div style="font-size:11px;color:#4b5563;margin-top:3px">Remote / Hybrid</div>
    </div>
  </div>
  <div style="background:#0d0d0d;border:1px solid #1a1a1a;border-radius:12px;overflow:hidden;margin-bottom:24px">
    <div style="padding:14px 20px;border-bottom:1px solid #1a1a1a">
      <span style="font-family:monospace;font-size:10px;letter-spacing:.1em;color:#4b5563;text-transform:uppercase">⭐ Top 5 Matched Jobs</span>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr>
        <th style="padding:8px 14px;text-align:left;font-family:monospace;font-size:9px;color:#374151;text-transform:uppercase;border-bottom:1px solid #1a1a1a">Company</th>
        <th style="padding:8px 14px;text-align:left;font-family:monospace;font-size:9px;color:#374151;text-transform:uppercase;border-bottom:1px solid #1a1a1a">Role</th>
        <th style="padding:8px 14px;text-align:left;font-family:monospace;font-size:9px;color:#374151;text-transform:uppercase;border-bottom:1px solid #1a1a1a">Salary</th>
        <th style="padding:8px 14px;text-align:left;font-family:monospace;font-size:9px;color:#374151;text-transform:uppercase;border-bottom:1px solid #1a1a1a">Score</th>
        <th style="padding:8px 14px;text-align:left;font-family:monospace;font-size:9px;color:#374151;text-transform:uppercase;border-bottom:1px solid #1a1a1a">Link</th>
      </tr></thead>
      <tbody>{top5_rows}</tbody>
    </table>
  </div>
  <div style="background:rgba(99,102,241,.06);border:1px solid rgba(99,102,241,.2);border-radius:10px;padding:16px 20px;margin-bottom:24px">
    <p style="color:#9ca3af;font-size:12px;line-height:1.7;margin:0">
      📎 Full dashboard attached as <strong style="color:#fff">{fname}</strong><br>
      Open in any browser to filter, sort, search, and export all {total} jobs.
    </p>
  </div>
  <p style="color:#1f2937;font-size:11px;text-align:center;font-family:monospace">
    Auto-generated · {now.strftime('%Y-%m-%d %H:%M')} · job_pipeline.py
  </p>
</div></body></html>"""

    try:
        msg            = MIMEMultipart("mixed")
        msg["From"]    = EMAIL_SENDER
        msg["To"]      = EMAIL_TO
        msg["Subject"] = f"[Job Radar] {total} DE Jobs · {opt_n} OPT Friendly · {now.strftime('%b %d')}"
        msg.attach(MIMEText(email_html, "html"))

        with open(dashboard_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{fname}"')
            msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo(); server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_TO, msg.as_string())
        print(f"  ✓ Email sent to {EMAIL_TO}")
    except Exception as e:
        print(f"  [EMAIL ERROR] {e}")
        traceback.print_exc()

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════

def run():
    sep = "═" * 56
    print(f"\n{sep}\n  DATA ENGINEER JOB PIPELINE\n  {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}\n{sep}\n")

    print("[1/7] Extracting resume skills …")
    skills = extract_resume_skills()

    print("\n[2/7] Scraping: LinkedIn · Indeed · Dice …")
    jobs = linkedin_jobs() + indeed_jobs() + dice_jobs()
    print(f"\n  Total raw: {len(jobs)} jobs")

    print("\n[3/7] Deduplicating …")
    df = remove_duplicates(pd.DataFrame(jobs))
    df = remove_seen_jobs(df)
    print(f"  After dedup: {len(df)} jobs")

    print("\n[3b/7] Filtering: last 24 h + YOE < 8 + US only …")
    df = df[df["posted"].apply(is_within_24h)]
    df = df[df["location"].apply(is_us_location)]
    df = df[df.apply(lambda r: yoe_ok(r["title"] + " " + str(r.get("description", ""))), axis=1)]
    print(f"  After filters: {len(df)} jobs")

    print("\n[4/7] Scoring against resume …")
    scored               = df.apply(lambda r: score_job(r["title"], str(r.get("description","")), skills), axis=1)
    df["total_score"]    = scored.apply(lambda x: x["total_score"])
    df["matched_skills"] = scored.apply(lambda x: x["matched_skills"])
    df["opt_signal"]     = scored.apply(lambda x: x["opt_signal"])

    print(f"\n[5/7] Filtering score ≥ {MIN_SCORE} …")
    df = df[df["total_score"] >= MIN_SCORE]
    print(f"  Qualifying: {len(df)} jobs")

    print("\n[6/7] Ranking all jobs → top 50 by match score …")
    df = top_jobs(df)
    save_history(df)
    print(f"  Final: {len(df)} jobs")

    print("\n[7/7] Generating dashboard & sending email …")
    path, fname = generate_dashboard(df, resume_skills=skills)
    print(f"  Saved: {fname}")
    send_email(df, path, fname)

    print(f"\n{sep}\n  ✓ DONE — {len(df)} jobs · {fname}\n{sep}\n")

if __name__ == "__main__":
    run()

# ══════════════════════════════════════════════════════
# .env TEMPLATE — create this as .env in your project folder
# ══════════════════════════════════════════════════════
# EMAIL_SENDER=yourgmail@gmail.com
# EMAIL_PASSWORD=xxxx xxxx xxxx xxxx    ← 16-char Gmail App Password
# EMAIL_TO=yourpersonalemail@gmail.com
# SMTP_HOST=smtp.gmail.com
# SMTP_PORT=587
#
# Gmail App Password (NOT your real password):
#   1. myaccount.google.com → Security → 2-Step Verification ON
#   2. Search "App Passwords" → Create → Name it "JobPipeline"
#   3. Copy the 16-char code into EMAIL_PASSWORD above
# ══════════════════════════════════════════════════════
