# 📡 Data Engineer Job Pipeline

A Python automation tool that scrapes Data Engineer job postings from **LinkedIn, Indeed, and Dice** every day, scores them against your resume, filters out mismatched roles, and delivers a ranked **interactive dark-themed HTML dashboard** to your inbox — automatically.

Built specifically for a Data Engineer job search: resume-aware scoring, experience-year filtering, OPT/visa signal detection, and remote/hybrid work mode classification.

---

## 🖥️ Dashboard Preview

The pipeline generates a self-contained HTML file with:
- Ranked job table with match scores (0–100)
- Filter buttons: All · Remote · Contract · Full-Time · OPT Friendly
- Sort by: Best Match · Score · Company A–Z
- Full-text search across all columns
- Skill demand bar charts and platform donut chart
- CSV export of filtered results
- OPT/visa signal per job: ✓ Sponsors · ✗ No Sponsorship · ⚡ Contract Friendly

---

## ⚙️ How It Works

```
[1/7] Extract resume skills from resume.pdf
[2/7] Scrape LinkedIn · Indeed · Dice (last 24h, US only)
[3/7] Deduplicate by URL and company+title
[3b/7] Filter: last 24h + YOE < 8 years + US only
[4/7] Score each job against your resume skills
[5/7] Drop jobs with score < MIN_SCORE (default 3)
[6/7] Rank all qualifying jobs → top 50
[7/7] Generate HTML dashboard + send email
```

---

## 🎯 Skill Scoring System

Skills are extracted from your `resume.pdf` automatically, then matched and weighted:

| Tier | Skills | Points |
|------|--------|--------|
| Tier 1 | PySpark · Databricks · Snowflake · BigQuery · Kafka · Airflow · Delta Lake | 3 pts each |
| Tier 2 | Spark · AWS · Azure · GCP · ETL · dbt · Data Pipeline · Data Warehouse | 2 pts each |
| Tier 3 | Python · SQL | 1 pt each |

**Seniority bonus:** Staff +5 · Principal/Lead +4 · Senior +3

Scores are normalised to 0–100 in the dashboard.

---

## 🔍 Filtering Logic

**24-hour filter** — only jobs posted in the last 24–48 hours

**YOE filter** — removes jobs requiring 8+ years of experience
```
Matches: "8+ years", "10 years experience", "minimum 8 years"
To change: edit yoe_ok() — change < 8 to your threshold
```

**Banned titles** (excluded automatically)
```
intern, junior, entry level, student, graduate,
new grad, associate, apprentice
```

**Required title keywords** (at least one)
```
data engineer, data platform, analytics engineer, etl,
pipeline engineer, spark engineer, databricks,
staff engineer, lead engineer, principal engineer
```

---

## 🧩 OPT / Visa Signal Detection

Each job is tagged with one of four signals:

| Signal | Meaning |
|--------|---------|
| ✓ Sponsors Visas | Explicit H1B/visa language in posting |
| ✗ No Sponsorship | Explicitly says no sponsorship |
| ⚡ Contract/W2 Friendly | Contract or C2C role |
| ⚡ Verify OPT Directly | No clear signal — check manually |

---

## 🗂️ Project Structure

```
de-job-pipeline/
├── main.py              ← Full pipeline (LinkedIn + Indeed + Dice)
├── job_pipeline.py      ← LinkedIn-only version
├── resume.pdf           ← Your resume (required, not committed)
├── .env                 ← Email credentials (required, not committed)
├── .gitignore
├── requirements.txt
└── job_history.csv      ← Auto-created; prevents duplicate alerts
```

---

## 🚀 Setup & Usage

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Place your resume
```bash
# Copy your resume to the project folder
cp /path/to/your/resume.pdf ./resume.pdf
```

### 3. Create `.env` file
```env
EMAIL_SENDER=yourgmail@gmail.com
EMAIL_PASSWORD=xxxx xxxx xxxx xxxx
EMAIL_TO=yourpersonalemail@gmail.com
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
```

> **Gmail App Password** (not your regular password):
> 1. Go to myaccount.google.com → Security
> 2. Enable 2-Step Verification
> 3. Search "App Passwords" → Create → Name it "JobPipeline"
> 4. Copy the 16-character code into `EMAIL_PASSWORD`

### 4. Run
```bash
python main.py
```

### 5. Schedule (optional) — runs at 9 AM and 9 PM daily
```bash
0 9,21 * * * cd /path/to/de-job-pipeline && python3 main.py
```

---

## ⚙️ Configuration

Edit the top of `main.py`:

```python
RESUME_FILE    = "resume.pdf"   # your resume filename
JOB_HISTORY    = "job_history.csv"
MAX_PER_SOURCE = 20             # per-platform cap
FINAL_TOP      = 50             # max jobs in dashboard
MIN_SCORE      = 3              # minimum resume match score
```

---

## 🛠️ Tech Stack

| Category | Tools |
|----------|-------|
| Language | Python 3 |
| HTTP / Scraping | `requests` · `cloudscraper` · `BeautifulSoup4` |
| Data Processing | `pandas` |
| Resume Parsing | `pdfminer.six` |
| Email | `smtplib` · `email` (stdlib) |
| Config | `python-dotenv` |
| Dashboard | Vanilla HTML/CSS/JS · Chart.js |

---

## 🔧 Troubleshooting

**Very few / 0 results**
- LinkedIn and Glassdoor block scrapers. Run from a home network, not a cloud/VPS server.
- Dice API is the most reliable source.

**Resume read failed**
- Ensure `resume.pdf` is in the same folder as the script.
- Use a text-based PDF (Word/Google Docs export). Scanned image PDFs cannot be parsed.
- On failure, the pipeline uses the full default skill list.

**Too many / too few results**
- Lower `MIN_SCORE` (e.g. `1`) for more results; raise (e.g. `5`) for fewer.
- Adjust `MAX_PER_SOURCE` to change the per-platform cap.

**YOE filter too strict**
- Edit `yoe_ok()` — change `< 8` to `< 10` or higher.

**Resetting seen jobs**
- Delete `job_history.csv` to start fresh.

---

## 📄 License

MIT — free to use and modify for personal job searching.
