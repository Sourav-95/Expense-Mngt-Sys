# Expense Management Pipeline

Automated daily pipeline that reads bank statement files from Google Drive, transforms and enriches the data, and writes monthly reports to Google Sheets — with incremental loading, consolidation, and email notifications.

---

## Architecture Overview

```
INPUT FOLDER (GDrive)
│
├── bank_statement_1.xls
└── bank_statement_2.xls
        │
        ▼
┌─────────────────────┐
│   Layer 1: INGEST   │
│   list + download   │
│   concat → raw_df   │
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│ Layer 2: TRANSFORM  │
│   clean             │
│   standardise       │
│   build uuid key    │
│   → transformed_df  │
└────────┬────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────┐
│              Layer 3: DATE ROUTER                    │
│                                                      │
│   group transformed_df by month from date column     │
│                                                      │
│   for each month group:                              │
│     get_or_create YYYY-MM folder                     │
│     get_or_create GSheet inside folder               │
│     read existing GSheet → existing_df               │
│                                                      │
│     existing_df EMPTY  →  fresh write                │
│     existing_df HAS DATA                             │
│       → anti_join(new, existing)                     │
│       → append delta rows only                       │
└──────────────────────────────────────────────────────┘
         │
         ▼
OUTPUT FOLDER (GDrive)
├── 2026-02/
│   └── 2026-02_raw  (GSheet)
├── 2026-03/
│   └── 2026-03_raw  (GSheet)
└── 2026-04/
    └── 2026-04_raw  (GSheet)
         │
         ▼
┌──────────────────────────┐
│  Semantic Layer          │  ← separate scheduled job
│  read all monthly sheets │
│  concat → master_df      │
│  dedup on uuid           │
│  sort by date            │
│  write → consolidated    │
└──────────────────────────┘
         │
         ▼
DASHBOARD FOLDER (GDrive)
└── consolidated_master  (GSheet)
         │
         ▼
┌─────────────────────────┐
│    notify.py            │
│  SUCCESS / FAILURE mail │
└─────────────────────────┘
```

---

## Project Structure

```
expense-mgt-system/
│
├── .github/
│   └── workflows/
│       ├── daily_pipeline.yml        # cron: 1AM IST daily + manual trigger
│       └── consolidate.yml           # triggers after daily pipeline succeeds
│
├── src/
│   ├── extract/
│   │   ├── extract.py                # reads xls/xlsx → dataframe per file
│   │   └── ingest_orchestrate.py     # downloads all files, concat → raw_df
│   │
│   ├── transformation/
│   │   ├── transformer_ops.py        # clean, standardise, enrich
│   │   ├── transform_orchestrate.py  # per-bank transform pipeline
│   │   └── unique_id_build.py        # uuid key generation
│   │
│   ├── load/
│   │   ├── data_router.py            # Layer 3 date routing + incremental load
│   │   └── consolidate.py            # semantic layer — monthly → master
│   │
│   └── notification/
│       └── notify_email.py           # Gmail SMTP email alerts
│
├── utils/
│   ├── auth.py                       # GCP service account authentication
│   ├── drive_utils.py                # reusable GDrive + GSheet helpers
│   └── logger.py                     # centralised logging — single file per run
│
├── config/
│   ├── constants.py                  # hardcoded values — mime types, folder IDs
│   └── settings.py                   # env-based config — credentials, email
│
├── logs/
│   └── .gitkeep                      # gitignored *.log, folder tracked
│
├── tests/
│   └── test_transform.py
│
├── main.py                           # pipeline orchestrator
├── requirements.txt
├── .env                              # local only — gitignored
├── .gitignore
└── README.md
```

---

## Google Drive Folder Structure

```
ExpenseManagementSystem/          ← share this folder with service account
│
├── inputs/                       # drop bank statement files here
│   ├── hdfc_statement.xls
│   └── axis_statement.xls
│
├── outputs/                      # auto-managed by pipeline
│   ├── 2026-02/
│   │   └── 2026-02_raw           (GSheet)
│   ├── 2026-03/
│   │   └── 2026-03_raw           (GSheet)
│   └── 2026-04/
│       └── 2026-04_raw           (GSheet)
│
└── dashboard/                    # written by consolidate.py
    └── consolidated_master       (GSheet)
```

---

## Key Design Decisions

| Decision | Reason |
|---|---|
| Route by date column, not `datetime.now()` | Handles vacation gaps and month-boundary data correctly |
| UUID from hash(date + particulars + dr + cr) | Deterministic — same row always produces same key |
| Anti-join before append | Prevents duplicates on every incremental run |
| GSheet as live store | Native append, direct dashboard connection, no download cycle |
| Fresh write for consolidation | Always rebuilds master from source — simpler than incremental |
| Consolidation decoupled from daily pipeline | Failure in consolidation doesn't affect daily run |
| Single log file per run | `YYYY-MM_runID_timestamp.log` — all modules write to same file |

---

## Incremental Load Logic

```
Condition 1 — Day 1, data spans two months (Apr + May):
  Apr folder exists → anti-join → append delta
  May folder missing → create folder + GSheet → fresh write

Condition 2 — Day 2+, same month data:
  May folder exists → anti-join → append new rows only

Condition 3 — Vacation gap, stale data spans two months:
  Same as Condition 1 — handled automatically by grouping on date column
```

---

## UUID Key Strategy

```
if transaction_id present AND length >= 5 AND not a stopword (FOR, TO, BY...):
    uuid = transaction_id.strip().upper()
else:
    uuid = SHA256(date | particulars | dr | cr)[:16]
```

---

## Setup

### 1. GCP Service Account

1. Create a GCP project
2. Enable **Google Drive API** and **Google Sheets API**
3. Create a Service Account → generate JSON key
4. Share `ExpenseManagementSystem/` GDrive folder with the service account email as **Editor**

### 2. Gmail App Password

1. Google Account → Security → enable **2-Step Verification**
2. Security → **App Passwords** → create one → copy 16-char password

### 3. Local `.env`

```
SA_KEY_PATH        = /path/to/sa_key.json
GMAIL_FROM         = your@gmail.com
GMAIL_APP_PASSWORD = xxxx xxxx xxxx xxxx
NOTIFY_TO_EMAIL    = recipient@gmail.com
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

### 5. Run locally

```bash
python main.py
```

---

## GitHub Actions Setup

### Secrets to configure

Go to repo → **Settings → Secrets and Variables → Actions** and add:

| Secret | Value |
|---|---|
| `GCP_SA_KEY` | entire JSON content of service account key file |
| `GMAIL_FROM` | your Gmail address |
| `GMAIL_APP_PASSWORD` | 16-char app password |
| `NOTIFY_TO_EMAIL` | recipient email |

### Workflows

| Workflow | Trigger | Script |
|---|---|---|
| `daily_pipeline.yml` | 1:00 AM IST daily + manual | `python main.py` |
| `consolidate.yml` | after daily pipeline succeeds + manual | `python -m src.load.consolidate` |

### Manual trigger

GitHub repo → **Actions tab** → select workflow → **Run workflow**

---

## Logging

Log files are generated per run under `logs/`:

```
logs/2026-04_9876543210_2026-04-29_07-30-00.log
```

Format:
```
2026-04-29 07:30:00 | src.extract.extract          | INFO    | Data loaded: 186 rows
2026-04-29 07:30:01 | src.transformation.transformer_ops | INFO | Transformation complete
2026-04-29 07:30:04 | src.load.data_router         | INFO    | Appended 148 rows
2026-04-29 07:30:05 | src.notification.notify_email | INFO   | Email Notification sent
```

- Locally → written to `logs/` folder
- GitHub Actions → visible in Actions tab under each step

---

## Email Notifications

Sent automatically after every pipeline run:

**Success:**
```
Subject: [expense-pipeline] DAILY | ✅ SUCCESS — 2026-04-29
Pipeline  : DAILY
Status    : SUCCESS
Details   : 209 rows processed
```

**Failure:**
```
Subject: [expense-pipeline] DAILY | ❌ FAILURE — 2026-04-29
Pipeline  : DAILY
Status    : FAILURE
Error     : Missing required columns: ['uuid']
```

---

## Future Enhancements

- [ ] Views layer — Summary / Category / Monthly pivot tabs per GSheet
- [ ] Dashboard in Preset.io / Looker Studio connected to consolidated master
- [ ] Migrate consolidated master to BigQuery for business scale
- [ ] ML categorisation model to reduce MANUAL flags

---

## Stack

| Component | Technology |
|---|---|
| Language | Python 3.11 |
| Data processing | pandas, openpyxl, xlrd |
| Google Drive | google-api-python-client |
| Google Sheets | gspread |
| Authentication | google-auth (service account) |
| Notifications | smtplib (Gmail SMTP) |
| Scheduling | GitHub Actions (cron) |
| Logging | Python logging module |
| Env management | python-dotenv |