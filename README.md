# JobsBot

Automatically scrapes **LinkedIn** and top Israeli tech company career pages for Software Engineer / Developer roles and sends a **Telegram** notification for every new posting. Runs serverlessly on AWS — no machine needs to stay on.

---

## Architecture

```
EventBridge (every 23 min)
        ↓
AWS Lambda  ←  main.py (lambda_handler)
        ↓
   run_pipeline()
    ├── scraper/linkedin.py     LinkedIn guest API (no login)
    └── scraper/companies.py   Google, Amazon, Nvidia, Intel, NICE, Unity, Wiz
        ↓
   notifier.py                 Formats + sends to Telegram
        ↓
   db.py  →  DynamoDB          Deduplication (never sends the same job twice)
```

---

## Installation (from scratch)

### Prerequisites

| Tool | Install |
|------|---------|
| Python 3.13+ | https://python.org |
| AWS CLI | https://aws.amazon.com/cli/ |
| SAM CLI | `pip install aws-sam-cli` |
| AWS account | https://aws.amazon.com |

---

### Step 1 — Clone the repo

```bash
git clone <repo-url>
cd JobsBot/jobs_bot
```

---

### Step 2 — Create a Telegram bot

1. Open Telegram → search **@BotFather** → send `/newbot`
2. Follow prompts → copy the **token** (looks like `123456789:ABCdef...`)
3. Send `/start` to your new bot
4. Open in browser: `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
5. Find `"chat": { "id": 123456789 }` → that's your **chat ID**

---

### Step 3 — Configure AWS CLI

```bash
aws configure
```

Enter your AWS access key, secret key, and region (`us-east-1` recommended).

---

### Step 4 — Store secrets in AWS SSM

Run these in your terminal (replace the values):

```bash
aws ssm put-parameter --name "/jobsbot/telegram-bot-token" --value "YOUR_TOKEN"   --type String
aws ssm put-parameter --name "/jobsbot/telegram-chat-id"  --value "YOUR_CHAT_ID" --type String
```

Your secrets are now stored in AWS — never in any file or git history.

---

### Step 5 — Deploy to AWS

```powershell
# Windows
.\deploy.ps1
```

```bash
# Mac / Linux
sam build && sam deploy --stack-name jobsbot --region us-east-1 --capabilities CAPABILITY_IAM --resolve-s3 --no-confirm-changeset
```

First deploy takes ~2 minutes. SAM creates:
- **Lambda function** — runs your bot code
- **DynamoDB table** — tracks seen jobs
- **EventBridge rule** — triggers Lambda every 23 min
- **IAM role** — gives Lambda permission to access DynamoDB

---

### Step 6 — Verify

1. Go to your Lambda in the AWS console:
   `https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/jobsbot`
2. Click **Test** → leave event as `{}` → **Test**
3. Check the Execution results — you should see jobs logged
4. Check Telegram — messages should start arriving

---

### How to redeploy after code changes

```powershell
# Windows
.\deploy.ps1

# Mac / Linux
sam build && sam deploy --stack-name jobsbot --region us-east-1 --capabilities CAPABILITY_IAM --resolve-s3 --no-confirm-changeset
```

---

## Configuration

All settings are environment variables. Set them in:
**AWS Console → Lambda → jobsbot → Configuration → Environment variables**

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | — | **Required.** Your Telegram bot token |
| `TELEGRAM_CHAT_ID` | — | **Required.** Your Telegram chat ID |
| `DYNAMODB_TABLE` | `jobsbot-seen-jobs` | DynamoDB table name |
| `MAX_EXPERIENCE_YEARS` | `4` | Drop jobs requiring more than N years |
| `LINKEDIN_SEARCH_KEYWORDS` | *(14 keywords)* | Comma-separated LinkedIn search terms |
| `INCLUDE_TITLE_TERMS` | *(see config.py)* | Job title must contain one of these |
| `EXCLUDE_TITLE_TERMS` | *(see config.py)* | Job title must contain none of these |
| `TARGET_LOCATIONS` | *(Israeli cities)* | Locations to include (comma-separated) |
| `EXCLUDE_LOCATIONS` | *(Israeli cities)* | Locations to exclude (comma-separated) |

### Examples

Only show senior jobs (5+ years):
```
MAX_EXPERIENCE_YEARS = 10
```

Add React to search keywords:
```
LINKEDIN_SEARCH_KEYWORDS = software developer,backend developer,react developer,full stack developer
```

Only Tel Aviv jobs:
```
TARGET_LOCATIONS = tel aviv,ramat gan,givatayim
```

Exclude additional titles:
```
EXCLUDE_TITLE_TERMS = frontend,devops,qa engineer,data scientist,team lead
```

---

## Local development

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
python main.py
```

Runs once immediately, then exits (no scheduler in local mode — add the schedule loop back from git history if needed).
