# LETZ Data Dashboard

A Streamlit-based analytics dashboard for monitoring user activity, engagement metrics, and product insights. Built to connect to a PostgreSQL database and provide real-time visibility into user journeys.

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Frontend/UI** | [Streamlit](https://streamlit.io/) v1.30+ | Interactive dashboard framework |
| **Database** | PostgreSQL | Data storage (connected via psycopg2) |
| **Data Processing** | Pandas v2.1+ | DataFrame operations and data manipulation |
| **Timezone Handling** | pytz | Converting timestamps to user local time |
| **Config Management** | python-dotenv | Environment variable loading for local dev |
| **Containerization** | Dev Containers | Codespaces/VS Code development environment |

---

## Project Structure

```
├── dashboard.py          # Main application (all dashboard logic)
├── requirements.txt      # Python dependencies
├── .env                  # Database credentials (not in git)
├── .streamlit/
│   └── config.toml       # Streamlit theme and server config
└── .devcontainer/
    └── devcontainer.json # GitHub Codespaces / VS Code Dev Container config
```

---

## Local Development Setup

### Prerequisites
- Python 3.11+
- Access to the PostgreSQL database
- Database credentials

### 1. Clone and create virtual environment

```bash
git clone <repo-url>
cd "Letz data dasboard"

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure database credentials

Create a `.env` file in the project root:

```env
DB_HOST=your-database-host.com
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432
```

### 4. Run the dashboard

```bash
streamlit run dashboard.py
```

The dashboard will be available at `http://localhost:8501`

---

## Deployment Options

### Option 1: Streamlit Community Cloud (Recommended)

1. Push the repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repo and select `dashboard.py`
4. Add database secrets in the Streamlit Cloud dashboard:
   - Go to **App settings** → **Secrets**
   - Add your credentials in TOML format:

```toml
DB_HOST = "your-database-host.com"
DB_NAME = "your_database_name"
DB_USER = "your_username"
DB_PASSWORD = "your_password"
DB_PORT = "5432"
```

The app auto-detects whether to use `st.secrets` (cloud) or `.env` (local).

### Option 2: GitHub Codespaces

The project includes a `.devcontainer/devcontainer.json` that auto-configures a dev environment:

1. Open the repo in GitHub
2. Click **Code** → **Codespaces** → **Create codespace**
3. The container will install dependencies and start the Streamlit server
4. Access via the forwarded port (8501)

For Codespaces, set secrets in your GitHub repo settings under **Secrets and variables** → **Codespaces**.

### Option 3: Docker / Self-Hosted

```bash
# Build and run with Docker
docker build -t letz-dashboard .
docker run -p 8501:8501 --env-file .env letz-dashboard
```

Or deploy to any platform that supports Python (Heroku, Railway, Render, etc.).

---

## Configuration

### Streamlit Theme (`.streamlit/config.toml`)

The dashboard uses a dark theme with teal accent colors:

```toml
[theme]
primaryColor = "#00d4aa"        # Teal accent
backgroundColor = "#0e1117"     # Dark background
secondaryBackgroundColor = "#1a1f2e"
textColor = "#ffffff"

[server]
headless = true                 # Required for cloud deployment
```

### Authentication

The dashboard has basic password protection (session-based). Credentials are **not** stored in code. Set them via:

- **Streamlit Cloud:** App settings → Secrets → add `AUTH_USERNAME` and `AUTH_PASSWORD`
- **Local:** add `AUTH_USERNAME` and `AUTH_PASSWORD` to your `.env` file

Never commit credentials to the repo.

---

## Dashboard Features

### Tab 1: Quick Insights
- **Metric cards**: Total users, new signups (today/week), active users, outside-24h-window count
- **User Journey Progress**: Onboarding completion, added slogan, activity completion, settings updated
- **Recovery Ladder**: Conversion rates (24h/72h), time-to-reactivation, ladder step drop-off
- **Recent Messages**: Live feed of the latest 10 messages with JSON payload parsing
- **All Users Table**: Expandable list with last activity, recovery template count, and CSV export

### Tab 2: User Deep Dive
- Select any user from dropdown
- **Engagement metrics**: XP, last activity, messages sent (24h/3d/7d), recovery sends
- **Activity Plan Calendar**: Weekly view of scheduled activities
- **Message History**: Full conversation log for the selected user

### Sidebar
- **Database Explorer**: Browse all tables and view their schemas
- **Auto-refresh toggle**: Clears cached data every 60 seconds when enabled

---

## Customizing Queries

All predefined queries live in the `QUERIES` dictionary in `dashboard.py` (~line 182). To add or modify:

```python
QUERIES = {
    "📊 My New Query": """
        SELECT column1, column2
        FROM my_table
        WHERE condition = true
        ORDER BY created_at DESC
        LIMIT 100
    """,
    # ... existing queries
}
```

Query results are displayed as interactive DataFrames with sorting and filtering.

---

## Key Implementation Details

### Database Connection
- Uses `@st.cache_resource` for connection pooling (single connection reused across reruns)
- Auto-detects `st.secrets` (cloud) vs `.env` (local)
- Includes connection health check with automatic reconnection on failure

### Timezone Handling
The dashboard parses various timezone formats stored in the `users.timezone` column:
- Named timezones: `"America/Sao_Paulo"`, `"Europe/London"`
- UTC offsets: `"UTC-3"`, `"GMT+5:30"`, `"-3"`

Timestamps are converted to users' local time for display.

### 24-Hour Active Window for Check-ins
**Important**: Check-ins are only sent to users who have been active within the last 24 hours. 

**Definition of "Active"**: A user is considered active if they have either:
- Sent a user message (`messages.sender = 'user'`), OR
- Completed an activity (`user_activities_history.completed_at`)

**Analyst Note**: If you see users not receiving check-ins despite having `check_in_time` set and `skip_check_in = false`, check their last user activity timestamp. They are likely outside the 24h active window.

### Message JSON Parsing
The `messages.message` column contains nested JSON payloads. The dashboard recursively extracts readable text from structures like:
- `{"flows": {"body": {"text": "..."}}}`
- `{"quickReply": {"body": {"text": "..."}}}`
- `{"postback": {"payload": {"text": "..."}}}`
- `{"interactive": {"button_reply": {"title": "..."}}}`

### User Deduplication
Users may have duplicate rows in the database. All queries use `COUNT(DISTINCT waid)` or `DISTINCT ON (waid)` to ensure accurate counts.

### User Retention tab — Lifetime and New users
In the **User Retention** tab, **Lifetime (days)** is **days since first activity through today** (in São Paulo time), not the calendar span between first and last activity. So:
- **New users (&lt; 7 days)**: users whose first message was fewer than 7 calendar days ago.
- **Established (7+ days)**: users whose first message was at least 7 days ago.
Spot checks: compute `today - first_active_date` in the same timezone (America/Sao_Paulo) and it should match the dashboard.

---

## Security Notes

- **Never commit `.env` or `.streamlit/secrets.toml`** — both are in `.gitignore`
- Database credentials should only be stored in environment variables or Streamlit secrets
- Auth is loaded from Streamlit secrets or `.env` only (never hardcoded); consider stronger auth for sensitive data

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Database connection failed" | Check `.env` credentials and network access to the DB host |
| Stale data showing | Click the auto-refresh checkbox or manually clear cache with `st.cache_resource.clear()` |
| Timezone conversion errors | Some offset formats may not parse; check the `parse_timezone()` function |
| Streamlit Cloud deploy fails | Ensure `requirements.txt` has all dependencies with compatible versions |

---

## Version History

- **v1.1** — Added Recovery Ladder metrics, user deep dive tab, activity calendar, template detection
- **v1.0** — Initial release with quick insights and basic user queries
