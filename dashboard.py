"""
LETZ Data Dashboard
Simple dashboard for viewing user activities and product insights.
"""

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
import re
import html
import pytz
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

try:
    from deep_translator import GoogleTranslator  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    GoogleTranslator = None

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="LETZ Dashboard",
    page_icon="📊",
    layout="wide"
)

# Simple password protection (per-session)
# Read from Streamlit secrets (production) or .env file (local development)
# NEVER hardcode credentials in the code - they must be set via secrets or .env!
try:
    # Try Streamlit secrets first (for cloud deployment)
    if hasattr(st, 'secrets') and 'AUTH_USERNAME' in st.secrets:
        AUTH_USERNAME = st.secrets["AUTH_USERNAME"]
        AUTH_PASSWORD = st.secrets["AUTH_PASSWORD"]
    else:
        # Fall back to .env file (for local development)
        AUTH_USERNAME = os.getenv("AUTH_USERNAME")
        AUTH_PASSWORD = os.getenv("AUTH_PASSWORD")
except Exception:
    # Final fallback to .env
    AUTH_USERNAME = os.getenv("AUTH_USERNAME")
    AUTH_PASSWORD = os.getenv("AUTH_PASSWORD")

# Security check: credentials must be set
if not AUTH_USERNAME or not AUTH_PASSWORD:
    st.error("""
    **Configuration Error**: Authentication credentials are not set.
    
    Please configure credentials in one of the following ways:
    
    **For Streamlit Cloud:**
    - Go to Settings → Secrets
    - Add: `AUTH_USERNAME` and `AUTH_PASSWORD`
    
    **For Local Development:**
    - Add `AUTH_USERNAME` and `AUTH_PASSWORD` to your `.env` file
    """)
    st.stop()

if "auth" not in st.session_state:
    st.session_state.auth = {"logged_in": False}

def render_login():
    st.title("LETZ Dashboard Login")
    with st.form("login_form"):
        user = st.text_input("Username")
        pwd = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")
        if submitted:
            if user == AUTH_USERNAME and pwd == AUTH_PASSWORD:
                st.session_state.auth["logged_in"] = True
                st.success("Logged in")
                try:
                    st.rerun()
                except Exception:
                    st.experimental_rerun()
            else:
                st.error("Invalid credentials")

# Force login before showing the app
if not st.session_state.auth.get("logged_in"):
    render_login()
    st.stop()

# Logout control
with st.sidebar:
    if st.button("Logout"):
        st.session_state.auth["logged_in"] = False
        st.experimental_rerun()

# Custom CSS for a clean look
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #00d4aa;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #888;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #252b3b 100%);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid #2d3748;
    }
    .sql-editor {
        font-family: 'Monaco', 'Menlo', monospace;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #2d3748;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_connection():
    """Create database connection. Supports both local .env and Streamlit Cloud secrets."""
    try:
        # Try Streamlit secrets first (for cloud deployment)
        try:
            if hasattr(st, 'secrets') and 'DB_HOST' in st.secrets:
                conn = psycopg2.connect(
                    host=st.secrets["DB_HOST"],
                    database=st.secrets["DB_NAME"],
                    user=st.secrets["DB_USER"],
                    password=st.secrets["DB_PASSWORD"],
                    port=st.secrets.get("DB_PORT", "5432"),
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                return conn
        except:
            pass  # No secrets.toml, fall back to .env
        
        # Fall back to .env
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432"),
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL query and return results as DataFrame."""
    last_error = None
    for attempt in range(2):
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()

        try:
            # Reset failed transactions and verify the cached connection is usable.
            conn.rollback()
            return pd.read_sql_query(query, conn)
        except Exception as e:
            last_error = e
            try:
                get_connection.clear()
            except Exception:
                st.cache_resource.clear()
            if attempt == 0:
                continue

    st.error(f"Query failed after reconnect: {last_error}")
    return pd.DataFrame()


def get_table_list() -> list:
    """Get list of all tables in the database."""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name;
    """
    df = run_query(query)
    return df['table_name'].tolist() if not df.empty else []


def get_table_schema(table_name: str) -> pd.DataFrame:
    """Get schema for a specific table using parameterized query."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        conn.rollback()
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        return pd.read_sql_query(query, conn, params=(table_name,))
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.cache_resource.clear()
        return pd.DataFrame()


def load_internal_users():
    """
    Load internal users from JSON file. Returns list of WAIDs to exclude.

    Primary source (tracked in this repo):
        .context/internal-users.json  (next to dashboard.py)

    Legacy fallback (for backwards compatibility):
        ../.context/internal-users.json

    Fallback:
        Hardcoded WAID list (to avoid silently disabling the filter if the JSON
        file is missing or malformed).
    """
    fallback_internal_waids = [
        '555198161419', '5511988649591', '555195455326',
        '555397038122', '5511970544995', '6593366209', '555199885544',
        '5512981257941'
    ]

    try:
        base_dir = os.path.dirname(__file__)

        # 1) Preferred location: .context/internal-users.json inside this repo
        repo_context_path = os.path.join(base_dir, ".context", "internal-users.json")

        # 2) Legacy location: ../.context/internal-users.json (older setup)
        legacy_context_path = os.path.join(base_dir, "..", ".context", "internal-users.json")

        for candidate_path in [repo_context_path, legacy_context_path]:
            if os.path.exists(candidate_path):
                with open(candidate_path, "r") as f:
                    internal_users_data = json.load(f)
                    waids = [user.get("waid") for user in internal_users_data.get("internal_users", [])]
                    # Filter out any Nones / empty strings
                    waids = [w for w in waids if w]
                    if waids:
                        return waids
        # If file doesn't exist or is empty, fall back to hardcoded list
        return fallback_internal_waids
    except Exception:
        # On any error, still fall back to hardcoded list so the filter
        # continues to work instead of silently doing nothing.
        return fallback_internal_waids


def load_investor_waids():
    """
    Load investor WAIDs from JSON file. Users with these WAIDs are flagged [investor] in dashboard.
    Primary location (repo-local):
        .context/special-users.json   (next to dashboard.py)
    Legacy fallback (older setup):
        ../.context/special-users.json
    """
    try:
        base_dir = os.path.dirname(__file__)
        for candidate in [
            os.path.join(base_dir, ".context", "special-users.json"),
            os.path.join(base_dir, "..", ".context", "special-users.json"),
        ]:
            if os.path.exists(candidate):
                with open(candidate, "r") as f:
                    data = json.load(f)
                    waids = data.get("investor_waids", [])
                    return [str(w).strip() for w in waids if w]
        return []
    except Exception:
        return []


def _normalize_waid(waid):
    """Return a clean WAID string, or empty if missing/invalid."""
    if waid is None or (isinstance(waid, float) and pd.isna(waid)):
        return ""
    waid_str = str(waid).strip()
    if not waid_str or waid_str.lower() in ("nan", "none"):
        return ""
    if waid_str.endswith(".0"):
        waid_str = waid_str[:-2]
    return waid_str


def is_investor_waid(waid):
    waid_str = _normalize_waid(waid)
    if not waid_str:
        return False
    return waid_str in load_investor_waids()


def parse_db_user_tags(raw_tags):
    """Parse users.tags into a list of tag strings."""
    if isinstance(raw_tags, list):
        return [str(t).strip() for t in raw_tags if str(t).strip()]
    if raw_tags is None or (isinstance(raw_tags, float) and pd.isna(raw_tags)):
        return []
    if isinstance(raw_tags, str):
        raw_str = raw_tags.strip()
        if not raw_str:
            return []
        try:
            parsed = json.loads(raw_str)
            if isinstance(parsed, list):
                return [str(t).strip() for t in parsed if str(t).strip()]
        except Exception:
            pass
        return [raw_str]
    tag_str = str(raw_tags).strip()
    return [tag_str] if tag_str else []


def format_user_tags_column(raw_tags, waid=None):
    """Combine investor flag and database user tags for table display."""
    tags = []
    if is_investor_waid(waid):
        tags.append("investor")
    tags.extend(parse_db_user_tags(raw_tags))
    return ", ".join(tags) if tags else "—"


def format_display_name(name, waid=None, tag="investor", user_id=None):
    """
    Append [investor] tag to name if user's WAID is in special-users.json.
    When the user has no name, show "Unknown [<user_id>]" if a user_id is available.
    """
    name_is_empty = (
        pd.isna(name)
        or name is None
        or str(name).strip() == ""
        or str(name).strip().lower() == "unknown"
    )
    if name_is_empty:
        base_name = "Unknown"
        if user_id is not None and not (isinstance(user_id, float) and pd.isna(user_id)):
            uid_str = str(user_id).strip()
            if uid_str.endswith(".0"):
                uid_str = uid_str[:-2]
            if uid_str:
                base_name = f"Unknown [{uid_str}]"
    else:
        base_name = str(name).strip()

    waid_str = _normalize_waid(waid)
    if not waid_str:
        return base_name
    if waid_str in load_investor_waids():
        return f"{base_name} [{tag}]"
    return base_name


def _append_user_id(display_name, user_id):
    """Append [<user_id>] to a display name. No-op if user_id is missing or already present."""
    if user_id is None or (isinstance(user_id, float) and pd.isna(user_id)):
        return display_name
    uid_str = str(user_id).strip()
    if uid_str.endswith(".0"):
        uid_str = uid_str[:-2]
    if not uid_str or f"[{uid_str}]" in str(display_name):
        return display_name
    return f"{display_name} [{uid_str}]"


def format_display_name_with_tags(name, waid=None, tags=None, user_id=None):
    """Format display name and append user tags (e.g. [dotz])."""
    base_name = format_display_name(name, waid, user_id=user_id)
    if tags is None or (isinstance(tags, float) and pd.isna(tags)):
        return base_name

    parsed_tags = []
    if isinstance(tags, list):
        parsed_tags = [str(t).strip() for t in tags if str(t).strip()]
    elif isinstance(tags, str):
        raw = tags.strip()
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    parsed_tags = [str(t).strip() for t in parsed if str(t).strip()]
                else:
                    parsed_tags = [raw]
            except Exception:
                parsed_tags = [raw]
    else:
        tag_str = str(tags).strip()
        if tag_str:
            parsed_tags = [tag_str]

    if not parsed_tags:
        return base_name
    return f"{base_name} [{' | '.join(parsed_tags)}]"


def render_wrapped_messages_table(df: pd.DataFrame) -> None:
    """Render message rows as a wrapped HTML table for screenshots."""
    if df.empty:
        return

    display_df = df.fillna("").copy()
    columns = list(display_df.columns)
    weights = {
        "Time": 1.2,
        "User": 1.8,
        "Tag": 1.2,
        "From": 1,
        "Status": 1,
        "Type": 0.8,
        "Message": 4,
        "Message (EN)": 4,
    }
    total_weight = sum(weights.get(col, 1) for col in columns) or 1
    colgroup = "".join(
        f"<col style='width: {weights.get(col, 1) / total_weight * 100:.2f}%;'>"
        for col in columns
    )

    header_html = "".join(f"<th>{html.escape(str(col))}</th>" for col in columns)
    rows_html = []
    for _, row in display_df.iterrows():
        failed_class = " failed-row" if str(row.get("Status", "")).strip().lower() == "failed" else ""
        cells = "".join(
            f"<td>{html.escape(str(row.get(col, '')), quote=False)}</td>"
            for col in columns
        )
        rows_html.append(f"<tr class='{failed_class}'>{cells}</tr>")

    table_html = f"""
<style>
.wrapped-message-table {{
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-size: 0.9rem;
}}
.wrapped-message-table th,
.wrapped-message-table td {{
    border: 1px solid rgba(250, 250, 250, 0.15);
    padding: 0.45rem 0.55rem;
    vertical-align: top;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    word-break: break-word;
}}
.wrapped-message-table th {{
    background: rgba(250, 250, 250, 0.08);
    font-weight: 700;
}}
.wrapped-message-table .failed-row td {{
    background-color: rgba(239, 68, 68, 0.16);
}}
</style>
<table class="wrapped-message-table">
    <colgroup>{colgroup}</colgroup>
    <thead><tr>{header_html}</tr></thead>
    <tbody>{''.join(rows_html)}</tbody>
</table>
"""
    st.markdown(table_html, unsafe_allow_html=True)


def get_internal_users_filter_sql(exclude_internal: bool = True) -> str:
    """Generate SQL filter clause to exclude internal users.
    
    Args:
        exclude_internal: If True, returns WHERE clause to exclude internal users.
                         If False, returns empty string (no filter).
    
    Returns:
        SQL WHERE clause string (e.g., "WHERE waid NOT IN (...)")
    """
    if not exclude_internal:
        return ""
    
    internal_waids = load_internal_users()
    if not internal_waids:
        return ""
    
    # Format as SQL array
    internal_waids_str = "', '".join(internal_waids)
    return f"WHERE waid NOT IN ('{internal_waids_str}')"


def get_internal_users_filter_join_sql(exclude_internal: bool = True, table_alias: str = "u") -> str:
    """Generate SQL JOIN/WHERE clause to exclude internal users when joining with users table.
    
    Args:
        exclude_internal: If True, returns clause to exclude internal users.
                         If False, returns empty string (no filter).
        table_alias: The alias used for the users table (default: "u")
    
    Returns:
        SQL WHERE clause string (e.g., "AND u.waid NOT IN (...)")
    """
    if not exclude_internal:
        return ""
    
    internal_waids = load_internal_users()
    if not internal_waids:
        return ""
    
    # Format as SQL array
    internal_waids_str = "', '".join(internal_waids)
    return f"AND {table_alias}.waid NOT IN ('{internal_waids_str}')"


USER_VISIBLE_MESSAGE_EXCLUDED_TYPES = (
    "think",
    "tool_use",
    "tool_result",
    "turn_audit",
    "weekly_digest",
)


def get_user_visible_message_filter_sql(table_alias: str = "m") -> str:
    """Exclude agent trace rows that were not actually sent to the user."""
    excluded_types = "', '".join(USER_VISIBLE_MESSAGE_EXCLUDED_TYPES)
    return f"AND LOWER(COALESCE({table_alias}.type, '')) NOT IN ('{excluded_types}')"


PLAN_PROPOSAL_MESSAGE_CONDITION = """
(
    m.message ILIKE '%O coach enviou uma imagem com o plano semanal. Atividades:%'
    OR m.message ILIKE '%The coach sent an image with the weekly plan. Activities:%'
    OR m.message ILIKE '%activity plan proposal%'
    OR m.message ILIKE '%plano de atividade%'
    OR m.message ILIKE '%plano de atividades%'
    OR (
        m.message ILIKE '%aqui está o plano da%semana%'
        AND (m.message ILIKE '%aprova%' OR m.message ILIKE '%approve%')
    )
)
"""


def get_beta_users_cte() -> str:
    """
    Beta users are users who accepted an activity plan or received a coach plan proposal.
    Accepted plans are represented by user_activities rows.
    """
    return f"""
WITH beta_users AS (
    SELECT DISTINCT u.id, u.waid
    FROM users u
    WHERE EXISTS (
        SELECT 1
        FROM user_activities ua
        WHERE ua.user_id = u.id
    )
    OR EXISTS (
        SELECT 1
        FROM messages m
        WHERE m.sender = 'companion'
          AND m.message IS NOT NULL
          AND (m.user_id = u.id OR m.waid = u.waid)
          AND {PLAN_PROPOSAL_MESSAGE_CONDITION}
    )
)
"""


def get_onboarded_users_cte() -> str:
    """Users who completed onboarding (ai_companion_flows type=onboarding, is_complete=true)."""
    return """
WITH onboarded_users AS (
    SELECT DISTINCT u.id, u.waid
    FROM users u
    WHERE EXISTS (
        SELECT 1
        FROM ai_companion_flows acf
        WHERE acf.user_id = u.id
          AND acf.type = 'onboarding'
          AND acf.is_complete = 'true'
    )
)
"""


@st.cache_data(ttl=300)
def get_quick_insights_headline_metrics(exclude_internal: bool = True) -> pd.DataFrame:
    """
    Fetch the top Quick Insights KPIs in one round trip.

    Onboarded users = completed onboarding flow (ai_companion_flows type onboarding,
    is_complete true). All headline counts use that cohort as the denominator.
    """
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    onboarded_users_cte = get_onboarded_users_cte()
    query = f"""
{onboarded_users_cte},
filtered_onboarded_users AS (
    SELECT DISTINCT u.id, u.waid, u.is_active, u.created_at, COALESCE(u.active_days, 0)::int AS active_days
    FROM onboarded_users ou
    JOIN users u ON u.id = ou.id
    WHERE 1 = 1
      {internal_filter_join}
),
latest_farewell AS (
    SELECT DISTINCT ON (rl.user_id)
        rl.user_id,
        rl.sent_at AS farewell_at
    FROM recovery_logs rl
    JOIN filtered_onboarded_users fou ON fou.id = rl.user_id
    WHERE rl.ladder_step = 'farewell'
      AND rl.sent_at >= NOW() - INTERVAL '7 days'
    ORDER BY rl.user_id, rl.sent_at DESC
),
churn_status AS (
    SELECT
        lf.user_id,
        EXISTS (
            SELECT 1
            FROM messages m
            JOIN users u2 ON u2.id = lf.user_id
            WHERE (m.user_id = u2.id OR m.waid = u2.waid)
              AND m.sender = 'user'
              AND m.sent_at > lf.farewell_at
        ) AS came_back
    FROM latest_farewell lf
),
recent_user_messages AS (
    SELECT
        COUNT(DISTINCT fou.waid) FILTER (
            WHERE m.sent_at >= NOW() - INTERVAL '24 hours'
        ) AS inside_24h,
        COUNT(DISTINCT fou.waid) FILTER (
            WHERE m.sent_at >= CURRENT_DATE
        ) AS messaged_today
    FROM filtered_onboarded_users fou
    JOIN messages m ON m.sender = 'user' AND (m.user_id = fou.id OR m.waid = fou.waid)
    WHERE m.sent_at >= NOW() - INTERVAL '24 hours'
),
completed_today AS (
    SELECT COUNT(DISTINCT uah.user_id) AS completed_today
    FROM user_activities_history uah
    JOIN filtered_onboarded_users fou ON fou.id = uah.user_id
    WHERE uah.completed_at >= CURRENT_DATE
      AND uah.completed_at < CURRENT_DATE + INTERVAL '1 day'
),
at_risk_5d AS (
    SELECT COUNT(DISTINCT fou.waid) AS at_risk_5d_count
    FROM filtered_onboarded_users fou
    WHERE fou.is_active = true
      AND NOT EXISTS (
        SELECT 1
        FROM messages m
        WHERE m.sender = 'user'
          AND (m.user_id = fou.id OR m.waid = fou.waid)
          AND m.sent_at >= NOW() - INTERVAL '5 days'
      )
),
active_users_5d AS (
    SELECT COUNT(DISTINCT fou.waid) AS active_users_5d_count
    FROM filtered_onboarded_users fou
    WHERE fou.is_active = true
      AND EXISTS (
        SELECT 1
        FROM user_activities_history uah
        WHERE uah.user_id = fou.id
          AND uah.completed_at IS NOT NULL
          AND uah.completed_at >= NOW() - INTERVAL '5 days'
      )
),
active_days_alive AS (
    SELECT
        ROUND(AVG(fou.active_days), 1) AS avg_active_days,
        SUM(fou.active_days) AS total_active_days
    FROM filtered_onboarded_users fou
    WHERE fou.is_active = true
),
active_days_at_risk AS (
    SELECT
        ROUND(AVG(fou.active_days), 1) AS avg_active_days,
        SUM(fou.active_days) AS total_active_days
    FROM filtered_onboarded_users fou
    WHERE fou.is_active = true
      AND NOT EXISTS (
        SELECT 1
        FROM messages m
        WHERE m.sender = 'user'
          AND (m.user_id = fou.id OR m.waid = fou.waid)
          AND m.sent_at >= NOW() - INTERVAL '5 days'
      )
),
active_days_churned AS (
    SELECT
        ROUND(AVG(fou.active_days), 1) AS avg_active_days,
        SUM(fou.active_days) AS total_active_days
    FROM churn_status cs
    JOIN filtered_onboarded_users fou ON fou.id = cs.user_id
    WHERE NOT cs.came_back
),
active_days_churned_lifetime AS (
    SELECT
        ROUND(AVG(fou.active_days), 1) AS avg_active_days,
        COUNT(fou.id) AS churned_count,
        SUM(fou.active_days) AS total_active_days
    FROM filtered_onboarded_users fou
    WHERE fou.is_active = false
)
SELECT
    (SELECT COUNT(DISTINCT waid) FROM filtered_onboarded_users) AS onboarded_users_count,
    (SELECT COUNT(DISTINCT waid) FROM filtered_onboarded_users WHERE is_active = true) AS alive_count,
    (SELECT COUNT(DISTINCT waid) FROM filtered_onboarded_users WHERE created_at >= NOW() - INTERVAL '7 days') AS new_7d_count,
    COALESCE((SELECT COUNT(*) FROM churn_status WHERE NOT came_back), 0) AS churned_7d_count,
    COALESCE((SELECT COUNT(*) FROM churn_status WHERE came_back), 0) AS churned_7d_came_back,
    COALESCE((SELECT inside_24h FROM recent_user_messages), 0) AS inside_24h,
    COALESCE((SELECT messaged_today FROM recent_user_messages), 0) AS messaged_today,
    COALESCE((SELECT completed_today FROM completed_today), 0) AS completed_today,
    COALESCE((SELECT at_risk_5d_count FROM at_risk_5d), 0) AS at_risk_5d_count,
    COALESCE((SELECT active_users_5d_count FROM active_users_5d), 0) AS active_users_5d_count,
    COALESCE((SELECT avg_active_days FROM active_days_alive), 0) AS alive_avg_active_days,
    COALESCE((SELECT total_active_days FROM active_days_alive), 0) AS alive_total_active_days,
    COALESCE((SELECT avg_active_days FROM active_days_at_risk), 0) AS at_risk_avg_active_days,
    COALESCE((SELECT total_active_days FROM active_days_at_risk), 0) AS at_risk_total_active_days,
    COALESCE((SELECT avg_active_days FROM active_days_churned), 0) AS churned_avg_active_days,
    COALESCE((SELECT total_active_days FROM active_days_churned), 0) AS churned_total_active_days,
    COALESCE((SELECT avg_active_days FROM active_days_churned_lifetime), 0) AS churned_lifetime_avg_active_days,
    COALESCE((SELECT total_active_days FROM active_days_churned_lifetime), 0) AS churned_lifetime_total_active_days,
    COALESCE((SELECT churned_count FROM active_days_churned_lifetime), 0) AS churned_lifetime_count
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_dau_metrics(exclude_internal: bool = True) -> pd.DataFrame:
    """
    Daily Active Users (DAU) for Quick Insights.
    Active = distinct users with ≥1 activity completion (user_activities_history) on that day.
    MAU = distinct users with ≥1 activity completion in the rolling 30-day window ending on that day.

    Returns three row_types in one DataFrame:
      'daily'       — last 14 days (for last-7d vs prev-7d metric comparison)
      'ratio_daily' — last 84 days with daily rolling DAU/MAU trend
      'weekly'      — last 12 weeks with avg_dau per week
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
base_users AS (
  SELECT u.id
  FROM users u
  WHERE u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
),
activity_events AS (
  SELECT DISTINCT
    uah.user_id,
    (uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date AS d
  FROM user_activities_history uah
  JOIN base_users bu ON bu.id = uah.user_id
  WHERE (uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date
        BETWEEN (now() AT TIME ZONE 'America/Sao_Paulo')::date - 113
            AND (now() AT TIME ZONE 'America/Sao_Paulo')::date - 1
),
daily_completions AS (
  SELECT
    d,
    COUNT(DISTINCT user_id) AS dau
  FROM activity_events
  GROUP BY d
),
days_84 AS (
  SELECT gs::date AS d
  FROM generate_series(
    (now() AT TIME ZONE 'America/Sao_Paulo')::date - 84,
    (now() AT TIME ZONE 'America/Sao_Paulo')::date - 1,
    interval '1 day'
  ) AS gs
),
daily_activity AS (
  SELECT
    d.d,
    COALESCE(dc.dau, 0)::numeric AS dau,
    COUNT(DISTINCT ae.user_id)::numeric AS rolling_mau_30d
  FROM days_84 d
  LEFT JOIN daily_completions dc ON dc.d = d.d
  LEFT JOIN activity_events ae ON ae.d BETWEEN d.d - 29 AND d.d
  GROUP BY d.d, dc.dau
),
days_14 AS (
  SELECT gs::date AS d
  FROM generate_series(
    (now() AT TIME ZONE 'America/Sao_Paulo')::date - 14,
    (now() AT TIME ZONE 'America/Sao_Paulo')::date - 1,
    interval '1 day'
  ) AS gs
),
daily_series AS (
  SELECT
    d.d::text AS activity_date,
    da.dau,
    da.rolling_mau_30d AS mau,
    NULL::numeric AS mau_prev,
    'daily'::text AS row_type
  FROM days_14 d
  JOIN daily_activity da ON da.d = d.d
),
ratio_daily_series AS (
  SELECT
    d::text AS activity_date,
    dau,
    rolling_mau_30d AS mau,
    NULL::numeric AS mau_prev,
    'ratio_daily'::text AS row_type
  FROM daily_activity
),
weeks AS (
  SELECT gs::date AS week_start
  FROM generate_series(
    date_trunc('week', (now() AT TIME ZONE 'America/Sao_Paulo')::date - 84)::date,
    date_trunc('week', (now() AT TIME ZONE 'America/Sao_Paulo')::date),
    interval '7 days'
  ) AS gs
),
weekly_series AS (
  SELECT
    TO_CHAR(w.week_start, 'YYYY-MM-DD') AS activity_date,
    ROUND(AVG(COALESCE(dc.dau, 0)), 1) AS dau,
    NULL::numeric AS mau,
    NULL::numeric AS mau_prev,
    'weekly'::text AS row_type
  FROM weeks w
  CROSS JOIN LATERAL (
    SELECT gs::date AS d
    FROM generate_series(w.week_start, (w.week_start + 6)::date, interval '1 day') AS gs
  ) dow
  LEFT JOIN daily_activity dc ON dc.d = dow.d
  GROUP BY w.week_start
)
SELECT activity_date, dau, mau, mau_prev, row_type FROM daily_series
UNION ALL
SELECT activity_date, dau, mau, mau_prev, row_type FROM ratio_daily_series
UNION ALL
SELECT activity_date, dau, mau, mau_prev, row_type FROM weekly_series
ORDER BY row_type, activity_date
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_quick_insights_engagement_table(exclude_internal: bool = True) -> pd.DataFrame:
    """
    Investor-facing engagement comparison for Quick Insights.

    Rows: DAU, avg rolling 30-day MAU, DAU/MAU, 7D/14D/30D retention.
    Columns: last 7 completed local days vs previous 7 completed local days
    (America/Sao_Paulo; today excluded for DAU/MAU).

    Retention (first-activity definition): among onboarded external users who
    completed a first activity and have matured into the window.
    - 7D: second activity within 7 days of first
    - 14D / 30D: any subsequent activity after day 14 / 30 from first
    """
    internal_waids = load_internal_users() if exclude_internal else []
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
params AS (
  SELECT
    CURRENT_TIMESTAMP AS as_of_ts,
    (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date AS as_of_local_date
),
report_days AS (
  SELECT 'Last 7D' AS period,
         generate_series(p.as_of_local_date - 7, p.as_of_local_date - 1, INTERVAL '1 day')::date AS local_date
  FROM params p
  UNION ALL
  SELECT 'Previous 7D',
         generate_series(p.as_of_local_date - 14, p.as_of_local_date - 8, INTERVAL '1 day')::date
  FROM params p
),
retention_snapshots AS (
  SELECT 'Current' AS period, as_of_ts AS snapshot_ts FROM params
  UNION ALL
  SELECT 'Previous 7D', as_of_ts - INTERVAL '7 days' FROM params
),
onboarded AS (
  SELECT u.id, MIN(acf.updated_at) AS onboarded_at
  FROM users u
  JOIN ai_companion_flows acf
    ON acf.user_id = u.id
   AND acf.type = 'onboarding'
   AND acf.is_complete = 'true'
  WHERE u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  GROUP BY u.id
),
activities AS (
  SELECT uah.user_id, uah.completed_at,
         (uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM user_activities_history uah
  JOIN onboarded o ON o.id = uah.user_id
  WHERE uah.completed_at >= o.onboarded_at
),
daily_dau AS (
  SELECT d.period, d.local_date, COUNT(DISTINCT a.user_id) AS dau
  FROM report_days d
  LEFT JOIN activities a ON a.local_date = d.local_date
  GROUP BY d.period, d.local_date
),
daily_mau AS (
  SELECT d.period, d.local_date, COUNT(DISTINCT a.user_id) AS mau
  FROM report_days d
  LEFT JOIN activities a
    ON a.local_date BETWEEN d.local_date - 29 AND d.local_date
  GROUP BY d.period, d.local_date
),
activity_summary AS (
  SELECT
    dd.period,
    ROUND(AVG(dd.dau), 1) AS avg_dau,
    ROUND(AVG(dm.mau), 1) AS avg_mau,
    ROUND(100.0 * AVG(dd.dau) / NULLIF(AVG(dm.mau), 0), 1) AS dau_mau_pct
  FROM daily_dau dd
  JOIN daily_mau dm
    ON dm.period = dd.period
   AND dm.local_date = dd.local_date
  GROUP BY dd.period
),
first_activity AS (
  SELECT s.period, s.snapshot_ts, o.id, MIN(a.completed_at) AS first_completed_at
  FROM retention_snapshots s
  JOIN onboarded o ON o.onboarded_at <= s.snapshot_ts
  JOIN activities a
    ON a.user_id = o.id
   AND a.completed_at <= s.snapshot_ts
  GROUP BY s.period, s.snapshot_ts, o.id
),
repeat_retention AS (
  SELECT
    f.period,
    f.snapshot_ts,
    f.id,
    f.first_completed_at,
    MAX((a.completed_at <= f.first_completed_at + INTERVAL '7 days')::int) = 1 AS repeat_within_7d,
    MAX((a.completed_at > f.first_completed_at + INTERVAL '14 days')::int) = 1 AS repeat_after_14d,
    MAX((a.completed_at > f.first_completed_at + INTERVAL '30 days')::int) = 1 AS repeat_after_30d
  FROM first_activity f
  LEFT JOIN activities a
    ON a.user_id = f.id
   AND a.completed_at > f.first_completed_at
   AND a.completed_at <= f.snapshot_ts
  GROUP BY f.period, f.snapshot_ts, f.id, f.first_completed_at
),
retention_summary AS (
  SELECT
    period,
    1 AS sort_order,
    '7D retention' AS metric,
    COUNT(*) FILTER (WHERE repeat_within_7d) AS numerator,
    COUNT(*) AS denominator,
    ROUND(
      100.0 * COUNT(*) FILTER (WHERE repeat_within_7d) / NULLIF(COUNT(*), 0),
      1
    ) AS rate
  FROM repeat_retention
  WHERE first_completed_at <= snapshot_ts - INTERVAL '7 days'
  GROUP BY period
  UNION ALL
  SELECT
    period,
    2,
    '14D retention',
    COUNT(*) FILTER (WHERE repeat_after_14d),
    COUNT(*),
    ROUND(
      100.0 * COUNT(*) FILTER (WHERE repeat_after_14d) / NULLIF(COUNT(*), 0),
      1
    )
  FROM repeat_retention
  WHERE first_completed_at <= snapshot_ts - INTERVAL '14 days'
  GROUP BY period
  UNION ALL
  SELECT
    period,
    3,
    '30D retention',
    COUNT(*) FILTER (WHERE repeat_after_30d),
    COUNT(*),
    ROUND(
      100.0 * COUNT(*) FILTER (WHERE repeat_after_30d) / NULLIF(COUNT(*), 0),
      1
    )
  FROM repeat_retention
  WHERE first_completed_at <= snapshot_ts - INTERVAL '30 days'
  GROUP BY period
),
metric_rows AS (
  SELECT
    1 AS sort_order,
    'DAU (avg. daily active users)' AS metric,
    'count' AS value_type,
    MAX(avg_dau) FILTER (WHERE period = 'Last 7D') AS current_value,
    MAX(avg_dau) FILTER (WHERE period = 'Previous 7D') AS previous_value,
    NULL::numeric AS current_numerator,
    NULL::numeric AS current_denominator,
    NULL::numeric AS previous_numerator,
    NULL::numeric AS previous_denominator
  FROM activity_summary
  UNION ALL
  SELECT
    2,
    'MAU (avg. rolling 30-day MAU)',
    'count',
    MAX(avg_mau) FILTER (WHERE period = 'Last 7D'),
    MAX(avg_mau) FILTER (WHERE period = 'Previous 7D'),
    NULL, NULL, NULL, NULL
  FROM activity_summary
  UNION ALL
  SELECT
    3,
    'DAU / MAU',
    'pct',
    MAX(dau_mau_pct) FILTER (WHERE period = 'Last 7D'),
    MAX(dau_mau_pct) FILTER (WHERE period = 'Previous 7D'),
    NULL, NULL, NULL, NULL
  FROM activity_summary
  UNION ALL
  SELECT
    3 + sort_order,
    metric,
    'retention',
    MAX(rate) FILTER (WHERE period = 'Current'),
    MAX(rate) FILTER (WHERE period = 'Previous 7D'),
    MAX(numerator) FILTER (WHERE period = 'Current'),
    MAX(denominator) FILTER (WHERE period = 'Current'),
    MAX(numerator) FILTER (WHERE period = 'Previous 7D'),
    MAX(denominator) FILTER (WHERE period = 'Previous 7D')
  FROM retention_summary
  GROUP BY metric, sort_order
)
SELECT
  sort_order,
  metric,
  value_type,
  current_value,
  previous_value,
  current_numerator,
  current_denominator,
  previous_numerator,
  previous_denominator
FROM metric_rows
ORDER BY sort_order
"""
    return run_query(query)


def get_llm_cost_base_cte(exclude_internal: bool = True) -> str:
    """
    Base CTE for Stack B LLM turn-level cost analysis (messages.type = 'turn_audit').

    "real_users" = Stack B users who have sent at least one message themselves
    (sender = 'user'). Per-user cost stats should only be computed over this
    population so "ghost" users -- who only ever received proactive/automated
    companion sends that happened to trigger an LLM call -- don't distort the
    denominator (or numerator) for avg/median/p25/p75 metrics.
    """
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    return f"""
WITH real_users AS (
    SELECT DISTINCT u.id AS user_id, u.waid, u.timezone, u.created_at
    FROM users u
    WHERE u.stack_variant = 'B'
      {internal_filter_join}
      AND EXISTS (
          SELECT 1 FROM messages m
          WHERE m.sender = 'user' AND (m.user_id = u.id OR m.waid = u.waid)
      )
),
llm_cost_base AS (
    SELECT
        ta.id AS turn_audit_id,
        ru.user_id,
        ta.sent_at AS date_time_utc,
        ta.sent_at AT TIME ZONE COALESCE(ru.timezone, 'America/Sao_Paulo') AS date_time_local,
        (
            (ta.sent_at AT TIME ZONE COALESCE(ru.timezone, 'America/Sao_Paulo'))::date
            - (ru.created_at AT TIME ZONE COALESCE(ru.timezone, 'America/Sao_Paulo'))::date
            + 1
        ) AS user_life_day,
        (ta.message::jsonb ->> 'usd')::numeric AS usd_cost
    FROM messages ta
    JOIN real_users ru ON ru.user_id = ta.user_id
    WHERE ta.type = 'turn_audit'
      AND ta.message IS NOT NULL
      AND ta.message LIKE '{{%'
)
"""


@st.cache_data(ttl=300)
def get_llm_cost_headline_metrics(exclude_internal: bool = True) -> pd.DataFrame:
    """Rolling current-7d vs prior-7d LLM cost totals and distinct real users with cost."""
    base_cte = get_llm_cost_base_cte(exclude_internal)
    query = f"""
{base_cte}
SELECT
    COALESCE(SUM(usd_cost) FILTER (WHERE date_time_utc >= NOW() - INTERVAL '7 days'), 0) AS current_7d_total,
    COALESCE(SUM(usd_cost) FILTER (WHERE date_time_utc >= NOW() - INTERVAL '14 days' AND date_time_utc < NOW() - INTERVAL '7 days'), 0) AS prior_7d_total,
    COUNT(DISTINCT user_id) FILTER (WHERE date_time_utc >= NOW() - INTERVAL '7 days') AS current_7d_users,
    COUNT(DISTINCT user_id) FILTER (WHERE date_time_utc >= NOW() - INTERVAL '14 days' AND date_time_utc < NOW() - INTERVAL '7 days') AS prior_7d_users
FROM llm_cost_base
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_llm_cost_weekly_trend(exclude_internal: bool = True) -> pd.DataFrame:
    """
    Per-real-user weekly LLM cost distribution (avg / median / p25 / p75), bucketed
    by local calendar week (Mon-Sun). Only fully completed weeks are included so the
    trend isn't skewed by a partial in-progress week.
    """
    base_cte = get_llm_cost_base_cte(exclude_internal)
    query = f"""
{base_cte},
weekly_user_cost AS (
    SELECT
        user_id,
        date_trunc('week', date_time_local)::date AS week_start,
        SUM(usd_cost) AS weekly_usd
    FROM llm_cost_base
    GROUP BY user_id, date_trunc('week', date_time_local)::date
)
SELECT
    week_start,
    COUNT(DISTINCT user_id) AS users,
    ROUND(AVG(weekly_usd)::numeric, 4) AS avg_usd,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY weekly_usd))::numeric, 4) AS median_usd,
    ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY weekly_usd))::numeric, 4) AS p25_usd,
    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY weekly_usd))::numeric, 4) AS p75_usd,
    ROUND(SUM(weekly_usd)::numeric, 2) AS total_usd
FROM weekly_user_cost
WHERE week_start < date_trunc('week', NOW() AT TIME ZONE 'America/Sao_Paulo')::date
GROUP BY week_start
ORDER BY week_start
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_llm_cost_by_life_day(exclude_internal: bool = True) -> pd.DataFrame:
    """Per-real-user LLM cost by day of user life (day 1 = signup day, local tz)."""
    base_cte = get_llm_cost_base_cte(exclude_internal)
    query = f"""
{base_cte},
per_user_day AS (
    SELECT user_id, user_life_day, SUM(usd_cost) AS daily_usd
    FROM llm_cost_base
    WHERE user_life_day >= 1
    GROUP BY user_id, user_life_day
)
SELECT
    user_life_day,
    COUNT(DISTINCT user_id) AS users_active,
    ROUND(AVG(daily_usd)::numeric, 4) AS avg_usd,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_usd))::numeric, 4) AS median_usd,
    ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY daily_usd))::numeric, 4) AS p25_usd,
    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY daily_usd))::numeric, 4) AS p75_usd,
    ROUND(SUM(daily_usd)::numeric, 2) AS total_usd
FROM per_user_day
GROUP BY user_life_day
ORDER BY user_life_day
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_user_journey_progress_metrics(exclude_internal: bool = True) -> pd.DataFrame:
    """Fetch current and previous 7-day journey funnel metrics in one query."""
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    beta_users_cte = get_beta_users_cte()
    query = f"""
{beta_users_cte},
cohort_users AS (
    SELECT
        u.id,
        u.metadata,
        u.onboarding_timestamp,
        CASE
            WHEN u.created_at >= NOW() - INTERVAL '7 days' THEN 'current'
            ELSE 'previous'
        END AS cohort,
        CASE
            WHEN u.created_at >= NOW() - INTERVAL '7 days' THEN NOW() - INTERVAL '7 days'
            ELSE NOW() - INTERVAL '14 days'
        END AS window_start,
        CASE
            WHEN u.created_at >= NOW() - INTERVAL '7 days' THEN NOW()
            ELSE NOW() - INTERVAL '7 days'
        END AS window_end
    FROM users u
    JOIN beta_users bu ON bu.id = u.id
    WHERE u.created_at >= NOW() - INTERVAL '14 days'
      {internal_filter_join}
),
journey_flags AS (
    SELECT
        cu.cohort,
        cu.id,
        (
            (cu.onboarding_timestamp IS NOT NULL
             AND cu.onboarding_timestamp >= cu.window_start
             AND cu.onboarding_timestamp < cu.window_end)
            OR EXISTS (
                SELECT 1
                FROM events e
                WHERE e.user_id = cu.id
                  AND e.event_type = 'onboarding_completed'
                  AND e.executed_at >= cu.window_start
                  AND e.executed_at < cu.window_end
            )
        ) AS completed_onboarding,
        (
            NULLIF(cu.metadata->>'mantra', '') IS NOT NULL
            OR EXISTS (
                SELECT 1
                FROM ai_companion_flows acf
                WHERE acf.user_id = cu.id
                  AND acf.type = 'post_onboarding'
                  AND NULLIF(acf.content->>'slogan', '') IS NOT NULL
            )
        ) AS added_slogan,
        EXISTS (
            SELECT 1
            FROM user_activities_history uah
            WHERE uah.user_id = cu.id
              AND uah.completed_at >= cu.window_start
              AND uah.completed_at < cu.window_end
        ) AS completed_activity,
        EXISTS (
            SELECT 1
            FROM messages m
            WHERE m.user_id = cu.id
              AND m.sender = 'user'
              AND m.type = 'audio'
              AND m.sent_at >= cu.window_start
              AND m.sent_at < cu.window_end
        ) AS sent_audio,
        EXISTS (
            SELECT 1
            FROM messages m
            WHERE m.user_id = cu.id
              AND m.sender = 'user'
              AND m.type IN ('image', 'photo')
              AND m.sent_at >= cu.window_start
              AND m.sent_at < cu.window_end
        ) AS sent_picture
    FROM cohort_users cu
)
SELECT
    cohort,
    COUNT(*) AS total_users,
    COUNT(*) FILTER (WHERE completed_onboarding) AS completed_onboarding,
    COUNT(*) FILTER (WHERE added_slogan) AS added_slogan,
    COUNT(*) FILTER (WHERE completed_activity) AS completed_activity,
    COUNT(*) FILTER (WHERE sent_audio) AS sent_audio,
    COUNT(*) FILTER (WHERE sent_picture) AS sent_picture
FROM journey_flags
GROUP BY cohort
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_recovery_ladder_quick_metrics(exclude_internal: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch Recovery Ladder metrics for beta users only."""
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    beta_users_cte = get_beta_users_cte()
    metrics_query = f"""
{beta_users_cte},
filtered_beta_users AS (
    SELECT DISTINCT u.id, u.waid
    FROM beta_users bu
    JOIN users u ON u.id = bu.id
    WHERE 1 = 1
      {internal_filter_join}
),
recovery_sends AS (
    SELECT
        r.id,
        r.user_id,
        r.sent_at,
        CASE
            WHEN r.sent_at >= NOW() - INTERVAL '7 days' THEN 'current'
            ELSE 'previous'
        END AS cohort
    FROM recovery_logs r
    JOIN filtered_beta_users fbu ON fbu.id = r.user_id
    WHERE r.sent_at >= NOW() - INTERVAL '14 days'
),
first_reply AS (
    SELECT
        rs.id,
        rs.user_id,
        rs.sent_at,
        rs.cohort,
        MIN(m.sent_at) AS reply_at
    FROM recovery_sends rs
    LEFT JOIN messages m
      ON m.user_id = rs.user_id
     AND m.sender = 'user'
     AND m.sent_at > rs.sent_at
    GROUP BY rs.id, rs.user_id, rs.sent_at, rs.cohort
),
send_counts AS (
    SELECT
        cohort,
        user_id,
        COUNT(*) AS send_count
    FROM recovery_sends
    GROUP BY cohort, user_id
),
multi_counts AS (
    SELECT
        cohort,
        COUNT(*) FILTER (WHERE send_count >= 2) AS users_2_plus,
        COUNT(*) FILTER (WHERE send_count >= 3) AS users_3_plus
    FROM send_counts
    GROUP BY cohort
)
SELECT
    fr.cohort,
    COUNT(DISTINCT fr.user_id) AS total_users,
    COUNT(DISTINCT fr.user_id) FILTER (
        WHERE fr.reply_at > fr.sent_at AND fr.reply_at <= fr.sent_at + INTERVAL '24 hours'
    ) AS conv24_users,
    COUNT(DISTINCT fr.user_id) FILTER (
        WHERE fr.reply_at > fr.sent_at AND fr.reply_at <= fr.sent_at + INTERVAL '72 hours'
    ) AS conv72_users,
    AVG(EXTRACT(EPOCH FROM (fr.reply_at - fr.sent_at))) FILTER (WHERE fr.reply_at IS NOT NULL) / 3600 AS avg_hours,
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (fr.reply_at - fr.sent_at)) / 3600
    ) FILTER (WHERE fr.reply_at IS NOT NULL) AS median_hours,
    COALESCE(MAX(mc.users_2_plus), 0) AS users_2_plus,
    COALESCE(MAX(mc.users_3_plus), 0) AS users_3_plus
FROM first_reply fr
LEFT JOIN multi_counts mc ON mc.cohort = fr.cohort
GROUP BY fr.cohort
"""
    dropoff_query = f"""
{beta_users_cte},
filtered_beta_users AS (
    SELECT DISTINCT u.id, u.waid
    FROM beta_users bu
    JOIN users u ON u.id = bu.id
    WHERE 1 = 1
      {internal_filter_join}
),
recovery_sends AS (
    SELECT
        r.id,
        r.ladder_step,
        COALESCE(r.template_name, 'Unknown') AS template_name,
        r.user_id,
        r.sent_at
    FROM recovery_logs r
    JOIN filtered_beta_users fbu ON fbu.id = r.user_id
    WHERE r.sent_at >= NOW() - INTERVAL '7 days'
),
conversions AS (
    SELECT DISTINCT r.id
    FROM recovery_sends r
    JOIN messages m ON m.user_id = r.user_id
        AND m.sender = 'user'
        AND m.sent_at > r.sent_at
)
SELECT
    r.ladder_step,
    r.template_name,
    COUNT(*) AS sends,
    COUNT(c.id) AS conversions,
    ROUND(100.0 * COUNT(c.id) / COUNT(*), 1) AS conversion_pct
FROM recovery_sends r
LEFT JOIN conversions c ON r.id = c.id
GROUP BY r.ladder_step, r.template_name
ORDER BY r.ladder_step, sends DESC
LIMIT 50
"""
    return run_query(metrics_query), run_query(dropoff_query)


@st.cache_data(ttl=300)
def get_message_delivery_detail() -> pd.DataFrame:
    """
    Message delivery (due vs received) for today, yesterday, day-before in America/Sao_Paulo.
    One row per (user, ref_date) with due_morning/evening, received_morning/evening, missed_morning/evening.
    Follows analysis/.context/dashboard-message-delivery-instructions.md.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"

    query = f"""
WITH ref_dates AS (
  SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) AS ref_date, 'today' AS period
  UNION ALL SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) - 1, 'yesterday'
  UNION ALL SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) - 2, 'day_before'
),
internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
grid AS (
  SELECT u.id, u.waid, u.full_name, u.check_in_time, u.daily_digest_time, u.timezone, u.skip_check_in, u.skip_daily_digest,
    r.ref_date, r.period,
    ((r.ref_date + u.check_in_time)::timestamp AT TIME ZONE COALESCE(NULLIF(TRIM(u.timezone), ''), 'UTC')) AS check_in_utc,
    ((r.ref_date + u.daily_digest_time)::timestamp AT TIME ZONE COALESCE(NULLIF(TRIM(u.timezone), ''), 'UTC')) AS digest_utc
  FROM users u
  CROSS JOIN ref_dates r
  WHERE u.is_active = true AND u.onboarding_timestamp IS NOT NULL
  AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  AND NOT EXISTS (
    SELECT 1 FROM reschedule rs
    WHERE rs.user_id = u.id
    AND rs.start_time < ((r.ref_date + 1)::timestamp AT TIME ZONE COALESCE(NULLIF(TRIM(u.timezone), ''), 'UTC'))
    AND rs.end_time > (r.ref_date::timestamp AT TIME ZONE COALESCE(NULLIF(TRIM(u.timezone), ''), 'UTC'))
  )
),
last_user AS (
  SELECT g.*,
    (SELECT MAX(m.sent_at) FROM messages m WHERE (m.user_id = g.id OR m.waid = g.waid) AND m.sender = 'user' AND m.sent_at <= g.check_in_utc) AS last_user_checkin,
    (SELECT MAX(m.sent_at) FROM messages m WHERE (m.user_id = g.id OR m.waid = g.waid) AND m.sender = 'user' AND m.sent_at <= g.digest_utc) AS last_user_digest
  FROM grid g
),
activity AS (
  SELECT lu.*,
    EXISTS (
      SELECT 1 FROM user_activities ua
      WHERE ua.user_id = lu.id AND ua.in_progress = true
      AND ua.days @> to_jsonb(trim(to_char(lu.ref_date, 'Day'))::text)
    ) AS has_activity
  FROM last_user lu
),
afk AS (
  SELECT a.*,
    (a.last_user_checkin IS NULL OR a.last_user_checkin < a.check_in_utc - interval '24 hours') AS afk_24h_checkin,
    (a.last_user_digest IS NULL OR a.last_user_digest < a.digest_utc - interval '24 hours') AS afk_24h_digest,
    (a.last_user_checkin >= a.check_in_utc - interval '48 hours' AND a.last_user_checkin < a.check_in_utc - interval '24 hours') AS in_24_48h_checkin
  FROM activity a
),
due_flags AS (
  SELECT af.*,
    (af.has_activity AND NOT COALESCE(af.skip_check_in, false) AND ( (NOT af.afk_24h_checkin) OR (af.afk_24h_checkin AND af.in_24_48h_checkin) )) AS due_morning,
    (af.has_activity AND NOT COALESCE(af.skip_daily_digest, false) AND NOT (af.afk_24h_checkin AND af.in_24_48h_checkin) AND NOT af.afk_24h_digest) AS due_evening
  FROM afk af
),
received AS (
  SELECT d.*,
    EXISTS (SELECT 1 FROM messages m WHERE (m.user_id = d.id OR m.waid = d.waid) AND m.sender = 'companion' AND m.sent_at >= d.check_in_utc - interval '1 hour' AND m.sent_at < d.check_in_utc + interval '1 hour') AS received_morning,
    EXISTS (SELECT 1 FROM messages m WHERE (m.user_id = d.id OR m.waid = d.waid) AND m.sender = 'companion' AND m.sent_at >= d.digest_utc - interval '1 hour' AND m.sent_at < d.digest_utc + interval '1 hour') AS received_evening
  FROM due_flags d
)
SELECT
  TO_CHAR(r.ref_date, 'YYYY-MM-DD') AS ref_date,
  r.period,
  r.check_in_time,
  r.daily_digest_time,
  r.id AS user_id,
  r.waid,
  r.full_name,
  r.due_morning,
  r.due_evening,
  r.received_morning,
  r.received_evening,
  (r.due_morning AND NOT r.received_morning AND (r.period <> 'today' OR CURRENT_TIMESTAMP > r.check_in_utc + interval '10 minutes')) AS missed_morning,
  (r.due_evening AND NOT r.received_evening AND (r.period <> 'today' OR CURRENT_TIMESTAMP > r.digest_utc + interval '10 minutes')) AS missed_evening
FROM received r
ORDER BY r.ref_date, r.check_in_time, r.full_name;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_onboarding_dropoff_detail() -> pd.DataFrame:
    """
    Onboarding drop-off for today, yesterday, day_before in America/Sao_Paulo.
    Two issue types: dropped_off_onboarding (messaged but no onboarding_completed), no_slogan (completed onboarding on ref_date but no slogan set).
    Returns ref_date, period, waid, full_name (null for drop-off), issue_type. Excludes internal users.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"

    query = f"""
WITH ref_dates AS (
  SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) AS ref_date, 'today' AS period
  UNION ALL SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) - 1, 'yesterday'
  UNION ALL SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) - 2, 'day_before'
),
internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
-- Dropped off at onboarding: messaged on ref_date but (no user row or onboarding_timestamp IS NULL); onboarding_started = first user message
dropped_off AS (
  SELECT
    r.ref_date,
    r.period,
    m.waid,
    NULL::varchar AS full_name,
    'dropped_off_onboarding' AS issue_type,
    (SELECT MIN(m2.sent_at) FROM messages m2 WHERE m2.waid = m.waid AND m2.sender = 'user') AS onboarding_started_at,
    NULL::timestamptz AS onboarding_completed_at,
    COALESCE(NULLIF(TRIM(u.timezone), ''), 'UTC') AS user_timezone
  FROM messages m
  CROSS JOIN ref_dates r
  LEFT JOIN users u ON (u.waid = m.waid OR (m.user_id IS NOT NULL AND u.id = m.user_id))
  WHERE m.sender = 'user'
    AND (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date = r.ref_date
    AND (u.id IS NULL OR u.onboarding_timestamp IS NULL)
    AND m.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  GROUP BY r.ref_date, r.period, m.waid, u.timezone
),
-- No slogan: completed onboarding on ref_date but no post_onboarding slogan set (not just active on ref_date)
no_slogan AS (
  SELECT DISTINCT
    r.ref_date,
    r.period,
    u.waid,
    u.full_name,
    'no_slogan' AS issue_type,
    NULL::timestamptz AS onboarding_started_at,
    u.onboarding_timestamp AS onboarding_completed_at,
    COALESCE(NULLIF(TRIM(u.timezone), ''), 'UTC') AS user_timezone
  FROM users u
  CROSS JOIN ref_dates r
  WHERE u.onboarding_timestamp IS NOT NULL
    AND (u.onboarding_timestamp AT TIME ZONE 'America/Sao_Paulo')::date = r.ref_date
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
    AND NOT EXISTS (
      SELECT 1 FROM ai_companion_flows acf
      WHERE acf.user_id = u.id AND acf.type = 'post_onboarding'
        AND acf.content->>'slogan' IS NOT NULL
    )
)
SELECT TO_CHAR(d.ref_date, 'YYYY-MM-DD') AS ref_date, d.period, d.waid, d.full_name, d.issue_type,
  d.onboarding_started_at,
  d.onboarding_completed_at,
  d.user_timezone
FROM dropped_off d
UNION ALL
SELECT TO_CHAR(n.ref_date, 'YYYY-MM-DD') AS ref_date, n.period, n.waid, n.full_name, n.issue_type,
  n.onboarding_started_at,
  n.onboarding_completed_at,
  n.user_timezone
FROM no_slogan n
ORDER BY ref_date, issue_type, waid;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_pending_reply_detail() -> pd.DataFrame:
    """
    Users whose last message has not received a companion reply within 1 hour.
    Once they receive a reply, they drop off this list. Returns waid, full_name, last_sent_at, last_message (raw).
    Excludes internal users.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"

    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
last_user_msg AS (
  SELECT DISTINCT ON (m.waid)
    m.waid,
    m.user_id,
    m.sent_at AS last_sent_at,
    m.message AS last_message
  FROM messages m
  WHERE m.sender = 'user'
    AND m.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  ORDER BY m.waid, m.sent_at DESC
),
no_reply_after AS (
  SELECT lum.waid, lum.user_id, lum.last_sent_at, lum.last_message
  FROM last_user_msg lum
  WHERE NOT EXISTS (
    SELECT 1 FROM messages m2
    WHERE m2.sender = 'companion'
      AND (m2.waid = lum.waid OR (lum.user_id IS NOT NULL AND m2.user_id = lum.user_id))
      AND m2.sent_at > lum.last_sent_at
  )
  AND (CURRENT_TIMESTAMP - lum.last_sent_at) > interval '1 hour'
)
SELECT
  n.waid,
  COALESCE(u.full_name, '—') AS full_name,
  n.last_sent_at,
  n.last_message
FROM no_reply_after n
LEFT JOIN users u ON (u.waid = n.waid OR (n.user_id IS NOT NULL AND u.id = n.user_id))
ORDER BY n.last_sent_at ASC;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_late_stage_recovery_alert_detail() -> pd.DataFrame:
    """
    Users in late-stage recovery (about to churn / just churned) who received a send
    on today, yesterday, or day_before (America/Sao_Paulo). One row per (user, period).
    Excludes internal users.

    Late-stage rungs (new day_* scheme + legacy + farewell):
      - day_5_recovery    — ≤3 cohort, final recovery message before farewell
      - day_20_recovery   — >3 cohort, final recovery push (was lose_score)
      - recovery_ladder_2 — legacy penultimate step (kept during rollout)
      - farewell          — marked inactive
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"

    query = f"""
WITH ref_dates AS (
  SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) AS ref_date, 'today' AS period
  UNION ALL SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) - 1, 'yesterday'
  UNION ALL SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date) - 2, 'day_before'
),
internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
)
SELECT
  TO_CHAR(rd.ref_date, 'YYYY-MM-DD') AS ref_date,
  rd.period,
  r.user_id,
  u.waid,
  COALESCE(u.full_name, '—') AS full_name,
  r.sent_at,
  r.ladder_step,
  COALESCE(r.template_name, 'Unknown') AS template_name,
  COALESCE(NULLIF(TRIM(u.timezone), ''), 'UTC') AS user_timezone
FROM recovery_logs r
JOIN users u ON u.id = r.user_id
CROSS JOIN ref_dates rd
WHERE r.ladder_step IN ('day_5_recovery', 'day_20_recovery', 'recovery_ladder_2', 'farewell')
  AND (r.sent_at AT TIME ZONE 'America/Sao_Paulo')::date = rd.ref_date
  AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
ORDER BY rd.ref_date DESC, r.sent_at DESC;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_recovery_alert_effectiveness_7d(exclude_internal: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Recovery alert effectiveness for Stack B: last 7 days vs previous 7 days.

    A "recovery alert" is any recovery_logs row whose ladder_step contains 'recovery'
    or equals 'onboarding_come_back'. Stack B only, external users only.

    summary_df columns: window_name, users_reached, came_back_count, recovery_rate_pct,
                        avg_active_days_all, avg_active_days_recovered, avg_active_days_not_recovered

    by_step_df columns (last_7d only): ladder_step, template_name, users_reached,
                                       came_back_count, recovery_rate_pct, avg_active_days
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    uv_msg_filter = get_user_visible_message_filter_sql("m")

    summary_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
windows AS (
  SELECT 'last_7d'::text AS window_name,
         ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 7) AS w_start,
         (now() AT TIME ZONE 'America/Sao_Paulo')::date       AS w_end
  UNION ALL
  SELECT 'prev_7d',
         ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 14),
         ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 7)
),
recovery_sends AS (
  SELECT
    rl.user_id,
    w.window_name,
    w.w_start,
    w.w_end,
    MIN(rl.sent_at AT TIME ZONE 'America/Sao_Paulo') AS first_alert_at
  FROM recovery_logs rl
  JOIN users u ON u.id = rl.user_id
  CROSS JOIN windows w
  WHERE u.stack_variant = 'B'
    AND u.is_active = true
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
    AND (rl.ladder_step LIKE '%recovery%' OR rl.ladder_step = 'onboarding_come_back')
    AND (rl.sent_at AT TIME ZONE 'America/Sao_Paulo')::date >= w.w_start
    AND (rl.sent_at AT TIME ZONE 'America/Sao_Paulo')::date < w.w_end
  GROUP BY rl.user_id, w.window_name, w.w_start, w.w_end
),
with_profile AS (
  SELECT rs.*, u.active_days
  FROM recovery_sends rs
  JOIN users u ON u.id = rs.user_id
),
came_back AS (
  SELECT
    wp.user_id,
    wp.window_name,
    CASE WHEN COUNT(m.id) > 0 THEN 1 ELSE 0 END AS came_back
  FROM with_profile wp
  LEFT JOIN messages m
    ON m.user_id = wp.user_id
    AND m.sender = 'user'
    AND (m.sent_at AT TIME ZONE 'America/Sao_Paulo') > wp.first_alert_at
    AND (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date < wp.w_end
    {uv_msg_filter}
  GROUP BY wp.user_id, wp.window_name
)
SELECT
  wp.window_name,
  COUNT(DISTINCT wp.user_id)                                                      AS users_reached,
  SUM(cb.came_back)                                                               AS came_back_count,
  ROUND(100.0 * SUM(cb.came_back) / NULLIF(COUNT(DISTINCT wp.user_id), 0), 1)   AS recovery_rate_pct,
  ROUND(AVG(wp.active_days), 1)                                                   AS avg_active_days_all,
  ROUND(AVG(CASE WHEN cb.came_back = 1 THEN wp.active_days END), 1)               AS avg_active_days_recovered,
  ROUND(AVG(CASE WHEN cb.came_back = 0 THEN wp.active_days END), 1)               AS avg_active_days_not_recovered
FROM with_profile wp
LEFT JOIN came_back cb ON cb.user_id = wp.user_id AND cb.window_name = wp.window_name
GROUP BY wp.window_name
ORDER BY wp.window_name DESC
"""

    by_step_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
w_start AS (
  SELECT ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 7) AS d_start,
         (now() AT TIME ZONE 'America/Sao_Paulo')::date        AS d_end
),
recovery_sends AS (
  SELECT
    rl.user_id,
    rl.ladder_step,
    COALESCE(rl.template_name, 'unknown') AS template_name,
    MIN(rl.sent_at AT TIME ZONE 'America/Sao_Paulo') AS first_alert_at,
    (SELECT d_end FROM w_start) AS w_end
  FROM recovery_logs rl
  JOIN users u ON u.id = rl.user_id
  CROSS JOIN w_start ws
  WHERE u.stack_variant = 'B'
    AND u.is_active = true
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
    AND (rl.ladder_step LIKE '%recovery%' OR rl.ladder_step = 'onboarding_come_back')
    AND (rl.sent_at AT TIME ZONE 'America/Sao_Paulo')::date >= ws.d_start
    AND (rl.sent_at AT TIME ZONE 'America/Sao_Paulo')::date < ws.d_end
  GROUP BY rl.user_id, rl.ladder_step, rl.template_name
),
came_back AS (
  SELECT
    rs.user_id,
    rs.ladder_step,
    rs.template_name,
    CASE WHEN COUNT(m.id) > 0 THEN 1 ELSE 0 END AS came_back
  FROM recovery_sends rs
  LEFT JOIN messages m
    ON m.user_id = rs.user_id
    AND m.sender = 'user'
    AND (m.sent_at AT TIME ZONE 'America/Sao_Paulo') > rs.first_alert_at
    AND (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date < rs.w_end
    {uv_msg_filter}
  GROUP BY rs.user_id, rs.ladder_step, rs.template_name
)
SELECT
  cb.ladder_step,
  cb.template_name,
  COUNT(DISTINCT cb.user_id)                                                      AS users_reached,
  SUM(cb.came_back)                                                               AS came_back_count,
  ROUND(100.0 * SUM(cb.came_back) / NULLIF(COUNT(DISTINCT cb.user_id), 0), 1)   AS recovery_rate_pct,
  ROUND(AVG(u.active_days), 1)                                                    AS avg_active_days
FROM came_back cb
JOIN users u ON u.id = cb.user_id
GROUP BY cb.ladder_step, cb.template_name
ORDER BY users_reached DESC
"""
    return run_query(summary_query), run_query(by_step_query)


@st.cache_data(ttl=300)
def get_recovery_rate_weekly_since(start_date_sp: str, exclude_internal: bool = True) -> pd.DataFrame:
    """
    Weekly recovery rate for Stack B recovery alerts since start_date_sp.

    For each Monday-start week: users who received a recovery alert AND came back
    (sent a user message after the first alert in the same week).

    Returns columns: week_start, users_reached, came_back_count, recovery_rate_pct,
                     avg_active_days_all, avg_active_days_recovered
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    uv_msg_filter = get_user_visible_message_filter_sql("m")

    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
weeks AS (
  SELECT gs::date AS week_start
  FROM generate_series(
    date_trunc('week', DATE '{start_date_sp}')::date,
    date_trunc('week', (now() AT TIME ZONE 'America/Sao_Paulo')::date),
    interval '7 days'
  ) AS gs
),
week_bounds AS (
  SELECT week_start, (week_start + interval '7 days')::date AS week_end
  FROM weeks
),
recovery_sends AS (
  SELECT
    wb.week_start,
    wb.week_end,
    rl.user_id,
    MIN(rl.sent_at AT TIME ZONE 'America/Sao_Paulo') AS first_alert_at
  FROM recovery_logs rl
  JOIN users u ON u.id = rl.user_id
  CROSS JOIN week_bounds wb
  WHERE u.stack_variant = 'B'
    AND u.is_active = true
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
    AND (rl.ladder_step LIKE '%recovery%' OR rl.ladder_step = 'onboarding_come_back')
    AND (rl.sent_at AT TIME ZONE 'America/Sao_Paulo')::date >= wb.week_start
    AND (rl.sent_at AT TIME ZONE 'America/Sao_Paulo')::date < wb.week_end
  GROUP BY wb.week_start, wb.week_end, rl.user_id
),
with_profile AS (
  SELECT rs.*, u.active_days
  FROM recovery_sends rs
  JOIN users u ON u.id = rs.user_id
),
came_back AS (
  SELECT
    wp.week_start,
    wp.user_id,
    CASE WHEN COUNT(m.id) > 0 THEN 1 ELSE 0 END AS came_back
  FROM with_profile wp
  LEFT JOIN messages m
    ON m.user_id = wp.user_id
    AND m.sender = 'user'
    AND (m.sent_at AT TIME ZONE 'America/Sao_Paulo') > wp.first_alert_at
    AND (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date < wp.week_end
    {uv_msg_filter}
  GROUP BY wp.week_start, wp.user_id
)
SELECT
  TO_CHAR(wp.week_start, 'YYYY-MM-DD')                                            AS week_start,
  COUNT(DISTINCT wp.user_id)                                                       AS users_reached,
  SUM(cb.came_back)                                                                AS came_back_count,
  ROUND(100.0 * SUM(cb.came_back) / NULLIF(COUNT(DISTINCT wp.user_id), 0), 1)    AS recovery_rate_pct,
  ROUND(AVG(wp.active_days), 1)                                                    AS avg_active_days_all,
  ROUND(AVG(CASE WHEN cb.came_back = 1 THEN wp.active_days END), 1)               AS avg_active_days_recovered
FROM with_profile wp
LEFT JOIN came_back cb ON cb.user_id = wp.user_id AND cb.week_start = wp.week_start
GROUP BY wp.week_start
ORDER BY wp.week_start
"""
    return run_query(query)


# Recovery-ladder rungs under the new day_{N}_* scheme (rolled out 2026-06).
# Maps each ladder_step to its cohort and a human-readable label. The day number
# uniquely identifies the cohort because recovery / fun-image days never collide
# across cohorts (see the recovery ladder diagram).
AT_RISK_COHORT_LE3 = "≤3 active days"
AT_RISK_COHORT_GT3 = ">3 active days"
AT_RISK_RUNG_SPEC = {
    # ≤3 active-days cohort
    "day_2_recovery": (AT_RISK_COHORT_LE3, "Recovery Ladder 1"),
    "day_3_random_fun_image": (AT_RISK_COHORT_LE3, "Random fun image"),
    "day_5_recovery": (AT_RISK_COHORT_LE3, "Recovery Ladder 2"),
    "day_10_random_fun_image": (AT_RISK_COHORT_LE3, "Random fun image"),
    # >3 active-days cohort
    "day_3_recovery": (AT_RISK_COHORT_GT3, "Recovery Ladder 1"),
    "day_5_random_fun_image": (AT_RISK_COHORT_GT3, "Random fun image"),
    "day_8_recovery": (AT_RISK_COHORT_GT3, "Recovery Ladder 2"),
    "day_10_recovery": (AT_RISK_COHORT_GT3, "Recovery Ladder 1 (repeat)"),
    "day_15_random_fun_image": (AT_RISK_COHORT_GT3, "Random fun image"),
    "day_20_recovery": (AT_RISK_COHORT_GT3, "Recovery (final push, was lose_score)"),
    "day_25_random_fun_image": (AT_RISK_COHORT_GT3, "Random fun image"),
    # Shared final step (cohort can't be inferred — Day 20 for ≤3, Day 35 for >3)
    "farewell": ("Farewell", "Farewell (marked inactive)"),
}
_AT_RISK_COHORT_ORDER = {AT_RISK_COHORT_LE3: 0, AT_RISK_COHORT_GT3: 1, "Farewell": 2, "Unknown": 3}

# Canonical recovery-ladder rungs for weekly reach tables (row order).
RECOVERY_LADDER_TABLE_RUNGS = [
    ("day_1_morning", "Day 1 — Morning"),
    ("day_1_evening", "Day 1 — Evening"),
    ("day_2_morning", "Day 2 — Morning"),
    ("day_2_evening", "Day 2 — Evening"),
    ("day_2_recovery", "Day 2 — Recovery"),
    ("day_3_recovery", "Day 3 — Recovery"),
    ("day_3_random_fun_image", "Day 3 — Fun image"),
    ("day_5_recovery", "Day 5 — Recovery"),
    ("day_5_random_fun_image", "Day 5 — Fun image"),
    ("day_8_recovery", "Day 8 — Recovery"),
    ("day_10_recovery", "Day 10 — Recovery"),
    ("day_10_random_fun_image", "Day 10 — Fun image"),
    ("day_15_random_fun_image", "Day 15 — Fun image"),
    ("day_20_recovery", "Day 20 — Recovery"),
    ("day_25_random_fun_image", "Day 25 — Fun image"),
    ("farewell", "Farewell"),
    ("recovery_ladder_1", "Recovery Ladder 1 (legacy)"),
    ("recovery_ladder_2", "Recovery Ladder 2 (legacy)"),
]


def _recovery_ladder_steps_sql() -> str:
    return "', '".join(step for step, _ in RECOVERY_LADDER_TABLE_RUNGS)


def _recovery_ladder_day3_plus_filter_sql(column: str = "r.ladder_step") -> str:
    """SQL predicate: recovery-ladder rungs from day 3 onward (plus farewell)."""
    return f"""(
      {column} = 'farewell'
      OR (
        {column} ~ '^day_[0-9]+_'
        AND (regexp_match({column}, '^day_([0-9]+)_'))[1]::int >= 3
      )
    )"""


def _parse_at_risk_rung(ladder_step: str) -> tuple[int | None, str, str, str]:
    """Return (day_num, step_type, cohort, label) for an at-risk ladder_step."""
    if ladder_step == "farewell":
        return None, "farewell", "Farewell", "Farewell (marked inactive)"
    m = re.match(r"^day_(\d+)_(recovery|random_fun_image)$", ladder_step or "")
    day_num = int(m.group(1)) if m else None
    step_type = m.group(2) if m else "unknown"
    cohort, label = AT_RISK_RUNG_SPEC.get(ladder_step, ("Unknown", ladder_step or "Unknown"))
    return day_num, step_type, cohort, label


@st.cache_data(ttl=300)
def get_at_risk_users_detail() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    At-risk user detail for Quick Insights expander.

    Returns (silent_df, reengaged_df):

    silent_df — active onboarded users with no inbound message in ≥5 days, sorted by
    last message date. Includes whether they received a recovery-ladder send since their
    last user message.

    reengaged_df — active onboarded users who were silent ≥5 days when a recovery-ladder
    send went out and replied within the last 7 days.

    Recovery-ladder rungs: day_{N}_recovery, day_{N}_random_fun_image, farewell, plus
    legacy recovery_ladder_1/2. Excludes internal users.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    internal_filter_join = get_internal_users_filter_join_sql(True, "u")
    onboarded_users_cte = get_onboarded_users_cte()
    recovery_rung_filter = """(
      r.ladder_step ~ '^day_[0-9]+_(recovery|random_fun_image)$'
      OR r.ladder_step = 'farewell'
      OR r.ladder_step IN ('recovery_ladder_1', 'recovery_ladder_2')
    )"""

    query = f"""
{onboarded_users_cte},
internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
filtered_onboarded AS (
  SELECT DISTINCT
    u.id,
    u.waid,
    u.is_active,
    COALESCE(u.full_name, 'Unknown') AS full_name,
    u.tags,
    COALESCE(u.active_days, 0)::int AS active_days
  FROM onboarded_users ou
  JOIN users u ON u.id = ou.id
  WHERE 1 = 1
    {internal_filter_join}
),
user_last_msg AS (
  SELECT
    fou.id AS user_id,
    MAX(m.sent_at) AS last_msg_at
  FROM filtered_onboarded fou
  LEFT JOIN messages m
    ON m.sender = 'user'
   AND (m.user_id = fou.id OR m.waid = fou.waid)
  GROUP BY fou.id
),
silent_users AS (
  SELECT fou.id AS user_id, fou.waid, fou.full_name, fou.tags, fou.active_days, ulm.last_msg_at
  FROM filtered_onboarded fou
  JOIN user_last_msg ulm ON ulm.user_id = fou.id
  WHERE fou.is_active = true
    AND (ulm.last_msg_at IS NULL OR ulm.last_msg_at < NOW() - INTERVAL '5 days')
),
latest_recovery AS (
  SELECT DISTINCT ON (su.user_id)
    su.user_id,
    true AS received_recovery_ladder
  FROM silent_users su
  JOIN recovery_logs r ON r.user_id = su.user_id
  WHERE {recovery_rung_filter}
    AND r.sent_at > COALESCE(su.last_msg_at, '-infinity'::timestamptz)
  ORDER BY su.user_id, r.sent_at DESC
),
silent_rows AS (
  SELECT
    su.user_id,
    su.waid,
    su.full_name,
    su.tags,
    su.active_days,
    su.last_msg_at,
    NULL::varchar AS recovery_ladder_step,
    NULL::timestamptz AS recovery_sent_at,
    false AS replied_after_recovery,
    NULL::timestamptz AS reengaged_reply_at,
    COALESCE(lr.received_recovery_ladder, false) AS received_recovery_ladder
  FROM silent_users su
  LEFT JOIN latest_recovery lr ON lr.user_id = su.user_id
),
reengaged_rows AS (
  SELECT DISTINCT ON (fou.id)
    fou.id AS user_id,
    fou.waid,
    fou.full_name,
    fou.tags,
    fou.active_days,
    ulm.last_msg_at,
    r.ladder_step AS recovery_ladder_step,
    r.sent_at AS recovery_sent_at,
    true AS replied_after_recovery,
    fr.first_reply_at AS reengaged_reply_at,
    true AS received_recovery_ladder
  FROM filtered_onboarded fou
  JOIN user_last_msg ulm ON ulm.user_id = fou.id
  JOIN recovery_logs r ON r.user_id = fou.id
  CROSS JOIN LATERAL (
    SELECT MIN(m.sent_at) AS first_reply_at
    FROM messages m
    WHERE m.sender = 'user'
      AND (m.user_id = fou.id OR m.waid = fou.waid)
      AND m.sent_at > r.sent_at
  ) fr
  CROSS JOIN LATERAL (
    SELECT MAX(m2.sent_at) AS last_msg_before_recovery
    FROM messages m2
    WHERE m2.sender = 'user'
      AND (m2.user_id = fou.id OR m2.waid = fou.waid)
      AND m2.sent_at < r.sent_at
  ) lmb
  WHERE fou.is_active = true
    AND {recovery_rung_filter}
    AND fr.first_reply_at >= NOW() - INTERVAL '7 days'
    AND (
      lmb.last_msg_before_recovery IS NULL
      OR lmb.last_msg_before_recovery < r.sent_at - INTERVAL '5 days'
    )
  ORDER BY fou.id, r.sent_at DESC
)
SELECT 'silent' AS row_type, * FROM silent_rows
UNION ALL
SELECT 'reengaged' AS row_type, * FROM reengaged_rows;
"""
    df = run_query(query)
    silent_cols = [
        "user_id", "waid", "full_name", "tags", "active_days", "last_msg_at", "received_recovery_ladder",
    ]
    reengaged_cols = [
        "user_id", "waid", "full_name", "tags", "active_days", "last_msg_at",
        "recovery_ladder_step", "recovery_sent_at", "reengaged_reply_at",
    ]
    empty_silent = pd.DataFrame(columns=silent_cols)
    empty_reengaged = pd.DataFrame(columns=reengaged_cols)
    if df.empty:
        return empty_silent, empty_reengaged

    silent_df = (
        df[df["row_type"] == "silent"][silent_cols]
        .sort_values(by="last_msg_at", ascending=True, na_position="first")
        .reset_index(drop=True)
    )
    reengaged_df = (
        df[df["row_type"] == "reengaged"][reengaged_cols]
        .sort_values(by="reengaged_reply_at", ascending=False)
        .reset_index(drop=True)
    )
    return silent_df, reengaged_df


# Friendly labels for any recovery_logs.ladder_step (new day_* scheme, legacy rungs,
# and routine check-ins). Used to tag message tables and the deep-dive step badge.
_LADDER_STEP_LABELS = {
    "recovery_ladder_1": "Recovery Ladder 1 (legacy)",
    "recovery_ladder_2": "Recovery Ladder 2 (legacy)",
    "farewell": "Farewell",
    "morning_checkin_1": "Morning check-in",
    "morning_checkin_2": "Morning check-in",
    "daily_digest_1": "Evening digest",
    "weekly_review": "Weekly review",
    "onboarding_come_back": "Onboarding come-back",
}


def _label_ladder_step(ladder_step: str | None) -> str:
    """Human-readable label for a recovery_logs.ladder_step value ('' if empty)."""
    if not ladder_step or pd.isna(ladder_step):
        return ""
    m = re.match(r"^day_(\d+)_(recovery|random_fun_image|farewell)$", str(ladder_step))
    if m:
        spec = AT_RISK_RUNG_SPEC.get(ladder_step)
        if m.group(2) == "farewell":
            sub = "Farewell"
        elif spec:
            sub = spec[1]
        else:
            sub = "Random fun image" if m.group(2) == "random_fun_image" else "Recovery"
        return f"Day {int(m.group(1))} — {sub}"
    m_shift = re.match(r"^day_(\d+)_(morning|evening)$", str(ladder_step))
    if m_shift:
        return f"Day {int(m_shift.group(1))} — {m_shift.group(2).capitalize()}"
    if ladder_step in _LADDER_STEP_LABELS:
        return _LADDER_STEP_LABELS[ladder_step]
    return str(ladder_step).replace("_", " ").strip().capitalize()


# PDF recovery-ladder milestones (Letz Recovery Ladder diagram).
_RECOVERY_MILESTONES_LE3 = [
    (1, "day_1_morning/evening"),
    (2, "day_2_recovery"),
    (3, "day_3_random_fun_image"),
    (5, "day_5_recovery"),
    (10, "day_10_random_fun_image"),
    (20, "day_20_farewell"),
]
_RECOVERY_MILESTONES_GT3 = [
    (1, "day_1_morning/evening"),
    (2, "day_2_morning/evening"),
    (3, "day_3_recovery"),
    (5, "day_5_random_fun_image"),
    (8, "day_8_recovery"),
    (10, "day_10_recovery"),
    (15, "day_15_random_fun_image"),
    (20, "day_20_recovery"),
    (25, "day_25_random_fun_image"),
    (35, "day_35_farewell"),
]


def _recovery_milestones(active_days: int) -> list[tuple[int, str]]:
    return _RECOVERY_MILESTONES_LE3 if int(active_days or 0) <= 3 else _RECOVERY_MILESTONES_GT3


def _pdf_ladder_step(active_days: int, days_afk: int) -> str | None:
    """Exact PDF ladder step for today's days_afk (None if between milestones)."""
    for day_num, step in _recovery_milestones(active_days):
        if days_afk == day_num:
            return step
    return None


def _next_ladder_step(active_days: int, days_afk: int) -> str | None:
    """Next PDF milestone step after the user's current days_afk."""
    farewell_day = 20 if int(active_days or 0) <= 3 else 35
    if days_afk >= farewell_day:
        return None
    for day_num, step in _recovery_milestones(active_days):
        if day_num > days_afk:
            return step
    return None


def _ladder_position_label(active_days: int, days_afk: int) -> str:
    """Human-readable ladder position (past milestone + next milestone)."""
    milestones = _recovery_milestones(active_days)
    farewell_day = milestones[-1][0]
    if days_afk <= 0:
        return "Active (not AFK)"
    if days_afk >= farewell_day:
        return f"Past farewell (day {farewell_day})"
    past = None
    nxt = None
    for day_num, _step in milestones:
        if day_num <= days_afk:
            past = day_num
        elif nxt is None:
            nxt = day_num
            break
    if past is None:
        return "Before first ladder step"
    if nxt is None:
        return f"Past day {past}"
    if days_afk in {m[0] for m in milestones}:
        return f"On day {days_afk} ladder"
    return f"Past day {past}, next: day {nxt}"


def _format_milestone_step(step: str | None) -> str:
    """Format a PDF milestone step id for display."""
    if not step:
        return "—"
    if "/" in step:
        return step.replace("_", " ").replace("/", " / ")
    labeled = _label_ladder_step(step)
    return labeled if labeled else step.replace("_", " ")


@st.cache_data(ttl=300)
def get_afk_users_distribution(exclude_internal: bool = True) -> pd.DataFrame:
    """
    Snapshot of active AFK users (no user message in 24h+) grouped by days_afk.
    Matches FETCH_RECOVERY_TARGETS days_afk logic (waid-based last user message).
    """
    internal_filter = get_internal_users_filter_join_sql(exclude_internal=True, table_alias="u")
    return run_query(f"""
WITH base AS (
  SELECT
    u.id,
    COALESCE(u.active_days, 0)::int AS active_days,
    EXTRACT(DAY FROM NOW() - last_msg.last_user_message_at)::int AS days_afk
  FROM users u
  CROSS JOIN LATERAL (
    SELECT MAX(me.sent_at) AS last_user_message_at
    FROM messages me
    WHERE me.waid = u.waid
      AND me.sender = 'user'
      AND me.type NOT IN ('think', 'tool_use', 'tool_result', 'turn_audit')
  ) last_msg
  WHERE u.is_active = true
    {internal_filter}
    AND last_msg.last_user_message_at IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM messages me
      WHERE me.waid = u.waid
        AND me.sender = 'user'
        AND me.sent_at > NOW() - INTERVAL '24 hours'
        AND me.type NOT IN ('think', 'tool_use', 'tool_result', 'turn_audit')
    )
)
SELECT
  days_afk,
  COUNT(*)::int AS users,
  COUNT(*) FILTER (WHERE active_days <= 3)::int AS low_active_users,
  COUNT(*) FILTER (WHERE active_days > 3)::int AS high_active_users
FROM base
WHERE days_afk >= 1
GROUP BY days_afk
ORDER BY days_afk
""")


@st.cache_data(ttl=300)
def get_reactivated_users_last_24h() -> pd.DataFrame:
    """
    Users who were marked inactive in the last 7 days (received farewell in recovery_logs)
    and have sent an inbound message to the coach within the last 24 hours.
    Returns columns: waid, full_name, last_message_at, farewell_at (most recent farewell before reactivation).
    Excludes internal users.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
had_farewell AS (
  SELECT DISTINCT ON (r.user_id)
    r.user_id,
    r.sent_at AS farewell_at
  FROM recovery_logs r
  JOIN users u ON u.id = r.user_id
  WHERE r.ladder_step = 'farewell'
    AND r.sent_at >= NOW() - INTERVAL '7 days'
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  ORDER BY r.user_id, r.sent_at DESC
),
recent_inbound AS (
  SELECT
    u.id AS user_id,
    MAX(m.sent_at) AS last_message_at
  FROM messages m
  JOIN users u ON (m.user_id = u.id OR m.waid = u.waid)
  WHERE m.sender = 'user'
    AND m.sent_at >= NOW() - INTERVAL '24 hours'
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  GROUP BY u.id
)
SELECT
  u.id AS user_id,
  u.waid,
  COALESCE(u.full_name, 'Unknown') AS full_name,
  u.tags,
  ri.last_message_at,
  hf.farewell_at
FROM had_farewell hf
JOIN recent_inbound ri ON ri.user_id = hf.user_id
JOIN users u ON u.id = hf.user_id
WHERE ri.last_message_at > hf.farewell_at
ORDER BY ri.last_message_at DESC;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_recovery_ladder_events(start_date_sp: str = "2026-02-01") -> pd.DataFrame:
    """
    Recovery template sends with latest-template attribution windows.

    For each template send, attributes response/activity to that send until next template
    for the same user (if any), then computes:
    - replied_before_next_template
    - activity_12h
    - activity_24h
    - response_minutes (first user reply time)
    Week buckets use Monday starts in America/Sao_Paulo.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"

    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
template_sends AS (
  SELECT
    r.id AS recovery_log_id,
    r.user_id,
    u.waid,
    u.is_active,
    COALESCE(u.full_name, 'Unknown') AS full_name,
    COALESCE(r.template_name, 'Unknown') AS template_name,
    r.ladder_step,
    r.sent_at AS template_sent_at_utc,
    (r.sent_at AT TIME ZONE 'America/Sao_Paulo') AS template_sent_at_sp,
    date_trunc('week', r.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS week_start_sp,
    LEAD(r.sent_at) OVER (PARTITION BY r.user_id ORDER BY r.sent_at) AS next_template_at_utc
  FROM recovery_logs r
  JOIN users u ON r.user_id = u.id
  WHERE (r.sent_at AT TIME ZONE 'America/Sao_Paulo')::date >= DATE '{start_date_sp}'
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
),
response_events AS (
  SELECT
    ts.*,
    reply.first_reply_at_utc,
    CASE
      WHEN reply.first_reply_at_utc IS NOT NULL
      THEN EXTRACT(EPOCH FROM (reply.first_reply_at_utc - ts.template_sent_at_utc)) / 60.0
      ELSE NULL
    END AS response_minutes
  FROM template_sends ts
  LEFT JOIN LATERAL (
    SELECT MIN(m.sent_at) AS first_reply_at_utc
    FROM messages m
    WHERE m.sender = 'user'
      AND (m.user_id = ts.user_id OR m.waid = ts.waid)
      AND m.sent_at > ts.template_sent_at_utc
      AND (ts.next_template_at_utc IS NULL OR m.sent_at < ts.next_template_at_utc)
  ) reply ON true
),
conversion_flags AS (
  SELECT
    re.*,
    (re.first_reply_at_utc IS NOT NULL) AS replied_before_next_template,
    EXISTS (
      SELECT 1
      FROM user_activities_history uah
      WHERE uah.user_id = re.user_id
        AND uah.completed_at > re.template_sent_at_utc
        AND uah.completed_at <= LEAST(
          re.template_sent_at_utc + INTERVAL '12 hours',
          COALESCE(re.next_template_at_utc, re.template_sent_at_utc + INTERVAL '100 years')
        )
    ) AS activity_12h,
    EXISTS (
      SELECT 1
      FROM user_activities_history uah
      WHERE uah.user_id = re.user_id
        AND uah.completed_at > re.template_sent_at_utc
        AND uah.completed_at <= LEAST(
          re.template_sent_at_utc + INTERVAL '24 hours',
          COALESCE(re.next_template_at_utc, re.template_sent_at_utc + INTERVAL '100 years')
        )
    ) AS activity_24h
  FROM response_events re
)
SELECT
  TO_CHAR(week_start_sp, 'YYYY-MM-DD') AS week_start_sp,
  template_name,
  ladder_step,
  user_id,
  waid,
  is_active,
  full_name,
  template_sent_at_utc,
  template_sent_at_sp,
  next_template_at_utc,
  first_reply_at_utc,
  replied_before_next_template,
  activity_12h,
  activity_24h,
  response_minutes
FROM conversion_flags
ORDER BY week_start_sp DESC, template_sent_at_utc DESC;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_recovery_weekly_active_user_reach(weeks_back: int = 6, exclude_internal: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """
    Weekly reach of recovery-ladder templates among active users.

    Returns (timeline_df, rung_df, active_users):
      - timeline_df: week_start, users_receiving, pct_active_users (% receiving any rung)
      - rung_df: week_start, ladder_step, users_receiving, pct_active_users (per rung)
      - active_users: current active-user denominator (is_active IS NOT FALSE)

    Weeks are Monday-start in America/Sao_Paulo. Excludes internal WAIDs when exclude_internal=True.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    steps_sql = _recovery_ladder_steps_sql()
    day3_plus_filter = _recovery_ladder_day3_plus_filter_sql("r.ladder_step")
    weeks_interval = max(int(weeks_back) - 1, 0)

    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
weeks AS (
  SELECT gs::date AS week_start
  FROM generate_series(
    date_trunc('week', ((now() AT TIME ZONE 'America/Sao_Paulo')::date - interval '{weeks_interval} weeks'))::date,
    date_trunc('week', (now() AT TIME ZONE 'America/Sao_Paulo')::date),
    interval '7 days'
  ) AS gs
),
active_base AS (
  SELECT COUNT(DISTINCT u.id)::bigint AS active_users
  FROM users u
  WHERE u.is_active IS NOT FALSE
    {internal_filter_join}
),
rung_sends AS (
  SELECT
    date_trunc('week', (r.sent_at AT TIME ZONE 'America/Sao_Paulo'))::date AS week_start,
    r.ladder_step,
    COUNT(DISTINCT r.user_id)::bigint AS users_receiving
  FROM recovery_logs r
  JOIN users u ON u.id = r.user_id
  WHERE u.is_active IS NOT FALSE
    AND r.ladder_step IN ('{steps_sql}')
    {internal_filter_join}
    AND date_trunc('week', (r.sent_at AT TIME ZONE 'America/Sao_Paulo'))::date IN (SELECT week_start FROM weeks)
  GROUP BY 1, 2
),
weekly_any AS (
  SELECT
    date_trunc('week', (r.sent_at AT TIME ZONE 'America/Sao_Paulo'))::date AS week_start,
    COUNT(DISTINCT r.user_id)::bigint AS users_receiving
  FROM recovery_logs r
  JOIN users u ON u.id = r.user_id
  WHERE u.is_active IS NOT FALSE
    AND {day3_plus_filter}
    {internal_filter_join}
    AND date_trunc('week', (r.sent_at AT TIME ZONE 'America/Sao_Paulo'))::date IN (SELECT week_start FROM weeks)
  GROUP BY 1
)
SELECT
  'timeline' AS row_type,
  w.week_start,
  NULL::varchar AS ladder_step,
  COALESCE(wa.users_receiving, 0)::bigint AS users_receiving,
  ab.active_users,
  ROUND(100.0 * COALESCE(wa.users_receiving, 0) / NULLIF(ab.active_users, 0), 1) AS pct_active_users
FROM weeks w
CROSS JOIN active_base ab
LEFT JOIN weekly_any wa ON wa.week_start = w.week_start

UNION ALL

SELECT
  'rung' AS row_type,
  rs.week_start,
  rs.ladder_step,
  rs.users_receiving,
  ab.active_users,
  ROUND(100.0 * rs.users_receiving / NULLIF(ab.active_users, 0), 1) AS pct_active_users
FROM rung_sends rs
CROSS JOIN active_base ab

ORDER BY row_type, week_start, ladder_step;
"""
    df = run_query(query)
    if df.empty:
        empty_timeline = pd.DataFrame(columns=["week_start", "users_receiving", "pct_active_users"])
        empty_rung = pd.DataFrame(columns=["week_start", "ladder_step", "users_receiving", "pct_active_users"])
        return empty_timeline, empty_rung, 0

    active_users = int(df["active_users"].iloc[0]) if "active_users" in df.columns and not df.empty else 0
    timeline_df = (
        df[df["row_type"] == "timeline"][["week_start", "users_receiving", "pct_active_users"]]
        .copy()
        .sort_values("week_start")
    )
    rung_df = (
        df[df["row_type"] == "rung"][["week_start", "ladder_step", "users_receiving", "pct_active_users"]]
        .copy()
    )
    return timeline_df, rung_df, active_users


@st.cache_data(ttl=300)
def get_recovery_weekly_message_baseline_metrics(start_date_sp: str) -> pd.DataFrame:
    """
    Week-by-week metrics for Recovery Ladder tab (messages table baseline).

    For each Monday-start week (America/Sao_Paulo), at week end (next Monday 00:00 SP):
    - Count distinct **active** users (`users.is_active` not false) whose last *user* message
      (any inbound to coach) at or before that instant was more than 24h / 48h / 72h before
      week end (or never messaged).
    - Count recovery ladder template rows (RL1/RL2) sent in that week.
    - Count distinct users who received a farewell template that week (recovery_logs.ladder_step = 'farewell').

    Excludes internal WAIDs.
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
weeks AS (
  SELECT gs::date AS week_start
  FROM generate_series(
    date_trunc('week', DATE '{start_date_sp}')::date,
    date_trunc('week', (now() AT TIME ZONE 'America/Sao_Paulo')::date),
    interval '7 days'
  ) AS gs
),
week_bounds AS (
  SELECT
    week_start,
    ((week_start + interval '7 days')::timestamp AT TIME ZONE 'America/Sao_Paulo') AS week_end_sp
  FROM weeks
),
user_silence AS (
  SELECT
    wb.week_start,
    wb.week_end_sp,
    u.id AS user_id,
    u.is_active,
    (
      SELECT MAX(m.sent_at)
      FROM messages m
      WHERE m.sender = 'user'
        AND (m.user_id = u.id OR m.waid = u.waid)
        AND m.sent_at <= wb.week_end_sp
    ) AS last_user_msg_at
  FROM week_bounds wb
  CROSS JOIN users u
  WHERE u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
),
agg AS (
  SELECT
    week_start,
    COUNT(DISTINCT user_id) FILTER (
      WHERE is_active IS NOT FALSE
        AND (
          last_user_msg_at IS NULL
          OR last_user_msg_at < week_end_sp - interval '24 hours'
        )
    ) AS unique_no_msg_gt_24h,
    COUNT(DISTINCT user_id) FILTER (
      WHERE is_active IS NOT FALSE
        AND (
          last_user_msg_at IS NULL
          OR last_user_msg_at < week_end_sp - interval '48 hours'
        )
    ) AS unique_no_msg_gt_48h,
    COUNT(DISTINCT user_id) FILTER (
      WHERE is_active IS NOT FALSE
        AND (
          last_user_msg_at IS NULL
          OR last_user_msg_at < week_end_sp - interval '72 hours'
        )
    ) AS unique_no_msg_gt_72h
  FROM user_silence
  GROUP BY week_start, week_end_sp
),
rl_week AS (
  SELECT
    date_trunc('week', (r.sent_at AT TIME ZONE 'America/Sao_Paulo'))::date AS week_start,
    COUNT(*)::bigint AS recovery_ladder_sends
  FROM recovery_logs r
  JOIN users u ON r.user_id = u.id
  WHERE r.ladder_step IN ('recovery_ladder_1', 'recovery_ladder_2')
    AND (r.sent_at AT TIME ZONE 'America/Sao_Paulo')::date >= DATE '{start_date_sp}'
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  GROUP BY 1
),
farewell_week AS (
  SELECT
    date_trunc('week', (r.sent_at AT TIME ZONE 'America/Sao_Paulo'))::date AS week_start,
    COUNT(DISTINCT r.user_id)::bigint AS unique_users_farewell
  FROM recovery_logs r
  JOIN users u ON r.user_id = u.id
  WHERE r.ladder_step = 'farewell'
    AND (r.sent_at AT TIME ZONE 'America/Sao_Paulo')::date >= DATE '{start_date_sp}'
    AND u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
  GROUP BY 1
)
SELECT
  TO_CHAR(a.week_start, 'YYYY-MM-DD') AS week_start,
  a.unique_no_msg_gt_24h,
  a.unique_no_msg_gt_48h,
  a.unique_no_msg_gt_72h,
  COALESCE(r.recovery_ladder_sends, 0)::bigint AS recovery_ladder_sends,
  COALESCE(f.unique_users_farewell, 0)::bigint AS unique_users_farewell_week
FROM agg a
LEFT JOIN rl_week r ON r.week_start = a.week_start
LEFT JOIN farewell_week f ON f.week_start = a.week_start
ORDER BY a.week_start DESC;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_recovery_weekly_waterfall_metrics(start_date_sp: str) -> pd.DataFrame:
    """
    Weekly waterfall-style flows for Recovery Ladder monitoring.

    Week definition:
    - Monday-start weeks in America/Sao_Paulo.
    - Event inclusion uses [week_start, week_end) boundaries.

    Flow model:
    - Start active / End active are stock snapshots inferred from farewell/reactivation events.
      Inactive proxy = user received farewell and has not sent a user message after that farewell yet.
    - Became inactive = distinct users with >=1 farewell in the week.
    - Reactivated = distinct users with first inbound user message after a farewell in the week.
    - New acquired = users.created_at in the week.

    Risk layers (event-based, with carry-over stock):
    - 24h silence risk episodes from gaps between user inbound messages (active users only).
    - RL risk episodes from RL1/RL2 sends until next inbound user message (active users only).
    """
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
base_users AS (
  SELECT
    u.id,
    u.waid,
    u.created_at,
    u.is_active
  FROM users u
  WHERE u.waid NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
),
weeks AS (
  SELECT gs::date AS week_start
  FROM generate_series(
    date_trunc('week', DATE '{start_date_sp}')::date,
    date_trunc('week', (now() AT TIME ZONE 'America/Sao_Paulo')::date),
    interval '7 days'
  ) AS gs
),
week_bounds AS (
  SELECT
    week_start,
    (week_start::timestamp AT TIME ZONE 'America/Sao_Paulo') AS week_start_ts,
    ((week_start + interval '7 days')::timestamp AT TIME ZONE 'America/Sao_Paulo') AS week_end_ts
  FROM weeks
),
inbound_msgs AS (
  SELECT
    bu.id AS user_id,
    m.sent_at
  FROM base_users bu
  JOIN messages m
    ON m.sender = 'user'
   AND (m.user_id = bu.id OR m.waid = bu.waid)
),
farewell_events AS (
  SELECT
    r.user_id,
    r.sent_at AS farewell_at,
    LEAD(r.sent_at) OVER (PARTITION BY r.user_id ORDER BY r.sent_at) AS next_farewell_at
  FROM recovery_logs r
  JOIN base_users bu ON bu.id = r.user_id
  WHERE r.ladder_step = 'farewell'
),
farewell_cycles AS (
  SELECT
    fe.user_id,
    fe.farewell_at,
    (
      SELECT MIN(im.sent_at)
      FROM inbound_msgs im
      WHERE im.user_id = fe.user_id
        AND im.sent_at > fe.farewell_at
        AND (fe.next_farewell_at IS NULL OR im.sent_at < fe.next_farewell_at)
    ) AS reactivated_at
  FROM farewell_events fe
),
active_stock AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT bu.id) FILTER (
      WHERE bu.created_at < wb.week_start_ts
        AND NOT EXISTS (
          SELECT 1
          FROM farewell_cycles fc
          WHERE fc.user_id = bu.id
            AND fc.farewell_at < wb.week_start_ts
            AND (fc.reactivated_at IS NULL OR fc.reactivated_at > wb.week_start_ts)
        )
    )::bigint AS start_active_users,
    COUNT(DISTINCT bu.id) FILTER (
      WHERE bu.created_at < wb.week_end_ts
        AND NOT EXISTS (
          SELECT 1
          FROM farewell_cycles fc
          WHERE fc.user_id = bu.id
            AND fc.farewell_at < wb.week_end_ts
            AND (fc.reactivated_at IS NULL OR fc.reactivated_at > wb.week_end_ts)
        )
    )::bigint AS end_active_users
  FROM week_bounds wb
  CROSS JOIN base_users bu
  GROUP BY wb.week_start
),
weekly_new_acquired AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT bu.id)::bigint AS new_acquired_users
  FROM week_bounds wb
  JOIN base_users bu
    ON bu.created_at >= wb.week_start_ts
   AND bu.created_at < wb.week_end_ts
  GROUP BY wb.week_start
),
weekly_became_inactive AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT fe.user_id)::bigint AS became_inactive_users
  FROM week_bounds wb
  JOIN farewell_events fe
    ON fe.farewell_at >= wb.week_start_ts
   AND fe.farewell_at < wb.week_end_ts
  GROUP BY wb.week_start
),
weekly_reactivated AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT fc.user_id)::bigint AS reactivated_users
  FROM week_bounds wb
  JOIN farewell_cycles fc
    ON fc.reactivated_at >= wb.week_start_ts
   AND fc.reactivated_at < wb.week_end_ts
  GROUP BY wb.week_start
),
weekly_flows AS (
  SELECT
    wb.week_start,
    COALESCE(wn.new_acquired_users, 0)::bigint AS new_acquired_users,
    COALESCE(wi.became_inactive_users, 0)::bigint AS became_inactive_users,
    COALESCE(wr.reactivated_users, 0)::bigint AS reactivated_users
  FROM week_bounds wb
  LEFT JOIN weekly_new_acquired wn ON wn.week_start = wb.week_start
  LEFT JOIN weekly_became_inactive wi ON wi.week_start = wb.week_start
  LEFT JOIN weekly_reactivated wr ON wr.week_start = wb.week_start
),
eligible_active_users AS (
  SELECT id, waid
  FROM base_users
  WHERE is_active IS TRUE
),
active_inbound_msgs AS (
  SELECT
    eau.id AS user_id,
    m.sent_at
  FROM eligible_active_users eau
  JOIN messages m
    ON m.sender = 'user'
   AND (m.user_id = eau.id OR m.waid = eau.waid)
),
silence_edges AS (
  SELECT
    aim.user_id,
    aim.sent_at AS msg_at,
    LEAD(aim.sent_at) OVER (PARTITION BY aim.user_id ORDER BY aim.sent_at) AS next_msg_at
  FROM active_inbound_msgs aim
),
risk24_episodes AS (
  SELECT
    se.user_id,
    (se.msg_at + interval '24 hours') AS risk_start_at,
    se.next_msg_at AS risk_end_at
  FROM silence_edges se
  WHERE se.next_msg_at IS NULL
     OR se.next_msg_at > se.msg_at + interval '24 hours'
),
rl_risk_events AS (
  SELECT
    r.user_id,
    r.sent_at AS risk_start_at
  FROM recovery_logs r
  JOIN eligible_active_users eau ON eau.id = r.user_id
  WHERE r.ladder_step IN ('recovery_ladder_1', 'recovery_ladder_2')
),
rl_risk_episodes AS (
  SELECT
    rre.user_id,
    rre.risk_start_at,
    (
      SELECT MIN(aim.sent_at)
      FROM active_inbound_msgs aim
      WHERE aim.user_id = rre.user_id
        AND aim.sent_at > rre.risk_start_at
    ) AS risk_end_at
  FROM rl_risk_events rre
),
risk24_week AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_start_at < wb.week_start_ts
        AND (r.risk_end_at IS NULL OR r.risk_end_at > wb.week_start_ts)
    )::bigint AS start_risk_24h_users,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_start_at >= wb.week_start_ts
        AND r.risk_start_at < wb.week_end_ts
    )::bigint AS new_risk_24h_users,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_end_at >= wb.week_start_ts
        AND r.risk_end_at < wb.week_end_ts
    )::bigint AS derisked_24h_users,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_start_at < wb.week_end_ts
        AND (r.risk_end_at IS NULL OR r.risk_end_at > wb.week_end_ts)
    )::bigint AS end_risk_24h_users
  FROM week_bounds wb
  LEFT JOIN risk24_episodes r ON TRUE
  GROUP BY wb.week_start
),
risk_rl_week AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_start_at < wb.week_start_ts
        AND (r.risk_end_at IS NULL OR r.risk_end_at > wb.week_start_ts)
    )::bigint AS start_risk_rl_users,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_start_at >= wb.week_start_ts
        AND r.risk_start_at < wb.week_end_ts
    )::bigint AS new_risk_rl_users,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_end_at >= wb.week_start_ts
        AND r.risk_end_at < wb.week_end_ts
    )::bigint AS derisked_rl_users,
    COUNT(DISTINCT r.user_id) FILTER (
      WHERE r.risk_start_at < wb.week_end_ts
        AND (r.risk_end_at IS NULL OR r.risk_end_at > wb.week_end_ts)
    )::bigint AS end_risk_rl_users
  FROM week_bounds wb
  LEFT JOIN rl_risk_episodes r ON TRUE
  GROUP BY wb.week_start
)
SELECT
  TO_CHAR(wb.week_start, 'YYYY-MM-DD') AS week_start,
  COALESCE(ast.start_active_users, 0)::bigint AS start_active_users,
  COALESCE(wf.new_acquired_users, 0)::bigint AS new_acquired_users,
  COALESCE(wf.reactivated_users, 0)::bigint AS reactivated_users,
  COALESCE(wf.became_inactive_users, 0)::bigint AS became_inactive_users,
  (
    COALESCE(ast.start_active_users, 0)
    + COALESCE(wf.new_acquired_users, 0)
    + COALESCE(wf.reactivated_users, 0)
    - COALESCE(wf.became_inactive_users, 0)
  )::bigint AS end_active_users_computed,
  COALESCE(ast.end_active_users, 0)::bigint AS end_active_users_observed,
  (
    COALESCE(ast.end_active_users, 0)
    - (
      COALESCE(ast.start_active_users, 0)
      + COALESCE(wf.new_acquired_users, 0)
      + COALESCE(wf.reactivated_users, 0)
      - COALESCE(wf.became_inactive_users, 0)
    )
  )::bigint AS active_reconciliation_gap,
  COALESCE(r24.start_risk_24h_users, 0)::bigint AS start_risk_24h_users,
  COALESCE(r24.new_risk_24h_users, 0)::bigint AS new_risk_24h_users,
  COALESCE(r24.derisked_24h_users, 0)::bigint AS derisked_24h_users,
  COALESCE(r24.end_risk_24h_users, 0)::bigint AS end_risk_24h_users,
  COALESCE(rrl.start_risk_rl_users, 0)::bigint AS start_risk_rl_users,
  COALESCE(rrl.new_risk_rl_users, 0)::bigint AS new_risk_rl_users,
  COALESCE(rrl.derisked_rl_users, 0)::bigint AS derisked_rl_users,
  COALESCE(rrl.end_risk_rl_users, 0)::bigint AS end_risk_rl_users
FROM week_bounds wb
LEFT JOIN active_stock ast ON ast.week_start = wb.week_start
LEFT JOIN weekly_flows wf ON wf.week_start = wb.week_start
LEFT JOIN risk24_week r24 ON r24.week_start = wb.week_start
LEFT JOIN risk_rl_week rrl ON rrl.week_start = wb.week_start
ORDER BY wb.week_start DESC;
"""
    return run_query(query)


@st.cache_data(ttl=300)
@st.cache_data(ttl=300)
def get_active_days_by_cohort_weekly(weeks_back: int = 12, exclude_internal: bool = True) -> pd.DataFrame:
    """
    Weekly avg active days (activity completions) per user for three cohorts:
      alive   — onboarded, is_active = true, messaged within last 5 days
      at_risk — onboarded, is_active = true, silent ≥5 days
      churned — onboarded, is_active = false

    Cohort membership is current (snapshot). The denominator for each cohort is its
    current size so the trend reflects engagement history for those users.

    Returns columns: week_start (str YYYY-MM-DD), cohort, avg_active_days, total_active_days, cohort_size.
    """
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    onboarded_users_cte = get_onboarded_users_cte()
    start = (datetime.now() - timedelta(weeks=weeks_back)).strftime("%Y-%m-%d")
    query = f"""
{onboarded_users_cte},
filtered_onboarded AS (
  SELECT DISTINCT
    u.id,
    u.waid,
    CASE
      WHEN u.is_active = false THEN 'churned'
      WHEN u.is_active = true AND NOT EXISTS (
        SELECT 1 FROM messages m
        WHERE m.sender = 'user'
          AND (m.user_id = u.id OR m.waid = u.waid)
          AND m.sent_at >= NOW() - INTERVAL '5 days'
      ) THEN 'at_risk'
      ELSE 'alive'
    END AS cohort
  FROM onboarded_users ou
  JOIN users u ON u.id = ou.id
  WHERE 1 = 1
    {internal_filter_join}
),
cohort_sizes AS (
  SELECT cohort, COUNT(DISTINCT id)::int AS cohort_size
  FROM filtered_onboarded
  GROUP BY cohort
),
weeks AS (
  SELECT gs::date AS week_start
  FROM generate_series(
    date_trunc('week', DATE '{start}')::date,
    date_trunc('week', (NOW() AT TIME ZONE 'America/Sao_Paulo')::date),
    interval '7 days'
  ) AS gs
),
weekly_user_active AS (
  SELECT
    fo.cohort,
    date_trunc('week', uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date AS week_start,
    uah.user_id,
    COUNT(DISTINCT (uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date) AS active_days_in_week
  FROM user_activities_history uah
  JOIN filtered_onboarded fo ON uah.user_id = fo.id
  WHERE uah.completed_at >= (DATE '{start}'::timestamp AT TIME ZONE 'America/Sao_Paulo')
  GROUP BY fo.cohort, week_start, uah.user_id
),
weekly_totals AS (
  SELECT cohort, week_start, SUM(active_days_in_week)::bigint AS total_active_days
  FROM weekly_user_active
  GROUP BY cohort, week_start
)
SELECT
  TO_CHAR(w.week_start, 'YYYY-MM-DD') AS week_start,
  cs.cohort,
  cs.cohort_size,
  COALESCE(wt.total_active_days, 0) AS total_active_days,
  CASE
    WHEN cs.cohort_size > 0
    THEN ROUND(COALESCE(wt.total_active_days, 0)::numeric / cs.cohort_size, 2)
    ELSE 0
  END AS avg_active_days
FROM weeks w
CROSS JOIN cohort_sizes cs
LEFT JOIN weekly_totals wt ON wt.week_start = w.week_start AND wt.cohort = cs.cohort
ORDER BY w.week_start, cs.cohort;
"""
    return run_query(query)


def get_beta_weekly_churn_rate_metrics(start_date_sp: str, exclude_internal: bool = True) -> pd.DataFrame:
    """
    Weekly churn chart metrics for Quick Insights, using beta users as denominator.

    Churn numerator = beta users who received a farewell in the week.
    Denominator = beta users active at week start, where active means no unresolved
    farewell before the week start.
    """
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    beta_users_cte = get_beta_users_cte()
    query = f"""
{beta_users_cte},
base_users AS (
  SELECT
    u.id,
    u.waid,
    u.created_at
  FROM beta_users bu
  JOIN users u ON u.id = bu.id
  WHERE 1 = 1
    {internal_filter_join}
),
weeks AS (
  SELECT gs::date AS week_start
  FROM generate_series(
    date_trunc('week', DATE '{start_date_sp}')::date,
    date_trunc('week', (now() AT TIME ZONE 'America/Sao_Paulo')::date),
    interval '7 days'
  ) AS gs
),
week_bounds AS (
  SELECT
    week_start,
    (week_start::timestamp AT TIME ZONE 'America/Sao_Paulo') AS week_start_ts,
    ((week_start + interval '7 days')::timestamp AT TIME ZONE 'America/Sao_Paulo') AS week_end_ts
  FROM weeks
),
inbound_msgs AS (
  SELECT
    bu.id AS user_id,
    m.sent_at
  FROM base_users bu
  JOIN messages m
    ON m.sender = 'user'
   AND (m.user_id = bu.id OR m.waid = bu.waid)
),
farewell_events AS (
  SELECT
    r.user_id,
    r.sent_at AS farewell_at,
    LEAD(r.sent_at) OVER (PARTITION BY r.user_id ORDER BY r.sent_at) AS next_farewell_at
  FROM recovery_logs r
  JOIN base_users bu ON bu.id = r.user_id
  WHERE r.ladder_step = 'farewell'
),
farewell_cycles AS (
  SELECT
    fe.user_id,
    fe.farewell_at,
    (
      SELECT MIN(im.sent_at)
      FROM inbound_msgs im
      WHERE im.user_id = fe.user_id
        AND im.sent_at > fe.farewell_at
        AND (fe.next_farewell_at IS NULL OR im.sent_at < fe.next_farewell_at)
    ) AS reactivated_at
  FROM farewell_events fe
),
active_stock AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT bu.id) FILTER (
      WHERE bu.created_at < wb.week_start_ts
        AND NOT EXISTS (
          SELECT 1
          FROM farewell_cycles fc
          WHERE fc.user_id = bu.id
            AND fc.farewell_at < wb.week_start_ts
            AND (fc.reactivated_at IS NULL OR fc.reactivated_at > wb.week_start_ts)
        )
    )::bigint AS start_active_users
  FROM week_bounds wb
  CROSS JOIN base_users bu
  GROUP BY wb.week_start
),
weekly_became_inactive AS (
  SELECT
    wb.week_start,
    COUNT(DISTINCT fe.user_id)::bigint AS became_inactive_users
  FROM week_bounds wb
  JOIN farewell_events fe
    ON fe.farewell_at >= wb.week_start_ts
   AND fe.farewell_at < wb.week_end_ts
  GROUP BY wb.week_start
)
SELECT
  TO_CHAR(wb.week_start, 'YYYY-MM-DD') AS week_start,
  COALESCE(ast.start_active_users, 0)::bigint AS start_active_users,
  COALESCE(wi.became_inactive_users, 0)::bigint AS became_inactive_users
FROM week_bounds wb
LEFT JOIN active_stock ast ON ast.week_start = wb.week_start
LEFT JOIN weekly_became_inactive wi ON wi.week_start = wb.week_start
ORDER BY wb.week_start DESC;
"""
    return run_query(query)


@st.cache_data(ttl=300)
def get_deep_dive_user_options() -> pd.DataFrame:
    """Load the user selector options for User Deep Dive."""
    return run_query("""
        WITH unique_users AS (
            SELECT DISTINCT ON (waid)
                id,
                COALESCE(full_name, 'Unknown') as full_name,
                waid,
                timezone,
                created_at,
                coach_name,
                metadata,
                tags
            FROM users
            ORDER BY waid, created_at DESC
        ),
        active_users AS (
            SELECT DISTINCT user_id
            FROM messages
            WHERE sender = 'user'
              AND sent_at >= NOW() - INTERVAL '24 hours'
              AND user_id IS NOT NULL
        ),
        user_slogans AS (
            SELECT DISTINCT ON (user_id)
                user_id,
                content->>'slogan' as slogan
            FROM ai_companion_flows
            WHERE type = 'post_onboarding'
              AND content->>'slogan' IS NOT NULL
            ORDER BY user_id, created_at DESC
        )
        SELECT
            u.id,
            u.full_name,
            u.waid,
            u.timezone,
            u.created_at,
            u.coach_name,
            COALESCE(NULLIF(u.metadata->>'mantra', ''), us.slogan) AS slogan,
            CASE WHEN a.user_id IS NOT NULL THEN true ELSE false END AS is_active_24h,
            CASE WHEN COALESCE(jsonb_array_length(COALESCE(u.tags, '[]'::jsonb)), 0) > 0 THEN true ELSE false END AS has_tags,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements_text(COALESCE(u.tags, '[]'::jsonb)) AS t(tag)
                    WHERE LOWER(t.tag) = 'dotz'
                ) THEN true
                ELSE false
            END AS is_dotz
        FROM unique_users u
        LEFT JOIN active_users a ON u.id = a.user_id
        LEFT JOIN user_slogans us ON u.id = us.user_id
        ORDER BY u.full_name ASC
        LIMIT 500
    """)


@st.cache_data(ttl=120)
def get_user_deep_dive_summary(user_id: int) -> pd.DataFrame:
    """Fetch the main User Deep Dive metrics in one query."""
    return run_query(f"""
        WITH user_base AS (
            SELECT
                id,
                waid,
                timezone,
                COALESCE(active_days, 0)::int AS active_days,
                active_days_goal::int AS active_days_goal,
                onboarding_timestamp,
                metadata
            FROM users
            WHERE id = {user_id}
        ),
        last_user_msg AS (
            SELECT MAX(me.sent_at) AS last_user_message_at
            FROM messages me
            JOIN user_base ub ON me.waid = ub.waid
            WHERE me.sender = 'user'
              AND me.type NOT IN ('think', 'tool_use', 'tool_result', 'turn_audit')
        ),
        message_stats AS (
            SELECT
                COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours') AS count_24h,
                COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '3 days') AS count_3d,
                COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days') AS count_7d,
                MAX(sent_at) AS last_user_message_at
            FROM messages
            WHERE user_id = {user_id}
              AND sender = 'user'
              AND sent_at IS NOT NULL
        ),
        last_completed AS (
            SELECT activity_type, completed_at
            FROM user_activities_history
            WHERE user_id = {user_id}
              AND completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 1
        ),
        recovery_summary AS (
            SELECT COUNT(*) AS recovery_count
            FROM recovery_logs
            WHERE user_id = {user_id}
        ),
        last_recovery AS (
            SELECT ladder_step, template_name, sent_at
            FROM recovery_logs
            WHERE user_id = {user_id}
            ORDER BY sent_at DESC
            LIMIT 1
        ),
        last_recovery_rung AS (
            SELECT ladder_step, sent_at
            FROM recovery_logs
            WHERE user_id = {user_id}
              AND (
                ladder_step ~ '^day_[0-9]+_(recovery|random_fun_image)$'
                OR ladder_step IN ('farewell', 'recovery_ladder_1', 'recovery_ladder_2')
              )
            ORDER BY sent_at DESC
            LIMIT 1
        ),
        last_ladder_since_afk AS (
            SELECT rl.ladder_step, rl.sent_at
            FROM recovery_logs rl
            JOIN last_user_msg lum ON true
            WHERE rl.user_id = {user_id}
              AND rl.ladder_step != 'weekly_review'
              AND rl.created_at > lum.last_user_message_at
            ORDER BY rl.created_at DESC
            LIMIT 1
        ),
        recovery_attempts_since_afk AS (
            SELECT COUNT(*)::int AS recovery_attempts
            FROM recovery_logs rl
            JOIN last_user_msg lum ON true
            WHERE rl.user_id = {user_id}
              AND rl.created_at > lum.last_user_message_at
        ),
        first_reply_after_last_recovery AS (
            SELECT m.sent_at
            FROM messages m
            JOIN last_recovery lr ON true
            WHERE m.user_id = {user_id}
              AND m.sender = 'user'
              AND m.sent_at > lr.sent_at
            ORDER BY m.sent_at
            LIMIT 1
        )
        SELECT
            COALESCE(ms.count_24h, 0)::int AS count_24h,
            COALESCE(ms.count_3d, 0)::int AS count_3d,
            COALESCE(ms.count_7d, 0)::int AS count_7d,
            ms.last_user_message_at,
            ub.active_days,
            ub.active_days_goal,
            lc.activity_type AS last_activity_type,
            lc.completed_at AS last_activity_completed_at,
            COALESCE(rs.recovery_count, 0)::int AS recovery_count,
            lr.template_name AS last_recovery_template_name,
            lr.ladder_step AS last_recovery_ladder_step,
            lr.sent_at AS last_recovery_sent_at,
            lrr.ladder_step AS last_rung_step,
            lrr.sent_at AS last_rung_sent_at,
            CASE
                WHEN lum.last_user_message_at IS NULL THEN NULL
                ELSE EXTRACT(DAY FROM NOW() - lum.last_user_message_at)::int
            END AS days_afk,
            llsa.ladder_step AS last_ladder_step_since_afk,
            llsa.sent_at AS last_ladder_step_since_afk_at,
            COALESCE(rasa.recovery_attempts, 0)::int AS recovery_attempts_since_afk,
            (fr.sent_at <= lr.sent_at + INTERVAL '24 hours') AS conv24,
            (fr.sent_at <= lr.sent_at + INTERVAL '72 hours') AS conv72,
            EXTRACT(EPOCH FROM (fr.sent_at - lr.sent_at)) / 3600 AS hours_to_reply,
            (
                ub.onboarding_timestamp IS NOT NULL
                OR EXISTS (
                    SELECT 1
                    FROM events e
                    WHERE e.user_id = {user_id}
                      AND e.event_type = 'onboarding_completed'
                )
            ) AS onboarding_completed,
            (
                NULLIF(ub.metadata->>'mantra', '') IS NOT NULL
                OR EXISTS (
                    SELECT 1
                    FROM ai_companion_flows acf
                    WHERE acf.user_id = {user_id}
                      AND acf.type = 'post_onboarding'
                      AND NULLIF(acf.content->>'slogan', '') IS NOT NULL
                )
            ) AS slogan_set,
            EXISTS (
                SELECT 1
                FROM user_activities_history uah
                WHERE uah.user_id = {user_id}
                  AND uah.completed_at IS NOT NULL
            ) AS first_activity_completed
        FROM user_base ub
        CROSS JOIN message_stats ms
        CROSS JOIN recovery_summary rs
        LEFT JOIN last_completed lc ON true
        LEFT JOIN last_recovery lr ON true
        LEFT JOIN last_recovery_rung lrr ON true
        LEFT JOIN last_user_msg lum ON true
        LEFT JOIN last_ladder_since_afk llsa ON true
        LEFT JOIN recovery_attempts_since_afk rasa ON true
        LEFT JOIN first_reply_after_last_recovery fr ON true
    """)


@st.cache_data(ttl=120)
def get_user_llm_cost_metrics(user_id: int) -> pd.DataFrame:
    """Per-user LLM cost from turn_audit rows (lifetime, last 7d, avg/day)."""
    return run_query(f"""
        WITH user_info AS (
            SELECT id, timezone, created_at
            FROM users
            WHERE id = {user_id}
        ),
        llm_turns AS (
            SELECT
                (ta.message::jsonb ->> 'usd')::numeric AS usd_cost,
                ta.sent_at AS date_time_utc
            FROM messages ta
            WHERE ta.user_id = {user_id}
              AND ta.type = 'turn_audit'
              AND ta.message IS NOT NULL
              AND ta.message LIKE '{{%'
        )
        SELECT
            COALESCE(SUM(lt.usd_cost), 0) AS lifetime_usd,
            COALESCE(
                SUM(lt.usd_cost) FILTER (WHERE lt.date_time_utc >= NOW() - INTERVAL '7 days'),
                0
            ) AS last_7d_usd,
            GREATEST(
                ((CURRENT_TIMESTAMP AT TIME ZONE COALESCE(ui.timezone, 'America/Sao_Paulo'))::date
                    - (ui.created_at AT TIME ZONE COALESCE(ui.timezone, 'America/Sao_Paulo'))::date
                    + 1),
                1
            ) AS tenure_days
        FROM user_info ui
        LEFT JOIN llm_turns lt ON true
        GROUP BY ui.timezone, ui.created_at
    """)


@st.cache_data(ttl=120)
def get_user_activity_plan(user_id: int) -> pd.DataFrame:
    """Fetch current activity plan rows for a user."""
    return run_query(f"""
        SELECT description, days, created_at
        FROM user_activities
        WHERE user_id = {user_id}
          AND in_progress = true
    """)


@st.cache_data(ttl=120)
def get_user_message_history(user_id: int, limit: int | None) -> pd.DataFrame:
    """Fetch message history for a user, tagged with the nearest recovery_logs send."""
    msg_limit_sql = f"LIMIT {int(limit)}" if limit else ""
    return run_query(f"""
        SELECT m.id as msg_id, m.sent_at, m.sender, m.type as msg_type, m.message, m.status,
               rl.ladder_step as matched_ladder_step
        FROM messages m
        LEFT JOIN LATERAL (
            SELECT rl.ladder_step
            FROM recovery_logs rl
            WHERE rl.user_id = {user_id}
              AND rl.sent_at BETWEEN m.sent_at - INTERVAL '120 seconds'
                                 AND m.sent_at + INTERVAL '120 seconds'
            ORDER BY ABS(EXTRACT(EPOCH FROM (rl.sent_at - m.sent_at)))
            LIMIT 1
        ) rl ON true
        WHERE m.user_id = {user_id} AND m.sent_at IS NOT NULL
          {get_user_visible_message_filter_sql("m")}
        ORDER BY m.sent_at DESC
        {msg_limit_sql}
    """)


@st.cache_data(ttl=120)
def get_user_recovery_response_by_type(user_id: int) -> pd.DataFrame:
    """
    Per-user response rate to recovery rungs, split into 'Recovery message'
    (day_N_recovery) vs 'Fun image' (day_N_random_fun_image). Farewell excluded.

    Windowed attribution: a send counts as responded if the user sent a message
    after it and before their next rung (any type). Returns columns:
    step_type, sends, responded.
    """
    return run_query(f"""
        WITH sends AS (
          SELECT
            CASE
              WHEN r.ladder_step ~ '_random_fun_image$'     THEN 'Fun image'
              WHEN r.ladder_step ~ '^day_[0-9]+_recovery$'  THEN 'Recovery message'
            END AS step_type,
            r.sent_at,
            LEAD(r.sent_at) OVER (ORDER BY r.sent_at) AS next_sent_at
          FROM recovery_logs r
          WHERE r.user_id = {user_id}
            AND r.ladder_step ~ '^day_[0-9]+_(recovery|random_fun_image)$'
        )
        SELECT
          s.step_type,
          COUNT(*)::int AS sends,
          COUNT(*) FILTER (WHERE EXISTS (
            SELECT 1 FROM messages m
            WHERE m.user_id = {user_id} AND m.sender = 'user'
              AND m.sent_at > s.sent_at
              AND (s.next_sent_at IS NULL OR m.sent_at < s.next_sent_at)
          ))::int AS responded
        FROM sends s
        WHERE s.step_type IS NOT NULL
        GROUP BY s.step_type
    """)


@st.cache_data(ttl=120)
def get_user_message_hour_counts(user_id: int, user_timezone: str | None) -> pd.DataFrame:
    """Return user message counts by local hour without fetching every timestamp."""
    tz = str(user_timezone or "").strip()
    interval_hours = None
    match = re.match(r"^(?:UTC|GMT)?([+-]\d{1,2})(?::?(\d{2}))?$", tz, re.IGNORECASE)
    if match:
        hours = int(match.group(1))
        minutes = int(match.group(2) or 0)
        interval_hours = hours + (minutes / 60.0 if hours >= 0 else -minutes / 60.0)

    if interval_hours is not None:
        hour_expr = f"EXTRACT(HOUR FROM ((sent_at AT TIME ZONE 'UTC') + INTERVAL '{interval_hours} hours'))"
    elif tz:
        safe_tz = tz.replace("'", "''")
        hour_expr = f"EXTRACT(HOUR FROM sent_at AT TIME ZONE '{safe_tz}')"
    else:
        hour_expr = "EXTRACT(HOUR FROM sent_at AT TIME ZONE 'UTC')"

    return run_query(f"""
        SELECT {hour_expr}::int AS hour, COUNT(*)::int AS message_count
        FROM messages
        WHERE user_id = {user_id}
          AND sender = 'user'
          AND sent_at IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """)


def is_template(raw_msg) -> bool:
    """Check if a message payload is a WhatsApp template message.
    
    Templates are identified by a "notification" key in the JSON payload,
    e.g. {"notification": {"name": "template_name", "locale": "pt_br", ...}}
    """
    if pd.isna(raw_msg) or raw_msg is None:
        return False
    msg_str = str(raw_msg).strip()
    try:
        data = json.loads(msg_str)
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                pass
        if isinstance(data, dict):
            # WhatsApp template messages have a "notification" key
            if 'notification' in data:
                return True
            # Also check for legacy/alternative template indicators
            if 'template' in data:
                return True
            if data.get('type') == 'template':
                return True
    except Exception:
        return False
    return False


def _extract_message_text_snippet(raw_msg, max_len=120):
    """Extract readable text from message JSON for display; return truncated snippet."""
    if pd.isna(raw_msg) or raw_msg is None:
        return ""
    msg_str = str(raw_msg).strip()
    try:
        data = json.loads(msg_str)
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                pass
    except Exception:
        return (msg_str[:max_len] + "…") if len(msg_str) > max_len else msg_str

    def find_text(obj, depth=0):
        if depth > 8 or obj is None:
            return None
        if isinstance(obj, str) and len(obj) > 2:
            return obj
        if isinstance(obj, dict):
            for key in ["text", "body", "title", "message", "content", "caption", "label", "description", "value"]:
                if key in obj:
                    val = obj[key]
                    if isinstance(val, str) and len(val) > 2:
                        return val
                    found = find_text(val, depth + 1)
                    if found:
                        return found
            for val in obj.values():
                if isinstance(val, (dict, list, str)):
                    found = find_text(val, depth + 1)
                    if found:
                        return found
        if isinstance(obj, list):
            for item in obj:
                found = find_text(item, depth + 1)
                if found:
                    return found
        return None

    found = None
    if isinstance(data, dict):
        for key in ["interactive", "postback", "template", "flows", "quickReply"]:
            if key in data:
                found = find_text(data[key])
                if found:
                    break
        if not found:
            found = find_text(data)
    else:
        found = find_text(data) if data is not None else None
    if not found and isinstance(data, str) and len(data) > 2:
        found = data
    text = (found or msg_str).strip()
    return (text[:max_len] + "…") if len(text) > max_len else text


def _parse_timezone(tz_str):
    """Parse timezone string like 'UTC-3', '-3', 'America/Sao_Paulo' to a tzinfo."""
    if not tz_str or pd.isna(tz_str):
        return None
    tz_str = str(tz_str).strip()
    try:
        return pytz.timezone(tz_str)
    except Exception:
        pass
    match = re.search(r"([+-]?)(\d{1,2})(?::(\d{2}))?", tz_str)
    if match:
        if "UTC-" in tz_str or "GMT-" in tz_str or tz_str.startswith("-"):
            sign = -1
        elif "UTC+" in tz_str or "GMT+" in tz_str or tz_str.startswith("+"):
            sign = 1
        else:
            sign = -1 if match.group(1) == "-" else 1
        hours = int(match.group(2)) * sign
        minutes = int(match.group(3) or 0)
        return timezone(timedelta(hours=hours, minutes=minutes))
    return None


def _format_ts_local(ts, tz_str, fmt="%Y-%m-%d %H:%M"):
    """Format a UTC timestamp in the user's local timezone."""
    if pd.isna(ts):
        return "—"
    try:
        t = pd.to_datetime(ts, utc=True)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        user_tz = _parse_timezone(tz_str)
        if user_tz is not None:
            t = t.tz_convert(user_tz)
            return t.strftime(fmt)
        return t.strftime(fmt) + " UTC"
    except Exception:
        return str(ts)[:16] if ts is not None else "—"


def _journey_blob(prev_pct_val, better: bool | None, worse: bool | None) -> str:
    """HTML caption comparing a metric to the previous 7-day window."""
    if better:
        color = "#0d7d0d"
    elif worse:
        color = "#c52222"
    else:
        color = "#6b7280"
    return f'<span style="font-size:0.9em;color:{color}">Prev 7d: {prev_pct_val}%</span>'


def _metric_delta(curr, prev, suffix="", precision=1):
    """Format a +/- delta string for st.metric, or '—' when comparison unavailable."""
    if curr is None or pd.isna(curr) or prev is None or pd.isna(prev):
        return "—"
    diff = curr - prev
    sign = "+" if diff >= 0 else ""
    return f"{sign}{round(diff, precision)}{suffix}"


def _format_pending_duration(delta):
    """Format a timedelta as e.g. '2h 15m' or '45m' for reply-pending display."""
    if delta is None or (hasattr(delta, "total_seconds") and delta.total_seconds() <= 0):
        return "—"
    total_secs = int(delta.total_seconds()) if hasattr(delta, "total_seconds") else int(delta)
    if total_secs < 60:
        return "<1m"
    mins, secs = divmod(total_secs, 60)
    hrs, mins = divmod(mins, 60)
    days, hrs = divmod(hrs, 24)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hrs:
        parts.append(f"{hrs}h")
    parts.append(f"{mins}m")
    return " ".join(parts)


# =============================================================================
# PREDEFINED QUERIES - Edit these to customize your dashboard
# =============================================================================

QUERIES = {
    "📊 Overview Stats": """
-- Quick overview of key counts (deduplicated by waid)
SELECT 
    (SELECT COUNT(DISTINCT waid) FROM users) as total_users,
    (SELECT COUNT(DISTINCT waid) FROM users WHERE is_active = true) as active_users,
    (SELECT COUNT(DISTINCT waid) FROM users WHERE created_at > NOW() - INTERVAL '7 days') as new_users_7d,
    (SELECT COUNT(DISTINCT waid) FROM users WHERE created_at > NOW() - INTERVAL '24 hours') as new_users_24h,
    (SELECT COUNT(*) FROM user_activities_history WHERE completed_at IS NOT NULL) as completed_activities,
    (SELECT COUNT(*) FROM user_milestones WHERE completed = true) as completed_milestones
""",
    
    "👥 All Users": """
-- List all unique users (deduplicated by waid, ordered by most recent)
SELECT * FROM (
    SELECT DISTINCT ON (waid)
        id,
        waid,
        full_name,
        gender,
        pillar,
        level,
        phase,
        is_active,
        timezone,
        created_at,
        onboarding_timestamp
    FROM users
    ORDER BY waid, created_at DESC
) unique_users
ORDER BY created_at DESC
LIMIT 100
""",

    "👤 User Details (by ID)": """
-- Get full details for a specific user (change user_id)
SELECT *
FROM users
WHERE id = 3  -- Change this ID
""",
    
    "📱 Recent Events": """
-- Recent user events/activities
SELECT 
    e.id,
    e.user_id,
    u.full_name,
    e.event_type,
    e.description,
    e.executed_at,
    e.created_at
FROM events e
LEFT JOIN users u ON e.user_id = u.id
ORDER BY e.created_at DESC
LIMIT 50
""",

    "💬 Recent Messages": """
-- Recent WhatsApp messages
SELECT 
    m.id,
    m.user_id,
    u.full_name,
    m.sender,
    m.type,
    m.message,
    m.sent_at,
    m.status
FROM messages m
LEFT JOIN users u ON m.user_id = u.id
ORDER BY m.sent_at DESC
LIMIT 50
""",

    "🎯 User Goals & Pillars": """
-- Users by pillar and goal (deduplicated by waid)
SELECT 
    pillar,
    goal,
    level,
    COUNT(DISTINCT waid) as user_count
FROM users
GROUP BY pillar, goal, level
ORDER BY user_count DESC
""",

    "✅ Completed Activities": """
-- Activities completed by users
SELECT 
    ua.user_activity_id,
    ua.user_id,
    u.full_name,
    ua.activity_type,
    ua.completed_at,
    ua.completion_method,
    ua.xp_earned,
    ua.created_at
FROM user_activities_history ua
LEFT JOIN users u ON ua.user_id = u.id
WHERE ua.completed_at IS NOT NULL
ORDER BY ua.completed_at DESC
LIMIT 50
""",

    "🏆 Milestone Progress": """
-- User milestone completions
SELECT 
    um.id,
    um.user_id,
    u.full_name,
    m.milestone,
    m.type as milestone_type,
    um.completed,
    um.created_at
FROM user_milestones um
LEFT JOIN users u ON um.user_id = u.id
LEFT JOIN milestones m ON um.milestone_id = m.id
ORDER BY um.created_at DESC
LIMIT 50
""",

    "📈 Daily Signups": """
-- Signups by day (deduplicated by waid)
SELECT 
    DATE(created_at) as signup_date,
    COUNT(DISTINCT waid) as signups
FROM users
GROUP BY DATE(created_at)
ORDER BY signup_date DESC
LIMIT 30
""",

    "📊 Users by Pillar": """
-- Distribution of users by pillar (deduplicated by waid)
SELECT 
    pillar,
    COUNT(DISTINCT waid) as count,
    ROUND(100.0 * COUNT(DISTINCT waid) / SUM(COUNT(DISTINCT waid)) OVER (), 1) as percentage
FROM users
WHERE pillar IS NOT NULL
GROUP BY pillar
ORDER BY count DESC
""",

    "📊 Users by Gender": """
-- Distribution of users by gender (deduplicated by waid)
SELECT 
    gender,
    COUNT(DISTINCT waid) as count,
    ROUND(100.0 * COUNT(DISTINCT waid) / SUM(COUNT(DISTINCT waid)) OVER (), 1) as percentage
FROM users
WHERE gender IS NOT NULL
GROUP BY gender
ORDER BY count DESC
""",

    "⏰ Activity by Hour": """
-- Message activity by hour of day
SELECT 
    EXTRACT(HOUR FROM sent_at) as hour,
    COUNT(*) as message_count
FROM messages
WHERE sent_at IS NOT NULL
GROUP BY EXTRACT(HOUR FROM sent_at)
ORDER BY hour
""",

    "🔄 User Phases": """
-- Users by onboarding/journey phase (deduplicated by waid)
SELECT 
    phase,
    COUNT(DISTINCT waid) as user_count
FROM users
GROUP BY phase
ORDER BY phase
""",

    "🌍 User Timezones": """
-- See what timezone values are stored for users
SELECT 
    timezone,
    COUNT(DISTINCT waid) as user_count
FROM users
WHERE timezone IS NOT NULL
GROUP BY timezone
ORDER BY user_count DESC
""",


    "📅 Daily Active Users": """
-- Daily active users who SENT messages (deduplicated by waid)
SELECT 
    DATE(m.sent_at) as date,
    COUNT(DISTINCT u.waid) as active_users
FROM messages m
LEFT JOIN users u ON m.user_id = u.id
WHERE m.sent_at >= NOW() - INTERVAL '30 days' 
  AND m.user_id IS NOT NULL
  AND m.sender = 'user'
GROUP BY DATE(m.sent_at)
ORDER BY date DESC
""",

    "🧑 Today's Active Users": """
-- Users who SENT messages today (deduplicated by waid)
SELECT 
    u.waid,
    u.full_name,
    u.pillar,
    COUNT(*) as messages_sent
FROM messages m
LEFT JOIN users u ON m.user_id = u.id
WHERE m.sent_at >= CURRENT_DATE 
  AND m.user_id IS NOT NULL
  AND m.sender = 'user'
GROUP BY u.waid, u.full_name, u.pillar
ORDER BY messages_sent DESC
""",
}


# =============================================================================
# MAIN DASHBOARD
# =============================================================================

st.markdown('<p class="main-header">LETZ Dashboard</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">User activity & product insights</p>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### 🗄️ Database Explorer")
    
    # Test connection
    conn = get_connection()
    if conn:
        st.success("✓ Connected to database")
    else:
        st.error("✗ Not connected")
        st.info("Check your .env file")
    
    st.markdown("---")
    st.markdown("### ⚙️ Settings")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()


# Main content navigation. Streamlit tabs execute every tab eagerly, so use a
# conditional selector to avoid running hidden tabs' database queries.
selected_section = st.radio(
    "Dashboard section",
    [
        "📊 Quick Insights",
        "💰 Cost",
        "🔍 User Deep Dive",
        "📈 User Retention",
        "🔔 Alerts",
        "🪜 Recovery Ladder",
    ],
    horizontal=True,
    label_visibility="collapsed",
)


# Tab 1: Quick Insights
if selected_section == "📊 Quick Insights":
    # Quick Insights always excludes internal users
    exclude_internal = True
    
    st.markdown("---")
    
    # Headline metrics (all by waid, excluding internal users)
    internal_filter = get_internal_users_filter_sql(exclude_internal)
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    beta_users_cte = get_beta_users_cte()
    try:
        headline_df = get_quick_insights_headline_metrics(exclude_internal)
        headline = headline_df.iloc[0] if not headline_df.empty else {}
        onboarded_users_count = int(headline.get("onboarded_users_count", 0))
        alive_count = int(headline.get("alive_count", 0))
        new_7d_count = int(headline.get("new_7d_count", 0))
        churned_7d_count = int(headline.get("churned_7d_count", 0))
        churned_7d_came_back = int(headline.get("churned_7d_came_back", 0))
        inside_24h = int(headline.get("inside_24h", 0))
        messaged_today = int(headline.get("messaged_today", 0))
        completed_today = int(headline.get("completed_today", 0))
        at_risk_5d_count = int(headline.get("at_risk_5d_count", 0))
        active_users_5d_count = int(headline.get("active_users_5d_count", 0))
        alive_avg_active_days = float(headline.get("alive_avg_active_days", 0) or 0)
        alive_total_active_days = int(headline.get("alive_total_active_days", 0) or 0)
        at_risk_avg_active_days = float(headline.get("at_risk_avg_active_days", 0) or 0)
        at_risk_total_active_days = int(headline.get("at_risk_total_active_days", 0) or 0)
        churned_avg_active_days = float(headline.get("churned_avg_active_days", 0) or 0)
        churned_total_active_days = int(headline.get("churned_total_active_days", 0) or 0)
        churned_lifetime_avg_active_days = float(headline.get("churned_lifetime_avg_active_days", 0) or 0)
        churned_lifetime_total_active_days = int(headline.get("churned_lifetime_total_active_days", 0) or 0)
        churned_lifetime_count = int(headline.get("churned_lifetime_count", 0) or 0)
    except Exception:
        onboarded_users_count = alive_count = new_7d_count = churned_7d_count = churned_7d_came_back = 0
        inside_24h = messaged_today = completed_today = at_risk_5d_count = active_users_5d_count = 0
        alive_avg_active_days = alive_total_active_days = 0
        at_risk_avg_active_days = at_risk_total_active_days = 0
        churned_avg_active_days = churned_total_active_days = 0
        churned_lifetime_avg_active_days = churned_lifetime_total_active_days = churned_lifetime_count = 0

    alive_pct = round(100 * alive_count / onboarded_users_count, 1) if onboarded_users_count else 0
    new_7d_pct = round(100 * new_7d_count / onboarded_users_count, 1) if onboarded_users_count else 0
    churned_7d_pct = round(100 * churned_7d_count / onboarded_users_count, 1) if onboarded_users_count else 0
    pct_inside_24h = round(100 * inside_24h / onboarded_users_count, 1) if onboarded_users_count else 0
    pct_messaged_today = round(100 * messaged_today / onboarded_users_count, 1) if onboarded_users_count else 0
    pct_activity_complete = round(100 * completed_today / onboarded_users_count, 1) if onboarded_users_count else 0
    at_risk_5d_pct = round(100 * at_risk_5d_count / onboarded_users_count, 1) if onboarded_users_count else 0
    active_users_5d_pct = round(100 * active_users_5d_count / onboarded_users_count, 1) if onboarded_users_count else 0

    # ── DAU / MAU data loading (charts) ───────────────────────────────────────
    try:
        _dau_df = get_dau_metrics(exclude_internal)
        _dau_daily = _dau_df[_dau_df["row_type"] == "daily"].copy() if not _dau_df.empty else pd.DataFrame()
        _dau_ratio_daily = _dau_df[_dau_df["row_type"] == "ratio_daily"].copy() if not _dau_df.empty else pd.DataFrame()
        _dau_weekly = _dau_df[_dau_df["row_type"] == "weekly"].copy() if not _dau_df.empty else pd.DataFrame()
        _dau_daily["dau"] = pd.to_numeric(_dau_daily["dau"], errors="coerce").fillna(0)
        _dau_daily["mau"] = pd.to_numeric(_dau_daily["mau"], errors="coerce").fillna(0)
        _dau_daily = _dau_daily.sort_values("activity_date")
    except Exception as _dau_err:
        _dau_daily = _dau_weekly = _dau_ratio_daily = pd.DataFrame()
        st.warning(f"Could not load DAU metrics: {_dau_err}")

    # ── 1. Engagement comparison table ───────────────────────────────────────
    def _fmt_engagement_cell(row, prefix: str) -> str:
        value = row.get(f"{prefix}_value")
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "—"
        value_type = row.get("value_type")
        if value_type == "retention":
            num = row.get(f"{prefix}_numerator")
            den = row.get(f"{prefix}_denominator")
            if num is not None and den is not None and not pd.isna(num) and not pd.isna(den):
                return f"{float(value):.1f}% ({int(num)} / {int(den)})"
            return f"{float(value):.1f}%"
        if value_type == "pct":
            return f"{float(value):.1f}%"
        return f"{float(value):.1f}"

    def _engagement_delta_html(curr, prev) -> str:
        if curr is None or prev is None or pd.isna(curr) or pd.isna(prev):
            return '<span style="color:#888;">—</span>'
        diff = float(curr) - float(prev)
        if abs(diff) < 1e-9:
            return '<span style="color:#888;">0.0</span>'
        # Higher is better for all rows in this table
        color = "#00d4aa" if diff > 0 else "#ff6b6b"
        arrow = "▲" if diff > 0 else "▼"
        return f'<span style="color:{color};font-weight:600;">{arrow} {diff:+.1f}</span>'

    try:
        _eng_df = get_quick_insights_engagement_table(exclude_internal)
    except Exception as _eng_err:
        _eng_df = pd.DataFrame()
        st.warning(f"Could not load engagement comparison: {_eng_err}")

    st.markdown("#### KPI")
    if not _eng_df.empty:
        _eng_rows_html = []
        for _, _row in _eng_df.iterrows():
            _curr = _fmt_engagement_cell(_row, "current")
            _prev = _fmt_engagement_cell(_row, "previous")
            _delta = _engagement_delta_html(_row.get("current_value"), _row.get("previous_value"))
            _eng_rows_html.append(
                "<tr>"
                f"<td style='padding:12px 14px;border-bottom:1px solid #2a2a2a;font-size:1.15rem;'>{_row['metric']}</td>"
                f"<td style='padding:12px 14px;border-bottom:1px solid #2a2a2a;text-align:right;font-size:1.15rem;'>{_curr}</td>"
                f"<td style='padding:12px 14px;border-bottom:1px solid #2a2a2a;text-align:right;font-size:1.15rem;'>{_prev}</td>"
                f"<td style='padding:12px 14px;border-bottom:1px solid #2a2a2a;text-align:right;font-size:1.15rem;'>{_delta}</td>"
                "</tr>"
            )
        st.markdown(
            "<table style='width:100%;border-collapse:collapse;margin:8px 0 4px 0;font-size:1.15rem;'>"
            "<thead><tr>"
            "<th style='text-align:left;padding:12px 14px;border-bottom:1px solid #444;color:#aaa;font-weight:500;font-size:1.05rem;'>Metric</th>"
            "<th style='text-align:right;padding:12px 14px;border-bottom:1px solid #444;color:#aaa;font-weight:500;font-size:1.05rem;'>Last 7D</th>"
            "<th style='text-align:right;padding:12px 14px;border-bottom:1px solid #444;color:#aaa;font-weight:500;font-size:1.05rem;'>Previous 7D</th>"
            "<th style='text-align:right;padding:12px 14px;border-bottom:1px solid #444;color:#aaa;font-weight:500;font-size:1.05rem;'>Δ</th>"
            "</tr></thead>"
            f"<tbody>{''.join(_eng_rows_html)}</tbody>"
            "</table>",
            unsafe_allow_html=True,
        )
        st.caption(
            "* Retention definition: among users who completed a first activity with Letz and had enough "
            "time to mature into the relevant window. 7D counts a second activity within 7 days of the first; "
            "14D and 30D count any subsequent activity after day 14 or 30, respectively. External users only."
        )
    else:
        st.info("No engagement comparison data available yet.")

    st.markdown("---")

    # ── 2. Onboarded → at-risk stats row ─────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Onboarded users", onboarded_users_count if onboarded_users_count else "—")
    col1.caption("Completed onboarding flow")
    col2.metric("New users (last 7d)", new_7d_count if new_7d_count is not None else "—")
    col2.caption(f"↳ {new_7d_pct}% of onboarded users")
    col3.metric("Alive users", alive_count if alive_count is not None else "—")
    col3.caption(f"↳ {alive_pct}% of onboarded users")
    col4.metric("Active users", active_users_5d_count if active_users_5d_count is not None else "—")
    col4.caption(f"Activity in past 5d · ↳ {active_users_5d_pct}% of onboarded")
    col5.metric("At risk users", at_risk_5d_count if at_risk_5d_count is not None else "—")
    col5.caption(f"No message in past 5d · ↳ {at_risk_5d_pct}% of onboarded")

    st.markdown("---")

    # ── 3. DAU daily comparison + DAU/MAU trend ──────────────────────────────
    st.markdown("#### 📈 DAU — last 7d vs previous 7d")
    st.caption("Distinct external users completing ≥1 activity per local day. Lines align day 1–7 in each 7-day period.")
    if len(_dau_daily) >= 14:
        try:
            import altair as alt

            _dau_compare = _dau_daily.copy().sort_values("activity_date").reset_index(drop=True)
            _dau_compare["activity_dt"] = pd.to_datetime(_dau_compare["activity_date"], errors="coerce")
            _dau_compare["period"] = ["Previous 7d"] * 7 + ["Last 7d"] * 7
            _dau_compare["day_in_period"] = (_dau_compare.index % 7) + 1
            _dau_compare["date_label"] = _dau_compare["activity_dt"].dt.strftime("%d %b")

            _dau_chart = (
                alt.Chart(_dau_compare)
                .mark_line(point=True)
                .encode(
                    x=alt.X("day_in_period:O", title="Day in 7-day period"),
                    y=alt.Y("dau:Q", title="DAU", scale=alt.Scale(zero=True)),
                    color=alt.Color(
                        "period:N",
                        title="Period",
                        scale=alt.Scale(domain=["Last 7d", "Previous 7d"], range=["#00d4aa", "#7aa2ff"]),
                    ),
                    tooltip=[
                        alt.Tooltip("period:N", title="Period"),
                        alt.Tooltip("date_label:N", title="Date"),
                        alt.Tooltip("dau:Q", title="DAU", format=".0f"),
                    ],
                )
                .properties(height=280)
            )
            st.altair_chart(_dau_chart, use_container_width=True)
        except Exception as _dau_chart_err:
            st.warning(f"Could not render DAU chart: {_dau_chart_err}")
    else:
        st.info("Need at least 14 days of activity data for DAU comparison.")

    st.markdown("#### DAU/MAU — rolling 30d trend")
    st.caption(
        "Daily DAU ÷ that day's rolling 30-day MAU. The bold line is a 7-day moving average. "
        "The engagement table above uses avg DAU ÷ avg rolling MAU over each 7-day window."
    )
    if not _dau_ratio_daily.empty:
        try:
            import altair as alt

            _ratio = _dau_ratio_daily.copy().sort_values("activity_date")
            _ratio["activity_dt"] = pd.to_datetime(_ratio["activity_date"], errors="coerce")
            _ratio["dau"] = pd.to_numeric(_ratio["dau"], errors="coerce").fillna(0)
            _ratio["mau"] = pd.to_numeric(_ratio["mau"], errors="coerce").fillna(0)
            _ratio["dau_mau"] = _ratio.apply(lambda row: row["dau"] / row["mau"] if row["mau"] else 0, axis=1)
            _ratio["dau_mau_7d_avg"] = _ratio["dau_mau"].rolling(window=7, min_periods=1).mean()

            _ratio_base = alt.Chart(_ratio).encode(
                x=alt.X("activity_dt:T", title="Date"),
                tooltip=[
                    alt.Tooltip("activity_dt:T", title="Date", format="%d %b"),
                    alt.Tooltip("dau:Q", title="DAU", format=".0f"),
                    alt.Tooltip("mau:Q", title="Rolling 30d MAU", format=".0f"),
                    alt.Tooltip("dau_mau:Q", title="DAU/MAU", format=".1%"),
                    alt.Tooltip("dau_mau_7d_avg:Q", title="7d avg", format=".1%"),
                ],
            )
            _ratio_daily_line = _ratio_base.mark_line(opacity=0.35, color="#7aa2ff").encode(
                y=alt.Y("dau_mau:Q", title="DAU/MAU", axis=alt.Axis(format="%"), scale=alt.Scale(zero=True))
            )
            _ratio_smoothed_line = _ratio_base.mark_line(point=False, strokeWidth=3, color="#00d4aa").encode(
                y=alt.Y("dau_mau_7d_avg:Q", title="DAU/MAU", axis=alt.Axis(format="%"), scale=alt.Scale(zero=True))
            )
            st.altair_chart((_ratio_daily_line + _ratio_smoothed_line).properties(height=280), use_container_width=True)
        except Exception as _ratio_chart_err:
            st.warning(f"Could not render DAU/MAU chart: {_ratio_chart_err}")
    else:
        st.info("No activity data available for DAU/MAU trend.")

    st.markdown("---")
    
    # Recent messages section
    st.markdown("### 💬 Recent Messages")
    
    # Show translation status (less intrusive)
    if GoogleTranslator is None:
        st.caption("ℹ️ Translation unavailable - install `deep-translator` to enable")
    translate_recent_messages = st.checkbox(
        "Translate recent messages to English",
        value=False,
        key="recent_messages_translate",
        help="Disabled by default because translating every row can slow the Quick Insights tab.",
    )
    wrap_recent_messages = st.checkbox(
        "Wrap recent messages for screenshots",
        value=False,
        key="recent_messages_wrap",
        help="Shows the same table with wrapped message text so it fits in screenshots.",
    )
    
    # Time range selector
    time_range = st.selectbox(
        "Filter by time range:",
        ["Last 20 messages", "Last 1 hour", "Last 24 hours"],
        key="recent_messages_range"
    )
    
    # Build query based on selected time range
    # Note: Using CURRENT_TIMESTAMP for timezone-aware comparison
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    
    recent_messages_user_select = """
                COALESCE(u.id, m.user_id) as user_id,
                u.full_name as user_name,
                COALESCE(u.waid, m.waid) as user_waid,
                u.tags as user_tags,
                u.timezone as user_timezone"""

    recent_messages_base_from = f"""
            FROM messages m
            LEFT JOIN users u ON (m.user_id = u.id OR m.waid = u.waid)
            LEFT JOIN LATERAL (
                SELECT rl.ladder_step
                FROM recovery_logs rl
                WHERE rl.user_id = COALESCE(u.id, m.user_id)
                  AND rl.sent_at BETWEEN m.sent_at - INTERVAL '120 seconds'
                                     AND m.sent_at + INTERVAL '120 seconds'
                ORDER BY ABS(EXTRACT(EPOCH FROM (rl.sent_at - m.sent_at)))
                LIMIT 1
            ) rl ON true
            WHERE m.sent_at IS NOT NULL
              {{time_filter}}
              {{message_filter}}
              {{internal_filter}}"""

    if time_range == "Last 20 messages":
        query = f"""
            SELECT *
            FROM (
                SELECT DISTINCT ON (m.id)
                    m.id as msg_id,
                    m.sent_at as timestamp,
                    m.type as msg_type,
                    {recent_messages_user_select},
                    m.sender,
                    m.message as raw_message,
                    m.status,
                    rl.ladder_step as matched_ladder_step
                {recent_messages_base_from.format(
                    time_filter="",
                    message_filter=get_user_visible_message_filter_sql("m"),
                    internal_filter=internal_filter_join if exclude_internal and internal_filter_join else "",
                )}
                ORDER BY m.id, u.created_at DESC NULLS LAST
            ) recent_messages_deduped
            ORDER BY timestamp DESC
            LIMIT 20
        """
    elif time_range == "Last 1 hour":
        query = f"""
            SELECT *
            FROM (
                SELECT DISTINCT ON (m.id)
                    m.id as msg_id,
                    m.sent_at as timestamp,
                    m.type as msg_type,
                    {recent_messages_user_select},
                    m.sender,
                    m.message as raw_message,
                    m.status,
                    rl.ladder_step as matched_ladder_step
                {recent_messages_base_from.format(
                    time_filter="AND m.sent_at >= NOW() - INTERVAL '1 hour'",
                    message_filter=get_user_visible_message_filter_sql("m"),
                    internal_filter=internal_filter_join if exclude_internal and internal_filter_join else "",
                )}
                ORDER BY m.id, u.created_at DESC NULLS LAST
            ) recent_messages_deduped
            ORDER BY timestamp DESC
        """
    else:  # Last 24 hours
        query = f"""
            SELECT *
            FROM (
                SELECT DISTINCT ON (m.id)
                    m.id as msg_id,
                    m.sent_at as timestamp,
                    m.type as msg_type,
                    {recent_messages_user_select},
                    m.sender,
                    m.message as raw_message,
                    m.status,
                    rl.ladder_step as matched_ladder_step
                {recent_messages_base_from.format(
                    time_filter="AND m.sent_at >= NOW() - INTERVAL '24 hours'",
                    message_filter=get_user_visible_message_filter_sql("m"),
                    internal_filter=internal_filter_join if exclude_internal and internal_filter_join else "",
                )}
                ORDER BY m.id, u.created_at DESC NULLS LAST
            ) recent_messages_deduped
            ORDER BY timestamp DESC
        """
    
    recent_messages = run_query(query)
    
    # Show message count for debugging
    if not recent_messages.empty:
        st.caption(f"Showing {len(recent_messages)} message(s)")
    
    if not recent_messages.empty:
        # Process messages to extract readable text from JSON
        def extract_message_text(raw_msg):
            """Extract the most human-readable text from a message payload.

            NOTE: this intentionally returns the full text without truncation
            so Recent Messages always shows the complete content.
            """
            if pd.isna(raw_msg) or raw_msg is None:
                return ""
            msg_str = str(raw_msg).strip()
            
            def parse_json(s):
                try:
                    data = json.loads(s)
                    if isinstance(data, str):  # handle double-encoded
                        try:
                            return json.loads(data)
                        except Exception:
                            return data
                    return data
                except Exception:
                    return None
            
            def find_text(obj, depth=0):
                if depth > 10 or obj is None:
                    return None
                if isinstance(obj, str) and len(obj) > 2:
                    return obj
                if isinstance(obj, dict):
                    for key in ["text", "body", "title", "message", "content", "caption", "label", "description", "value"]:
                        if key in obj:
                            val = obj[key]
                            if isinstance(val, str) and len(val) > 2:
                                return val
                            found = find_text(val, depth + 1)
                            if found:
                                return found
                    # payloads often carry text
                    if "payload" in obj:
                        payload = obj["payload"]
                        if isinstance(payload, str):
                            pj = parse_json(payload)
                            if isinstance(pj, (dict, list)):
                                found = find_text(pj, depth + 1)
                                if found:
                                    return found
                            if len(payload) > 2:
                                return payload
                        else:
                            found = find_text(payload, depth + 1)
                            if found:
                                return found
                    for val in obj.values():
                        if isinstance(val, (dict, list, str)):
                            found = find_text(val, depth + 1)
                            if found:
                                return found
                if isinstance(obj, list):
                    for item in obj:
                        found = find_text(item, depth + 1)
                        if found:
                            return found
                return None
            
            data = parse_json(msg_str)
            
            if isinstance(data, dict):
                # WhatsApp template notifications: prefer body parameter text (actual template content)
                notification = data.get("notification")
                if isinstance(notification, dict):
                    template_texts = []
                    for comp in notification.get("components", []) or []:
                        if isinstance(comp, dict) and comp.get("type") == "body":
                            for param in comp.get("parameters", []) or []:
                                if isinstance(param, dict):
                                    txt = param.get("text")
                                    if isinstance(txt, str) and txt.strip():
                                        template_texts.append(txt.strip())
                    if template_texts:
                        # Usually first parameter is user name; longest text is typically the actual body content.
                        main_body = max(template_texts, key=len)
                        return re.sub(r"\s+", " ", main_body).strip()
                for key in ["interactive", "postback", "template"]:
                    if key in data:
                        found = find_text(data[key])
                        if found:
                            return found
            
            if data is not None:
                found = find_text(data)
                if found:
                    return found
            
            if isinstance(data, str) and len(data) > 2:
                return data
            
            # Fallback: show payload itself (untrimmed) if it looks like JSON,
            # otherwise return the original string.
            if msg_str.startswith('{') or msg_str.startswith('['):
                return msg_str
            return msg_str

        # Simple cached English translation helper (optional if deep_translator is installed)
        if "recent_msg_translations" not in st.session_state:
            st.session_state.recent_msg_translations = {}

        def translate_to_english(text: str) -> str:
            if not text or text.strip() == "":
                return ""
            # If translator library is unavailable, just return original text
            if GoogleTranslator is None:
                return "[Translation unavailable - deep_translator not installed]"
            cache = st.session_state.recent_msg_translations
            if text in cache:
                return cache[text]
            try:
                # Limit text length to avoid API issues (Google Translate has limits)
                text_to_translate = text[:5000] if len(text) > 5000 else text
                translated = GoogleTranslator(source="auto", target="en").translate(text_to_translate)
                cache[text] = translated
                return translated
            except Exception as e:
                # If translation fails, cache the error and return original text
                # This prevents repeated failed attempts for the same text
                cache[text] = text
                # Log error for debugging (only show once per session)
                if "translation_error_shown" not in st.session_state:
                    st.session_state.translation_error_shown = True
                    st.warning(f"Translation error: {str(e)}. Showing original text. Check if deep_translator is properly installed.")
                return text
        
        # Parse timezone string like "UTC-3", "-3", "America/Sao_Paulo"
        def parse_timezone(tz_str):
            if not tz_str or pd.isna(tz_str):
                return None
            tz_str = str(tz_str).strip()
            
            # Try standard pytz timezone name first
            try:
                return pytz.timezone(tz_str)
            except:
                pass
            
            # Handle formats like "UTC-3", "GMT-3", "UTC+5:30", "-3", "-03:00"
            match = re.search(r'([+-]?)(\d{1,2})(?::(\d{2}))?', tz_str)
            if match:
                sign = -1 if match.group(1) == '-' else 1
                # Check if there's a minus before the number in the original string
                if 'UTC-' in tz_str or 'GMT-' in tz_str or tz_str.startswith('-'):
                    sign = -1
                elif 'UTC+' in tz_str or 'GMT+' in tz_str or tz_str.startswith('+'):
                    sign = 1
                
                hours = int(match.group(2)) * sign
                minutes = int(match.group(3) or 0)
                offset = timedelta(hours=hours, minutes=minutes)
                return timezone(offset)
            
            return None
        
        # Format timestamp in user's local timezone
        def format_timestamp_local(row):
            ts = row['timestamp']
            tz_str = row.get('user_timezone')
            
            if pd.isna(ts):
                return ""
            try:
                # Parse timestamp
                if isinstance(ts, str):
                    ts = pd.to_datetime(ts)
                
                # Make timezone-aware (assume UTC if naive)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=pytz.UTC)
                
                # Convert to user's timezone
                user_tz = parse_timezone(tz_str)
                if user_tz:
                    ts = ts.astimezone(user_tz)
                    return ts.strftime("%b %d, %H:%M")
                else:
                    # Show UTC if no timezone
                    return ts.strftime("%b %d, %H:%M") + " UTC"
            except Exception as e:
                return str(ts)[:16]

        def format_tags(raw_tags):
            if isinstance(raw_tags, list):
                tags = [str(t).strip() for t in raw_tags if str(t).strip()]
                return ", ".join(tags) if tags else "—"
            if raw_tags is None or pd.isna(raw_tags):
                return "—"
            if isinstance(raw_tags, str):
                raw_str = raw_tags.strip()
                if not raw_str:
                    return "—"
                try:
                    parsed = json.loads(raw_str)
                    if isinstance(parsed, list):
                        tags = [str(t).strip() for t in parsed if str(t).strip()]
                        return ", ".join(tags) if tags else "—"
                except Exception:
                    pass
                return raw_str
            return str(raw_tags).strip() or "—"
        
        # Detect audio messages: type='audio' or MIME like audio/ogg; codecs=opus, or message body
        def is_audio_message(msg_type, raw_msg):
            if pd.isna(msg_type):
                msg_type = ""
            t = str(msg_type).strip().lower()
            raw_str = "" if pd.isna(raw_msg) else str(raw_msg)
            return (
                t == "audio"
                or "audio/" in t
                or "audio/ogg" in raw_str
                or ("opus" in raw_str and "audio" in raw_str.lower())
            )

        # Detect sticker messages: check for "sticker" key in JSON and webp mime type
        def is_sticker_message(msg_type, raw_msg):
            if pd.isna(msg_type):
                msg_type = ""
            t = str(msg_type).strip().lower()
            raw_str = "" if pd.isna(raw_msg) else str(raw_msg)
            # Check if type is "sticker" or if message JSON contains "sticker" key with webp
            if t == "sticker":
                return True
            if '"sticker"' in raw_str and "image/webp" in raw_str:
                return True
            return False

        # Detect image messages: type image/photo or MIME like image/jpeg (excluding stickers)
        def is_image_message(msg_type, raw_msg):
            if pd.isna(msg_type):
                msg_type = ""
            t = str(msg_type).strip().lower()
            raw_str = "" if pd.isna(raw_msg) else str(raw_msg)
            # Exclude stickers from images
            if is_sticker_message(msg_type, raw_msg):
                return False
            # Check for image type or MIME
            if t in ("image", "photo"):
                return True
            if "image/" in t or ('"image"' in raw_str and "image/jpeg" in raw_str):
                return True
            return False

        # Merge audio + transcript: transcript is a separate row (same user, within 120s).
        skip_idx = set()
        transcript_for_audio = {}
        for i in range(len(recent_messages)):
            row = recent_messages.iloc[i]
            if not is_audio_message(row.get("msg_type"), row.get("raw_message")):
                continue
            try:
                ts_cur = pd.to_datetime(row["timestamp"])
            except Exception:
                continue
            for candidate_idx in [i - 1, i + 1]:
                if candidate_idx < 0 or candidate_idx >= len(recent_messages):
                    continue
                if candidate_idx in skip_idx:
                    continue
                other = recent_messages.iloc[candidate_idx]
                if other["sender"] != row["sender"] or is_audio_message(other.get("msg_type"), other.get("raw_message")):
                    continue
                try:
                    ts_other = pd.to_datetime(other["timestamp"])
                    if abs((ts_cur - ts_other).total_seconds()) <= 120:
                        transcript_for_audio[i] = candidate_idx
                        skip_idx.add(candidate_idx)
                        break
                except Exception:
                    pass

        # Merge image + description: description is stored in message immediately before (same user, within 120s).
        # Look backward first (i-1), as that's the typical pattern per user's description.
        interpretation_for_image = {}
        for i in range(len(recent_messages)):
            row = recent_messages.iloc[i]
            if not is_image_message(row.get("msg_type"), row.get("raw_message")):
                continue
            try:
                ts_cur = pd.to_datetime(row["timestamp"])
            except Exception:
                continue
            # Check i-1 first (message before image), then i+1 as fallback
            for candidate_idx in [i - 1, i + 1]:
                if candidate_idx < 0 or candidate_idx >= len(recent_messages):
                    continue
                if candidate_idx in skip_idx:
                    continue
                other = recent_messages.iloc[candidate_idx]
                if other["sender"] != row["sender"] or is_image_message(other.get("msg_type"), other.get("raw_message")):
                    continue
                try:
                    ts_other = pd.to_datetime(other["timestamp"])
                    if abs((ts_cur - ts_other).total_seconds()) <= 120:
                        interpretation_for_image[i] = candidate_idx
                        skip_idx.add(candidate_idx)
                        break
                except Exception:
                    pass

        # Merge sticker + description: stricter than image/audio to avoid false grouping.
        # Only use immediately previous message (i-1), from same sender, if it's text-like.
        description_for_sticker = {}
        def is_text_like_for_sticker(msg_type, raw_msg):
            if pd.isna(msg_type):
                msg_type = ""
            t = str(msg_type).strip().lower()
            raw = "" if pd.isna(raw_msg) else str(raw_msg)
            # Exclude obvious non-text/media/template payloads
            if is_sticker_message(msg_type, raw_msg) or is_audio_message(msg_type, raw_msg) or is_image_message(msg_type, raw_msg):
                return False
            if "notification" in raw.lower() or '"template"' in raw.lower():
                return False
            # Text-ish types are safe to merge as sticker descriptions
            return t in ("text", "interactive", "quickreply", "postback", "flows", "") or "text" in raw.lower()

        for i in range(len(recent_messages)):
            row = recent_messages.iloc[i]
            if not is_sticker_message(row.get("msg_type"), row.get("raw_message")):
                continue
            try:
                ts_cur = pd.to_datetime(row["timestamp"])
            except Exception:
                continue
            # Only previous row to prevent swallowing the next unrelated chat message.
            for candidate_idx in [i - 1]:
                if candidate_idx < 0 or candidate_idx >= len(recent_messages):
                    continue
                if candidate_idx in skip_idx:
                    continue
                other = recent_messages.iloc[candidate_idx]
                if other["sender"] != row["sender"]:
                    continue
                if not is_text_like_for_sticker(other.get("msg_type"), other.get("raw_message")):
                    continue
                try:
                    ts_other = pd.to_datetime(other["timestamp"])
                    # Keep a tight window for caption-style companion messages.
                    if 0 <= (ts_cur - ts_other).total_seconds() <= 30:
                        description_for_sticker[i] = candidate_idx
                        skip_idx.add(candidate_idx)
                        break
                except Exception:
                    pass

        # Type label: template, then icon for audio/image/sticker, else db type (text, interactive, quickReply, flows, etc.)
        def get_type_label(msg_type_val, is_audio, is_image, is_sticker, is_tmpl):
            if is_tmpl:
                return "template"
            if is_audio:
                return "🎧"
            if is_sticker:
                return "sticker"
            if is_image:
                return "📷"
            t = msg_type_val if msg_type_val is not None and pd.notna(msg_type_val) else ""
            return str(t).strip() or "—"

        # Build display rows: one per message, with type column and merged transcript/interpretation/description text
        def get_display_text(idx):
            if idx in skip_idx:
                return ""
            row = recent_messages.iloc[idx]
            raw = row.get("raw_message")
            if is_audio_message(row.get("msg_type"), raw):
                if idx in transcript_for_audio:
                    trans_idx = transcript_for_audio[idx]
                    prev = recent_messages.iloc[trans_idx]
                    return extract_message_text(prev.get("raw_message")) or "[Audio]"
                return "[Audio]"
            if is_sticker_message(row.get("msg_type"), raw):
                if idx in description_for_sticker:
                    desc_idx = description_for_sticker[idx]
                    prev = recent_messages.iloc[desc_idx]
                    return extract_message_text(prev.get("raw_message")) or "[Sticker]"
                return "[Sticker]"
            if is_image_message(row.get("msg_type"), raw):
                if idx in interpretation_for_image:
                    interp_idx = interpretation_for_image[idx]
                    prev = recent_messages.iloc[interp_idx]
                    return extract_message_text(prev.get("raw_message")) or "[Image]"
                return "[Image]"
            return extract_message_text(raw)

        rows_display = []
        for i in range(len(recent_messages)):
            if i in skip_idx:
                continue
            row = recent_messages.iloc[i]
            msg_type_val = row.get("msg_type")
            raw_msg = row.get("raw_message")
            is_audio = is_audio_message(msg_type_val, raw_msg)
            is_sticker = is_sticker_message(msg_type_val, raw_msg)
            is_image = is_image_message(msg_type_val, raw_msg)
            text = get_display_text(i)
            type_label = get_type_label(msg_type_val, is_audio, is_image, is_sticker, is_template(raw_msg))
            step_label = _label_ladder_step(row.get("matched_ladder_step")) if row["sender"] != "user" else ""
            if step_label:
                type_label = f"{type_label} · 🪜 {step_label}"
            rows_display.append({
                "Time": format_timestamp_local(row),
                "User": format_display_name(row.get("user_name"), row.get("user_waid"), user_id=row.get("user_id")),
                "Tag": format_user_tags_column(row.get("user_tags"), row.get("user_waid")),
                "From": "👤 User" if row["sender"] == "user" else "🤖 Bot",
                "Status": str(row.get("status")).lower() if pd.notna(row.get("status")) else "—",
                "Type": type_label,
                "Message": text,
                "Message (EN)": translate_to_english(text) if translate_recent_messages else "",
            })

        display_df = pd.DataFrame(rows_display)
        display_obj = display_df
        if not display_df.empty and "Status" in display_df.columns:
            def _highlight_failed_row(row):
                failed = str(row.get("Status", "")).strip().lower() == "failed"
                style = "background-color: rgba(239, 68, 68, 0.16);" if failed else ""
                return [style] * len(row)
            display_obj = display_df.style.apply(_highlight_failed_row, axis=1)
        
        if wrap_recent_messages:
            render_wrapped_messages_table(display_df)
        else:
            st.dataframe(
                display_obj,
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Time": st.column_config.TextColumn(width="small"),
                    "User": st.column_config.TextColumn(width="medium"),
                    "Tag": st.column_config.TextColumn(width="small"),
                    "From": st.column_config.TextColumn(width="small"),
                    "Status": st.column_config.TextColumn(width="small"),
                    "Type": st.column_config.TextColumn(width="medium"),
                    "Message": st.column_config.TextColumn(width="large"),
                    "Message (EN)": st.column_config.TextColumn(width="large"),
                }
            )
    else:
        st.info("No messages found")
    
    st.markdown("---")

    # ── 5. Active Days stats ─────────────────────────────────────────────────
    st.markdown("#### Active Days & Engagement")
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Active Days — Alive", f"{alive_avg_active_days:.1f} avg")
    d1.caption(f"↳ {alive_total_active_days} total active days across alive users")
    d2.metric("Active Days — At Risk", f"{at_risk_avg_active_days:.1f} avg")
    d2.caption(f"↳ {at_risk_total_active_days} total active days across at-risk users")
    d3.metric("Active Days — Churned (7d)", f"{churned_avg_active_days:.1f} avg")
    d3.caption(f"↳ {churned_total_active_days} total active days across churned users")
    d4.metric("Active Days — Churned (lifetime)", f"{churned_lifetime_avg_active_days:.1f} avg")
    d4.caption(f"↳ {churned_lifetime_total_active_days} total · {churned_lifetime_count} users ever churned")

    c4, c5, c6, c7 = st.columns(4)
    c4.metric("% inside 24h", f"{pct_inside_24h}%")
    c4.caption(f"↳ {inside_24h} users")
    c5.metric("Messaged today", f"{pct_messaged_today}%")
    c5.caption(f"↳ {messaged_today} users")
    c6.metric("Active today", f"{pct_activity_complete}%")
    c6.caption(f"↳ {completed_today} users")
    c7.metric("Churned users (last 7d)", churned_7d_count if churned_7d_count is not None else "—")
    c7.caption(f"↳ {churned_7d_pct}% of onboarded users · {churned_7d_came_back} came back")

    st.markdown("---")

    # ── 6. Deep dive expandables ─────────────────────────────────────────────
    try:
        new_today_list = run_query(f"""
            {beta_users_cte}
            SELECT id, COALESCE(full_name, 'Unknown') AS name, waid, tags, is_beta
            FROM (
                SELECT DISTINCT ON (u.waid)
                    u.id,
                    u.full_name,
                    u.waid,
                    u.tags,
                    u.created_at,
                    EXISTS (
                        SELECT 1
                        FROM beta_users bu
                        WHERE bu.id = u.id OR bu.waid = u.waid
                    ) AS is_beta
                FROM users u
                WHERE u.created_at >= NOW() - INTERVAL '7 days'
                  AND EXISTS (
                      SELECT 1
                      FROM messages m
                      WHERE m.sender = 'user'
                        AND (m.user_id = u.id OR m.waid = u.waid)
                  )
                  {internal_filter_join}
                ORDER BY u.waid, u.created_at DESC
            ) unique_users
            ORDER BY created_at DESC
            LIMIT 200
        """)
        with st.expander("New users - names"):
            if new_today_list.empty:
                st.caption("No users")
            else:
                beta_new_users = new_today_list[new_today_list["is_beta"] == True]
                pre_plan_new_users = new_today_list[new_today_list["is_beta"] != True]

                st.markdown(f"**Onboarded users with a plan** ({len(beta_new_users)})")
                if beta_new_users.empty:
                    st.caption("No users")
                else:
                    for _, row in beta_new_users.iterrows():
                        st.caption(f"• {format_display_name_with_tags(row['name'], row.get('waid'), row.get('tags'), user_id=row.get('id'))}")

                st.markdown(f"**Messaging coach, no plan yet** ({len(pre_plan_new_users)})")
                if pre_plan_new_users.empty:
                    st.caption("No users")
                else:
                    for _, row in pre_plan_new_users.iterrows():
                        st.caption(f"• {format_display_name_with_tags(row['name'], row.get('waid'), row.get('tags'), user_id=row.get('id'))}")
    except:
        st.warning("Could not load new users (past 7d) list")

    try:
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        inactive_7d_list = run_query(f"""
            {beta_users_cte},
            latest_farewell AS (
                SELECT DISTINCT ON (rl.user_id)
                    rl.user_id,
                    rl.sent_at AS farewell_at
                FROM recovery_logs rl
                JOIN beta_users bu ON bu.id = rl.user_id
                JOIN users u ON rl.user_id = u.id
                WHERE rl.ladder_step = 'farewell'
                  AND rl.sent_at >= NOW() - INTERVAL '7 days'
                  {internal_filter_join}
                ORDER BY rl.user_id, rl.sent_at DESC
            )
            SELECT
                u.id AS user_id,
                COALESCE(u.full_name, 'Unknown') AS name,
                u.waid AS phone,
                u.tags,
                u.active_days,
                TO_CHAR(
                    date_trunc('week', u.onboarding_timestamp AT TIME ZONE 'America/Sao_Paulo'),
                    'YYYY-MM-DD'
                ) AS onboarding_week,
                lf.farewell_at,
                EXISTS (
                    SELECT 1
                    FROM messages m
                    WHERE (m.user_id = u.id OR m.waid = u.waid)
                      AND m.sender = 'user'
                      AND m.sent_at > lf.farewell_at
                ) AS came_back
            FROM latest_farewell lf
            JOIN users u ON u.id = lf.user_id
            ORDER BY lf.farewell_at DESC
        """)
        still_inactive_df = inactive_7d_list[inactive_7d_list["came_back"] == False] if not inactive_7d_list.empty else pd.DataFrame()
        came_back_df = inactive_7d_list[inactive_7d_list["came_back"] == True] if not inactive_7d_list.empty else pd.DataFrame()
        inactive_7d_count = len(still_inactive_df) if not still_inactive_df.empty else 0
        with st.expander(f"Churned users (past 7d) — ({inactive_7d_count})"):
            if inactive_7d_list.empty:
                st.caption("No users")
            else:
                st.markdown("**Still inactive**")
                if still_inactive_df.empty:
                    st.caption("No users")
                else:
                    for _, row in still_inactive_df.iterrows():
                        name = format_display_name_with_tags(row['name'], row.get('phone'), row.get('tags'), user_id=row.get('user_id'))
                        phone = row.get('phone', '—')
                        active_days_val = row.get('active_days', '—')
                        st.caption(f"• {name} · 📞 {phone} · 🏃 {active_days_val} active days")

                st.markdown("**Came back after farewell**")
                if came_back_df.empty:
                    st.caption("No users")
                else:
                    for _, row in came_back_df.iterrows():
                        name = format_display_name_with_tags(row['name'], row.get('phone'), row.get('tags'), user_id=row.get('user_id'))
                        phone = row.get('phone', '—')
                        active_days_val = row.get('active_days', '—')
                        st.caption(f"• {name} · 📞 {phone} · 🏃 {active_days_val} active days")
    except Exception as e:
        st.warning(f"Could not load inactive users list: {e}")

    # At Risk Users: silent ≥5d (headline cohort) + recovery-ladder highlights + re-engaged
    try:
        silent_at_risk_df, reengaged_at_risk_df = get_at_risk_users_detail()
        sp_tz = "America/Sao_Paulo"

        with st.expander("⚠️ At Risk Users"):
            st.markdown(f"**Silent ≥5 days** ({len(silent_at_risk_df)})")
            if silent_at_risk_df.empty:
                st.caption("No users")
            else:
                for _, row in silent_at_risk_df.iterrows():
                    name = format_display_name_with_tags(
                        row["full_name"], row.get("waid"), row.get("tags"), user_id=row.get("user_id")
                    )
                    active_days_val = row.get("active_days", "—")
                    if pd.notna(row.get("last_msg_at")):
                        last_msg_sp = _format_ts_local(row["last_msg_at"], sp_tz, fmt="%d-%m-%Y, %H:%M")
                        line = f"{name} — last message {last_msg_sp}"
                    else:
                        line = f"{name} — never messaged"
                    line += f" · 🏃 {active_days_val} active days"
                    if row.get("received_recovery_ladder"):
                        line += " 🪜"
                    st.caption(f"• {line}")

            st.markdown(f"**Re-engaged after recovery** ({len(reengaged_at_risk_df)})")
            if reengaged_at_risk_df.empty:
                st.caption("No users")
            else:
                for _, row in reengaged_at_risk_df.iterrows():
                    name = format_display_name_with_tags(
                        row["full_name"], row.get("waid"), row.get("tags"), user_id=row.get("user_id")
                    )
                    active_days_val = row.get("active_days", "—")
                    rung_label = _label_ladder_step(row.get("recovery_ladder_step"))
                    recovery_sp = _format_ts_local(row["recovery_sent_at"], sp_tz, fmt="%d-%m-%Y, %H:%M")
                    reply_sp = _format_ts_local(row["reengaged_reply_at"], sp_tz, fmt="%d-%m-%Y, %H:%M")
                    st.caption(
                        f"• {name} · 🏃 {active_days_val} active days — 🪜 {rung_label} ({recovery_sp}) · ✅ replied {reply_sp}"
                    )
    except Exception as e:
        st.warning(f"Could not load At Risk Users: {e}")

    # Reactivated Users: previously inactive (received farewell) who messaged in the last 24h
    try:
        reactivated_df = get_reactivated_users_last_24h()
        sp_tz = "America/Sao_Paulo"
        with st.expander("🔄 Reactivated Users"):
            st.caption("Previously inactive users (received farewell) who sent a message to the coach in the last 24 hours.")
            if reactivated_df.empty:
                st.caption("No users")
            else:
                for _, row in reactivated_df.iterrows():
                    last_msg_sp = _format_ts_local(row["last_message_at"], sp_tz)
                    farewell_sp = _format_ts_local(row["farewell_at"], sp_tz)
                    st.caption(f"• {format_display_name_with_tags(row['full_name'], row.get('waid'), row.get('tags'), user_id=row.get('user_id'))} — messaged {last_msg_sp} (farewell: {farewell_sp})")
    except Exception as e:
        st.warning(f"Could not load Reactivated Users: {e}")




# Tab 1b: Cost
if selected_section == "💰 Cost":
    # Cost analysis always excludes internal users so spend reflects real customers.
    exclude_internal = True
    st.markdown("---")
    st.caption(
        "Stack B LLM turn-level cost, sourced from `messages.type = 'turn_audit'`. "
        "Per-user stats (avg / median / p25 / p75) only count **real users** — "
        "Stack B users who have sent at least one message themselves — so users "
        "who never messaged don't distort the denominator."
    )

    # ── 0. Headline: current 7d vs prior 7d ──────────────────────────────
    try:
        cost_headline_df = get_llm_cost_headline_metrics(exclude_internal)
        cost_headline = cost_headline_df.iloc[0] if not cost_headline_df.empty else {}
        current_7d_total = float(cost_headline.get("current_7d_total", 0) or 0)
        prior_7d_total = float(cost_headline.get("prior_7d_total", 0) or 0)
        current_7d_users = int(cost_headline.get("current_7d_users", 0) or 0)
        prior_7d_users = int(cost_headline.get("prior_7d_users", 0) or 0)
    except Exception as e:
        st.warning(f"Could not load headline cost metrics: {e}")
        current_7d_total = prior_7d_total = 0.0
        current_7d_users = prior_7d_users = 0

    current_7d_avg = (current_7d_total / current_7d_users) if current_7d_users else 0.0
    prior_7d_avg = (prior_7d_total / prior_7d_users) if prior_7d_users else 0.0
    total_delta_pct = (
        round(100 * (current_7d_total - prior_7d_total) / prior_7d_total, 1)
        if prior_7d_total else None
    )
    avg_delta_pct = (
        round(100 * (current_7d_avg - prior_7d_avg) / prior_7d_avg, 1)
        if prior_7d_avg else None
    )

    cost_col1, cost_col2, cost_col3 = st.columns(3)
    cost_col1.metric("Total LLM cost (last 7d)", f"${current_7d_total:,.2f}")
    if total_delta_pct is None:
        cost_col1.caption(f"Prior 7d: ${prior_7d_total:,.2f} (no comparable data)")
    else:
        arrow = "🔺" if total_delta_pct >= 0 else "🔻"
        cost_col1.caption(f"{arrow} {abs(total_delta_pct)}% vs prior 7d (${prior_7d_total:,.2f})")

    cost_col2.metric("Avg cost / real user (last 7d)", f"${current_7d_avg:,.3f}")
    if avg_delta_pct is None:
        cost_col2.caption(f"Prior 7d: ${prior_7d_avg:,.3f} (no comparable data)")
    else:
        arrow = "🔺" if avg_delta_pct >= 0 else "🔻"
        cost_col2.caption(f"{arrow} {abs(avg_delta_pct)}% vs prior 7d (${prior_7d_avg:,.3f})")

    cost_col3.metric("Real users with cost (last 7d)", current_7d_users)
    cost_col3.caption(f"vs {prior_7d_users} in the prior 7d")

    st.markdown("---")

    # ── 1. Weekly per-user cost trend (median / avg / p25 / p75) ───────────
    st.markdown("#### 📦 Weekly LLM cost per real user — trend")
    st.caption(
        "One box per completed local calendar week (Mon–Sun). Box spans p25–p75, "
        "white tick = median, orange line = average."
    )
    try:
        weekly_cost_df = get_llm_cost_weekly_trend(exclude_internal)
    except Exception as e:
        st.warning(f"Could not load weekly cost trend: {e}")
        weekly_cost_df = pd.DataFrame()

    if not weekly_cost_df.empty:
        weekly_cost_df = weekly_cost_df.copy()
        weekly_cost_df["week_start"] = pd.to_datetime(weekly_cost_df["week_start"])
        for _col in ["avg_usd", "median_usd", "p25_usd", "p75_usd", "total_usd"]:
            weekly_cost_df[_col] = pd.to_numeric(weekly_cost_df[_col], errors="coerce")
        weekly_cost_df["week_label"] = weekly_cost_df["week_start"].dt.strftime("%d %b")

        try:
            import altair as alt

            _box_layer = (
                alt.Chart(weekly_cost_df)
                .mark_bar(size=26, color="#00d4aa", opacity=0.35)
                .encode(
                    x=alt.X("week_start:T", title="Week starting"),
                    y=alt.Y("p25_usd:Q", title="LLM cost per real user ($)"),
                    y2=alt.Y2("p75_usd:Q"),
                    tooltip=[
                        alt.Tooltip("week_label:N", title="Week"),
                        alt.Tooltip("users:Q", title="Real users"),
                        alt.Tooltip("p25_usd:Q", title="p25", format="$.3f"),
                        alt.Tooltip("median_usd:Q", title="Median", format="$.3f"),
                        alt.Tooltip("avg_usd:Q", title="Average", format="$.3f"),
                        alt.Tooltip("p75_usd:Q", title="p75", format="$.3f"),
                    ],
                )
            )
            _median_layer = (
                alt.Chart(weekly_cost_df)
                .mark_tick(color="#ffffff", thickness=2, size=26)
                .encode(x="week_start:T", y="median_usd:Q")
            )
            _avg_layer = (
                alt.Chart(weekly_cost_df)
                .mark_line(point=True, color="#ff9f40", strokeWidth=2)
                .encode(x="week_start:T", y="avg_usd:Q")
            )
            st.altair_chart(
                (_box_layer + _median_layer + _avg_layer).properties(height=320),
                use_container_width=True,
            )
            st.caption("🟩 Box = p25–p75 · ⬜ White tick = median · 🟧 Orange line = average")
        except Exception as _chart_err:
            st.warning(f"Could not render weekly cost chart: {_chart_err}")

        with st.expander("View weekly cost data"):
            st.dataframe(
                weekly_cost_df[
                    ["week_label", "users", "avg_usd", "median_usd", "p25_usd", "p75_usd", "total_usd"]
                ].rename(columns={
                    "week_label": "Week",
                    "users": "Real users",
                    "avg_usd": "Avg $",
                    "median_usd": "Median $",
                    "p25_usd": "p25 $",
                    "p75_usd": "p75 $",
                    "total_usd": "Total $",
                }),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("Not enough data yet for a weekly trend (need at least one fully completed week).")

    st.markdown("---")

    # ── 2. Cost per user life day ──────────────────────────────
    st.markdown("#### 📅 LLM cost per real user by day of user life")
    st.caption(
        "Day 1 = signup day (local time). Life days with fewer than 5 active real "
        "users are hidden from the chart to avoid noisy long-tail days."
    )
    try:
        life_day_cost_df = get_llm_cost_by_life_day(exclude_internal)
    except Exception as e:
        st.warning(f"Could not load cost-by-life-day data: {e}")
        life_day_cost_df = pd.DataFrame()

    if not life_day_cost_df.empty:
        life_day_cost_df = life_day_cost_df.copy()
        for _col in ["users_active", "avg_usd", "median_usd", "p25_usd", "p75_usd", "total_usd"]:
            life_day_cost_df[_col] = pd.to_numeric(life_day_cost_df[_col], errors="coerce")
        life_day_chart_df = life_day_cost_df[life_day_cost_df["users_active"] >= 5]

        if not life_day_chart_df.empty:
            try:
                import altair as alt

                _band_layer = (
                    alt.Chart(life_day_chart_df)
                    .mark_area(color="#00d4aa", opacity=0.15)
                    .encode(
                        x=alt.X("user_life_day:Q", title="Day of user life"),
                        y=alt.Y("p25_usd:Q", title="LLM cost per real user ($)"),
                        y2=alt.Y2("p75_usd:Q"),
                    )
                )
                _median_line = (
                    alt.Chart(life_day_chart_df)
                    .mark_line(color="#ffffff", strokeWidth=2)
                    .encode(x="user_life_day:Q", y="median_usd:Q")
                )
                _avg_line = (
                    alt.Chart(life_day_chart_df)
                    .mark_line(color="#ff9f40", strokeWidth=2, strokeDash=[4, 3])
                    .encode(
                        x="user_life_day:Q",
                        y="avg_usd:Q",
                        tooltip=[
                            alt.Tooltip("user_life_day:Q", title="Life day"),
                            alt.Tooltip("users_active:Q", title="Real users"),
                            alt.Tooltip("p25_usd:Q", title="p25", format="$.3f"),
                            alt.Tooltip("median_usd:Q", title="Median", format="$.3f"),
                            alt.Tooltip("avg_usd:Q", title="Average", format="$.3f"),
                            alt.Tooltip("p75_usd:Q", title="p75", format="$.3f"),
                        ],
                    )
                )
                st.altair_chart(
                    (_band_layer + _median_line + _avg_line).properties(height=320),
                    use_container_width=True,
                )
                st.caption("🟩 Shaded band = p25–p75 · ⬜ White line = median · 🟧 Dashed orange line = average")
            except Exception as _chart_err:
                st.warning(f"Could not render life-day cost chart: {_chart_err}")
        else:
            st.info("Not enough real users per life day yet (need ≥5 active real users on a given day).")

        with st.expander("View cost-by-life-day data"):
            st.dataframe(
                life_day_cost_df.rename(columns={
                    "user_life_day": "Life day",
                    "users_active": "Real users",
                    "avg_usd": "Avg $",
                    "median_usd": "Median $",
                    "p25_usd": "p25 $",
                    "p75_usd": "p75 $",
                    "total_usd": "Total $",
                }),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No life-day cost data found.")


# Tab 2: User Deep Dive
if selected_section == "🔍 User Deep Dive":
    st.markdown("### 🔍 User Deep Dive")
    st.caption("Select a user to view messages, activity plan, active days, and engagement")
    
    users_df = get_deep_dive_user_options()
    
    if users_df.empty:
        st.info("No users found")
    else:
        deep_dive_filter = st.selectbox(
            "User filter",
            ["All users", "Only users with tags"],
            key="deep_dive_user_filter",
        )
        if deep_dive_filter == "Only users with tags":
            users_df = users_df[users_df["has_tags"] == True].copy()

        if users_df.empty:
            st.info("No users match the selected filter")
            st.stop()

        users_df["label"] = users_df.apply(
            lambda r: (
                f"{format_display_name(r['full_name'], r['waid'], user_id=r.get('id'))}"
                f"{' [DOTZ]' if r.get('is_dotz') else ''}"
                f"{' *' if r.get('is_active_24h') else ''} ({r['waid']})"
            ),
            axis=1,
        )
        selected_label = st.selectbox("Select user", users_df["label"])
        selected_row = users_df[users_df["label"] == selected_label].iloc[0]
        user_id = int(selected_row['id'])
        user_tz_str = selected_row.get('timezone')
        user_coach = selected_row.get('coach_name', '—')
        user_slogan = selected_row.get('slogan', '—')
        
        # Helper: parse timezone strings like "UTC-3", "-3", "America/Sao_Paulo"
        def parse_tz(tz_str):
            if not tz_str or pd.isna(tz_str):
                return None
            tz_str = str(tz_str).strip()
            try:
                return pytz.timezone(tz_str)
            except:
                pass
            match = re.search(r'([+-]?)(\d{1,2})(?::(\d{2}))?', tz_str)
            if match:
                sign = -1 if match.group(1) == '-' else 1
                if 'UTC-' in tz_str or 'GMT-' in tz_str or tz_str.startswith('-'):
                    sign = -1
                elif 'UTC+' in tz_str or 'GMT+' in tz_str or tz_str.startswith('+'):
                    sign = 1
                hours = int(match.group(2)) * sign
                minutes = int(match.group(3) or 0)
                offset = timedelta(hours=hours, minutes=minutes)
                return timezone(offset)
            return None
        
        def format_ts_local(ts):
            if pd.isna(ts) or ts is None:
                return "—"
            try:
                if isinstance(ts, str):
                    ts = pd.to_datetime(ts)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=pytz.UTC)
                user_tz = parse_tz(user_tz_str)
                if user_tz:
                    ts = ts.astimezone(user_tz)
                return ts.strftime("%b %d, %H:%M")
            except:
                return str(ts)[:16]
        
        # Engagement, progress, recovery, and funnel metrics in one cached query.
        summary_df = get_user_deep_dive_summary(user_id)
        summary = summary_df.iloc[0] if not summary_df.empty else pd.Series(dtype=object)

        count_24h = int(summary.get('count_24h', 0) or 0)
        count_3d = int(summary.get('count_3d', 0) or 0)
        count_7d = int(summary.get('count_7d', 0) or 0)
        last_user_message_at = summary.get('last_user_message_at')
        last_active = format_ts_local(last_user_message_at) if pd.notna(last_user_message_at) else "—"

        active_days_count = int(summary.get('active_days', 0) or 0)
        _goal = summary.get('active_days_goal')
        active_days_goal_str = str(int(_goal)) if _goal is not None and pd.notna(_goal) else "—"

        last_activity_name = summary.get('last_activity_type') if pd.notna(summary.get('last_activity_type')) else "—"
        last_activity_completed_at = summary.get('last_activity_completed_at')
        last_activity_time = format_ts_local(last_activity_completed_at) if pd.notna(last_activity_completed_at) else "—"

        def is_outside_24h(ts):
            if ts is None or pd.isna(ts):
                return True
            try:
                t = pd.to_datetime(ts)
                if t.tzinfo is None:
                    t = t.tz_localize(pytz.UTC)
                cutoff = pd.Timestamp.utcnow().tz_localize(None) if pd.Timestamp.utcnow().tzinfo is None else pd.Timestamp.utcnow()
                if cutoff.tzinfo is None:
                    cutoff = cutoff.tz_localize(pytz.UTC)
                return t < cutoff - pd.Timedelta(hours=24)
            except Exception:
                return True
        outside_24h_flag = is_outside_24h(last_user_message_at)

        # Recovery ladder position (PDF milestones + last send since AFK)
        days_afk = int(summary.get('days_afk', 0) or 0) if pd.notna(summary.get('days_afk')) else 0
        is_afk = outside_24h_flag and days_afk >= 1
        pdf_step = _pdf_ladder_step(active_days_count, days_afk) if is_afk else None
        next_ladder_step = _next_ladder_step(active_days_count, days_afk) if is_afk else None
        ladder_position = _ladder_position_label(active_days_count, days_afk) if is_afk else "Active (not AFK)"

        last_ladder_since_afk = summary.get('last_ladder_step_since_afk') if pd.notna(summary.get('last_ladder_step_since_afk')) else None
        last_ladder_since_afk_at = summary.get('last_ladder_step_since_afk_at')
        recovery_attempts_since_afk = int(summary.get('recovery_attempts_since_afk', 0) or 0)

        last_rung_step = summary.get('last_rung_step') if pd.notna(summary.get('last_rung_step')) else None
        last_rung_sent_at = summary.get('last_rung_sent_at')
        recovery_cohort_label = "≤3 active days" if active_days_count <= 3 else ">3 active days"

        try:
            llm_cost_df = get_user_llm_cost_metrics(user_id)
            llm_cost = llm_cost_df.iloc[0] if not llm_cost_df.empty else {}
            llm_lifetime_usd = float(llm_cost.get("lifetime_usd", 0) or 0)
            llm_last_7d_usd = float(llm_cost.get("last_7d_usd", 0) or 0)
            llm_tenure_days = int(llm_cost.get("tenure_days", 1) or 1)
            llm_avg_per_day = llm_lifetime_usd / llm_tenure_days if llm_tenure_days else 0.0
        except Exception:
            llm_lifetime_usd = llm_last_7d_usd = llm_avg_per_day = 0.0
            llm_tenure_days = 1

        # Activity plan (schedule) from user_activities
        plan_df = get_user_activity_plan(user_id)
        
        week_full = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        week_short = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        day_to_idx = {day: i for i, day in enumerate(week_full)}
        
        def parse_days(val):
            if val is None:
                return []
            # If already a list/array-like, return normalized strings
            if isinstance(val, (list, tuple)):
                return [str(v) for v in val]
            # Strings: try JSON parse first
            if isinstance(val, str):
                try:
                    data = json.loads(val)
                    if isinstance(data, list):
                        return [str(v) for v in data]
                except Exception:
                    # Fallback: comma-separated list
                    if ',' in val:
                        return [v.strip() for v in val.split(',')]
                # If nothing parsed, treat as single value
                return [val.strip()]
            # Any other type -> best effort string
            try:
                return [str(val)]
            except Exception:
                return []
        
        # Build weekly calendar and find next activity
        calendar = {day: [] for day in week_short}
        next_activity_name = "—"
        next_activity_day = "—"
        today_idx = datetime.utcnow().weekday()  # Monday = 0
        best_delta = None
        
        if not plan_df.empty:
            for _, row in plan_df.iterrows():
                act_name = row.get('description') or 'Activity'
                days_list = parse_days(row.get('days'))
                for day in days_list:
                    normalized = day.strip().capitalize()
                    if normalized in week_full:
                        idx = day_to_idx[normalized]
                        calendar[week_short[idx]].append(act_name)
                        delta = (idx - today_idx) % 7
                        if best_delta is None or delta < best_delta:
                            best_delta = delta
                            next_activity_name = act_name
                            next_activity_day = week_full[idx]
        
        # User info row (slogan and coach)
        info_col1, info_col2 = st.columns(2)
        with info_col1:
            st.info(f"**Coach:** {user_coach if user_coach and pd.notna(user_coach) else '—'}")
        with info_col2:
            st.info(f"**Slogan / Mantra:** {user_slogan if user_slogan and pd.notna(user_slogan) else '—'}")

        st.markdown("#### 🪜 Recovery Ladder Position")
        afk_col1, afk_col2 = st.columns(2)
        with afk_col1:
            next_label = _format_milestone_step(next_ladder_step)
            st.metric("Next Ladder Step", next_label, ladder_position)
        with afk_col2:
            if pdf_step:
                st.metric("Today's PDF Step", _format_milestone_step(pdf_step), recovery_cohort_label)
            else:
                st.metric("Today's PDF Step", "—" if is_afk else "N/A", recovery_cohort_label)

        if is_afk:
            if last_ladder_since_afk:
                since_afk_label = _label_ladder_step(last_ladder_since_afk)
                since_afk_time = format_ts_local(last_ladder_since_afk_at) if pd.notna(last_ladder_since_afk_at) else "—"
                st.markdown(
                    f"**Last step since AFK:** {since_afk_label} · sent {since_afk_time}"
                    f" · {recovery_attempts_since_afk} recovery attempt(s) this AFK period"
                )
            else:
                st.markdown(f"**Last step since AFK:** — (none yet) · {recovery_cohort_label}")
        elif last_rung_step:
            rung_label = _label_ladder_step(last_rung_step)
            rung_time = format_ts_local(last_rung_sent_at) if pd.notna(last_rung_sent_at) else "—"
            st.markdown(f"**Most recent recovery send:** {rung_label} · sent {rung_time} · {recovery_cohort_label}")
        else:
            st.markdown(f"**Recovery ladder:** — (no recovery sends) · {recovery_cohort_label}")

        # Most active times analysis (in user's local timezone)
        st.markdown("#### 📊 Most Active Times")
        
        # Get user timezone for conversion
        user_tz = parse_tz(user_tz_str)
        tz_name = user_tz_str if user_tz_str else "UTC"
        
        # Aggregate active hours in SQL instead of fetching every message timestamp.
        message_hours_df = get_user_message_hour_counts(user_id, user_tz_str)

        if not message_hours_df.empty:
            all_hours = pd.DataFrame({'hour': range(24)})
            message_hours_df['hour'] = message_hours_df['hour'].astype(int)
            active_times_df = all_hours.merge(message_hours_df, on='hour', how='left').fillna(0)
            active_times_df['message_count'] = active_times_df['message_count'].astype(int)

            def format_hour(h):
                h = int(h)
                if h == 0:
                    return "12am"
                elif h < 12:
                    return f"{h}am"
                elif h == 12:
                    return "12pm"
                else:
                    return f"{h-12}pm"

            active_times_df['hour_label'] = active_times_df['hour'].apply(format_hour)

            import altair as alt

            chart = alt.Chart(active_times_df).mark_bar(
                color='#00d4aa',
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X('hour_label:N',
                        sort=list(active_times_df['hour_label']),
                        title='Hour of Day (Local Time)',
                        axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('message_count:Q', title='Messages'),
                tooltip=[
                    alt.Tooltip('hour_label:N', title='Hour'),
                    alt.Tooltip('message_count:Q', title='Messages')
                ]
            ).properties(
                height=300,
                title=f'Message Activity by Hour ({tz_name})'
            ).configure_axis(
                grid=True,
                gridColor='#2d3748'
            ).configure_view(
                strokeWidth=0
            )

            st.altair_chart(chart, use_container_width=True)

            peak_hour_row = active_times_df.loc[active_times_df['message_count'].idxmax()]
            if peak_hour_row['message_count'] > 0:
                peak_hour = peak_hour_row['hour_label']
                peak_count = int(peak_hour_row['message_count'])
                total_messages = int(active_times_df['message_count'].sum())
                st.caption(f"**Peak activity:** {peak_hour} ({peak_count} messages, {round(100 * peak_count / total_messages, 1)}% of total)")
        else:
            st.caption("No message activity data available")

        st.markdown("---")
        
        # Metrics row (active days + goal from users table per db-dictionary)
        m1, m1_goal, m2, m3, m4, m5, m6, m7, m8 = st.columns(9)
        m1.metric("📅 Active Days", active_days_count)
        m1_goal.metric("🎯 Active Days Goal", active_days_goal_str)
        m2.metric("✅ Last Completed", last_activity_name, last_activity_time)
        m3.metric("⏭️ Next Activity", next_activity_name, next_activity_day)
        m4.metric("⏱️ Last Active", last_active)
        m5.metric("💬 Messages Sent (24h)", count_24h, f"3d: {count_3d} • 7d: {count_7d}")
        m6.metric("💰 LLM cost (lifetime)", f"${llm_lifetime_usd:,.2f}")
        m7.metric("💰 LLM cost (7d)", f"${llm_last_7d_usd:,.2f}")
        m8.metric("💰 LLM avg / day", f"${llm_avg_per_day:,.3f}", f"{llm_tenure_days}d tenure")

        # Activity plan weekly calendar
        st.markdown("#### 📅 Activity Plan (weekly)")
        if plan_df.empty:
            st.info("No activity plan found for this user.")
        else:
            cols = st.columns(7)
            for i, day in enumerate(week_short):
                with cols[i]:
                    items = calendar.get(day, [])
                    st.markdown(f"**{day}**")
                    st.markdown(f"<div style='font-size: 22px; font-weight: bold; color: #00d4aa;'>{len(items)}</div>", unsafe_allow_html=True)
                    if items:
                        for act in items[:4]:
                            st.caption(f"• {act}")
                        if len(items) > 4:
                            with st.expander(f"+{len(items)-4} more"):
                                for act in items[4:]:
                                    st.caption(f"• {act}")
                    else:
                        st.caption("—")
        
        st.markdown("#### 💬 Message History")
        msg_limit_options = {"Last 20": 20, "Last 50": 50, "Last 100": 100, "All": None}
        msg_limit_label = st.selectbox(
            "Messages to show",
            list(msg_limit_options.keys()),
            index=0,
            key="msg_history_limit",
        )
        msg_limit = msg_limit_options[msg_limit_label]
        translate_deep_dive_messages = st.checkbox(
            "Translate message history to English",
            value=False,
            key="user_deepdive_translate_messages",
            help="Disabled by default because translating every row can slow the deep dive.",
        )
        wrap_deep_dive_messages = st.checkbox(
            "Wrap message history for screenshots",
            value=False,
            key="user_deepdive_wrap_messages",
            help="Shows the same table with wrapped message text so it fits in screenshots.",
        )

        messages_df = get_user_message_history(user_id, msg_limit)
        
        def extract_msg_text(raw_msg):
            """Extract the most human-readable text from a message payload.
            
            NOTE: this intentionally returns the full text without truncation
            so the message history shows complete content.
            """
            if pd.isna(raw_msg) or raw_msg is None:
                return ""
            msg_str = str(raw_msg).strip()
            
            def parse_json(s):
                try:
                    data = json.loads(s)
                    if isinstance(data, str):
                        try:
                            return json.loads(data)
                        except Exception:
                            return data
                    return data
                except Exception:
                    return None
            
            def find_text(obj, depth=0):
                if depth > 10 or obj is None:
                    return None
                if isinstance(obj, str) and len(obj) > 2:
                    return obj
                if isinstance(obj, dict):
                    for key in ["text", "body", "title", "message", "content", "caption", "label", "description", "value"]:
                        if key in obj:
                            val = obj[key]
                            if isinstance(val, str) and len(val) > 2:
                                return val
                            found = find_text(val, depth + 1)
                            if found:
                                return found
                    if "payload" in obj:
                        payload = obj["payload"]
                        if isinstance(payload, str):
                            pj = parse_json(payload)
                            if isinstance(pj, (dict, list)):
                                found = find_text(pj, depth + 1)
                                if found:
                                    return found
                            if len(payload) > 2:
                                return payload
                        else:
                            found = find_text(payload, depth + 1)
                            if found:
                                return found
                    for val in obj.values():
                        if isinstance(val, (dict, list, str)):
                            found = find_text(val, depth + 1)
                            if found:
                                return found
                if isinstance(obj, list):
                    for item in obj:
                        found = find_text(item, depth + 1)
                        if found:
                            return found
                return None
            
            data = parse_json(msg_str)
            if isinstance(data, dict):
                # WhatsApp template notifications: prefer body parameter text (actual template content)
                notification = data.get("notification")
                if isinstance(notification, dict):
                    template_texts = []
                    for comp in notification.get("components", []) or []:
                        if isinstance(comp, dict) and comp.get("type") == "body":
                            for param in comp.get("parameters", []) or []:
                                if isinstance(param, dict):
                                    txt = param.get("text")
                                    if isinstance(txt, str) and txt.strip():
                                        template_texts.append(txt.strip())
                    if template_texts:
                        main_body = max(template_texts, key=len)
                        return re.sub(r"\s+", " ", main_body).strip()
                for key in ["interactive", "postback", "template"]:
                    if key in data:
                        found = find_text(data[key])
                        if found:
                            return found
            
            if data is not None:
                found = find_text(data)
                if found:
                    return found
            
            if isinstance(data, str) and len(data) > 2:
                return data
            
            # Fallback: show payload itself (untrimmed) if it looks like JSON,
            # otherwise return the original string.
            if msg_str.startswith("{") or msg_str.startswith("["):
                return msg_str
            return msg_str
        
        # Simple cached English translation helper (same as Recent Messages)
        if "user_deepdive_translations" not in st.session_state:
            st.session_state.user_deepdive_translations = {}
        
        def translate_to_english(text: str) -> str:
            if not text or text.strip() == "":
                return ""
            # If translator library is unavailable, just return original text
            if GoogleTranslator is None:
                return "[Translation unavailable - deep_translator not installed]"
            cache = st.session_state.user_deepdive_translations
            if text in cache:
                return cache[text]
            try:
                # Limit text length to avoid API issues (Google Translate has limits)
                text_to_translate = text[:5000] if len(text) > 5000 else text
                translated = GoogleTranslator(source="auto", target="en").translate(text_to_translate)
                cache[text] = translated
                return translated
            except Exception as e:
                # If translation fails, cache the error and return original text
                # This prevents repeated failed attempts for the same text
                cache[text] = text
                # Log error for debugging (only show once per session)
                if "translation_error_shown_deepdive" not in st.session_state:
                    st.session_state.translation_error_shown_deepdive = True
                    st.warning(f"Translation error: {str(e)}. Showing original text. Check if deep_translator is properly installed.")
                return text
        
        if messages_df.empty:
            st.info("No messages found for this user.")
        else:
            # Detect audio: type='audio' or MIME like audio/ogg; codecs=opus, or message body
            def is_audio_msg(msg_type, raw_msg):
                if pd.isna(msg_type):
                    msg_type = ""
                t = str(msg_type).strip().lower()
                raw_str = "" if pd.isna(raw_msg) else str(raw_msg)
                return (
                    t == "audio"
                    or "audio/" in t
                    or "audio/ogg" in raw_str
                    or ("opus" in raw_str and "audio" in raw_str.lower())
                )

            # Detect sticker: check for "sticker" key in JSON and webp mime type
            def is_sticker_msg(msg_type, raw_msg):
                if pd.isna(msg_type):
                    msg_type = ""
                t = str(msg_type).strip().lower()
                raw_str = "" if pd.isna(raw_msg) else str(raw_msg)
                if t == "sticker":
                    return True
                if '"sticker"' in raw_str and "image/webp" in raw_str:
                    return True
                return False

            # Detect image: type image/photo or MIME like image/jpeg (excluding stickers)
            def is_image_msg(msg_type, raw_msg):
                if pd.isna(msg_type):
                    msg_type = ""
                t = str(msg_type).strip().lower()
                raw_str = "" if pd.isna(raw_msg) else str(raw_msg)
                # Exclude stickers from images
                if is_sticker_msg(msg_type, raw_msg):
                    return False
                if t in ("image", "photo"):
                    return True
                if "image/" in t or ('"image"' in raw_str and "image/jpeg" in raw_str):
                    return True
                return False

            # Merge audio + transcript (same user, within 120s).
            skip_idx_dd = set()
            transcript_for_audio_dd = {}
            for i in range(len(messages_df)):
                row = messages_df.iloc[i]
                if not is_audio_msg(row.get("msg_type"), row.get("message")):
                    continue
                try:
                    ts_cur = pd.to_datetime(row["sent_at"])
                except Exception:
                    continue
                for candidate_idx in [i - 1, i + 1]:
                    if candidate_idx < 0 or candidate_idx >= len(messages_df):
                        continue
                    if candidate_idx in skip_idx_dd:
                        continue
                    other = messages_df.iloc[candidate_idx]
                    if other["sender"] != row["sender"] or is_audio_msg(other.get("msg_type"), other.get("message")):
                        continue
                    try:
                        ts_other = pd.to_datetime(other["sent_at"])
                        if abs((ts_cur - ts_other).total_seconds()) <= 120:
                            transcript_for_audio_dd[i] = candidate_idx
                            skip_idx_dd.add(candidate_idx)
                            break
                    except Exception:
                        pass

            # Merge image + description: description is stored in message before (same user, within 120s).
            # Check i-1 first as that's the typical pattern.
            interpretation_for_image_dd = {}
            for i in range(len(messages_df)):
                row = messages_df.iloc[i]
                if not is_image_msg(row.get("msg_type"), row.get("message")):
                    continue
                try:
                    ts_cur = pd.to_datetime(row["sent_at"])
                except Exception:
                    continue
                for candidate_idx in [i - 1, i + 1]:
                    if candidate_idx < 0 or candidate_idx >= len(messages_df):
                        continue
                    if candidate_idx in skip_idx_dd:
                        continue
                    other = messages_df.iloc[candidate_idx]
                    if other["sender"] != row["sender"] or is_image_msg(other.get("msg_type"), other.get("message")):
                        continue
                    try:
                        ts_other = pd.to_datetime(other["sent_at"])
                        if abs((ts_cur - ts_other).total_seconds()) <= 120:
                            interpretation_for_image_dd[i] = candidate_idx
                            skip_idx_dd.add(candidate_idx)
                            break
                    except Exception:
                        pass

            # Merge sticker + description: stricter than image/audio to avoid false grouping.
            # Only use immediately previous message (i-1), from same sender, if it's text-like.
            description_for_sticker_dd = {}
            def is_text_like_for_sticker_dd(msg_type, raw_msg):
                if pd.isna(msg_type):
                    msg_type = ""
                t = str(msg_type).strip().lower()
                raw = "" if pd.isna(raw_msg) else str(raw_msg)
                if is_sticker_msg(msg_type, raw_msg) or is_audio_msg(msg_type, raw_msg) or is_image_msg(msg_type, raw_msg):
                    return False
                if "notification" in raw.lower() or '"template"' in raw.lower():
                    return False
                return t in ("text", "interactive", "quickreply", "postback", "flows", "") or "text" in raw.lower()

            for i in range(len(messages_df)):
                row = messages_df.iloc[i]
                if not is_sticker_msg(row.get("msg_type"), row.get("message")):
                    continue
                try:
                    ts_cur = pd.to_datetime(row["sent_at"])
                except Exception:
                    continue
                # Only previous row to prevent swallowing the next unrelated chat message.
                for candidate_idx in [i - 1]:
                    if candidate_idx < 0 or candidate_idx >= len(messages_df):
                        continue
                    if candidate_idx in skip_idx_dd:
                        continue
                    other = messages_df.iloc[candidate_idx]
                    if other["sender"] != row["sender"]:
                        continue
                    if not is_text_like_for_sticker_dd(other.get("msg_type"), other.get("message")):
                        continue
                    try:
                        ts_other = pd.to_datetime(other["sent_at"])
                        # Keep a tight window for caption-style companion messages.
                        if 0 <= (ts_cur - ts_other).total_seconds() <= 30:
                            description_for_sticker_dd[i] = candidate_idx
                            skip_idx_dd.add(candidate_idx)
                            break
                    except Exception:
                        pass

            # Type label: template, then icon for audio/image/sticker, else db type (text, interactive, quickReply, flows, etc.)
            def get_type_label_dd(msg_type_val, is_audio, is_image, is_sticker, is_tmpl):
                if is_tmpl:
                    return "template"
                if is_audio:
                    return "🎧"
                if is_sticker:
                    return "sticker"
                if is_image:
                    return "📷"
                t = msg_type_val if msg_type_val is not None and pd.notna(msg_type_val) else ""
                return str(t).strip() or "—"

            def get_msg_display_text(idx):
                if idx in skip_idx_dd:
                    return ""
                row = messages_df.iloc[idx]
                raw = row.get("message")
                if is_audio_msg(row.get("msg_type"), raw):
                    if idx in transcript_for_audio_dd:
                        trans_idx = transcript_for_audio_dd[idx]
                        prev = messages_df.iloc[trans_idx]
                        return extract_msg_text(prev.get("message")) or "[Audio]"
                    return "[Audio]"
                if is_sticker_msg(row.get("msg_type"), raw):
                    if idx in description_for_sticker_dd:
                        desc_idx = description_for_sticker_dd[idx]
                        prev = messages_df.iloc[desc_idx]
                        return extract_msg_text(prev.get("message")) or "[Sticker]"
                    return "[Sticker]"
                if is_image_msg(row.get("msg_type"), raw):
                    if idx in interpretation_for_image_dd:
                        interp_idx = interpretation_for_image_dd[idx]
                        prev = messages_df.iloc[interp_idx]
                        return extract_msg_text(prev.get("message")) or "[Image]"
                    return "[Image]"
                return extract_msg_text(raw)

            rows_history = []
            for i in range(len(messages_df)):
                if i in skip_idx_dd:
                    continue
                row = messages_df.iloc[i]
                is_audio = is_audio_msg(row.get("msg_type"), row.get("message"))
                is_sticker = is_sticker_msg(row.get("msg_type"), row.get("message"))
                is_image = is_image_msg(row.get("msg_type"), row.get("message"))
                text = get_msg_display_text(i)
                msg_type_val = row.get("msg_type")
                type_label = get_type_label_dd(msg_type_val, is_audio, is_image, is_sticker, is_template(row.get("message")))
                step_label = _label_ladder_step(row.get("matched_ladder_step")) if row["sender"] != "user" else ""
                if step_label:
                    type_label = f"{type_label} · 🪜 {step_label}"
                rows_history.append({
                    "Time": format_ts_local(row["sent_at"]),
                    "From": "👤 User" if row["sender"] == "user" else "🤖 Bot",
                    "Status": str(row.get("status")).lower() if pd.notna(row.get("status")) else "—",
                    "Type": type_label,
                    "Message": text,
                    "Message (EN)": translate_to_english(text) if translate_deep_dive_messages else "",
                })

            history_df = pd.DataFrame(rows_history)
            history_obj = history_df
            if not history_df.empty and "Status" in history_df.columns:
                def _highlight_failed_history_row(row):
                    failed = str(row.get("Status", "")).strip().lower() == "failed"
                    style = "background-color: rgba(239, 68, 68, 0.16);" if failed else ""
                    return [style] * len(row)
                history_obj = history_df.style.apply(_highlight_failed_history_row, axis=1)
            if wrap_deep_dive_messages:
                render_wrapped_messages_table(history_df)
            else:
                st.dataframe(
                    history_obj,
                    use_container_width=True,
                    hide_index=True,
                    height=420,
                    column_config={
                        "Time": st.column_config.TextColumn(width="small"),
                        "From": st.column_config.TextColumn(width="small"),
                        "Status": st.column_config.TextColumn(width="small"),
                        "Type": st.column_config.TextColumn(width="medium"),
                        "Message": st.column_config.TextColumn(width="large"),
                        "Message (EN)": st.column_config.TextColumn(width="large"),
                    }
                )


# Tab 3: User Retention
if selected_section == "📈 User Retention":
    # Recovery: weekly reply timeline + active/risk waterfall (top of tab)
    import altair as alt

    _ret_rl_start = (datetime.now() - timedelta(weeks=12)).strftime("%Y-%m-%d")
    try:
        _ret_recovery_events = get_recovery_ladder_events(_ret_rl_start)
    except Exception as e:
        _ret_recovery_events = pd.DataFrame()
        st.warning(f"Could not load recovery ladder data for retention tab: {e}")

    if _ret_recovery_events is not None and not _ret_recovery_events.empty:
        _ret_df = _ret_recovery_events.copy()
        _ret_df["week_start_sp"] = pd.to_datetime(_ret_df["week_start_sp"], errors="coerce")
        _ret_df["replied_before_next_template"] = _ret_df["replied_before_next_template"].fillna(False).astype(int)
        _ret_df = _ret_df[_ret_df["week_start_sp"].notna()].copy()
        _ret_weekly = (
            _ret_df.groupby("week_start_sp")
            .agg(
                templates_sent=("user_id", "size"),
                replied_templates=("replied_before_next_template", "sum"),
            )
            .reset_index()
            .sort_values("week_start_sp", ascending=False)
        )
        _ret_weekly["reply_rate_pct"] = (
            100.0 * _ret_weekly["replied_templates"] / _ret_weekly["templates_sent"]
        ).round(1)

        st.markdown("#### 📈 Weekly Timeline — Reply Rate (% sends)")
        _timeline_df = _ret_weekly[["week_start_sp", "reply_rate_pct"]].copy().sort_values("week_start_sp")
        _timeline_df = _timeline_df.rename(
            columns={"week_start_sp": "Week", "reply_rate_pct": "Reply rate (%)"}
        )
        _timeline_df["Week"] = pd.to_datetime(_timeline_df["Week"]).dt.strftime("%Y-%m-%d")
        _ret_timeline_chart = (
            alt.Chart(_timeline_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("Week:N", sort=None, title="Week"),
                y=alt.Y("Reply rate (%)", title="Reply rate (%)"),
                tooltip=[
                    alt.Tooltip("Week:N", title="Week"),
                    alt.Tooltip("Reply rate (%)", title="Reply rate (%)"),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(_ret_timeline_chart, use_container_width=True)

        st.markdown("#### 📋 Weekly Waterfall — Active, Inactive, Risk & De-risk")

        _ret_wf_default_start = (datetime.now() - timedelta(weeks=12)).date()
        _ret_wf_default_end = datetime.now().date()
        _ret_use_custom_wf = st.toggle(
            "Use custom date range for waterfall section",
            value=False,
            key="ret_waterfall_custom_dates_toggle",
            help="When off, uses the last 12 weeks.",
        )
        _ret_wf_start = _ret_wf_default_start
        _ret_wf_end = _ret_wf_default_end
        if _ret_use_custom_wf:
            _rw1, _rw2 = st.columns(2)
            with _rw1:
                _ret_wf_start = st.date_input(
                    "Waterfall start date",
                    value=_ret_wf_default_start,
                    key="ret_waterfall_start_date",
                )
            with _rw2:
                _ret_wf_end = st.date_input(
                    "Waterfall end date",
                    value=_ret_wf_default_end,
                    key="ret_waterfall_end_date",
                )

        if _ret_wf_start > _ret_wf_end:
            st.warning("Waterfall date range is invalid (`start > end`). Swapping dates automatically.")
            _ret_wf_start, _ret_wf_end = _ret_wf_end, _ret_wf_start

        try:
            _ret_waterfall_df = get_recovery_weekly_waterfall_metrics(_ret_wf_start.strftime("%Y-%m-%d"))
        except Exception as e:
            _ret_waterfall_df = pd.DataFrame()
            st.warning(f"Could not load weekly active/risk waterfall metrics: {e}")

        if _ret_waterfall_df is not None and not _ret_waterfall_df.empty:
            _ret_wf = _ret_waterfall_df.copy()
            _ret_wf["week_start_dt"] = pd.to_datetime(_ret_wf["week_start"], errors="coerce")
            _ret_wf = _ret_wf[
                (_ret_wf["week_start_dt"].notna())
                & (_ret_wf["week_start_dt"] >= pd.to_datetime(_ret_wf_start))
                & (_ret_wf["week_start_dt"] <= pd.to_datetime(_ret_wf_end))
            ].copy()
            _ret_wf = _ret_wf.sort_values("week_start", ascending=False)

            if _ret_wf.empty:
                st.info("No weekly waterfall data for the selected date range.")
            else:

                def _ret_build_waterfall_steps(steps):
                    rows = []
                    running = 0
                    for step_label, value, step_type in steps:
                        val = int(value)
                        if step_type == "total":
                            y0 = 0
                            y1 = val
                            running = val
                            display_val = val
                            direction = "total"
                        else:
                            y0 = running
                            y1 = running + val
                            running = y1
                            display_val = val
                            direction = "up" if val >= 0 else "down"
                        rows.append(
                            {
                                "step": step_label,
                                "start": y0,
                                "end": y1,
                                "value": display_val,
                                "bar_type": direction,
                            }
                        )
                    return pd.DataFrame(rows)

                st.markdown("#### 📈 Active user stack (flow per week)")
                _ret_seg_order = [
                    "Active (week start)",
                    "New acquired",
                    "Reactivated",
                    "Inactive / churned (farewell)",
                ]
                _ret_seg_stack_order = {s: i for i, s in enumerate(_ret_seg_order)}
                _ret_stack_rows = []
                for _, _rr in _ret_wf.sort_values("week_start").iterrows():
                    _ret_stack_rows.extend(
                        [
                            {
                                "week_start": _rr["week_start"],
                                "segment": "Active (week start)",
                                "stack_order": _ret_seg_stack_order["Active (week start)"],
                                "users": int(_rr["start_active_users"]),
                            },
                            {
                                "week_start": _rr["week_start"],
                                "segment": "New acquired",
                                "stack_order": _ret_seg_stack_order["New acquired"],
                                "users": int(_rr["new_acquired_users"]),
                            },
                            {
                                "week_start": _rr["week_start"],
                                "segment": "Reactivated",
                                "stack_order": _ret_seg_stack_order["Reactivated"],
                                "users": int(_rr["reactivated_users"]),
                            },
                            {
                                "week_start": _rr["week_start"],
                                "segment": "Inactive / churned (farewell)",
                                "stack_order": _ret_seg_stack_order["Inactive / churned (farewell)"],
                                "users": -int(_rr["became_inactive_users"]),
                            },
                        ]
                    )
                _ret_stock_stack_df = pd.DataFrame(_ret_stack_rows)
                _ret_stock_stack_df["users_abs"] = _ret_stock_stack_df["users"].abs()
                _ret_stock_chart = (
                    alt.Chart(_ret_stock_stack_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("week_start:N", sort=None, title="Week"),
                        y=alt.Y("users:Q", stack="zero", title="Users"),
                        color=alt.Color(
                            "segment:N",
                            title="",
                            sort=_ret_seg_order,
                            scale=alt.Scale(
                                domain=_ret_seg_order,
                                range=["#60a5fa", "#22c55e", "#a78bfa", "#ef4444"],
                            ),
                        ),
                        order=alt.Order("stack_order:Q"),
                        tooltip=[
                            alt.Tooltip("week_start:N", title="Week"),
                            alt.Tooltip("segment:N", title="Segment"),
                            alt.Tooltip("users_abs:Q", title="Users (|count|)"),
                        ],
                    )
                    .properties(height=340)
                )
                st.altair_chart(_ret_stock_chart, use_container_width=True)

                st.markdown("#### 📈 Inactive / churned — cumulative (farewell sends)")
                _ret_wf_cum = _ret_wf.sort_values("week_start").copy()
                _ret_wf_cum["new_inactive"] = _ret_wf_cum["became_inactive_users"].astype(int)
                _ret_wf_cum["cum_inactive_end"] = _ret_wf_cum["new_inactive"].cumsum()
                _ret_wf_cum["prior_cumulative"] = _ret_wf_cum["cum_inactive_end"] - _ret_wf_cum["new_inactive"]
                _ret_cum_stack_rows = []
                for _, _rcr in _ret_wf_cum.iterrows():
                    _ret_cum_stack_rows.append(
                        {
                            "week_start": _rcr["week_start"],
                            "segment": "Prior cumulative",
                            "stack_order": 0,
                            "users": int(_rcr["prior_cumulative"]),
                        }
                    )
                    _ret_cum_stack_rows.append(
                        {
                            "week_start": _rcr["week_start"],
                            "segment": "New this week",
                            "stack_order": 1,
                            "users": int(_rcr["new_inactive"]),
                        }
                    )
                _ret_cum_stack_df = pd.DataFrame(_ret_cum_stack_rows)
                _ret_cum_seg_order = ["Prior cumulative", "New this week"]
                _ret_cum_tooltip = _ret_cum_stack_df.merge(
                    _ret_wf_cum[["week_start", "cum_inactive_end"]], on="week_start", how="left"
                )
                _ret_inactive_cum_chart = (
                    alt.Chart(_ret_cum_tooltip)
                    .mark_bar()
                    .encode(
                        x=alt.X("week_start:N", sort=None, title="Week"),
                        y=alt.Y("users:Q", stack="zero", title="Users (cumulative inactive)"),
                        color=alt.Color(
                            "segment:N",
                            title="",
                            sort=_ret_cum_seg_order,
                            scale=alt.Scale(
                                domain=_ret_cum_seg_order,
                                range=["#64748b", "#ef4444"],
                            ),
                        ),
                        order=alt.Order("stack_order:Q"),
                        tooltip=[
                            alt.Tooltip("week_start:N", title="Week"),
                            alt.Tooltip("segment:N", title="Segment"),
                            alt.Tooltip("users:Q", title="Users in segment"),
                            alt.Tooltip("cum_inactive_end:Q", title="Total cumulative (week end)"),
                        ],
                    )
                    .properties(height=320)
                )
                st.altair_chart(_ret_inactive_cum_chart, use_container_width=True)
                st.caption(
                    "Running total of **became inactive (farewell)** counts week over week from the **waterfall start date**. "
                    "Each bar stacks **prior cumulative** (gray) + **new this week** (red); bar top = cumulative through that week."
                )

                st.markdown("#### 📈 Weekly Timeline — Active & At-risk Stock")
                _ret_selected_week = st.selectbox(
                    "Select week for waterfall visuals",
                    options=_ret_wf["week_start"].tolist(),
                    index=0,
                    key="ret_waterfall_week_select",
                )
                _ret_wrow = _ret_wf[_ret_wf["week_start"] == _ret_selected_week].iloc[0]

                _ret_active_steps = _ret_build_waterfall_steps(
                    [
                        ("Start active", _ret_wrow["start_active_users"], "total"),
                        ("+ New acquired", _ret_wrow["new_acquired_users"], "delta"),
                        ("+ Reactivated", _ret_wrow["reactivated_users"], "delta"),
                        ("- Became inactive", -int(_ret_wrow["became_inactive_users"]), "delta"),
                        ("End active", _ret_wrow["end_active_users_observed"], "total"),
                    ]
                )
                _ret_risk_24h_steps = _ret_build_waterfall_steps(
                    [
                        ("Start risk stock", _ret_wrow["start_risk_24h_users"], "total"),
                        ("+ New at risk", _ret_wrow["new_risk_24h_users"], "delta"),
                        ("- De-risked", -int(_ret_wrow["derisked_24h_users"]), "delta"),
                        ("End risk stock", _ret_wrow["end_risk_24h_users"], "total"),
                    ]
                )
                _ret_risk_rl_steps = _ret_build_waterfall_steps(
                    [
                        ("Start risk stock", _ret_wrow["start_risk_rl_users"], "total"),
                        ("+ New at risk", _ret_wrow["new_risk_rl_users"], "delta"),
                        ("- De-risked", -int(_ret_wrow["derisked_rl_users"]), "delta"),
                        ("End risk stock", _ret_wrow["end_risk_rl_users"], "total"),
                    ]
                )
                _ret_color_scale = alt.Scale(
                    domain=["up", "down", "total"],
                    range=["#22c55e", "#ef4444", "#60a5fa"],
                )
                _ret_active_chart = (
                    alt.Chart(_ret_active_steps)
                    .mark_bar()
                    .encode(
                        x=alt.X("step:N", sort=None, title=None),
                        y=alt.Y("start:Q", title="Users"),
                        y2="end:Q",
                        color=alt.Color("bar_type:N", scale=_ret_color_scale, legend=None),
                        tooltip=[
                            alt.Tooltip("step:N", title="Step"),
                            alt.Tooltip("value:Q", title="Delta / Level"),
                            alt.Tooltip("start:Q", title="From"),
                            alt.Tooltip("end:Q", title="To"),
                        ],
                    )
                    .properties(height=300, title=f"Active Stock Waterfall ({_ret_selected_week})")
                )
                _ret_risk_24h_chart = (
                    alt.Chart(_ret_risk_24h_steps)
                    .mark_bar()
                    .encode(
                        x=alt.X("step:N", sort=None, title=None),
                        y=alt.Y("start:Q", title="Users"),
                        y2="end:Q",
                        color=alt.Color("bar_type:N", scale=_ret_color_scale, legend=None),
                        tooltip=[
                            alt.Tooltip("step:N", title="Step"),
                            alt.Tooltip("value:Q", title="Delta / Level"),
                            alt.Tooltip("start:Q", title="From"),
                            alt.Tooltip("end:Q", title="To"),
                        ],
                    )
                    .properties(
                        height=300,
                        title=f"At-risk Stock Waterfall (24h No Message) ({_ret_selected_week})",
                    )
                )
                _ret_risk_rl_chart = (
                    alt.Chart(_ret_risk_rl_steps)
                    .mark_bar()
                    .encode(
                        x=alt.X("step:N", sort=None, title=None),
                        y=alt.Y("start:Q", title="Users"),
                        y2="end:Q",
                        color=alt.Color("bar_type:N", scale=_ret_color_scale, legend=None),
                        tooltip=[
                            alt.Tooltip("step:N", title="Step"),
                            alt.Tooltip("value:Q", title="Delta / Level"),
                            alt.Tooltip("start:Q", title="From"),
                            alt.Tooltip("end:Q", title="To"),
                        ],
                    )
                    .properties(
                        height=300,
                        title=f"At-risk Stock Waterfall (Recovery Ladder) ({_ret_selected_week})",
                    )
                )
                st.altair_chart(_ret_active_chart, use_container_width=True)
                _ret_c1, _ret_c2 = st.columns(2)
                with _ret_c1:
                    st.altair_chart(_ret_risk_24h_chart, use_container_width=True)
                with _ret_c2:
                    st.altair_chart(_ret_risk_rl_chart, use_container_width=True)
                st.caption(
                    f"Active reconciliation gap for `{_ret_selected_week}`: "
                    f"{int(_ret_wrow['active_reconciliation_gap'])} "
                    "(observed end active - computed end active)."
                )
                _ret_waterfall_display = _ret_wf.drop(columns=["week_start_dt"]).rename(
                    columns={
                        "week_start": "Week",
                        "start_active_users": "Start active users",
                        "new_acquired_users": "+ New acquired",
                        "reactivated_users": "+ Reactivated",
                        "became_inactive_users": "- Became inactive (farewell)",
                        "end_active_users_computed": "End active (computed)",
                        "end_active_users_observed": "End active (observed)",
                        "active_reconciliation_gap": "Active reconciliation gap",
                        "start_risk_24h_users": "Start risk 24h stock",
                        "new_risk_24h_users": "New at-risk 24h",
                        "derisked_24h_users": "De-risked after 24h",
                        "end_risk_24h_users": "End risk 24h stock",
                        "start_risk_rl_users": "Start RL risk stock",
                        "new_risk_rl_users": "New at-risk RL",
                        "derisked_rl_users": "De-risked after RL",
                        "end_risk_rl_users": "End RL risk stock",
                    }
                )
                st.dataframe(_ret_waterfall_display, use_container_width=True, hide_index=True)
        else:
            st.info("No weekly waterfall data for this date range.")
    else:
        st.caption(
            "No recovery ladder sends in the last 12 weeks — reply timeline and waterfall are unavailable."
        )

    st.markdown("---")
    st.markdown("### 📈 User Retention by Weekly Cohort")
    # Use shared loader so all tabs/queries stay in sync with the latest internal-users.json
    internal_waids = load_internal_users()
    
    # Build the retention query
    internal_waids_str = "', '".join(internal_waids)

    retention_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),

product_users AS (
  SELECT id, full_name, waid
  FROM users
  WHERE waid NOT IN (SELECT waid FROM internal_waids)
),

user_messages AS (
  SELECT
    u.id  AS user_id,
    u.full_name,
    u.waid,
    (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),

user_first_last AS (
  SELECT
    user_id,
    full_name,
    waid,
    MIN(local_date) AS first_active_date,
    MAX(local_date) AS last_active_date
  FROM user_messages
  GROUP BY user_id, full_name, waid
),

-- 1) Assign each user to a weekly cohort by first-active week (local time)
user_with_cohort_week AS (
  SELECT
    ufl.*,
    date_trunc('week', ufl.first_active_date)::date AS cohort_week_start
  FROM user_first_last ufl
),

-- 2) Map each active date to a cohort_day within that user's lifetime
user_days AS (
  SELECT
    um.user_id,
    uwc.full_name,
    uwc.waid,
    uwc.cohort_week_start,
    um.local_date,
    uwc.first_active_date,
    (um.local_date - uwc.first_active_date)       AS day_offset,
    (um.local_date - uwc.first_active_date) + 1   AS cohort_day
  FROM user_messages um
  JOIN user_with_cohort_week uwc ON uwc.user_id = um.user_id
),

-- 3) Cohort size per week (denominator)
cohort_sizes AS (
  SELECT
    cohort_week_start,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM user_with_cohort_week
  GROUP BY cohort_week_start
),

-- 4) Numerator: active users per cohort week & cohort day
users_active_on_day AS (
  SELECT
    cohort_week_start,
    cohort_day,
    COUNT(DISTINCT user_id) AS active_users,
    STRING_AGG(DISTINCT full_name, ', ' ORDER BY full_name) AS user_names
  FROM user_days
  GROUP BY cohort_week_start, cohort_day
),

-- 5) Final weekly cohort retention table
cohort_retention_weekly AS (
  SELECT
    ua.cohort_week_start,
    ua.cohort_day,
    cs.cohort_size,
    COALESCE(ua.active_users, 0) AS active_users,
    ROUND(
      100.0 * COALESCE(ua.active_users, 0) / cs.cohort_size,
      1
    ) AS retention_pct,
    COALESCE(ua.user_names, '') AS user_names
  FROM users_active_on_day ua
  JOIN cohort_sizes cs
    ON cs.cohort_week_start = ua.cohort_week_start
)

SELECT
  cohort_week_start,
  cohort_day,
  cohort_size,
  active_users,
  retention_pct,
  user_names
FROM cohort_retention_weekly
ORDER BY cohort_week_start, cohort_day
"""
    
    try:
        retention_df = run_query(retention_query)
        
        if retention_df.empty:
            st.info("No retention data available yet. Users need to send messages to generate cohort data.")
        else:
            # Summary metrics
            total_cohorts = retention_df['cohort_week_start'].nunique()
            total_users = retention_df.groupby('cohort_week_start')['cohort_size'].first().sum()
            
            # Get rolling-window retention data for summary metrics
            try:
                rolling_retention_summary_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
product_users AS (
  SELECT id, full_name, waid
  FROM users
  WHERE waid NOT IN (SELECT waid FROM internal_waids)
),
user_messages AS (
  SELECT
    u.id  AS user_id,
    u.full_name,
    u.waid,
    (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),
user_first_last AS (
  SELECT
    user_id,
    full_name,
    waid,
    MIN(local_date) AS first_active_date,
    MAX(local_date) AS last_active_date
  FROM user_messages
  GROUP BY user_id, full_name, waid
),
user_with_cohort_week AS (
  SELECT
    ufl.*,
    date_trunc('week', ufl.first_active_date)::date AS cohort_week_start
  FROM user_first_last ufl
),
user_days AS (
  SELECT
    um.user_id,
    uwc.full_name,
    uwc.waid,
    uwc.cohort_week_start,
    um.local_date,
    uwc.first_active_date,
    (um.local_date - uwc.first_active_date) AS cohort_day
  FROM user_messages um
  JOIN user_with_cohort_week uwc ON uwc.user_id = um.user_id
),
user_activity_by_day AS (
  SELECT
    user_id,
    cohort_week_start,
    MAX(CASE WHEN cohort_day <= 7 AND cohort_day >= 1 THEN 1 ELSE 0 END) AS active_day1_to_7
  FROM user_days
  GROUP BY user_id, cohort_week_start
),
cohort_sizes AS (
  SELECT
    cohort_week_start,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM user_with_cohort_week
  GROUP BY cohort_week_start
),
rolling_retention_summary AS (
  SELECT
    uwc.cohort_week_start,
    uwc.user_id,
    COALESCE(uabd.active_day1_to_7, 0) AS active_within_7d
  FROM user_with_cohort_week uwc
  LEFT JOIN user_activity_by_day uabd ON uabd.user_id = uwc.user_id AND uabd.cohort_week_start = uwc.cohort_week_start
),
cohort_7d_retention AS (
  SELECT
    rrs.cohort_week_start,
    cs.cohort_size,
    CASE 
      WHEN (CURRENT_DATE - rrs.cohort_week_start) >= 7 
      THEN ROUND(100.0 * SUM(rrs.active_within_7d) / cs.cohort_size, 1) 
      ELSE NULL 
    END AS retention_7d
  FROM rolling_retention_summary rrs
  JOIN cohort_sizes cs ON cs.cohort_week_start = rrs.cohort_week_start
  GROUP BY rrs.cohort_week_start, cs.cohort_size
)
SELECT AVG(retention_7d) AS avg_7d_retention
FROM cohort_7d_retention
WHERE retention_7d IS NOT NULL
"""
                avg_7d_result = run_query(rolling_retention_summary_query)
                avg_7d_retention = avg_7d_result['avg_7d_retention'].iloc[0] if not avg_7d_result.empty and not pd.isna(avg_7d_result['avg_7d_retention'].iloc[0]) else None
            except Exception:
                avg_7d_retention = None
            
            # Get avg and median days active for top section
            try:
                days_active_summary_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
product_users AS (
  SELECT id, full_name, waid
  FROM users
  WHERE waid NOT IN (SELECT waid FROM internal_waids)
),
user_messages AS (
  SELECT
    u.id  AS user_id,
    u.full_name,
    u.waid,
    (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),
user_first_last AS (
  SELECT
    user_id,
    full_name,
    waid,
    MIN(local_date) AS first_active_date,
    MAX(local_date) AS last_active_date
  FROM user_messages
  GROUP BY user_id, full_name, waid
),
user_with_cohort_week AS (
  SELECT
    ufl.*,
    date_trunc('week', ufl.first_active_date)::date AS cohort_week_start
  FROM user_first_last ufl
),
user_days AS (
  SELECT
    um.user_id,
    uwc.cohort_week_start,
    um.local_date,
    (um.local_date - uwc.first_active_date) AS cohort_day
  FROM user_messages um
  JOIN user_with_cohort_week uwc ON uwc.user_id = um.user_id
),
user_days_active AS (
  SELECT
    user_id,
    cohort_week_start,
    COUNT(DISTINCT local_date) FILTER (WHERE cohort_day <= 13) AS days_active_week2
  FROM user_days
  GROUP BY user_id, cohort_week_start
)
SELECT
  ROUND(AVG(days_active_week2), 1) AS avg_days_active,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_active_week2) AS median_days_active
FROM user_days_active
"""
                days_active_summary = run_query(days_active_summary_query)
                avg_days_active_summary = days_active_summary['avg_days_active'].iloc[0] if not days_active_summary.empty else None
                median_days_active_summary = days_active_summary['median_days_active'].iloc[0] if not days_active_summary.empty else None
            except Exception:
                avg_days_active_summary = None
                median_days_active_summary = None
            
            # Avg activities completed per week (per user: total completions / weeks in product, then avg)
            try:
                avg_activities_per_week_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
product_users AS (
  SELECT id, full_name, waid
  FROM users
  WHERE waid NOT IN (SELECT waid FROM internal_waids)
),
user_messages AS (
  SELECT u.id AS user_id, (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),
user_first_active AS (
  SELECT user_id, MIN(local_date) AS first_active_date
  FROM user_messages
  GROUP BY user_id
),
today_local AS (
  SELECT ((NOW() AT TIME ZONE 'America/Sao_Paulo')::date) AS d
),
user_completions AS (
  SELECT
    uah.user_id,
    COUNT(*)::int AS total_completions
  FROM user_activities_history uah
  JOIN user_first_active ufa ON ufa.user_id = uah.user_id
  GROUP BY uah.user_id
),
user_weeks AS (
  SELECT
    ufa.user_id,
    GREATEST(1, ((tl.d - ufa.first_active_date) / 7)) AS weeks_in_product
  FROM user_first_active ufa
  CROSS JOIN today_local tl
)
SELECT ROUND(AVG(uc.total_completions::numeric / uw.weeks_in_product), 1) AS avg_activities_per_week
FROM user_completions uc
JOIN user_weeks uw ON uw.user_id = uc.user_id
"""
                avg_act_result = run_query(avg_activities_per_week_query)
                avg_activities_per_week = avg_act_result['avg_activities_per_week'].iloc[0] if not avg_act_result.empty and not pd.isna(avg_act_result['avg_activities_per_week'].iloc[0]) else None
            except Exception:
                avg_activities_per_week = None
            
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            col1.metric("Total Cohort Weeks", total_cohorts)
            col2.metric("Total Users (All Cohorts)", total_users)
            col3.metric("Avg 7d Retention", f"{avg_7d_retention:.1f}%" if avg_7d_retention is not None and not pd.isna(avg_7d_retention) else "N/A")
            col4.metric("Avg Days Active", f"{avg_days_active_summary:.1f}" if avg_days_active_summary is not None and not pd.isna(avg_days_active_summary) else "N/A")
            col5.metric("Median Days Active", f"{median_days_active_summary:.1f}" if median_days_active_summary is not None and not pd.isna(median_days_active_summary) else "N/A")
            col6.metric("Avg activities/week", f"{avg_activities_per_week:.1f}" if avg_activities_per_week is not None and not pd.isna(avg_activities_per_week) else "N/A")
            
            # Quick overview: three tables (most active / most inactive / new users)
            try:
                overview_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
product_users AS (
  SELECT id, full_name, waid
  FROM users
  WHERE waid NOT IN (SELECT waid FROM internal_waids)
),
user_messages AS (
  SELECT
    u.id AS user_id,
    u.full_name,
    u.waid,
    (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),
user_first_last AS (
  SELECT
    user_id,
    full_name,
    MAX(waid) AS waid,
    MIN(local_date) AS first_active_date,
    MAX(local_date) AS last_active_date
  FROM user_messages
  GROUP BY user_id, full_name
),
-- "Today" in same timezone as local_date (America/Sao_Paulo) for consistent lifetime
today_local AS (
  SELECT ((NOW() AT TIME ZONE 'America/Sao_Paulo')::date) AS d
),
user_days_count AS (
  SELECT user_id, COUNT(DISTINCT local_date)::int AS active_days
  FROM user_messages
  GROUP BY user_id
),
activities_completed AS (
  SELECT uah.user_id, COUNT(*)::int AS activities_completed
  FROM user_activities_history uah
  JOIN product_users pu ON pu.id = uah.user_id
  WHERE uah.completed_at IS NOT NULL
  GROUP BY uah.user_id
),
user_stats AS (
  SELECT
    ufl.user_id,
    ufl.full_name,
    ufl.waid,
    (tl.d - ufl.first_active_date) AS lifetime,
    udc.active_days,
    COALESCE(ac.activities_completed, 0)::int AS activities_completed
  FROM user_first_last ufl
  CROSS JOIN today_local tl
  JOIN user_days_count udc ON udc.user_id = ufl.user_id
  LEFT JOIN activities_completed ac ON ac.user_id = ufl.user_id
)
SELECT user_id, full_name, waid, lifetime, active_days, activities_completed FROM user_stats ORDER BY full_name
"""
                overview_df = run_query(overview_query)
                if not overview_df.empty:
                    established = overview_df[overview_df['lifetime'] >= 7].copy()
                    new_users = overview_df[overview_df['lifetime'] < 7].copy()
                    most_active = established.sort_values('activities_completed', ascending=False)[['user_id', 'full_name', 'waid', 'activities_completed', 'active_days', 'lifetime']].reset_index(drop=True)
                    most_active['full_name'] = most_active.apply(lambda r: format_display_name(r['full_name'], r['waid'], user_id=r.get('user_id')), axis=1)
                    most_active = most_active[['full_name', 'activities_completed', 'active_days', 'lifetime']]
                    most_active.columns = ['Name', 'Activities completed', 'Active days', 'Lifetime (days)']
                    most_inactive = established.sort_values('activities_completed', ascending=True)[['user_id', 'full_name', 'waid', 'activities_completed', 'active_days', 'lifetime']].reset_index(drop=True)
                    most_inactive['full_name'] = most_inactive.apply(lambda r: format_display_name(r['full_name'], r['waid'], user_id=r.get('user_id')), axis=1)
                    most_inactive = most_inactive[['full_name', 'activities_completed', 'active_days', 'lifetime']]
                    most_inactive.columns = ['Name', 'Activities completed', 'Active days', 'Lifetime (days)']
                    new_users_display = new_users.sort_values('activities_completed', ascending=False)[['user_id', 'full_name', 'waid', 'activities_completed', 'active_days', 'lifetime']].reset_index(drop=True)
                    new_users_display['full_name'] = new_users_display.apply(lambda r: format_display_name(r['full_name'], r['waid'], user_id=r.get('user_id')), axis=1)
                    new_users_display = new_users_display[['full_name', 'activities_completed', 'active_days', 'lifetime']]
                    new_users_display.columns = ['Name', 'Activities completed', 'Active days', 'Lifetime (days)']
                    st.markdown("#### Quick overview")
                    st.caption("Active/inactive order by total activities completed. Lifetime (days) = days since first activity (São Paulo). Established = 7+ days; New users = under 7 days.")
                    t1, t2, t3 = st.columns(3)
                    with t1:
                        st.markdown("**Most active** (established)")
                        st.dataframe(most_active, use_container_width=True, hide_index=True)
                    with t2:
                        st.markdown("**Most inactive** (established)")
                        st.dataframe(most_inactive, use_container_width=True, hide_index=True)
                    with t3:
                        st.markdown("**New users** (< 7 days)")
                        st.dataframe(new_users_display, use_container_width=True, hide_index=True)
            except Exception:
                pass
            
            st.markdown("---")
            
            # Retention curves
            st.markdown("#### 📉 Retention Curves by Cohort")
            st.caption("Each line shows the retention curve for a weekly cohort: retention % (y-axis) over days since first active (x-axis).")

            # Build data for retention curves (one line per cohort)
            import altair as alt
            
            curves_data = retention_df.copy()
            curves_data['cohort_week_start_str'] = curves_data['cohort_week_start'].astype(str)

            # Limit to first 50 days for readability
            curves_data_filtered = curves_data[curves_data['cohort_day'] <= 50].copy()

            if not curves_data_filtered.empty:
                curves_chart = alt.Chart(curves_data_filtered).mark_line(point=True).encode(
                    x=alt.X('cohort_day:Q', title='Days Since First Active'),
                    y=alt.Y('retention_pct:Q', title='Retention %'),
                    color=alt.Color('cohort_week_start_str:N', title='Cohort Week'),
                    tooltip=[
                        alt.Tooltip('cohort_week_start_str:N', title='Cohort Week'),
                        alt.Tooltip('cohort_day:Q', title='Day'),
                        alt.Tooltip('retention_pct:Q', title='Retention %', format='.1f'),
                        alt.Tooltip('active_users:Q', title='Active Users'),
                        alt.Tooltip('cohort_size:Q', title='Cohort Size'),
                    ]
                ).properties(
                    height=300
                )

                st.altair_chart(curves_chart, use_container_width=True)
            
            st.markdown("---")
            
            # Activity completions by cohort: consecutive 7-day blocks (W1=days 0-6, W2=7-13, W3=14-20, W4=21-27)
            try:
                activity_completions_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
product_users AS (
  SELECT id, full_name, waid
  FROM users
  WHERE waid NOT IN (SELECT waid FROM internal_waids)
),
user_messages AS (
  SELECT
    u.id  AS user_id,
    (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),
user_first_last AS (
  SELECT user_id, MIN(local_date) AS first_active_date
  FROM user_messages
  GROUP BY user_id
),
user_with_cohort_week AS (
  SELECT
    user_id,
    first_active_date,
    date_trunc('week', first_active_date)::date AS cohort_week_start
  FROM user_first_last
),
-- Consecutive 7-day blocks: W1=0-6, W2=7-13, W3=14-20, W4=21-27 (count per block only)
completions_by_block AS (
  SELECT
    uah.user_id,
    uwc.cohort_week_start,
    COUNT(*) FILTER (WHERE ((uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date - uwc.first_active_date) BETWEEN 0 AND 6)::int AS c_w1,
    COUNT(*) FILTER (WHERE ((uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date - uwc.first_active_date) BETWEEN 7 AND 13)::int AS c_w2,
    COUNT(*) FILTER (WHERE ((uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date - uwc.first_active_date) BETWEEN 14 AND 20)::int AS c_w3,
    COUNT(*) FILTER (WHERE ((uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date - uwc.first_active_date) BETWEEN 21 AND 27)::int AS c_w4
  FROM user_activities_history uah
  JOIN user_with_cohort_week uwc ON uwc.user_id = uah.user_id
  WHERE ((uah.completed_at AT TIME ZONE 'America/Sao_Paulo')::date - uwc.first_active_date) BETWEEN 0 AND 27
  GROUP BY uah.user_id, uwc.cohort_week_start
),
user_completions_blocks AS (
  SELECT
    uwc.user_id,
    uwc.cohort_week_start,
    COALESCE(c.c_w1, 0) AS c_w1,
    COALESCE(c.c_w2, 0) AS c_w2,
    COALESCE(c.c_w3, 0) AS c_w3,
    COALESCE(c.c_w4, 0) AS c_w4
  FROM user_with_cohort_week uwc
  LEFT JOIN completions_by_block c ON c.user_id = uwc.user_id AND c.cohort_week_start = uwc.cohort_week_start
),
cohort_sizes AS (
  SELECT cohort_week_start, COUNT(DISTINCT user_id) AS cohort_size
  FROM user_with_cohort_week
  GROUP BY cohort_week_start
)
SELECT
  ucb.cohort_week_start,
  cs.cohort_size,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 1) / cs.cohort_size, 1) AS w1_pct_1plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 2) / cs.cohort_size, 1) AS w1_pct_2plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 3) / cs.cohort_size, 1) AS w1_pct_3plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 4) / cs.cohort_size, 1) AS w1_pct_4plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w2 >= 1) / cs.cohort_size, 1) AS w2_pct_1plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w2 >= 2) / cs.cohort_size, 1) AS w2_pct_2plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w2 >= 3) / cs.cohort_size, 1) AS w2_pct_3plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w2 >= 4) / cs.cohort_size, 1) AS w2_pct_4plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w3 >= 1) / cs.cohort_size, 1) AS w3_pct_1plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w3 >= 2) / cs.cohort_size, 1) AS w3_pct_2plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w3 >= 3) / cs.cohort_size, 1) AS w3_pct_3plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w3 >= 4) / cs.cohort_size, 1) AS w3_pct_4plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w4 >= 1) / cs.cohort_size, 1) AS w4_pct_1plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w4 >= 2) / cs.cohort_size, 1) AS w4_pct_2plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w4 >= 3) / cs.cohort_size, 1) AS w4_pct_3plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w4 >= 4) / cs.cohort_size, 1) AS w4_pct_4plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 1 AND ucb.c_w2 >= 1) / cs.cohort_size, 1) AS w1_w2_1plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 2 AND ucb.c_w2 >= 2) / cs.cohort_size, 1) AS w1_w2_2plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 3 AND ucb.c_w2 >= 3) / cs.cohort_size, 1) AS w1_w2_3plus,
  ROUND(100.0 * COUNT(*) FILTER (WHERE ucb.c_w1 >= 4 AND ucb.c_w2 >= 4) / cs.cohort_size, 1) AS w1_w2_4plus
FROM user_completions_blocks ucb
JOIN cohort_sizes cs ON cs.cohort_week_start = ucb.cohort_week_start
GROUP BY ucb.cohort_week_start, cs.cohort_size
ORDER BY ucb.cohort_week_start DESC
"""
                activity_completions_df = run_query(activity_completions_query)
                if not activity_completions_df.empty:
                    st.markdown("---")
                    st.markdown("#### 🏃 Activity completions by cohort (consecutive 7-day blocks)")

                    import altair as alt
                    try:
                        from datetime import datetime
                        import pytz
                        today_local = datetime.now(pytz.timezone("America/Sao_Paulo")).date()
                    except Exception:
                        today_local = pd.Timestamp.utcnow().date()

                    ac = activity_completions_df.copy()
                    ac["cohort_week_start"] = pd.to_datetime(ac["cohort_week_start"])
                    ac["days_old"] = (pd.Timestamp(today_local) - ac["cohort_week_start"]).dt.days

                    # Line chart: X = W1, W2, W3, W4 (consecutive blocks); Y = avg % who did 1+/2+/3+/4+ in THAT block
                    chart_rows = []
                    for w_idx, (w_name, w_days) in enumerate([("W1", 7), ("W2", 14), ("W3", 21), ("W4", 28)]):
                        eligible = ac[ac["days_old"] >= w_days]
                        if eligible.empty:
                            continue
                        for metric in ["1plus", "2plus", "3plus", "4plus"]:
                            col = f"w{w_idx + 1}_pct_{metric}"
                            if col in eligible.columns:
                                chart_rows.append({
                                    "window": w_name,
                                    "metric": f"{metric.replace('plus', '+')} in block",
                                    "pct": eligible[col].mean(),
                                })
                    if chart_rows:
                        chart_df = pd.DataFrame(chart_rows)
                        chart = alt.Chart(chart_df).mark_line(point=True).encode(
                            x=alt.X("window:N", title="Block", sort=["W1", "W2", "W3", "W4"]),
                            y=alt.Y("pct:Q", title="%"),
                            color=alt.Color("metric:N", title="Completed", scale=alt.Scale(range=["#00d4aa", "#7b68ee", "#f59e0b", "#ef4444"])),
                            tooltip=[alt.Tooltip("window:N", title="Block"), alt.Tooltip("metric:N", title="Metric"), alt.Tooltip("pct:Q", title="%", format=".1f")],
                        ).properties(height=320, title="% of cohort with 1+ / 2+ / 3+ / 4+ activities per block")
                        st.altair_chart(chart, use_container_width=True)
                        st.caption("Averages include only cohorts old enough for that block (e.g. W4 only cohorts ≥ 28 days old).")
                    else:
                        st.caption("Not enough cohort data yet for the chart.")

                    # Table 1: W1 only — % completed 1+, 2+, 3+, 4+ in first 7 days (cohorts with data)
                    st.markdown("**W1 (first 7 days) — % of cohort who completed 1+, 2+, 3+, 4+**")
                    ac_w1 = ac[ac["days_old"] >= 7][["cohort_week_start", "cohort_size", "w1_pct_1plus", "w1_pct_2plus", "w1_pct_3plus", "w1_pct_4plus"]].copy()
                    ac_w1["cohort_week_start"] = ac_w1["cohort_week_start"].dt.strftime("%Y-%m-%d")
                    ac_w1 = ac_w1.rename(columns={
                        "cohort_week_start": "Cohort Week",
                        "cohort_size": "Cohort Size",
                        "w1_pct_1plus": "% 1+",
                        "w1_pct_2plus": "% 2+",
                        "w1_pct_3plus": "% 3+",
                        "w1_pct_4plus": "% 4+",
                    })
                    st.dataframe(
                        ac_w1,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Cohort Week": st.column_config.TextColumn(width="small"),
                            "Cohort Size": st.column_config.NumberColumn(width="small", format="%d"),
                            "% 1+": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 2+": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 3+": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 4+": st.column_config.NumberColumn(width="small", format="%.1f"),
                        }
                    )
                    # Table 2: W2 only — % completed 1+, 2+, 3+, 4+ in second 7 days
                    st.markdown("**W2 (second 7 days) — % of cohort who completed 1+, 2+, 3+, 4+**")
                    ac_w2 = ac[ac["days_old"] >= 14][["cohort_week_start", "cohort_size", "w2_pct_1plus", "w2_pct_2plus", "w2_pct_3plus", "w2_pct_4plus"]].copy()
                    ac_w2["cohort_week_start"] = ac_w2["cohort_week_start"].dt.strftime("%Y-%m-%d")
                    ac_w2 = ac_w2.rename(columns={
                        "cohort_week_start": "Cohort Week",
                        "cohort_size": "Cohort Size",
                        "w2_pct_1plus": "% 1+",
                        "w2_pct_2plus": "% 2+",
                        "w2_pct_3plus": "% 3+",
                        "w2_pct_4plus": "% 4+",
                    })
                    st.dataframe(
                        ac_w2,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Cohort Week": st.column_config.TextColumn(width="small"),
                            "Cohort Size": st.column_config.NumberColumn(width="small", format="%d"),
                            "% 1+": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 2+": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 3+": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 4+": st.column_config.NumberColumn(width="small", format="%.1f"),
                        }
                    )
                    # Table 3: 2 consecutive weeks (W1 and W2) — % who hit 1+/2+/3+/4+ in both W1 and W2
                    st.markdown("**2 consecutive weeks (W1 & W2) — % of cohort who completed 1+, 2+, 3+, or 4+ in both weeks**")
                    ac_w1w2 = ac[ac["days_old"] >= 14][["cohort_week_start", "cohort_size", "w1_w2_1plus", "w1_w2_2plus", "w1_w2_3plus", "w1_w2_4plus"]].copy()
                    ac_w1w2["cohort_week_start"] = ac_w1w2["cohort_week_start"].dt.strftime("%Y-%m-%d")
                    ac_w1w2 = ac_w1w2.rename(columns={
                        "cohort_week_start": "Cohort Week",
                        "cohort_size": "Cohort Size",
                        "w1_w2_1plus": "% 1+ both",
                        "w1_w2_2plus": "% 2+ both",
                        "w1_w2_3plus": "% 3+ both",
                        "w1_w2_4plus": "% 4+ both",
                    })
                    st.dataframe(
                        ac_w1w2,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Cohort Week": st.column_config.TextColumn(width="small"),
                            "Cohort Size": st.column_config.NumberColumn(width="small", format="%d"),
                            "% 1+ both": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 2+ both": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 3+ both": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% 4+ both": st.column_config.NumberColumn(width="small", format="%.1f"),
                        }
                    )
                else:
                    st.markdown("---")
                    st.markdown("#### 🏃 Activity completions by cohort (consecutive 7-day blocks)")
                    st.info("No activity completion data by cohort yet.")
            except Exception as e:
                st.warning(f"Could not load activity completions by cohort: {e}")

            # Rolling-window retention metrics (at bottom of tab)
            st.markdown("---")
            st.markdown("#### 📊 Rolling-Window Retention Metrics")
            st.caption("Retention measured as % of users with at least one interaction within the specified time window.")
            rolling_retention_query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
),
product_users AS (
  SELECT id, full_name, waid
  FROM users
  WHERE waid NOT IN (SELECT waid FROM internal_waids)
),
user_messages AS (
  SELECT
    u.id  AS user_id,
    u.full_name,
    u.waid,
    (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),
user_first_last AS (
  SELECT
    user_id,
    full_name,
    waid,
    MIN(local_date) AS first_active_date,
    MAX(local_date) AS last_active_date
  FROM user_messages
  GROUP BY user_id, full_name, waid
),
user_with_cohort_week AS (
  SELECT
    ufl.*,
    date_trunc('week', ufl.first_active_date)::date AS cohort_week_start
  FROM user_first_last ufl
),
user_days AS (
  SELECT
    um.user_id,
    uwc.full_name,
    uwc.waid,
    uwc.cohort_week_start,
    um.local_date,
    uwc.first_active_date,
    (um.local_date - uwc.first_active_date) AS cohort_day
  FROM user_messages um
  JOIN user_with_cohort_week uwc ON uwc.user_id = um.user_id
),
cohort_sizes AS (
  SELECT
    cohort_week_start,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM user_with_cohort_week
  GROUP BY cohort_week_start
),
user_activity_by_day AS (
  SELECT
    user_id,
    cohort_week_start,
    MAX(CASE WHEN cohort_day = 1 THEN 1 ELSE 0 END) AS active_day1,
    MAX(CASE WHEN cohort_day <= 2 AND cohort_day >= 1 THEN 1 ELSE 0 END) AS active_day1_or_2,
    MAX(CASE WHEN cohort_day <= 3 AND cohort_day >= 1 THEN 1 ELSE 0 END) AS active_day1_to_3,
    MAX(CASE WHEN cohort_day <= 7 AND cohort_day >= 1 THEN 1 ELSE 0 END) AS active_day1_to_7,
    MAX(CASE WHEN cohort_day <= 14 AND cohort_day >= 1 THEN 1 ELSE 0 END) AS active_day1_to_14
  FROM user_days
  GROUP BY user_id, cohort_week_start
),
rolling_retention AS (
  SELECT
    uwc.cohort_week_start,
    uwc.user_id,
    uwc.first_active_date,
    COALESCE(uabd.active_day1, 0) AS active_within_1d,
    COALESCE(uabd.active_day1_or_2, 0) AS active_within_2d,
    COALESCE(uabd.active_day1_to_3, 0) AS active_within_3d,
    COALESCE(uabd.active_day1_to_7, 0) AS active_within_7d,
    COALESCE(uabd.active_day1_to_14, 0) AS active_within_14d,
    COUNT(DISTINCT ud.local_date) FILTER (WHERE ud.cohort_day <= 6) AS days_active_week1,
    COUNT(DISTINCT ud.local_date) FILTER (WHERE ud.cohort_day <= 13) AS days_active_week2,
    COUNT(*) FILTER (WHERE ud.cohort_day <= 6) AS interactions_week1
  FROM user_with_cohort_week uwc
  LEFT JOIN user_days ud ON ud.user_id = uwc.user_id
  LEFT JOIN user_activity_by_day uabd ON uabd.user_id = uwc.user_id AND uabd.cohort_week_start = uwc.cohort_week_start
  GROUP BY uwc.cohort_week_start, uwc.user_id, uwc.first_active_date, uabd.active_day1, uabd.active_day1_or_2, uabd.active_day1_to_3, uabd.active_day1_to_7, uabd.active_day1_to_14
),
cohort_rolling_metrics AS (
  SELECT
    rr.cohort_week_start,
    cs.cohort_size,
    (CURRENT_DATE - rr.cohort_week_start) AS days_since_cohort_start,
    CASE WHEN (CURRENT_DATE - rr.cohort_week_start) >= 1 THEN ROUND(100.0 * SUM(rr.active_within_1d) / cs.cohort_size, 1) ELSE NULL END AS retention_1d,
    CASE WHEN (CURRENT_DATE - rr.cohort_week_start) >= 2 THEN ROUND(100.0 * SUM(rr.active_within_2d) / cs.cohort_size, 1) ELSE NULL END AS retention_2d,
    CASE WHEN (CURRENT_DATE - rr.cohort_week_start) >= 3 THEN ROUND(100.0 * SUM(rr.active_within_3d) / cs.cohort_size, 1) ELSE NULL END AS retention_3d,
    CASE WHEN (CURRENT_DATE - rr.cohort_week_start) >= 7 THEN ROUND(100.0 * SUM(rr.active_within_7d) / cs.cohort_size, 1) ELSE NULL END AS retention_7d,
    ROUND(AVG(rr.days_active_week2), 1) AS avg_days_active,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rr.days_active_week2) AS median_days_active,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rr.interactions_week1 >= 10) / cs.cohort_size, 1) AS pct_10plus_interactions_week1
  FROM rolling_retention rr
  JOIN cohort_sizes cs ON cs.cohort_week_start = rr.cohort_week_start
  GROUP BY rr.cohort_week_start, cs.cohort_size
)
SELECT
  cohort_week_start,
  cohort_size,
  retention_1d,
  retention_2d,
  retention_3d,
  retention_7d,
  avg_days_active,
  median_days_active,
  pct_10plus_interactions_week1
FROM cohort_rolling_metrics
ORDER BY cohort_week_start DESC
"""
            try:
                rolling_retention_df = run_query(rolling_retention_query)
                if not rolling_retention_df.empty:
                    display_rolling_df = rolling_retention_df.copy()
                    display_rolling_df['cohort_week_start'] = pd.to_datetime(display_rolling_df['cohort_week_start']).dt.strftime('%Y-%m-%d')
                    display_rolling_df = display_rolling_df.rename(columns={
                        'cohort_week_start': 'Cohort Week',
                        'cohort_size': 'Cohort Size',
                        'retention_1d': '1d Retention',
                        'retention_2d': '2d Retention',
                        'retention_3d': '3d Retention',
                        'retention_7d': '7d Retention',
                        'avg_days_active': 'Avg Days Active',
                        'median_days_active': 'Median Days Active',
                        'pct_10plus_interactions_week1': '% with 10+ Interactions (Week 1)'
                    })
                    retention_cols = ['1d Retention', '2d Retention', '3d Retention', '7d Retention']
                    for col in retention_cols:
                        if col in display_rolling_df.columns:
                            display_rolling_df[col] = display_rolling_df[col].apply(lambda x: 'N/A' if pd.isna(x) else f"{x:.1f}%")
                    st.dataframe(
                        display_rolling_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Cohort Week": st.column_config.TextColumn(width="small"),
                            "Cohort Size": st.column_config.NumberColumn(width="small", format="%d"),
                            "1d Retention": st.column_config.TextColumn(width="small"),
                            "2d Retention": st.column_config.TextColumn(width="small"),
                            "3d Retention": st.column_config.TextColumn(width="small"),
                            "7d Retention": st.column_config.TextColumn(width="small"),
                            "Avg Days Active": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "Median Days Active": st.column_config.NumberColumn(width="small", format="%.1f"),
                            "% with 10+ Interactions (Week 1)": st.column_config.NumberColumn(width="small", format="%.1f"),
                        }
                    )
                else:
                    st.info("No rolling-window retention data available yet.")
            except Exception as e:
                st.warning(f"Could not load rolling-window retention metrics: {e}")
            
    except Exception as e:
        st.error(f"Error loading retention data: {e}")
        st.exception(e)


# =============================================================================
# Alerts — LLM eval space
# Reviews the last 24h of messages via OpenRouter and flags user messages that
# show frustration or confusion, using surrounding companion context.
# =============================================================================

# Default daily-run time. Stored as a configurable variable only — there is no
# automated trigger yet (Streamlit Cloud sleeps when idle; a true cron would
# need an external runner). The "Analyse" button runs the eval on demand.
ALERTS_DAILY_RUN_TIME = "09:00"

# Default OpenRouter model. Override with OPENROUTER_MODEL in .env / st.secrets.
ALERTS_DEFAULT_MODEL = "anthropic/claude-sonnet-4.5"

# Internal/system message types that should never be shown to a reviewer.
_EVAL_EXCLUDED_TYPES = ("think", "tool_use", "tool_result", "turn_audit", "weekly_digest")

# Cap on messages per user thread sent to the LLM (keeps token usage bounded).
_EVAL_MAX_MSGS_PER_USER = 20


def get_openrouter_config():
    """Return (api_key, model) from Streamlit secrets or .env. api_key may be None."""
    api_key = None
    model = None
    try:
        if hasattr(st, "secrets"):
            if "OPENROUTER_API_KEY" in st.secrets:
                api_key = st.secrets["OPENROUTER_API_KEY"]
            if "OPENROUTER_MODEL" in st.secrets:
                model = st.secrets["OPENROUTER_MODEL"]
    except Exception:
        pass
    if not api_key:
        api_key = os.getenv("OPENROUTER_API_KEY")
    if not model:
        model = os.getenv("OPENROUTER_MODEL")
    return api_key, (model or ALERTS_DEFAULT_MODEL)


@st.cache_data(ttl=120)
def get_recent_messages_for_eval() -> pd.DataFrame:
    """User + companion messages from the last 24h, excluding internal users and
    internal trace types. Ordered by user then sent_at so threads stay chronological."""
    internal_waids = load_internal_users()
    internal_waids_str = "', '".join(internal_waids) if internal_waids else "''"
    excluded_types_str = "', '".join(_EVAL_EXCLUDED_TYPES)

    query = f"""
WITH internal_waids AS (
  SELECT unnest(ARRAY['{internal_waids_str}'])::varchar AS waid
)
SELECT
  m.user_id,
  m.waid,
  u.full_name,
  u.timezone AS user_timezone,
  m.sender,
  m.type,
  m.message,
  m.sent_at
FROM messages m
LEFT JOIN users u ON u.id = m.user_id
WHERE m.sent_at >= NOW() - interval '24 hours'
  AND m.sender IN ('user', 'companion')
  AND (m.type IS NULL OR m.type NOT IN ('{excluded_types_str}'))
  AND COALESCE(m.waid, u.waid) NOT IN (SELECT waid FROM internal_waids WHERE waid <> '')
ORDER BY COALESCE(m.user_id::text, m.waid), m.sent_at
"""
    return run_query(query)


def _build_eval_threads(df: pd.DataFrame) -> list:
    """Group rows into per-user chronological transcripts.

    Returns a list of dicts: {key, waid, full_name, user_timezone, messages:[...]}
    where each message has {idx, sender, text, sent_at}. Only the last
    _EVAL_MAX_MSGS_PER_USER messages per user are kept to bound token usage.
    """
    if df is None or df.empty:
        return []

    threads = []
    df = df.copy()
    # Stable grouping key: prefer user_id, fall back to waid.
    df["_group_key"] = df.apply(
        lambda r: str(r["user_id"]) if pd.notna(r.get("user_id")) else f"waid:{r.get('waid')}",
        axis=1,
    )

    for key, group in df.groupby("_group_key", sort=False):
        group = group.sort_values("sent_at")
        if len(group) > _EVAL_MAX_MSGS_PER_USER:
            group = group.tail(_EVAL_MAX_MSGS_PER_USER)
        first = group.iloc[0]
        messages = []
        for i, (_, row) in enumerate(group.iterrows()):
            text = _extract_message_text_snippet(row["message"], max_len=600)
            if not text:
                continue
            messages.append(
                {
                    "idx": i,
                    "sender": row["sender"],
                    "text": text,
                    "sent_at": row["sent_at"],
                }
            )
        # Skip threads with no user messages — nothing to flag.
        if not any(m["sender"] == "user" for m in messages):
            continue
        threads.append(
            {
                "key": key,
                "waid": first.get("waid"),
                "full_name": first.get("full_name"),
                "user_timezone": first.get("user_timezone"),
                "messages": messages,
            }
        )
    return threads


_EVAL_SYSTEM_PROMPT = (
    "You review a WhatsApp conversation between a user and an AI wellness/fitness "
    "coaching companion. The conversation is mostly in Brazilian Portuguese.\n\n"
    "Your job: flag USER messages (sender = 'user') that a human reviewer should look "
    "at. Use the surrounding companion messages as context to judge intent.\n\n"
    "Flag a message when it shows any of these issue types:\n"
    "- frustration: annoyance, anger, complaints (e.g. bot repeats itself, broken flow)\n"
    "- confusion: the user does not understand what to do or what the bot meant\n"
    "- dissatisfaction: unhappy with the product, coaching, or results\n"
    "- churn_risk: wants to quit, cancel, stop, or says the app isn't worth it\n"
    "- unanswered_question: a direct user question the companion ignored or failed to answer\n"
    "- bug_report: the user reports something broken or not working\n"
    "- sensitive: health/safety concern, distress, or anything needing human attention\n\n"
    "Do NOT flag neutral, positive, or simply brief messages. Only flag genuine issues.\n\n"
    "Return ONLY valid JSON with this exact shape:\n"
    '{\"flags\": [{\"idx\": <int index of the user message>, '
    '\"issue_type\": \"frustration\" | \"confusion\" | \"dissatisfaction\" | '
    '\"churn_risk\" | \"unanswered_question\" | \"bug_report\" | \"sensitive\", '
    '\"severity\": \"low\" | \"med\" | \"high\", '
    '\"explanation\": \"<one short sentence, in English>\"}]}\n'
    "If nothing should be flagged, return {\"flags\": []}."
)


def _parse_llm_json(content):
    """Parse JSON from an LLM response that may be wrapped in markdown code fences.

    Some providers (e.g. Anthropic via OpenRouter) ignore response_format and wrap
    output in ```json ... ``` fences, so a plain json.loads would fail.
    """
    if not content:
        return {}
    s = str(content).strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9]*\s*", "", s)
        s = re.sub(r"\s*```$", "", s).strip()
    try:
        return json.loads(s)
    except Exception:
        pass
    # Fallback: grab the first {...} block anywhere in the text.
    match = re.search(r"\{.*\}", s, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            return {}
    return {}


def _eval_one_thread(client, model, thread):
    """Call the LLM for a single thread; return list of flag dicts (may be empty)."""
    lines = []
    for m in thread["messages"]:
        role = "USER" if m["sender"] == "user" else "COMPANION"
        lines.append(f"[{m['idx']}] {role}: {m['text']}")
    transcript = "\n".join(lines)

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _EVAL_SYSTEM_PROMPT},
            {"role": "user", "content": f"Conversation transcript:\n\n{transcript}"},
        ],
        response_format={"type": "json_object"},
        temperature=0,
        max_tokens=1200,
    )
    content = resp.choices[0].message.content or "{}"
    parsed = _parse_llm_json(content)
    raw_flags = parsed.get("flags", []) if isinstance(parsed, dict) else []
    msgs_by_idx = {m["idx"]: m for m in thread["messages"]}
    flags = []
    for f in raw_flags:
        try:
            idx = int(f.get("idx"))
        except (TypeError, ValueError):
            continue
        msg = msgs_by_idx.get(idx)
        # Only accept flags that point at an actual user message.
        if not msg or msg["sender"] != "user":
            continue
        flags.append(
            {
                "idx": idx,
                "issue_type": str(f.get("issue_type", "")).strip() or "frustration",
                "severity": str(f.get("severity", "")).strip().lower() or "med",
                "explanation": str(f.get("explanation", "")).strip(),
            }
        )
    return flags


def run_message_eval(threads, api_key, model, progress_callback=None):
    """Run the LLM eval over all threads. Returns (flagged_users, errors).

    flagged_users: list of per-user dicts (only users with >=1 flag), each with
      waid, full_name, user_timezone, messages (the analysed thread), flags, and
      max_severity. errors: list of (user_label, error_str).
    """
    from openai import OpenAI  # local import so a missing dep doesn't break import

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
    flagged_users = []
    errors = []
    total = len(threads)
    for i, thread in enumerate(threads):
        try:
            flags = _eval_one_thread(client, model, thread)
            if flags:
                max_sev = min(_SEVERITY_ORDER.get(f["severity"], 1) for f in flags)
                flagged_users.append(
                    {
                        "waid": thread["waid"],
                        "full_name": thread["full_name"],
                        "user_timezone": thread["user_timezone"],
                        "messages": thread["messages"],
                        "flags": flags,
                        "max_severity_rank": max_sev,
                    }
                )
        except Exception as e:  # keep going on per-user failures
            label = thread.get("full_name") or thread.get("waid") or thread.get("key")
            errors.append((str(label), str(e)))
        if progress_callback:
            progress_callback((i + 1) / total if total else 1.0)
    return flagged_users, errors


_SEVERITY_ORDER = {"high": 0, "med": 1, "low": 2}
_SEVERITY_BADGE = {"high": "🔴 high", "med": "🟠 med", "low": "🟡 low"}


# Tab 4: Alerts — LLM eval space
if selected_section == "🔔 Alerts":
    st.markdown("### 🔔 Alerts — Message Eval")

    st.caption(
        "An LLM reviews the last 24 hours of conversations and flags **users** whose "
        "messages show issues (frustration, confusion, churn risk, unanswered questions, "
        "bugs, etc.), using the surrounding companion context. Open a user to inspect the "
        "conversation."
    )

    api_key, eval_model = get_openrouter_config()

    if api_key:
        st.markdown(f"**LLM:** `{eval_model}` · OpenRouter ✅")
    else:
        st.warning(
            "No OpenRouter API key found. Add `OPENROUTER_API_KEY` to your `.env` "
            "(or Streamlit secrets) to enable analysis. Optionally set "
            "`OPENROUTER_MODEL` to override the default."
        )

    st.markdown("---")

    eval_state = st.session_state.get("alerts_eval")

    # Last-run line + Analyse button
    run_col, info_col = st.columns([1, 3])
    with run_col:
        analyse_clicked = st.button(
            "🔍 Analyse last 24h",
            type="primary",
            disabled=not api_key,
            use_container_width=True,
        )
    with info_col:
        if eval_state and eval_state.get("last_run"):
            st.markdown(
                f"**Last run:** {eval_state['last_run'].strftime('%Y-%m-%d %H:%M:%S %Z')} "
                f"· model `{eval_state.get('model', '?')}` · "
                f"{eval_state.get('n_messages', 0)} messages from "
                f"{eval_state.get('n_users', 0)} users"
            )
        else:
            st.markdown("**Last run:** _never run yet_")

    if analyse_clicked:
        with st.spinner("Loading last 24h of messages…"):
            try:
                msgs_df = get_recent_messages_for_eval()
            except Exception as e:
                msgs_df = pd.DataFrame()
                st.error(f"Could not load messages: {e}")
            threads = _build_eval_threads(msgs_df)

        if not threads:
            st.session_state["alerts_eval"] = {
                "last_run": datetime.now(pytz.timezone("America/Sao_Paulo")),
                "model": eval_model,
                "n_users": 0,
                "n_messages": 0,
                "flagged_users": [],
                "errors": [],
            }
            st.info("No user conversations found in the last 24 hours.")
        else:
            n_messages = int(sum(len(t["messages"]) for t in threads))
            progress = st.progress(0.0, text=f"Reviewing {len(threads)} conversations…")

            def _update(frac):
                progress.progress(min(frac, 1.0), text=f"Reviewing {len(threads)} conversations…")

            flagged_users, errors = run_message_eval(threads, api_key, eval_model, progress_callback=_update)
            progress.empty()

            st.session_state["alerts_eval"] = {
                "last_run": datetime.now(pytz.timezone("America/Sao_Paulo")),
                "model": eval_model,
                "n_users": len(threads),
                "n_messages": n_messages,
                "flagged_users": flagged_users,
                "errors": errors,
            }
        eval_state = st.session_state.get("alerts_eval")
        st.rerun()

    # Render results from session state
    if eval_state is None:
        st.info("Click **Analyse last 24h** to review messages with the LLM.")
    else:
        flagged_users = eval_state.get("flagged_users", [])
        errors = eval_state.get("errors", [])

        if errors:
            with st.expander(f"⚠️ {len(errors)} conversation(s) failed during analysis"):
                for label, err in errors:
                    st.caption(f"**{label}**: {err}")

        if not flagged_users:
            st.success("✅ No issues flagged in the last 24 hours.")
        else:
            total_issues = sum(len(u.get("flags", [])) for u in flagged_users)
            st.markdown(f"#### 🚩 {len(flagged_users)} user(s) flagged ({total_issues} issue(s))")
            st.caption("Each user appears once. Open a row to see the conversation and the flagged messages.")

            sorted_users = sorted(
                flagged_users,
                key=lambda u: (u.get("max_severity_rank", 1), str(u.get("full_name") or "")),
            )

            for u in sorted_users:
                display_name = format_display_name(u.get("full_name"), u.get("waid"))
                flags = u.get("flags", [])
                top_badge = _SEVERITY_BADGE.get(
                    next((k for k, v in _SEVERITY_ORDER.items() if v == u.get("max_severity_rank", 1)), "med"),
                    "med",
                )
                issue_types = sorted({f.get("issue_type", "") for f in flags})
                header = f"{top_badge} · {display_name} — {', '.join(issue_types)} ({len(flags)})"

                with st.expander(header):
                    # Issue summary (one line per flag)
                    st.markdown("**Flagged issues**")
                    for f in sorted(flags, key=lambda f: _SEVERITY_ORDER.get(f.get("severity", "med"), 1)):
                        badge = _SEVERITY_BADGE.get(f.get("severity", "med"), f.get("severity", "med"))
                        expl = f.get("explanation", "")
                        st.markdown(f"- {badge} · **{f.get('issue_type', '')}** — {expl}")

                    # Full conversation as a table, with flagged messages marked
                    flag_by_idx = {f["idx"]: f for f in flags}
                    rows = []
                    for m in u.get("messages", []):
                        fl = flag_by_idx.get(m["idx"])
                        rows.append(
                            {
                                "time": _format_ts_local(m.get("sent_at"), u.get("user_timezone")),
                                "sender": "🧑 User" if m["sender"] == "user" else "🤖 Companion",
                                "message": m.get("text", ""),
                                "flag": (
                                    f"{_SEVERITY_BADGE.get(fl.get('severity', 'med'), '')} {fl.get('issue_type', '')}"
                                    if fl
                                    else ""
                                ),
                            }
                        )
                    convo_df = pd.DataFrame(rows, columns=["time", "sender", "message", "flag"])
                    st.markdown("**Conversation**")
                    st.dataframe(convo_df, use_container_width=True, hide_index=True)
                    st.caption(f"WAID: {u.get('waid')}")


# Tab 5: Recovery Ladder
if selected_section == "🪜 Recovery Ladder":
    st.markdown("### 🪜 Recovery Ladder")

    # Section 0: current AFK snapshot — distribution by days AFK (FETCH_RECOVERY_TARGETS logic)
    st.markdown("#### 📊 AFK Users by Days Silent (current snapshot)")
    st.caption("Active users with no inbound message in 24h+. Days AFK = days since last user message (waid-based). Internal users excluded.")
    try:
        afk_dist_df = get_afk_users_distribution(exclude_internal=True)
    except Exception as e:
        afk_dist_df = pd.DataFrame()
        st.warning(f"Could not load AFK distribution: {e}")

    if afk_dist_df.empty:
        st.info("No AFK users right now.")
    else:
        total_afk = int(afk_dist_df["users"].sum())
        low_active_total = int(afk_dist_df["low_active_users"].sum())
        high_active_total = int(afk_dist_df["high_active_users"].sum())
        k_afk1, k_afk2, k_afk3 = st.columns(3)
        k_afk1.metric("Total AFK users", total_afk)
        k_afk2.metric("≤3 active days", low_active_total)
        k_afk3.metric(">3 active days", high_active_total)

        try:
            import altair as alt

            chart_src = afk_dist_df.copy()
            chart_src["days_afk_num"] = chart_src["days_afk"].astype(int)
            chart_src["days_afk_label"] = chart_src["days_afk_num"].astype(str)
            bar_chart = (
                alt.Chart(chart_src)
                .mark_bar(color="#00d4aa")
                .encode(
                    x=alt.X("days_afk_label:N", sort=alt.EncodingSortField(field="days_afk_num", order="ascending"), title="Days AFK"),
                    y=alt.Y("users:Q", title="Users"),
                    tooltip=[
                        alt.Tooltip("days_afk_label:N", title="Days AFK"),
                        alt.Tooltip("users:Q", title="Users"),
                        alt.Tooltip("low_active_users:Q", title="≤3 active days"),
                        alt.Tooltip("high_active_users:Q", title=">3 active days"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(bar_chart, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not render AFK distribution chart: {e}")

        display_df = afk_dist_df.copy()
        display_df.columns = ["Days AFK", "Users", "≤3 active days", ">3 active days"]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Section 1: Recovery Alert Effectiveness (Stack B) ──────────────────────
    st.markdown("#### 📬 Recovery Alert Effectiveness — Stack B")
    st.caption(
        "Stack B only. A 'recovery alert' is any send whose `ladder_step` contains 'recovery' or equals "
        "`onboarding_come_back`. 'Came back' = user sent at least one message after the alert within the same window. "
        "Last 7d vs previous 7d."
    )

    try:
        rae_summary_df, rae_step_df = get_recovery_alert_effectiveness_7d(exclude_internal=True)
    except Exception as _e:
        rae_summary_df, rae_step_df = pd.DataFrame(), pd.DataFrame()
        st.warning(f"Could not load recovery alert effectiveness: {_e}")

    if not rae_summary_df.empty:
        _rae_last = rae_summary_df[rae_summary_df["window_name"] == "last_7d"].iloc[0] if "last_7d" in rae_summary_df["window_name"].values else None
        _rae_prev = rae_summary_df[rae_summary_df["window_name"] == "prev_7d"].iloc[0] if "prev_7d" in rae_summary_df["window_name"].values else None

        _rae_c1, _rae_c2, _rae_c3, _rae_c4, _rae_c5, _rae_c6 = st.columns(6)
        if _rae_last is not None:
            _rae_c1.metric(
                "Users reached",
                int(_rae_last["users_reached"]),
                _metric_delta(int(_rae_last["users_reached"]), int(_rae_prev["users_reached"]) if _rae_prev is not None else None),
            )
            _rae_c2.metric(
                "Came back",
                int(_rae_last["came_back_count"]),
                _metric_delta(int(_rae_last["came_back_count"]), int(_rae_prev["came_back_count"]) if _rae_prev is not None else None),
            )
            _rae_c3.metric(
                "Recovery rate",
                f"{_rae_last['recovery_rate_pct']}%",
                _metric_delta(float(_rae_last["recovery_rate_pct"]), float(_rae_prev["recovery_rate_pct"]) if _rae_prev is not None else None, "pp"),
            )
            _rae_c4.metric(
                "Avg active days (all)",
                _rae_last["avg_active_days_all"],
                _metric_delta(float(_rae_last["avg_active_days_all"]) if _rae_last["avg_active_days_all"] is not None else None, float(_rae_prev["avg_active_days_all"]) if _rae_prev is not None and _rae_prev["avg_active_days_all"] is not None else None),
            )
            _rae_c5.metric(
                "Avg active days (recovered)",
                _rae_last["avg_active_days_recovered"] if _rae_last["avg_active_days_recovered"] is not None else "—",
                _metric_delta(float(_rae_last["avg_active_days_recovered"]) if _rae_last["avg_active_days_recovered"] is not None else None, float(_rae_prev["avg_active_days_recovered"]) if _rae_prev is not None and _rae_prev["avg_active_days_recovered"] is not None else None),
            )
            _rae_c6.metric(
                "Avg active days (not recovered)",
                _rae_last["avg_active_days_not_recovered"] if _rae_last["avg_active_days_not_recovered"] is not None else "—",
                _metric_delta(float(_rae_last["avg_active_days_not_recovered"]) if _rae_last["avg_active_days_not_recovered"] is not None else None, float(_rae_prev["avg_active_days_not_recovered"]) if _rae_prev is not None and _rae_prev["avg_active_days_not_recovered"] is not None else None),
                delta_color="inverse",
            )
        else:
            st.info("No recovery alerts in the last 7 days.")

    # Per-step breakdown table (last 7d)
    if not rae_step_df.empty:
        st.markdown("**Last 7d — by ladder step**")
        _rae_display = rae_step_df.rename(columns={
            "ladder_step": "Ladder step",
            "template_name": "Template",
            "users_reached": "Users reached",
            "came_back_count": "Came back",
            "recovery_rate_pct": "Recovery rate (%)",
            "avg_active_days": "Avg active days",
        })
        st.dataframe(_rae_display, use_container_width=True, hide_index=True)

    # Weekly recovery rate chart (Stack B, since Jun 1)
    st.markdown("**Recovery rate over time (Stack B · weekly · from Jun 1)**")
    try:
        rae_weekly_df = get_recovery_rate_weekly_since("2026-06-01", exclude_internal=True)
    except Exception as _e:
        rae_weekly_df = pd.DataFrame()
        st.warning(f"Could not load weekly recovery rate: {_e}")

    if not rae_weekly_df.empty:
        try:
            import altair as alt

            _rae_wk = rae_weekly_df.copy()
            _rae_wk["week_start"] = pd.to_datetime(_rae_wk["week_start"], errors="coerce")
            _rae_wk["week_label"] = _rae_wk["week_start"].dt.strftime("w/o %d %b")
            _rae_wk["recovery_rate_pct"] = pd.to_numeric(_rae_wk["recovery_rate_pct"], errors="coerce").fillna(0)
            _rae_wk["users_reached"] = pd.to_numeric(_rae_wk["users_reached"], errors="coerce").fillna(0)
            _rae_wk["came_back_count"] = pd.to_numeric(_rae_wk["came_back_count"], errors="coerce").fillna(0)
            _rae_wk["avg_active_days_all"] = pd.to_numeric(_rae_wk["avg_active_days_all"], errors="coerce")

            _rae_base = alt.Chart(_rae_wk).encode(
                x=alt.X("week_label:N", sort=list(_rae_wk["week_label"]), title="Week"),
            )
            _rae_rate_line = _rae_base.mark_line(point=True, color="#00d4aa").encode(
                y=alt.Y("recovery_rate_pct:Q", title="Recovery rate (%)", axis=alt.Axis(titleColor="#00d4aa")),
                tooltip=[
                    alt.Tooltip("week_label:N", title="Week"),
                    alt.Tooltip("recovery_rate_pct:Q", title="Recovery rate (%)", format=".1f"),
                    alt.Tooltip("users_reached:Q", title="Users reached"),
                    alt.Tooltip("came_back_count:Q", title="Came back"),
                    alt.Tooltip("avg_active_days_all:Q", title="Avg active days", format=".1f"),
                ],
            )
            _rae_bar = _rae_base.mark_bar(opacity=0.3, color="#4a9eff").encode(
                y=alt.Y("users_reached:Q", title="Users reached", axis=alt.Axis(titleColor="#4a9eff")),
            )
            _rae_chart = alt.layer(_rae_bar, _rae_rate_line).resolve_scale(y="independent").properties(height=320)
            st.altair_chart(_rae_chart, use_container_width=True)
            st.caption("Bars = users reached (left axis) · Line = recovery rate % (right axis) · Stack B · external users only")
        except Exception as _e:
            st.warning(f"Could not render weekly recovery rate chart: {_e}")
            st.dataframe(rae_weekly_df, use_container_width=True, hide_index=True)
    else:
        st.info("No data for weekly recovery rate chart.")

    st.markdown("---")

    try:
        timeline_df, rung_df, active_users = get_recovery_weekly_active_user_reach(weeks_back=6, exclude_internal=True)
    except Exception as e:
        timeline_df = pd.DataFrame()
        rung_df = pd.DataFrame()
        active_users = 0
        st.warning(f"Could not load weekly recovery-ladder reach data: {e}")

    # Section 1: weekly timeline — % of active users receiving any recovery-ladder template
    st.markdown("#### 📈 % Active Users Receiving a Template (weekly)")
    if timeline_df.empty:
        st.info("No recovery-ladder sends in the last 6 weeks.")
    else:
        try:
            import altair as alt

            st.caption(f"Active users (denominator): **{active_users}**")
            chart_df = timeline_df.copy()
            chart_df["week_start"] = pd.to_datetime(chart_df["week_start"], errors="coerce")
            chart_df["week_label"] = chart_df["week_start"].dt.strftime("Week of %d-%m-%Y")
            chart_df["pct_active_users"] = pd.to_numeric(chart_df["pct_active_users"], errors="coerce").fillna(0)

            timeline_chart = (
                alt.Chart(chart_df)
                .mark_line(point=True, color="#00d4aa")
                .encode(
                    x=alt.X("week_label:N", sort=list(chart_df["week_label"]), title="Week"),
                    y=alt.Y("pct_active_users:Q", title="% active users receiving a template"),
                    tooltip=[
                        alt.Tooltip("week_label:N", title="Week"),
                        alt.Tooltip("pct_active_users:Q", title="% active users", format=".1f"),
                        alt.Tooltip("users_receiving:Q", title="Users receiving"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(timeline_chart, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not render weekly template reach timeline: {e}")

    st.markdown("---")

    # Section 2: 12w+ view — ladder performance and reply timeline
    default_start = (datetime.now() - timedelta(weeks=12)).strftime("%Y-%m-%d")
    try:
        recovery_events = get_recovery_ladder_events(default_start)
    except Exception as e:
        recovery_events = pd.DataFrame()
        st.error(f"Could not load recovery ladder data: {e}")

    if recovery_events.empty:
        st.info("No recovery ladder sends found for the date range.")
    else:
        df = recovery_events.copy()
        df["week_start_sp"] = pd.to_datetime(df["week_start_sp"], errors="coerce")
        df["replied_before_next_template"] = df["replied_before_next_template"].fillna(False).astype(int)
        df["activity_12h"] = df["activity_12h"].fillna(False).astype(int)
        df["activity_24h"] = df["activity_24h"].fillna(False).astype(int)
        df["response_minutes"] = pd.to_numeric(df["response_minutes"], errors="coerce")
        df["template_sent_at_utc"] = pd.to_datetime(df["template_sent_at_utc"], utc=True, errors="coerce")
        df["template_sent_at_sp"] = pd.to_datetime(df["template_sent_at_sp"], errors="coerce")
        df = df[df["week_start_sp"].notna()].copy()

        def _safe_pct(numerator, denominator):
            return round(100.0 * numerator / denominator, 1) if denominator else 0.0

        weekly = (
            df.groupby("week_start_sp")
            .agg(
                templates_sent=("user_id", "size"),
                users_targeted=("user_id", "nunique"),
                replied_templates=("replied_before_next_template", "sum"),
                activity_12h_templates=("activity_12h", "sum"),
                activity_24h_templates=("activity_24h", "sum"),
                avg_response_min=("response_minutes", "mean"),
                median_response_min=("response_minutes", "median"),
            )
            .reset_index()
            .sort_values("week_start_sp", ascending=False)
        )
        weekly["reply_rate_pct"] = (100.0 * weekly["replied_templates"] / weekly["templates_sent"]).round(1)
        weekly["activity_12h_rate_pct"] = (100.0 * weekly["activity_12h_templates"] / weekly["templates_sent"]).round(1)
        weekly["activity_24h_rate_pct"] = (100.0 * weekly["activity_24h_templates"] / weekly["templates_sent"]).round(1)
        weekly["avg_response_min"] = weekly["avg_response_min"].round(1)
        weekly["median_response_min"] = weekly["median_response_min"].round(1)
        week_order = sorted(weekly["week_start_sp"].dropna().unique(), reverse=True)
        current_week = week_order[0] if len(week_order) >= 1 else None
        prev_week = week_order[1] if len(week_order) >= 2 else None
        prev2_week = week_order[2] if len(week_order) >= 3 else None

        def _week_label(dt_obj):
            return pd.to_datetime(dt_obj).strftime("%Y-%m-%d") if dt_obj is not None else "—"

        st.markdown(
            f"#### 📊 Week Snapshot — Current: `{_week_label(current_week)}`"
        )

        cw = weekly[weekly["week_start_sp"] == current_week].iloc[0] if current_week is not None and not weekly[weekly["week_start_sp"] == current_week].empty else None
        pw = weekly[weekly["week_start_sp"] == prev_week].iloc[0] if prev_week is not None and not weekly[weekly["week_start_sp"] == prev_week].empty else None

        k1, k2, k3, k4, k5, k6 = st.columns(6)
        if cw is not None:
            k1.metric(
                "Templates sent",
                int(cw["templates_sent"]),
                _metric_delta(int(cw["templates_sent"]), int(pw["templates_sent"]) if pw is not None else None),
            )
            k2.metric(
                "Reply rate (% sends)",
                f"{cw['reply_rate_pct']}%",
                _metric_delta(float(cw["reply_rate_pct"]), float(pw["reply_rate_pct"]) if pw is not None else None, "pp"),
            )
            k3.metric(
                "Activity 12h (% sends)",
                f"{cw['activity_12h_rate_pct']}%",
                _metric_delta(float(cw["activity_12h_rate_pct"]), float(pw["activity_12h_rate_pct"]) if pw is not None else None, "pp"),
            )
            k4.metric(
                "Activity 24h (% sends)",
                f"{cw['activity_24h_rate_pct']}%",
                _metric_delta(float(cw["activity_24h_rate_pct"]), float(pw["activity_24h_rate_pct"]) if pw is not None else None, "pp"),
            )
            no_reply_curr = round(100.0 - float(cw["reply_rate_pct"]), 1)
            no_reply_prev = (round(100.0 - float(pw["reply_rate_pct"]), 1) if pw is not None else None)
            k5.metric(
                "No-reply rate (% sends)",
                f"{no_reply_curr}%",
                _metric_delta(no_reply_curr, no_reply_prev, "pp"),
                delta_color="inverse",
            )
            median_curr = float(cw["median_response_min"]) if pd.notna(cw["median_response_min"]) else None
            median_prev = float(pw["median_response_min"]) if pw is not None and pd.notna(pw["median_response_min"]) else None
            k6.metric(
                "Median reply time (min)",
                f"{cw['median_response_min'] if pd.notna(cw['median_response_min']) else '—'}",
                _metric_delta(median_curr, median_prev, "m"),
                delta_color="inverse",
            )
        else:
            k1.metric("Templates sent", "—")
            k2.metric("Reply rate (% sends)", "—")
            k3.metric("Activity 12h (% sends)", "—")
            k4.metric("Activity 24h (% sends)", "—")
            k5.metric("No-reply rate (% sends)", "—")
            k6.metric("Median reply time (min)", "—")

    st.markdown("---")

    # Section 3: rung-by-week matrix — % of active users receiving each ladder rung
    st.markdown("#### 🪜 Recovery Ladder Rungs by Week (% active users)")
    week_starts = (
        pd.to_datetime(timeline_df["week_start"], errors="coerce").dropna().sort_values().tolist()
        if not timeline_df.empty
        else []
    )
    week_starts = week_starts[-3:]
    if not week_starts:
        st.info("No weekly rung data available.")
    else:
        week_labels = [pd.to_datetime(w).strftime("Week of %d-%m-%Y") for w in week_starts]
        rung_lookup = {}
        if not rung_df.empty:
            for _, row in rung_df.iterrows():
                wk = pd.to_datetime(row["week_start"], errors="coerce")
                if pd.isna(wk):
                    continue
                rung_lookup[(row["ladder_step"], wk.date())] = row["pct_active_users"]

        matrix_rows = []
        for ladder_step, rung_label in RECOVERY_LADDER_TABLE_RUNGS:
            row_out = {"Rung": rung_label}
            for wk, wlabel in zip(week_starts, week_labels):
                wk_date = pd.to_datetime(wk).date()
                pct = rung_lookup.get((ladder_step, wk_date))
                row_out[wlabel] = f"{pct:.1f}%" if pct is not None and pd.notna(pct) else "0.0%"
            matrix_rows.append(row_out)

        st.dataframe(pd.DataFrame(matrix_rows), use_container_width=True, hide_index=True)

# Footer
st.markdown("---")
st.caption(f"LETZ Data Dashboard v1.1 • Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

