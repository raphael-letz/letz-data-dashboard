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
                    port=st.secrets.get("DB_PORT", "5432")
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
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL query and return results as DataFrame."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        # Check if connection is still alive
        conn.rollback()  # Reset any failed transaction
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Query failed: {e}")
        # Clear the cached connection if it failed
        st.cache_resource.clear()
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
        '555397038122', '5511970544995', '6593366209', '555199885544'
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


# Alerts badge: count users with missed morning/evening today (message delivery)
try:
    _delivery_df = get_message_delivery_detail()
    _today_missed = _delivery_df[
        (_delivery_df["period"] == "today")
        & (_delivery_df["missed_morning"] | _delivery_df["missed_evening"])
    ]
    _alert_count = len(_today_missed)
except Exception:
    _alert_count = 0
_alert_label = "🔔 Alerts" + (f" ({_alert_count})" if _alert_count else "")

# Main content tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Quick Insights",
    "🔍 User Deep Dive",
    "📈 User Retention",
    _alert_label,
])


# Tab 1: Quick Insights
with tab1:
    # Internal users filter toggle (subtle, compact design)
    filter_col1, filter_col2 = st.columns([1, 11])
    with filter_col1:
        exclude_internal = st.toggle(
            "🔒",
            value=True,
            help="Exclude internal users (coaches, test accounts) from all metrics. Internal users are loaded from analysis/.context/internal-users.json",
            label_visibility="collapsed"
        )
    with filter_col2:
        internal_users_list = load_internal_users()
        if exclude_internal:
            if internal_users_list:
                st.caption(f"Excluding {len(internal_users_list)} internal users")
            else:
                st.caption("Filter active (no internal users file found)")
        else:
            st.caption("Showing all users (including internal)")
    
    st.markdown("---")
    
    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    
    # Try to get quick stats (all deduplicated by waid)
    try:
        # User count (unique waids)
        internal_filter = get_internal_users_filter_sql(exclude_internal)
        user_count_query = f"SELECT COUNT(DISTINCT waid) as count FROM users {internal_filter}" if internal_filter else "SELECT COUNT(DISTINCT waid) as count FROM users"
        user_count_df = run_query(user_count_query)
        total_users_count = user_count_df['count'].iloc[0] if not user_count_df.empty else 0
        if user_count_df is not None and not user_count_df.empty:
            col1.metric("Total Users", total_users_count)
        else:
            col1.metric("Total Users", "—")
    except:
        col1.metric("Total Users", "—")
    
    try:
        # Today's users (unique waids)
        internal_filter = get_internal_users_filter_sql(exclude_internal)
        where_clause = "WHERE created_at >= CURRENT_DATE" if not internal_filter else f"{internal_filter} AND created_at >= CURRENT_DATE"
        today_users = run_query(f"""
            SELECT COUNT(DISTINCT waid) as count FROM users 
            {where_clause}
        """)
        if not today_users.empty:
            col2.metric("New Today", today_users['count'].iloc[0])
        else:
            col2.metric("New Today", "—")
    except:
        col2.metric("New Today", "—")
    
    try:
        # New users in rolling past 7 days (unique waids)
        internal_filter = get_internal_users_filter_sql(exclude_internal)
        where_clause = "WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'" if not internal_filter else f"{internal_filter} AND created_at >= CURRENT_DATE - INTERVAL '7 days'"
        week_users = run_query(f"""
            SELECT COUNT(DISTINCT waid) as count FROM users 
            {where_clause}
        """)
        if not week_users.empty:
            col3.metric("New (past 7 days)", week_users['count'].iloc[0])
        else:
            col3.metric("New (past 7 days)", "—")
    except:
        col3.metric("New (past 7 days)", "—")
    
    try:
        # Active today (unique waids who SENT a message today)
        internal_filter = get_internal_users_filter_join_sql(exclude_internal, "u")
        active_today = run_query(f"""
            SELECT COUNT(DISTINCT u.waid) as count 
            FROM messages m
            JOIN users u ON m.user_id = u.id
            WHERE m.sent_at >= CURRENT_DATE 
              AND m.user_id IS NOT NULL
              AND m.sender = 'user'
              {internal_filter}
        """)
        if not active_today.empty:
            col4.metric("Active Today", active_today['count'].iloc[0])
        else:
            col4.metric("Active Today", "—")
    except:
        col4.metric("Active Today", "—")
    
    try:
        # Users outside the 24h window based on last user message (sender='user')
        if 'total_users_count' not in locals():
            internal_filter = get_internal_users_filter_sql(exclude_internal)
            user_count_query = f"SELECT COUNT(DISTINCT waid) as count FROM users {internal_filter}" if internal_filter else "SELECT COUNT(DISTINCT waid) as count FROM users"
            user_count_df = run_query(user_count_query)
            total_users_count = user_count_df['count'].iloc[0] if not user_count_df.empty else 0
        
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        active_24h_df = run_query(f"""
            SELECT COUNT(DISTINCT u.waid) as count
            FROM messages m
            JOIN users u ON m.user_id = u.id
            WHERE m.sender = 'user'
              AND m.sent_at >= NOW() - INTERVAL '24 hours'
              {internal_filter_join}
        """)
        active_24h = active_24h_df['count'].iloc[0] if not active_24h_df.empty else 0
        outside = max(total_users_count - active_24h, 0)
        outside_pct = round(100 * outside / total_users_count, 1) if total_users_count > 0 else 0
        col5.metric("% Outside 24h", f"{outside_pct}%", f"{outside} users")
    except:
        col5.metric("% Outside 24h", "—")
    
    try:
        # Templates Sent 24h - filter by internal users
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        if exclude_internal and internal_filter_join:
            templates_24h_df = run_query(f"""
                SELECT COUNT(*) as count
                FROM recovery_logs r
                JOIN users u ON r.user_id = u.id
                WHERE r.sent_at >= NOW() - INTERVAL '24 hours'
                  {internal_filter_join}
            """)
        else:
            templates_24h_df = run_query("""
                SELECT COUNT(*) as count
                FROM recovery_logs
                WHERE sent_at >= NOW() - INTERVAL '24 hours'
            """)
        templates_24h = templates_24h_df['count'].iloc[0] if not templates_24h_df.empty else 0
        col6.metric("Templates Sent 24h", templates_24h)
    except:
        col6.metric("Templates Sent 24h", "—")
    
    try:
        # Users with Activities Scheduled Today - filter by internal users
        today_day = datetime.utcnow().strftime("%A")
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        if exclude_internal and internal_filter_join:
            activity_today_df = run_query(f"""
                SELECT COUNT(DISTINCT ua.user_id) as count
                FROM user_activities ua
                JOIN users u ON ua.user_id = u.id
                WHERE ua.days::jsonb ? '{today_day}'
                  {internal_filter_join}
            """)
        else:
            activity_today_df = run_query(f"""
                SELECT COUNT(DISTINCT user_id) as count
                FROM user_activities
                WHERE days::jsonb ? '{today_day}'
            """)
        activity_today = activity_today_df['count'].iloc[0] if not activity_today_df.empty else 0
        col7.metric("Users with Activities Scheduled Today", activity_today, today_day)
    except:
        col7.metric("Users with Activities Scheduled Today", "—")
    
    # Expandable simple lists for key metrics
    try:
        internal_filter = get_internal_users_filter_sql(exclude_internal)
        where_clause = "WHERE created_at >= CURRENT_DATE" if not internal_filter else f"{internal_filter} AND created_at >= CURRENT_DATE"
        new_today_list = run_query(f"""
            SELECT COALESCE(full_name, 'Unknown') AS name
            FROM (
                SELECT DISTINCT ON (waid) full_name, created_at
                FROM users
                {where_clause}
                ORDER BY waid, created_at DESC
            ) unique_users
            ORDER BY created_at DESC
            LIMIT 200
        """)
        with st.expander("New Today - names"):
            if new_today_list.empty:
                st.caption("No users")
            else:
                for n in new_today_list['name']:
                    st.caption(f"• {n}")
    except:
        st.warning("Could not load New Today list")
    
    try:
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        active_today_list = run_query(f"""
            SELECT DISTINCT COALESCE(u.full_name, 'Unknown') AS name
            FROM messages m
            JOIN users u ON m.user_id = u.id
            WHERE m.sent_at >= CURRENT_DATE 
              AND m.user_id IS NOT NULL
              AND m.sender = 'user'
              {internal_filter_join}
            ORDER BY name
            LIMIT 200
        """)
        with st.expander("Active Today - names"):
            if active_today_list.empty:
                st.caption("No users")
            else:
                for n in active_today_list['name']:
                    st.caption(f"• {n}")
    except:
        st.warning("Could not load Active Today list")
    
    try:
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        templates_24h_list = run_query(f"""
            SELECT 
                COALESCE(u.full_name, 'Unknown') AS name,
                COALESCE(r.template_name, 'Unknown') AS template_name
            FROM recovery_logs r
            JOIN users u ON r.user_id = u.id
            WHERE r.sent_at >= NOW() - INTERVAL '24 hours'
              {internal_filter_join}
            ORDER BY name, template_name
            LIMIT 200
        """)
        with st.expander("Templates Sent 24h - recipients"):
            if templates_24h_list.empty:
                st.caption("No users")
            else:
                for _, row in templates_24h_list.iterrows():
                    st.caption(f"• {row['name']} ({row['template_name']})")
    except:
        st.warning("Could not load Templates 24h list")
    
    # User Journey Stats
    st.markdown("### 🎯 User Journey Progress")
    
    try:
        # Get total unique users count (deduplicated by waid) — used for slogan, activity, settings, etc.
        internal_filter = get_internal_users_filter_sql(exclude_internal)
        total_users_query = f"SELECT COUNT(DISTINCT waid) as total FROM users {internal_filter}" if internal_filter else "SELECT COUNT(DISTINCT waid) as total FROM users"
        total_users_result = run_query(total_users_query)
        total_users = total_users_result['total'].iloc[0] if not total_users_result.empty else 0

        # All known WAIDs (messages OR users) — used for "Completed Onboarding %" so we include users who only exist in messages (not yet onboarded)
        all_waids_query = """
            WITH all_waids AS (
                SELECT waid FROM messages WHERE sender = 'user'
                UNION
                SELECT waid FROM users
            )
            SELECT COUNT(DISTINCT waid) AS total FROM all_waids
        """
        if internal_filter:
            all_waids_query = all_waids_query.rstrip() + " " + internal_filter
        total_known_waids_result = run_query(all_waids_query)
        total_known_waids = total_known_waids_result['total'].iloc[0] if not total_known_waids_result.empty else 0
        
        if total_users > 0 or total_known_waids > 0:
            # Get counts for each journey milestone from events table (unique users per event_type)
            internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
            if exclude_internal and internal_filter_join:
                journey_stats = run_query(f"""
                    SELECT 
                        e.event_type,
                        COUNT(DISTINCT e.user_id) as user_count
                    FROM events e
                    JOIN users u ON e.user_id = u.id
                    WHERE e.event_type IN ('onboarding_completed', 'settings_updated')
                      {internal_filter_join}
                    GROUP BY e.event_type
                """)
            else:
                journey_stats = run_query("""
                    SELECT 
                        event_type,
                        COUNT(DISTINCT user_id) as user_count
                    FROM events
                    WHERE event_type IN ('onboarding_completed', 'settings_updated')
                    GROUP BY event_type
                """)
            
            # Convert to dict for easy lookup
            stats_dict = {}
            if not journey_stats.empty:
                for _, row in journey_stats.iterrows():
                    stats_dict[row['event_type']] = row['user_count']
            
            # Get unique users who have added a slogan (from post_onboarding flow)
            internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
            if exclude_internal and internal_filter_join:
                added_slogan_result = run_query(f"""
                    SELECT COUNT(DISTINCT acf.user_id) as count
                    FROM ai_companion_flows acf
                    JOIN users u ON acf.user_id = u.id
                    WHERE acf.type = 'post_onboarding'
                      AND acf.content->>'slogan' IS NOT NULL
                      {internal_filter_join}
                """)
            else:
                added_slogan_result = run_query("""
                    SELECT COUNT(DISTINCT user_id) as count
                    FROM ai_companion_flows
                    WHERE type = 'post_onboarding'
                      AND content->>'slogan' IS NOT NULL
                """)
            added_slogan_count = added_slogan_result['count'].iloc[0] if not added_slogan_result.empty else 0
            
            # Get unique users who completed at least one activity
            internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
            if exclude_internal and internal_filter_join:
                completed_activities_result = run_query(f"""
                    SELECT COUNT(DISTINCT uah.user_id) as count
                    FROM user_activities_history uah
                    JOIN users u ON uah.user_id = u.id
                    WHERE uah.completed_at IS NOT NULL
                      {internal_filter_join}
                """)
            else:
                completed_activities_result = run_query("""
                    SELECT COUNT(DISTINCT user_id) as count
                    FROM user_activities_history
                    WHERE completed_at IS NOT NULL
                """)
            completed_activities_count = completed_activities_result['count'].iloc[0] if not completed_activities_result.empty else 0

            # Get unique users who have sent at least one audio message
            internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
            if exclude_internal and internal_filter_join:
                audio_users_result = run_query(f"""
                    SELECT COUNT(DISTINCT m.user_id) as count
                    FROM messages m
                    JOIN users u ON m.user_id = u.id
                    WHERE m.sender = 'user'
                      AND m.user_id IS NOT NULL
                      AND m.type = 'audio'
                      {internal_filter_join}
                """)
            else:
                audio_users_result = run_query("""
                    SELECT COUNT(DISTINCT user_id) as count
                    FROM messages
                    WHERE sender = 'user'
                      AND user_id IS NOT NULL
                      AND type = 'audio'
                """)
            audio_users_count = audio_users_result['count'].iloc[0] if not audio_users_result.empty else 0

            # Get unique users who have sent at least one picture/image message
            internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
            if exclude_internal and internal_filter_join:
                image_users_result = run_query(f"""
                    SELECT COUNT(DISTINCT m.user_id) as count
                    FROM messages m
                    JOIN users u ON m.user_id = u.id
                    WHERE m.sender = 'user'
                      AND m.user_id IS NOT NULL
                      AND m.type IN ('image', 'photo')
                      {internal_filter_join}
                """)
            else:
                image_users_result = run_query("""
                    SELECT COUNT(DISTINCT user_id) as count
                    FROM messages
                    WHERE sender = 'user'
                      AND user_id IS NOT NULL
                      AND type IN ('image', 'photo')
                """)
            image_users_count = image_users_result['count'].iloc[0] if not image_users_result.empty else 0
            
            # Calculate percentages (onboarding % uses all known WAIDs so it can be < 100%; others use users table)
            onboarding_pct = round(100 * stats_dict.get('onboarding_completed', 0) / total_known_waids, 1) if total_known_waids else 0
            slogan_pct = round(100 * added_slogan_count / total_users, 1) if total_users else 0
            activity_pct = round(100 * completed_activities_count / total_users, 1) if total_users else 0
            settings_pct = round(100 * stats_dict.get('settings_updated', 0) / total_users, 1) if total_users else 0
            audio_pct = round(100 * audio_users_count / total_users, 1) if total_users else 0
            image_pct = round(100 * image_users_count / total_users, 1) if total_users else 0
            
            # Display as metrics (all based on unique users)
            jcol1, jcol2, jcol3, jcol4, jcol5, jcol6 = st.columns(6)
            jcol1.metric(
                "✅ Completed Onboarding", 
                f"{onboarding_pct}%",
                f"{stats_dict.get('onboarding_completed', 0)} of {total_known_waids} users"
            )
            jcol2.metric(
                "💬 Added Slogan", 
                f"{slogan_pct}%",
                f"{added_slogan_count} users"
            )
            jcol3.metric(
                "🏃 Completed Activity", 
                f"{activity_pct}%",
                f"{completed_activities_count} users"
            )
            jcol4.metric(
                "⚙️ Updated Settings", 
                f"{settings_pct}%",
                f"{stats_dict.get('settings_updated', 0)} users"
            )
            jcol5.metric(
                "🎧 Sent Audio", 
                f"{audio_pct}%",
                f"{audio_users_count} users"
            )
            jcol6.metric(
                "📷 Sent Picture", 
                f"{image_pct}%",
                f"{image_users_count} users"
            )
            
        else:
            st.info("No users found")
    except Exception as e:
        st.warning(f"Could not load journey stats: {e}")
    
    st.markdown("### 🪜 Recovery Ladder")
    try:
        # Conversion rates 24h / 72h
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        if exclude_internal and internal_filter_join:
            conversion_df = run_query(f"""
                WITH sent AS (
                    SELECT DISTINCT r.user_id 
                    FROM recovery_logs r
                    JOIN users u ON r.user_id = u.id
                    WHERE 1=1 {internal_filter_join}
                ),
                conv24 AS (
                    SELECT DISTINCT r.user_id
                    FROM recovery_logs r
                    JOIN users u ON r.user_id = u.id
                    JOIN messages m ON m.user_id = r.user_id
                    WHERE m.sender = 'user'
                      AND m.sent_at > r.sent_at
                      AND m.sent_at <= r.sent_at + INTERVAL '24 hours'
                      {internal_filter_join}
                ),
                conv72 AS (
                    SELECT DISTINCT r.user_id
                    FROM recovery_logs r
                    JOIN users u ON r.user_id = u.id
                    JOIN messages m ON m.user_id = r.user_id
                    WHERE m.sender = 'user'
                      AND m.sent_at > r.sent_at
                      AND m.sent_at <= r.sent_at + INTERVAL '72 hours'
                      {internal_filter_join}
                )
                SELECT 
                    (SELECT COUNT(*) FROM sent) AS total_users,
                    (SELECT COUNT(*) FROM conv24) AS conv24_users,
                    (SELECT COUNT(*) FROM conv72) AS conv72_users
            """)
        else:
            conversion_df = run_query("""
                WITH sent AS (
                    SELECT DISTINCT user_id FROM recovery_logs
                ),
                conv24 AS (
                    SELECT DISTINCT r.user_id
                    FROM recovery_logs r
                    JOIN messages m ON m.user_id = r.user_id
                    WHERE m.sender = 'user'
                      AND m.sent_at > r.sent_at
                      AND m.sent_at <= r.sent_at + INTERVAL '24 hours'
                ),
                conv72 AS (
                    SELECT DISTINCT r.user_id
                    FROM recovery_logs r
                    JOIN messages m ON m.user_id = r.user_id
                    WHERE m.sender = 'user'
                      AND m.sent_at > r.sent_at
                      AND m.sent_at <= r.sent_at + INTERVAL '72 hours'
                )
                SELECT 
                    (SELECT COUNT(*) FROM sent) AS total_users,
                    (SELECT COUNT(*) FROM conv24) AS conv24_users,
                    (SELECT COUNT(*) FROM conv72) AS conv72_users
            """)
        total_sent_users = conversion_df['total_users'].iloc[0] if not conversion_df.empty else 0
        conv24_users = conversion_df['conv24_users'].iloc[0] if not conversion_df.empty else 0
        conv72_users = conversion_df['conv72_users'].iloc[0] if not conversion_df.empty else 0
        conv24_pct = round(100 * conv24_users / total_sent_users, 1) if total_sent_users else 0
        conv72_pct = round(100 * conv72_users / total_sent_users, 1) if total_sent_users else 0

        # Ladder drop-off by step and template
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        if exclude_internal and internal_filter_join:
            dropoff_df = run_query(f"""
                WITH recovery_sends AS (
                    SELECT 
                        r.id,
                        COALESCE(r.ladder_step, 0) AS ladder_step,
                        COALESCE(r.template_name, 'Unknown') AS template_name,
                        r.user_id,
                        r.sent_at
                    FROM recovery_logs r
                    JOIN users u ON r.user_id = u.id
                    WHERE 1=1 {internal_filter_join}
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
            """)
        else:
            dropoff_df = run_query("""
                WITH recovery_sends AS (
                    SELECT 
                        r.id,
                        COALESCE(r.ladder_step, 0) AS ladder_step,
                        COALESCE(r.template_name, 'Unknown') AS template_name,
                        r.user_id,
                        r.sent_at
                    FROM recovery_logs r
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
            """)

        # Time to reactivation (avg/median hours)
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        if exclude_internal and internal_filter_join:
            reactivation_df = run_query(f"""
                WITH first_reply AS (
                    SELECT 
                        r.id AS rec_id,
                        r.user_id,
                        r.sent_at,
                        MIN(m.sent_at) AS reply_at
                    FROM recovery_logs r
                    JOIN users u ON r.user_id = u.id
                    JOIN messages m ON m.user_id = r.user_id AND m.sender = 'user' AND m.sent_at > r.sent_at
                    WHERE 1=1 {internal_filter_join}
                    GROUP BY r.id, r.user_id, r.sent_at
                )
                SELECT 
                    AVG(EXTRACT(EPOCH FROM (reply_at - sent_at)))/3600 AS avg_hours,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (reply_at - sent_at))/3600) AS median_hours
                FROM first_reply
            """)
        else:
            reactivation_df = run_query("""
                WITH first_reply AS (
                    SELECT 
                        r.id AS rec_id,
                        r.user_id,
                        r.sent_at,
                        MIN(m.sent_at) AS reply_at
                    FROM recovery_logs r
                    JOIN messages m ON m.user_id = r.user_id AND m.sender = 'user' AND m.sent_at > r.sent_at
                    GROUP BY r.id, r.user_id, r.sent_at
                )
                SELECT 
                    AVG(EXTRACT(EPOCH FROM (reply_at - sent_at)))/3600 AS avg_hours,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (reply_at - sent_at))/3600) AS median_hours
                FROM first_reply
            """)
        avg_hours = round(reactivation_df['avg_hours'].iloc[0], 1) if not reactivation_df.empty and pd.notna(reactivation_df['avg_hours'].iloc[0]) else None
        median_hours = round(reactivation_df['median_hours'].iloc[0], 1) if not reactivation_df.empty and pd.notna(reactivation_df['median_hours'].iloc[0]) else None

        # Users with multiple sends (2nd / 3rd+ ladder steps)
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        if exclude_internal and internal_filter_join:
            multi_send_df = run_query(f"""
                SELECT 
                    COUNT(*) FILTER (WHERE send_count >= 2) AS users_2_plus,
                    COUNT(*) FILTER (WHERE send_count >= 3) AS users_3_plus
                FROM (
                    SELECT r.user_id, COUNT(*) AS send_count
                    FROM recovery_logs r
                    JOIN users u ON r.user_id = u.id
                    WHERE 1=1 {internal_filter_join}
                    GROUP BY r.user_id
                ) t
            """)
        else:
            multi_send_df = run_query("""
                SELECT 
                    COUNT(*) FILTER (WHERE send_count >= 2) AS users_2_plus,
                    COUNT(*) FILTER (WHERE send_count >= 3) AS users_3_plus
                FROM (
                    SELECT user_id, COUNT(*) AS send_count
                    FROM recovery_logs
                    GROUP BY user_id
                ) t
            """)
        users_2_plus = multi_send_df['users_2_plus'].iloc[0] if not multi_send_df.empty else 0
        users_3_plus = multi_send_df['users_3_plus'].iloc[0] if not multi_send_df.empty else 0

        rcol1, rcol2, rcol3, rcol4 = st.columns(4)
        rcol1.metric("Conv 24h", f"{conv24_pct}%", f"{conv24_users}/{total_sent_users} users")
        rcol2.metric("Conv 72h", f"{conv72_pct}%", f"{conv72_users}/{total_sent_users} users")
        rcol3.metric("Avg → Reactivation (h)", avg_hours if avg_hours is not None else "—", f"median {median_hours} h" if median_hours is not None else None)
        rcol4.metric("Users 2nd/3rd+", f"{users_2_plus} / {users_3_plus}")

        with st.expander("Ladder drop-off by step & template", expanded=False):
            if dropoff_df.empty:
                st.caption("No recovery logs yet")
            else:
                st.dataframe(dropoff_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Could not load recovery ladder stats: {e}")
    
    st.markdown("---")
    
    # Recent messages section
    st.markdown("### 💬 Recent Messages")
    
    # Show translation status (less intrusive)
    if GoogleTranslator is None:
        st.caption("ℹ️ Translation unavailable - install `deep-translator` to enable")
    
    # Time range selector
    time_range = st.selectbox(
        "Filter by time range:",
        ["Last 20 messages", "Last 1 hour", "Last 24 hours"],
        key="recent_messages_range"
    )
    
    # Build query based on selected time range
    # Note: Using CURRENT_TIMESTAMP for timezone-aware comparison
    internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
    
    if time_range == "Last 20 messages":
        query = f"""
            SELECT 
                m.id as msg_id,
                m.sent_at as timestamp,
                m.type as msg_type,
                u.full_name as user_name,
                u.timezone as user_timezone,
                m.sender,
                m.message as raw_message
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.id
            WHERE m.sent_at IS NOT NULL
              {internal_filter_join if exclude_internal and internal_filter_join else ""}
            ORDER BY m.sent_at DESC
            LIMIT 20
        """
    elif time_range == "Last 1 hour":
        query = f"""
            SELECT 
                m.id as msg_id,
                m.sent_at as timestamp,
                m.type as msg_type,
                u.full_name as user_name,
                u.timezone as user_timezone,
                m.sender,
                m.message as raw_message
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.id
            WHERE m.sent_at IS NOT NULL
              AND m.sent_at >= NOW() - INTERVAL '1 hour'
              {internal_filter_join if exclude_internal and internal_filter_join else ""}
            ORDER BY m.sent_at DESC
        """
    else:  # Last 24 hours
        query = f"""
            SELECT 
                m.id as msg_id,
                m.sent_at as timestamp,
                m.type as msg_type,
                u.full_name as user_name,
                u.timezone as user_timezone,
                m.sender,
                m.message as raw_message
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.id
            WHERE m.sent_at IS NOT NULL
              AND m.sent_at >= NOW() - INTERVAL '24 hours'
              {internal_filter_join if exclude_internal and internal_filter_join else ""}
            ORDER BY m.sent_at DESC
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

        # Merge sticker + description: same pattern as images (message before sticker)
        description_for_sticker = {}
        for i in range(len(recent_messages)):
            row = recent_messages.iloc[i]
            if not is_sticker_message(row.get("msg_type"), row.get("raw_message")):
                continue
            try:
                ts_cur = pd.to_datetime(row["timestamp"])
            except Exception:
                continue
            # Check i-1 first (message before sticker), then i+1 as fallback
            for candidate_idx in [i - 1, i + 1]:
                if candidate_idx < 0 or candidate_idx >= len(recent_messages):
                    continue
                if candidate_idx in skip_idx:
                    continue
                other = recent_messages.iloc[candidate_idx]
                if other["sender"] != row["sender"] or is_sticker_message(other.get("msg_type"), other.get("raw_message")):
                    continue
                try:
                    ts_other = pd.to_datetime(other["timestamp"])
                    if abs((ts_cur - ts_other).total_seconds()) <= 120:
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
            rows_display.append({
                "Time": format_timestamp_local(row),
                "User": row["user_name"] if pd.notna(row["user_name"]) else "Unknown",
                "From": "👤 User" if row["sender"] == "user" else "🤖 Bot",
                "Type": get_type_label(msg_type_val, is_audio, is_image, is_sticker, is_template(raw_msg)),
                "Message": text,
                "Message (EN)": translate_to_english(text),
            })

        display_df = pd.DataFrame(rows_display)
        
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Time": st.column_config.TextColumn(width="small"),
                "User": st.column_config.TextColumn(width="medium"),
                "From": st.column_config.TextColumn(width="small"),
                "Type": st.column_config.TextColumn(width="small"),
                "Message": st.column_config.TextColumn(width="large"),
                "Message (EN)": st.column_config.TextColumn(width="large"),
            }
        )
    else:
        st.info("No messages found")
    
    st.markdown("---")
    
    # User Activity by Hour chart
    st.markdown("### 📊 User Activity by Hour")
    
    # Date range selector
    date_col1, date_col2 = st.columns(2)
    with date_col1:
        default_start = datetime.now() - timedelta(days=7)
        start_date = st.date_input("Start date", value=default_start, key="activity_start")
    with date_col2:
        end_date = st.date_input("End date", value=datetime.now(), key="activity_end")
    
    try:
        # Query message activity by hour for the selected date range (user messages only)
        internal_filter_join = get_internal_users_filter_join_sql(exclude_internal, "u")
        if exclude_internal and internal_filter_join:
            activity_by_hour = run_query(f"""
                SELECT 
                    EXTRACT(HOUR FROM m.sent_at) as hour,
                    COUNT(*) as message_count
                FROM messages m
                JOIN users u ON m.user_id = u.id
                WHERE m.sent_at >= '{start_date}'::date
                  AND m.sent_at < '{end_date}'::date + INTERVAL '1 day'
                  AND m.sender = 'user'
                  AND m.sent_at IS NOT NULL
                  {internal_filter_join}
                GROUP BY EXTRACT(HOUR FROM m.sent_at)
                ORDER BY hour
            """)
        else:
            activity_by_hour = run_query(f"""
                SELECT 
                    EXTRACT(HOUR FROM sent_at) as hour,
                    COUNT(*) as message_count
                FROM messages
                WHERE sent_at >= '{start_date}'::date
                  AND sent_at < '{end_date}'::date + INTERVAL '1 day'
                  AND sender = 'user'
                  AND sent_at IS NOT NULL
                GROUP BY EXTRACT(HOUR FROM sent_at)
                ORDER BY hour
            """)
        
        if not activity_by_hour.empty:
            # Fill in missing hours with 0
            all_hours = pd.DataFrame({'hour': range(24)})
            activity_by_hour['hour'] = activity_by_hour['hour'].astype(int)
            activity_data = all_hours.merge(activity_by_hour, on='hour', how='left').fillna(0)
            activity_data['message_count'] = activity_data['message_count'].astype(int)
            
            # Format hour labels (e.g., "6am", "2pm")
            def format_hour(h):
                if h == 0:
                    return "12am"
                elif h < 12:
                    return f"{h}am"
                elif h == 12:
                    return "12pm"
                else:
                    return f"{h-12}pm"
            
            activity_data['hour_label'] = activity_data['hour'].apply(format_hour)
            
            # Create bar chart using Altair for better control
            import altair as alt
            
            chart = alt.Chart(activity_data).mark_bar(
                color='#00d4aa',
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X('hour_label:N', 
                        sort=list(activity_data['hour_label']),
                        title='Hour of Day',
                        axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('message_count:Q', title='Messages'),
                tooltip=[
                    alt.Tooltip('hour_label:N', title='Hour'),
                    alt.Tooltip('message_count:Q', title='Messages')
                ]
            ).properties(
                height=300
            ).configure_axis(
                grid=True,
                gridColor='#2d3748'
            ).configure_view(
                strokeWidth=0
            )
            
            st.altair_chart(chart, use_container_width=True)
            
            # Show summary stats
            total_msgs = activity_data['message_count'].sum()
            peak_hour = activity_data.loc[activity_data['message_count'].idxmax()]
            st.caption(f"Total: {total_msgs:,} user messages • Peak hour: {peak_hour['hour_label']} ({int(peak_hour['message_count'])} messages)")
        else:
            st.info("No message activity found for the selected date range")
    except Exception as e:
        st.warning(f"Could not load activity chart: {e}")
    
    st.markdown("---")
    
    # All users (deduplicated by waid) with last message info
    st.markdown("### 👥 All Users")
    
    # Query users with their last sent/received message times and content
    internal_filter = get_internal_users_filter_sql(exclude_internal)
    where_clause = "" if not internal_filter else internal_filter.replace("WHERE", "AND")
    all_users = run_query(f"""
        WITH unique_users AS (
            SELECT DISTINCT ON (waid) 
                id, waid, full_name, gender, age, coach_name, pillar, level, phase, is_active, timezone, created_at, updated_at
            FROM users 
            {internal_filter if internal_filter else ""}
            ORDER BY waid, created_at DESC
        ),
        last_sent AS (
            SELECT DISTINCT ON (m.user_id)
                m.user_id,
                m.sent_at as last_sent_at,
                m.message as last_sent_msg
            FROM messages m
            WHERE m.sender = 'user'
            ORDER BY m.user_id, m.sent_at DESC
        ),
        last_received AS (
            SELECT DISTINCT ON (m.user_id)
                m.user_id,
                m.sent_at as last_received_at,
                m.message as last_received_msg
            FROM messages m
            WHERE m.sender != 'user'
            ORDER BY m.user_id, m.sent_at DESC
        ),
        recovery_counts AS (
            SELECT 
                user_id,
                COUNT(*) AS recovery_templates_sent
            FROM recovery_logs
            GROUP BY user_id
        )
        SELECT 
            u.id,
            u.waid,
            u.full_name,
            u.gender,
            u.age,
            u.coach_name,
            u.pillar,
            u.level,
            u.phase,
            u.is_active,
            u.timezone,
            u.created_at,
            ls.last_sent_at,
            ls.last_sent_msg,
            lr.last_received_at,
            lr.last_received_msg,
            COALESCE(rc.recovery_templates_sent, 0) AS recovery_templates_sent
        FROM unique_users u
        LEFT JOIN last_sent ls ON u.id = ls.user_id
        LEFT JOIN last_received lr ON u.id = lr.user_id
        LEFT JOIN recovery_counts rc ON u.id = rc.user_id
        ORDER BY u.created_at DESC
    """)
    
    if not all_users.empty:
        # Parse timezone string like "UTC-3", "-3", "America/Sao_Paulo"
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
        
        # Format timestamp in user's local timezone
        def format_ts(ts, tz_str):
            if pd.isna(ts) or ts is None:
                return "—"
            try:
                if isinstance(ts, str):
                    ts = pd.to_datetime(ts)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=pytz.UTC)
                user_tz = parse_tz(tz_str)
                if user_tz:
                    ts = ts.astimezone(user_tz)
                    return ts.strftime("%b %d, %H:%M")
                else:
                    return ts.strftime("%b %d, %H:%M") + " UTC"
            except:
                return str(ts)[:16] if ts else "—"
        
        # Extract readable text from message JSON
        def extract_msg(raw_msg):
            if pd.isna(raw_msg) or raw_msg is None:
                return "—"
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
            if data is not None:
                found = find_text(data)
                if found:
                    return found[:80] + "..." if len(found) > 80 else found
            
            if isinstance(data, str) and len(data) > 2:
                return data[:80] if len(data) > 80 else data
            
            if msg_str.startswith("{") or msg_str.startswith("["):
                return msg_str[:80]
            return msg_str[:80] if len(msg_str) > 80 else msg_str
        
        # Build display dataframe
        def is_outside_24h(ts):
            if ts is None or pd.isna(ts):
                return True
            try:
                t = pd.to_datetime(ts)
                if t.tzinfo is None:
                    t = t.tz_localize(pytz.UTC)
                cutoff = pd.Timestamp.utcnow().tz_localize(None) if pd.Timestamp.utcnow().tzinfo else pd.Timestamp.utcnow()
                cutoff = cutoff.tz_localize(pytz.UTC) if cutoff.tzinfo is None else cutoff
                return t < cutoff - pd.Timedelta(hours=24)
            except Exception:
                return True
        
        # Fetch total XP per user (user_id) for display; left join-like via map
        xp_df = run_query("""
            SELECT user_id, COALESCE(SUM(xp_earned), 0) AS total_xp
            FROM user_activities_history
            GROUP BY user_id
        """)
        xp_map = {}
        if not xp_df.empty and 'user_id' in xp_df.columns and 'total_xp' in xp_df.columns:
            xp_map = dict(zip(xp_df['user_id'], xp_df['total_xp']))
        
        display_df = pd.DataFrame({
            'Name': all_users['full_name'].fillna('Unknown'),
            'WhatsApp ID': all_users['waid'],
            'Age': all_users['age'].fillna('—') if 'age' in all_users.columns else '—',
            'Gender': all_users['gender'].fillna('—') if 'gender' in all_users.columns else '—',
            'Coach Name': all_users['coach_name'].fillna('—') if 'coach_name' in all_users.columns else '—',
            'Level': all_users['level'].fillna('—'),
            'XP': all_users['id'].map(xp_map).fillna(0).astype(int) if 'id' in all_users.columns else 0,
            'Signed Up': all_users.apply(lambda r: format_ts(r['created_at'], r['timezone']), axis=1),
            'Last Active': all_users.apply(lambda r: format_ts(r['last_sent_at'], r['timezone']), axis=1),
            'Outside 24h': all_users.apply(lambda r: 'Yes' if is_outside_24h(r['last_sent_at']) else 'No', axis=1),
            'Recovery Templates Sent': all_users['recovery_templates_sent'].fillna(0).astype(int) if 'recovery_templates_sent' in all_users.columns else 0,
            'Last Sent': all_users.apply(lambda r: format_ts(r['last_sent_at'], r['timezone']), axis=1),
            'Last Sent Msg': all_users['last_sent_msg'].apply(extract_msg),
            'Last Received': all_users.apply(lambda r: format_ts(r['last_received_at'], r['timezone']), axis=1),
            'Last Received Msg': all_users['last_received_msg'].apply(extract_msg),
        })
        
        # Show count and expandable table
        st.caption(f"{len(display_df)} total users")
        
        with st.expander("📋 View All Users", expanded=False):
            st.dataframe(
                display_df, 
                use_container_width=True, 
                hide_index=True,
                height=400
            )
            
            # Download button
    else:
        st.info("No users found or table doesn't exist yet")
    


# Tab 2: User Deep Dive
with tab2:
    st.markdown("### 🔍 User Deep Dive")
    st.caption("Select a user to view messages, activity plan, XP, and engagement")
    
    users_df = run_query("""
        WITH unique_users AS (
            SELECT DISTINCT ON (waid)
                id,
                COALESCE(full_name, 'Unknown') as full_name,
                waid,
                timezone,
                created_at,
                coach_name
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
            us.slogan,
            CASE WHEN a.user_id IS NOT NULL THEN true ELSE false END as is_active_24h
        FROM unique_users u
        LEFT JOIN active_users a ON u.id = a.user_id
        LEFT JOIN user_slogans us ON u.id = us.user_id
        ORDER BY u.full_name ASC
        LIMIT 500
    """)
    
    if users_df.empty:
        st.info("No users found")
    else:
        users_df['label'] = users_df.apply(lambda r: f"{r['full_name']}{' *' if r.get('is_active_24h') else ''} ({r['waid']})", axis=1)
        selected_label = st.selectbox("Select user", users_df['label'])
        selected_row = users_df[users_df['label'] == selected_label].iloc[0]
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
        
        # Engagement metrics
        msg_counts = run_query(f"""
            SELECT 
                COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours') as count_24h,
                COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '3 days') as count_3d,
                COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days') as count_7d
            FROM messages
            WHERE user_id = {user_id}
              AND sender = 'user'
              AND sent_at IS NOT NULL
        """)
        mc_row = msg_counts.iloc[0] if not msg_counts.empty else {}
        count_24h = int(mc_row.get('count_24h', 0)) if isinstance(mc_row, pd.Series) else 0
        count_3d = int(mc_row.get('count_3d', 0)) if isinstance(mc_row, pd.Series) else 0
        count_7d = int(mc_row.get('count_7d', 0)) if isinstance(mc_row, pd.Series) else 0
        
        last_active_df = run_query(f"""
            SELECT sent_at 
            FROM messages 
            WHERE user_id = {user_id} AND sender = 'user' AND sent_at IS NOT NULL
            ORDER BY sent_at DESC
            LIMIT 1
        """)
        last_active = format_ts_local(last_active_df['sent_at'].iloc[0]) if not last_active_df.empty else "—"
        
        xp_total_df = run_query(f"""
            SELECT COALESCE(SUM(xp_earned), 0) as total_xp
            FROM user_activities_history
            WHERE user_id = {user_id}
        """)
        total_xp = int(xp_total_df['total_xp'].iloc[0]) if not xp_total_df.empty else 0
        
        last_completed_df = run_query(f"""
            SELECT activity_type, completed_at
            FROM user_activities_history
            WHERE user_id = {user_id} AND completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 1
        """)
        last_activity_name = last_completed_df['activity_type'].iloc[0] if not last_completed_df.empty else "—"
        last_activity_time = format_ts_local(last_completed_df['completed_at'].iloc[0]) if not last_completed_df.empty else "—"
        
        # 24h active window flag based on last user message
        last_msg_df = run_query(f"""
            SELECT sent_at
            FROM messages
            WHERE user_id = {user_id} AND sender = 'user' AND sent_at IS NOT NULL
            ORDER BY sent_at DESC
            LIMIT 1
        """)
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
        outside_24h_flag = is_outside_24h(last_msg_df['sent_at'].iloc[0]) if not last_msg_df.empty else True
        
        # Recovery stats for this user
        user_recovery_df = run_query(f"""
            SELECT 
                id,
                ladder_step,
                template_name,
                converted,
                sent_at
            FROM recovery_logs
            WHERE user_id = {user_id}
            ORDER BY sent_at DESC
            LIMIT 10
        """)
        user_recovery_count = len(user_recovery_df) if not user_recovery_df.empty else 0
        last_recovery_name = user_recovery_df['template_name'].iloc[0] if not user_recovery_df.empty else "—"
        last_recovery_step = user_recovery_df['ladder_step'].iloc[0] if not user_recovery_df.empty else None
        last_recovery_time = format_ts_local(user_recovery_df['sent_at'].iloc[0]) if not user_recovery_df.empty else "—"
        
        # Conversion after last recovery (24h/72h) and time to reply
        conv_after_df = run_query(f"""
            WITH last_rec AS (
                SELECT sent_at
                FROM recovery_logs
                WHERE user_id = {user_id}
                ORDER BY sent_at DESC
                LIMIT 1
            ),
            reply AS (
                SELECT m.sent_at
                FROM messages m, last_rec lr
                WHERE m.user_id = {user_id}
                  AND m.sender = 'user'
                  AND m.sent_at > lr.sent_at
                ORDER BY m.sent_at
                LIMIT 1
            )
            SELECT 
                CASE WHEN EXISTS (
                    SELECT 1 FROM reply r, last_rec lr
                    WHERE r.sent_at <= lr.sent_at + INTERVAL '24 hours'
                ) THEN true ELSE false END AS conv24,
                CASE WHEN EXISTS (
                    SELECT 1 FROM reply r, last_rec lr
                    WHERE r.sent_at <= lr.sent_at + INTERVAL '72 hours'
                ) THEN true ELSE false END AS conv72,
                (SELECT EXTRACT(EPOCH FROM (r.sent_at - lr.sent_at))/3600
                 FROM reply r, last_rec lr
                 LIMIT 1) AS hours_to_reply
        """)
        conv24 = bool(conv_after_df['conv24'].iloc[0]) if not conv_after_df.empty else False
        conv72 = bool(conv_after_df['conv72'].iloc[0]) if not conv_after_df.empty else False
        hrs_reply = conv_after_df['hours_to_reply'].iloc[0] if not conv_after_df.empty else None
        hrs_reply_fmt = f"{hrs_reply:.1f} h" if hrs_reply is not None and pd.notna(hrs_reply) else "—"
        
        # Activity plan (schedule) from user_activities
        plan_df = run_query(f"""
            SELECT 
                activity,
                days,
                created_at
            FROM user_activities
            WHERE user_id = {user_id}
        """)
        
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
                act_name = row.get('activity') or 'Activity'
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
            st.info(f"**Slogan:** {user_slogan if user_slogan and pd.notna(user_slogan) else '—'}")
        
        # Funnel metrics: onboarding completed, slogan set, first activity completed
        st.markdown("#### 🎯 User Journey Funnel")
        funnel_col1, funnel_col2, funnel_col3 = st.columns(3)
        
        # Check onboarding completed
        onboarding_check = run_query(f"""
            SELECT COUNT(*) as count
            FROM events
            WHERE user_id = {user_id}
              AND event_type = 'onboarding_completed'
        """)
        onboarding_completed = onboarding_check['count'].iloc[0] > 0 if not onboarding_check.empty else False
        
        # Check slogan set
        slogan_check = run_query(f"""
            SELECT COUNT(*) as count
            FROM ai_companion_flows
            WHERE user_id = {user_id}
              AND type = 'post_onboarding'
              AND content->>'slogan' IS NOT NULL
        """)
        slogan_set = slogan_check['count'].iloc[0] > 0 if not slogan_check.empty else False
        
        # Check first activity completed
        first_activity_check = run_query(f"""
            SELECT COUNT(*) as count
            FROM user_activities_history
            WHERE user_id = {user_id}
              AND completed_at IS NOT NULL
        """)
        first_activity_completed = first_activity_check['count'].iloc[0] > 0 if not first_activity_check.empty else False
        
        with funnel_col1:
            status_icon = "✅" if onboarding_completed else "❌"
            st.metric("Onboarding Completed", status_icon, "Step 1")
        with funnel_col2:
            status_icon = "✅" if slogan_set else "❌"
            st.metric("Slogan Set", status_icon, "Step 2")
        with funnel_col3:
            status_icon = "✅" if first_activity_completed else "❌"
            st.metric("First Activity Completed", status_icon, "Step 3")
        
        # Most active times analysis (in user's local timezone)
        st.markdown("#### 📊 Most Active Times")
        
        # Get user timezone for conversion
        user_tz = parse_tz(user_tz_str)
        tz_name = user_tz_str if user_tz_str else "UTC"
        
        # Query messages and convert to user's local timezone
        messages_for_times = run_query(f"""
            SELECT sent_at
            FROM messages
            WHERE user_id = {user_id}
              AND sender = 'user'
              AND sent_at IS NOT NULL
        """)
        
        if not messages_for_times.empty:
            # Convert timestamps to user's local timezone
            def convert_to_local_hour(ts):
                try:
                    if isinstance(ts, str):
                        ts = pd.to_datetime(ts)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=pytz.UTC)
                    
                    if user_tz:
                        ts_local = ts.astimezone(user_tz)
                        return ts_local.hour
                    else:
                        return ts.hour
                except Exception:
                    return None
            
            messages_for_times['local_hour'] = messages_for_times['sent_at'].apply(convert_to_local_hour)
            messages_for_times = messages_for_times[messages_for_times['local_hour'].notna()]
            
            if not messages_for_times.empty:
                # Count messages per hour
                hour_counts = messages_for_times['local_hour'].value_counts().sort_index()
                
                # Create full 24-hour dataframe
                all_hours = pd.DataFrame({'hour': range(24)})
                hour_counts_df = pd.DataFrame({
                    'hour': hour_counts.index,
                    'message_count': hour_counts.values
                })
                
                # Merge to get all hours with counts (fill missing with 0)
                active_times_df = all_hours.merge(hour_counts_df, on='hour', how='left').fillna(0)
                active_times_df['message_count'] = active_times_df['message_count'].astype(int)
                
                # Format hour labels for display
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
                
                # Create visual chart using Altair
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
                
                # Show peak activity insight
                peak_hour_row = active_times_df.loc[active_times_df['message_count'].idxmax()]
                if peak_hour_row['message_count'] > 0:
                    peak_hour = peak_hour_row['hour_label']
                    peak_count = int(peak_hour_row['message_count'])
                    total_messages = int(active_times_df['message_count'].sum())
                    st.caption(f"**Peak activity:** {peak_hour} ({peak_count} messages, {round(100 * peak_count / total_messages, 1)}% of total)")
            else:
                st.caption("No message activity data available")
        else:
            st.caption("No message activity data available")
        
        st.markdown("---")
        
        # Metrics row
        m1, m2, m3, m4, m5, m6, m7, m8 = st.columns(8)
        m1.metric("⭐ XP Earned", total_xp)
        m2.metric("✅ Last Completed", last_activity_name, last_activity_time)
        m3.metric("⏭️ Next Activity", next_activity_name, next_activity_day)
        m4.metric("⏱️ Last Active", last_active)
        m5.metric("💬 Messages Sent (24h)", count_24h, f"3d: {count_3d} • 7d: {count_7d}")
        m6.metric("Outside 24h", "Yes" if outside_24h_flag else "No")
        m7.metric("Recovery Sends", user_recovery_count, f"Last: {last_recovery_time}")
        m8.metric("Recovery Conversion", "✓ 24h" if conv24 else ("✓ 72h" if conv72 else "—"), hrs_reply_fmt)
        
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
        
        messages_df = run_query(f"""
            SELECT id as msg_id, sent_at, sender, type as msg_type, message
            FROM messages
            WHERE user_id = {user_id} AND sent_at IS NOT NULL
            ORDER BY sent_at DESC
            LIMIT 100
        """)
        
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

            # Merge sticker + description: same pattern as images (message before sticker)
            description_for_sticker_dd = {}
            for i in range(len(messages_df)):
                row = messages_df.iloc[i]
                if not is_sticker_msg(row.get("msg_type"), row.get("message")):
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
                    if other["sender"] != row["sender"] or is_sticker_msg(other.get("msg_type"), other.get("message")):
                        continue
                    try:
                        ts_other = pd.to_datetime(other["sent_at"])
                        if abs((ts_cur - ts_other).total_seconds()) <= 120:
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
                rows_history.append({
                    "Time": format_ts_local(row["sent_at"]),
                    "From": "👤 User" if row["sender"] == "user" else "🤖 Bot",
                    "Type": get_type_label_dd(msg_type_val, is_audio, is_image, is_sticker, is_template(row.get("message"))),
                    "Message": text,
                    "Message (EN)": translate_to_english(text),
                })

            history_df = pd.DataFrame(rows_history)
            st.dataframe(
                history_df,
                use_container_width=True,
                hide_index=True,
                height=420,
                column_config={
                    "Time": st.column_config.TextColumn(width="small"),
                    "From": st.column_config.TextColumn(width="small"),
                    "Type": st.column_config.TextColumn(width="small"),
                    "Message": st.column_config.TextColumn(width="large"),
                    "Message (EN)": st.column_config.TextColumn(width="large"),
                }
            )


# Tab 3: User Retention
with tab3:
    st.markdown("### 📈 User Retention by Weekly Cohort")
    
    # Load internal users from JSON file
    try:
        internal_users_path = os.path.join(os.path.dirname(__file__), "..", ".context", "internal-users.json")
        if os.path.exists(internal_users_path):
            with open(internal_users_path, 'r') as f:
                internal_users_data = json.load(f)
                internal_waids = [user['waid'] for user in internal_users_data.get('internal_users', [])]
        else:
            # Fallback to hardcoded list if JSON doesn't exist
            internal_waids = [
                '555198161419', '5511988649591', '555195455326',
                '555397038122', '5511970544995', '6593366209', '555199885544'
            ]
    except Exception:
        # Fallback to hardcoded list on any error
        internal_waids = [
            '555198161419', '5511988649591', '555195455326',
            '555397038122', '5511970544995', '6593366209', '555199885544'
        ]
    
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
    (m.sent_at AT TIME ZONE 'America/Sao_Paulo')::date AS local_date
  FROM messages m
  JOIN product_users u ON (u.waid = m.waid OR u.id = m.user_id)
  WHERE m.sender = 'user'
),
user_first_last AS (
  SELECT
    user_id,
    full_name,
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
user_stats AS (
  SELECT
    ufl.user_id,
    ufl.full_name,
    (tl.d - ufl.first_active_date) AS lifetime,
    udc.active_days
  FROM user_first_last ufl
  CROSS JOIN today_local tl
  JOIN user_days_count udc ON udc.user_id = ufl.user_id
)
SELECT user_id, full_name, lifetime, active_days FROM user_stats ORDER BY full_name
"""
                overview_df = run_query(overview_query)
                if not overview_df.empty:
                    established = overview_df[overview_df['lifetime'] >= 7].copy()
                    new_users = overview_df[overview_df['lifetime'] < 7].copy()
                    most_active = established.sort_values('active_days', ascending=False)[['full_name', 'active_days', 'lifetime']].reset_index(drop=True)
                    most_active.columns = ['Name', 'Active days', 'Lifetime (days)']
                    most_inactive = established.sort_values('active_days', ascending=True)[['full_name', 'active_days', 'lifetime']].reset_index(drop=True)
                    most_inactive.columns = ['Name', 'Active days', 'Lifetime (days)']
                    new_users_display = new_users.sort_values('active_days', ascending=False)[['full_name', 'active_days', 'lifetime']].reset_index(drop=True)
                    new_users_display.columns = ['Name', 'Active days', 'Lifetime (days)']
                    st.markdown("#### Quick overview")
                    st.caption("Lifetime (days) = days since first activity through today (São Paulo). Established = 7+ days; New users = under 7 days.")
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


# Tab 4: Alerts (message delivery + future: onboarding drop-off)
with tab4:
    st.markdown("### 🔔 Alerts")

    try:
        delivery_df = get_message_delivery_detail()
    except Exception as e:
        delivery_df = pd.DataFrame()
        st.error(f"Could not load message delivery data: {e}")

    if not delivery_df.empty:
        # Load onboarding drop-off for alert counts in date selector
        try:
            _onboarding_df = get_onboarding_dropoff_detail()
        except Exception:
            _onboarding_df = pd.DataFrame()

        # Date selector: today / yesterday / day_before (with alert count per date)
        periods = [
            ("today", "Today"),
            ("yesterday", "Yesterday"),
            ("day_before", "Day before yesterday"),
        ]
        period_options = [p[1] for p in periods]
        period_keys = [p[0] for p in periods]
        ref_dates_in_df = delivery_df.groupby("period").agg({"ref_date": "first"}).to_dict().get("ref_date", {})
        labels_date_only = []  # e.g. "Today (2026-02-03)" — used in body text
        labels_with_dates = []  # same + " — N alerts" — used in radio only
        for key, label in periods:
            d = ref_dates_in_df.get(key, "")
            date_only_str = f"{label}" + (f" ({d})" if d else "")
            labels_date_only.append(date_only_str)
            msg_missed = int(
                delivery_df.loc[delivery_df["period"] == key, "missed_morning"].sum()
                + delivery_df.loc[delivery_df["period"] == key, "missed_evening"].sum()
            )
            onb_count = len(_onboarding_df[_onboarding_df["period"] == key]) if not _onboarding_df.empty else 0
            total_alerts = msg_missed + onb_count
            labels_with_dates.append(date_only_str + f" — {total_alerts} alert{'s' if total_alerts != 1 else ''}")
        selected_idx = st.radio(
            "**Select date**",
            range(len(labels_with_dates)),
            format_func=lambda i: labels_with_dates[i],
            horizontal=True,
            key="alerts_date_radio",
        )
        selected_period = period_keys[selected_idx]
        selected_ref_date = ref_dates_in_df.get(selected_period, "")

        df_day = delivery_df[delivery_df["period"] == selected_period].copy()
        missed_count = int(df_day["missed_morning"].sum() + df_day["missed_evening"].sum())
        date_label = labels_date_only[selected_idx]  # date only, no alert count (for body text)

        if missed_count == 0:
            st.success(f"✅ No missed messages for **{date_label}**.")
        else:
            st.info(f"**{missed_count} message(s) missed** for **{date_label}**.")

        st.markdown(f"#### 📬 Message delivery — {date_label}")
        # One row per (time, slot): Time | Slot | Due | Missed | Users (always show table for verification)
        df_day["time_morning"] = df_day["check_in_time"].apply(lambda x: str(x)[:8] if pd.notna(x) else "")
        df_day["time_evening"] = df_day["daily_digest_time"].apply(lambda x: str(x)[:8] if pd.notna(x) else "")
        morning = df_day.groupby("time_morning").agg(
            due=("due_morning", "sum"),
            missed=("missed_morning", "sum"),
        ).reset_index()
        morning["slot"] = "Morning"
        morning_names = df_day[df_day["missed_morning"].fillna(False)].groupby("time_morning")["full_name"].apply(lambda x: ", ".join(sorted(x.unique()))).reset_index()
        morning_names.columns = ["time_morning", "users"]
        morning = morning.merge(morning_names, on="time_morning", how="left")
        morning["users"] = morning["users"].fillna("").astype(str)
        evening = df_day.groupby("time_evening").agg(
            due=("due_evening", "sum"),
            missed=("missed_evening", "sum"),
        ).reset_index()
        evening["slot"] = "Evening"
        evening_names = df_day[df_day["missed_evening"].fillna(False)].groupby("time_evening")["full_name"].apply(lambda x: ", ".join(sorted(x.unique()))).reset_index()
        evening_names.columns = ["time_evening", "users"]
        evening = evening.merge(evening_names, on="time_evening", how="left")
        evening["users"] = evening["users"].fillna("").astype(str)
        table = pd.concat([
            morning.rename(columns={"time_morning": "check-in time"})[["check-in time", "slot", "due", "missed", "users"]],
            evening.rename(columns={"time_evening": "check-in time"})[["check-in time", "slot", "due", "missed", "users"]],
        ], ignore_index=True)
        table = table.sort_values(["check-in time", "slot"])
        st.markdown("**By check-in time**")
        st.dataframe(table, use_container_width=True, hide_index=True)

        # Reply pending > 1hr: users whose last message has no companion reply and > 1hr ago
        st.markdown("---")
        st.markdown("### ⏳ Reply pending > 1hr")
        st.caption("Users who sent a message and have not received a reply within 1 hour. Resets once they get a reply.")

        try:
            pending_df = get_pending_reply_detail()
        except Exception as e:
            pending_df = pd.DataFrame()
            st.error(f"Could not load pending-reply data: {e}")

        if pending_df.empty:
            st.success("✅ No users waiting for a reply > 1hr.")
        else:
            st.info(f"**{len(pending_df)} user(s)** waiting for a reply > 1hr.")
        if not pending_df.empty:
            now_utc = pd.Timestamp.utcnow()
            pending_display = pending_df[["waid", "full_name", "last_sent_at", "last_message"]].copy()
            pending_display["last message"] = pending_display["last_message"].apply(
                lambda m: _extract_message_text_snippet(m, max_len=100)
            )
            pending_display["reply pending"] = pd.to_datetime(pending_display["last_sent_at"], utc=True).apply(
                lambda t: _format_pending_duration(now_utc - t) if pd.notna(t) else "—"
            )
            pending_display["last message at"] = pd.to_datetime(pending_display["last_sent_at"], utc=True).apply(
                lambda t: t.strftime("%Y-%m-%d %H:%M UTC") if pd.notna(t) else "—"
            )
            st.dataframe(
                pending_display[["waid", "full_name", "last message at", "last message", "reply pending"]],
                use_container_width=True,
                hide_index=True,
            )

        # Onboarding drop-off (same date selector; reuse _onboarding_df from date selector)
        st.markdown("---")
        st.markdown("### 🚪 Onboarding drop-off")
        st.caption("Users who messaged on the selected date but dropped off at onboarding (WAID only), or completed onboarding but didn't set a slogan.")

        onboarding_df = _onboarding_df if not _onboarding_df.empty else pd.DataFrame()
        if not onboarding_df.empty:
            od_day = onboarding_df[onboarding_df["period"] == selected_period].copy()
            dropped = od_day[od_day["issue_type"] == "dropped_off_onboarding"]
            no_slogan = od_day[od_day["issue_type"] == "no_slogan"]
            n_dropped = len(dropped)
            n_slogan = len(no_slogan)

            if n_dropped == 0:
                st.success(f"✅ No users dropped off at onboarding for **{date_label}**.")
            else:
                st.info(f"**{n_dropped} user(s) dropped off at onboarding** for **{date_label}** (messaged but did not complete onboarding).")
            st.markdown("**Dropped off at onboarding** (WAID, onboarding started = first user message)")
            if dropped.empty:
                st.caption("None")
            else:
                dropped_display = dropped[["waid", "onboarding_started_at", "user_timezone"]].copy()
                dropped_display["onboarding started"] = dropped_display.apply(
                    lambda row: _format_ts_local(row["onboarding_started_at"], row["user_timezone"]), axis=1
                )
                st.dataframe(dropped_display[["waid", "onboarding started"]], use_container_width=True, hide_index=True)

            if n_slogan == 0:
                st.success(f"✅ No users without slogan for **{date_label}**.")
            else:
                st.info(f"**{n_slogan} user(s) without slogan** for **{date_label}**.")
            st.markdown("**Didn't set slogan** (completed onboarding on selected day but no slogan in post_onboarding flow)")
            if no_slogan.empty:
                st.caption("None")
            else:
                slogan_display = no_slogan[["waid", "full_name", "onboarding_completed_at", "user_timezone"]].copy()
                slogan_display["onboarding completed"] = slogan_display.apply(
                    lambda row: _format_ts_local(row["onboarding_completed_at"], row["user_timezone"]), axis=1
                )
                st.dataframe(slogan_display[["waid", "full_name", "onboarding completed"]], use_container_width=True, hide_index=True)
        else:
            st.info("No onboarding drop-off data available.")

    else:
        st.info("No message delivery data available. Check database connection and that users/messages/reschedule tables exist.")


# Footer
st.markdown("---")
st.caption(f"LETZ Data Dashboard v1.1 • Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

