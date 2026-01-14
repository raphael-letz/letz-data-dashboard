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

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="LETZ Dashboard",
    page_icon="📊",
    layout="wide"
)

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
        
        # Table explorer
        tables = get_table_list()
        if tables:
            st.markdown("**Tables:**")
            selected_table = st.selectbox("Select table", tables, label_visibility="collapsed")
            
            if selected_table:
                with st.expander(f"📋 Schema: {selected_table}"):
                    schema = get_table_schema(selected_table)
                    st.dataframe(schema, use_container_width=True, hide_index=True)
                
    else:
        st.error("✗ Not connected")
        st.info("Check your .env file")
    
    st.markdown("---")
    st.markdown("### ⚙️ Settings")
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)
    
    if auto_refresh:
        st.cache_resource.clear()


# Main content tabs
tab1, tab2 = st.tabs(["📊 Quick Insights", "🔍 User Deep Dive"])


# Tab 1: Quick Insights
with tab1:
    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    
    # Try to get quick stats (all deduplicated by waid)
    try:
        # User count (unique waids)
        user_count_df = run_query("SELECT COUNT(DISTINCT waid) as count FROM users")
        total_users_count = user_count_df['count'].iloc[0] if not user_count_df.empty else 0
        if user_count_df is not None and not user_count_df.empty:
            col1.metric("Total Users", total_users_count)
        else:
            col1.metric("Total Users", "—")
    except:
        col1.metric("Total Users", "—")
    
    try:
        # Today's users (unique waids)
        today_users = run_query("""
            SELECT COUNT(DISTINCT waid) as count FROM users 
            WHERE created_at >= CURRENT_DATE
        """)
        if not today_users.empty:
            col2.metric("New Today", today_users['count'].iloc[0])
        else:
            col2.metric("New Today", "—")
    except:
        col2.metric("New Today", "—")
    
    try:
        # This week's users (unique waids)
        week_users = run_query("""
            SELECT COUNT(DISTINCT waid) as count FROM users 
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
        """)
        if not week_users.empty:
            col3.metric("New This Week", week_users['count'].iloc[0])
        else:
            col3.metric("New This Week", "—")
    except:
        col3.metric("New This Week", "—")
    
    try:
        # Active today (unique waids who SENT a message today)
        active_today = run_query("""
            SELECT COUNT(DISTINCT u.waid) as count 
            FROM messages m
            JOIN users u ON m.user_id = u.id
            WHERE m.sent_at >= CURRENT_DATE 
              AND m.user_id IS NOT NULL
              AND m.sender = 'user'
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
            user_count_df = run_query("SELECT COUNT(DISTINCT waid) as count FROM users")
            total_users_count = user_count_df['count'].iloc[0] if not user_count_df.empty else 0
        
        active_24h_df = run_query("""
            SELECT COUNT(DISTINCT u.waid) as count
            FROM messages m
            JOIN users u ON m.user_id = u.id
            WHERE m.sender = 'user'
              AND m.sent_at >= NOW() - INTERVAL '24 hours'
        """)
        active_24h = active_24h_df['count'].iloc[0] if not active_24h_df.empty else 0
        outside = max(total_users_count - active_24h, 0)
        outside_pct = round(100 * outside / total_users_count, 1) if total_users_count > 0 else 0
        col5.metric("% Outside 24h", f"{outside_pct}%", f"{outside} users")
    except:
        col5.metric("% Outside 24h", "—")
    
    try:
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
        today_day = datetime.utcnow().strftime("%A")
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
        new_today_list = run_query("""
            SELECT COALESCE(full_name, 'Unknown') AS name
            FROM users
            WHERE created_at >= CURRENT_DATE
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
        active_today_list = run_query("""
            SELECT DISTINCT COALESCE(u.full_name, 'Unknown') AS name
            FROM messages m
            JOIN users u ON m.user_id = u.id
            WHERE m.sent_at >= CURRENT_DATE 
              AND m.user_id IS NOT NULL
              AND m.sender = 'user'
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
        templates_24h_list = run_query("""
            SELECT DISTINCT COALESCE(u.full_name, 'Unknown') AS name
            FROM recovery_logs r
            JOIN users u ON r.user_id = u.id
            WHERE r.sent_at >= NOW() - INTERVAL '24 hours'
            ORDER BY name
            LIMIT 200
        """)
        with st.expander("Templates Sent 24h - recipients"):
            if templates_24h_list.empty:
                st.caption("No users")
            else:
                for n in templates_24h_list['name']:
                    st.caption(f"• {n}")
    except:
        st.warning("Could not load Templates 24h list")
    
    st.markdown("---")
    
    # User Journey Stats
    st.markdown("### 🎯 User Journey Progress")
    
    try:
        # Get total unique users count
        total_users_result = run_query("SELECT COUNT(DISTINCT waid) as total FROM users")
        total_users = total_users_result['total'].iloc[0] if not total_users_result.empty else 0
        
        if total_users > 0:
            # Get counts for each journey milestone from events table
            journey_stats = run_query("""
                SELECT 
                    event_type,
                    COUNT(DISTINCT user_id) as user_count
                FROM events
                WHERE event_type IN ('onboarding_completed', 'update_experience', 'settings_updated')
                GROUP BY event_type
            """)
            
            # Convert to dict for easy lookup
            stats_dict = {}
            if not journey_stats.empty:
                for _, row in journey_stats.iterrows():
                    stats_dict[row['event_type']] = row['user_count']
            
            # Get unique users who completed an activity (have completed_at timestamp in user_activities_history)
            # This is also the count for "Earned XP" since completing activity = earning XP
            completed_activities_result = run_query("""
                SELECT COUNT(DISTINCT user_id) as count
                FROM user_activities_history
                WHERE completed_at IS NOT NULL
            """)
            completed_activities_count = completed_activities_result['count'].iloc[0] if not completed_activities_result.empty else 0
            
            # Calculate percentages
            onboarding_pct = round(100 * stats_dict.get('onboarding_completed', 0) / total_users, 1)
            activity_pct = round(100 * completed_activities_count / total_users, 1)
            xp_pct = activity_pct  # Same as completed activity - completing activity = earning XP
            settings_pct = round(100 * stats_dict.get('settings_updated', 0) / total_users, 1)
            
            # Display as metrics
            jcol1, jcol2, jcol3, jcol4 = st.columns(4)
            jcol1.metric(
                "✅ Completed Onboarding", 
                f"{onboarding_pct}%",
                f"{stats_dict.get('onboarding_completed', 0)} users"
            )
            jcol2.metric(
                "🏃 Completed Activity", 
                f"{activity_pct}%",
                f"{completed_activities_count} users"
            )
            jcol3.metric(
                "⭐ Earned XP", 
                f"{xp_pct}%",
                f"{completed_activities_count} users"
            )
            jcol4.metric(
                "⚙️ Updated Settings", 
                f"{settings_pct}%",
                f"{stats_dict.get('settings_updated', 0)} users"
            )
            
        else:
            st.info("No users found")
    except Exception as e:
        st.warning(f"Could not load journey stats: {e}")
    
    st.markdown("---")
    
    # Recent messages section
    st.markdown("### 💬 Recent Messages")
    recent_messages = run_query("""
        SELECT 
            m.sent_at as timestamp,
            u.full_name as user_name,
            u.timezone as user_timezone,
            m.sender,
            m.message as raw_message
        FROM messages m
        LEFT JOIN users u ON m.user_id = u.id
        WHERE m.sent_at IS NOT NULL
        ORDER BY m.sent_at DESC
        LIMIT 10
    """)
    
    if not recent_messages.empty:
        # Helper to check if message payload is a template
        def is_template(raw_msg):
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
                    if 'template' in data:
                        return True
                    if data.get('type') == 'template':
                        return True
                if isinstance(data, str) and 'template' in data.lower():
                    return True
            except Exception:
                return False
            return False
        
        # Process messages to extract readable text from JSON
        def extract_message_text(raw_msg):
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
                if "interactive" in data:
                    found = find_text(data["interactive"])
                    if found:
                        return found[:200]
                if "postback" in data:
                    found = find_text(data["postback"])
                    if found:
                        return found[:200]
                if "template" in data:
                    found = find_text(data["template"])
                    if found:
                        return found[:200]
            
            if data is not None:
                found = find_text(data)
                if found:
                    return found[:200]
            
            if isinstance(data, str) and len(data) > 2:
                return data[:200]
            
            # Fallback: show truncated payload instead of "[Complex message]"
            if msg_str.startswith('{') or msg_str.startswith('['):
                return msg_str[:200]
            return msg_str[:150] if len(msg_str) > 150 else msg_str
        
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
        
        # Build display dataframe
        display_df = pd.DataFrame({
            'Time': recent_messages.apply(format_timestamp_local, axis=1),
            'User': recent_messages['user_name'].fillna('Unknown'),
            'From': recent_messages['sender'].apply(lambda x: '👤 User' if x == 'user' else '🤖 Bot'),
            'Message': recent_messages['raw_message'].apply(extract_message_text),
            'Template?': recent_messages['raw_message'].apply(lambda x: 'Yes' if is_template(x) else 'No')
        })
        
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Time": st.column_config.TextColumn(width="small"),
                "User": st.column_config.TextColumn(width="medium"),
                "From": st.column_config.TextColumn(width="small"),
                "Message": st.column_config.TextColumn(width="large"),
            }
        )
    else:
        st.info("No messages found")
    
    st.markdown("---")
    
    # All users (deduplicated by waid) with last message info
    st.markdown("### 👥 All Users")
    
    # Query users with their last sent/received message times and content
    all_users = run_query("""
        WITH unique_users AS (
            SELECT DISTINCT ON (waid) 
                id, waid, full_name, gender, pillar, level, phase, is_active, timezone, created_at, updated_at
            FROM users 
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
            u.updated_at,
            CASE 
                WHEN u.updated_at < NOW() - INTERVAL '24 hours' THEN true 
                ELSE false 
            END as outside_24h,
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
            csv = display_df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                "all_users.csv",
                "text/csv"
            )
    else:
        st.info("No users found or table doesn't exist yet")
    


# Tab 2: User Deep Dive
with tab2:
    st.markdown("### 🔍 User Deep Dive")
    st.caption("Select a user to view messages, activity plan, XP, and engagement")
    
    users_df = run_query("""
        SELECT 
            id,
            COALESCE(full_name, 'Unknown') as full_name,
            waid,
            timezone,
            created_at
        FROM users
        ORDER BY created_at DESC
        LIMIT 500
    """)
    
    if users_df.empty:
        st.info("No users found")
    else:
        users_df['label'] = users_df.apply(lambda r: f"{r['full_name']} ({r['waid']})", axis=1)
        selected_label = st.selectbox("Select user", users_df['label'])
        selected_row = users_df[users_df['label'] == selected_label].iloc[0]
        user_id = int(selected_row['id'])
        user_tz_str = selected_row.get('timezone')
        
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
        
        # Metrics row
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("⭐ XP Earned", total_xp)
        m2.metric("✅ Last Completed", last_activity_name, last_activity_time)
        m3.metric("⏭️ Next Activity", next_activity_name, next_activity_day)
        m4.metric("⏱️ Last Active", last_active)
        m5.metric("💬 Messages Sent (24h)", count_24h, f"3d: {count_3d} • 7d: {count_7d}")
        m6.metric("Outside 24h", "Yes" if outside_24h_flag else "No")
        
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
            SELECT sent_at, sender, message
            FROM messages
            WHERE user_id = {user_id} AND sent_at IS NOT NULL
            ORDER BY sent_at DESC
            LIMIT 100
        """)
        
        def extract_msg_text(raw_msg):
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
            if data is not None:
                found = find_text(data)
                if found:
                    return found[:200]
            
            if isinstance(data, str) and len(data) > 2:
                return data[:200]
            
            if msg_str.startswith("{") or msg_str.startswith("["):
                return msg_str[:200]
            return msg_str[:200]
        
        if messages_df.empty:
            st.info("No messages found for this user.")
        else:
            history_df = pd.DataFrame({
                "Time": messages_df['sent_at'].apply(format_ts_local),
                "From": messages_df['sender'].apply(lambda x: '👤 User' if x == 'user' else '🤖 Bot'),
                "Message": messages_df['message'].apply(extract_msg_text),
                "Template?": messages_df['message'].apply(lambda x: 'Yes' if is_template(x) else 'No')
            })
            st.dataframe(
                history_df,
                use_container_width=True,
                hide_index=True,
                height=420
            )


# Footer
st.markdown("---")
st.caption(f"LETZ Data Dashboard v1.1 • Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

