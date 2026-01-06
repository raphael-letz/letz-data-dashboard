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
        # Try Streamlit secrets first (for cloud deployment), then fall back to .env
        if hasattr(st, 'secrets') and 'DB_HOST' in st.secrets:
            conn = psycopg2.connect(
                host=st.secrets["DB_HOST"],
                database=st.secrets["DB_NAME"],
                user=st.secrets["DB_USER"],
                password=st.secrets["DB_PASSWORD"],
                port=st.secrets.get("DB_PORT", "5432")
            )
        else:
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
    """Get schema for a specific table."""
    query = f"""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    return run_query(query)


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
    (SELECT COUNT(*) FROM user_activities WHERE completed = true) as completed_activities,
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
    ua.id,
    ua.user_id,
    u.full_name,
    ua.activity,
    ua.completed,
    ua.progress,
    ua.created_at,
    ua.last_activity_at
FROM user_activities ua
LEFT JOIN users u ON ua.user_id = u.id
WHERE ua.completed = true
ORDER BY ua.last_activity_at DESC
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
                
                if st.button(f"Preview {selected_table}", use_container_width=True):
                    st.session_state['custom_query'] = f"SELECT * FROM {selected_table} LIMIT 20"
    else:
        st.error("✗ Not connected")
        st.info("Check your .env file")
    
    st.markdown("---")
    st.markdown("### ⚙️ Settings")
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)
    
    if auto_refresh:
        st.cache_resource.clear()


# Main content tabs
tab1, tab2, tab3 = st.tabs(["📊 Quick Insights", "🔍 Custom Query", "📋 Predefined Queries"])


# Tab 1: Quick Insights
with tab1:
    col1, col2, col3, col4 = st.columns(4)
    
    # Try to get quick stats (all deduplicated by waid)
    try:
        # User count (unique waids)
        user_count = run_query("SELECT COUNT(DISTINCT waid) as count FROM users")
        if not user_count.empty:
            col1.metric("Total Users", user_count['count'].iloc[0])
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
    
    st.markdown("---")
    
    # User Journey Stats
    st.markdown("### 🎯 User Journey Progress")
    
    try:
        # Get total unique users count
        total_users_result = run_query("SELECT COUNT(DISTINCT waid) as total FROM users")
        total_users = total_users_result['total'].iloc[0] if not total_users_result.empty else 0
        
        if total_users > 0:
            # Get counts for each journey milestone
            journey_stats = run_query("""
                SELECT 
                    event_type,
                    COUNT(DISTINCT user_id) as user_count
                FROM events
                WHERE event_type IN ('onboarding_completed', 'complete_activity', 'update_experience', 'settings_updated')
                GROUP BY event_type
            """)
            
            # Convert to dict for easy lookup
            stats_dict = {}
            if not journey_stats.empty:
                for _, row in journey_stats.iterrows():
                    stats_dict[row['event_type']] = row['user_count']
            
            # Calculate percentages
            onboarding_pct = round(100 * stats_dict.get('onboarding_completed', 0) / total_users, 1)
            activity_pct = round(100 * stats_dict.get('complete_activity', 0) / total_users, 1)
            xp_pct = round(100 * stats_dict.get('update_experience', 0) / total_users, 1)
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
                f"{stats_dict.get('complete_activity', 0)} users"
            )
            jcol3.metric(
                "⭐ Earned XP", 
                f"{xp_pct}%",
                f"{stats_dict.get('update_experience', 0)} users"
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
    
    # Weekly Activity Calendar
    st.markdown("### 📅 Weekly Activity Schedule")
    st.caption("Users who completed onboarding and have activities scheduled")
    
    try:
        # Get users with activities who have completed onboarding
        # days column is JSONB - can be a string like "Tuesday,Monday" or possibly an array
        calendar_data = run_query("""
            SELECT 
                ua.days::text as days,
                u.full_name,
                u.id as user_id
            FROM user_activities ua
            JOIN users u ON ua.user_id = u.id
            JOIN events e ON e.user_id = u.id AND e.event_type = 'onboarding_completed'
            WHERE ua.days IS NOT NULL 
              AND ua.days::text != 'null'
              AND ua.days::text != '""'
              AND ua.days::text != ''
            ORDER BY u.full_name
        """)
        
        if not calendar_data.empty:
            weekday_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            weekday_map = {
                'monday': 'Mon', 'tuesday': 'Tue', 'wednesday': 'Wed',
                'thursday': 'Thu', 'friday': 'Fri', 'saturday': 'Sat', 'sunday': 'Sun'
            }
            
            # Build users by day - parse days (can be comma-separated or JSON)
            users_by_day = {day: set() for day in weekday_names}
            
            for _, row in calendar_data.iterrows():
                days_str = row['days']
                user_name = row['full_name'] or 'Unknown'
                
                if pd.notna(days_str) and days_str:
                    days_str = str(days_str).strip()
                    
                    # Remove JSON quotes if present (e.g., '"Tuesday,Monday"' -> 'Tuesday,Monday')
                    if days_str.startswith('"') and days_str.endswith('"'):
                        days_str = days_str[1:-1]
                    
                    # Split comma-separated days
                    for day in days_str.split(','):
                        day_clean = day.strip().lower()
                        day_short = weekday_map.get(day_clean, day_clean[:3].title())
                        if day_short in users_by_day:
                            users_by_day[day_short].add(user_name)
            
            # Convert sets to sorted lists
            for day in users_by_day:
                users_by_day[day] = sorted(list(users_by_day[day]))
            
            # Display as columns
            cols = st.columns(7)
            for i, day in enumerate(weekday_names):
                with cols[i]:
                    users = users_by_day.get(day, [])
                    user_count = len(users)
                    st.markdown(f"**{day}**")
                    st.markdown(f"<div style='font-size: 24px; font-weight: bold; color: #00d4aa;'>{user_count}</div>", unsafe_allow_html=True)
                    
                    # Show user names (limit to 5 with expander for more)
                    if users:
                        display_users = users[:5]
                        for user in display_users:
                            st.caption(f"• {user}")
                        if len(users) > 5:
                            with st.expander(f"+{len(users)-5} more"):
                                for user in users[5:]:
                                    st.caption(f"• {user}")
                    else:
                        st.caption("—")
        else:
            st.info("No activity schedule data found")
    except Exception as e:
        st.warning(f"Could not load activity calendar: {e}")
    
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
        # Process messages to extract readable text from JSON
        def extract_message_text(raw_msg):
            if pd.isna(raw_msg) or raw_msg is None:
                return ""
            msg_str = str(raw_msg).strip()
            
            # Helper to parse JSON (handles double-encoded strings)
            def parse_json(s):
                try:
                    data = json.loads(s)
                    # If result is still a string, try parsing again (double-encoded)
                    if isinstance(data, str):
                        try:
                            return json.loads(data)
                        except:
                            return data
                    return data
                except:
                    return None
            
            # Helper to recursively find text in nested dicts
            def find_text(obj, depth=0):
                if depth > 10:
                    return None
                if isinstance(obj, str) and len(obj) > 2:
                    return obj
                if isinstance(obj, dict):
                    # Priority: check 'text' key first at any level
                    if 'text' in obj:
                        val = obj['text']
                        if isinstance(val, str) and len(val) > 2:
                            return val
                    # Then check other common keys
                    for key in ['title', 'body', 'message', 'content', 'caption']:
                        if key in obj:
                            val = obj[key]
                            if isinstance(val, str) and len(val) > 2:
                                return val
                            result = find_text(val, depth + 1)
                            if result:
                                return result
                    # Check all other values
                    for val in obj.values():
                        if isinstance(val, (dict, list)):
                            result = find_text(val, depth + 1)
                            if result:
                                return result
                if isinstance(obj, list):
                    for item in obj:
                        result = find_text(item, depth + 1)
                        if result:
                            return result
                return None
            
            # Parse the JSON
            data = parse_json(msg_str)
            
            if isinstance(data, dict):
                # === WHATSAPP MESSAGE TYPES ===
                
                # 1. Flows messages: {"flows": {"body": {"text": "..."}}}
                if 'flows' in data:
                    text = find_text(data['flows'])
                    if text:
                        return text[:200]
                
                # 2. QuickReply messages: {"quickReply": {"body": {"text": "..."}}}
                if 'quickReply' in data:
                    text = find_text(data['quickReply'])
                    if text:
                        return text[:200]
                
                # 3. Postback (button click): {"postback": {"payload": {"text": "...", "value": "..."}}}
                if 'postback' in data:
                    payload = data['postback'].get('payload', {})
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except:
                            return f"🔘 {payload[:100]}"
                    if isinstance(payload, dict):
                        text = payload.get('text', '')
                        value = payload.get('value', '')
                        if text:
                            # Clean up unicode escapes
                            return f"🔘 {text}"
                        if value:
                            return f"🔘 [{value}]"
                    text = find_text(data['postback'])
                    if text:
                        return f"🔘 {text[:100]}"
                
                # 4. Interactive messages
                if 'interactive' in data:
                    interactive = data['interactive']
                    if isinstance(interactive, dict):
                        if 'button_reply' in interactive:
                            return f"🔘 {interactive['button_reply'].get('title', 'Button')}"
                        if 'list_reply' in interactive:
                            return f"📋 {interactive['list_reply'].get('title', 'Selection')}"
                        text = find_text(interactive)
                        if text:
                            return text[:200]
                
                # 5. Media messages
                if 'image' in data:
                    caption = find_text(data.get('image', {}))
                    return f"📷 Image{': ' + caption[:80] if caption else ''}"
                if 'document' in data:
                    doc = data['document']
                    filename = doc.get('filename', '') if isinstance(doc, dict) else ''
                    return f"📄 Document{': ' + filename if filename else ''}"
                if 'audio' in data:
                    return "🎵 Audio message"
                if 'video' in data:
                    return "🎬 Video"
                if 'sticker' in data:
                    return "😀 Sticker"
                if 'location' in data:
                    return "📍 Location"
                if 'contacts' in data:
                    return "👤 Contact shared"
                
                # 6. Template messages
                if 'template' in data:
                    text = find_text(data['template'])
                    if text:
                        return text[:200]
                
                # 7. Generic fallback - find any text
                text = find_text(data)
                if text:
                    return text[:200]
            
            # If data is a plain string (not dict), return it
            if isinstance(data, str) and len(data) > 2:
                return data[:200]
            
            # Last resort: return truncated original (but try to show it's JSON)
            if msg_str.startswith('{') or msg_str.startswith('['):
                return "[Complex message]"
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
            'Message': recent_messages['raw_message'].apply(extract_message_text)
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
    
    # Recent users (deduplicated by waid, showing 10 most recent unique users)
    st.markdown("### 👥 Recent Users")
    recent_users = run_query("""
        SELECT * FROM (
            SELECT DISTINCT ON (waid) id, waid, full_name, gender, pillar, level, phase, is_active, created_at 
            FROM users 
            ORDER BY waid, created_at DESC
        ) unique_users
        ORDER BY created_at DESC
        LIMIT 10
    """)
    if not recent_users.empty:
        st.dataframe(recent_users, use_container_width=True, hide_index=True)
    else:
        st.info("No users found or table doesn't exist yet")


# Tab 2: Custom Query
with tab2:
    st.markdown("### 🔍 Run Custom SQL")
    st.caption("Write and execute any SQL query")
    
    # Get query from session state or use default
    default_query = st.session_state.get('custom_query', 'SELECT * FROM users LIMIT 10')
    
    custom_query = st.text_area(
        "SQL Query",
        value=default_query,
        height=150,
        key="sql_input",
        help="Enter your SQL query here"
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        run_button = st.button("▶️ Run Query", type="primary", use_container_width=True)
    with col2:
        if st.button("🗑️ Clear", use_container_width=True):
            st.session_state['custom_query'] = ''
            st.rerun()
    
    if run_button and custom_query:
        with st.spinner("Running query..."):
            start_time = datetime.now()
            result = run_query(custom_query)
            elapsed = (datetime.now() - start_time).total_seconds()
        
        if not result.empty:
            st.success(f"✓ {len(result)} rows returned in {elapsed:.2f}s")
            st.dataframe(result, use_container_width=True, hide_index=True)
            
            # Download button
            csv = result.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                "query_result.csv",
                "text/csv",
                use_container_width=False
            )
        else:
            st.warning("No results or query failed")


# Tab 3: Predefined Queries
with tab3:
    st.markdown("### 📋 Predefined Queries")
    st.caption("Click to run common queries - edit the QUERIES dict in dashboard.py to customize")
    
    for query_name, query_sql in QUERIES.items():
        with st.expander(query_name):
            st.code(query_sql, language="sql")
            
            if st.button(f"Run: {query_name}", key=f"run_{query_name}"):
                with st.spinner("Running..."):
                    result = run_query(query_sql)
                
                if not result.empty:
                    st.success(f"✓ {len(result)} rows")
                    st.dataframe(result, use_container_width=True, hide_index=True)
                else:
                    st.warning("No results - table may not exist or is empty")


# Footer
st.markdown("---")
st.caption(f"LETZ Data Dashboard v1.1 • Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

