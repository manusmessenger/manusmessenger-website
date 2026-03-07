import eventlet
eventlet.monkey_patch()

from flask import Flask, request, jsonify, send_file, render_template
from flask_socketio import SocketIO, emit, join_room, leave_room
from datetime import datetime, timedelta
import uuid
from collections import defaultdict
import json
import os
import re
import base64
from werkzeug.utils import secure_filename
import hashlib
from functools import wraps
import threading
import time
from supabase import create_client, Client # type: ignore

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-change-this-in-production')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['ALLOWED_EXTENSIONS'] = {'png', 'jpg', 'jpeg', 'gif', 'mp4', 'mov', 'avi', 'pdf', 'doc', 'docx', 'txt', 'zip', 'rar'}

# ==================== SUPABASE CONNECTION ====================
SUPABASE_URL        = os.environ.get('SUPABASE_URL', '')
SUPABASE_SECRET_KEY = os.environ.get('SUPABASE_SECRET_KEY', '')

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
print("✅ Supabase connected")

# ==================== ADMIN CREDENTIALS ====================
ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'abhishek')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'abhishek2630@')
ADMIN_TOKEN    = None

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# Create uploads directory if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ==================== IN-MEMORY RUNTIME STATE ====================
# These are NOT persistent — they track live connection state only.
# All permanent data (users, messages, stories) lives in Supabase.
user_sessions    = {}                          # sid -> user_id
online_users     = set()                       # user_ids currently connected
user_to_sid      = {}                          # user_id -> [sids]
user_last_seen   = {}                          # user_id -> ISO string (runtime cache)
user_last_active = {}                          # user_id -> datetime (heartbeat)
user_active_tabs = defaultdict(int)            # user_id -> tab count
story_views      = defaultdict(set)            # story_id -> set of viewer_ids
unread_counts    = defaultdict(lambda: defaultdict(int))
user_chats       = defaultdict(list)
admin_sessions   = set()
admin_activity   = []

# Runtime caches (populated from Supabase on demand)
users        = {}               # user_id -> user dict
messages     = defaultdict(list) # room_id -> [msg dicts]
user_stories = defaultdict(list) # user_id -> [story dicts]

# ==================== DB HELPERS ====================

def db_get_all_users():
    """Load all users from Supabase into the runtime cache."""
    try:
        res = supabase.table('users').select('*').execute()
        users.clear()
        for row in res.data:
            uid = row['id']
            users[uid] = {
                'username':      row['username'],
                'email':         row.get('email', ''),
                'created_at':    row.get('created_at', datetime.utcnow().isoformat()),
                'password_hash': row.get('password', ''),
                'status':        'suspended' if row.get('suspended') else 'offline',
                'suspended':     row.get('suspended', False),
            }
        print(f"✅ Loaded {len(users)} users from Supabase")
    except Exception as e:
        print(f"❌ db_get_all_users error: {e}")

def db_get_user_by_id(user_id):
    """Fetch a single user row from Supabase."""
    try:
        res = supabase.table('users').select('*').eq('id', user_id).execute()
        return res.data[0] if res.data else None
    except Exception as e:
        print(f"❌ db_get_user_by_id error: {e}")
        return None

def db_get_messages_for_room(uid1, uid2):
    """Fetch all messages between two users from Supabase."""
    try:
        # Simpler query: fetch all messages where sender is uid1 OR uid2,
        # then filter in Python to only keep messages between these two users.
        res = supabase.table('messages').select('*')\
            .or_(f'sender.eq.{uid1},sender.eq.{uid2}')\
            .order('timestamp', desc=False)\
            .execute()
        result = []
        for row in res.data:
            # Python-side filter: only messages strictly between uid1 and uid2
            sender   = row['sender']
            receiver = row['receiver']
            if not ((sender == uid1 and receiver == uid2) or
                    (sender == uid2 and receiver == uid1)):
                continue
            msg = {
                'id':        row['id'],
                'sender':    sender,
                'receiver':  receiver,
                'type':      row.get('message_type', 'text'),
                'text':      row.get('message', ''),
                'timestamp': row.get('timestamp', datetime.utcnow().isoformat()),
                'read':      row.get('read', False),
            }
            if row.get('file_data'):
                try:
                    msg['file_data'] = json.loads(row['file_data']) if isinstance(row['file_data'], str) else row['file_data']
                except Exception:
                    pass
            result.append(msg)
        return result
    except Exception as e:
        print(f"❌ db_get_messages_for_room error: {e}")
        return []

def db_insert_message(msg):
    """Persist a message to Supabase. Returns the Supabase-generated UUID."""
    try:
        row = {
            'sender':       msg['sender'],
            'receiver':     msg['receiver'],
            'message_type': msg.get('type', 'text'),
            'message':      msg.get('text', ''),
            'timestamp':    msg['timestamp'],
            'read':         msg.get('read', False),
        }
        if msg.get('file_data'):
            row['file_data'] = json.dumps(msg['file_data'])
        res = supabase.table('messages').insert(row).execute()
        # Return the UUID Supabase generated so the runtime msg can be updated
        return res.data[0]['id'] if res.data else None
    except Exception as e:
        print(f"❌ db_insert_message error: {e}")
        return None

def db_get_active_stories():
    """Load all stories from the last 24 hours from Supabase into runtime cache."""
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        res = supabase.table('stories').select('*').gte('created_at', cutoff).order('created_at', desc=False).execute()
        user_stories.clear()
        for row in res.data:
            uid = next((u for u, ud in users.items() if ud['username'] == row['username']), None)
            if not uid:
                continue
            story = {
                'id':        row['id'],
                'user_id':   uid,
                'file_data': {
                    'url':       row['story_url'],
                    'filename':  row.get('filename', ''),
                    'file_type': row.get('file_type', 'image'),
                },
                'type':      row.get('file_type', 'image'),
                'timestamp': row.get('created_at', datetime.utcnow().isoformat()),
                'view_count': 0,
            }
            user_stories[uid].append(story)
        print(f"✅ Loaded stories for {len(user_stories)} users from Supabase")
    except Exception as e:
        print(f"❌ db_get_active_stories error: {e}")

def db_insert_story(uid, story):
    """Persist a story to Supabase. Returns the Supabase-generated UUID."""
    try:
        username = users.get(uid, {}).get('username', '')
        file_url = story['file_data']['url'] if story.get('file_data') else ''
        res = supabase.table('stories').insert({
            'username':   username,
            'story_url':  file_url,
            'file_type':  story.get('type', 'image'),
            'filename':   story['file_data'].get('filename', '') if story.get('file_data') else '',
            'created_at': story['timestamp'],
        }).execute()
        return res.data[0]['id'] if res.data else None
    except Exception as e:
        print(f"❌ db_insert_story error: {e}")
        return None

def db_insert_file(uid, file_data):
    """Persist a file record to Supabase. Supabase auto-generates the UUID."""
    try:
        username = users.get(uid, {}).get('username', '')
        supabase.table('files').insert({
            'username':   username,
            'file_url':   file_data.get('url', ''),
            'created_at': datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        print(f"❌ db_insert_file error: {e}")

def db_get_user_message_count(user_id):
    try:
        res = supabase.table('messages').select('id', count='exact')\
            .or_(f'sender.eq.{user_id},receiver.eq.{user_id}').execute()
        return res.count or 0
    except Exception:
        return 0

def db_get_user_story_count(user_id):
    try:
        username = users.get(user_id, {}).get('username', '')
        if not username:
            return 0
        res = supabase.table('stories').select('id', count='exact').eq('username', username).execute()
        return res.count or 0
    except Exception:
        return 0

# ==================== STARTUP ====================
db_get_all_users()

# ==================== BACKGROUND TASK ====================

def check_inactive_users():
    while True:
        try:
            time.sleep(30)
            current_time = datetime.utcnow()
            users_to_mark_offline = []
            for user_id in list(online_users):
                if users.get(user_id, {}).get('status') == 'suspended':
                    continue
                last_active = user_last_active.get(user_id)
                if last_active:
                    if isinstance(last_active, str):
                        last_active = datetime.fromisoformat(last_active)
                    if (current_time - last_active).total_seconds() > 300:
                        users_to_mark_offline.append(user_id)
            for user_id in users_to_mark_offline:
                if user_id in online_users:
                    online_users.discard(user_id)
                    user_last_seen[user_id] = datetime.utcnow().isoformat()
                    if user_id in users:
                        users[user_id]['status'] = 'offline'
                    if user_id in user_to_sid:
                        del user_to_sid[user_id]
                    for admin_sid in admin_sessions:
                        socketio.emit('user_offline', {
                            'user_id':   user_id,
                            'username':  users.get(user_id, {}).get('username', 'Unknown'),
                            'last_seen': user_last_seen[user_id]
                        }, room=admin_sid)
        except Exception as e:
            print(f"❌ Inactive checker error: {e}")

inactive_checker_thread = threading.Thread(target=check_inactive_users, daemon=True)
inactive_checker_thread.start()
print("🔄 Background inactive user checker started")

# ==================== UTILITY FUNCTIONS ====================

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def generate_admin_token():
    return hashlib.sha256(f"{ADMIN_USERNAME}{datetime.utcnow().isoformat()}".encode()).hexdigest()

def require_admin(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Unauthorized'}), 401
        token = auth_header.split(' ')[1]
        if token != ADMIN_TOKEN:
            return jsonify({'error': 'Invalid token'}), 401
        return f(*args, **kwargs)
    return decorated_function

def log_admin_activity(action, details=''):
    admin_activity.append({
        'timestamp': datetime.utcnow().isoformat(),
        'action':    action,
        'details':   details
    })
    if len(admin_activity) > 100:
        admin_activity.pop(0)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def get_room_id(uid1, uid2):
    return "-".join(sorted([uid1, uid2]))

def update_user_chat_list(user_id, other_user_id, timestamp=None):
    if not timestamp:
        timestamp = datetime.utcnow()
    user_chats[user_id] = [(pid, t) for (pid, t) in user_chats[user_id] if pid != other_user_id]
    user_chats[user_id].insert(0, (other_user_id, timestamp))
    if len(user_chats[user_id]) > 50:
        user_chats[user_id] = user_chats[user_id][:50]

def mark_messages_as_read(user_id, other_user_id):
    unread_counts[user_id][other_user_id] = 0
    room = get_room_id(user_id, other_user_id)
    for msg in messages[room]:
        if msg['receiver'] == user_id and msg['sender'] == other_user_id:
            msg['read'] = True
    try:
        supabase.table('messages')\
            .update({'read': True})\
            .eq('sender', other_user_id)\
            .eq('receiver', user_id)\
            .execute()
    except Exception as e:
        print(f"❌ mark_messages_as_read DB error: {e}")

def increment_unread_count(receiver_id, sender_id):
    unread_counts[receiver_id][sender_id] += 1
    update_user_chat_list(receiver_id, sender_id)

def get_all_users_for_user(user_id):
    all_users_list = []
    for other_id, user_data in users.items():
        if other_id == user_id or user_data.get('status') == 'suspended' or user_data.get('suspended'):
            continue
        last_msg_time   = None
        last_msg_preview = "No messages yet"
        unread_count    = unread_counts[user_id].get(other_id, 0)
        room = get_room_id(user_id, other_id)
        if messages[room]:
            last_msg = messages[room][-1]
            last_msg_time = datetime.fromisoformat(last_msg['timestamp'])
            if last_msg.get('type') == 'text':
                prefix = "You: " if last_msg['sender'] == user_id else ""
                last_msg_preview = prefix + last_msg['text'][:30] + ("..." if len(last_msg['text']) > 30 else "")
            elif last_msg.get('type') == 'image':
                last_msg_preview = "📷 Image" if last_msg['sender'] != user_id else "📷 You sent an image"
            elif last_msg.get('type') == 'video':
                last_msg_preview = "🎥 Video" if last_msg['sender'] != user_id else "🎥 You sent a video"
            elif last_msg.get('type') == 'file':
                last_msg_preview = "📎 File" if last_msg['sender'] != user_id else "📎 You sent a file"
            elif last_msg.get('type') == 'emoji':
                last_msg_preview = last_msg.get('text', '😀') if last_msg['sender'] != user_id else f"You: {last_msg.get('text', '😀')}"
        try:
            current_user_time = datetime.fromisoformat(users[user_id]['created_at'])
            other_user_time   = datetime.fromisoformat(user_data['created_at'])
            is_new_user = other_user_time > current_user_time
        except Exception:
            is_new_user = False
        all_users_list.append({
            'user_id':           other_id,
            'username':          user_data['username'],
            'is_online':         other_id in online_users,
            'unread_count':      unread_count,
            'last_message_time': last_msg_time.isoformat() if last_msg_time else None,
            'last_message':      last_msg_preview,
            'is_new_user':       is_new_user,
            'has_chatted':       len(messages[room]) > 0,
            'last_seen':         user_last_seen.get(other_id)
        })
    all_users_list.sort(key=lambda x: (
        -x['has_chatted'],
        -x['unread_count'],
        -x['is_online'],
        x['last_message_time'] if x['last_message_time'] else ''
    ), reverse=True)
    return all_users_list

def get_user_status(user_id):
    if user_id not in users:
        return 'offline'
    if users[user_id].get('suspended') or users[user_id].get('status') == 'suspended':
        return 'suspended'
    if user_id not in online_users:
        return 'offline'
    if user_id not in user_to_sid or not user_to_sid[user_id]:
        return 'offline'
    if user_active_tabs.get(user_id, 0) == 0:
        return 'away'
    return 'online'

def update_user_status(user_id, status):
    for other_user_id in users.keys():
        if other_user_id != user_id:
            room = get_room_id(user_id, other_user_id)
            socketio.emit('user_status_change', {
                'user_id':   user_id,
                'status':    status,
                'last_seen': user_last_seen.get(user_id)
            }, room=room)
    for admin_sid in admin_sessions:
        socketio.emit('user_status_update', {
            'user_id':   user_id,
            'status':    status,
            'last_seen': user_last_seen.get(user_id)
        }, room=admin_sid)

def notify_new_user_to_all(new_user_id, new_username):
    new_user_info = {
        'user_id':     new_user_id,
        'username':    new_username,
        'status':      get_user_status(new_user_id),
        'is_new_user': True,
        'last_seen':   user_last_seen.get(new_user_id)
    }
    for user_id in online_users:
        if user_id != new_user_id:
            for sid in user_to_sid.get(user_id, []):
                socketio.emit('user_discovered', new_user_info, room=sid)

def notify_existing_users_to_new_user(new_user_id):
    existing_users = []
    for user_id, user_data in users.items():
        if user_id != new_user_id and not user_data.get('suspended') and user_data.get('status') != 'suspended':
            existing_users.append({
                'user_id':     user_id,
                'username':    user_data['username'],
                'status':      get_user_status(user_id),
                'is_new_user': False,
                'last_seen':   user_last_seen.get(user_id)
            })
    for sid in user_to_sid.get(new_user_id, []):
        socketio.emit('existing_users', {'users': existing_users}, room=sid)

def broadcast_story_to_all_users(story_data, exclude_user_id=None):
    for user_id in list(online_users):
        if user_id == exclude_user_id:
            continue
        for sid in user_to_sid.get(user_id, []):
            try:
                socketio.emit('story_added', story_data, room=sid)
            except Exception as e:
                print(f"❌ story broadcast error: {e}")
    for admin_sid in admin_sessions:
        try:
            socketio.emit('admin_new_story', {
                'user_id':  story_data['user_id'],
                'username': users.get(story_data['user_id'], {}).get('username', 'Unknown'),
                'story':    story_data['story']
            }, room=admin_sid)
        except Exception as e:
            print(f"❌ admin story broadcast error: {e}")

def notify_story_owner_view_count(story_id, story_owner_id):
    view_count = len(story_views.get(story_id, set()))
    for sid in user_to_sid.get(story_owner_id, []):
        socketio.emit('story_view_count_update', {
            'story_id':   story_id,
            'view_count': view_count
        }, room=sid)

def count_user_messages(user_id):
    return db_get_user_message_count(user_id)

def count_user_stories(user_id):
    return db_get_user_story_count(user_id)

# ==================== ADMIN ROUTES ====================

@app.route('/admin')
def admin_panel():
    return render_template('admin.html')

@app.route('/admin/login', methods=['POST'])
def admin_login():
    global ADMIN_TOKEN
    data = request.json
    if data.get('username') == ADMIN_USERNAME and data.get('password') == ADMIN_PASSWORD:
        ADMIN_TOKEN = generate_admin_token()
        log_admin_activity('Admin Login', 'Admin logged in')
        return jsonify({'token': ADMIN_TOKEN})
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/admin/users', methods=['GET'])
@require_admin
def get_all_users_admin():
    db_get_all_users()
    users_list = []
    for user_id, user_data in users.items():
        users_list.append({
            'user_id':       user_id,
            'username':      user_data['username'],
            'email':         user_data.get('email', ''),
            'created_at':    user_data['created_at'],
            'status':        get_user_status(user_id),
            'last_seen':     user_last_seen.get(user_id),
            'message_count': count_user_messages(user_id),
            'story_count':   count_user_stories(user_id)
        })
    users_list.sort(key=lambda x: x['created_at'], reverse=True)
    return jsonify({'users': users_list})

@app.route('/admin/stats', methods=['GET'])
@require_admin
def get_admin_stats():
    try:
        msg_res        = supabase.table('messages').select('*').execute()
        total_messages = len(msg_res.data)
        all_msgs       = msg_res.data
    except Exception:
        total_messages = 0
        all_msgs       = []
    try:
        story_res        = supabase.table('stories').select('*').execute()
        total_stories    = len(story_res.data)
        all_stories_raw  = story_res.data
    except Exception:
        total_stories   = 0
        all_stories_raw = []
    actual_online_users = sum(
        1 for uid in online_users
        if not users.get(uid, {}).get('suspended') and uid in user_to_sid and user_to_sid[uid]
    )
    formatted_msgs = []
    for row in all_msgs:
        msg = {
            'id':        row['id'],
            'sender':    row['sender'],
            'receiver':  row['receiver'],
            'type':      row.get('message_type', 'text'),
            'text':      row.get('message', ''),
            'timestamp': row.get('timestamp', ''),
            'read':      row.get('read', False),
        }
        if row.get('file_data'):
            try:
                msg['file_data'] = json.loads(row['file_data']) if isinstance(row['file_data'], str) else row['file_data']
            except Exception:
                pass
        formatted_msgs.append(msg)
    formatted_stories = {}
    for row in all_stories_raw:
        uid = next((u for u, ud in users.items() if ud['username'] == row['username']), row['username'])
        if uid not in formatted_stories:
            formatted_stories[uid] = []
        formatted_stories[uid].append({
            'id':        row['id'],
            'user_id':   uid,
            'file_data': {'url': row['story_url'], 'file_type': row.get('file_type', 'image')},
            'type':      row.get('file_type', 'image'),
            'timestamp': row.get('created_at', ''),
        })
    return jsonify({
        'total_users':    len(users),
        'online_users':   actual_online_users,
        'total_messages': total_messages,
        'total_stories':  total_stories,
        'messages':       formatted_msgs,
        'stories':        formatted_stories,
    })

@app.route('/admin/user/<user_id>', methods=['GET'])
@require_admin
def get_user_details_admin(user_id):
    if user_id not in users:
        db_get_all_users()
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    user_data = users[user_id]
    try:
        sent_res = supabase.table('messages').select('receiver').eq('sender', user_id).execute()
        recv_res = supabase.table('messages').select('sender').eq('receiver', user_id).execute()
        partners = set()
        for r in sent_res.data:
            partners.add(r['receiver'])
        for r in recv_res.data:
            partners.add(r['sender'])
        active_chats = len(partners)
    except Exception:
        active_chats = 0
    return jsonify({
        'user': {
            'user_id':       user_id,
            'username':      user_data['username'],
            'email':         user_data.get('email', ''),
            'created_at':    user_data['created_at'],
            'status':        get_user_status(user_id),
            'last_seen':     user_last_seen.get(user_id),
            'message_count': count_user_messages(user_id),
            'story_count':   count_user_stories(user_id),
            'active_chats':  active_chats
        }
    })

@app.route('/admin/user/<user_id>/stories', methods=['GET'])
@require_admin
def get_user_stories_admin(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    username = users[user_id]['username']
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        res    = supabase.table('stories').select('*').eq('username', username).gte('created_at', cutoff).execute()
        active_stories = []
        for row in res.data:
            active_stories.append({
                'id':        row['id'],
                'user_id':   user_id,
                'file_data': {'url': row['story_url'], 'file_type': row.get('file_type', 'image')},
                'type':      row.get('file_type', 'image'),
                'timestamp': row.get('created_at', ''),
                'view_count': len(story_views.get(row['id'], set())),
                'viewers': [
                    {'user_id': vid, 'username': users.get(vid, {}).get('username', 'Unknown')}
                    for vid in story_views.get(row['id'], set())
                ]
            })
    except Exception as e:
        print(f"❌ get_user_stories_admin error: {e}")
        active_stories = []
    return jsonify({'user_id': user_id, 'username': username, 'stories': active_stories})

@app.route('/admin/stories/all', methods=['GET'])
@require_admin
def get_all_stories():
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        res    = supabase.table('stories').select('*').gte('created_at', cutoff).execute()
        all_active_stories = {}
        for row in res.data:
            uid = next((u for u, ud in users.items() if ud['username'] == row['username']), None)
            if not uid:
                continue
            if uid not in all_active_stories:
                all_active_stories[uid] = {'username': row['username'], 'stories': []}
            all_active_stories[uid]['stories'].append({
                'id':        row['id'],
                'user_id':   uid,
                'file_data': {'url': row['story_url'], 'file_type': row.get('file_type', 'image')},
                'type':      row.get('file_type', 'image'),
                'timestamp': row.get('created_at', ''),
                'view_count': len(story_views.get(row['id'], set())),
            })
    except Exception as e:
        print(f"❌ get_all_stories error: {e}")
        all_active_stories = {}
    return jsonify({'stories': all_active_stories})

@app.route('/admin/user/<user_id>/reset-password', methods=['POST'])
@require_admin
def admin_reset_password(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    data         = request.json
    new_password = data.get('new_password')
    if not new_password or len(new_password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    new_hash = hash_password(new_password)
    try:
        supabase.table('users').update({'password': new_hash}).eq('id', user_id).execute()
        users[user_id]['password_hash'] = new_hash
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    log_admin_activity('Password Reset', f'Reset password for {users[user_id]["username"]} ({user_id})')
    return jsonify({'success': True})

@app.route('/admin/user/<user_id>/suspend', methods=['POST'])
@require_admin
def suspend_user(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    try:
        supabase.table('users').update({'suspended': True}).eq('id', user_id).execute()
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    users[user_id]['status']    = 'suspended'
    users[user_id]['suspended'] = True
    if user_id in user_to_sid:
        for sid in list(user_to_sid[user_id]):
            socketio.emit('account_suspended', {'message': 'Your account has been suspended by an administrator'}, room=sid)
        del user_to_sid[user_id]
    online_users.discard(user_id)
    user_last_seen[user_id] = datetime.utcnow().isoformat()
    for admin_sid in admin_sessions:
        socketio.emit('user_status_update', {'user_id': user_id, 'status': 'suspended', 'last_seen': user_last_seen[user_id]}, room=admin_sid)
    for other_user_id in list(online_users):
        if other_user_id != user_id:
            room = get_room_id(user_id, other_user_id)
            socketio.emit('user_status_change', {'user_id': user_id, 'status': 'suspended', 'last_seen': user_last_seen[user_id]}, room=room)
    log_admin_activity('User Suspended', f'Suspended {users[user_id]["username"]} ({user_id})')
    return jsonify({'success': True, 'status': 'suspended'})

@app.route('/admin/user/<user_id>/unsuspend', methods=['POST'])
@require_admin
def unsuspend_user(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    try:
        supabase.table('users').update({'suspended': False}).eq('id', user_id).execute()
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    users[user_id]['status']    = 'offline'
    users[user_id]['suspended'] = False
    for admin_sid in admin_sessions:
        socketio.emit('user_status_update', {'user_id': user_id, 'status': 'offline', 'last_seen': user_last_seen.get(user_id)}, room=admin_sid)
    log_admin_activity('User Unsuspended', f'Unsuspended {users[user_id]["username"]} ({user_id})')
    return jsonify({'success': True, 'status': 'offline'})

@app.route('/admin/user/<user_id>', methods=['DELETE'])
@require_admin
def delete_user(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    username = users[user_id]['username']
    try:
        supabase.table('messages').delete().or_(f'sender.eq.{user_id},receiver.eq.{user_id}').execute()
        supabase.table('stories').delete().eq('username', username).execute()
        supabase.table('files').delete().eq('username', username).execute()
        supabase.table('users').delete().eq('id', user_id).execute()
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    had_stories = user_id in user_stories
    if user_id in user_stories:
        for story in user_stories[user_id]:
            story_views.pop(story.get('id', ''), None)
        del user_stories[user_id]
    rooms_to_delete = [r for r in list(messages.keys()) if user_id in r]
    for r in rooms_to_delete:
        del messages[r]
    online_users.discard(user_id)
    if user_id in user_to_sid:
        for sid in user_to_sid[user_id].copy():
            socketio.emit('account_deleted', {'message': 'Your account has been deleted by an administrator'}, room=sid)
        del user_to_sid[user_id]
    del users[user_id]
    if had_stories:
        for online_uid in list(online_users):
            for sid in user_to_sid.get(online_uid, []):
                socketio.emit('user_stories_removed', {'user_id': user_id}, room=sid)
    log_admin_activity('User Deleted', f'Deleted {username} ({user_id})')
    return jsonify({'success': True})

@app.route('/admin/analytics', methods=['GET'])
@require_admin
def get_analytics():
    period = request.args.get('period', 'daily')
    now    = datetime.utcnow()
    data   = []
    labels = []
    if period == 'daily':
        for i in range(6, -1, -1):
            date     = now - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            active_count = sum(1 for ls in user_last_seen.values() if ls and ls[:10] == date_str)
            data.append(active_count)
            labels.append(date.strftime('%a'))
    elif period == 'weekly':
        for i in range(3, -1, -1):
            start_date   = now - timedelta(weeks=i+1)
            end_date     = now - timedelta(weeks=i)
            active_count = sum(
                1 for ls in user_last_seen.values()
                if ls and start_date <= datetime.fromisoformat(ls) <= end_date
            )
            data.append(active_count)
            labels.append(f"Week {4-i}")
    elif period == 'monthly':
        for i in range(5, -1, -1):
            month_date   = now - timedelta(days=i*30)
            active_count = sum(
                1 for ls in user_last_seen.values()
                if ls and datetime.fromisoformat(ls).month == month_date.month
                       and datetime.fromisoformat(ls).year  == month_date.year
            )
            data.append(active_count)
            labels.append(month_date.strftime('%b'))
    return jsonify({'data': data, 'labels': labels})

@app.route('/admin/analytics/custom', methods=['GET'])
@require_admin
def get_custom_analytics():
    date_from = request.args.get('from')
    date_to   = request.args.get('to')
    if not date_from or not date_to:
        return jsonify({'error': 'Missing date range'}), 400
    try:
        start_date = datetime.fromisoformat(date_from)
        end_date   = datetime.fromisoformat(date_to).replace(hour=23, minute=59, second=59)
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400
    try:
        new_users_res   = supabase.table('users').select('created_at').gte('created_at', start_date.isoformat()).lte('created_at', end_date.isoformat()).execute()
        msg_res         = supabase.table('messages').select('timestamp').gte('timestamp', start_date.isoformat()).lte('timestamp', end_date.isoformat()).execute()
        story_res       = supabase.table('stories').select('created_at').gte('created_at', start_date.isoformat()).lte('created_at', end_date.isoformat()).execute()
        new_users_count = len(new_users_res.data)
        message_count   = len(msg_res.data)
        story_count     = len(story_res.data)
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    active_users = sum(
        1 for ls in user_last_seen.values()
        if ls and start_date <= datetime.fromisoformat(ls) <= end_date
    )
    return jsonify({'active_users': active_users, 'new_users': new_users_count, 'messages': message_count, 'stories': story_count})

@app.route('/admin/activity/recent', methods=['GET'])
@require_admin
def get_recent_activity():
    activities = []
    try:
        msg_res = supabase.table('messages').select('*').order('timestamp', desc=True).limit(20).execute()
        for row in msg_res.data:
            sender_id = row['sender']
            username  = users.get(sender_id, {}).get('username', 'Unknown')
            activities.append({
                'type':      'message',
                'username':  username,
                'action':    f"sent a {row.get('message_type','text')}",
                'timestamp': row.get('timestamp', '')
            })
    except Exception as e:
        print(f"❌ recent_activity messages error: {e}")
    try:
        story_res = supabase.table('stories').select('*').order('created_at', desc=True).limit(10).execute()
        for row in story_res.data:
            activities.append({
                'type':      'story',
                'username':  row['username'],
                'action':    'posted a story',
                'timestamp': row.get('created_at', '')
            })
    except Exception as e:
        print(f"❌ recent_activity stories error: {e}")
    activities.sort(key=lambda x: x['timestamp'], reverse=True)
    return jsonify({'activities': activities[:30]})

@app.route('/admin/chats/recent', methods=['GET'])
@require_admin
def get_recent_chats():
    try:
        res   = supabase.table('messages').select('*').order('timestamp', desc=True).limit(100).execute()
        chats = []
        for row in res.data:
            msg = {
                'id':        row['id'],
                'sender':    row['sender'],
                'receiver':  row['receiver'],
                'type':      row.get('message_type', 'text'),
                'text':      row.get('message', ''),
                'timestamp': row.get('timestamp', ''),
                'read':      row.get('read', False),
            }
            if row.get('file_data'):
                try:
                    msg['file_data'] = json.loads(row['file_data']) if isinstance(row['file_data'], str) else row['file_data']
                except Exception:
                    pass
            chats.append(msg)
    except Exception as e:
        print(f"❌ get_recent_chats error: {e}")
        chats = []
    return jsonify({'chats': chats})

# ==================== REGULAR ROUTES ====================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/register', methods=['POST'])
def register():
    data     = request.json
    username = data.get('username', '').strip()
    email    = data.get('email', '').strip().lower()
    password = data.get('password', '')
    if not username or len(username) < 2:
        return jsonify({'error': 'Username must be at least 2 characters'}), 400
    if not email or not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$', email):
        return jsonify({'error': 'Valid email address is required (e.g. name@example.com)'}), 400
    if password and len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    try:
        uname_res = supabase.table('users').select('id').ilike('username', username).execute()
        if uname_res.data:
            return jsonify({'error': 'Username already taken'}), 409
        email_res = supabase.table('users').select('id').eq('email', email).execute()
        if email_res.data:
            return jsonify({'error': 'Email already registered'}), 409
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    user_id    = None
    created_at = datetime.utcnow().isoformat()
    pw_hash    = hash_password(password)
    try:
        res = supabase.table('users').insert({
            'username':   username,
            'email':      email,
            'password':   pw_hash,
            'suspended':  False,
            'created_at': created_at,
        }).execute()
        # Read the UUID that Supabase auto-generated
        user_id = res.data[0]['id']
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    users[user_id] = {
        'username':      username,
        'email':         email,
        'created_at':    created_at,
        'password_hash': pw_hash,
        'status':        'offline',
        'suspended':     False,
    }
    notify_new_user_to_all(user_id, username)
    return jsonify({'user_id': user_id, 'username': username, 'email': email, 'created_at': created_at})

@app.route('/login', methods=['POST'])
def login():
    data     = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '')
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    try:
        res = supabase.table('users').select('*').or_(
            f'username.ilike.{username},email.ilike.{username}'
        ).execute()
        if not res.data:
            return jsonify({'error': 'User not found'}), 404
        row = res.data[0]
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    if row.get('suspended'):
        return jsonify({'error': 'Account suspended. Contact administrator.'}), 403
    if row.get('password') != hash_password(password):
        return jsonify({'error': 'Invalid password'}), 401
    user_id = row['id']
    users[user_id] = {
        'username':      row['username'],
        'email':         row.get('email', ''),
        'created_at':    row.get('created_at', datetime.utcnow().isoformat()),
        'password_hash': row.get('password', ''),
        'status':        'offline',
        'suspended':     row.get('suspended', False),
    }
    return jsonify({
        'user_id':    user_id,
        'username':   row['username'],
        'email':      row.get('email'),
        'created_at': row.get('created_at')
    })

@app.route('/change_password', methods=['POST'])
def change_password():
    data         = request.json
    user_id      = data.get('user_id')
    new_password = data.get('new_password')
    if not user_id or user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    if not new_password or len(new_password) < 6:
        return jsonify({'error': 'New password must be at least 6 characters'}), 400
    new_hash = hash_password(new_password)
    try:
        supabase.table('users').update({'password': new_hash}).eq('id', user_id).execute()
        users[user_id]['password_hash'] = new_hash
    except Exception as e:
        return jsonify({'error': f'DB error: {e}'}), 500
    return jsonify({'success': True, 'message': 'Password changed successfully'})

@app.route('/reset_password', methods=['POST'])
def reset_password():
    data         = request.json
    username     = data.get('username', '').strip()
    user_id      = data.get('user_id', '').strip()
    new_password = data.get('new_password')
    if not username:
        return jsonify({'error': 'Username is required'}), 400
    if not user_id:
        return jsonify({'error': 'User ID is required'}), 400
    if not new_password or len(new_password) < 6:
        return jsonify({'error': 'New password must be at least 6 characters'}), 400
    if user_id in users and users[user_id]['username'].lower() == username.lower():
        new_hash = hash_password(new_password)
        try:
            supabase.table('users').update({'password': new_hash}).eq('id', user_id).execute()
            users[user_id]['password_hash'] = new_hash
        except Exception as e:
            return jsonify({'error': f'DB error: {e}'}), 500
        return jsonify({'success': True, 'message': 'Password reset successfully'})
    return jsonify({'error': 'Username and User ID do not match.'}), 404

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    if file and allowed_file(file.filename):
        filename       = secure_filename(file.filename)
        file_id        = str(uuid.uuid4())
        file_extension = filename.rsplit('.', 1)[1].lower()
        saved_filename = f"{file_id}.{file_extension}"
        filepath       = os.path.join(app.config['UPLOAD_FOLDER'], saved_filename)
        # TODO: Render deletes local files on restart. In production this should be
        # replaced with Supabase Storage upload using supabase.storage.from_("bucket").upload()
        file.save(filepath)
        file_type = 'image' if file_extension in ['png', 'jpg', 'jpeg', 'gif'] else \
                    'video' if file_extension in ['mp4', 'mov', 'avi'] else 'file'
        file_url    = f'/uploads/{saved_filename}'
        uploader_id = request.form.get('user_id')
        if uploader_id and uploader_id in users:
            try:
                db_insert_file(uploader_id, {'url': file_url})
            except Exception:
                pass
        return jsonify({'file_id': file_id, 'filename': filename, 'file_type': file_type, 'url': file_url})
    return jsonify({'error': 'File type not allowed'}), 400

@app.route('/health')
def health():
    return {'status': 'ok'}

@app.route('/uploads/<filename>')
def serve_file(filename):
    return send_file(os.path.join(app.config['UPLOAD_FOLDER'], filename))

@app.route('/users')
def get_all_users():
    exclude_id = request.args.get('exclude')
    user_list  = []
    for uid, data in users.items():
        if uid != exclude_id and not data.get('suspended') and data.get('status') != 'suspended':
            user_list.append({
                'user_id':   uid,
                'username':  data['username'],
                'status':    get_user_status(uid),
                'last_seen': user_last_seen.get(uid)
            })
    return jsonify(user_list)

@app.route('/chats/<user_id>')
def get_user_chats(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    return jsonify(get_all_users_for_user(user_id))

@app.route('/unread_count/<user_id>')
def get_unread_count(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    return jsonify({'total_unread': sum(unread_counts[user_id].values())})

@app.route('/mark_as_read/<user_id>/<other_user_id>', methods=['POST'])
def mark_chat_as_read(user_id, other_user_id):
    if user_id not in users or other_user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    mark_messages_as_read(user_id, other_user_id)
    return jsonify({'success': True})

@app.route('/search/<query>')
def search_users(query):
    query_lower = query.lower()
    matches = []
    for uid, data in users.items():
        if not data.get('suspended') and data.get('status') != 'suspended':
            if query_lower in uid.lower() or query_lower in data['username'].lower():
                matches.append({
                    'user_id':   uid,
                    'username':  data['username'],
                    'status':    get_user_status(uid),
                    'last_seen': user_last_seen.get(uid)
                })
    return jsonify(matches[:20])

@app.route('/get_stories', methods=['GET'])
def get_stories():
    db_get_active_stories()
    return jsonify({'stories': {uid: stories for uid, stories in user_stories.items()}})

# ==================== SOCKET.IO EVENTS ====================

@socketio.on('connect')
def handle_connect():
    print(f'🔗 New client connected: {request.sid}')

@socketio.on('admin_connected')
def handle_admin_connected(data):
    token = data.get('token')
    if token == ADMIN_TOKEN:
        admin_sessions.add(request.sid)
        print(f"🛡️ Admin connected: {request.sid}")
        db_get_active_stories()
        active_stories = {
            uid: [dict(s, view_count=len(story_views.get(s['id'], set()))) for s in stories]
            for uid, stories in user_stories.items()
        }
        socketio.emit('admin_stories_update', {
            'stories': active_stories,
            'users':   {uid: users[uid]['username'] for uid in users}
        }, room=request.sid)

@socketio.on('set_user')
def handle_set_user(data):
    user_id = data.get('user_id')
    # Refresh from DB if missing from memory (server restart scenario)
    if user_id and user_id not in users:
        row = db_get_user_by_id(user_id)
        if row:
            users[user_id] = {
                'username':      row['username'],
                'email':         row.get('email', ''),
                'created_at':    row.get('created_at', datetime.utcnow().isoformat()),
                'password_hash': row.get('password', ''),
                'status':        'suspended' if row.get('suspended') else 'offline',
                'suspended':     row.get('suspended', False),
            }
    if user_id and user_id in users:
        if users[user_id].get('suspended') or users[user_id].get('status') == 'suspended':
            emit('account_suspended', {'message': 'Your account has been suspended by an administrator'})
            return
        user_sessions[request.sid] = user_id
        if user_id not in user_to_sid:
            user_to_sid[user_id] = []
        user_to_sid[user_id].append(request.sid)
        user_active_tabs[user_id] = user_active_tabs.get(user_id, 0) + 1
        if user_id not in online_users:
            online_users.add(user_id)
            user_last_seen[user_id]   = datetime.utcnow().isoformat()
            user_last_active[user_id] = datetime.utcnow()
            users[user_id]['status']  = 'online'
            update_user_status(user_id, 'online')
            for admin_sid in admin_sessions:
                socketio.emit('user_online', {'user_id': user_id, 'username': users[user_id]['username']}, room=admin_sid)
        notify_existing_users_to_new_user(user_id)
        emit('chat_list_updated', {'chats': get_all_users_for_user(user_id)})
        emit('unread_count_updated', {'total_unread': sum(unread_counts[user_id].values())})
        # Load stories from DB
        db_get_active_stories()
        active_stories = {
            uid: [dict(s, view_count=len(story_views.get(s['id'], set()))) for s in stories]
            for uid, stories in user_stories.items()
        }
        views_dict = {sid: list(viewers) for sid, viewers in story_views.items()}
        emit('existing_stories', {'stories': active_stories, 'views': views_dict})
        emit('connection_established', {'user_id': user_id})

@socketio.on('user_active')
def handle_user_active(data):
    user_id = user_sessions.get(request.sid)
    if user_id:
        if users.get(user_id, {}).get('suspended') or users.get(user_id, {}).get('status') == 'suspended':
            emit('account_suspended', {'message': 'Your account has been suspended by an administrator'})
            return
        user_last_active[user_id] = datetime.utcnow()
        user_last_seen[user_id]   = datetime.utcnow().isoformat()
        if user_id not in online_users:
            online_users.add(user_id)
            if user_id in users:
                users[user_id]['status'] = 'online'
            for admin_sid in admin_sessions:
                socketio.emit('user_online', {'user_id': user_id, 'username': users.get(user_id, {}).get('username', 'Unknown')}, room=admin_sid)
        update_user_status(user_id, 'online')

@socketio.on('user_inactive')
def handle_user_inactive(data):
    user_id = user_sessions.get(request.sid)
    if user_id:
        user_last_seen[user_id] = datetime.utcnow().isoformat()

@socketio.on('user_going_offline')
def handle_user_going_offline(data):
    pass  # Handled by disconnect

@socketio.on('get_chat_list')
def handle_get_chat_list():
    user_id = user_sessions.get(request.sid)
    if user_id:
        emit('chat_list_updated', {'chats': get_all_users_for_user(user_id)})

@socketio.on('join_chat')
def handle_join_chat(data):
    user_id   = user_sessions.get(request.sid)
    target_id = data.get('target_id')
    if not user_id or target_id not in users:
        return
    room = get_room_id(user_id, target_id)
    join_room(room)
    # Load from DB if not in runtime cache
    if not messages[room]:
        messages[room] = db_get_messages_for_room(user_id, target_id)
    mark_messages_as_read(user_id, target_id)
    update_user_chat_list(user_id, target_id)
    emit('chat_history', {
        'messages': messages[room],
        'target_user': {
            'user_id':   target_id,
            'username':  users[target_id]['username'],
            'status':    get_user_status(target_id),
            'last_seen': user_last_seen.get(target_id)
        }
    })
    emit('chat_list_updated', {'chats': get_all_users_for_user(user_id)})
    emit('unread_count_updated', {'total_unread': sum(unread_counts[user_id].values())})
    socketio.emit('messages_read', {'reader_id': user_id}, room=room)

@socketio.on('send_message')
def handle_message(data):
    user_id      = user_sessions.get(request.sid)
    target_id    = data.get('target_id')
    text         = data.get('text', '').strip()
    message_type = data.get('type', 'text')
    file_data    = data.get('file_data')
    if not user_id or not target_id:
        return
    if message_type == 'text' and not text:
        return
    room = get_room_id(user_id, target_id)
    msg = {
        'id':        str(uuid.uuid4()),  # temporary local ID for runtime/emit
        'sender':    user_id,
        'receiver':  target_id,
        'type':      message_type,
        'text':      text,
        'timestamp': datetime.utcnow().isoformat(),
        'read':      False,
    }
    if message_type in ['image', 'video', 'file'] and file_data:
        msg['file_data'] = file_data
    # Persist to Supabase and update msg id with the real UUID
    db_id = db_insert_message(msg)
    if db_id:
        msg['id'] = db_id
    if message_type in ['image', 'video', 'file'] and file_data:
        try:
            db_insert_file(user_id, file_data)
        except Exception:
            pass
    # Update runtime cache
    messages[room].append(msg)
    if len(messages[room]) > 100:
        messages[room] = messages[room][-100:]
    timestamp = datetime.utcnow()
    update_user_chat_list(user_id, target_id, timestamp)
    update_user_chat_list(target_id, user_id, timestamp)
    increment_unread_count(target_id, user_id)
    socketio.emit('chat_list_updated', {'chats': get_all_users_for_user(user_id)}, room=request.sid)
    for sid in user_to_sid.get(target_id, []):
        socketio.emit('chat_list_updated', {'chats': get_all_users_for_user(target_id)}, room=sid)
        socketio.emit('unread_count_updated', {'total_unread': sum(unread_counts[target_id].values())}, room=sid)
    emit('new_message', msg, room=room)
    emit('message_delivered', {'message_id': msg['id']})

@socketio.on('typing')
def handle_typing(data):
    user_id   = user_sessions.get(request.sid)
    target_id = data.get('target_id')
    if user_id and target_id:
        room = get_room_id(user_id, target_id)
        emit('user_typing', {'user_id': user_id, 'is_typing': data.get('is_typing', False)}, room=room, include_self=False)

@socketio.on('add_story')
def handle_add_story(data):
    user_id = user_sessions.get(request.sid)
    if not user_id:
        return {'success': False, 'error': 'Not authenticated'}
    story_id = str(uuid.uuid4())  # temporary local ID
    story = {
        'id':         story_id,
        'user_id':    user_id,
        'file_data':  data.get('file_data'),
        'type':       data.get('type'),
        'timestamp':  datetime.utcnow().isoformat(),
        'view_count': 0,
    }
    if not story['file_data'] or not story['type']:
        return {'success': False, 'error': 'Invalid story data'}
    # Persist and replace local id with Supabase-generated UUID
    db_id = db_insert_story(user_id, story)
    if db_id:
        story['id'] = db_id
        story_id    = db_id
    user_stories[user_id].append(story)
    story_views[story_id] = set()
    if len(user_stories[user_id]) > 10:
        old_story = user_stories[user_id][0]
        story_views.pop(old_story['id'], None)
        user_stories[user_id] = user_stories[user_id][-10:]
    broadcast_story_to_all_users({'user_id': user_id, 'story': story}, exclude_user_id=user_id)
    emit('story_upload_success', {
        'story_id':   story_id,
        'story':      story,
        'view_count': 0,
        'message':    'Story uploaded and shared with everyone!'
    })
    return {'success': True, 'story_id': story_id}

@socketio.on('view_story')
def handle_view_story(data):
    story_id       = data.get('story_id')
    viewer_id      = data.get('viewer_id')
    story_owner_id = data.get('story_owner_id')
    if story_id and viewer_id:
        story_views[story_id].add(viewer_id)
        view_count = len(story_views[story_id])
        if story_owner_id and story_owner_id in user_to_sid:
            notify_story_owner_view_count(story_id, story_owner_id)
            viewer_username = users.get(viewer_id, {}).get('username', 'Unknown')
            for sid in user_to_sid[story_owner_id]:
                socketio.emit('story_view_update', {
                    'story_id':        story_id,
                    'viewer_id':       viewer_id,
                    'viewer_username': viewer_username,
                    'view_count':      view_count,
                }, room=sid)

@socketio.on('get_message_history')
def handle_get_message_history(data):
    user_id = data.get('user_id')
    if not user_id or user_id not in users:
        return
    try:
        res = supabase.table('messages').select('*')\
            .or_(f'sender.eq.{user_id},receiver.eq.{user_id}')\
            .order('timestamp', desc=True)\
            .limit(200)\
            .execute()
        user_messages = []
        for row in res.data:
            msg = {
                'id':        row['id'],
                'sender':    row['sender'],
                'receiver':  row['receiver'],
                'type':      row.get('message_type', 'text'),
                'text':      row.get('message', ''),
                'timestamp': row.get('timestamp', ''),
                'read':      row.get('read', False),
            }
            if row.get('file_data'):
                try:
                    msg['file_data'] = json.loads(row['file_data']) if isinstance(row['file_data'], str) else row['file_data']
                except Exception:
                    pass
            user_messages.append(msg)
    except Exception as e:
        print(f"❌ get_message_history error: {e}")
        user_messages = []
    emit('message_history', {'messages': user_messages})

@socketio.on('disconnect')
def handle_disconnect():
    if request.sid in admin_sessions:
        admin_sessions.discard(request.sid)
        print(f"🛡️ Admin disconnected: {request.sid}")
        return
    user_id = user_sessions.pop(request.sid, None)
    if user_id:
        if user_id in user_to_sid:
            if request.sid in user_to_sid[user_id]:
                user_to_sid[user_id].remove(request.sid)
            user_active_tabs[user_id] = max(0, user_active_tabs.get(user_id, 0) - 1)
            if not user_to_sid[user_id]:
                del user_to_sid[user_id]
                online_users.discard(user_id)
                user_last_seen[user_id] = datetime.utcnow().isoformat()
                if user_id in users and not users[user_id].get('suspended') and users[user_id].get('status') != 'suspended':
                    users[user_id]['status'] = 'offline'
                    update_user_status(user_id, 'offline')
                    for admin_sid in admin_sessions:
                        socketio.emit('user_offline', {
                            'user_id':   user_id,
                            'username':  users.get(user_id, {}).get('username', 'Unknown'),
                            'last_seen': user_last_seen[user_id]
                        }, room=admin_sid)

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 MANUS MESSENGER SERVER STARTING")
    print("="*60)
    print(f"\n📱 Main App:    http://localhost:5000")
    print(f"🛡️  Admin Panel: http://localhost:5000/admin")
    print(f"\n🔐 Admin: {ADMIN_USERNAME}")
    print(f"\n⚠️  Set SUPABASE_URL and SUPABASE_SECRET_KEY as env vars on Render!")
    print("="*60 + "\n")
    socketio.run(app, host='0.0.0.0', port=10000)