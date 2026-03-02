from flask import Flask, request, jsonify, send_file
from flask_socketio import SocketIO, emit, join_room
from datetime import datetime, timedelta
import uuid
from collections import defaultdict
import json
import os
import base64
from werkzeug.utils import secure_filename
import hashlib
from functools import wraps
import threading
import time

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-this-in-production'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['ALLOWED_EXTENSIONS'] = {'png', 'jpg', 'jpeg', 'gif', 'mp4', 'mov', 'avi', 'pdf', 'doc', 'docx', 'txt', 'zip', 'rar'}

# Admin credentials (In production, use environment variables and proper authentication)
ADMIN_USERNAME = 'myusername'
ADMIN_PASSWORD = 'MySecurePassword123'  # Change this in production!
ADMIN_TOKEN = None

socketio = SocketIO(app, cors_allowed_origins="*")

# Create uploads directory if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# In-memory storage
users = {}  # user_id -> {'username': name, 'created_at': timestamp, 'password_hash': hash, 'email': email, 'status': 'online/offline/suspended'}
user_sessions = {}  # sid -> user_id
online_users = set()
messages = defaultdict(list)  # room_id -> [messages]
user_to_sid = {}  # user_id -> [sids]
user_last_seen = {}  # user_id -> last_seen timestamp
user_last_active = {}  # user_id -> last active timestamp (for heartbeat tracking)
user_active_tabs = defaultdict(int)  # user_id -> active tab count
user_stories = defaultdict(list)  # user_id -> [story_objects]
story_views = defaultdict(set)  # story_id -> set of viewer_ids

# Background task to check for inactive users
def check_inactive_users():
    """Background task to mark users as offline if they haven't sent heartbeat in 5 minutes"""
    while True:
        try:
            time.sleep(30)

            current_time = datetime.now()
            users_to_mark_offline = []

            for user_id in list(online_users):
                # FIXED: Skip suspended users — background task must never un-suspend them
                if users.get(user_id, {}).get('status') == 'suspended':
                    continue

                last_active = user_last_active.get(user_id)

                if last_active:
                    if isinstance(last_active, str):
                        last_active = datetime.fromisoformat(last_active)

                    time_diff = (current_time - last_active).total_seconds()
                    if time_diff > 300:  # 5 minutes
                        users_to_mark_offline.append(user_id)
                        print(f"⏰ User {user_id} inactive for {time_diff:.0f}s — marking offline")

            for user_id in users_to_mark_offline:
                if user_id in online_users:
                    online_users.discard(user_id)
                    user_last_seen[user_id] = datetime.now().isoformat()

                    if user_id in users:
                        users[user_id]['status'] = 'offline'

                    if user_id in user_to_sid:
                        del user_to_sid[user_id]

                    for admin_sid in admin_sessions:
                        socketio.emit('user_offline', {
                            'user_id': user_id,
                            'username': users.get(user_id, {}).get('username', 'Unknown'),
                            'last_seen': user_last_seen[user_id]
                        }, room=admin_sid)

                    print(f"✅ Marked user {user_id} as offline (inactive)")

        except Exception as e:
            print(f"❌ Error in inactive user checker: {e}")

# Start background task
inactive_checker_thread = threading.Thread(target=check_inactive_users, daemon=True)
inactive_checker_thread.start()
print("🔄 Background inactive user checker started")

# Track unread messages and last message time
unread_counts = defaultdict(lambda: defaultdict(int))
last_message_time = defaultdict(dict)
user_chats = defaultdict(list)

# Track admin connections
admin_sessions = set()

# Admin activity log
admin_activity = []

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def generate_admin_token():
    return hashlib.sha256(f"{ADMIN_USERNAME}{datetime.now().isoformat()}".encode()).hexdigest()

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
        'timestamp': datetime.now().isoformat(),
        'action': action,
        'details': details
    })
    if len(admin_activity) > 100:
        admin_activity.pop(0)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def get_room_id(uid1, uid2):
    return "-".join(sorted([uid1, uid2]))

def update_user_chat_list(user_id, other_user_id, timestamp=None):
    if not timestamp:
        timestamp = datetime.now()
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

def increment_unread_count(receiver_id, sender_id):
    unread_counts[receiver_id][sender_id] += 1
    update_user_chat_list(receiver_id, sender_id)

def get_all_users_for_user(user_id):
    all_users_list = []

    for other_id, user_data in users.items():
        if other_id == user_id or user_data.get('status') == 'suspended':
            continue

        last_msg_time = None
        last_msg_preview = "No messages yet"
        unread_count = unread_counts[user_id].get(other_id, 0)

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
        elif other_id in last_message_time.get(user_id, {}):
            last_msg_time = last_message_time[user_id][other_id]

        current_user_time = datetime.fromisoformat(users[user_id]['created_at'])
        other_user_time = datetime.fromisoformat(user_data['created_at'])
        is_new_user = other_user_time > current_user_time

        all_users_list.append({
            'user_id': other_id,
            'username': user_data['username'],
            'is_online': other_id in online_users,
            'unread_count': unread_count,
            'last_message_time': last_msg_time.isoformat() if last_msg_time else None,
            'last_message': last_msg_preview,
            'is_new_user': is_new_user,
            'has_chatted': len(messages[room]) > 0,
            'last_seen': user_last_seen.get(other_id)
        })

    all_users_list.sort(key=lambda x: (
        -x['has_chatted'],
        -x['unread_count'],
        -x['is_online'],
        x['last_message_time'] if x['last_message_time'] else ''
    ), reverse=True)

    return all_users_list

def get_user_status(user_id):
    """Get detailed user status — suspended always wins"""
    if user_id not in users:
        return 'offline'

    # Suspended status always takes priority, regardless of online_users set
    if users[user_id].get('status') == 'suspended':
        return 'suspended'

    if user_id not in online_users:
        return 'offline'

    if user_id not in user_to_sid or len(user_to_sid[user_id]) == 0:
        return 'offline'

    if user_active_tabs.get(user_id, 0) == 0:
        return 'away'

    return 'online'

def update_user_status(user_id, status):
    """Broadcast user status change to all relevant rooms and admins"""
    for other_user_id in users.keys():
        if other_user_id != user_id:
            room = get_room_id(user_id, other_user_id)
            socketio.emit('user_status_change', {
                'user_id': user_id,
                'status': status,
                'last_seen': user_last_seen.get(user_id)
            }, room=room)

    for admin_sid in admin_sessions:
        socketio.emit('user_status_update', {
            'user_id': user_id,
            'status': status,
            'last_seen': user_last_seen.get(user_id)
        }, room=admin_sid)

def notify_new_user_to_all(new_user_id, new_username):
    new_user_info = {
        'user_id': new_user_id,
        'username': new_username,
        'status': get_user_status(new_user_id),
        'is_new_user': True,
        'last_seen': user_last_seen.get(new_user_id)
    }
    for user_id in online_users:
        if user_id != new_user_id:
            for sid in user_to_sid.get(user_id, []):
                socketio.emit('user_discovered', new_user_info, room=sid)

def notify_existing_users_to_new_user(new_user_id):
    existing_users = []
    for user_id, user_data in users.items():
        if user_id != new_user_id and user_data.get('status') != 'suspended':
            existing_users.append({
                'user_id': user_id,
                'username': user_data['username'],
                'status': get_user_status(user_id),
                'is_new_user': False,
                'last_seen': user_last_seen.get(user_id)
            })
    for sid in user_to_sid.get(new_user_id, []):
        socketio.emit('existing_users', {'users': existing_users}, room=sid)

def count_user_messages(user_id):
    count = 0
    for room_messages in messages.values():
        count += sum(1 for msg in room_messages if msg['sender'] == user_id)
    return count

def count_user_stories(user_id):
    return len(user_stories.get(user_id, []))

def broadcast_story_to_all_users(story_data, exclude_user_id=None):
    print(f"📢 Broadcasting story from user {story_data['user_id']} to all users")
    for user_id in list(online_users):
        if user_id == exclude_user_id:
            continue
        if user_id in user_to_sid:
            for sid in user_to_sid[user_id]:
                try:
                    socketio.emit('story_added', story_data, room=sid)
                except Exception as e:
                    print(f"❌ Failed to send story to {user_id}: {e}")
    for admin_sid in admin_sessions:
        try:
            socketio.emit('admin_new_story', {
                'user_id': story_data['user_id'],
                'username': users.get(story_data['user_id'], {}).get('username', 'Unknown'),
                'story': story_data['story']
            }, room=admin_sid)
        except Exception as e:
            print(f"❌ Failed to send story to admin: {e}")

def notify_story_owner_view_count(story_id, story_owner_id):
    view_count = len(story_views.get(story_id, set()))
    if story_owner_id in user_to_sid:
        for sid in user_to_sid[story_owner_id]:
            socketio.emit('story_view_count_update', {
                'story_id': story_id,
                'view_count': view_count
            }, room=sid)

# ==================== ADMIN ROUTES ====================

@app.route('/admin')
def admin_panel():
    return send_file('admin.html')

@app.route('/admin/login', methods=['POST'])
def admin_login():
    global ADMIN_TOKEN
    data = request.json
    username = data.get('username')
    password = data.get('password')
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        ADMIN_TOKEN = generate_admin_token()
        log_admin_activity('Admin Login', 'Admin logged in')
        return jsonify({'token': ADMIN_TOKEN})
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/admin/users', methods=['GET'])
@require_admin
def get_all_users_admin():
    users_list = []
    for user_id, user_data in users.items():
        real_status = get_user_status(user_id)
        users_list.append({
            'user_id': user_id,
            'username': user_data['username'],
            'email': user_data.get('email', ''),
            'created_at': user_data['created_at'],
            'status': real_status,
            'last_seen': user_last_seen.get(user_id),
            'message_count': count_user_messages(user_id),
            'story_count': count_user_stories(user_id)
        })
    users_list.sort(key=lambda x: x['created_at'], reverse=True)
    return jsonify({'users': users_list})

@app.route('/admin/stats', methods=['GET'])
@require_admin
def get_admin_stats():
    total_messages = sum(len(room_msgs) for room_msgs in messages.values())
    total_stories = sum(len(stories) for stories in user_stories.values())
    actual_online_users = 0
    for user_id in online_users:
        # FIXED: Don't count suspended users as online even if still in online_users set
        if users.get(user_id, {}).get('status') == 'suspended':
            continue
        if user_id in user_to_sid and len(user_to_sid[user_id]) > 0:
            actual_online_users += 1
    return jsonify({
        'total_users': len(users),
        'online_users': actual_online_users,
        'total_messages': total_messages,
        'total_stories': total_stories,
        'messages': [msg for room_msgs in messages.values() for msg in room_msgs],
        'stories': dict(user_stories)
    })

@app.route('/admin/user/<user_id>', methods=['GET'])
@require_admin
def get_user_details_admin(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    user_data = users[user_id]
    active_chats = 0
    for other_id in users.keys():
        if other_id != user_id:
            room = get_room_id(user_id, other_id)
            if messages[room]:
                active_chats += 1
    return jsonify({
        'user': {
            'user_id': user_id,
            'username': user_data['username'],
            'email': user_data.get('email', ''),
            'created_at': user_data['created_at'],
            'status': get_user_status(user_id),
            'last_seen': user_last_seen.get(user_id),
            'message_count': count_user_messages(user_id),
            'story_count': count_user_stories(user_id),
            'active_chats': active_chats
        }
    })

@app.route('/admin/user/<user_id>/stories', methods=['GET'])
@require_admin
def get_user_stories(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    current_time = datetime.now()
    active_stories = []
    if user_id in user_stories:
        for story in user_stories[user_id]:
            story_time = datetime.fromisoformat(story['timestamp'])
            if (current_time - story_time).total_seconds() < 86400:
                story_with_views = story.copy()
                story_with_views['view_count'] = len(story_views.get(story['id'], set()))
                story_with_views['viewers'] = [
                    {'user_id': vid, 'username': users.get(vid, {}).get('username', 'Unknown')}
                    for vid in story_views.get(story['id'], set())
                ]
                active_stories.append(story_with_views)
    return jsonify({
        'user_id': user_id,
        'username': users[user_id]['username'],
        'stories': active_stories
    })

@app.route('/admin/stories/all', methods=['GET'])
@require_admin
def get_all_stories():
    current_time = datetime.now()
    all_active_stories = {}
    for user_id, stories in user_stories.items():
        if user_id in users:
            all_active_stories[user_id] = {
                'username': users[user_id]['username'],
                'stories': []
            }
            for story in stories:
                story_time = datetime.fromisoformat(story['timestamp'])
                if (current_time - story_time).total_seconds() < 86400:
                    story_with_views = story.copy()
                    story_with_views['view_count'] = len(story_views.get(story['id'], set()))
                    all_active_stories[user_id]['stories'].append(story_with_views)
            if len(all_active_stories[user_id]['stories']) == 0:
                del all_active_stories[user_id]
    return jsonify({'stories': all_active_stories})

@app.route('/admin/user/<user_id>/reset-password', methods=['POST'])
@require_admin
def admin_reset_password(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    data = request.json
    new_password = data.get('new_password')
    if not new_password or len(new_password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    users[user_id]['password_hash'] = hash_password(new_password)
    log_admin_activity('Password Reset', f'Reset password for user {users[user_id]["username"]} ({user_id})')
    return jsonify({'success': True})

# ==================== FIX: suspend_user ====================
@app.route('/admin/user/<user_id>/suspend', methods=['POST'])
@require_admin
def suspend_user(user_id):
    """
    Suspend a user.
    Fixes vs original:
    1. Removes user from online_users so get_user_status() returns 'suspended' cleanly
    2. Cleans up user_to_sid so background thread doesn't wrongly flip status
    3. Broadcasts 'suspended' status to all admin sessions via update_user_status()
    4. Records last_seen at time of suspension
    """
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404

    # 1. Set the suspended flag in users dict
    users[user_id]['status'] = 'suspended'

    # 2. Kick the user's active socket connections and notify them
    if user_id in user_to_sid:
        for sid in list(user_to_sid[user_id]):
            socketio.emit('account_suspended', {
                'message': 'Your account has been suspended by an administrator'
            }, room=sid)
        del user_to_sid[user_id]  # Clean up session mapping

    # 3. Remove from online_users set so background checker ignores them
    #    and get_user_status() returns 'suspended' without any ambiguity
    online_users.discard(user_id)

    # 4. Record last seen timestamp
    user_last_seen[user_id] = datetime.now().isoformat()

    # 5. Broadcast the status change to ALL admin sessions so the table updates live
    for admin_sid in admin_sessions:
        socketio.emit('user_status_update', {
            'user_id': user_id,
            'status': 'suspended',
            'last_seen': user_last_seen[user_id]
        }, room=admin_sid)

    # 6. Also notify other regular users that this user is now offline/gone
    for other_user_id in list(online_users):
        if other_user_id != user_id:
            room = get_room_id(user_id, other_user_id)
            socketio.emit('user_status_change', {
                'user_id': user_id,
                'status': 'suspended',
                'last_seen': user_last_seen[user_id]
            }, room=room)

    log_admin_activity('User Suspended', f'Suspended user {users[user_id]["username"]} ({user_id})')
    print(f"🚫 User {user_id} ({users[user_id]['username']}) suspended by admin")

    return jsonify({'success': True, 'status': 'suspended'})

# ==================== FIX: unsuspend_user ====================
@app.route('/admin/user/<user_id>/unsuspend', methods=['POST'])
@require_admin
def unsuspend_user(user_id):
    """
    Unsuspend a user.
    Fixes vs original:
    1. Sets status to 'offline' correctly
    2. Broadcasts the status change to all admin sessions so the table updates live
    3. Returns the new status in the response so the frontend can confirm
    """
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404

    # 1. Lift the suspension — user starts as offline until they log in again
    users[user_id]['status'] = 'offline'

    # 2. Broadcast the new status to ALL admin sessions
    for admin_sid in admin_sessions:
        socketio.emit('user_status_update', {
            'user_id': user_id,
            'status': 'offline',
            'last_seen': user_last_seen.get(user_id)
        }, room=admin_sid)

    log_admin_activity('User Unsuspended', f'Unsuspended user {users[user_id]["username"]} ({user_id})')
    print(f"✅ User {user_id} ({users[user_id]['username']}) unsuspended by admin")

    return jsonify({'success': True, 'status': 'offline'})

@app.route('/admin/user/<user_id>', methods=['DELETE'])
@require_admin
def delete_user(user_id):
    if user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    username = users[user_id]['username']
    rooms_to_delete = [room_id for room_id in list(messages.keys()) if user_id in room_id]
    for room_id in rooms_to_delete:
        del messages[room_id]
    had_stories = user_id in user_stories
    if user_id in user_stories:
        # Clean up story_views for every story this user had
        for story in user_stories[user_id]:
            story_views.pop(story.get('id', ''), None)
        del user_stories[user_id]
    online_users.discard(user_id)
    if user_id in user_to_sid:
        for sid in user_to_sid[user_id].copy():
            socketio.emit('account_deleted', {
                'message': 'Your account has been deleted by an administrator'
            }, room=sid)
        del user_to_sid[user_id]
    del users[user_id]

    # FIX: Notify ALL online users to remove this user's stories from their UI
    if had_stories:
        for online_uid in list(online_users):
            for sid in user_to_sid.get(online_uid, []):
                socketio.emit('user_stories_removed', {'user_id': user_id}, room=sid)

    log_admin_activity('User Deleted', f'Deleted user {username} ({user_id})')
    return jsonify({'success': True})

@app.route('/admin/analytics', methods=['GET'])
@require_admin
def get_analytics():
    period = request.args.get('period', 'daily')
    now = datetime.now()
    data = []
    labels = []
    if period == 'daily':
        for i in range(6, -1, -1):
            date = now - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            active_count = sum(
                1 for last_seen in user_last_seen.values()
                if last_seen and datetime.fromisoformat(last_seen).strftime('%Y-%m-%d') == date_str
            )
            data.append(active_count)
            labels.append(date.strftime('%a'))
    elif period == 'weekly':
        for i in range(3, -1, -1):
            start_date = now - timedelta(weeks=i+1)
            end_date = now - timedelta(weeks=i)
            active_count = sum(
                1 for last_seen in user_last_seen.values()
                if last_seen and start_date <= datetime.fromisoformat(last_seen) <= end_date
            )
            data.append(active_count)
            labels.append(f"Week {4-i}")
    elif period == 'monthly':
        for i in range(5, -1, -1):
            month_date = now - timedelta(days=i*30)
            active_count = sum(
                1 for last_seen in user_last_seen.values()
                if last_seen and (
                    datetime.fromisoformat(last_seen).month == month_date.month and
                    datetime.fromisoformat(last_seen).year == month_date.year
                )
            )
            data.append(active_count)
            labels.append(month_date.strftime('%b'))
    return jsonify({'data': data, 'labels': labels})

@app.route('/admin/analytics/custom', methods=['GET'])
@require_admin
def get_custom_analytics():
    date_from = request.args.get('from')
    date_to = request.args.get('to')
    if not date_from or not date_to:
        return jsonify({'error': 'Missing date range'}), 400
    try:
        start_date = datetime.fromisoformat(date_from)
        end_date = datetime.fromisoformat(date_to).replace(hour=23, minute=59, second=59)
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400
    active_users = sum(
        1 for last_seen in user_last_seen.values()
        if last_seen and start_date <= datetime.fromisoformat(last_seen) <= end_date
    )
    new_users = sum(
        1 for user_data in users.values()
        if start_date <= datetime.fromisoformat(user_data['created_at']) <= end_date
    )
    message_count = sum(
        1 for room_msgs in messages.values()
        for msg in room_msgs
        if start_date <= datetime.fromisoformat(msg['timestamp']) <= end_date
    )
    story_count = sum(
        1 for stories_list in user_stories.values()
        for story in stories_list
        if start_date <= datetime.fromisoformat(story['timestamp']) <= end_date
    )
    return jsonify({
        'active_users': active_users,
        'new_users': new_users,
        'messages': message_count,
        'stories': story_count
    })

@app.route('/admin/activity/recent', methods=['GET'])
@require_admin
def get_recent_activity():
    activities = []
    all_messages = [msg for room_msgs in messages.values() for msg in room_msgs]
    all_messages.sort(key=lambda x: x['timestamp'], reverse=True)
    for msg in all_messages[:20]:
        sender_id = msg['sender']
        if sender_id in users:
            activities.append({
                'type': 'message',
                'username': users[sender_id]['username'],
                'action': f"sent a {msg['type']}",
                'timestamp': msg['timestamp']
            })
    all_stories = [
        {'user_id': uid, 'story': story}
        for uid, stories in user_stories.items()
        for story in stories
    ]
    all_stories.sort(key=lambda x: x['story']['timestamp'], reverse=True)
    for item in all_stories[:10]:
        uid = item['user_id']
        if uid in users:
            activities.append({
                'type': 'story',
                'username': users[uid]['username'],
                'action': 'posted a story',
                'timestamp': item['story']['timestamp']
            })
    activities.sort(key=lambda x: x['timestamp'], reverse=True)
    return jsonify({'activities': activities[:30]})

@app.route('/admin/chats/recent', methods=['GET'])
@require_admin
def get_recent_chats():
    all_chats = [msg for room_msgs in messages.values() for msg in room_msgs]
    all_chats.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    return jsonify({'chats': all_chats[:100]})

# ==================== REGULAR ROUTES ====================

@app.route('/')
def index():
    return send_file('index.html')

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username', '').strip()
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    if not username or len(username) < 2:
        return jsonify({'error': 'Username must be at least 2 characters'}), 400
    # Require proper email: something@something.tld  (at least 2-char TLD)
    import re as _re
    if not email or not _re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$', email):
        return jsonify({'error': 'Valid email address is required (e.g. name@example.com)'}), 400
    if password and len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    for user_data in users.values():
        if user_data['username'].lower() == username.lower():
            return jsonify({'error': 'Username already taken'}), 409
        if email and user_data.get('email') == email:
            return jsonify({'error': 'Email already registered'}), 409
    user_id = str(uuid.uuid4())[:8]
    users[user_id] = {
        'username': username,
        'created_at': datetime.now().isoformat(),
        'email': email,
        'password_hash': hash_password(password),
        'status': 'offline'
    }
    notify_new_user_to_all(user_id, username)
    return jsonify({
        'user_id': user_id,
        'username': username,
        'email': email,
        'created_at': users[user_id]['created_at']
    })

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '')
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    for user_id, user_data in users.items():
        if (user_data['username'].lower() == username.lower() or
                user_data.get('email', '').lower() == username.lower()):
            if user_data.get('status') == 'suspended':
                return jsonify({'error': 'Account suspended. Contact administrator.'}), 403
            if user_data['password_hash'] == hash_password(password):
                return jsonify({
                    'user_id': user_id,
                    'username': user_data['username'],
                    'email': user_data.get('email'),
                    'created_at': user_data['created_at']
                })
            else:
                return jsonify({'error': 'Invalid password'}), 401
    return jsonify({'error': 'User not found'}), 404

@app.route('/change_password', methods=['POST'])
def change_password():
    data = request.json
    user_id = data.get('user_id')
    new_password = data.get('new_password')
    if not user_id or user_id not in users:
        return jsonify({'error': 'User not found'}), 404
    if not new_password or len(new_password) < 6:
        return jsonify({'error': 'New password must be at least 6 characters'}), 400
    users[user_id]['password_hash'] = hash_password(new_password)
    return jsonify({'success': True, 'message': 'Password changed successfully'})

@app.route('/reset_password', methods=['POST'])
def reset_password():
    data = request.json
    username = data.get('username', '').strip()
    user_id = data.get('user_id', '').strip()
    new_password = data.get('new_password')
    if not username:
        return jsonify({'error': 'Username is required'}), 400
    if not user_id:
        return jsonify({'error': 'User ID is required'}), 400
    if not new_password or len(new_password) < 6:
        return jsonify({'error': 'New password must be at least 6 characters'}), 400
    if user_id in users and users[user_id]['username'].lower() == username.lower():
        users[user_id]['password_hash'] = hash_password(new_password)
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
        filename = secure_filename(file.filename)
        file_id = str(uuid.uuid4())
        file_extension = filename.rsplit('.', 1)[1].lower()
        saved_filename = f"{file_id}.{file_extension}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], saved_filename)
        file.save(filepath)
        file_type = 'image' if file_extension in ['png', 'jpg', 'jpeg', 'gif'] else \
                    'video' if file_extension in ['mp4', 'mov', 'avi'] else 'file'
        return jsonify({
            'file_id': file_id,
            'filename': filename,
            'file_type': file_type,
            'url': f'/uploads/{saved_filename}'
        })
    return jsonify({'error': 'File type not allowed'}), 400

@app.route('/uploads/<filename>')
def serve_file(filename):
    return send_file(os.path.join(app.config['UPLOAD_FOLDER'], filename))

@app.route('/users')
def get_all_users():
    exclude_id = request.args.get('exclude')
    user_list = []
    for uid, data in users.items():
        if uid != exclude_id and data.get('status') != 'suspended':
            user_list.append({
                'user_id': uid,
                'username': data['username'],
                'status': get_user_status(uid),
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
    query = query.lower()
    matches = []
    for uid, data in users.items():
        if data.get('status') != 'suspended':
            if query in uid.lower() or query in data['username'].lower():
                matches.append({
                    'user_id': uid,
                    'username': data['username'],
                    'status': get_user_status(uid),
                    'last_seen': user_last_seen.get(uid)
                })
    return jsonify(matches[:20])

@app.route('/get_stories', methods=['GET'])
def get_stories():
    current_time = datetime.now()
    active_stories = {}
    for user_id, stories in user_stories.items():
        active_stories[user_id] = [
            story for story in stories
            if (current_time - datetime.fromisoformat(story['timestamp'])).total_seconds() < 86400
        ]
        user_stories[user_id] = active_stories[user_id]
    return jsonify({'stories': active_stories})

# ==================== SOCKET.IO EVENTS ====================

@socketio.on('connect')
def handle_connect():
    print(f'\n🔗 New client connected: {request.sid}')

@socketio.on('admin_connected')
def handle_admin_connected(data):
    token = data.get('token')
    if token == ADMIN_TOKEN:
        admin_sessions.add(request.sid)
        print(f"🛡️ Admin connected: {request.sid}")
        current_time = datetime.now()
        active_stories = {}
        for user_id, stories in user_stories.items():
            active_stories[user_id] = []
            for story in stories:
                story_time = datetime.fromisoformat(story['timestamp'])
                if (current_time - story_time).total_seconds() < 86400:
                    s = story.copy()
                    s['view_count'] = len(story_views.get(story['id'], set()))
                    active_stories[user_id].append(s)
        socketio.emit('admin_stories_update', {
            'stories': active_stories,
            'users': {uid: users[uid]['username'] for uid in users.keys()}
        }, room=request.sid)

@socketio.on('set_user')
def handle_set_user(data):
    user_id = data.get('user_id')
    if user_id and user_id in users:
        if users[user_id].get('status') == 'suspended':
            emit('account_suspended', {'message': 'Your account has been suspended by an administrator'})
            return
        user_sessions[request.sid] = user_id
        if user_id not in user_to_sid:
            user_to_sid[user_id] = []
        user_to_sid[user_id].append(request.sid)
        user_active_tabs[user_id] = user_active_tabs.get(user_id, 0) + 1
        if user_id not in online_users:
            online_users.add(user_id)
            user_last_seen[user_id] = datetime.now().isoformat()
            user_last_active[user_id] = datetime.now()
            users[user_id]['status'] = 'online'
            update_user_status(user_id, 'online')
            for admin_sid in admin_sessions:
                socketio.emit('user_online', {
                    'user_id': user_id,
                    'username': users[user_id]['username']
                }, room=admin_sid)
        notify_existing_users_to_new_user(user_id)
        all_users = get_all_users_for_user(user_id)
        emit('chat_list_updated', {'chats': all_users})
        total_unread = sum(unread_counts[user_id].values())
        emit('unread_count_updated', {'total_unread': total_unread})
        current_time = datetime.now()
        active_stories = {}
        for uid, stories in user_stories.items():
            active_stories[uid] = []
            for story in stories:
                try:
                    story_time = datetime.fromisoformat(story['timestamp'])
                    if (current_time - story_time).total_seconds() < 86400:
                        s = story.copy()
                        s['view_count'] = len(story_views.get(story['id'], set()))
                        active_stories[uid].append(s)
                except Exception as e:
                    print(f"⚠️ Error processing story: {e}")
        views_dict = {sid: list(viewers) for sid, viewers in story_views.items()}
        emit('existing_stories', {'stories': active_stories, 'views': views_dict})
        emit('connection_established', {'user_id': user_id})

@socketio.on('user_active')
def handle_user_active(data):
    user_id = user_sessions.get(request.sid)
    if user_id:
        # FIXED: Don't allow suspended users to go back online via heartbeat
        if users.get(user_id, {}).get('status') == 'suspended':
            emit('account_suspended', {'message': 'Your account has been suspended by an administrator'})
            return
        user_last_active[user_id] = datetime.now()
        user_last_seen[user_id] = datetime.now().isoformat()
        if user_id not in online_users:
            online_users.add(user_id)
            if user_id in users:
                users[user_id]['status'] = 'online'
            for admin_sid in admin_sessions:
                socketio.emit('user_online', {
                    'user_id': user_id,
                    'username': users.get(user_id, {}).get('username', 'Unknown')
                }, room=admin_sid)
        update_user_status(user_id, 'online')

@socketio.on('user_inactive')
def handle_user_inactive(data):
    user_id = user_sessions.get(request.sid)
    if user_id:
        user_last_seen[user_id] = datetime.now().isoformat()

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
    user_id = user_sessions.get(request.sid)
    target_id = data.get('target_id')
    if not user_id or target_id not in users:
        return
    room = get_room_id(user_id, target_id)
    join_room(room)
    mark_messages_as_read(user_id, target_id)
    update_user_chat_list(user_id, target_id)
    emit('chat_history', {
        'messages': messages[room],
        'target_user': {
            'user_id': target_id,
            'username': users[target_id]['username'],
            'status': get_user_status(target_id),
            'last_seen': user_last_seen.get(target_id)
        }
    })
    emit('chat_list_updated', {'chats': get_all_users_for_user(user_id)})
    total_unread = sum(unread_counts[user_id].values())
    emit('unread_count_updated', {'total_unread': total_unread})
    socketio.emit('messages_read', {'reader_id': user_id}, room=get_room_id(user_id, target_id))

@socketio.on('send_message')
def handle_message(data):
    user_id = user_sessions.get(request.sid)
    target_id = data.get('target_id')
    text = data.get('text', '').strip()
    message_type = data.get('type', 'text')
    file_data = data.get('file_data')
    if not user_id or not target_id:
        return
    if message_type == 'text' and not text:
        return
    room = get_room_id(user_id, target_id)
    msg = {
        'id': str(uuid.uuid4()),
        'sender': user_id,
        'receiver': target_id,
        'type': message_type,
        'text': text,
        'timestamp': datetime.now().isoformat(),
        'read': False
    }
    if message_type in ['image', 'video', 'file'] and file_data:
        msg['file_data'] = file_data
    messages[room].append(msg)
    if len(messages[room]) > 100:
        messages[room] = messages[room][-100:]
    timestamp = datetime.now()
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
    user_id = user_sessions.get(request.sid)
    target_id = data.get('target_id')
    if user_id and target_id:
        room = get_room_id(user_id, target_id)
        emit('user_typing', {
            'user_id': user_id,
            'is_typing': data.get('is_typing', False)
        }, room=room, include_self=False)

@socketio.on('add_story')
def handle_add_story(data):
    user_id = user_sessions.get(request.sid)
    if not user_id:
        return {'success': False, 'error': 'Not authenticated'}
    story_id = str(uuid.uuid4())
    story = {
        'id': story_id,
        'user_id': user_id,
        'file_data': data.get('file_data'),
        'type': data.get('type'),
        'timestamp': datetime.now().isoformat(),
        'view_count': 0
    }
    if not story['file_data'] or not story['type']:
        return {'success': False, 'error': 'Invalid story data'}
    user_stories[user_id].append(story)
    story_views[story_id] = set()
    if len(user_stories[user_id]) > 10:
        old_story = user_stories[user_id][0]
        if old_story['id'] in story_views:
            del story_views[old_story['id']]
        user_stories[user_id] = user_stories[user_id][-10:]
    broadcast_story_to_all_users({'user_id': user_id, 'story': story}, exclude_user_id=user_id)
    emit('story_upload_success', {
        'story_id': story_id,
        'story': story,
        'view_count': 0,
        'message': 'Story uploaded and shared with everyone!'
    })
    return {'success': True, 'story_id': story_id}

@socketio.on('view_story')
def handle_view_story(data):
    story_id = data.get('story_id')
    viewer_id = data.get('viewer_id')
    story_owner_id = data.get('story_owner_id')
    if story_id and viewer_id:
        story_views[story_id].add(viewer_id)
        view_count = len(story_views[story_id])
        if story_owner_id and story_owner_id in user_to_sid:
            notify_story_owner_view_count(story_id, story_owner_id)
            viewer_username = users.get(viewer_id, {}).get('username', 'Unknown')
            for sid in user_to_sid[story_owner_id]:
                socketio.emit('story_view_update', {
                    'story_id': story_id,
                    'viewer_id': viewer_id,
                    'viewer_username': viewer_username,
                    'view_count': view_count
                }, room=sid)

@socketio.on('get_message_history')
def handle_get_message_history(data):
    user_id = data.get('user_id')
    if not user_id or user_id not in users:
        return
    user_messages = [
        msg for room_msgs in messages.values()
        for msg in room_msgs
        if msg['sender'] == user_id or msg['receiver'] == user_id
    ]
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
                user_last_seen[user_id] = datetime.now().isoformat()
                # Only set offline if not suspended
                if user_id in users and users[user_id].get('status') != 'suspended':
                    users[user_id]['status'] = 'offline'
                    update_user_status(user_id, 'offline')
                    for admin_sid in admin_sessions:
                        socketio.emit('user_offline', {
                            'user_id': user_id,
                            'username': users.get(user_id, {}).get('username', 'Unknown'),
                            'last_seen': user_last_seen[user_id]
                        }, room=admin_sid)

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 MANUS MESSENGER SERVER STARTING")
    print("="*60)
    print(f"\n📱 Main App: http://localhost:5000")
    print(f"🛡️  Admin Panel: http://localhost:5000/admin")
    print(f"\n🔐 Admin Credentials:")
    print(f"   Username: {ADMIN_USERNAME}")
    print(f"   Password: {ADMIN_PASSWORD}")
    print(f"\n⚠️  IMPORTANT: Change admin credentials in production!")
    print("="*60 + "\n")
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)