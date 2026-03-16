"""
Notification Service API - Starter (Synchronous)

This version sends notifications SYNCHRONOUSLY.
Each request blocks for 3 seconds while "sending" the notification.

YOUR TASK: Convert this to use rq for background processing!
"""

from queue import Queue

from flask import Flask, jsonify, request
import time
import uuid
from datetime import datetime

import os
from dotenv import load_dotenv
from redis import Redis
from rq.decorators import job

from tasks import send_notification

load_dotenv()

app = Flask(__name__)

# In-memory store for notifications
notifications = {}

# Redis connection for RQ
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
redis_conn = Redis.from_url(redis_url)

queue = Queue('notifications', connection=redis_conn)


@job('default', connection=redis_conn)
def send_notification_async(notification_id, email, message):
    """
    Send a notification (ASYNC - runs in background!)

    In production, this would call an email service like Mailgun.
    We simulate the slow API with time.sleep().
    """
    print(f"[Sending] Notification {notification_id} to {email}...")

    # This is the problem - blocking for 3 seconds!
    time.sleep(3)

    sent_at = datetime.utcnow().isoformat() + "Z"
    print(f"[Sent] Notification {notification_id} at {sent_at}")

    return {
        "notification_id": notification_id,
        "email": email,
        "status": "sent",
        "sent_at": sent_at
    }


@app.route('/')
def index():
    return jsonify({
        "service": "Notification Service (Synchronous - SLOW!)",
        "endpoints": {
            "POST /notifications": "Send a notification (takes 3 seconds!)",
            "GET /notifications": "List all notifications",
            "GET /notifications/<id>": "Get a notification"
        }
    })


@app.route('/notifications', methods=['POST'])
def create_notification():
    """
    Send a notification.

    WARNING: This blocks for 3 seconds!
    The user has to wait while we "send" the notification.

    TODO: Convert this to use rq for background processing!
    """
    data = request.get_json()

    if not data or 'email' not in data:
        return jsonify({"error": "Email is required"}), 400

    # Create notification record
    notification_id = str(uuid.uuid4())
    email = data['email']
    message = data.get('message', 'You have a new notification!')

    # THIS IS THE PROBLEM: We block here for 3 seconds!
    # The user can't do anything while we wait.
    result = send_notification_async(notification_id, email, message)

    notification = {
        "id": notification_id,
        "email": email,
        "message": message,
        "status": result['status'],
        "sent_at": result['sent_at']
    }
    notifications[notification_id] = notification

    return jsonify(notification), 201


@app.route('/notifications', methods=['GET'])
def list_notifications():
    """List all notifications."""
    return jsonify({
        "notifications": list(notifications.values())
    })


@app.route('/notifications/<notification_id>', methods=['GET'])
def get_notification(notification_id):
    """Get a single notification."""
    notification = notifications.get(notification_id)
    if not notification:
        return jsonify({"error": "Notification not found"}), 404
    return jsonify(notification)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
