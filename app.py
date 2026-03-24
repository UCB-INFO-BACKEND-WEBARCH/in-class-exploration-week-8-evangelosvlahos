"""
Notification Service API - Starter (Synchronous)

This version sends notifications SYNCHRONOUSLY.
Each request blocks for 3 seconds while "sending" the notification.

YOUR TASK: Convert this to use rq for background processing!
"""

from rq import Queue

from flask import Flask, jsonify, request
import time
import uuid
from datetime import datetime

import os

from rq.job import Job


from redis import Redis
from rq.decorators import job

from tasks import send_notification


app = Flask(__name__)
 
# In-memory store for notifications
notifications = {}

# Redis connection for RQ
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
redis_conn = Redis.from_url(redis_url)

queue = Queue('notifications', connection=redis_conn)


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
    notification_id = str(uuid.uuid1())
    email = data['email']
    message = data.get('message', 'You have a new notification!')

    # THIS IS THE PROBLEM: We block here for 3 seconds!
    # The user can't do anything while we wait.
    job = send_notification.delay(notification_id, email, message)


    return {"job_id": job.id}, 202


@app.route('/notifications', methods=['GET'])
def list_notifications():
    """List all notifications."""
    return jsonify({
        "notifications": list(notifications.values())
    })


@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        return {"error": "Job not found"}, 404

    response = {
        "job_id": job_id,
        "status": job.get_status()
    }

    if job.is_finished:
        response["result"] = job.result
    elif job.is_failed:
        response["error"] = str(job.exc_info)

    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
