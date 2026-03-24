from redis import Redis
from rq.decorators import job
import os
import time
import datetime

redis_conn = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))

@job('notifications', connection=redis_conn)
def send_notification(notification_id, email, message):
    
    print(f"Starting to send notification {notification_id} to {email}")
    time.sleep(3)
    print(f"Finished sending notification {notification_id} to {email}")
    sent_at = datetime.datetime.now().isoformat()
    return {"notification_id": notification_id, 
            "email": email, 
            "status": "sent", 
            "sent_at": sent_at}