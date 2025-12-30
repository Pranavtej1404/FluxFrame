import os
import json
import time
import requests
from redis import Redis
from pymongo import MongoClient
from datetime import datetime

# Config
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
MODEL_BASE_URL = os.getenv("MODEL_BASE_URL", "http://localhost:8001")
MODEL_SR_URL = os.getenv("MODEL_SR_URL", "http://localhost:8002")

# Clients
redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
mongo_client = MongoClient(MONGO_URL)
db = mongo_client.fluxframe

# Queues
QUEUE_PREPROCESS = "preprocess_queue"
QUEUE_INFERENCE = "inference_queue"
QUEUE_POSTPROCESS = "postprocess_queue"

def log(job_id, message):
    print(f"[{datetime.now()}] [Job {job_id}] {message}")
    db.jobs.update_one(
        {"_id": job_id},
        {"$push": {"history": {"status": message, "timestamp": datetime.utcnow()}}}
    )

def handle_preprocess(task):
    job_id = task['job_id']
    log(job_id, "Started Preprocessing")
    
    # Simulate FFmpeg extraction
    time.sleep(2) # Fake work
    
    # Update Job
    db.jobs.update_one({"_id": job_id}, {"$set": {"status": "preprocessed"}})
    
    # Push to Inference
    redis_client.rpush(QUEUE_INFERENCE, json.dumps(task))
    log(job_id, "Finished Preprocessing -> Pushed to Inference")

def handle_inference(task):
    job_id = task['job_id']
    log(job_id, "Started Inference")
    
    # 1. Call Base Diffusion Model
    try:
        resp = requests.post(f"{MODEL_BASE_URL}/generate_lowres", json={"job_id": job_id})
        if resp.status_code != 200:
            raise Exception(f"Base Model failed: {resp.text}")
    except Exception as e:
        log(job_id, f"Error calling Base Model: {e}")
        return

    # 2. Call Super Resolution Model
    try:
        resp = requests.post(f"{MODEL_SR_URL}/super_resolution", json={"job_id": job_id})
        if resp.status_code != 200:
            raise Exception(f"SR Model failed: {resp.text}")
    except Exception as e:
        log(job_id, f"Error calling SR Model: {e}")
        return

    # Update Job
    db.jobs.update_one({"_id": job_id}, {"$set": {"status": "inferred"}})
    
    # Push to Postprocess
    redis_client.rpush(QUEUE_POSTPROCESS, json.dumps(task))
    log(job_id, "Finished Inference -> Pushed to Postprocess")

def handle_postprocess(task):
    job_id = task['job_id']
    log(job_id, "Started Postprocessing")
    
    # Simulate Reassembly
    time.sleep(2)
    
    # Update Job to Complete
    db.jobs.update_one({"_id": job_id}, {"$set": {"status": "completed"}})
    log(job_id, "Job Completed Successfully")

def worker_loop():
    print("Worker started. Waiting for jobs...")
    while True:
        # Check queues in priority order or round-robin
        # Simple implementation: Check Preprocess -> Inference -> Postprocess
        
        # 1. Preprocess
        task_data = redis_client.lpop(QUEUE_PREPROCESS)
        if task_data:
            handle_preprocess(json.loads(task_data))
            continue
            
        # 2. Inference
        task_data = redis_client.lpop(QUEUE_INFERENCE)
        if task_data:
            handle_inference(json.loads(task_data))
            continue
            
        # 3. Postprocess
        task_data = redis_client.lpop(QUEUE_POSTPROCESS)
        if task_data:
            handle_postprocess(json.loads(task_data))
            continue
            
        time.sleep(1)

if __name__ == "__main__":
    worker_loop()
