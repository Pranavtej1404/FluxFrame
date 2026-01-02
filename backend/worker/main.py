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
    video_id = task['video_id']
    file_path = task['file_path']
    
    log(job_id, "Started Preprocessing: Frame Extraction")
    
    try:
        import ffmpeg
        
        # 1. Setup paths
        base_dir = "/media"
        frames_dir = f"{base_dir}/frames/{job_id}"
        audio_path = f"{base_dir}/audio/{job_id}.wav"
        
        os.makedirs(frames_dir, exist_ok=True)
        os.makedirs(f"{base_dir}/audio", exist_ok=True)
        
        # 2. Probe Metadata
        probe = ffmpeg.probe(file_path)
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
        
        width = int(video_stream['width'])
        height = int(video_stream['height'])
        
        # Parse FPS (e.g., "30/1" or "30")
        r_frame_rate = video_stream['r_frame_rate']
        num, den = map(int, r_frame_rate.split('/'))
        fps = num / den if den > 0 else 30.0
        
        # 3. Extract Frames
        log(job_id, f"Extracting frames from {file_path} to {frames_dir}")
        (
            ffmpeg
            .input(file_path)
            .output(f"{frames_dir}/frame_%06d.png")
            .run(capture_stdout=True, capture_stderr=True)
        )
        
        # 4. Extract Audio (if exists)
        audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)
        has_audio = False
        if audio_stream:
            log(job_id, "Extracting audio")
            (
                ffmpeg
                .input(file_path)
                .output(audio_path)
                .run(capture_stdout=True, capture_stderr=True)
            )
            has_audio = True
            
        # 5. Count extracted frames
        frame_files = sorted(os.listdir(frames_dir))
        frame_count = len(frame_files)
        
        # 6. Advanced Preprocessing Analysis
        import preprocessing
        log(job_id, "Running advanced preprocessing (scene detection, motion analysis)...")
        analysis_result = preprocessing.analyze_video(frames_dir, fps=fps)
        
        # 7. Update Video Metadata in DB
        db.videos.update_one(
            {"_id": video_id},
            {"$set": {
                "fps": fps,
                "width": width,
                "height": height,
                "total_frames": frame_count,
                "has_audio": has_audio,
                "audio_path": audio_path if has_audio else None,
                "analysis": analysis_result # Store dense analysis data here
            }}
        )
        
        # 8. Update Job Status & Manifest
        manifest = {
            "job_id": job_id,
            "frame_count": frame_count,
            "fps": fps,
            "frame_path_template": f"{frames_dir}/frame_%06d.png",
            "shot_segments": analysis_result["shot_segments"] if analysis_result else [],
            "interpolation_params": analysis_result["interpolation_params"] if analysis_result else {}
        }
        
        db.jobs.update_one(
            {"_id": job_id}, 
            {
                "$set": {
                    "status": "preprocessed", 
                    "manifest": manifest,
                    "processing_stats": {
                        "motion_score": analysis_result["motion_analysis"]["average_score"] if analysis_result else 0
                    }
                },
                "$push": {"history": {"status": "frames_extracted_and_analyzed", "timestamp": datetime.utcnow()}}
            }
        )
        
        # Push to Inference
        redis_client.rpush(QUEUE_INFERENCE, json.dumps(task))
        log(job_id, "Finished Preprocessing -> Pushed to Inference")
        
    except ffmpeg.Error as e:
        error_log = e.stderr.decode('utf8')
        log(job_id, f"FFmpeg Error: {error_log}")
        db.jobs.update_one({"_id": job_id}, {"$set": {"status": "failed", "error": error_log}})
    except Exception as e:
        log(job_id, f"Preprocessing Error: {str(e)}")
        db.jobs.update_one({"_id": job_id}, {"$set": {"status": "failed", "error": str(e)}})

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
