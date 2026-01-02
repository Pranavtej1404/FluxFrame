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
        
        # 7. Upload Frames to GridFS
        import gridfs
        fs = gridfs.GridFS(db)
        log(job_id, "Uploading frames to GridFS...")
        
        gridfs_frames = []
        frame_files = sorted(os.listdir(frames_dir))
        
        for idx, filename in enumerate(frame_files):
            filepath = os.path.join(frames_dir, filename)
            with open(filepath, "rb") as f:
                file_id = fs.put(f, filename=filename, metadata={"job_id": job_id, "index": idx})
                gridfs_frames.append({"index": idx, "file_id": str(file_id)})
        
        # 8. Update Video Metadata in DB
        db.videos.update_one(
            {"_id": video_id},
            {"$set": {
                "fps": fps,
                "width": width,
                "height": height,
                "total_frames": frame_count,
                "has_audio": has_audio,
                "audio_path": audio_path if has_audio else None,
                "analysis": analysis_result
            }}
        )
        
        # 9. Update Job Status & Manifest
        manifest = {
            "job_id": job_id,
            "frame_count": frame_count,
            "fps": fps,
            "frame_path_template": f"{frames_dir}/frame_%06d.png",
            "shot_segments": analysis_result["shot_segments"] if analysis_result else [],
            "interpolation_params": analysis_result["interpolation_params"] if analysis_result else {},
            "gridfs_frames": gridfs_frames
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
    
    # Fetch Manifest
    job = db.jobs.find_one({"_id": job_id})
    if not job or "manifest" not in job or "gridfs_frames" not in job["manifest"]:
        log(job_id, "Inference Failed: Missing manifest or frames")
        return

    frames = job["manifest"]["gridfs_frames"]
    # Sort just in case
    frames.sort(key=lambda x: x["index"])
    
    generated_frames = []
    
    # Loop through pairs
    # Limit for demo: Process only first 5 pairs to avoid timeout/cost if video is long
    limit = 5 
    log(job_id, f"Processing first {limit} frame pairs for demo...")
    
    for i in range(min(len(frames) - 1, limit)):
        start_frame = frames[i]
        end_frame = frames[i+1]
        
        payload = {
            "job_id": job_id,
            "frame_start_id": start_frame["file_id"],
            "frame_end_id": end_frame["file_id"],
            "cfg_scale": 7.0
        }
        
        try:
            resp = requests.post(f"{MODEL_BASE_URL}/generate_lowres", json=payload)
            if resp.status_code != 200:
                log(job_id, f"Base Model error for pair {i}: {resp.text}")
                continue
                
            data = resp.json()
            gen_id = data["generated_frame_id"]
            generated_frames.append({
                "index": start_frame["index"] + 0.5,
                "file_id": gen_id,
                "parent_start": start_frame["file_id"],
                "parent_end": end_frame["file_id"]
            })
            log(job_id, f"Generated frame {start_frame['index']}.5")
            
        except Exception as e:
            log(job_id, f"Error calling Base Model: {e}")
            
    # Update Job with Generated Frames
    db.jobs.update_one(
        {"_id": job_id}, 
        {
            "$set": {"status": "inferred", "generated_frames": generated_frames}
        }
    )
    
    # Push to Postprocess
    redis_client.rpush(QUEUE_POSTPROCESS, json.dumps(task))
    log(job_id, "Finished Inference -> Pushed to Postprocess")

def handle_postprocess(task):
    job_id = task['job_id']
    log(job_id, "Started Postprocessing")
    
    # 1. Setup
    import shutil
    import gridfs
    import ffmpeg
    
    fs = gridfs.GridFS(db)
    base_dir = "/media"
    reconstruct_dir = f"{base_dir}/reconstruct/{job_id}"
    
    if os.path.exists(reconstruct_dir):
        shutil.rmtree(reconstruct_dir)
    os.makedirs(reconstruct_dir)
    
    # 2. Fetch Metadata & Frames
    job = db.jobs.find_one({"_id": job_id})
    if not job:
        log(job_id, "Postprocessing Failed: Job not found")
        return
        
    original_frames = job.get("manifest", {}).get("gridfs_frames", [])
    generated_frames = job.get("generated_frames", [])
    
    all_frames = original_frames + generated_frames
    all_frames.sort(key=lambda x: x["index"])
    
    log(job_id, f"Reconstructing video with {len(all_frames)} frames (Original: {len(original_frames)}, Generated: {len(generated_frames)})")
    
    # 3. Download Frames
    from bson.objectid import ObjectId
    
    for i, frame in enumerate(all_frames):
        try:
            file_id = frame["file_id"]
            # Convert to ObjectId if necessary (GridFS needs ObjectId often, but we stored as string)
            # If we stored as string of ObjectId, we convert back. 
            try:
                oid = ObjectId(file_id)
            except:
                oid = file_id # Fallback if it's a filename or other ID scheme
                
            grid_out = fs.get(oid)
            
            save_path = f"{reconstruct_dir}/frame_{i:06d}.png"
            with open(save_path, "wb") as f:
                f.write(grid_out.read())
        except Exception as e:
            log(job_id, f"Error downloading frame {i} ({file_id}): {e}")
            # Skip/Error handling? For now, we continue, might result in glitch.
            
    # 4. Calculate Target FPS
    # Assuming constant frame rate input
    # New FPS = Old FPS * (New Count / Old Count)
    # But safeguard against zero division
    original_fps = 30 # Default
    if "fps" in job.get("manifest", {}):
        original_fps = job["manifest"]["fps"]
    elif "fps" in job:
        original_fps = job["fps"]
        
    if len(original_frames) > 0:
        target_fps = original_fps * (len(all_frames) / len(original_frames))
    else:
        target_fps = original_fps # Fallback
        
    log(job_id, f"Target FPS: {target_fps:.2f} (Base: {original_fps})")
    
    # 5. Encode Video
    output_video_path = f"{reconstruct_dir}/output.mp4"
    audio_path = f"{base_dir}/audio/{job_id}.wav"
    has_audio = os.path.exists(audio_path)
    
    try:
        stream = ffmpeg.input(f"{reconstruct_dir}/frame_%06d.png", framerate=target_fps)
        
        if has_audio:
            audio_stream = ffmpeg.input(audio_path)
            stream = ffmpeg.output(stream, audio_stream, output_video_path, vcodec='libx264', pix_fmt='yuv420p', acodec='aac', shortest=None)
        else:
            stream = ffmpeg.output(stream, output_video_path, vcodec='libx264', pix_fmt='yuv420p')
            
        stream.run(overwrite_output=True, capture_stdout=True, capture_stderr=True)
        log(job_id, "FFmpeg encoding complete")
        
        # 6. Upload Final Video
        final_filename = f"final_{job_id}.mp4"
        with open(output_video_path, "rb") as f:
            video_id = fs.put(f, filename=final_filename, content_type="video/mp4")
            
        # 7. Update Job
        db.jobs.update_one(
            {"_id": job_id}, 
            {
                "$set": {
                    "status": "completed",
                    "final_video_id": str(video_id),
                    "output_fps": target_fps,
                    "completed_at": datetime.utcnow()
                },
                "$push": {"history": {"status": "completed", "timestamp": datetime.utcnow()}}
            }
        )
        log(job_id, f"Job Completed! Final Video ID: {video_id}")
        
    except ffmpeg.Error as e:
        error_log = e.stderr.decode('utf8')
        log(job_id, f"FFmpeg Reassembly Error: {error_log}")
        db.jobs.update_one({"_id": job_id}, {"$set": {"status": "failed", "error": f"Reassembly failed: {error_log}"}})
    except Exception as e:
        log(job_id, f"Reassembly Error: {e}")
        db.jobs.update_one({"_id": job_id}, {"$set": {"status": "failed", "error": str(e)}})
        
    # Cleanup
    try:
        
        shutil.rmtree(reconstruct_dir)
        # Optional: remove audio file too
    except:
        pass

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
