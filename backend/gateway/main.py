import os
import shutil
import uuid
import json
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from redis import Redis
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

app = FastAPI(title="FluxFrame API Gateway")

# Configs
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Clients
mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client.fluxframe
redis_client = Redis.from_url(REDIS_URL, decode_responses=True)

# Queues
QUEUE_PREPROCESS = "preprocess_queue"

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class JobBase(BaseModel):
    video_id: str
    status: str
    created_at: datetime

class JobCreateResponse(BaseModel):
    job_id: str
    status: str

@app.get("/health")
def health_check():
    try:
        redis_client.ping()
        return {"status": "ok", "redis": "connected", "mongo": "connected"}
    except Exception as e:
        return {"status": "error", "details": str(e)}

@app.post("/upload", response_model=JobCreateResponse)
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    cfg_scale: float = 7.5,
    interp_frames: int = 2
):
    # 1. Generate IDs
    video_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    
    # 2. Save file temporarily (GridFS in real prod, local tmp here for simplicity)
    os.makedirs("/tmp/uploads", exist_ok=True)
    file_location = f"/tmp/uploads/{video_id}_{file.filename}"
    with open(file_location, "wb+") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # 3. Create Video Record in Mongo
    video_doc = {
        "_id": video_id,
        "filename": file.filename,
        "file_path": file_location,
        "status": "uploaded",
        "created_at": datetime.utcnow()
    }
    await db.videos.insert_one(video_doc)
    
    # 4. Create Job Record
    job_doc = {
        "_id": job_id,
        "video_id": video_id,
        "status": "queued",
        "params": {
            "cfg_scale": cfg_scale,
            "interp_frames": interp_frames
        },
        "created_at": datetime.utcnow(),
        "history": [
            {"status": "queued", "timestamp": datetime.utcnow()}
        ]
    }
    await db.jobs.insert_one(job_doc)
    
    # 5. Push to Redis (Preprocess Queue)
    task_payload = json.dumps({"job_id": job_id, "video_id": video_id, "file_path": file_location})
    redis_client.rpush(QUEUE_PREPROCESS, task_payload)
    
    return {"job_id": job_id, "status": "queued"}

@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    job = await db.jobs.find_one({"_id": job_id})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/jobs")
async def list_jobs():
    jobs = await db.jobs.find().to_list(100)
    return jobs
