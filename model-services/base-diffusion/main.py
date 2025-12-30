from fastapi import FastAPI
from pydantic import BaseModel
import time

app = FastAPI()

class GenRequest(BaseModel):
    job_id: str

@app.post("/generate_lowres")
def generate(req: GenRequest):
    print(f"Base Diffusion generating for Job {req.job_id}...")
    time.sleep(3) # Simulate GPU Inference
    return {"status": "success", "frames_generated": 10}
