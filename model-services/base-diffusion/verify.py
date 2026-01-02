import requests
import io
from PIL import Image
from pymongo import MongoClient
import gridfs
import os
import sys

# Configuration
API_URL = "http://localhost:8001/generate_lowres"
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")

def create_dummy_images():
    img1 = Image.new('RGB', (100, 100), color = 'red')
    img2 = Image.new('RGB', (100, 100), color = 'blue')
    return img1, img2

def verify_service():
    print("Connecting to MongoDB...")
    try:
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
        db = client.fluxframe
        fs = gridfs.GridFS(db)
        client.server_info() # Trigger connection check
    except Exception as e:
        print(f"Skipping verification: MongoDB not accessible on localhost (expected if running in Docker container). Error: {e}")
        return

    print("Creating dummy images in GridFS...")
    img1, img2 = create_dummy_images()
    
    # Save to GridFS
    buf1 = io.BytesIO()
    img1.save(buf1, format="PNG")
    buf1.seek(0)
    id1 = str(fs.put(buf1, filename="test_start.png"))
    
    buf2 = io.BytesIO()
    img2.save(buf2, format="PNG")
    buf2.seek(0)
    id2 = str(fs.put(buf2, filename="test_end.png"))
    
    print(f"Stored images: {id1}, {id2}")
    
    payload = {
        "job_id": "test_job_001",
        "frame_start_id": id1,
        "frame_end_id": id2,
        "cfg_scale": 7.0
    }
    
    print(f"Sending request to {API_URL}...")
    try:
        # Note: This will fail if the service isn't actually running on localhost:8001 yet.
        # This script is intended to be run AFTER 'docker compose up' 
        resp = requests.post(API_URL, json=payload, timeout=5)
        
        if resp.status_code == 200:
            data = resp.json()
            gen_id = data['generated_frame_id']
            print(f"Success! Generated Frame ID: {gen_id}")
            
            # Verify Output
            out_grid = fs.get(gen_id) # Should work if ObjectId
            # Note: My service returns string ID. PyMongo GridFS might need ObjectId wrapping if put returned ObjectId
            # The service converted str(file_id) so we are good. Wait, fs.get(id) needs ObjectId usually.
            
            from bson.objectid import ObjectId
            out_grid = fs.get(ObjectId(gen_id))
            out_img = Image.open(out_grid)
            print(f"Retrieved generated image size: {out_img.size}")
            
            # Check center pixel color (Red + Blue blended = Purple-ish)
            # R=255,0,0 + 0,0,255. Blend 0.5 -> 127, 0, 127
            center_pixel = out_img.getpixel((50, 50))
            print(f"Center Pixel Color: {center_pixel}")
            
            if 100 < center_pixel[0] < 155 and 100 < center_pixel[2] < 155:
                print("Color Verification: PASSED (Purple-ish detected)")
            else:
                print("Color Verification: WARN (Unexpected color, maybe Gemini generated something unique?)")
                
        else:
            print(f"Request failed: {resp.status_code} - {resp.text}")
            
    except requests.exceptions.ConnectionError:
        print("Could not connect to service. Ensure 'docker compose up model-base' is running.")

if __name__ == "__main__":
    verify_service()
