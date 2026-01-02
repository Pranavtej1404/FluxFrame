import os
import cv2
import numpy as np
from glob import glob

def load_frames(frames_dir):
    """
    Load frame paths sorted by index.
    Assumes naming convention frame_%06d.png
    """
    return sorted(glob(os.path.join(frames_dir, "frame_*.png")))

def calculate_histogram_diff(frame1, frame2):
    """
    Calculate the chi-square distance between two frame histograms.
    Input frames should be in BGR (standard OpenCV).
    """
    # Convert to HSV for better color perception matching, or just use GRAY for speed
    # Using HSV is better for shot detection usually
    hsv1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2HSV)
    hsv2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2HSV)
    
    # Calculate histograms for H, S, and V channels
    # 50 bins for Hue, 60 for Saturation, 60 for Value
    hist1 = cv2.calcHist([hsv1], [0, 1, 2], None, [50, 60, 60], [0, 180, 0, 256, 0, 256])
    hist2 = cv2.calcHist([hsv2], [0, 1, 2], None, [50, 60, 60], [0, 180, 0, 256, 0, 256])
    
    cv2.normalize(hist1, hist1, 0, 1, cv2.NORM_MINMAX)
    cv2.normalize(hist2, hist2, 0, 1, cv2.NORM_MINMAX)
    
    # Compare methods: CORREL, CHISQR, INTERSECT, BHATTACHARYYA
    # Bhattacharyya is good (low is match, high is mismatch)
    score = cv2.compareHist(hist1, hist2, cv2.HISTCMP_BHATTACHARYYA)
    return score

def detect_scenes(frame_paths, threshold=0.3):
    """
    Detect shot boundaries using histogram differences.
    Returns a list of segments: [{'start': 0, 'end': 15, 'type': 'normal'}, ...]
    """
    if not frame_paths:
        return []

    cuts = [0] # List of frame indices where cuts happen
    
    # We don't need to process every pixel for shot detection. Resize helps speed.
    process_size = (128, 128) 
    
    prev_frame = cv2.imread(frame_paths[0])
    prev_frame = cv2.resize(prev_frame, process_size)
    
    for i in range(1, len(frame_paths)):
        curr_frame = cv2.imread(frame_paths[i])
        curr_frame = cv2.resize(curr_frame, process_size)
        
        diff = calculate_histogram_diff(prev_frame, curr_frame)
        
        if diff > threshold:
            cuts.append(i)
            
        prev_frame = curr_frame
        
    # Create segments
    segments = []
    total_frames = len(frame_paths)
    
    for i in range(len(cuts)):
        start = cuts[i]
        end = cuts[i+1] - 1 if i < len(cuts) - 1 else total_frames - 1
        segments.append({
            "start": start,
            "end": end,
            "type": "shot"
        })
        
    return segments

def analyze_motion_optical_flow(frame_paths, sample_rate=5):
    """
    Calculate average motion score using Farneback Optical Flow.
    Skipping frames (sample_rate) to speed up processing.
    """
    if not frame_paths:
        return 0, []
        
    motion_scores = []
    total_motion = 0.0
    count = 0
    
    # Process at reduced resolution
    process_size = (320, 240)
    
    prev_frame = cv2.imread(frame_paths[0])
    prev_frame = cv2.resize(prev_frame, process_size)
    prev_gray = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY)
    
    for i in range(1, len(frame_paths), sample_rate):
        curr_frame = cv2.imread(frame_paths[i])
        curr_frame = cv2.resize(curr_frame, process_size)
        curr_gray = cv2.cvtColor(curr_frame, cv2.COLOR_BGR2GRAY)
        
        flow = cv2.calcOpticalFlowFarneback(
            prev_gray, curr_gray, None, 
            pyr_scale=0.5, levels=3, winsize=15, 
            iterations=3, poly_n=5, poly_sigma=1.2, flags=0
        )
        
        # Calculate magnitude
        magnitude, _ = cv2.cartToPolar(flow[..., 0], flow[..., 1])
        avg_magnitude = np.mean(magnitude)
        
        motion_scores.append({
            "frame_index": i,
            "score": float(avg_magnitude)
        })
        total_motion += avg_magnitude
        count += 1
        
        prev_gray = curr_gray
        
    avg_score = total_motion / count if count > 0 else 0
    return avg_score, motion_scores

def determine_interpolation_params(avg_motion_score, fps):
    """
    Heuristic to decide interpolation factor.
    High motion -> Fewer interpolated frames (harder for model).
    Low motion -> More interpolated frames (easier).
    """
    # These thresholds are arbitrary and should be tuned
    if avg_motion_score > 5.0:
        interp_factor = 2 # 2x interpolation (30 -> 60)
        mode = "conservative"
    elif avg_motion_score > 2.0:
        interp_factor = 4 # 4x interpolation (30 -> 120)
        mode = "balanced"
    else:
        interp_factor = 8 # 8x interpolation (Smoooooth)
        mode = "aggressive"
        
    # Cap total FPS to reasonable limit (e.g., 240fps)
    if fps * interp_factor > 240:
        interp_factor = max(1, int(240 / fps))
        
    return {
        "factor": interp_factor,
        "mode": mode,
        "base_motion_score": float(avg_motion_score)
    }

def analyze_video(frames_dir, fps=30):
    """
    Main entry point for preprocessing worker.
    """
    frame_paths = load_frames(frames_dir)
    
    if not frame_paths:
        return None
        
    # 1. Detect Shots
    shot_segments = detect_scenes(frame_paths)
    
    # 2. Analyze Motion
    global_motion_score, motion_scores = analyze_motion_optical_flow(frame_paths)
    
    # 3. Determine Parameters
    interp_params = determine_interpolation_params(global_motion_score, fps)
    
    return {
        "shot_segments": shot_segments,
        "motion_analysis": {
            "average_score": float(global_motion_score),
            "frame_scores": motion_scores
        },
        "interpolation_params": interp_params,
        "frame_count": len(frame_paths)
    }
