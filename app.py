import os
import logging
from flask import Flask, request, send_file, jsonify
import subprocess
import uuid
from werkzeug.utils import secure_filename
from functools import wraps
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv
import requests
from io import BytesIO
import tempfile
import json
import boto3
from botocore.exceptions import ClientError
import threading
import shutil
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Configuration
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'output'
DEBUG_FOLDER = 'debug'
ALLOWED_VIDEO_EXTENSIONS = {'mp4', 'avi', 'mov'}

# AWS Configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# Initialize S3 client if credentials are available
s3_client = None
if AWS_ACCESS_KEY and AWS_SECRET_KEY:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

# Ensure upload, output and debug folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(DEBUG_FOLDER, exist_ok=True)

# Create Flask app
app = Flask(__name__)

# Configure limiter to use in-memory storage
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

# Load API_KEY from environment variable
API_KEY = os.getenv('API_KEY')
if not API_KEY:
    raise ValueError("No API_KEY set for Flask application. Set it in the .env file.")

# Thread-local storage for request-specific data
request_data = threading.local()

def get_request_folder():
    """Get or create a unique folder for the current request"""
    if not hasattr(request_data, 'folder'):
        # Create a unique folder for this request using timestamp and UUID
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        request_id = str(uuid.uuid4())
        request_data.folder = os.path.join(UPLOAD_FOLDER, f"request_{timestamp}_{request_id}")
        os.makedirs(request_data.folder, exist_ok=True)
    return request_data.folder

def cleanup_request_folder():
    """Clean up the request-specific folder"""
    if hasattr(request_data, 'folder') and os.path.exists(request_data.folder):
        try:
            shutil.rmtree(request_data.folder)
        except Exception as e:
            logging.error(f"Error cleaning up request folder {request_data.folder}: {str(e)}")

# Helper functions
def allowed_file(filename, allowed_extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions or filename.lower().endswith(tuple(allowed_extensions))

def merge_audio_files(input_files, output_file):
    input_list = [f"file '{os.path.abspath(f)}'" for f in input_files]
    list_file = os.path.abspath(os.path.join(UPLOAD_FOLDER, 'list.txt'))
    
    with open(list_file, 'w') as f:
        f.write('\n'.join(input_list))
    
    cmd = [
        'ffmpeg',
        '-f', 'concat',
        '-safe', '0',
        '-i', list_file,
        '-c', 'copy',
        output_file
    ]
    
    subprocess.run(cmd, check=True)
    
    os.remove(list_file)
    for file in input_files:
        os.remove(file)

def merge_audio_with_video(audio_file, video_file, output_file):
    cmd = [FFMPEG_PATH, '-i', video_file, '-i', audio_file, '-c:v', 'copy', '-c:a', 'aac', '-map', '0:v:0', '-map', '1:a:0', output_file]
    subprocess.run(cmd, check=True)

def api_key_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.headers.get('X-API-Key') != API_KEY:
            return jsonify({"error": "Invalid API key"}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/merge_audio', methods=['POST'])
@limiter.limit("10 per minute")
def merge_audio():
    if 'X-API-Key' not in request.headers or request.headers['X-API-Key'] != API_KEY:
        return jsonify({'error': 'Invalid API Key'}), 401

    data = request.json
    if not data or 'files' not in data:
        return jsonify({'error': 'No file data in the request'}), 400

    files = data['files']
    if len(files) < 1:
        return jsonify({'error': 'At least one audio file is required'}), 400

    valid_files = []
    for file in files:
        if 'url' not in file or 'filename' not in file:
            return jsonify({'error': 'Invalid file data structure'}), 400

        filename = file['filename']
        if '.' not in filename:
            filename += '.mp3'  # Assume MP3 if no extension is provided

        if allowed_file(filename, ALLOWED_VIDEO_EXTENSIONS):
            try:
                response = requests.get(file['url'])
                response.raise_for_status()
                temp_filename = str(uuid.uuid4()) + os.path.splitext(filename)[1]
                filepath = os.path.join(UPLOAD_FOLDER, temp_filename)
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                valid_files.append(filepath)
            except requests.RequestException as e:
                return jsonify({'error': f'Error downloading file {filename}: {str(e)}'}), 400
        else:
            return jsonify({'error': f'Invalid file type: {filename}'}), 400

    output_filename = f"{uuid.uuid4()}.mp3"
    output_filepath = os.path.join(OUTPUT_FOLDER, output_filename)

    try:
        if len(valid_files) == 1:
            # If only one file, just copy it to the output
            import shutil
            shutil.copy2(valid_files[0], output_filepath)
        else:
            # Merge multiple files
            merge_audio_files(valid_files, output_filepath)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error processing audio files: {str(e)}")
        return jsonify({'error': 'Error processing audio files'}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return jsonify({'error': 'An unexpected error occurred'}), 500
    finally:
        # Clean up temporary files
        for file in valid_files:
            if os.path.exists(file):
                os.remove(file)

    return send_file(output_filepath, as_attachment=True, download_name='processed_audio.mp3')

@app.route('/merge_audio_video', methods=['POST'])
@limiter.limit("10 per minute")
def merge_audio_video():
    if 'audio' not in request.files or 'video' not in request.files:
        return jsonify({"error": "Both audio and video files are required"}), 400
    
    audio_file = request.files['audio']
    video_file = request.files['video']
    
    if not (audio_file and allowed_file(audio_file.filename, ALLOWED_VIDEO_EXTENSIONS)):
        return jsonify({"error": "Invalid audio file type"}), 400
    
    if not (video_file and allowed_file(video_file.filename, ALLOWED_VIDEO_EXTENSIONS)):
        return jsonify({"error": "Invalid video file type"}), 400
    
    audio_filename = secure_filename(audio_file.filename)
    video_filename = secure_filename(video_file.filename)
    
    audio_path = os.path.join(UPLOAD_FOLDER, audio_filename)
    video_path = os.path.join(UPLOAD_FOLDER, video_filename)
    
    audio_file.save(audio_path)
    video_file.save(video_path)
    
    output_filename = f"{uuid.uuid4()}.mp4"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
    try:
        merge_audio_with_video(audio_path, video_path, output_path)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error merging audio with video: {str(e)}")
        return jsonify({"error": "Error merging audio with video"}), 500
    finally:
        os.remove(audio_path)
        os.remove(video_path)
    
    return send_file(output_path, as_attachment=True, download_name=output_filename)

@app.route('/merge_videos', methods=['POST'])
@api_key_required
@limiter.limit("10 per minute")
def merge_videos():
    temp_files = []
    processed_files = []
    list_file = None
    
    try:
        data = request.json
        if not data or 'files' not in data:
            return jsonify({'error': 'No file data in the request'}), 400

        files = data['files']
        if len(files) < 1:
            return jsonify({'error': 'At least one video file is required'}), 400

        # Download and process each video
        for file_data in files:
            if not all(k in file_data for k in ['url', 'filename', 'timestamp']):
                return jsonify({'error': 'Invalid file data structure. Each file must have url, filename, and timestamp'}), 400

            url = file_data['url']
            filename = file_data['filename']
            start_time, end_time = file_data['timestamp']

            # Download from S3
            video_content = download_from_s3(url)
            
            # Save to temporary file
            temp_input = os.path.join(UPLOAD_FOLDER, f"{uuid.uuid4()}.mp4")
            with open(temp_input, 'wb') as f:
                f.write(video_content)
            temp_files.append(temp_input)

            # Process with timestamps
            temp_output = os.path.join(UPLOAD_FOLDER, f"{uuid.uuid4()}.mp4")
            process_video_with_timestamps(temp_input, start_time, end_time, temp_output)
            processed_files.append(temp_output)

        # Create the final merged video
        output_filename = f"{uuid.uuid4()}.mp4"
        output_filepath = os.path.join(OUTPUT_FOLDER, output_filename)

        # Create a file list for ffmpeg
        list_file = os.path.join(UPLOAD_FOLDER, 'file_list.txt')
        with open(list_file, 'w') as f:
            for file in processed_files:
                f.write(f"file '{file}'\n")

        # Merge all processed videos
        merge_cmd = [
            'ffmpeg',
            '-y',
            '-f', 'concat',
            '-safe', '0',
            '-i', list_file,
            '-c:v', 'libx264',
            '-preset', 'medium',
            '-crf', '18',
            '-movflags', '+faststart',
            '-c:a', 'aac',
            '-b:a', '192k',
            '-strict', 'experimental',
            output_filepath
        ]
        subprocess.run(merge_cmd, check=True)

        return send_file(output_filepath, as_attachment=True, download_name='merged_video.mp4')

    except Exception as e:
        logging.error(f"Error in merge_videos: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        # Clean up temporary files
        for file in temp_files + processed_files:
            if os.path.exists(file):
                try:
                    os.remove(file)
                except Exception as e:
                    logging.error(f"Error removing temporary file {file}: {str(e)}")
        if list_file and os.path.exists(list_file):
            try:
                os.remove(list_file)
            except Exception as e:
                logging.error(f"Error removing list file {list_file}: {str(e)}")

@app.route('/process_scene', methods=['POST'])
@limiter.limit("10 per minute")
def process_scene():
    try:
        data = request.json
        if not data or 'shots' not in data:
            return jsonify({'error': 'Missing required data. Need shots'}), 400

        shots = data['shots']
        audio_url = data.get('audio')  # Make audio optional
        scene_id = data.get('scene_id', str(uuid.uuid4()))

        if len(shots) < 1:
            return jsonify({'error': 'At least one shot is required'}), 400

        # Get request-specific folder
        request_folder = get_request_folder()
        temp_files = []
        processed_files = []
        list_file = None

        try:
            # Step 1: Process all shots (cut videos according to timestamps)
            for i, shot in enumerate(shots):
                if not all(k in shot for k in ['url', 'filename']):
                    return jsonify({'error': 'Invalid shot data structure. Each shot must have url and filename'}), 400

                url = shot['url']
                filename = shot['filename']
                timestamp = shot.get('timestamp')
                
                logging.info(f"\nProcessing shot {i+1}/{len(shots)}:")
                logging.info(f"URL: {url}")
                logging.info(f"Filename: {filename}")
                
                if timestamp:
                    start_time, end_time = timestamp
                    logging.info(f"Timestamp: {start_time} to {end_time}")
                    logging.info(f"Duration: {end_time - start_time} seconds")
                else:
                    logging.info("No timestamp provided, will use full video")
                    start_time = None
                    end_time = None

                # Download from S3
                video_content = download_from_s3(url)
                
                # Save to temporary file in request-specific folder
                temp_input = os.path.abspath(os.path.join(request_folder, f"{uuid.uuid4()}.mp4"))
                with open(temp_input, 'wb') as f:
                    f.write(video_content)
                temp_files.append(temp_input)
                logging.info(f"Saved input video to: {temp_input}")

                # Process with timestamps if provided, otherwise use full video
                temp_output = os.path.abspath(os.path.join(request_folder, f"{uuid.uuid4()}.mp4"))
                try:
                    if timestamp:
                        process_video_with_timestamps(temp_input, start_time, end_time, temp_output)
                    else:
                        shutil.copy2(temp_input, temp_output)
                    processed_files.append(temp_output)
                    logging.info(f"Processed video saved to: {temp_output}")
                except Exception as e:
                    logging.error(f"Error processing shot {i+1}: {str(e)}")
                    raise

            # Step 2: Merge all processed videos
            shots_merged = os.path.abspath(os.path.join(request_folder, f"{uuid.uuid4()}.mp4"))
            list_file = os.path.abspath(os.path.join(request_folder, 'shots_list.txt'))
            
            # Create the list file with absolute paths
            with open(list_file, 'w', encoding='utf-8') as f:
                for i, file in enumerate(processed_files):
                    if not os.path.exists(file):
                        logging.error(f"Processed file not found: {file}")
                        raise Exception(f"Processed file not found: {file}")
                    size = os.path.getsize(file)
                    logging.info(f"File {i}: {file} (size: {size} bytes)")
                    if size == 0:
                        logging.error(f"File {file} is empty!")
                    f.write(f"file '{file}'\n")

            # Merge videos
            merge_cmd = [
                'ffmpeg',
                '-y',
                '-f', 'concat',
                '-safe', '0',
                '-i', list_file,
                '-c:v', 'libx264',
                '-preset', 'medium',
                '-crf', '18',
                '-movflags', '+faststart',
                '-c:a', 'aac',
                '-b:a', '192k',
                '-strict', 'experimental',
                shots_merged
            ]
            
            logging.info(f"\nExecuting merge command: {' '.join(merge_cmd)}")
            result = subprocess.run(merge_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logging.error(f"FFmpeg error output: {result.stderr}")
                raise Exception(f"Failed to merge videos: {result.stderr}")

            # Step 3: Add audio if provided
            if audio_url:
                try:
                    audio_content = download_from_s3(audio_url)
                    audio_file = os.path.abspath(os.path.join(request_folder, f"{uuid.uuid4()}.mp3"))
                    with open(audio_file, 'wb') as f:
                        f.write(audio_content)
                    temp_files.append(audio_file)

                    # Final output with audio
                    output_filename = f"scene_{scene_id}.mp4"
                    output_filepath = os.path.abspath(os.path.join(OUTPUT_FOLDER, output_filename))

                    merge_audio_cmd = [
                        'ffmpeg',
                        '-y',
                        '-i', shots_merged,
                        '-i', audio_file,
                        '-c:v', 'copy',
                        '-c:a', 'aac',
                        '-map', '0:v:0',
                        '-map', '1:a:0',
                        output_filepath
                    ]
                    
                    result = subprocess.run(merge_audio_cmd, capture_output=True, text=True)
                    if result.returncode != 0:
                        logging.error(f"FFmpeg error output: {result.stderr}")
                        raise Exception(f"Failed to merge audio: {result.stderr}")
                except Exception as e:
                    logging.warning(f"Error adding audio: {str(e)}, continuing with video only")
                    output_filename = f"scene_{scene_id}.mp4"
                    output_filepath = os.path.abspath(os.path.join(OUTPUT_FOLDER, output_filename))
                    shutil.copy2(shots_merged, output_filepath)
            else:
                # No audio provided, just use the merged video
                output_filename = f"scene_{scene_id}.mp4"
                output_filepath = os.path.abspath(os.path.join(OUTPUT_FOLDER, output_filename))
                shutil.copy2(shots_merged, output_filepath)

            if not os.path.exists(output_filepath) or os.path.getsize(output_filepath) == 0:
                raise Exception("Final output file is empty or does not exist")

            logging.info(f"Final output saved to: {output_filepath}")
            return send_file(output_filepath, as_attachment=True, download_name=output_filename)

        finally:
            # Clean up request-specific folder
            cleanup_request_folder()

    except Exception as e:
        logging.error(f"Error in process_scene: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/merge_scenes', methods=['POST'])
@limiter.limit("10 per minute")
def merge_scenes():
    temp_files = []
    list_file = None
    
    try:
        data = request.json
        if not data or 'scenes' not in data:
            return jsonify({'error': 'No scenes data in the request'}), 400

        scenes = data['scenes']
        if len(scenes) < 1:
            return jsonify({'error': 'At least one scene is required'}), 400

        # Download all scene files
        for scene in scenes:
            if 'url' not in scene:
                return jsonify({'error': 'Invalid scene data structure. Each scene must have url'}), 400

            url = scene['url']
            video_content = download_from_s3(url)
            
            temp_file = os.path.join(UPLOAD_FOLDER, f"{uuid.uuid4()}.mp4")
            with open(temp_file, 'wb') as f:
                f.write(video_content)
            temp_files.append(temp_file)

        # Create final video
        output_filename = f"final_video_{uuid.uuid4()}.mp4"
        output_filepath = os.path.join(OUTPUT_FOLDER, output_filename)

        # Create file list for ffmpeg
        list_file = os.path.join(UPLOAD_FOLDER, 'scenes_list.txt')
        with open(list_file, 'w') as f:
            for file in temp_files:
                f.write(f"file '{file}'\n")
    
        # Merge all scenes
        merge_cmd = [
            'ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', list_file,
            '-c', 'copy',
            output_filepath
        ]
        subprocess.run(merge_cmd, check=True)

        return send_file(output_filepath, as_attachment=True, download_name=output_filename)

    except Exception as e:
        logging.error(f"Error in merge_scenes: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        # Clean up temporary files
        for file in temp_files:
            if os.path.exists(file):
                try:
                    os.remove(file)
                except Exception as e:
                    logging.error(f"Error removing temporary file {file}: {str(e)}")
        if list_file and os.path.exists(list_file):
            try:
                os.remove(list_file)
            except Exception as e:
                logging.error(f"Error removing list file {list_file}: {str(e)}")

def download_from_s3(url):
    """Download a file from S3 URL"""
    try:
        # Log the original URL
        logging.info(f"Original URL: {url}")
        
        # Ensure the URL is properly formatted for public access
        if 's3.' in url and '.amazonaws.com' in url:
            # The URL is already in the correct format for public access
            # Example: https://videoautomation-media-assets.s3.us-east-2.amazonaws.com/...
            pass
        else:
            # If it's using a different format, convert to public endpoint
            parts = url.split('.')
            bucket_name = parts[0].replace('https://', '')
            key = '/'.join(parts[3:])
            url = f'https://{bucket_name}.s3.amazonaws.com/{key}'
        
        logging.info(f"Downloading from URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        # Log the content length to verify we got data
        content_length = len(response.content)
        logging.info(f"Downloaded {content_length} bytes")
        
        if content_length == 0:
            raise Exception("Downloaded file is empty")
            
        # Save the downloaded file to debug folder
        debug_filename = f"downloaded_{uuid.uuid4()}.mp4"
        debug_path = os.path.join(DEBUG_FOLDER, debug_filename)
        with open(debug_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"Saved downloaded file to debug folder: {debug_path}")
            
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading from S3: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response status code: {e.response.status_code}")
            logging.error(f"Response content: {e.response.text}")
        raise

def process_video_with_timestamps(input_file, start_time, end_time, output_file):
    """Process a video file with start and end timestamps"""
    try:
        # Log the processing parameters
        logging.info(f"\n{'='*50}")
        logging.info(f"Processing video: {input_file}")
        logging.info(f"Original timestamps - Start time: {start_time}, End time: {end_time}")
        
        # Check the actual duration of the input video
        probe_cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_file
        ]
        input_duration = float(subprocess.check_output(probe_cmd, text=True).strip())
        logging.info(f"Input video duration: {input_duration} seconds")
        
        # Calculate the duration we want to extract
        target_duration = end_time - start_time
        logging.info(f"Requested duration: {target_duration} seconds")
        
        # If the requested duration is longer than the video, adjust it
        if target_duration > input_duration:
            logging.warning(f"Requested duration ({target_duration}s) is longer than video duration ({input_duration}s). Adjusting to video duration.")
            target_duration = input_duration
        
        # Process the video with the calculated duration
        # Using high quality settings
        cmd = [
            'ffmpeg',
            '-y',  # Overwrite output file if exists
            '-ss', str(start_time),  # Start time
            '-i', input_file,  # Input file
            '-t', str(target_duration),  # Duration
            '-c:v', 'libx264',  # Use x264 encoder
            '-preset', 'medium',  # Balance between speed and quality
            '-crf', '18',  # High quality (lower = better quality, 18 is visually lossless)
            '-movflags', '+faststart',  # Enable fast start for web playback
            '-c:a', 'aac',  # Use AAC for audio
            '-b:a', '192k',  # Good quality audio bitrate
            '-strict', 'experimental',
            output_file
        ]
        
        logging.info(f"Processing video with command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"FFmpeg error output: {result.stderr}")
            raise Exception(f"Failed to process video: {result.stderr}")
            
        # Verify the output file
        if not os.path.exists(output_file):
            raise Exception(f"Output file was not created: {output_file}")
            
        output_size = os.path.getsize(output_file)
        if output_size == 0:
            raise Exception(f"Output file is empty: {output_file}")
            
        logging.info(f"Output file size: {output_size} bytes")
        
        # Copy the processed file to debug folder
        debug_filename = f"processed_{uuid.uuid4()}.mp4"
        debug_path = os.path.join(DEBUG_FOLDER, debug_filename)
        import shutil
        shutil.copy2(output_file, debug_path)
        logging.info(f"Saved processed file to debug folder: {debug_path}")
        
        # Verify the output video duration
        probe_cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            output_file
        ]
        try:
            output_duration = float(subprocess.check_output(probe_cmd, text=True).strip())
            logging.info(f"Output video duration: {output_duration} seconds")
            if abs(output_duration - target_duration) > 0.5:  # Allow 0.5 second tolerance
                logging.warning(f"Output duration ({output_duration}s) differs significantly from target duration ({target_duration}s)")
        except Exception as e:
            logging.warning(f"Could not verify output duration: {str(e)}")
            
        logging.info(f"{'='*50}\n")
            
    except Exception as e:
        logging.error(f"Error processing video: {str(e)}")
        raise

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)