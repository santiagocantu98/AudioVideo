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
from redis import Redis
import requests
from io import BytesIO
from urllib.parse import urlparse, parse_qs

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Configuration
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'output'
ALLOWED_AUDIO_EXTENSIONS = {'mp3', 'wav', 'ogg'}
ALLOWED_VIDEO_EXTENSIONS = {'mp4', 'avi', 'mov'}

# Ensure upload and output folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

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

        if allowed_file(filename, ALLOWED_AUDIO_EXTENSIONS):
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
    
    if not (audio_file and allowed_file(audio_file.filename, ALLOWED_AUDIO_EXTENSIONS)):
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
@limiter.limit("10 per minute")
def merge_videos():
    try:
        if 'X-API-Key' not in request.headers or request.headers['X-API-Key'] != API_KEY:
            return jsonify({'error': 'Invalid API Key'}), 401

        data = request.json
        if not data or 'files' not in data:
            return jsonify({'error': 'No file data in the request'}), 400

        files = data['files']
        if len(files) < 2:
            return jsonify({'error': 'At least two video files are required'}), 400

        valid_files = []
        for file in files:
            if 'url' not in file or 'filename' not in file:
                return jsonify({'error': 'Invalid file data structure'}), 400

            url = file['url']
            filename = file['filename']

            if not filename.lower().endswith(tuple(ALLOWED_VIDEO_EXTENSIONS)):
                return jsonify({'error': f'Invalid file type: {filename}'}), 400

            file_id = extract_file_id_from_drive_link(url)
            if not file_id:
                return jsonify({'error': f'Invalid Google Drive link: {url}'}), 400

            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            response = requests.get(download_url)
            response.raise_for_status()

            temp_filename = str(uuid.uuid4()) + os.path.splitext(filename)[1]
            filepath = os.path.join(UPLOAD_FOLDER, temp_filename)
            with open(filepath, 'wb') as f:
                f.write(response.content)
            valid_files.append(filepath)

        output_filename = f"{uuid.uuid4()}.mp4"
        output_filepath = os.path.join(OUTPUT_FOLDER, output_filename)

        merge_video_files(valid_files, output_filepath)

        return send_file(output_filepath, as_attachment=True, download_name='merged_video.mp4')

    except requests.RequestException as e:
        return jsonify({'error': f'Error downloading file: {str(e)}'}), 400
    except subprocess.CalledProcessError as e:
        logging.error(f"Error merging video files: {str(e)}")
        return jsonify({'error': 'Error merging video files'}), 500
    except Exception as e:
        logging.error(f"Unhandled exception in merge_videos: {str(e)}")
        return jsonify({'error': 'An unexpected error occurred'}), 500
    finally:
        # Clean up temporary files
        for file in valid_files:
            if os.path.exists(file):
                os.remove(file)

def merge_video_files(input_files, output_file):
    input_list = [f"file '{os.path.abspath(f)}'" for f in input_files]
    list_file = os.path.abspath(os.path.join(UPLOAD_FOLDER, 'video_list.txt'))
    
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

def extract_file_id_from_drive_link(url):
    parsed_url = urlparse(url)
    if parsed_url.netloc == 'drive.google.com':
        if parsed_url.path.startswith('/file/d/'):
            return parsed_url.path.split('/')[3]
        elif parsed_url.path == '/open':
            query_params = parse_qs(parsed_url.query)
            return query_params.get('id', [None])[0]
    return None

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)