import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BASE_URL = 'http://localhost:5000'  # Adjust if your app runs on a different port
API_KEY = os.getenv('API_KEY')

def test_merge_videos():
    json_data = {
        "files": [
            {
                "url": "https://drive.google.com/file/d/1kBwQ9J47EAOlfVj7LMXYdoVBYoU5xBSM/view?usp=drive_link",
                "filename": "video1.mp4"
            },
            {
                "url": "https://drive.google.com/file/d/1kBwQ9J47EAOlfVj7LMXYdoVBYoU5xBSM/view?usp=drive_link",
                "filename": "video2.mp4"
            }
        ]
    }
    headers = {'X-API-Key': API_KEY, 'Content-Type': 'application/json'}
    response = requests.post(f'{BASE_URL}/merge_videos', json=json_data, headers=headers)
    print('Merge Videos Status:', response.status_code)
    if response.status_code == 200:
        with open('merged_video.mp4', 'wb') as f:
            f.write(response.content)
        print('Merged video saved as merged_video.mp4')
    else:
        print('Error:', response.json())

if __name__ == '__main__':
    if not API_KEY:
        print("Error: API_KEY not found in .env file")
    else:
        test_merge_videos()
