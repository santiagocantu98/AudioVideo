import os
import requests
from dotenv import load_dotenv
from app import merge_audio  # Import the merge_audio function directly

# Load environment variables from .env file
load_dotenv()

BASE_URL = 'http://localhost:5000'  # Adjust if your app runs on a different port
API_KEY = os.getenv('API_KEY')

def test_merge_audio_api():
    json_data = {
        "files": [
            {
                "url": "https://example.com/test_audio1.mp3",
                "filename": "test_audio1.mp3"
            },
            {
                "url": "https://example.com/test_audio2.mp3",
                "filename": "test_audio2.mp3"
            }
        ]
    }
    headers = {'X-API-Key': API_KEY, 'Content-Type': 'application/json'}
    response = requests.post(f'{BASE_URL}/merge_audio', json=json_data, headers=headers)
    print('Merge Audio API Status:', response.status_code)
    if response.status_code == 200:
        with open('merged_audio_api.mp3', 'wb') as f:
            f.write(response.content)
        print('Merged audio saved as merged_audio_api.mp3')
    else:
        print('Error:', response.json())

def test_merge_audio_direct():
    # Prepare test audio files
    test_files = [
        {
            "url": "file:///" + os.path.abspath("test_audio1.mp3"),
            "filename": "test_audio1.mp3"
        },
        {
            "url": "file:///" + os.path.abspath("test_audio2.mp3"),
            "filename": "test_audio2.mp3"
        }
    ]

    json_data = {
        "files": test_files
    }

    # Simulate the merge_audio function call
    response = merge_audio(json_data)

    # Check if the response is successful
    if response[1] == 200:
        with open('merged_audio_direct.mp3', 'wb') as f:
            f.write(response[0].read())
        print('Merged audio saved as merged_audio_direct.mp3')
    else:
        print('Error:', response[0].json())

if __name__ == '__main__':
    if not API_KEY:
        print("Error: API_KEY not found in .env file")
    else:
        # test_merge_audio_api()
        test_merge_audio_direct()
