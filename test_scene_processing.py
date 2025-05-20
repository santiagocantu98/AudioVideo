import requests
import os
from dotenv import load_dotenv
import json
import time

# Load environment variables from .env file
load_dotenv()

BASE_URL = 'http://localhost:5000'  # Adjust if your app runs on a different port

def test_process_scene():
    """Test the process_scene endpoint with a complete scene"""
    # Test data with all videos alternating
    test_data = {
        "shots": [
            {
                "url": "https://videoautomation-media-assets.s3.us-east-2.amazonaws.com/prompts/7/shots/72/generation_jobs/66/videos/e63eadd3-5b2d-4614-a64a-9c1fa01204c6.mp4",
                "filename": "shot_1.mp4",
                "timestamp": [0, 4]
            },
            {
                "url": "https://videoautomation-media-assets.s3.us-east-2.amazonaws.com/prompts/7/shots/73/d730a78e-5059-41cf-a746-68bf562f93f4.mp4",
                "filename": "shot_2.mp4",
                "timestamp": [0, 4]
            }
        ],
        "audio": "https://videoautomation-media-assets.s3.us-east-2.amazonaws.com/prompts/7/scenes/134/audios/scene_134.mp3",
        "scene_id": "test_scene"
    }

    # URL del endpoint
    url = "http://localhost:5000/process_scene"
    
    # Headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        # Realizar la petición
        response = requests.post(url, headers=headers, json=test_data)
        
        # Verificar si la petición fue exitosa
        if response.status_code == 200:
            # Guardar el archivo de salida
            output_path = os.path.join('output', 'scene_test_scene.mp4')
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Video procesado exitosamente y guardado en: {output_path}")
        else:
            print(f"Error en la petición: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"Error durante la prueba: {str(e)}")

def test_merge_scenes(scene_files):
    """Test merging multiple processed scenes"""
    # Since we're only processing one scene, we don't need to merge
    print("Scene already processed and saved as scene_1_processed.mp4")
    return

def main():
    # Process a single scene
    test_process_scene()

if __name__ == '__main__':
    main() 