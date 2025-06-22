from flask import request, Response
import tempfile
import os
import time
import whisper
import math
import soundfile as sf
import yt_dlp
import json

from app import app

@app.route('/process_youtube_stream', methods=['POST'])
def process_youtube_stream():
    return Response(f"data: {{'error': 'No se proporcionó URL de YouTube'}}\n\n", mimetype='text/event-stream')
