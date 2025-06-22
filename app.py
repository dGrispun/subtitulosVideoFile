import os
import tempfile
import time
from flask import Flask, request, jsonify, Response
import whisper
import math
import soundfile as sf
import numpy as np
import yt_dlp

app = Flask(__name__)
streaming = False

# Función para convertir video a audio
def convert_to_audio(video_path):
    audio_path = video_path.rsplit('.', 1)[0] + '.wav'
    os.system(f'ffmpeg -i "{video_path}" -ac 1 -ar 16000 -c:a pcm_s16le "{audio_path}" -y')
    return audio_path

# Ruta para procesar video local
@app.route('/process_local', methods=['POST'])
def process_local():
    global streaming
    try:
        if streaming:
            return jsonify({'error': 'Ya se está procesando un video'}), 400
        if 'file' not in request.files:
            return jsonify({'error': 'No se proporcionó archivo'}), 400
        file = request.files['file']
        if not file.filename:
            return jsonify({'error': 'Archivo inválido'}), 400
        # Verificar formato permitido
        if not file.filename.lower().endswith(('.mp4', '.avi', '.mov', '.mkv')):
            return jsonify({'error': 'Formato de video no soportado. Usa MP4, AVI, MOV o MKV.'}), 400
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_file:
            file.save(temp_file.name)
            temp_file_path = temp_file.name
        time.sleep(0.1)
        audio_path = convert_to_audio(temp_file_path)
        # Transcribir con Whisper y obtener segmentos
        model = whisper.load_model('base')
        result = model.transcribe(audio_path, language='es', word_timestamps=True)
        os.remove(audio_path)
        os.remove(temp_file_path)
        # Generar texto con marcas de tiempo por segmento
        transcript_with_timestamps = ''
        for segment in result.get('segments', []):
            start = segment['start']
            end = segment['end']
            text = segment['text'].strip()
            # Formato [mm:ss - mm:ss] Texto
            transcript_with_timestamps += f"[{int(start//60):02d}:{int(start%60):02d} - {int(end//60):02d}:{int(end%60):02d}] {text}\n"
        return jsonify({'transcript': transcript_with_timestamps.strip()})
    except Exception as e:
        return jsonify({'error': f'Error al procesar video local: {str(e)}'}), 500

@app.route('/process_local_stream', methods=['POST'])
def process_local_stream():
    import json
    global streaming
    # Limpiar archivo de texto antes de iniciar la transcripción (para ambos endpoints)
    open('texto.txt', 'w', encoding='utf-8').close()
    # Accede a request.files y guarda el archivo ANTES del generador
    if streaming:
        return Response(f"data: {json.dumps({'error': 'Ya se está procesando un video'})}\n\n", mimetype='text/event-stream')
    if 'file' not in request.files:
        return Response(f"data: {json.dumps({'error': 'No se proporcionó archivo'})}\n\n", mimetype='text/event-stream')
    file = request.files['file']
    if not file.filename:
        return Response(f"data: {json.dumps({'error': 'Archivo inválido'})}\n\n", mimetype='text/event-stream')
    if not file.filename.lower().endswith(('.mp4', '.avi', '.mov', '.mkv')):
        return Response(f"data: {json.dumps({'error': 'Formato de video no soportado. Usa MP4, AVI, MOV o MKV.'})}\n\n", mimetype='text/event-stream')
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_file:
        file.save(temp_file.name)
        temp_file_path = temp_file.name
    time.sleep(0.1)
    audio_path = convert_to_audio(temp_file_path)
    chunk_duration = 15  # segundos por chunk
    # Limpiar archivo de texto antes de iniciar la transcripción
    open('texto.txt', 'w', encoding='utf-8').close()
    def generate():
        import sys
        try:
            model = whisper.load_model('base')
            audio, sr = sf.read(audio_path)
            total_duration = len(audio) / sr
            num_chunks = math.ceil(total_duration / chunk_duration)
            full_transcript = ''
            with open('texto.txt', 'a', encoding='utf-8') as ftxt:
                for i in range(num_chunks):
                    start_sec = i * chunk_duration
                    end_sec = min((i + 1) * chunk_duration, total_duration)
                    start_sample = int(start_sec * sr)
                    end_sample = int(end_sec * sr)
                    chunk_audio = audio[start_sample:end_sample]
                    chunk_path = audio_path.replace('.wav', f'_chunk{i}.wav')
                    sf.write(chunk_path, chunk_audio, sr)
                    chunk_result = model.transcribe(chunk_path, language='es', word_timestamps=True)
                    os.remove(chunk_path)
                    for segment in chunk_result.get('segments', []):
                        start = segment['start'] + start_sec
                        end = segment['end'] + start_sec
                        text = segment['text'].strip()
                        ts = f"[{int(start//60):02d}:{int(start%60):02d} - {int(end//60):02d}:{int(end%60):02d}]"
                        segment_text = f'{ts} {text}'
                        full_transcript += segment_text + '\n'
                        ftxt.write(segment_text + '\n')
                        ftxt.flush()
                        os.fsync(ftxt.fileno())
                        yield f"data: {{\"segment\": \"{segment_text}\"}}\n\n"
                        sys.stdout.flush()
            os.remove(audio_path)
            os.remove(temp_file_path)
            # Enviar la transcripción completa al final
            yield f"data: {{\"full_transcript\": {json.dumps(full_transcript.strip())}}}\n\n"
            yield f"data: {{\"done\": true}}\n\n"
        except Exception as e:
            yield f"data: {{\"error\": \"Error al procesar video local: {str(e)}\"}}\n\n"
    return Response(generate(), mimetype='text/event-stream')

@app.route('/process_youtube_stream', methods=['POST'])
def process_youtube_stream():
    import json
    youtube_url = request.form.get('youtube_url')
    if not youtube_url:
        return Response(f"data: {{'error': 'No se proporcionó URL de YouTube'}}\n\n", mimetype='text/event-stream')
    # Limpiar archivo de texto antes de iniciar la transcripción (para ambos endpoints)
    open('texto.txt', 'w', encoding='utf-8').close()
    try:
        # Descargar video de YouTube
        temp_dir = tempfile.mkdtemp()
        ydl_opts = {
            'format': 'bestvideo+bestaudio/best',
            'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
            'merge_output_format': 'mp4',
            'quiet': True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(youtube_url, download=True)
            filename = ydl.prepare_filename(info)
            if not filename.endswith('.mp4'):
                filename = filename.rsplit('.', 1)[0] + '.mp4'
        time.sleep(0.1)
        audio_path = convert_to_audio(filename)
        chunk_duration = 15
        # Limpiar archivo de texto antes de iniciar la transcripción
        open('texto.txt', 'w', encoding='utf-8').close()
        def generate():
            import sys
            try:
                model = whisper.load_model('base')
                audio, sr = sf.read(audio_path)
                total_duration = len(audio) / sr
                num_chunks = math.ceil(total_duration / chunk_duration)
                full_transcript = ''
                with open('texto.txt', 'a', encoding='utf-8') as ftxt:
                    for i in range(num_chunks):
                        start_sec = i * chunk_duration
                        end_sec = min((i + 1) * chunk_duration, total_duration)
                        start_sample = int(start_sec * sr)
                        end_sample = int(end_sec * sr)
                        chunk_audio = audio[start_sample:end_sample]
                        chunk_path = audio_path.replace('.wav', f'_chunk{i}.wav')
                        sf.write(chunk_path, chunk_audio, sr)
                        chunk_result = model.transcribe(chunk_path, language='es', word_timestamps=True)
                        os.remove(chunk_path)
                        for segment in chunk_result.get('segments', []):
                            start = segment['start'] + start_sec
                            end = segment['end'] + start_sec
                            text = segment['text'].strip()
                            ts = f"[{int(start//60):02d}:{int(start%60):02d} - {int(end//60):02d}:{int(end%60):02d}]"
                            segment_text = f'{ts} {text}'
                            full_transcript += segment_text + '\n'
                            ftxt.write(segment_text + '\n')
                            ftxt.flush()
                            os.fsync(ftxt.fileno())
                            yield f"data: {{\"segment\": \"{segment_text}\"}}\n\n"
                            sys.stdout.flush()
                os.remove(audio_path)
                os.remove(filename)
                # Enviar la transcripción completa al final
                yield f"data: {{\"full_transcript\": {json.dumps(full_transcript.strip())}}}\n\n"
                yield f"data: {{\"done\": true}}\n\n"
            except Exception as e:
                yield f"data: {{\"error\": \"Error al procesar video de YouTube: {str(e)}\"}}\n\n"
        return Response(generate(), mimetype='text/event-stream')
    except Exception as e:
        return Response(f"data: {{'error': 'Error al descargar video de YouTube: {str(e)}'}}\n\n", mimetype='text/event-stream')

@app.route('/')
def index():
    return '<h2>Servidor Flask activo. Usa /process_local para transcribir videos.</h2>'

@app.route('/transcribe')
def transcribe_page():
    html = open(os.path.join('templates', 'transcribe.html'), encoding='utf-8').read()
    # Forzar dark theme en el HTML (por si el usuario tiene override)
    html = html.replace('<html', '<html style="background:#181a1b;color:#e0e0e0;"')
    return html

@app.route('/transcribe_stream')
def transcribe_stream_page():
    html = open(os.path.join('templates', 'transcribe_stream.html'), encoding='utf-8').read()
    html = html.replace('<html', '<html style="background:#181a1b;color:#e0e0e0;"')
    return html

if __name__ == '__main__':
    app.run(debug=True)