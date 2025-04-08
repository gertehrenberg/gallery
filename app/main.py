import io
import os
import base64

from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from PIL import Image, ImageOps

app = FastAPI()

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

FOLDER_ID = os.environ.get("FOLDER_ID", "DEIN_ORDNER_ID_HIER")

SECRET_PATH = 'secrets'
CRED_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')

# Cache: file_id -> orientiertes Vollbild (JPEG-Bytes)
image_cache = {}


def get_drive_service():
    if not os.path.exists(CRED_FILE):
        raise RuntimeError("credentials.json fehlt")

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CRED_FILE, SCOPES)
        creds = flow.run_console()
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)

def download_fullsize_oriented(file_id: str) -> bytes:
    """
    Lädt das Bild von Google Drive, korrigiert EXIF,
    und speichert es als JPEG (unverkleinert).
    """
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)

    image = Image.open(fh)
    image = ImageOps.exif_transpose(image)

    out_fh = io.BytesIO()
    image.save(out_fh, format='JPEG')  # Immer JPEG
    out_fh.seek(0)
    return out_fh.read()

def scale_image_to_400(image_data: bytes) -> bytes:
    """
    Nimmt orientierte Vollgröße als Bytes (JPEG),
    skaliert auf max. 400 px und gibt sie als Bytes zurück.
    """
    fh = io.BytesIO(image_data)
    img = Image.open(fh)

    max_dim = 400
    width, height = img.size
    if width > max_dim or height > max_dim:
        scale = min(max_dim / width, max_dim / height)
        new_width = int(width * scale)
        new_height = int(height * scale)
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    out_fh = io.BytesIO()
    img.save(out_fh, format='JPEG')
    out_fh.seek(0)
    return out_fh.read()

def download_text_file(file_id: str) -> str:
    """Liest den Inhalt einer .txt-Datei von Google Drive als String."""
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read().decode('utf-8', errors='replace')

def find_text_file_id(text_filename: str) -> str | None:
    """
    Sucht genau eine .txt Datei namens text_filename
    im gleichen Ordner (FOLDER_ID). Liefert deren ID zurück oder None.
    """
    service = get_drive_service()
    # Name muss exakt text_filename sein
    # und darf nicht gelöscht sein
    # MIME-Type egal, damit wir nicht auf 'text/plain' beschränkt sind.
    # Bei Bedarf kann man 'and mimeType = "text/plain"' ergänzen,
    # wenn man sicher ist, dass es so erkannt wird.
    newline_filename = text_filename + "\n"  # Füge ein '\n' an
    q = f"'{FOLDER_ID}' in parents and trashed=false and name='{newline_filename}'"
    response = service.files().list(q=q, fields="files(id, name)").execute()
    files = response.get('files', [])
    if len(files) >= 1:
        return files[0]['id']
    return None

@app.get("/", response_class=HTMLResponse)
def show_three_images():
    """
    Zeigt bis zu 3 Bilder aus dem angegebenen FOLDER_ID als Thumbnails,
    jeweils mit darunterstehendem Text (falls eine .txt-Datei existiert).
    """
    try:
        service = get_drive_service()

        # 1) BILDER LADEN
        query = f"'{FOLDER_ID}' in parents and trashed = false and mimeType contains 'image/'"
        results = service.files().list(q=query, pageSize=3, fields="files(id, name, mimeType)").execute()
        files = results.get('files', [])
        if not files:
            return "<html><body><h1>Keine Bilder gefunden</h1></body></html>"

        three_files = files[:3]
        images_html_parts = []

        for f in three_files:
            file_id = f['id']
            file_name = f['name']  # z.B. "bild.jpg"

            # Vollbild laden + cache
            if file_id not in image_cache:
                try:
                    image_cache[file_id] = download_fullsize_oriented(file_id)
                except Exception as e:
                    images_html_parts.append(
                        f'<div style="margin: 5px;"><p>Fehler bei {file_name}: {str(e)}</p></div>'
                    )
                    continue

            # Erzeuge Thumbnail
            scaled_data = scale_image_to_400(image_cache[file_id])
            thumbnail_src = f"data:image/jpeg;base64,{base64.b64encode(scaled_data).decode('utf-8')}"

            # Passende Textdatei (on-demand) suchen: z.B. "bild.jpg.txt"
            txt_filename = file_name + ".txt"
            txt_id = find_text_file_id(txt_filename)
            if txt_id:
                try:
                    text_content = download_text_file(txt_id)
                except Exception as e:
                    text_content = f"Fehler beim Laden von {txt_filename}: {str(e)}"
            else:
                text_content = f"Keine Textdatei <b>{txt_filename}</b> gefunden."

            # HTML
            img_html = f"""
<div class='eintrag'>
  <img src='{thumbnail_src}' alt='{file_name}' class='bild' onclick='openLightbox("{file_id}")'>
  <div class='bildname'>{file_name}</div>
  <form class='checkbox-container'>
    <label><input type='checkbox' name='{file_name}_delete'> Löschen</label>
    <label><input type='checkbox' name='{file_name}_recheck'> Neu beurteilen</label>
    <label><input type='checkbox' name='{file_name}_bad'> Schlecht</label>
    <label><input type='checkbox' name='{file_name}_sex'> Sex</label>
    <label><input type='checkbox' name='{file_name}_animal'> Tiere</label>
  </form>
  <div class='text'>{text_content}</div>
</div>
"""
            images_html_parts.append(img_html)

        # Gesamtes HTML
        final_html = f"""
<!DOCTYPE html>
<html lang='de'>
<head>
  <meta charset='UTF-8'>
  <title>Galerie – Seite 1</title>
  <style>
    body {{
      font-family: sans-serif;
      margin: 0;
      background: #f7f7f7;
    }}
    .sticky-nav {{
      position: sticky;
      top: 0;
      background: #fff;
      padding: 10px;
      border-bottom: 1px solid #ccc;
      z-index: 1000;
      text-align: center;
    }}
    .sticky-nav a {{
      margin: 0 8px;
      text-decoration: none;
      font-weight: bold;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(6, 1fr);
      gap: 20px;
      padding: 20px;
    }}
    .eintrag {{
      grid-column: span 2;
      background: white;
      border: 1px solid #ddd;
      border-radius: 10px;
      padding: 10px;
      box-shadow: 2px 2px 6px rgba(0,0,0,0.1);
      display: flex;
      flex-direction: column;
      gap: 10px;
    }}
    .bild-links {{
      text-align: center;
      font-size: 0.8em;
      margin-bottom: 8px;
    }}
    .bild-links a {{
      margin: 0 5px;
      text-decoration: none;
      color: #0077cc;
    }}
    .bild {{
      display: block;
      margin: 0 auto;
      max-width: 400px;
      height: auto;
      border-radius: 5px;
      cursor: zoom-in;
      transition: transform 0.2s ease;
    }}
    .bild:hover {{
      transform: scale(1.03);
    }}
    .bildname {{
      text-align: center;
      font-weight: bold;
      margin-top: 8px;
    }}
    .text {{
      white-space: pre-wrap;
    }}
    .highlight {{
      background: yellow;
      color: red;
      font-weight: bold;
    }}
    .highlight-green {{
      background: #ccffcc;
      color: green;
      font-weight: bold;
    }}
    .lightbox {{
      display: flex;
      align-items: center;
      justify-content: center;
      position: fixed;
      z-index: 9999;
      left: 0;
      top: 0;
      width: 100%;
      height: 100%;
      background-color: rgba(0,0,0,0.9);
    }}
    .lightbox img {{
      max-width: 90%;
      max-height: 90%;
      box-shadow: 0 0 20px rgba(255,255,255,0.3);
    }}
    .checkbox-container {{
      display: flex;
      justify-content: center;
      gap: 10px;
      margin-top: 10px;
    }}
  </style>
</head>
<body>
<div class='sticky-nav'>
  <a href='#'>⏮ Anfang</a>
  <a href='#'>⏪ -1</a>
  <a href='#'>⬅ Zurück</a>
  <span style='margin: 0 15px; font-weight: bold;'>Seite 1</span>
  <a href='#'>Weiter ➡</a>
  <a href='#'>⏩ +1</a>
  <a href='#'>⏭ Ende</a>
</div>
<div class='grid'>
{''.join(images_html_parts)}
</div>
<script>
function openLightbox(fileId) {{
  const overlay = document.createElement('div');
  overlay.className = 'lightbox';
  overlay.innerHTML = '<img src="/original/' + fileId + '" alt="">';
  overlay.addEventListener('click', () => overlay.remove());
  document.body.appendChild(overlay);
  }}
</script>
</body>
</html>
"""
        return final_html

    except Exception as e:
        return f"<html><body><h1>Error: {str(e)}</h1></body></html>"

@app.get("/original/{file_id}")
def show_original_image(file_id: str):
    """
    Zeige das orientierte Vollbild aus dem Cache (JPEG).
    """
    if file_id not in image_cache:
        return HTMLResponse(
            f"<p>Fehler: Kein Bild mit file_id={file_id} im Cache.</p>",
            status_code=404
        )
    return StreamingResponse(io.BytesIO(image_cache[file_id]), media_type="image/jpeg")
