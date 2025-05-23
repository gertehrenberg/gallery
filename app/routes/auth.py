import os

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

router = APIRouter()

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

SECRET_PATH = 'secrets'
CLIENT_SECRETS_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')

REDIRECT_URI = "http://localhost/gallery/auth/callback"


def get_flow():
    return Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )


def save_credentials_to_file(creds):
    with open(TOKEN_FILE, "w") as token_file:
        token_file.write(creds.to_json())


def load_credentials_from_file():
    if not os.path.exists(TOKEN_FILE):
        return None
    return Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)


@router.get("/auth")
def auth_start():
    flow = get_flow()
    auth_url, _ = flow.authorization_url(
        access_type='offline',
        include_granted_scopes=False,
        prompt='consent'
    )
    return RedirectResponse(auth_url)


@router.get("/auth/callback")
def auth_callback(request: Request):
    flow = get_flow()
    flow.fetch_token(code=request.query_params["code"])
    creds = flow.credentials
    save_credentials_to_file(creds)
    return RedirectResponse("/gallery?page=1&count=1&folder=real")
