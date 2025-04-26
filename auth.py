from google_auth_oauthlib.flow import InstalledAppFlow
from pathlib import Path

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

CRED_FILE = Path("secrets/credentials.json")
TOKEN_FILE = Path("secrets/token.json")

# Manuelle Methode – erzeugt URL, du kopierst sie in den Browser
flow = InstalledAppFlow.from_client_secrets_file(str(CRED_FILE), SCOPES)
auth_url, _ = flow.authorization_url(prompt='consent')

print("\n🔗 Öffne diese URL im Browser und gib den Auth-Code unten ein:\n")
print(auth_url)
code = input("\n🔐 Auth-Code: ").strip()

flow.fetch_token(code=code)
creds = flow.credentials

with TOKEN_FILE.open("w") as f:
    f.write(creds.to_json())

print(f"\n✅ Token gespeichert in: {TOKEN_FILE.resolve()}")
