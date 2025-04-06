from google_auth_oauthlib.flow import InstalledAppFlow
import os

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
CRED_FILE = 'secrets/credentials.json'
TOKEN_FILE = 'secrets/token.json'

flow = InstalledAppFlow.from_client_secrets_file(CRED_FILE, SCOPES)

auth_url, _ = flow.authorization_url(prompt='consent')

print("\n🔗 Bitte öffne diese URL in deinem Browser:\n")
print(auth_url)
print("\n⚠️  Du wirst danach einen Code erhalten.\n")

code = input("🔐 Bitte den Auth-Code hier eingeben: ").strip()

flow.fetch_token(code=code)

with open(TOKEN_FILE, 'w') as token:
    token.write(flow.credentials.to_json())

print("\n✅ token.json wurde erfolgreich erstellt!")
