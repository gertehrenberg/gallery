from fastapi import HTTPException
from starlette.requests import Request  # Importiere Request von starlette


def require_login(request: Request):
    """
    Überprüft, ob ein Benutzer angemeldet ist.

    Diese Funktion ist eine Abhängigkeit, die in FastAPI-Routen verwendet wird,
    um sicherzustellen, dass nur authentifizierte Benutzer auf bestimmte Routen zugreifen können.
    """
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=307, headers={"Location": "/gallery/login"})
    return user
