from urllib.parse import urlencode

from fastapi import APIRouter, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse

from app.config import Settings, UserType

router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request):
    error = request.query_params.get("error")
    return f"""
    <html>
    <head>
        <title>Login</title>
        <style>
            body {{
                background-color: #f9f9fb;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                font-family: Arial, sans-serif;
            }}
            .login-box {{
                background: white;
                padding: 2em;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
                width: 320px;
            }}
            h2 {{
                text-align: center;
                margin-bottom: 1em;
            }}
            form {{
                display: flex;
                flex-direction: column;
            }}
            input {{
                margin-bottom: 1em;
                padding: 0.75em;
                border: 1px solid #ccc;
                border-radius: 4px;
            }}
            .error {{
                color: red;
                text-align: center;
                margin-bottom: 1em;
            }}
            button {{
                background-color: #ff6a5f;
                color: white;
                border: none;
                padding: 0.75em;
                border-radius: 4px;
                cursor: pointer;
                font-weight: bold;
            }}
            button:hover {{
                background-color: #e95a50;
            }}
        </style>
    </head>
    <body>
        <div class="login-box">
            <h2>Sign in</h2>
            {'<div class="error">' + error + '</div>' if error else ''}
            <form action="/gallery/login" method="post">
                <input type="text" name="username" placeholder="Username" required>
                <input type="password" name="password" placeholder="Password" required>
                <button type="submit">Sign in</button>
            </form>
        </div>
    </body>
    </html>
    """


@router.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == "guest" and password == "*guest*":
        request.session["user"] = username
        Settings.set_user_type(UserType.GUEST)
        return RedirectResponse(
            url="/gallery/?page=1&count=6&folder=real&textflag=4",
            status_code=status.HTTP_302_FOUND)
    elif username == "admin" and password == "*admin*":
        request.session["user"] = username
        Settings.set_user_type(UserType.ADMIN)
        return RedirectResponse(
            url="/gallery/?page=1&count=6&folder=real&textflag=4",
            status_code=status.HTTP_302_FOUND)

    # Ungültige Anmeldedaten
    query = urlencode({"error": "Ungültige Anmeldedaten"})
    return RedirectResponse(
        url=f"/gallery/login?{query}",
        status_code=status.HTTP_302_FOUND
    )


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/gallery/login")
