from email import base64mime
from fastapi import FastAPI, Form, Cookie
from fastapi.responses import Response
from typing import Optional
import hmac
import hashlib
import base64
import json

 
app = FastAPI()

SECRET_KEY = '58a49d17490993211a5e41203f08d6ffe95ccc6db07755e993d0926c19abb075'
PASSWORD_SALT = '52215d6af0181cecedb536e5d59049f193ac5fe11f18a7dfdeec2aaa7b8a2a43'


users = {
    'st1gii@mail.ru' : {
        'name': 'Alexey',
        'password': '89e477d541243e7a4cadf6ed0d9e74fbacc6c957b8a71e8b9ab09720b2875601',
        'balance' : 100000
    },
    'petr@mail.ru' : {
        'name': 'Petr',
        'password': 'b5b4f514c7b824c75af3656539fb50dad308863beb39e2cc8a08657cbd6108b4',
        'balance': 1000
    }
}


def sign_data(data: str) -> str:
    """Возвращает подписанные данные"""
    return hmac.new(SECRET_KEY.encode(),
    msg=data.encode(),
    digestmod=hashlib.sha256).hexdigest().upper()

def get_username_from_sign_string(username_sign: str) -> Optional[str]:
    username_base64, sign = username_sign.split('.')
    username = base64.b64decode(username_base64.encode()).decode()
    valid_sign = sign_data(username)
    if hmac.compare_digest(valid_sign, sign):
        return username

def verify_password(username: str, password: str) -> bool:
    password_hash = hashlib.sha256((password + PASSWORD_SALT).encode()).hexdigest().lower()
    stored_password_hash = users[username]['password'].lower()
    return password_hash == stored_password_hash

@app.get('/')
def index_page(username : Optional[str] = Cookie(default=None)):
    with open('./templates/login.html', 'r', encoding='utf-8') as f:
        login_page = f.read()
    if not username:
        return Response(login_page, media_type='text/html')
    valid_username = get_username_from_sign_string(username)
    if not valid_username:
        response = Response(login_page, media_type='text/html')
        response.delete_cookie(key='username')
        return response
    try:
        user = users[valid_username]
    except KeyError:
        response = Response(login_page, media_type='text/html')
        response.delete_cookie(key='username')
        return response
    return Response(
        f'Привет, {user["name"]}!<br />Баланс {user["balance"]}', media_type='text/html')


@app.post('/login')
def login_page(username : str = Form(...), password : str = Form(...)):
    user = users.get(username)
    if not user or not verify_password(username, password):
        return Response(
            json.dumps({
                'success': False,
                'message': 'Я Вас не знаю'
            }),
            media_type='application/json')
    
    response = Response(
        json.dumps({
            'success': True,
            'message': f'Привет, {user["name"]}!<br />Баланс {user["balance"]}'
        }),
        media_type='application/json')

    username_sign = base64.b64encode(username.encode()).decode() + '.' + \
        sign_data(username)
    response.set_cookie(key='username', value=username_sign)
    return response

