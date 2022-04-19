"""
script to get a new token from auth0
"""

import requests
import os
from dotenv import find_dotenv, load_dotenv
from jose import jwt  # noqa
from calendar import timegm
from datetime import datetime


load_dotenv(find_dotenv())
AUTH0_DOMAIN = os.getenv('AUTH0_DOMAIN')
AUTH0_AUDIENCE = os.getenv('AUTH0_AUDIENCE')
AUTH0_CLIENT_ID = os.getenv('AUTH0_CLIENT_ID')
AUTH0_CLIENT_SECRET = os.getenv('AUTH0_CLIENT_SECRET')
#print("AUTH0_DOMAIN:" + AUTH0_DOMAIN)
#print("AUTH0_AUDIENCE:" + AUTH0_AUDIENCE)
#print("AUTH0_CLIENT_ID:" + AUTH0_CLIENT_ID)

def get_new_token():
    print(AUTH0_DOMAIN);
    response = requests.post(
        url=f'https://{AUTH0_DOMAIN}/oauth/token',
        headers={'Content-Type': 'application/json'},
        json={
            'client_id': AUTH0_CLIENT_ID,
            'client_secret': AUTH0_CLIENT_SECRET,
            'audience': AUTH0_AUDIENCE,
            'grant_type': 'client_credentials'
        }
    )
    return str(response.json()['access_token'])


def get_token():
    try:
        with open('auth/auth_token.txt', 'r') as f:
            token = f.read()
    except FileNotFoundError:
        token = get_new_token()
        with open('auth/auth_token.txt', 'w') as f:
            f.write(token)

    if int(jwt.get_unverified_claims(token)['exp']) - 60 > (timegm(datetime.utcnow().utctimetuple())):
        return {'authorization': 'Bearer ' + token}
    else:
        new_token = get_new_token()
        with open('auth/auth_token.txt', 'w') as f:
            f.write(new_token)
        return {'authorization': 'Bearer ' + new_token}
