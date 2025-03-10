from google.cloud import bigquery
import jwt
import pandas as pd
import pendulum
import requests

from config import GHOST_ADMIN_API_KEY, GHOST_ADMIN_URL


def generate_jwt():
    id, secret = GHOST_ADMIN_API_KEY.split(':')
    iat = int(pendulum.now().timestamp())
    header = {'alg': 'HS256', 'typ': 'JWT', 'kid': id}
    payload = {'iat': iat, 'exp': iat + 5 * 60, 'aud': '/admin/'}
    return jwt.encode(payload, bytes.fromhex(secret), algorithm='HS256', headers=header)


def fetch_members():
    token = generate_jwt()
    url = f"{GHOST_ADMIN_URL}/members"
    headers = {'Authorization': f'Ghost {token}'}
    page = 1
    members = []
    while True:
        r = requests.get(f"{url}/?limit=all&page={page}", headers=headers)
        if r.status_code == 401:
            headers['Authorization'] = f'Ghost {generate_jwt()}'
            r = requests.get(f"{url}/?limit=all&page={page}", headers=headers)
        if r.status_code == 200:
            data = r.json()
            if data['members']:
                members.extend(data['members'])
                page += 1
            else:
                return members
        else:
            r.raise_for_status()


def fetch_newsletters():
    token = generate_jwt()
    url = f"{GHOST_ADMIN_URL}/posts/?filter=tag:newsletters"
    headers = {'Authorization': f'Ghost {token}'}
    r = requests.get(url, headers=headers)
    data = r.json()
    newsletters = pd.DataFrame(data['posts'])
    newsletters = newsletters[newsletters['status']=='published']
    start_date = pendulum.now("Asia/Seoul").subtract(days=7).date()
    end_date = pendulum.now("Asia/Seoul").date()
    newsletters = newsletters[newsletters['published_at'].apply(
        lambda x: start_date <= pd.to_datetime(x).tz_convert('Asia/Seoul').date() < end_date
    )]
    return newsletters.to_dict(orient='records')


def fetch():
    return {
        'members': fetch_members(), 
        'newsletters': fetch_newsletters()
    }