#!/usr/bin/env python3
"""
One-time Gmail Monitor OAuth setup.

Gets a refresh token for mail@jvmatches.com and prints the env vars
to paste into your .env and Railway.

Usage:
    python3 scripts/setup_gmail_monitor.py

You'll need:
  - GMAIL_MONITOR_CLIENT_ID    (from Google Cloud Console)
  - GMAIL_MONITOR_CLIENT_SECRET (from Google Cloud Console)

The script opens a browser for the OAuth consent screen,
then exchanges the code for a refresh token automatically.
"""

import http.server
import json
import os
import sys
import threading
import urllib.parse
import urllib.request
import webbrowser

# Scopes needed:
#   readonly — read newsletter emails
#   modify   — mark emails as read (remove UNREAD label)
# NOT gmail.send — keep this account read-only for security
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
]
REDIRECT_PORT = 8765
REDIRECT_URI = f'http://localhost:{REDIRECT_PORT}/oauth/callback'

auth_code: str = ''
server_done = threading.Event()


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if 'code' in params:
            auth_code = params['code'][0]
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<h2>Authorization successful! You can close this tab.</h2>')
        elif 'error' in params:
            self.send_response(400)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(
                f'<h2>Error: {params["error"][0]}</h2>'.encode()
            )
        else:
            self.send_response(404)
            self.end_headers()

        server_done.set()

    def log_message(self, format, *args):
        pass  # suppress access log


def get_authorization_url(client_id: str) -> str:
    params = {
        'client_id': client_id,
        'redirect_uri': REDIRECT_URI,
        'response_type': 'code',
        'scope': ' '.join(SCOPES),
        'access_type': 'offline',
        'prompt': 'consent',  # force refresh_token even if previously authorized
    }
    return 'https://accounts.google.com/o/oauth2/v2/auth?' + urllib.parse.urlencode(params)


def exchange_code_for_tokens(client_id: str, client_secret: str, code: str) -> dict:
    data = urllib.parse.urlencode({
        'client_id': client_id,
        'client_secret': client_secret,
        'code': code,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code',
    }).encode()

    req = urllib.request.Request(
        'https://oauth2.googleapis.com/token',
        data=data,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        method='POST',
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main():
    print('\n=== Gmail Monitor OAuth Setup ===\n')
    print('You need OAuth 2.0 credentials from Google Cloud Console.')
    print('If you already have GOOGLE_OAUTH_CLIENT_ID/SECRET from the outreach app,')
    print('you can reuse them (just authorize the additional Gmail scopes).\n')

    client_id = os.environ.get('GOOGLE_OAUTH_CLIENT_ID') or \
                input('Client ID: ').strip()
    client_secret = os.environ.get('GOOGLE_OAUTH_CLIENT_SECRET') or \
                    input('Client Secret: ').strip()

    if not client_id or not client_secret:
        print('ERROR: Client ID and Secret are required.')
        sys.exit(1)

    # Start local callback server
    server = http.server.HTTPServer(('localhost', REDIRECT_PORT), CallbackHandler)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    # Open browser for consent
    auth_url = get_authorization_url(client_id)
    print(f'\nOpening browser for OAuth consent...')
    print(f'(If browser does not open, visit: {auth_url})\n')
    print(f'IMPORTANT: Sign in as: mail@jvmatches.com\n')
    webbrowser.open(auth_url)

    # Wait for callback
    server_done.wait(timeout=120)
    server.server_close()

    if not auth_code:
        print('ERROR: No authorization code received. Did you complete the consent flow?')
        sys.exit(1)

    print('Authorization code received. Exchanging for tokens...')
    try:
        tokens = exchange_code_for_tokens(client_id, client_secret, auth_code)
    except Exception as exc:
        print(f'ERROR exchanging code: {exc}')
        sys.exit(1)

    refresh_token = tokens.get('refresh_token', '')
    if not refresh_token:
        print('ERROR: No refresh_token in response. Did you use prompt=consent?')
        print(f'Full response: {tokens}')
        sys.exit(1)

    print('\n' + '='*60)
    print('SUCCESS! Add these to your .env and Railway env vars:')
    print('='*60)
    print(f'GMAIL_MONITOR_ADDRESS=mail@jvmatches.com')
    print(f'GMAIL_MONITOR_CLIENT_ID={client_id}')
    print(f'GMAIL_MONITOR_CLIENT_SECRET={client_secret}')
    print(f'GMAIL_MONITOR_REFRESH_TOKEN={refresh_token}')
    print('='*60)
    print('\nTest with:')
    print('  python3 manage.py poll_gmail_inbox')
    print()


if __name__ == '__main__':
    main()
