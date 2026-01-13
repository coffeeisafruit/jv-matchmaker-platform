"""
Email Service for sending emails via Gmail and Outlook OAuth.

Handles OAuth token management, email sending, and tracking.
"""

import base64
import logging
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from urllib.parse import urlencode

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


class EmailService:
    """Service for sending emails via connected OAuth accounts."""

    def __init__(self, email_connection):
        """
        Initialize the email service with a connected email account.

        Args:
            email_connection: EmailConnection model instance
        """
        self.connection = email_connection
        self.provider = email_connection.provider

    def _refresh_token_if_needed(self) -> bool:
        """
        Refresh the access token if it's expired.

        Returns:
            True if token is valid (or refreshed), False if refresh failed
        """
        if not self.connection.is_token_expired():
            return True

        if self.provider == 'gmail':
            return self._refresh_google_token()
        elif self.provider == 'outlook':
            return self._refresh_microsoft_token()

        return False

    def _refresh_google_token(self) -> bool:
        """Refresh Google OAuth token."""
        try:
            import requests

            response = requests.post(
                'https://oauth2.googleapis.com/token',
                data={
                    'client_id': settings.GOOGLE_OAUTH_CLIENT_ID,
                    'client_secret': settings.GOOGLE_OAUTH_CLIENT_SECRET,
                    'refresh_token': self.connection.refresh_token,
                    'grant_type': 'refresh_token',
                }
            )

            if response.status_code == 200:
                data = response.json()
                self.connection.access_token = data['access_token']
                expires_in = data.get('expires_in', 3600)
                self.connection.token_expires_at = timezone.now() + timedelta(seconds=expires_in)
                self.connection.save(update_fields=['_access_token', 'token_expires_at', 'updated_at'])
                logger.info(f"Refreshed Google token for {self.connection.email_address}")
                return True
            else:
                logger.error(f"Failed to refresh Google token: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error refreshing Google token: {e}")
            return False

    def _refresh_microsoft_token(self) -> bool:
        """Refresh Microsoft OAuth token."""
        try:
            from msal import ConfidentialClientApplication

            app = ConfidentialClientApplication(
                settings.MICROSOFT_OAUTH_CLIENT_ID,
                authority="https://login.microsoftonline.com/common",
                client_credential=settings.MICROSOFT_OAUTH_CLIENT_SECRET,
            )

            result = app.acquire_token_by_refresh_token(
                self.connection.refresh_token,
                scopes=settings.MICROSOFT_OAUTH_SCOPES,
            )

            if 'access_token' in result:
                self.connection.access_token = result['access_token']
                if 'refresh_token' in result:
                    self.connection.refresh_token = result['refresh_token']
                expires_in = result.get('expires_in', 3600)
                self.connection.token_expires_at = timezone.now() + timedelta(seconds=expires_in)
                self.connection.save(update_fields=['_access_token', '_refresh_token', 'token_expires_at', 'updated_at'])
                logger.info(f"Refreshed Microsoft token for {self.connection.email_address}")
                return True
            else:
                logger.error(f"Failed to refresh Microsoft token: {result.get('error_description', 'Unknown error')}")
                return False

        except Exception as e:
            logger.error(f"Error refreshing Microsoft token: {e}")
            return False

    def send_email(
        self,
        to_email: str,
        subject: str,
        body: str,
        to_name: str = '',
        is_html: bool = False,
    ) -> dict:
        """
        Send an email through the connected account.

        Args:
            to_email: Recipient email address
            subject: Email subject line
            body: Email body (plain text or HTML)
            to_name: Optional recipient name
            is_html: Whether the body is HTML

        Returns:
            Dict with 'success', 'message_id', 'thread_id', 'error' keys
        """
        # Ensure we have a valid token
        if not self._refresh_token_if_needed():
            return {
                'success': False,
                'error': 'Failed to refresh authentication token. Please reconnect your email.',
                'message_id': None,
                'thread_id': None,
            }

        # Send via appropriate provider
        if self.provider == 'gmail':
            return self._send_via_gmail(to_email, subject, body, to_name, is_html)
        elif self.provider == 'outlook':
            return self._send_via_outlook(to_email, subject, body, to_name, is_html)

        return {
            'success': False,
            'error': f'Unknown email provider: {self.provider}',
            'message_id': None,
            'thread_id': None,
        }

    def _send_via_gmail(
        self,
        to_email: str,
        subject: str,
        body: str,
        to_name: str = '',
        is_html: bool = False,
    ) -> dict:
        """Send email via Gmail API."""
        try:
            from googleapiclient.discovery import build
            from google.oauth2.credentials import Credentials

            # Build credentials
            credentials = Credentials(
                token=self.connection.access_token,
                refresh_token=self.connection.refresh_token,
                token_uri='https://oauth2.googleapis.com/token',
                client_id=settings.GOOGLE_OAUTH_CLIENT_ID,
                client_secret=settings.GOOGLE_OAUTH_CLIENT_SECRET,
            )

            # Build Gmail service
            service = build('gmail', 'v1', credentials=credentials)

            # Create message
            if is_html:
                message = MIMEMultipart('alternative')
                message.attach(MIMEText(body, 'plain'))
                message.attach(MIMEText(body, 'html'))
            else:
                message = MIMEText(body)

            message['to'] = f'"{to_name}" <{to_email}>' if to_name else to_email
            message['from'] = self.connection.email_address
            message['subject'] = subject

            # Encode message
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')

            # Send message
            result = service.users().messages().send(
                userId='me',
                body={'raw': raw_message}
            ).execute()

            # Update last used timestamp
            self.connection.last_used_at = timezone.now()
            self.connection.save(update_fields=['last_used_at'])

            logger.info(f"Sent email via Gmail to {to_email}")
            return {
                'success': True,
                'message_id': result.get('id'),
                'thread_id': result.get('threadId'),
                'error': None,
            }

        except Exception as e:
            logger.error(f"Error sending via Gmail: {e}")
            return {
                'success': False,
                'message_id': None,
                'thread_id': None,
                'error': str(e),
            }

    def _send_via_outlook(
        self,
        to_email: str,
        subject: str,
        body: str,
        to_name: str = '',
        is_html: bool = False,
    ) -> dict:
        """Send email via Microsoft Graph API."""
        try:
            import requests

            # Build message payload
            message_payload = {
                'message': {
                    'subject': subject,
                    'body': {
                        'contentType': 'HTML' if is_html else 'Text',
                        'content': body,
                    },
                    'toRecipients': [
                        {
                            'emailAddress': {
                                'address': to_email,
                                'name': to_name or to_email,
                            }
                        }
                    ],
                },
                'saveToSentItems': True,
            }

            # Send via Graph API
            response = requests.post(
                'https://graph.microsoft.com/v1.0/me/sendMail',
                headers={
                    'Authorization': f'Bearer {self.connection.access_token}',
                    'Content-Type': 'application/json',
                },
                json=message_payload,
            )

            if response.status_code == 202:
                # Update last used timestamp
                self.connection.last_used_at = timezone.now()
                self.connection.save(update_fields=['last_used_at'])

                logger.info(f"Sent email via Outlook to {to_email}")
                return {
                    'success': True,
                    'message_id': None,  # Graph API doesn't return message ID on send
                    'thread_id': None,
                    'error': None,
                }
            else:
                error_data = response.json() if response.text else {}
                error_msg = error_data.get('error', {}).get('message', response.text)
                logger.error(f"Error sending via Outlook: {error_msg}")
                return {
                    'success': False,
                    'message_id': None,
                    'thread_id': None,
                    'error': error_msg,
                }

        except Exception as e:
            logger.error(f"Error sending via Outlook: {e}")
            return {
                'success': False,
                'message_id': None,
                'thread_id': None,
                'error': str(e),
            }


class OAuthHelper:
    """Helper class for OAuth authentication flows."""

    @staticmethod
    def get_google_auth_url(state: str) -> str:
        """
        Get Google OAuth authorization URL.

        Args:
            state: State parameter for CSRF protection

        Returns:
            Authorization URL to redirect user to
        """
        params = {
            'client_id': settings.GOOGLE_OAUTH_CLIENT_ID,
            'redirect_uri': settings.GOOGLE_OAUTH_REDIRECT_URI,
            'scope': ' '.join(settings.GOOGLE_OAUTH_SCOPES),
            'response_type': 'code',
            'access_type': 'offline',
            'prompt': 'consent',  # Force consent to get refresh token
            'state': state,
        }
        return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"

    @staticmethod
    def exchange_google_code(code: str) -> dict:
        """
        Exchange Google authorization code for tokens.

        Args:
            code: Authorization code from OAuth callback

        Returns:
            Dict with access_token, refresh_token, expires_in, email
        """
        import requests

        # Exchange code for tokens
        token_response = requests.post(
            'https://oauth2.googleapis.com/token',
            data={
                'client_id': settings.GOOGLE_OAUTH_CLIENT_ID,
                'client_secret': settings.GOOGLE_OAUTH_CLIENT_SECRET,
                'code': code,
                'grant_type': 'authorization_code',
                'redirect_uri': settings.GOOGLE_OAUTH_REDIRECT_URI,
            }
        )

        if token_response.status_code != 200:
            raise Exception(f"Token exchange failed: {token_response.text}")

        token_data = token_response.json()

        # Get user email
        userinfo_response = requests.get(
            'https://www.googleapis.com/oauth2/v2/userinfo',
            headers={'Authorization': f'Bearer {token_data["access_token"]}'}
        )

        if userinfo_response.status_code != 200:
            raise Exception(f"Failed to get user info: {userinfo_response.text}")

        user_data = userinfo_response.json()

        return {
            'access_token': token_data['access_token'],
            'refresh_token': token_data.get('refresh_token'),
            'expires_in': token_data.get('expires_in', 3600),
            'email': user_data['email'],
            'scopes': token_data.get('scope', '').split(' '),
        }

    @staticmethod
    def get_microsoft_auth_url(state: str) -> str:
        """
        Get Microsoft OAuth authorization URL.

        Args:
            state: State parameter for CSRF protection

        Returns:
            Authorization URL to redirect user to
        """
        params = {
            'client_id': settings.MICROSOFT_OAUTH_CLIENT_ID,
            'redirect_uri': settings.MICROSOFT_OAUTH_REDIRECT_URI,
            'scope': ' '.join(settings.MICROSOFT_OAUTH_SCOPES),
            'response_type': 'code',
            'response_mode': 'query',
            'state': state,
        }
        return f"https://login.microsoftonline.com/common/oauth2/v2.0/authorize?{urlencode(params)}"

    @staticmethod
    def exchange_microsoft_code(code: str) -> dict:
        """
        Exchange Microsoft authorization code for tokens.

        Args:
            code: Authorization code from OAuth callback

        Returns:
            Dict with access_token, refresh_token, expires_in, email
        """
        import requests

        # Exchange code for tokens
        token_response = requests.post(
            'https://login.microsoftonline.com/common/oauth2/v2.0/token',
            data={
                'client_id': settings.MICROSOFT_OAUTH_CLIENT_ID,
                'client_secret': settings.MICROSOFT_OAUTH_CLIENT_SECRET,
                'code': code,
                'grant_type': 'authorization_code',
                'redirect_uri': settings.MICROSOFT_OAUTH_REDIRECT_URI,
                'scope': ' '.join(settings.MICROSOFT_OAUTH_SCOPES),
            }
        )

        if token_response.status_code != 200:
            raise Exception(f"Token exchange failed: {token_response.text}")

        token_data = token_response.json()

        # Get user email from Graph API
        userinfo_response = requests.get(
            'https://graph.microsoft.com/v1.0/me',
            headers={'Authorization': f'Bearer {token_data["access_token"]}'}
        )

        if userinfo_response.status_code != 200:
            raise Exception(f"Failed to get user info: {userinfo_response.text}")

        user_data = userinfo_response.json()

        return {
            'access_token': token_data['access_token'],
            'refresh_token': token_data.get('refresh_token'),
            'expires_in': token_data.get('expires_in', 3600),
            'email': user_data.get('mail') or user_data.get('userPrincipalName'),
            'scopes': token_data.get('scope', '').split(' '),
        }


def generate_mailto_link(
    to_email: str,
    subject: str,
    body: str,
    to_name: str = '',
) -> str:
    """
    Generate a mailto: link for users without connected email.

    Args:
        to_email: Recipient email address
        subject: Email subject line
        body: Email body (plain text)
        to_name: Optional recipient name (not used in mailto)

    Returns:
        Mailto URL string
    """
    from urllib.parse import quote

    # Clean up body - mailto doesn't support HTML
    clean_body = body.replace('\r\n', '\n').replace('\r', '\n')

    params = {
        'subject': subject,
        'body': clean_body,
    }

    query = '&'.join(f"{k}={quote(v)}" for k, v in params.items())
    return f"mailto:{to_email}?{query}"
