import base64
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config import settings

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


class GmailService:
    def __init__(self) -> None:
        self.credentials_path = Path(settings.gmail_credentials_path)
        self.token_path = Path(settings.gmail_token_path)

    def _authorize(self) -> Credentials:
        creds = None
        if self.token_path.exists():
            creds = Credentials.from_authorized_user_file(str(self.token_path), SCOPES)

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        elif not creds or not creds.valid:
            if not self.credentials_path.exists():
                raise FileNotFoundError(f"Gmail credentials not found at {self.credentials_path}")
            flow = InstalledAppFlow.from_client_secrets_file(str(self.credentials_path), SCOPES)
            creds = flow.run_local_server(port=0)
            self.token_path.write_text(creds.to_json(), encoding="utf-8")
        return creds

    @staticmethod
    def _pick_header(headers: List[Dict[str, str]], key: str) -> str:
        key = key.lower()
        for h in headers:
            if h.get("name", "").lower() == key:
                return h.get("value", "")
        return ""

    @staticmethod
    def _decode_base64(data: str) -> str:
        fixed = data.replace("-", "+").replace("_", "/")
        decoded = base64.b64decode(fixed + "===")
        return decoded.decode("utf-8", errors="ignore")

    def _decode_body(self, payload: Dict[str, Any]) -> str:
        body = payload.get("body", {}).get("data")
        if body:
            return self._decode_base64(body)

        for part in payload.get("parts", []) or []:
            mime = part.get("mimeType", "")
            data = part.get("body", {}).get("data")
            if not data:
                continue
            if mime == "text/plain":
                return self._decode_base64(data)
            if mime == "text/html":
                return self._decode_base64(data).replace("<br>", "\n").replace("&nbsp;", " ")
        return ""

    @staticmethod
    def _normalize_received(date_value: str, fallback_epoch_ms: str) -> str:
        if date_value:
            try:
                return parsedate_to_datetime(date_value).isoformat()
            except Exception:
                pass
        try:
            return datetime.utcfromtimestamp(int(fallback_epoch_ms) / 1000.0).isoformat()
        except Exception:
            return datetime.utcnow().isoformat()

    def fetch_recent_messages(self, max_results: int = 50):
        creds = self._authorize()
        service = build("gmail", "v1", credentials=creds)
        refs = service.users().messages().list(userId="me", maxResults=max_results, q=settings.gmail_query).execute().get("messages", [])

        out = []
        for ref in refs:
            full = service.users().messages().get(userId="me", id=ref["id"], format="full").execute()
            headers = full.get("payload", {}).get("headers", [])
            sender = self._pick_header(headers, "From")
            subject = self._pick_header(headers, "Subject")
            raw_date = self._pick_header(headers, "Date")
            out.append(
                {
                    "gmail_message_id": full.get("id"),
                    "thread_id": full.get("threadId"),
                    "sender": sender,
                    "subject": subject,
                    "snippet": full.get("snippet", ""),
                    "body": self._decode_body(full.get("payload", {})),
                    "received_at": self._normalize_received(raw_date, full.get("internalDate", "0")),
                    "raw_payload": full,
                }
            )
        return out
