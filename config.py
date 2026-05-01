import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _env_bool(key: str, default: bool = True) -> bool:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Settings:
    flask_secret_key: str = os.getenv("FLASK_SECRET_KEY", "dev-secret")
    db_path: str = os.getenv("DB_PATH", "instance/job_pipeline.db")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    gmail_credentials_path: str = os.getenv("GMAIL_CREDENTIALS_PATH", "credentials.json")
    gmail_token_path: str = os.getenv("GMAIL_TOKEN_PATH", "token.json")
    gmail_query: str = os.getenv("GMAIL_QUERY", "newer_than:30d")
    target_min_score: int = field(default_factory=lambda: _env_int("TARGET_MIN_SCORE", 50))
    fetch_job_pages: bool = field(default_factory=lambda: _env_bool("FETCH_JOB_PAGES", True))
    enable_ddg_discovery: bool = field(default_factory=lambda: _env_bool("ENABLE_DDG_DISCOVERY", True))


settings = Settings()
