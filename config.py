import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _settings_db_path() -> str:
    raw = os.getenv("DB_PATH")
    if raw and str(raw).strip():
        return str(raw).strip()
    if os.getenv("JOB_PIPELINE_EMBEDDED") == "1":
        d = os.getenv("JOB_PIPELINE_DATA_DIR", "").strip()
        if d:
            return str(Path(d) / "job_pipeline.db")
    return "instance/job_pipeline.db"


def _settings_gmail_token_path() -> str:
    raw = os.getenv("GMAIL_TOKEN_PATH")
    if raw and str(raw).strip():
        return str(raw).strip()
    if os.getenv("JOB_PIPELINE_EMBEDDED") == "1":
        d = os.getenv("JOB_PIPELINE_DATA_DIR", "").strip()
        if d:
            return str(Path(d) / "token.json")
    return "token.json"


def _settings_gmail_credentials_path() -> str:
    raw = os.getenv("GMAIL_CREDENTIALS_PATH")
    if raw and str(raw).strip():
        return str(raw).strip()
    if os.getenv("JOB_PIPELINE_EMBEDDED") == "1":
        d = os.getenv("JOB_PIPELINE_DATA_DIR", "").strip()
        if d:
            p = Path(d) / "credentials.json"
            if p.is_file():
                return str(p)
    return "credentials.json"


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
    db_path: str = field(default_factory=_settings_db_path)
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    gmail_credentials_path: str = field(default_factory=_settings_gmail_credentials_path)
    gmail_token_path: str = field(default_factory=_settings_gmail_token_path)
    gmail_query: str = os.getenv("GMAIL_QUERY", "newer_than:30d")
    target_min_score: int = field(default_factory=lambda: _env_int("TARGET_MIN_SCORE", 50))
    fetch_job_pages: bool = field(default_factory=lambda: _env_bool("FETCH_JOB_PAGES", True))
    enable_ddg_discovery: bool = field(default_factory=lambda: _env_bool("ENABLE_DDG_DISCOVERY", True))


settings = Settings()


def reload_runtime_settings() -> None:
    """
    Re-read .env and apply SQLite app_settings overrides for OpenAI keys, then rebuild `settings`.
    Call after saving OpenAI settings in the UI (same process picks up changes for new JobPipeline()).
    """
    global settings
    load_dotenv()
    try:
        from database import app_settings as _aps

        k = _aps.get_setting("OPENAI_API_KEY")
        if k and str(k).strip():
            os.environ["OPENAI_API_KEY"] = str(k).strip()
        m = _aps.get_setting("OPENAI_MODEL")
        if m and str(m).strip():
            os.environ["OPENAI_MODEL"] = str(m).strip()
    except Exception:
        pass
    settings = Settings()
