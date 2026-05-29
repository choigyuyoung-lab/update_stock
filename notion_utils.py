import os
from dotenv import load_dotenv
load_dotenv()
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, cast

from notion_client import Client
from notion_client.errors import HTTPResponseError

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_PAGE_SIZE = 100


def get_env_var(name: str, required: bool = True, default: Optional[str] = None) -> str:
    value: Optional[str] = os.environ.get(name, default)
    if required and not value:
        raise EnvironmentError(f"환경 변수 {name}이(가) 설정되지 않았습니다.")
    # At this point, mypy/pylance may still see Optional[str]; cast to `str` to satisfy checkers
    return cast(str, value)


def build_notion_client(auth_token: str, use_httpx: bool = False, timeout: float = 60.0) -> Client:
    if use_httpx:
        import httpx

        # httpx.Client type can be passed to notion_client; annotate as Any to satisfy static checkers
        httpx_client: Any = httpx.Client(timeout=timeout)
        return Client(auth=auth_token, client=httpx_client)
    return Client(auth=auth_token)


def _format_notion_error(error: Exception) -> str:
    # Format common fields from HTTPResponseError if present, using getattr to avoid attribute errors
    if isinstance(error, HTTPResponseError):
        status = getattr(error, "status", None)
        message = getattr(error, "message", None) or str(error)
        body = getattr(error, "body", None)
        return f"status={status}, message={message}, body={body}"
    return str(error)


def safe_databases_query(
    client: Any,
    database_id: str,
    start_cursor: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> Dict[str, Any]:
    attempt = 1
    while True:
        try:
            params = {"database_id": database_id, "page_size": page_size}
            if start_cursor:
                params["start_cursor"] = start_cursor
            # notion_client methods may return SyncAsync[...] (sync value or awaitable).
            # Use cast to Dict[str, Any] to satisfy static type checkers in synchronous usage.
            return cast(Dict[str, Any], client.databases.query(**params))
        except HTTPResponseError as error:
            status = getattr(error, "status", None)
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                print(f"   ⚠️ Notion query retry {attempt}/{max_retries} - status={status}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            raise
        except Exception as error:
            if attempt < max_retries:
                print(f"   ⚠️ Notion query retry {attempt}/{max_retries}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            raise


def paginate_database(
    client: Any,
    database_id: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    retry_delay: float = 1.0,
) -> Iterable[Dict[str, Any]]:
    start_cursor = None
    while True:
        response = safe_databases_query(client, database_id, start_cursor=start_cursor, page_size=page_size)
        for page in response.get("results", []):
            yield page
        if not response.get("has_more"):
            break
        start_cursor = response.get("next_cursor")
        time.sleep(retry_delay)


def safe_page_update(
    client: Any,
    page_id: str,
    properties: Dict[str, Any],
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> bool:
    if not properties:
        return False

    attempt = 1
    while True:
        try:
            # pages.update may be SyncAsync[...] — running synchronously here; cast return as Any
            _ = cast(Any, client.pages.update(page_id=page_id, properties=properties))
            return True
        except HTTPResponseError as error:
            status = getattr(error, "status", None)
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                print(f"   ⚠️ Notion update retry {attempt}/{max_retries} - status={status}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            print(f"   ❌ Notion update failed: {_format_notion_error(error)}")
            return False
        except Exception as error:
            if attempt < max_retries:
                print(f"   ⚠️ Notion update retry {attempt}/{max_retries}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            print(f"   ❌ Notion update failed: {error}")
            return False


def get_page_text(props: Dict[str, Any], names: List[str]) -> str:
    for name in names:
        prop = props.get(name, {})
        for key in ("title", "rich_text"):
            content = prop.get(key)
            if content and isinstance(content, list) and len(content) > 0:
                text = content[0].get("plain_text", "")
                if text:
                    return text.strip()
    return ""


def kst_isoformat() -> str:
    return datetime.now(timezone(timedelta(hours=9))).isoformat()
