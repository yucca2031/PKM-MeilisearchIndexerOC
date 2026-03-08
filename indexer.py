#!/usr/bin/env python3
"""Index exported Outlook email data into Meilisearch."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

import meilisearch


DEFAULT_SOURCE_DIR = "/mnt/omv-mail"
DEFAULT_MEILI_URL = "http://localhost:7700"
DEFAULT_MEILI_KEY = "pkm-meili-key-2026!"
DEFAULT_INDEX_NAME = "emails"
DEFAULT_BATCH_SIZE = 500
DEFAULT_STATE_FILE_NAME = "_indexer_state.json"
DEFAULT_STATE_DIR_NAME = ".indexer-state"
MAX_BODY_CHARS = 10_000
FALLBACK_BODY_CHARS = 5_000
TASK_WAIT_TIMEOUT_MS = 600_000
TASK_WAIT_INTERVAL_MS = 500
RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5
PROGRESS_INTERVAL = 100

T = TypeVar("T")


INDEX_SETTINGS = {
    "searchableAttributes": [
        "subject",
        "body",
        "sender_name",
        "sender_email",
        "to",
        "cc",
        "attachment_names",
        "conversation_topic",
        "meeting_location",
        "meeting_attendees",
    ],
    "displayedAttributes": [
        "id",
        "subject",
        "body",
        "sender_name",
        "sender_email",
        "to",
        "cc",
        "received_date",
        "received_time",
        "source_store",
        "folder",
        "folder_path",
        "importance",
        "has_attachment",
        "attachment_count",
        "attachment_names",
        "is_calendar_item",
        "meeting_time",
        "meeting_location",
        "meeting_attendees",
        "conversation_topic",
        "is_reply",
        "is_forward",
        "file_path",
        "mail_file",
        "body_text_path",
        "attachment_dir",
        "mail_id",
        "internet_message_id",
        "body_length",
        "categories",
        "subject_raw",
        "received_year",
        "received_month",
    ],
    "filterableAttributes": [
        "source_store",
        "folder",
        "sender_name",
        "sender_email",
        "received_year",
        "received_month",
        "importance",
        "has_attachment",
        "is_calendar_item",
        "is_reply",
        "is_forward",
    ],
    "sortableAttributes": ["received_time", "body_length"],
    "pagination": {"maxTotalHits": 5000},
    "typoTolerance": {
        "enabled": True,
        "minWordSizeForTypos": {
            "oneTypo": 4,
            "twoTypos": 8,
        },
    },
}


class RetryableOperationError(RuntimeError):
    """Wrap failures from retryable Meilisearch operations."""


class Logger:
    def __init__(self, verbose: bool = False) -> None:
        self.verbose = verbose

    def _log(self, level: str, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} [{level}] {message}")

    def info(self, message: str) -> None:
        self._log("INFO", message)

    def warning(self, message: str) -> None:
        self._log("WARNING", message)

    def error(self, message: str) -> None:
        self._log("ERROR", message)

    def debug(self, message: str) -> None:
        if self.verbose:
            self._log("INFO", message)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index exported Outlook emails into Meilisearch")
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE_DIR,
        help=f"Root directory of exported emails (default: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--meili-url",
        default=DEFAULT_MEILI_URL,
        help=f"Meilisearch URL (default: {DEFAULT_MEILI_URL})",
    )
    parser.add_argument(
        "--meili-key",
        default=os.environ.get("MEILI_MASTER_KEY", DEFAULT_MEILI_KEY),
        help=(
            "Meilisearch master key "
            f"(default: {DEFAULT_MEILI_KEY}; can also be set via MEILI_MASTER_KEY env var)"
        ),
    )
    parser.add_argument("--incremental", action="store_true", help="Only index files changed since the last successful run")
    parser.add_argument("--reindex", action="store_true", help="Delete existing index and recreate it from scratch")
    parser.add_argument("--apply-settings", action="store_true", help="Re-apply index settings without requiring --reindex")
    parser.add_argument("--limit", type=int, default=0, help="Max documents to index (for testing)")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Documents per batch sent to Meilisearch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be indexed without actually indexing")
    parser.add_argument("--stats", action="store_true", help="Show index statistics and exit")
    parser.add_argument("--verbose", action="store_true", help="Detailed output including each file processed")
    return parser.parse_args(argv)


def state_file_for_source(source_root: Path) -> Path:
    legacy_state_path = source_root / DEFAULT_STATE_FILE_NAME
    if legacy_state_path.exists() and os.access(source_root, os.W_OK):
        return legacy_state_path
    return writable_state_file_for_source(source_root)


def writable_state_file_for_source(source_root: Path) -> Path:
    state_dir = Path(__file__).resolve().parent / DEFAULT_STATE_DIR_NAME
    source_hash = hashlib.md5(str(source_root).encode("utf-8", errors="ignore")).hexdigest()[:12]
    state_name = f"{source_root.name or 'root'}_{source_hash}_{DEFAULT_STATE_FILE_NAME}"
    return state_dir / state_name


def load_state_file_for_source(source_root: Path) -> Path:
    legacy_state_path = source_root / DEFAULT_STATE_FILE_NAME
    if legacy_state_path.exists():
        return legacy_state_path
    return writable_state_file_for_source(source_root)


def log_exception_message(exc: Exception) -> str:
    return str(exc) or exc.__class__.__name__


def retry_call(func: Callable[[], T], logger: Logger, action: str) -> T:
    last_error: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return func()
        except Exception as exc:  # pragma: no cover - depends on remote failures
            last_error = exc
            if attempt >= RETRY_ATTEMPTS:
                break
            logger.warning(
                f"{action} failed on attempt {attempt}/{RETRY_ATTEMPTS}: {log_exception_message(exc)}; retrying in {RETRY_DELAY_SECONDS}s"
            )
            time.sleep(RETRY_DELAY_SECONDS)
    assert last_error is not None
    raise RetryableOperationError(f"{action} failed after {RETRY_ATTEMPTS} attempts: {log_exception_message(last_error)}")


def task_uid(task: Any) -> int:
    if hasattr(task, "task_uid"):
        return int(task.task_uid)
    if hasattr(task, "uid"):
        return int(task.uid)
    if isinstance(task, dict):
        for key in ("taskUid", "task_uid", "uid"):
            if key in task:
                return int(task[key])
    raise RuntimeError(f"Unable to read task UID from response: {task!r}")


def task_status(task: Any) -> str:
    if hasattr(task, "status"):
        return str(task.status)
    if isinstance(task, dict):
        return str(task.get("status", ""))
    return ""


def task_error(task: Any) -> Any:
    if hasattr(task, "error"):
        return task.error
    if isinstance(task, dict):
        return task.get("error")
    return None


def wait_for_task(client: meilisearch.Client, raw_task: Any, logger: Logger) -> dict[str, Any]:
    uid = task_uid(raw_task)

    def run() -> dict[str, Any]:
        try:
            task = client.wait_for_task(
                uid,
                timeout_in_ms=TASK_WAIT_TIMEOUT_MS,
                interval_in_ms=TASK_WAIT_INTERVAL_MS,
            )
        except TypeError:
            task = client.wait_for_task(uid)
        if task_status(task) == "failed":
            raise RuntimeError(f"Meilisearch task {uid} failed: {task_error(task)}")
        return task

    return retry_call(run, logger, f"wait for Meilisearch task {uid}")


def index_exists(client: meilisearch.Client, index_name: str, logger: Logger) -> bool:
    def run() -> bool:
        try:
            client.get_index(index_name)
            return True
        except meilisearch.errors.MeilisearchApiError as exc:
            if getattr(exc, "code", None) == "index_not_found":
                return False
            raise

    return retry_call(run, logger, f"check if index {index_name} exists")


def get_index(client: meilisearch.Client, index_name: str, logger: Logger) -> meilisearch.Index:
    return retry_call(lambda: client.get_index(index_name), logger, f"get index {index_name}")


def create_index(client: meilisearch.Client, index_name: str, logger: Logger) -> meilisearch.Index:
    task = retry_call(
        lambda: client.create_index(index_name, {"primaryKey": "id"}),
        logger,
        f"create index {index_name}",
    )
    wait_for_task(client, task, logger)
    return get_index(client, index_name, logger)


def delete_index(client: meilisearch.Client, index_name: str, logger: Logger) -> None:
    if not index_exists(client, index_name, logger):
        return
    task = retry_call(lambda: client.index(index_name).delete(), logger, f"delete index {index_name}")
    wait_for_task(client, task, logger)


def ensure_index(client: meilisearch.Client, index_name: str, logger: Logger) -> tuple[meilisearch.Index, bool]:
    if index_exists(client, index_name, logger):
        return get_index(client, index_name, logger), False
    return create_index(client, index_name, logger), True


def configure_index(index: meilisearch.Index, client: meilisearch.Client, logger: Logger) -> None:
    task = retry_call(lambda: index.update_settings(INDEX_SETTINGS), logger, "update index settings")
    wait_for_task(client, task, logger)


def discover_json_files(source_root: Path) -> list[Path]:
    files: list[Path] = []
    for path in source_root.rglob("*.json"):
        if not path.is_file():
            continue
        relative_parts = path.relative_to(source_root).parts
        if any(part.startswith("_") for part in relative_parts):
            continue
        same_base_msg = path.with_suffix(".msg")
        same_base_txt = path.with_suffix(".txt")
        if not same_base_msg.is_file() and not same_base_txt.is_file():
            continue
        files.append(path)
    files.sort()
    return files


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {"last_run": "", "total_indexed": 0, "indexed_files": {}}

    try:
        with state_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {"last_run": "", "total_indexed": 0, "indexed_files": {}}

    if not isinstance(payload, dict):
        return {"last_run": "", "total_indexed": 0, "indexed_files": {}}

    indexed_files_raw = payload.get("indexed_files")
    indexed_files: dict[str, dict[str, str]] = {}
    if isinstance(indexed_files_raw, dict):
        for key, value in indexed_files_raw.items():
            rel = str(key)
            if isinstance(value, dict):
                indexed_files[rel] = {
                    "fingerprint": str(value.get("fingerprint") or ""),
                    "doc_id": str(value.get("doc_id") or ""),
                    "base_id": str(value.get("base_id") or ""),
                }
            else:
                indexed_files[rel] = {
                    "fingerprint": str(value),
                    "doc_id": "",
                    "base_id": "",
                }

    return {
        "last_run": str(payload.get("last_run") or ""),
        "total_indexed": int(payload.get("total_indexed") or 0),
        "indexed_files": indexed_files,
    }


def save_state(state_path: Path, indexed_files: dict[str, dict[str, str]], total_indexed: int) -> None:
    payload = {
        "last_run": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "total_indexed": total_indexed,
        "indexed_files": indexed_files,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def file_fingerprint(path: Path) -> str:
    stat_result = path.stat()
    return f"{stat_result.st_mtime_ns}:{stat_result.st_size}"


def datetime_parts(dt: datetime) -> tuple[int, str, int, int]:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    timestamp = int(dt.timestamp())
    return timestamp, dt.date().isoformat(), dt.year, dt.month


def parse_datetime_value(raw_value: Any) -> datetime | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None

    value = raw_value.strip()

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass

    common_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S",
        "%d %b %Y %H:%M:%S",
    )
    for fmt in common_formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None


def datetime_from_filename(json_path: Path) -> datetime | None:
    stem = json_path.stem.strip()
    if not stem:
        return None

    match = re.match(r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{6})(?:_|$)", stem)
    if not match:
        return None

    try:
        return datetime.strptime(f"{match.group('date')} {match.group('time')}", "%Y-%m-%d %H%M%S")
    except ValueError:
        return None


def parse_datetime(raw_value: Any, json_path: Path | None = None) -> tuple[int, str, int, int]:
    dt = parse_datetime_value(raw_value)
    if dt is None and json_path is not None:
        dt = datetime_from_filename(json_path)
    if dt is None:
        return 0, "", 0, 0
    return datetime_parts(dt)


def normalize_people_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        clean = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(clean)
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_categories(value: Any) -> str:
    return normalize_people_field(value)


def normalize_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def normalize_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


def normalize_subject(subject: str) -> str:
    readable = subject.replace("__", ": ")
    readable = readable.replace("_", " ")
    return " ".join(readable.split())


def extract_folder(folder_path: Any) -> str:
    if not isinstance(folder_path, str) or not folder_path.strip():
        return ""
    cleaned = folder_path.replace("\\", "/").strip("/")
    if not cleaned:
        return ""
    return cleaned.split("/")[-1]


def document_folder(metadata: dict[str, Any], json_path: Path, source_root: Path) -> str:
    folder = extract_folder(metadata.get("folder_path"))
    if folder:
        return folder

    try:
        relative_parent = json_path.relative_to(source_root).parent
    except ValueError:
        relative_parent = json_path.parent

    if not relative_parent or str(relative_parent) == ".":
        return ""
    return relative_parent.name.strip()


def normalize_source_store(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    store = value.strip()
    if store.lower().endswith(".pst"):
        return store[:-4]
    return store


def document_source_store(metadata: dict[str, Any], json_path: Path, source_root: Path) -> str:
    source_store = normalize_source_store(metadata.get("source_store"))
    if source_store:
        return source_store

    try:
        relative_path = json_path.relative_to(source_root)
    except ValueError:
        relative_path = json_path

    parts = [part.strip() for part in relative_path.parts[:-1] if part.strip()]
    if not parts:
        return ""
    return parts[0]


def normalize_importance(value: Any) -> str:
    if value is None:
        return "Normal"
    if isinstance(value, (int, float)):
        raw = str(int(value))
    else:
        raw = str(value).strip()
    if not raw:
        return "Normal"
    mapping = {"0": "Low", "1": "Normal", "2": "High"}
    if raw in mapping:
        return mapping[raw]
    lowered = raw.lower()
    if lowered in {"low", "normal", "high"}:
        return raw.capitalize()
    return "Normal"


def attachment_names(attachments: Any) -> str:
    if not isinstance(attachments, list):
        return ""
    names: list[str] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        name = item.get("original_name") or item.get("filename")
        if not name:
            continue
        names.append(str(name).strip())
    return ", ".join(name for name in names if name)


def load_json_file(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(payload, dict):
        return payload
    return None


def read_text_file(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except OSError:
        return ""


def read_body(json_path: Path, metadata: dict[str, Any], max_chars: int) -> str:
    body_path = metadata.get("body_text_path")
    if isinstance(body_path, str) and body_path.strip():
        txt_path = json_path.parent / body_path.strip()
    else:
        txt_path = json_path.with_suffix(".txt")

    if not txt_path.exists() or not txt_path.is_file():
        return ""

    text = read_text_file(txt_path)
    if len(text) > max_chars:
        return text[:max_chars]
    return text


def fallback_id(json_path: Path, source_root: Path) -> str:
    del source_root
    filename_hash = export_hash_from_filename(json_path)
    if filename_hash:
        return filename_hash

    stem = json_path.stem.encode("utf-8", errors="ignore")
    return hashlib.md5(stem).hexdigest()[:6]


def export_hash_from_filename(json_path: Path) -> str:
    stem = json_path.stem.strip()
    if not stem:
        return ""

    parts = stem.split("_", 3)
    if len(parts) >= 3 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[0]) and re.fullmatch(r"\d{6}", parts[1]):
        candidate = parts[2].strip().lower()
        if re.fullmatch(r"[0-9a-f]{6,}", candidate):
            return candidate
        return ""

    candidates = re.findall(r"(?<![0-9A-Fa-f])([0-9A-Fa-f]{6,})(?![0-9A-Fa-f])", stem)
    if candidates:
        return candidates[0].lower()
    return ""


def document_base_id(metadata: dict[str, Any], json_path: Path, source_root: Path) -> str:
    export_hash = str(metadata.get("export_hash") or "").strip().lower()
    if export_hash:
        return export_hash
    return fallback_id(json_path, source_root)


def split_collision_id(doc_id: str) -> tuple[str, int]:
    base, separator, suffix = doc_id.rpartition("_")
    if separator and suffix.isdigit():
        return base, int(suffix)
    return doc_id, 1



def unique_document_id(base_id: str, id_counts: dict[str, int]) -> str:
    current = id_counts.get(base_id, 0)
    if current <= 0:
        id_counts[base_id] = 1
        return base_id
    current += 1
    id_counts[base_id] = current
    return f"{base_id}_{current}"


def build_document(
    json_path: Path,
    source_root: Path,
    known_doc_ids_by_path: dict[str, str],
    known_doc_ids_by_base: dict[str, str],
    id_counts: dict[str, int],
    max_body_chars: int,
) -> tuple[dict[str, Any] | None, str, str]:
    metadata = load_json_file(json_path)
    if metadata is None:
        return None, "", ""

    raw_subject = str(metadata.get("subject") or "").strip()
    received_ts, received_date, received_year, received_month = parse_datetime(metadata.get("received_time"), json_path)
    relative_json = str(json_path.relative_to(source_root)).replace(os.sep, "/")
    relative_msg = str(json_path.relative_to(source_root).with_suffix(".msg")).replace(os.sep, "/")

    doc_id = document_base_id(metadata, json_path, source_root)

    existing_doc_id = known_doc_ids_by_path.get(relative_json)
    if not existing_doc_id:
        existing_doc_id = known_doc_ids_by_base.get(doc_id, "")
    if existing_doc_id:
        final_doc_id = existing_doc_id
        base_id, suffix = split_collision_id(final_doc_id)
        id_counts[base_id] = max(id_counts.get(base_id, 0), suffix)
    else:
        final_doc_id = unique_document_id(doc_id, id_counts)

    is_calendar = normalize_bool(metadata.get("is_calendar_item"), default=False)
    meeting_time = None
    meeting_location = None
    meeting_attendees = None
    if is_calendar:
        start_time = metadata.get("start_time")
        if isinstance(start_time, str) and start_time.strip():
            meeting_time = start_time.strip()
        location = metadata.get("location")
        if isinstance(location, str) and location.strip():
            meeting_location = location.strip()
        required_attendees = str(metadata.get("required_attendees") or "").strip()
        optional_attendees = str(metadata.get("optional_attendees") or "").strip()
        attendees = [item for item in (required_attendees, optional_attendees) if item]
        if attendees:
            meeting_attendees = "; ".join(attendees)

    body = read_body(json_path, metadata, max_body_chars)
    body_length = normalize_int(metadata.get("body_length"), default=len(body))

    document = {
        "id": final_doc_id,
        "subject": normalize_subject(raw_subject),
        "subject_raw": raw_subject,
        "body": body,
        "sender_name": str(metadata.get("sender_name") or "").strip(),
        "sender_email": str(metadata.get("sender_email") or "").strip(),
        "to": normalize_people_field(metadata.get("to")),
        "cc": normalize_people_field(metadata.get("cc")),
        "received_time": received_ts,
        "received_date": received_date,
        "received_year": received_year,
        "received_month": received_month,
        "source_store": document_source_store(metadata, json_path, source_root),
        "folder": document_folder(metadata, json_path, source_root),
        "folder_path": str(metadata.get("folder_path") or "").strip(),
        "importance": normalize_importance(metadata.get("importance")),
        "has_attachment": normalize_bool(metadata.get("has_attachment"), default=False),
        "attachment_count": normalize_int(metadata.get("attachment_count"), default=0),
        "attachment_names": attachment_names(metadata.get("attachments")),
        "is_calendar_item": is_calendar,
        "meeting_time": meeting_time,
        "meeting_location": meeting_location,
        "meeting_attendees": meeting_attendees,
        "conversation_topic": str(metadata.get("conversation_topic") or "").strip(),
        "is_reply": normalize_bool(metadata.get("is_reply"), default=False),
        "is_forward": normalize_bool(metadata.get("is_forward"), default=False),
        "categories": normalize_categories(metadata.get("categories")),
        "body_length": body_length,
        "file_path": relative_msg,
        "mail_file": str(metadata.get("mail_file") or "").strip(),
        "body_text_path": str(metadata.get("body_text_path") or "").strip(),
        "attachment_dir": str(metadata.get("attachment_dir") or "").strip(),
        "mail_id": str(metadata.get("mail_id") or "").strip(),
        "internet_message_id": str(metadata.get("internet_message_id") or "").strip(),
    }
    return document, relative_json, doc_id


def chunked(items: list[T], size: int) -> list[list[T]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def build_indexed_files_map(all_json_files: list[Path], source_root: Path, current_state: dict[str, Any]) -> dict[str, dict[str, str]]:
    existing = current_state.get("indexed_files", {})
    next_state: dict[str, dict[str, str]] = {}
    for path in all_json_files:
        relative = str(path.relative_to(source_root)).replace(os.sep, "/")
        previous = existing.get(relative, {}) if isinstance(existing, dict) else {}
        previous_doc_id = ""
        previous_base_id = ""
        if isinstance(previous, dict):
            previous_doc_id = str(previous.get("doc_id") or "")
            previous_base_id = str(previous.get("base_id") or "")
        next_state[relative] = {
            "fingerprint": file_fingerprint(path),
            "doc_id": previous_doc_id,
            "base_id": previous_base_id,
        }
    return next_state


def select_files(
    all_json_files: list[Path],
    source_root: Path,
    incremental: bool,
    current_state: dict[str, Any],
) -> tuple[list[Path], dict[str, dict[str, str]]]:
    next_state = build_indexed_files_map(all_json_files, source_root, current_state)
    if not incremental:
        return all_json_files, next_state

    indexed_files = current_state.get("indexed_files", {})
    selected: list[Path] = []
    for path in all_json_files:
        relative = str(path.relative_to(source_root)).replace(os.sep, "/")
        previous = indexed_files.get(relative, {}) if isinstance(indexed_files, dict) else {}
        previous_fingerprint = ""
        if isinstance(previous, dict):
            previous_fingerprint = str(previous.get("fingerprint") or "")
        elif previous:
            previous_fingerprint = str(previous)
        if next_state[relative]["fingerprint"] != previous_fingerprint:
            selected.append(path)
    return selected, next_state


def clone_documents_with_shorter_bodies(documents: list[dict[str, Any]], max_chars: int) -> list[dict[str, Any]]:
    shortened: list[dict[str, Any]] = []
    for document in documents:
        copy = dict(document)
        body = str(copy.get("body") or "")
        if len(body) > max_chars:
            copy["body"] = body[:max_chars]
        shortened.append(copy)
    return shortened


def add_documents_with_retry(
    index: meilisearch.Index,
    client: meilisearch.Client,
    documents: list[dict[str, Any]],
    logger: Logger,
) -> None:
    try:
        task = retry_call(lambda: index.add_documents(documents), logger, f"index batch of {len(documents)} documents")
        wait_for_task(client, task, logger)
    except Exception:
        shortened = clone_documents_with_shorter_bodies(documents, FALLBACK_BODY_CHARS)
        task = retry_call(
            lambda: index.add_documents(shortened),
            logger,
            f"retry batch of {len(documents)} documents with shorter bodies",
        )
        wait_for_task(client, task, logger)


def index_documents(
    client: meilisearch.Client,
    index: meilisearch.Index,
    documents: list[dict[str, Any]],
    batch_size: int,
    logger: Logger,
) -> int:
    indexed_count = 0
    for batch in chunked(documents, batch_size):
        add_documents_with_retry(index, client, batch, logger)
        indexed_count += len(batch)
    return indexed_count


def format_duration(seconds: float) -> str:
    total_seconds = max(int(seconds), 0)
    minutes, remaining_seconds = divmod(total_seconds, 60)
    if minutes:
        return f"{minutes}m {remaining_seconds}s"
    return f"{remaining_seconds}s"


def create_client(args: argparse.Namespace) -> meilisearch.Client:
    return meilisearch.Client(args.meili_url, args.meili_key)


def response_value(payload: Any, *names: str) -> Any:
    if isinstance(payload, dict):
        for name in names:
            if name in payload:
                return payload[name]
        return None
    for name in names:
        if hasattr(payload, name):
            return getattr(payload, name)
    return None


def show_stats(client: meilisearch.Client, logger: Logger) -> int:
    if not index_exists(client, DEFAULT_INDEX_NAME, logger):
        logger.info(f"Index {DEFAULT_INDEX_NAME} does not exist. document_count=0")
        return 0
    index = get_index(client, DEFAULT_INDEX_NAME, logger)
    stats = retry_call(lambda: index.get_stats(), logger, f"get stats for index {DEFAULT_INDEX_NAME}")
    number_of_documents = int(response_value(stats, "numberOfDocuments", "number_of_documents") or 0)
    is_indexing = bool(response_value(stats, "isIndexing", "is_indexing") or False)
    logger.info(f"Index {DEFAULT_INDEX_NAME}: document_count={number_of_documents}, is_indexing={is_indexing}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logger = Logger(verbose=args.verbose)

    if args.batch_size <= 0:
        logger.error("batch-size must be > 0")
        return 2
    if args.limit < 0:
        logger.error("limit must be >= 0")
        return 2

    source_root = Path(args.source).resolve()
    if not source_root.exists() or not source_root.is_dir():
        logger.error(f"source directory does not exist: {source_root}")
        return 2

    state_path = state_file_for_source(source_root)
    load_state_path = load_state_file_for_source(source_root)
    client = create_client(args)

    if args.stats:
        return show_stats(client, logger)

    started_at = time.monotonic()
    current_state = {"last_run": "", "total_indexed": 0, "indexed_files": {}} if args.reindex else load_state(load_state_path)
    all_json_files = discover_json_files(source_root)
    selected_files, next_state = select_files(all_json_files, source_root, args.incremental, current_state)

    if args.limit:
        selected_files = selected_files[: args.limit]

    known_doc_ids_by_path = {
        path: details.get("doc_id", "")
        for path, details in current_state.get("indexed_files", {}).items()
        if isinstance(details, dict) and details.get("doc_id")
    }
    known_doc_ids_by_base: dict[str, str] = {}
    for details in current_state.get("indexed_files", {}).values():
        if not isinstance(details, dict):
            continue
        doc_id = str(details.get("doc_id") or "")
        if not doc_id:
            continue

        base_id = str(details.get("base_id") or "").strip().lower()
        if not base_id:
            base_id, _ = split_collision_id(doc_id)
        known_doc_ids_by_base.setdefault(base_id, doc_id)

    id_counts: dict[str, int] = {}
    for doc_id in known_doc_ids_by_path.values():
        base_id, suffix = split_collision_id(doc_id)
        id_counts[base_id] = max(id_counts.get(base_id, 0), suffix)

    documents: list[dict[str, Any]] = []
    skipped = 0
    errors = 0

    logger.info(
        f"Scanning {source_root} ({len(all_json_files)} json files found; {len(selected_files)} selected for {'incremental' if args.incremental else 'full'} indexing)"
    )

    for index_number, path in enumerate(selected_files, start=1):
        document, relative_json, base_id = build_document(
            path,
            source_root,
            known_doc_ids_by_path,
            known_doc_ids_by_base,
            id_counts,
            MAX_BODY_CHARS,
        )
        if document is None:
            skipped += 1
            logger.warning(f"Skipping malformed JSON: {path}")
            continue
        documents.append(document)
        next_state[relative_json]["doc_id"] = str(document["id"])
        next_state[relative_json]["base_id"] = base_id
        if args.verbose:
            logger.debug(f"Prepared {path} -> id={document['id']}")
        if index_number % PROGRESS_INTERVAL == 0 or index_number == len(selected_files):
            progress = int((index_number / max(len(selected_files), 1)) * 100)
            logger.info(f"Progress: {index_number}/{len(selected_files)} files processed ({progress}%)")

    if args.dry_run:
        logger.info(f"Dry run complete. Would index {len(documents)} documents, skipped {skipped} files.")
        return 0

    if args.reindex:
        logger.info(f"Reindex requested; deleting existing index {DEFAULT_INDEX_NAME} if present")
        delete_index(client, DEFAULT_INDEX_NAME, logger)

    index, created = ensure_index(client, DEFAULT_INDEX_NAME, logger)
    if created or args.reindex or args.apply_settings:
        logger.info(f"Applying settings to index {DEFAULT_INDEX_NAME}")
        configure_index(index, client, logger)

    indexed_count = 0
    if documents:
        indexed_count = index_documents(client, index, documents, args.batch_size, logger)
        total_indexed = int(current_state.get("total_indexed") or 0) + indexed_count
    else:
        total_indexed = int(current_state.get("total_indexed") or 0)

    save_state(state_path, next_state, total_indexed)
    duration = format_duration(time.monotonic() - started_at)
    logger.info(f"Done! Indexed: {indexed_count}, Skipped: {skipped}, Errors: {errors}, Time: {duration}")
    logger.info(f"State file: {state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
