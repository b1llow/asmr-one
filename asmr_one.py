#!/usr/bin/env nix-shell
#!nix-shell -i python3 -p python3Packages.blake3
"""ASMR One: batch-download asmr.one favorites using the official download API."""

from __future__ import annotations

import argparse
import copy
import datetime
import email.utils
import fcntl
import heapq
import http.client
import json
import math
import os
import re
import ssl
import stat as stat_module
import sys
import tempfile
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from blake3 import blake3

APP_NAME = "asmr-one"
APP_DISPLAY_NAME = "ASMR One"
CONFIG_HOME_ENV = "ASMR_ONE_HOME"
DEFAULT_MIRRORS = (
    "https://api.asmr-200.com",
    "https://api.asmr.one",
    "https://api.asmr-100.com",
    "https://api.asmr-300.com",
)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)
AUDIO_EXTS = {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".aac", ".opus"}
SUB_EXTS = {".lrc", ".vtt", ".srt", ".ass", ".txt"}
JA_HINTS = ("日本語", "日語", "日语", "japanese", "ja-jp", "ja_jp", "01日")
PLAYLIST_SOURCES = {"auto", "playlists", "favorites"}
SYSTEM_PLAYLIST_DISPLAY_NAMES = {
    "__SYS_PLAYLIST_LIKED": "Liked",
    "__SYS_PLAYLIST_MARKED": "Marked",
}
SYSTEM_PLAYLIST_ALIASES = {
    "liked": "__SYS_PLAYLIST_LIKED",
    "marked": "__SYS_PLAYLIST_MARKED",
}
CONFIG_DIR = Path(os.environ.get(CONFIG_HOME_ENV, Path.home() / ".config" / APP_NAME))
TOKEN_PATH = CONFIG_DIR / "token.json"
CHECKSUM_FILE_NAME = "checksums.json"
CHECKSUM_VERSION = 1
CHECKSUM_ALGORITHM = "blake3"
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
PART_STATE_VERSION = 1
PART_CHECKPOINT_SIZE = 64 * 1024 * 1024
WORK_LOCK_FILE_NAME = ".asmr-one.lock"
MAX_LOCAL_COMPONENT_BYTES = 240
MAX_WORK_FOLDER_BYTES = 180
RETRY_DELAYS = (60.0, 5 * 60.0, 30 * 60.0, 4 * 60 * 60.0, 24 * 60 * 60.0)
MAX_RETRY_AFTER_SECONDS = RETRY_DELAYS[-1]

_print_lock = threading.Lock()


class LocalStateError(RuntimeError):
    """A local file or checksum manifest is unsafe or inconsistent."""


class RetryableDownloadError(RuntimeError):
    """A download failed transiently after preserving safe partial progress."""


class IncompleteDownloadError(RetryableDownloadError):
    pass


class DownloadTransportError(RetryableDownloadError):
    pass


class DownloadProtocolError(RetryableDownloadError):
    pass


class RequestTransportError(RuntimeError):
    """All network transports were exhausted before a response was obtained."""

    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class PayloadError(RuntimeError):
    """A successful response had a permanently invalid application payload."""


class RemoteFileUnavailableError(RuntimeError):
    pass


class WorkLockedError(RuntimeError):
    pass


@dataclass(frozen=True)
class LocalFilePlan:
    file: dict[str, Any]
    dest: Path
    relative_path: str
    status: str
    resume: bool = False
    digest: str | None = None

    @property
    def needs_download(self) -> bool:
        return self.status in {"missing", "partial", "corrupt", "stale"}


@dataclass(frozen=True)
class DownloadResult:
    status: str
    digest: str


@dataclass(frozen=True)
class DownloadOutcome:
    result: DownloadResult
    record: dict[str, Any]
    context: LocalFileContext
    plan: LocalFilePlan


@dataclass(frozen=True)
class FetchedPage:
    items: list[dict[str, Any]]
    page_count: int | None
    has_more: bool


@dataclass(frozen=True)
class LocalFileContext:
    file: dict[str, Any]
    dest: Path
    relative_path: str
    record_key: str
    record: dict[str, Any] | None


@dataclass(frozen=True)
class FileInspection:
    plan: LocalFilePlan | None = None
    hash_reason: str | None = None
    expected_digest: str | None = None
    resume: bool = False


@dataclass(frozen=True)
class HashedFile:
    digest: str
    record: dict[str, Any]


@dataclass(frozen=True)
class PartialState:
    remote: dict[str, Any]
    committed_size: int
    committed_blake3: str
    etag: str | None = None


@dataclass(frozen=True)
class RetryPolicy:
    delays: tuple[float, ...]
    is_retryable: Callable[[BaseException], bool]
    retry_after: Callable[[BaseException], float | None] | None = None


@dataclass(frozen=True)
class PreparedWork:
    root: Path
    manifest: dict[str, Any]
    files: list[LocalFileContext]
    manifest_keys_changed: bool


@dataclass(frozen=True)
class WorkOutcome:
    shown_id: str
    ok: int
    skip: int
    fail: int


@dataclass(frozen=True)
class DownloadSummary:
    works: int
    ok: int
    skip: int
    fail: int


@dataclass(frozen=True)
class ScheduledTask:
    owner: str
    label: str
    run: Callable[[], Any]
    on_success: Callable[[Any], None]
    on_error: Callable[[BaseException], None]
    retry_policy: RetryPolicy | None = None
    on_retry: Callable[[BaseException, int, int, float], None] | None = None
    attempt: int = 0
    eligible_at: float = 0.0


@dataclass
class WorkState:
    owner: str
    work: dict[str, Any]
    root: Path | None = None
    manifest: dict[str, Any] | None = None
    remaining_files: int = 0
    prepared: bool = False
    ok: int = 0
    skip: int = 0
    fail: int = 0
    status_counts: dict[str, int] = field(
        default_factory=lambda: {
            status: 0
            for status in ("valid", "adopt", "missing", "partial", "corrupt", "stale")
        }
    )
    manifest_generation: int = 0
    persisted_generation: int = 0
    save_inflight: bool = False
    manifest_failed: bool = False
    completed: bool = False
    work_lock: WorkLock | None = None

    @property
    def shown_id(self) -> str:
        return str(self.work.get("source_id") or self.work.get("id") or "work")


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def die(msg: str, code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)


def ensure_safe_work_directory(root: Path, directory: Path) -> None:
    try:
        relative = directory.relative_to(root)
    except ValueError as exc:
        raise LocalStateError(
            f"download directory escapes work root: {directory}"
        ) from exc
    if any(part in {"", ".", ".."} for part in relative.parts):
        raise LocalStateError(f"unsafe download directory: {directory}")
    if root.is_symlink():
        raise LocalStateError(f"work directory must not be a symlink: {root}")
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise LocalStateError(f"work directory is not a real directory: {root}")
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise LocalStateError(
                f"download directory must not be a symlink: {current}"
            )
        current.mkdir(exist_ok=True)
        if current.is_symlink() or not current.is_dir():
            raise LocalStateError(
                f"download path is not a real directory: {current}"
            )


class WorkLock:
    def __init__(self, path: Path, fd: int):
        self.path = path
        self._fd = fd

    @classmethod
    def acquire(cls, root: Path) -> WorkLock:
        ensure_safe_work_directory(root, root)
        path = root / WORK_LOCK_FILE_NAME
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(path, flags, 0o644)
        except OSError as exc:
            if path.is_symlink():
                raise LocalStateError(
                    f"work lock must not be a symlink: {path}"
                ) from exc
            raise
        try:
            if not stat_module.S_ISREG(os.fstat(fd).st_mode):
                raise LocalStateError(f"work lock is not a regular file: {path}")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise WorkLockedError(
                    f"work is locked by another process: {root}"
                ) from exc
            return cls(path, fd)
        except BaseException:
            os.close(fd)
            raise

    def close(self) -> None:
        if self._fd < 0:
            return
        fd, self._fd = self._fd, -1
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def format_delay(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    if seconds % 86400 == 0 and seconds >= 86400:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0 and seconds >= 3600:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0 and seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"


class TaskScheduler:
    """Run dynamically spawned tasks with one process-wide worker budget."""

    def __init__(
        self,
        jobs: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if jobs < 1:
            raise ValueError("jobs must be at least 1")
        self.jobs = jobs
        self._clock = clock
        self._sleeper = sleeper
        self._ready: dict[str, deque[ScheduledTask]] = {}
        self._owner_cycle: deque[str] = deque()
        self._queued_owners: set[str] = set()
        self._inflight: dict[Future[Any], ScheduledTask] = {}
        self._delayed: list[tuple[float, int, ScheduledTask]] = []
        self._delay_sequence = 0
        self._discarded_owners: set[str] = set()

    def enqueue(self, task: ScheduledTask, *, front: bool = False) -> None:
        if task.owner in self._discarded_owners:
            return
        if task.eligible_at > self._clock():
            self._delay_sequence += 1
            heapq.heappush(
                self._delayed,
                (task.eligible_at, self._delay_sequence, task),
            )
            return
        self._enqueue_ready(task, front=front)

    def _enqueue_ready(self, task: ScheduledTask, *, front: bool = False) -> None:
        if task.owner in self._discarded_owners:
            return
        queue = self._ready.setdefault(task.owner, deque())
        if front:
            queue.appendleft(task)
        else:
            queue.append(task)
        if task.owner not in self._queued_owners:
            self._queued_owners.add(task.owner)
            self._owner_cycle.append(task.owner)

    def discard_ready(self, owner: str) -> None:
        self._discarded_owners.add(owner)
        self._ready.pop(owner, None)
        if owner in self._queued_owners:
            self._queued_owners.remove(owner)
            self._owner_cycle = deque(
                queued_owner
                for queued_owner in self._owner_cycle
                if queued_owner != owner
            )
        if any(task.owner == owner for _, _, task in self._delayed):
            self._delayed = [item for item in self._delayed if item[2].owner != owner]
            heapq.heapify(self._delayed)

    def owner_has_runnable(self, owner: str) -> bool:
        return bool(self._ready.get(owner)) or any(
            task.owner == owner for task in self._inflight.values()
        )

    def _promote_due(self) -> None:
        now = self._clock()
        while self._delayed and self._delayed[0][0] <= now:
            _, _, task = heapq.heappop(self._delayed)
            self._enqueue_ready(replace(task, eligible_at=0.0))

    def _next_delay(self) -> float | None:
        if not self._delayed:
            return None
        return max(0.0, self._delayed[0][0] - self._clock())

    def _pop_ready(self) -> ScheduledTask | None:
        while self._owner_cycle:
            owner = self._owner_cycle.popleft()
            queue = self._ready.get(owner)
            if not queue:
                self._queued_owners.discard(owner)
                self._ready.pop(owner, None)
                continue
            task = queue.popleft()
            if queue:
                self._owner_cycle.append(owner)
            else:
                self._queued_owners.remove(owner)
                del self._ready[owner]
            return task
        return None

    def _handle_error(self, task: ScheduledTask, exc: BaseException) -> None:
        if task.owner in self._discarded_owners:
            return
        policy = task.retry_policy
        if (
            policy is None
            or task.attempt >= len(policy.delays)
            or not policy.is_retryable(exc)
        ):
            task.on_error(exc)
            return

        delay = policy.delays[task.attempt]
        if policy.retry_after is not None:
            server_delay = policy.retry_after(exc)
            if server_delay is not None:
                delay = max(delay, server_delay)
        retry_number = task.attempt + 1
        retried = replace(
            task,
            attempt=retry_number,
            eligible_at=self._clock() + delay,
        )
        self.enqueue(retried)
        if task.on_retry is not None:
            task.on_retry(exc, retry_number, len(policy.delays), delay)

    def run(self) -> None:
        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            while self._owner_cycle or self._inflight or self._delayed:
                self._promote_due()
                while len(self._inflight) < self.jobs:
                    task = self._pop_ready()
                    if task is None:
                        break
                    self._inflight[pool.submit(task.run)] = task
                if not self._inflight:
                    delay = self._next_delay()
                    if delay is not None and delay > 0:
                        self._sleeper(delay)
                    continue
                done, _ = wait(
                    tuple(self._inflight),
                    timeout=self._next_delay(),
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    task = self._inflight.pop(future)
                    try:
                        result = future.result()
                    except BaseException as exc:  # worker failures are task results
                        self._handle_error(task, exc)
                    else:
                        if task.owner in self._discarded_owners:
                            continue
                        task.on_success(result)


class OrderedPageStream:
    """Fetch pages concurrently while emitting their items in page order."""

    def __init__(
        self,
        scheduler: TaskScheduler,
        *,
        owner: str,
        label: str,
        fetch_page: Callable[[int], FetchedPage],
        on_items: Callable[[list[dict[str, Any]]], None],
        on_done: Callable[[], None],
        on_error: Callable[[BaseException], None],
        should_stop: Callable[[], bool],
        retry_policy: RetryPolicy | None = None,
        on_retry: Callable[[str, BaseException, int, int, float], None] | None = None,
    ):
        self.scheduler = scheduler
        self.owner = owner
        self.label = label
        self.fetch_page = fetch_page
        self.on_items = on_items
        self.on_done = on_done
        self.on_error = on_error
        self.should_stop = should_stop
        self.retry_policy = retry_policy
        self.on_retry = on_retry
        self._scheduled: set[int] = set()
        self._pages: dict[int, FetchedPage] = {}
        self._next_to_emit = 1
        self._final_page: int | None = None
        self._finished = False

    def start(self) -> None:
        self._schedule(1)

    def _schedule(self, page: int) -> None:
        if self._finished or self.should_stop() or page in self._scheduled:
            return
        self._scheduled.add(page)
        task_label = f"{self.label} page={page}"
        self.scheduler.enqueue(
            ScheduledTask(
                owner=self.owner,
                label=task_label,
                run=lambda page=page: self.fetch_page(page),
                on_success=lambda result, page=page: self._receive(page, result),
                on_error=self._fail,
                retry_policy=self.retry_policy,
                on_retry=(
                    None
                    if self.on_retry is None
                    else lambda exc, attempt, total, delay: self.on_retry(
                        task_label, exc, attempt, total, delay
                    )
                ),
            )
        )

    def _receive(self, page: int, result: FetchedPage) -> None:
        if self._finished or self.should_stop():
            return
        if result.page_count is not None:
            if result.page_count < page:
                self._fail(
                    RuntimeError(
                        f"{self.label} reports {result.page_count} pages at page {page}"
                    )
                )
                return
            if self._final_page is not None and self._final_page != result.page_count:
                self._fail(
                    RuntimeError(f"{self.label} pagination changed while fetching")
                )
                return
            self._final_page = result.page_count
            for next_page in range(1, result.page_count + 1):
                self._schedule(next_page)
        elif result.has_more:
            self._schedule(page + 1)
        else:
            self._final_page = page

        self._pages[page] = result
        while self._next_to_emit in self._pages:
            emitted = self._pages.pop(self._next_to_emit)
            self._next_to_emit += 1
            self.on_items(emitted.items)
            if self.should_stop():
                return
        if self._final_page is not None and self._next_to_emit > self._final_page:
            self._finished = True
            self.on_done()

    def _fail(self, exc: BaseException) -> None:
        if self._finished:
            return
        self._finished = True
        self.on_error(exc)


def config_dir() -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(CONFIG_DIR, 0o700)
    except OSError:
        pass
    return CONFIG_DIR


def load_token() -> str | None:
    env = os.environ.get("ASMR_TOKEN")
    if env:
        return env.strip()
    if not TOKEN_PATH.is_file():
        return None
    try:
        data = json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    token = data.get("token")
    return token.strip() if isinstance(token, str) and token.strip() else None


def save_token(token: str, name: str | None = None) -> None:
    config_dir()
    payload = {"token": token}
    if name:
        payload["name"] = name
    TOKEN_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.chmod(TOKEN_PATH, 0o600)


def load_env_credentials() -> tuple[str | None, str | None]:
    return os.environ.get("ASMR_NAME"), os.environ.get("ASMR_PASSWORD")


def validate_credentials(name: str, password: str) -> None:
    if len(name) < 5 or len(password) < 5:
        die("username and password must be at least 5 characters (site rule)")


def playlist_display_name(playlist: dict[str, Any]) -> str:
    raw = str(playlist.get("name") or "")
    name = SYSTEM_PLAYLIST_DISPLAY_NAMES.get(raw, raw)
    return re.sub(r"\s+", " ", name).strip() or "(unnamed)"


def select_playlists(
    playlists: list[dict[str, Any]], selectors: list[str] | None
) -> list[dict[str, Any]]:
    if not selectors:
        return playlists

    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for selector in selectors:
        wanted = selector.strip()
        matches = [
            playlist for playlist in playlists if str(playlist.get("id")) == wanted
        ]
        if not matches:
            system_name = SYSTEM_PLAYLIST_ALIASES.get(wanted.casefold())
            if system_name:
                matches = [
                    playlist
                    for playlist in playlists
                    if playlist.get("name") == system_name
                ]
            else:
                matches = [
                    playlist
                    for playlist in playlists
                    if str(playlist.get("name") or "").strip() == wanted
                ]
        if not matches:
            die(f"playlist not found: {selector!r}; run: asmr-one playlists")
        if len(matches) > 1:
            ids = ", ".join(str(playlist.get("id")) for playlist in matches)
            die(f"playlist name is ambiguous: {selector!r}; use an ID: {ids}")
        playlist = matches[0]
        playlist_id = str(playlist.get("id"))
        if playlist_id not in seen_ids:
            seen_ids.add(playlist_id)
            selected.append(playlist)
    return selected


def work_identity(work: dict[str, Any]) -> tuple[str, str]:
    if work.get("id") is not None:
        return "id", str(work["id"])
    if work.get("source_id") is not None:
        return "source_id", str(work["source_id"])
    return "payload", json.dumps(work, ensure_ascii=False, sort_keys=True, default=str)


def truncate_utf8(value: str, max_bytes: int) -> str:
    if len(value.encode("utf-8")) <= max_bytes:
        return value
    return value.encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def sanitize(name: str, *, max_bytes: int = MAX_LOCAL_COMPONENT_BYTES) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name).strip(" .")
    name = truncate_utf8(name, max_bytes).rstrip(" .")
    return name or "untitled"


def work_folder_name(work: dict[str, Any]) -> str:
    source = sanitize(str(work.get("source_id") or work.get("id") or "work"))
    title = sanitize(str(work.get("title") or "untitled"))
    return sanitize(f"{source} {title}", max_bytes=MAX_WORK_FOLDER_BYTES)


def first_list_container(
    payload: Mapping[str, Any], keys: tuple[str, ...]
) -> list[Any] | None:
    empty: list[Any] | None = None
    for key in keys:
        candidate = payload.get(key)
        if not isinstance(candidate, list):
            continue
        if candidate:
            return candidate
        if empty is None:
            empty = candidate
    return empty


def flatten_tracks(nodes: Any, prefix: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    if isinstance(nodes, dict):
        nodes = first_list_container(nodes, ("children", "tracks", "files"))
    if not isinstance(nodes, list):
        raise PayloadError("track payload has no list container")
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise PayloadError(f"track entry {index} is not an object")
        title = str(node.get("title") or "file")
        children = node.get("children")
        if node.get("type") == "folder" or (
            children is not None and "mediaDownloadUrl" not in node
        ):
            if not isinstance(children, list):
                raise PayloadError(f"track folder {title!r} has invalid children")
            files.extend(flatten_tracks(children, prefix + (title,)))
            continue
        url = node.get("mediaDownloadUrl") or node.get("mediaStreamUrl")
        if not url:
            continue
        ext = Path(str(url).split("?", 1)[0]).suffix.lower()
        files.append(
            {
                "title": title,
                "path": prefix + (title,),
                "url": url,
                "size": int(node["size"])
                if str(node.get("size") or "").isdigit()
                else node.get("size"),
                "hash": node.get("hash"),
                "type": node.get("type") or "file",
                "ext": ext,
            }
        )
    return files


def object_items(items: list[Any], label: str) -> list[dict[str, Any]]:
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise PayloadError(f"{label} entry {index} is not an object")
    return items


def work_items(items: list[Any], label: str) -> list[dict[str, Any]]:
    works: list[dict[str, Any]] = []
    for index, item in enumerate(object_items(items, label)):
        if "work" not in item:
            works.append(item)
            continue
        work = item["work"]
        if not isinstance(work, dict):
            raise PayloadError(f"{label} entry {index} has invalid work")
        works.append(work)
    return works


def path_lang_score(parts: tuple[str, ...]) -> int:
    blob = " ".join(parts).lower()
    if any(h.lower() in blob for h in JA_HINTS):
        return 0
    if any(x in blob for x in ("英語", "english", "en-us", "en_us", "02英")):
        return 2
    if any(x in blob for x in ("簡体", "简体", "中文", "zh-cn", "zh_cn")):
        return 3
    if any(x in blob for x in ("繁体", "繁體", "zh-tw", "zh_tw")):
        return 4
    if any(x in blob for x in ("韓国", "한국", "korean", "ko-kr")):
        return 5
    return 1


def format_rank(ext: str, preferred: str) -> int:
    ext = ext.lower()
    if preferred == "all":
        return 0
    if preferred == "best":
        order = {".wav": 0, ".flac": 1, ".m4a": 2, ".mp3": 3, ".ogg": 4, ".aac": 5}
        return order.get(ext, 9)
    want = f".{preferred.lstrip('.').lower()}"
    return 0 if ext == want else 9


def select_files(
    files: list[dict[str, Any]],
    *,
    audio_format: str,
    include_subs: bool,
    all_langs: bool,
) -> list[dict[str, Any]]:
    if not all_langs:
        best_lang = min((path_lang_score(f["path"]) for f in files), default=1)
        files = [f for f in files if path_lang_score(f["path"]) == best_lang]

    if audio_format == "all":
        chosen = [f for f in files if include_subs or f["ext"] not in SUB_EXTS]
    else:
        audio = [f for f in files if f["ext"] in AUDIO_EXTS or f["type"] == "audio"]
        if audio_format == "best" and audio:
            best = min(format_rank(f["ext"], audio_format) for f in audio)
            audio = [f for f in audio if format_rank(f["ext"], audio_format) == best]
        elif audio_format != "best":
            wanted = f".{audio_format.lstrip('.').lower()}"
            audio = [f for f in audio if f["ext"].lower() == wanted]
        chosen = list(audio)
        if include_subs:
            audio_stems = {Path(f["title"]).stem.lower() for f in audio}
            for f in files:
                if f["ext"] in SUB_EXTS and Path(f["title"]).stem.lower().split(".", 1)[
                    0
                ] in {s.split(".", 1)[0] for s in audio_stems}:
                    chosen.append(f)
    # de-dupe by relative path
    seen: set[tuple[str, ...]] = set()
    out: list[dict[str, Any]] = []
    for f in chosen:
        key = f["path"]
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    return out


class ApiError(RuntimeError):
    def __init__(
        self,
        status: int,
        body: str,
        headers: Mapping[str, str] | None = None,
    ):
        super().__init__(f"HTTP {status}: {body[:240]}")
        self.status = status
        self.body = body
        self.headers = {
            str(key).lower(): str(value) for key, value in (headers or {}).items()
        }
        self.retry_after_hint: float | None = None


class DownloadAuthorizationError(ApiError):
    """A raw media URL expired and should be refreshed before retrying."""


def bounded_retry_after(seconds: float) -> float | None:
    if not math.isfinite(seconds):
        return None
    return min(MAX_RETRY_AFTER_SECONDS, max(0.0, seconds))


def retry_after_seconds(exc: BaseException) -> float | None:
    if isinstance(exc, RequestTransportError):
        if exc.retry_after is None:
            return None
        return bounded_retry_after(exc.retry_after)
    if not isinstance(exc, ApiError):
        return None
    candidates: list[float] = []
    if exc.retry_after_hint is not None:
        hint = bounded_retry_after(exc.retry_after_hint)
        if hint is not None:
            candidates.append(hint)
    value = exc.headers.get("retry-after")
    if value is not None:
        try:
            parsed = bounded_retry_after(float(value))
        except ValueError:
            try:
                retry_at = email.utils.parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=datetime.timezone.utc)
                parsed = bounded_retry_after(retry_at.timestamp() - time.time())
            except (TypeError, ValueError, OverflowError, OSError):
                parsed = None
        if parsed is not None:
            candidates.append(parsed)
    return max(candidates) if candidates else None


def is_retryable_network_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            RetryableDownloadError,
            RequestTransportError,
            DownloadAuthorizationError,
        ),
    ):
        return True
    if not isinstance(exc, ApiError):
        return False
    return exc.status in {408, 425, 429} or 500 <= exc.status <= 599


NETWORK_RETRY_POLICY = RetryPolicy(
    RETRY_DELAYS,
    is_retryable_network_error,
    retry_after_seconds,
)
LOCK_RETRY_POLICY = RetryPolicy(
    RETRY_DELAYS,
    lambda exc: isinstance(exc, WorkLockedError),
)


class Client:
    def __init__(self, token: str | None = None, timeout: int = 30):
        self.token = token
        self.timeout = timeout
        self.mirror = DEFAULT_MIRRORS[0]
        self._ctx = ssl.create_default_context()

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {
            "User-Agent": USER_AGENT,
            "Referer": "https://asmr.one/",
            "Origin": "https://asmr.one",
            "Accept": "application/json, */*",
        }
        if extra:
            headers.update(
                key_value
                for key_value in extra.items()
                if key_value[0].casefold() != "authorization"
            )
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        raw_url: str | None = None,
        stream: bool = False,
        headers: dict[str, str] | None = None,
        range_header: str | None = None,
    ) -> tuple[int, Any]:
        body = None
        extra = dict(headers or {})
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            extra["Content-Type"] = "application/json"
        if range_header:
            extra["Range"] = range_header
        last_err: Exception | None = None
        longest_retry_after: float | None = None
        attempts: list[tuple[str, str | None]]
        if raw_url:
            attempts = [(raw_url, None)]
        else:
            mirrors = (self.mirror,) + tuple(
                mirror for mirror in DEFAULT_MIRRORS if mirror != self.mirror
            )
            attempts = [
                (f"{mirror.rstrip('/')}{path}", mirror) for mirror in mirrors
            ]
        for url, api_mirror in attempts:
            req = urllib.request.Request(
                url,
                data=body,
                method=method,
                headers=self._headers(extra),
            )
            if api_mirror is not None and self.token:
                # Sent to the selected API mirror, but not copied by urllib
                # when a redirect targets another origin.
                req.add_unredirected_header(
                    "Authorization", f"Bearer {self.token}"
                )
            try:
                resp = urllib.request.urlopen(
                    req, timeout=self.timeout, context=self._ctx
                )
                status = getattr(resp, "status", 200)
                if stream:
                    if api_mirror is not None:
                        self.mirror = api_mirror
                    return status, resp
                try:
                    raw = resp.read()
                finally:
                    resp.close()
                if raw:
                    try:
                        payload = json.loads(raw.decode("utf-8"))
                    except json.JSONDecodeError:
                        payload = raw
                else:
                    payload = None
                if api_mirror is not None:
                    self.mirror = api_mirror
                return status, payload
            except urllib.error.HTTPError as exc:
                try:
                    payload = exc.read().decode("utf-8", "replace")
                except (
                    urllib.error.URLError,
                    TimeoutError,
                    ssl.SSLError,
                    http.client.HTTPException,
                    ConnectionError,
                    OSError,
                ) as payload_error:
                    last_err = payload_error
                    continue
                finally:
                    exc.close()
                error_type = (
                    DownloadAuthorizationError
                    if raw_url and exc.code in {401, 403}
                    else ApiError
                )
                error = error_type(
                    exc.code,
                    payload,
                    dict(exc.headers.items()) if exc.headers is not None else None,
                )
                if (
                    exc.code in {408, 425, 429} or 500 <= exc.code <= 599
                ) and not raw_url:
                    observed_retry_after = retry_after_seconds(error)
                    if observed_retry_after is not None:
                        longest_retry_after = max(
                            longest_retry_after or 0.0,
                            observed_retry_after,
                        )
                    last_err = error
                    continue
                raise error from None
            except (
                urllib.error.URLError,
                TimeoutError,
                ssl.SSLError,
                http.client.HTTPException,
                ConnectionError,
                OSError,
            ) as exc:
                last_err = exc
                continue
        if isinstance(last_err, ApiError):
            last_err.retry_after_hint = longest_retry_after
            raise last_err
        raise RequestTransportError(
            str(last_err) if last_err else "all mirrors failed",
            retry_after=longest_retry_after,
        )

    def login(self, name: str, password: str) -> tuple[str, dict[str, Any]]:
        _, payload = self.request(
            "POST", "/api/auth/me", data={"name": name, "password": password}
        )
        if not isinstance(payload, dict):
            raise PayloadError(f"unexpected login payload: {payload!r}")
        token = payload.get("token") or payload.get("access_token")
        user = payload.get("user") if isinstance(payload.get("user"), dict) else payload
        if isinstance(user, dict) and not token:
            token = user.get("token")
        if not token:
            raise PayloadError(
                f"login succeeded but no token in {sorted(payload.keys())}"
            )
        self.token = str(token)
        return self.token, user if isinstance(user, dict) else {}

    def whoami(self) -> dict[str, Any]:
        _, payload = self.request("GET", "/api/auth/me")
        return payload if isinstance(payload, dict) else {"raw": payload}

    def work_info(self, work_id: str | int) -> dict[str, Any]:
        lookup = re.sub(r"^(?:RJ|VJ)0*", "", str(work_id), flags=re.IGNORECASE)
        _, payload = self.request("GET", f"/api/workInfo/{lookup}")
        if not isinstance(payload, dict):
            raise PayloadError(f"bad workInfo for {work_id}")
        return payload

    def tracks(self, work_id: str | int) -> list[dict[str, Any]]:
        lookup = re.sub(r"^(?:RJ|VJ)0*", "", str(work_id), flags=re.IGNORECASE)
        _, payload = self.request("GET", f"/api/tracks/{lookup}?v=2")
        try:
            return flatten_tracks(payload)
        except PayloadError as exc:
            raise PayloadError(f"bad tracks payload for {work_id}: {exc}") from exc

    def _page(self, path: str, page: int, page_size: int) -> dict[str, Any]:
        qs = urllib.parse.urlencode(
            {
                "page": page,
                "pageSize": page_size,
                "order": "updated_at",
                "sort": "desc",
            }
        )
        _, payload = self.request("GET", f"{path}?{qs}")
        return payload if isinstance(payload, dict) else {"works": payload}

    @staticmethod
    def _page_count(
        payload: dict[str, Any],
        *,
        page: int,
        page_size: int,
        page_items: int,
    ) -> int | None:
        pagination = (
            payload.get("pagination")
            if isinstance(payload.get("pagination"), dict)
            else {}
        )
        current = int(pagination.get("currentPage") or pagination.get("page") or page)
        page_count = pagination.get("pageCount") or pagination.get("totalPages")
        if page_count is not None and current >= int(page_count):
            return max(1, int(page_count))
        if page_count is not None:
            return max(1, int(page_count))
        try:
            effective_page_size = int(pagination.get("pageSize") or page_size)
        except (TypeError, ValueError):
            effective_page_size = page_size
        if effective_page_size < 1:
            effective_page_size = page_size
        total_count = pagination.get("totalCount")
        if total_count is not None:
            try:
                total = max(0, int(total_count))
            except (TypeError, ValueError):
                pass
            else:
                return max(1, (total + effective_page_size - 1) // effective_page_size)
        return current if page_items < effective_page_size else None

    @classmethod
    def _fetched_page(
        cls,
        payload: dict[str, Any],
        items: list[dict[str, Any]],
        *,
        page: int,
        page_size: int,
    ) -> FetchedPage:
        page_count = cls._page_count(
            payload,
            page=page,
            page_size=page_size,
            page_items=len(items),
        )
        return FetchedPage(
            items,
            page_count,
            page_count is None or page < page_count,
        )

    def playlists_page(
        self, *, filter_by: str, page: int, page_size: int
    ) -> FetchedPage:
        qs = urllib.parse.urlencode(
            {"page": page, "pageSize": page_size, "filterBy": filter_by}
        )
        _, payload = self.request("GET", f"/api/playlist/get-playlists?{qs}")
        if not isinstance(payload, dict) or not isinstance(
            payload.get("playlists"), list
        ):
            keys = (
                sorted(payload.keys())
                if isinstance(payload, dict)
                else type(payload).__name__
            )
            raise PayloadError(f"unexpected playlists shape: {keys}")
        playlists = object_items(payload["playlists"], "playlist")
        return self._fetched_page(payload, playlists, page=page, page_size=page_size)

    def playlist_works_page(
        self, playlist_id: str, *, page: int, page_size: int
    ) -> FetchedPage:
        qs = urllib.parse.urlencode(
            {"id": playlist_id, "page": page, "pageSize": page_size}
        )
        _, payload = self.request("GET", f"/api/playlist/get-playlist-works?{qs}")
        if not isinstance(payload, dict) or not isinstance(payload.get("works"), list):
            keys = (
                sorted(payload.keys())
                if isinstance(payload, dict)
                else type(payload).__name__
            )
            raise PayloadError(f"unexpected playlist works shape: {keys}")
        works = work_items(payload["works"], "playlist work")
        return self._fetched_page(payload, works, page=page, page_size=page_size)

    def review_page(self, *, page: int, page_size: int) -> FetchedPage:
        payload = self._page("/api/review", page, page_size)
        raw_works = first_list_container(
            payload,
            ("works", "items", "favorites"),
        )
        if raw_works is None:
            raise PayloadError(f"unexpected collection shape: {sorted(payload.keys())}")
        works = work_items(raw_works, "review work")
        return self._fetched_page(payload, works, page=page, page_size=page_size)

    def iter_playlists(
        self, *, filter_by: str = "all", page_size: int = 50
    ) -> Iterator[dict[str, Any]]:
        page = 1
        while True:
            result = self.playlists_page(
                filter_by=filter_by, page=page, page_size=page_size
            )
            yield from result.items
            if not result.has_more:
                break
            page += 1

    def iter_playlist_works(
        self, playlist_id: str, *, page_size: int = 50
    ) -> Iterator[dict[str, Any]]:
        page = 1
        while True:
            result = self.playlist_works_page(
                playlist_id, page=page, page_size=page_size
            )
            yield from result.items
            if not result.has_more:
                break
            page += 1

    def iter_collection(
        self, source: str, page_size: int = 50
    ) -> Iterator[dict[str, Any]]:
        if source in PLAYLIST_SOURCES:
            seen: set[tuple[str, str]] = set()
            for playlist in self.iter_playlists(filter_by="all", page_size=page_size):
                playlist_id = playlist.get("id")
                if playlist_id is None:
                    raise PayloadError("playlist is missing its id")
                for work in self.iter_playlist_works(
                    str(playlist_id), page_size=page_size
                ):
                    key = work_identity(work)
                    if key not in seen:
                        seen.add(key)
                        yield work
            return
        if source != "review":
            raise ValueError(f"unknown collection source: {source}")

        log("collection endpoint: /api/review")
        page = 1
        while True:
            result = self.review_page(page=page, page_size=page_size)
            yield from result.items
            if not result.has_more:
                break
            page += 1


def relative_file_path(file: dict[str, Any]) -> Path:
    parts = [sanitize(p) for p in file["path"]]
    return Path(*parts) if parts else Path(sanitize(file["title"]))


def part_file_path(dest: Path) -> Path:
    return dest.with_suffix(dest.suffix + ".part")


def part_state_path(dest: Path) -> Path:
    part = part_file_path(dest)
    return part.with_suffix(part.suffix + ".json")


def expected_file_size(file: dict[str, Any]) -> int | None:
    value = file.get("size")
    try:
        size = int(value)
    except (TypeError, ValueError):
        return None
    return size if size >= 0 else None


def stable_remote_id(file: Mapping[str, Any]) -> str | None:
    value = file.get("hash")
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def remote_fingerprint(file: dict[str, Any]) -> dict[str, Any]:
    url_path = urllib.parse.urlsplit(str(file.get("url") or "")).path
    return {
        "id": stable_remote_id(file),
        "url_path": url_path,
        "source_path": [str(part) for part in file.get("path") or ()],
        "size": expected_file_size(file),
    }


def remote_identity_matches(
    saved: Mapping[str, Any], current: Mapping[str, Any]
) -> bool:
    saved_id = saved.get("id")
    current_id = current.get("id")
    if (
        isinstance(saved_id, str)
        and bool(saved_id.strip())
        and isinstance(current_id, str)
        and saved_id == current_id
    ):
        return saved.get("source_path") == current.get(
            "source_path"
        ) and saved.get("size") == current.get("size")
    return dict(saved) == dict(current)


def local_path_key(path: Path) -> str:
    return unicodedata.normalize("NFC", path.as_posix()).casefold()


def empty_checksum_manifest() -> dict[str, Any]:
    return {
        "version": CHECKSUM_VERSION,
        "algorithm": CHECKSUM_ALGORITHM,
        "files": {},
    }


def json_object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def checksum_manifest_key(relative: str, manifest_path: Path) -> str:
    relative_path = Path(relative)
    if (
        not relative
        or "\\" in relative
        or relative_path.is_absolute()
        or relative_path.as_posix() != relative
        or any(part in {"", ".", ".."} for part in relative_path.parts)
    ):
        raise LocalStateError(f"invalid checksum path {relative!r} in {manifest_path}")
    reserved = {
        local_path_key(Path(CHECKSUM_FILE_NAME)): CHECKSUM_FILE_NAME,
        local_path_key(Path(WORK_LOCK_FILE_NAME)): WORK_LOCK_FILE_NAME,
    }
    reserved_name = (
        reserved.get(local_path_key(Path(relative_path.parts[0])))
        if relative_path.parts
        else None
    )
    if reserved_name is not None:
        raise LocalStateError(
            f"checksum path conflicts with reserved {reserved_name}: {relative!r}"
        )
    return local_path_key(relative_path)


def load_checksum_manifest(root: Path) -> dict[str, Any]:
    path = root / CHECKSUM_FILE_NAME
    if path.is_symlink():
        raise LocalStateError(f"checksum manifest must not be a symlink: {path}")
    if not path.exists():
        return empty_checksum_manifest()
    if not path.is_file():
        raise LocalStateError(f"checksum manifest is not a file: {path}")
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=json_object_without_duplicates,
        )
    except (OSError, ValueError) as exc:
        raise LocalStateError(f"cannot read checksum manifest {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise LocalStateError(f"invalid checksum manifest root: {path}")
    if payload.get("version") != CHECKSUM_VERSION:
        raise LocalStateError(
            f"unsupported checksum manifest version {payload.get('version')!r}: {path}"
        )
    if payload.get("algorithm") != CHECKSUM_ALGORITHM:
        raise LocalStateError(
            f"unsupported checksum algorithm {payload.get('algorithm')!r}: {path}"
        )
    records = payload.get("files")
    if not isinstance(records, dict):
        raise LocalStateError(f"invalid checksum manifest files map: {path}")
    normalized_keys: dict[str, str] = {}
    for relative, record in records.items():
        if not isinstance(relative, str) or not isinstance(record, dict):
            raise LocalStateError(f"invalid checksum record in {path}")
        normalized = checksum_manifest_key(relative, path)
        previous = normalized_keys.get(normalized)
        if previous is not None:
            raise LocalStateError(
                f"checksum paths collide locally: {previous!r} and {relative!r} in {path}"
            )
        normalized_keys[normalized] = relative
        digest = record.get("blake3")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
            raise LocalStateError(f"invalid BLAKE3 for {relative!r} in {path}")
        size = record.get("size")
        mtime_ns = record.get("mtime_ns")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or isinstance(mtime_ns, bool)
            or not isinstance(mtime_ns, int)
            or not isinstance(record.get("remote"), dict)
        ):
            raise LocalStateError(f"invalid metadata for {relative!r} in {path}")
    return payload


def fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save_json_atomic(path: Path, payload: dict[str, Any], *, prefix: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise LocalStateError(f"JSON state must not be a symlink: {path}")
    fd, tmp_name = tempfile.mkstemp(prefix=prefix, suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        fsync_directory(path.parent)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def save_checksum_manifest(root: Path, manifest: dict[str, Any]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    path = root / CHECKSUM_FILE_NAME
    if path.is_symlink():
        raise LocalStateError(f"checksum manifest must not be a symlink: {path}")
    save_json_atomic(path, manifest, prefix=".checksums.")


def strong_etag(headers: Mapping[str, Any]) -> str | None:
    value = headers.get("ETag") or headers.get("etag")
    if value is None:
        return None
    etag = str(value).strip()
    if not etag or etag.casefold().startswith("w/"):
        return None
    return etag


def load_partial_state(dest: Path) -> PartialState | None:
    path = part_state_path(dest)
    if path.is_symlink():
        raise LocalStateError(f"partial state must not be a symlink: {path}")
    if not path.exists():
        return None
    if not path.is_file():
        raise LocalStateError(f"partial state is not a regular file: {path}")
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=json_object_without_duplicates,
        )
    except OSError as exc:
        raise LocalStateError(f"cannot read partial state {path}: {exc}") from exc
    except ValueError:
        return None
    if not isinstance(payload, dict) or payload.get("version") != PART_STATE_VERSION:
        return None
    remote = payload.get("remote")
    size = payload.get("committed_size")
    digest = payload.get("committed_blake3")
    etag = payload.get("etag")
    if (
        not isinstance(remote, dict)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", digest) is None
        or (etag is not None and not isinstance(etag, str))
        or (isinstance(etag, str) and strong_etag({"ETag": etag}) is None)
    ):
        return None
    return PartialState(remote, size, digest.lower(), etag)


def save_partial_state(dest: Path, state: PartialState) -> None:
    save_json_atomic(
        part_state_path(dest),
        {
            "version": PART_STATE_VERSION,
            "remote": state.remote,
            "committed_size": state.committed_size,
            "committed_blake3": state.committed_blake3,
            "etag": state.etag,
        },
        prefix=".part-state.",
    )


def remove_partial_state(dest: Path) -> None:
    path = part_state_path(dest)
    if path.is_symlink():
        raise LocalStateError(f"partial state must not be a symlink: {path}")
    try:
        path.unlink()
    except FileNotFoundError:
        return
    fsync_directory(path.parent)


def partial_identity_is_usable(state: PartialState, remote: dict[str, Any]) -> bool:
    state_remote_id = state.remote.get("id")
    remote_id = remote.get("id")
    if (
        isinstance(state_remote_id, str)
        and bool(state_remote_id.strip())
        and isinstance(remote_id, str)
        and state_remote_id == remote_id
    ):
        return remote_identity_matches(state.remote, remote)
    return state.etag is not None and state.remote == remote


def new_blake3_hasher() -> Any:
    # One global scheduler owns concurrency. Nested BLAKE3 worker pools would
    # multiply --jobs and oversubscribe the machine.
    return blake3(max_threads=1)


def update_hasher_from_file(hasher: Any, path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise LocalStateError(f"checksum target is not a regular file: {path}")
    with path.open("rb") as fh:
        while chunk := fh.read(DOWNLOAD_CHUNK_SIZE):
            hasher.update(chunk)


def checksum_file(
    path: Path, *, return_stat: bool = False
) -> str | tuple[str, os.stat_result]:
    if path.is_symlink() or not path.is_file():
        raise LocalStateError(f"checksum target is not a regular file: {path}")
    before = path.stat()
    hasher = new_blake3_hasher()
    update_hasher_from_file(hasher, path)
    if path.is_symlink() or not path.is_file():
        raise LocalStateError(f"checksum target changed while hashing: {path}")
    after = path.stat()
    if stat_signature(before) != stat_signature(after):
        raise LocalStateError(f"checksum target changed while hashing: {path}")
    digest = hasher.hexdigest()
    return (digest, after) if return_stat else digest


def checksum_record(
    file: dict[str, Any],
    dest: Path,
    digest: str,
    *,
    stat_result: os.stat_result | None = None,
) -> dict[str, Any]:
    stat = stat_result or dest.stat()
    return {
        "remote": remote_fingerprint(file),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "blake3": digest,
    }


def prepare_local_files(
    root: Path,
    files: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> tuple[list[LocalFileContext], bool]:
    mapped: list[tuple[dict[str, Any], Path, str, str]] = []
    occupied: dict[str, str] = {}
    occupied_descendants: dict[str, str] = {}
    reserved = {
        local_path_key(Path(CHECKSUM_FILE_NAME)): CHECKSUM_FILE_NAME,
        local_path_key(Path(WORK_LOCK_FILE_NAME)): WORK_LOCK_FILE_NAME,
    }
    for file in files:
        relative = relative_file_path(file)
        relative_string = relative.as_posix()
        reserved_name = (
            reserved.get(local_path_key(Path(relative.parts[0])))
            if relative.parts
            else None
        )
        if reserved_name is not None:
            raise LocalStateError(
                f"remote path conflicts with reserved {reserved_name}: {relative_string}"
            )
        for local_path, label in (
            (relative, relative_string),
            (part_file_path(relative), f"{relative_string} (partial)"),
            (part_state_path(relative), f"{relative_string} (partial state)"),
        ):
            key = local_path_key(local_path)
            previous = occupied.get(key)
            parts = key.split("/")
            if previous is None:
                for index in range(1, len(parts)):
                    previous = occupied.get("/".join(parts[:index]))
                    if previous is not None:
                        break
            if previous is None:
                previous = occupied_descendants.get(key)
            if previous is not None:
                raise LocalStateError(
                    f"remote paths collide locally after sanitizing: {previous!r} and {label!r}"
                )
            occupied[key] = label
            for index in range(1, len(parts)):
                occupied_descendants.setdefault("/".join(parts[:index]), label)
        mapped.append(
            (file, root / relative, relative_string, local_path_key(relative))
        )

    manifest_path = root / CHECKSUM_FILE_NAME
    raw_records: dict[str, Any] = manifest["files"]
    records: dict[str, Any] = {}
    record_sources: dict[str, str] = {}
    manifest_keys_changed = False
    for raw_key, record in raw_records.items():
        normalized_key = checksum_manifest_key(raw_key, manifest_path)
        previous = record_sources.get(normalized_key)
        if previous is not None:
            raise LocalStateError(
                f"checksum paths collide locally: {previous!r} and {raw_key!r} in {manifest_path}"
            )
        records[normalized_key] = record
        record_sources[normalized_key] = raw_key
        manifest_keys_changed |= raw_key != normalized_key
    manifest["files"] = records

    contexts = [
        LocalFileContext(file, dest, relative, record_key, records.get(record_key))
        for file, dest, relative, record_key in mapped
    ]
    return contexts, manifest_keys_changed


def inspect_local_file(context: LocalFileContext, *, verify: bool) -> FileInspection:
    file = context.file
    dest = context.dest
    relative = context.relative_path
    record = context.record
    part = part_file_path(dest)
    state_path = part_state_path(dest)
    for path in (dest, part, state_path):
        if path.is_symlink():
            raise LocalStateError(f"download target must not be a symlink: {path}")
        if path.exists() and not path.is_file():
            raise LocalStateError(f"download target is not a regular file: {path}")

    expected_size = expected_file_size(file)
    fingerprint = remote_fingerprint(file)
    remote_matches = bool(
        isinstance(record, dict)
        and isinstance(record.get("remote"), dict)
        and remote_identity_matches(record["remote"], fingerprint)
    )
    part_exists = part.exists()
    part_size = part.stat().st_size if part_exists else 0
    partial_state = load_partial_state(dest)
    committed_size = partial_state.committed_size if partial_state is not None else -1
    part_usable = bool(
        part_exists
        and partial_state is not None
        and partial_identity_is_usable(partial_state, fingerprint)
        and part_size >= committed_size
        and (
            (
                committed_size > 0
                and (expected_size is None or committed_size <= expected_size)
            )
            or (committed_size == 0 and expected_size == 0)
        )
    )
    partial_stale = bool(
        part_exists
        and partial_state is not None
        and not partial_identity_is_usable(partial_state, fingerprint)
    )

    if not dest.exists():
        if part_exists and isinstance(record, dict) and not remote_matches:
            plan = LocalFilePlan(file, dest, relative, "stale", resume=part_usable)
        elif partial_stale:
            plan = LocalFilePlan(file, dest, relative, "stale", resume=False)
        elif (
            part_exists
            and not part_usable
            and expected_size is not None
            and part_size > expected_size
        ):
            plan = LocalFilePlan(file, dest, relative, "corrupt", resume=False)
        elif part_usable:
            plan = LocalFilePlan(file, dest, relative, "partial", resume=True)
        else:
            plan = LocalFilePlan(file, dest, relative, "missing", resume=False)
        return FileInspection(plan=plan)

    stat = dest.stat()
    if isinstance(record, dict) and not remote_matches:
        return FileInspection(
            plan=LocalFilePlan(file, dest, relative, "stale", resume=part_usable)
        )
    if expected_size is not None and stat.st_size != expected_size:
        return FileInspection(
            plan=LocalFilePlan(file, dest, relative, "corrupt", resume=part_usable)
        )
    if not isinstance(record, dict):
        if expected_size is None:
            return FileInspection(
                plan=LocalFilePlan(file, dest, relative, "stale", resume=part_usable)
            )
        return FileInspection(hash_reason="adopt")
    if record.get("size") != stat.st_size:
        return FileInspection(
            plan=LocalFilePlan(file, dest, relative, "corrupt", resume=part_usable)
        )
    if not verify and record.get("mtime_ns") == stat.st_mtime_ns:
        return FileInspection(plan=LocalFilePlan(file, dest, relative, "valid"))
    return FileInspection(
        hash_reason="verify",
        expected_digest=str(record["blake3"]),
        resume=part_usable,
    )


def hash_local_file(context: LocalFileContext) -> HashedFile:
    digest, stat_result = checksum_file(context.dest, return_stat=True)
    return HashedFile(
        digest,
        checksum_record(
            context.file,
            context.dest,
            digest,
            stat_result=stat_result,
        ),
    )


def resolve_hashed_file(
    context: LocalFileContext,
    inspection: FileInspection,
    hashed: HashedFile,
) -> tuple[LocalFilePlan, dict[str, Any] | None]:
    if inspection.hash_reason == "adopt":
        return (
            LocalFilePlan(
                context.file,
                context.dest,
                context.relative_path,
                "adopt",
                digest=hashed.digest,
            ),
            hashed.record,
        )
    if inspection.hash_reason != "verify" or inspection.expected_digest is None:
        raise RuntimeError("unexpected local hash continuation")
    if hashed.digest.lower() == inspection.expected_digest.lower():
        return (
            LocalFilePlan(
                context.file,
                context.dest,
                context.relative_path,
                "valid",
                digest=hashed.digest,
            ),
            hashed.record,
        )
    return (
        LocalFilePlan(
            context.file,
            context.dest,
            context.relative_path,
            "corrupt",
            resume=inspection.resume,
        ),
        None,
    )


def classify_local_files(
    root: Path,
    files: list[dict[str, Any]],
    manifest: dict[str, Any],
    *,
    verify: bool,
) -> tuple[list[LocalFilePlan], dict[str, dict[str, Any]], bool]:
    contexts, manifest_keys_changed = prepare_local_files(root, files, manifest)
    plans: list[LocalFilePlan] = []
    updates: dict[str, dict[str, Any]] = {}
    for context in contexts:
        inspection = inspect_local_file(context, verify=verify)
        if inspection.plan is not None:
            plans.append(inspection.plan)
            continue
        hashed = hash_local_file(context)
        plan, update = resolve_hashed_file(context, inspection, hashed)
        plans.append(plan)
        if update is not None:
            updates[context.record_key] = update
    return plans, updates, manifest_keys_changed


def parse_content_range(value: str | None) -> tuple[int, int, int | None]:
    match = re.fullmatch(
        r"bytes\s+(\d+)-(\d+)/(\d+|\*)", value or "", flags=re.IGNORECASE
    )
    if not match:
        raise RuntimeError(f"invalid Content-Range: {value!r}")
    start, end = int(match.group(1)), int(match.group(2))
    total = None if match.group(3) == "*" else int(match.group(3))
    if end < start or (total is not None and end >= total):
        raise RuntimeError(f"invalid Content-Range: {value!r}")
    return start, end, total


def parse_content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        length = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid Content-Length: {value!r}") from exc
    if length < 0:
        raise RuntimeError(f"invalid Content-Length: {value!r}")
    return length


def prepare_download_work(
    work: dict[str, Any],
    tracks: list[dict[str, Any]],
    *,
    out: str,
    audio_format: str,
    include_subs: bool,
    all_langs: bool,
) -> PreparedWork:
    files = select_files(
        tracks,
        audio_format=audio_format,
        include_subs=include_subs,
        all_langs=all_langs,
    )
    root = Path(out) / work_folder_name(work)
    manifest = load_checksum_manifest(root)
    contexts, manifest_keys_changed = prepare_local_files(root, files, manifest)
    return PreparedWork(root, manifest, contexts, manifest_keys_changed)


def stat_signature(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def require_file_signature(
    path: Path,
    expected: tuple[int, int, int, int, int],
) -> None:
    if path.is_symlink() or not path.is_file():
        raise LocalStateError(f"validated partial file disappeared: {path}")
    if stat_signature(path.stat()) != expected:
        raise LocalStateError(f"partial file changed before installation: {path}")


def open_validated_partial(
    dest: Path,
    remote: dict[str, Any],
    expected_size: int | None,
    *,
    resume: bool,
) -> tuple[Any | None, Any, PartialState | None, tuple[int, int, int, int, int] | None]:
    tmp = part_file_path(dest)
    hasher = new_blake3_hasher()
    state = load_partial_state(dest)
    if (
        not resume
        or state is None
        or not partial_identity_is_usable(state, remote)
        or not tmp.exists()
        or (expected_size is not None and state.committed_size > expected_size)
    ):
        return None, hasher, None, None
    if tmp.is_symlink() or not tmp.is_file():
        raise LocalStateError(f"partial file is not a regular file: {tmp}")

    fh = tmp.open("r+b")
    try:
        actual_size = os.fstat(fh.fileno()).st_size
        if actual_size < state.committed_size:
            fh.close()
            return None, new_blake3_hasher(), None, None
        if actual_size > state.committed_size:
            fh.truncate(state.committed_size)
            fh.flush()
            os.fsync(fh.fileno())
        before = stat_signature(os.fstat(fh.fileno()))
        fh.seek(0)
        remaining = state.committed_size
        while remaining:
            chunk = fh.read(min(DOWNLOAD_CHUNK_SIZE, remaining))
            if not chunk:
                fh.close()
                return None, new_blake3_hasher(), None, None
            hasher.update(chunk)
            remaining -= len(chunk)
        after = stat_signature(os.fstat(fh.fileno()))
        if before != after or hasher.hexdigest().lower() != state.committed_blake3:
            fh.close()
            return None, new_blake3_hasher(), None, None
        fh.seek(state.committed_size)
        return fh, hasher, state, after
    except BaseException:
        if not fh.closed:
            fh.close()
        raise


def checkpoint_partial(
    dest: Path,
    fh: Any,
    hasher: Any,
    remote: dict[str, Any],
    etag: str | None,
) -> tuple[PartialState, tuple[int, int, int, int, int]]:
    fh.flush()
    os.fsync(fh.fileno())
    before = stat_signature(os.fstat(fh.fileno()))
    position = fh.tell()
    actual_size = before[2]
    if actual_size != position:
        raise LocalStateError(
            f"partial file size changed while downloading: {part_file_path(dest)}"
        )
    state = PartialState(remote, actual_size, hasher.hexdigest().lower(), etag)
    save_partial_state(dest, state)
    after = stat_signature(os.fstat(fh.fileno()))
    if before != after:
        raise LocalStateError(
            f"partial file changed while checkpointing: {part_file_path(dest)}"
        )
    return state, after


def validate_range_response(
    headers: Mapping[str, Any],
    *,
    existing: int,
    expected_size: int | None,
) -> tuple[int, int]:
    start, end, total = parse_content_range(headers.get("Content-Range"))
    if start != existing:
        raise RuntimeError(f"Content-Range starts at {start}, expected {existing}")
    if expected_size is not None and total != expected_size:
        raise RuntimeError(
            f"Content-Range total {total!r} != expected size {expected_size}"
        )
    if total is None or end + 1 != total:
        raise RuntimeError("Content-Range does not reach the end of the file")
    expected_body_size = end - start + 1
    content_length = parse_content_length(headers.get("Content-Length"))
    if content_length is not None and content_length != expected_body_size:
        raise RuntimeError("Content-Length does not match Content-Range")
    return expected_body_size, total


def close_response(response: Any | None) -> None:
    if response is not None:
        response.close()


def download_one(
    client: Client,
    url: str,
    dest: Path,
    expected_size: int | None,
    *,
    resume: bool,
    remote: dict[str, Any],
    checkpoint_size: int = PART_CHECKPOINT_SIZE,
    work_root: Path | None = None,
) -> DownloadResult:
    if checkpoint_size < 1:
        raise ValueError("checkpoint_size must be at least 1")
    safe_root = work_root or dest.parent
    ensure_safe_work_directory(safe_root, dest.parent)
    tmp = part_file_path(dest)
    for path in (dest, tmp, part_state_path(dest)):
        if path.is_symlink():
            raise LocalStateError(f"download target must not be a symlink: {path}")
        if path.exists() and not path.is_file():
            raise LocalStateError(f"download target is not a regular file: {path}")

    partial_fh, hasher, partial_state, partial_signature = open_validated_partial(
        dest,
        remote,
        expected_size,
        resume=resume,
    )
    existing = partial_state.committed_size if partial_state is not None else 0
    remote_id = remote.get("id")
    has_stable_remote_id = isinstance(remote_id, str) and bool(remote_id.strip())
    if (
        partial_fh is not None
        and expected_size is not None
        and existing == expected_size
        and has_stable_remote_id
    ):
        digest = hasher.hexdigest()
        if partial_signature is None:
            raise LocalStateError(f"validated partial file disappeared: {tmp}")
        partial_fh.close()
        ensure_safe_work_directory(safe_root, dest.parent)
        require_file_signature(tmp, partial_signature)
        os.replace(tmp, dest)
        fsync_directory(dest.parent)
        remove_partial_state(dest)
        return DownloadResult("resume", digest)

    range_header = f"bytes={existing}-" if existing else None
    request_headers = (
        {"If-Range": partial_state.etag}
        if existing and partial_state is not None and partial_state.etag is not None
        else None
    )
    resp: Any | None = None
    try:
        status, resp = client.request(
            "GET",
            "",
            raw_url=url,
            stream=True,
            headers=request_headers,
            range_header=range_header,
        )
    except ApiError as exc:
        if not existing or exc.status != 416:
            if partial_fh is not None:
                partial_fh.close()
            raise
        if partial_fh is not None:
            partial_fh.close()
            partial_fh = None
        existing = 0
        hasher = new_blake3_hasher()
        status, resp = client.request("GET", "", raw_url=url, stream=True)

    result_status = "ok"
    effective_size = expected_size
    expected_body_size: int | None = None
    try:
        headers = getattr(resp, "headers", {})
        if existing and status == 206:
            try:
                expected_body_size, total = validate_range_response(
                    headers,
                    existing=existing,
                    expected_size=expected_size,
                )
                response_etag = strong_etag(headers)
                if (
                    partial_state is not None
                    and partial_state.etag is not None
                    and response_etag is not None
                    and response_etag != partial_state.etag
                ):
                    raise RuntimeError("ETag changed during ranged download")
            except RuntimeError:
                close_response(resp)
                resp = None
                if partial_fh is not None:
                    partial_fh.close()
                    partial_fh = None
                existing = 0
                hasher = new_blake3_hasher()
                status, resp = client.request("GET", "", raw_url=url, stream=True)
                headers = getattr(resp, "headers", {})
            else:
                if effective_size is None:
                    effective_size = total
                result_status = "resume"
        elif existing and status == 200:
            if partial_fh is not None:
                partial_fh.close()
                partial_fh = None
            existing = 0
            hasher = new_blake3_hasher()

        if existing and status == 206 and result_status == "resume":
            if partial_fh is None or partial_signature is None:
                raise LocalStateError(f"validated partial file disappeared: {tmp}")
            if stat_signature(os.fstat(partial_fh.fileno())) != partial_signature:
                raise LocalStateError(f"partial file changed before download: {tmp}")
            fh = partial_fh
            selected_etag = strong_etag(headers) or (
                partial_state.etag if partial_state is not None else None
            )
        elif status == 206:
            try:
                expected_body_size, total = validate_range_response(
                    headers,
                    existing=0,
                    expected_size=expected_size,
                )
            except RuntimeError as exc:
                raise DownloadProtocolError(str(exc)) from exc
            effective_size = total if effective_size is None else effective_size
            selected_etag = strong_etag(headers)
            fh = tmp.open("wb")
            checkpoint_partial(dest, fh, hasher, remote, selected_etag)
        elif status != 200:
            raise DownloadProtocolError(f"unexpected download HTTP status {status}")
        else:
            content_length = parse_content_length(headers.get("Content-Length"))
            if (
                content_length is not None
                and expected_size is not None
                and content_length != expected_size
            ):
                raise DownloadProtocolError(
                    f"Content-Length {content_length} != expected size {expected_size}"
                )
            expected_body_size = content_length
            if effective_size is None and content_length is not None:
                effective_size = content_length
            selected_etag = strong_etag(headers)
            fh = tmp.open("wb")
            checkpoint_partial(dest, fh, hasher, remote, selected_etag)

        start_size = existing
        start_hasher = hasher.copy()
        start_etag = (
            partial_state.etag
            if result_status == "resume" and partial_state
            else selected_etag
        )
        received_size = 0
        last_checkpoint = start_size
        try:
            while True:
                try:
                    chunk = resp.read(DOWNLOAD_CHUNK_SIZE)
                except http.client.IncompleteRead as exc:
                    chunk = bytes(exc.partial or b"")
                    if chunk:
                        fh.write(chunk)
                        hasher.update(chunk)
                        received_size += len(chunk)
                    checkpoint_partial(dest, fh, hasher, remote, selected_etag)
                    raise DownloadTransportError(
                        f"incomplete HTTP read after {received_size} bytes"
                    ) from exc
                except (
                    TimeoutError,
                    urllib.error.URLError,
                    ssl.SSLError,
                    ConnectionError,
                    OSError,
                ) as exc:
                    checkpoint_partial(dest, fh, hasher, remote, selected_etag)
                    raise DownloadTransportError(str(exc)) from exc
                if not chunk:
                    break
                fh.write(chunk)
                hasher.update(chunk)
                received_size += len(chunk)
                if fh.tell() - last_checkpoint >= checkpoint_size:
                    checkpoint_partial(dest, fh, hasher, remote, selected_etag)
                    last_checkpoint = fh.tell()
            if expected_body_size is not None and received_size < expected_body_size:
                checkpoint_partial(dest, fh, hasher, remote, selected_etag)
                raise IncompleteDownloadError(
                    f"response body length {received_size} != expected {expected_body_size}"
                )
            if expected_body_size is not None and received_size > expected_body_size:
                fh.truncate(start_size)
                fh.seek(start_size)
                checkpoint_partial(
                    dest,
                    fh,
                    start_hasher,
                    remote,
                    start_etag,
                )
                raise DownloadProtocolError(
                    f"response body length {received_size} != expected {expected_body_size}"
                )
            final_state, final_signature = checkpoint_partial(
                dest,
                fh,
                hasher,
                remote,
                selected_etag,
            )
        finally:
            fh.close()
    finally:
        close_response(resp)
        if partial_fh is not None and not partial_fh.closed:
            partial_fh.close()

    final_size = final_state.committed_size
    if effective_size is not None and final_size != effective_size:
        if final_size < effective_size:
            raise IncompleteDownloadError(
                f"size mismatch {final_size} != {effective_size}"
            )
        raise DownloadProtocolError(f"size mismatch {final_size} != {effective_size}")
    if dest.is_symlink():
        raise LocalStateError(f"download target must not be a symlink: {dest}")
    ensure_safe_work_directory(safe_root, dest.parent)
    require_file_signature(tmp, final_signature)
    os.replace(tmp, dest)
    fsync_directory(dest.parent)
    remove_partial_state(dest)
    return DownloadResult(result_status, final_state.committed_blake3)


def download_file_and_record(
    client: Client,
    context: LocalFileContext,
    plan: LocalFilePlan,
    *,
    work_root: Path | None = None,
) -> DownloadOutcome:
    result = download_one(
        client,
        plan.file["url"],
        plan.dest,
        expected_file_size(plan.file),
        resume=plan.resume,
        remote=remote_fingerprint(plan.file),
        work_root=work_root,
    )
    return DownloadOutcome(
        result,
        checksum_record(context.file, context.dest, result.digest),
        context,
        plan,
    )


def cmd_login(args: argparse.Namespace) -> None:
    env_name, env_password = load_env_credentials()
    name = args.name or env_name or input("username: ").strip()
    password = args.password or env_password
    if not password:
        import getpass

        password = getpass.getpass("password: ")
    validate_credentials(name, password)
    client = Client(timeout=args.timeout)
    token, user = client.login(name, password)
    save_token(token, name=name)
    shown = user.get("name") or user.get("username") or name
    log(f"logged in as {shown}; token saved to {TOKEN_PATH}")


def require_client(args: argparse.Namespace, *, need_login: bool = True) -> Client:
    token = load_token()
    if token or not need_login:
        return Client(token=token, timeout=args.timeout)

    name, password = load_env_credentials()
    if not name and not password:
        die("not logged in. set ASMR_NAME and ASMR_PASSWORD, or run: asmr-one login")
    if not name or not password:
        die("automatic login requires both ASMR_NAME and ASMR_PASSWORD")
    validate_credentials(name, password)

    client = Client(timeout=args.timeout)
    token, _ = client.login(name, password)
    client.token = token
    return client


def collect_playlist_works(
    client: Client, playlists: list[dict[str, Any]], *, page_size: int
) -> list[dict[str, Any]]:
    works: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for playlist in playlists:
        playlist_id = playlist.get("id")
        if playlist_id is None:
            raise PayloadError("playlist is missing its id")
        for work in client.iter_playlist_works(str(playlist_id), page_size=page_size):
            key = work_identity(work)
            if key in seen:
                continue
            seen.add(key)
            works.append(work)
    return works


def collect_works(client: Client, args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.works:
        works = []
        for code in args.works:
            works.append(client.work_info(code))
        return works

    selectors = getattr(args, "playlist_selectors", None)
    if args.source == "review":
        if selectors:
            die("--playlist cannot be used with --source review")
        works = list(client.iter_collection("review", page_size=args.page_size))
    else:
        playlists = list(
            client.iter_playlists(filter_by="all", page_size=args.page_size)
        )
        playlists = select_playlists(playlists, selectors)
        log(f"{len(playlists)} playlists selected")
        works = collect_playlist_works(client, playlists, page_size=args.page_size)
    if args.limit:
        works = works[: args.limit]
    return works


def cmd_whoami(args: argparse.Namespace) -> None:
    client = require_client(args)
    payload = client.whoami()
    user = payload.get("user") if isinstance(payload.get("user"), dict) else payload
    log(json.dumps(user, ensure_ascii=False, indent=2))


def cmd_playlists(args: argparse.Namespace) -> None:
    client = require_client(args)
    playlists = list(client.iter_playlists(filter_by="all", page_size=args.page_size))
    log(f"{len(playlists)} playlists")
    for playlist in playlists:
        playlist_id = playlist.get("id") or ""
        name = playlist_display_name(playlist)
        count = playlist.get("works_count")
        log(f"{playlist_id}\t{name}\t{count if count is not None else ''}")


def cmd_list(args: argparse.Namespace) -> None:
    client = require_client(args, need_login=not args.works)
    works = collect_works(client, args)
    log(f"{len(works)} works")
    for work in works:
        source = work.get("source_id") or work.get("id")
        title = work.get("title") or ""
        log(f"{source}\t{title}")


class DownloadOperation:
    """A retryable file operation that refreshes remote metadata after failure."""

    def __init__(
        self,
        client: Client,
        work: dict[str, Any],
        args: argparse.Namespace,
        root: Path,
        context: LocalFileContext,
        plan: LocalFilePlan,
    ):
        self.client = client
        self.work = work
        self.args = args
        self.root = root
        self.context = context
        self.plan = plan
        self.calls = 0

    def _refresh(self) -> None:
        work_id = self.work.get("id") or self.work.get("source_id")
        if work_id is None:
            raise RemoteFileUnavailableError("work is missing its id")
        tracks = self.client.tracks(work_id)
        files = select_files(
            tracks,
            audio_format=self.args.format,
            include_subs=not self.args.no_subs,
            all_langs=not self.args.ja_only,
        )
        contexts, _ = prepare_local_files(
            self.root,
            files,
            empty_checksum_manifest(),
        )
        remote_id = stable_remote_id(self.context.file)
        if remote_id is not None:
            id_matches = [
                candidate
                for candidate in contexts
                if stable_remote_id(candidate.file) == remote_id
            ]
            if len(id_matches) > 1:
                raise RemoteFileUnavailableError(
                    f"remote id {remote_id!r} is no longer unique"
                )
            if not id_matches:
                raise RemoteFileUnavailableError(
                    f"remote id {remote_id!r} is no longer available"
                )
            refreshed = id_matches[0]
        else:
            path_matches = [
                candidate
                for candidate in contexts
                if candidate.record_key == self.context.record_key
            ]
            if len(path_matches) != 1:
                raise RemoteFileUnavailableError(
                    f"remote file is missing or ambiguous: {self.context.relative_path}"
                )
            refreshed = path_matches[0]
        original = self.context
        self.context = LocalFileContext(
            refreshed.file,
            original.dest,
            original.relative_path,
            original.record_key,
            original.record,
        )
        self.plan = LocalFilePlan(
            refreshed.file,
            original.dest,
            original.relative_path,
            "partial",
            resume=True,
        )

    def __call__(self) -> DownloadOutcome:
        if self.calls:
            self._refresh()
        self.calls += 1
        return download_file_and_record(
            self.client,
            self.context,
            self.plan,
            work_root=self.root,
        )


class DownloadCoordinator:
    DISCOVERY_OWNER = "discovery"

    def __init__(self, client: Client, args: argparse.Namespace):
        self.client = client
        self.args = args
        self.scheduler = TaskScheduler(args.jobs)
        self.pending_works: deque[dict[str, Any]] = deque()
        self.active_works: dict[str, WorkState] = {}
        self.outcomes: list[WorkOutcome] = []
        self.seen_works: set[tuple[str, str]] = set()
        self.resolved_work_owners: dict[tuple[str, str], str] = {}
        self.admitted_works = 0
        self.collection_failures = 0
        self.discovery_stopped = False
        self.discovery_done = False
        self._work_serial = 0
        self._streams: list[OrderedPageStream] = []
        self._playlists: list[dict[str, Any]] = []
        self._playlist_listing_done = False
        self._playlist_count = 0
        self._playlist_release_index = 0
        self._playlist_buffers: dict[int, deque[dict[str, Any]]] = {}
        self._playlist_done: set[int] = set()
        self.max_active_works = args.jobs * 2

    def run_collection(self) -> DownloadSummary:
        works = getattr(self.args, "works", None)
        if works:
            seen_codes: set[str] = set()
            for code in works:
                normalized = re.sub(
                    r"^(?:RJ|VJ)0*", "", str(code), flags=re.IGNORECASE
                ).casefold()
                if normalized in seen_codes:
                    continue
                seen_codes.add(normalized)
                self._admit_work({"source_id": code}, apply_limit=False)
            self.discovery_done = True
        elif self.args.source == "review":
            if getattr(self.args, "playlist_selectors", None):
                die("--playlist cannot be used with --source review")
            log("collection endpoint: /api/review")
            self._start_review_stream()
        else:
            self._start_playlist_listing()
        try:
            self.scheduler.run()
            return self._summary()
        finally:
            self._release_all_locks()

    def run_direct(self, works: list[dict[str, Any]]) -> DownloadSummary:
        for work in works:
            self._admit_work(work, apply_limit=False)
        self.discovery_done = True
        try:
            self.scheduler.run()
            return self._summary()
        finally:
            self._release_all_locks()

    def _summary(self) -> DownloadSummary:
        if self.admitted_works == 0 and self.collection_failures == 0:
            die("collection is empty")
        return DownloadSummary(
            works=self.admitted_works,
            ok=sum(outcome.ok for outcome in self.outcomes),
            skip=sum(outcome.skip for outcome in self.outcomes),
            fail=sum(outcome.fail for outcome in self.outcomes)
            + self.collection_failures,
        )

    def _new_stream(
        self,
        *,
        label: str,
        fetch_page: Callable[[int], FetchedPage],
        on_items: Callable[[list[dict[str, Any]]], None],
        on_done: Callable[[], None],
    ) -> None:
        stream = OrderedPageStream(
            self.scheduler,
            owner=self.DISCOVERY_OWNER,
            label=label,
            fetch_page=fetch_page,
            on_items=on_items,
            on_done=on_done,
            on_error=self._collection_error,
            should_stop=lambda: self.discovery_stopped,
            retry_policy=NETWORK_RETRY_POLICY,
            on_retry=self._collection_retry,
        )
        self._streams.append(stream)
        stream.start()

    def _collection_retry(
        self,
        label: str,
        exc: BaseException,
        attempt: int,
        total: int,
        delay: float,
    ) -> None:
        log(
            f"RETRY collection {label} attempt={attempt}/{total} "
            f"after={format_delay(delay)}  {exc}"
        )

    def _start_review_stream(self) -> None:
        self._new_stream(
            label="review",
            fetch_page=lambda page: self.client.review_page(
                page=page, page_size=self.args.page_size
            ),
            on_items=lambda works: [self._admit_work(work) for work in works],
            on_done=self._finish_discovery,
        )

    def _start_playlist_listing(self) -> None:
        selectors = getattr(self.args, "playlist_selectors", None)

        def receive(playlists: list[dict[str, Any]]) -> None:
            self._playlists.extend(playlists)
            if not selectors:
                for playlist in playlists:
                    self._start_playlist_work_stream(playlist)

        def finish() -> None:
            self._playlist_listing_done = True
            if selectors:
                selected = select_playlists(self._playlists, selectors)
                for playlist in selected:
                    self._start_playlist_work_stream(playlist)
                log(f"{len(selected)} playlists selected")
            else:
                log(f"{len(self._playlists)} playlists selected")
            self._maybe_finish_playlist_discovery()

        self._new_stream(
            label="playlists",
            fetch_page=lambda page: self.client.playlists_page(
                filter_by="all", page=page, page_size=self.args.page_size
            ),
            on_items=receive,
            on_done=finish,
        )

    def _start_playlist_work_stream(self, playlist: dict[str, Any]) -> None:
        if self.discovery_stopped:
            return
        playlist_id = playlist.get("id")
        if playlist_id is None:
            self._collection_error(PayloadError("playlist is missing its id"))
            return
        index = self._playlist_count
        self._playlist_count += 1
        self._playlist_buffers[index] = deque()

        def receive(works: list[dict[str, Any]]) -> None:
            self._playlist_buffers[index].extend(works)
            self._release_playlist_works()

        def finish() -> None:
            self._playlist_done.add(index)
            self._release_playlist_works()
            self._maybe_finish_playlist_discovery()

        self._new_stream(
            label=f"playlist {playlist_id}",
            fetch_page=lambda page: self.client.playlist_works_page(
                str(playlist_id), page=page, page_size=self.args.page_size
            ),
            on_items=receive,
            on_done=finish,
        )

    def _release_playlist_works(self) -> None:
        while self._playlist_release_index < self._playlist_count:
            index = self._playlist_release_index
            buffered = self._playlist_buffers[index]
            while buffered and not self.discovery_stopped:
                self._admit_work(buffered.popleft())
            if self.discovery_stopped or index not in self._playlist_done:
                return
            self._playlist_release_index += 1

    def _maybe_finish_playlist_discovery(self) -> None:
        if (
            self._playlist_listing_done
            and len(self._playlist_done) == self._playlist_count
        ):
            self._finish_discovery()

    def _finish_discovery(self) -> None:
        self.discovery_done = True

    def _stop_discovery(self) -> None:
        self.discovery_stopped = True
        self.discovery_done = True
        self.scheduler.discard_ready(self.DISCOVERY_OWNER)

    def _collection_error(self, exc: BaseException) -> None:
        if self.discovery_stopped:
            return
        self.collection_failures += 1
        log(f"FAIL collection: {exc}")
        self._stop_discovery()

    def _admit_work(self, work: dict[str, Any], *, apply_limit: bool = True) -> None:
        if self.discovery_stopped and apply_limit:
            return
        limit = getattr(self.args, "limit", None)
        if apply_limit and limit and limit > 0 and self.admitted_works >= limit:
            self._stop_discovery()
            return
        identity = work_identity(work)
        if identity in self.seen_works:
            return
        self.seen_works.add(identity)
        self.pending_works.append(work)
        self.admitted_works += 1
        self._fill_active_works()
        if apply_limit and limit and limit > 0 and self.admitted_works >= limit:
            self._stop_discovery()

    def _fill_active_works(self) -> None:
        def runnable_count() -> int:
            return sum(
                self.scheduler.owner_has_runnable(owner) for owner in self.active_works
            )

        while (
            self.pending_works
            and len(self.active_works) < self.max_active_works
            and runnable_count() < self.args.jobs
        ):
            self._work_serial += 1
            owner = f"work:{self._work_serial}"
            state = WorkState(owner, self.pending_works.popleft())
            self.active_works[owner] = state
            self._start_work(state)

    def _enqueue_work_task(
        self,
        state: WorkState,
        *,
        label: str,
        run: Callable[[], Any],
        on_success: Callable[[Any], None],
        on_error: Callable[[BaseException], None] | None = None,
        retry_policy: RetryPolicy | None = None,
        front: bool = False,
    ) -> None:
        self.scheduler.enqueue(
            ScheduledTask(
                owner=state.owner,
                label=label,
                run=run,
                on_success=on_success,
                on_error=on_error or (lambda exc: self._fail_work(state, exc)),
                retry_policy=retry_policy,
                on_retry=lambda exc, attempt, total, delay: self._work_retry(
                    state, label, exc, attempt, total, delay
                ),
            ),
            front=front,
        )

    def _work_retry(
        self,
        state: WorkState,
        label: str,
        exc: BaseException,
        attempt: int,
        total: int,
        delay: float,
    ) -> None:
        if state.completed:
            return
        log(
            f"RETRY [{state.shown_id}] {label} attempt={attempt}/{total} "
            f"after={format_delay(delay)}  {exc}"
        )
        self._fill_active_works()

    def _start_work(self, state: WorkState) -> None:
        if state.work.get("id") is None:
            lookup = state.work.get("source_id")
            self._enqueue_work_task(
                state,
                label="work-info",
                run=lambda lookup=lookup: self.client.work_info(lookup),
                on_success=lambda work: self._work_info_ready(state, work),
                retry_policy=NETWORK_RETRY_POLICY,
            )
        elif self._claim_resolved_work(state):
            self._schedule_work_lock(state)

    def _work_info_ready(self, state: WorkState, work: dict[str, Any]) -> None:
        if state.completed:
            return
        state.work = work
        if self._claim_resolved_work(state):
            self._schedule_work_lock(state)

    def _claim_resolved_work(self, state: WorkState) -> bool:
        identity = work_identity(state.work)
        existing_owner = self.resolved_work_owners.get(identity)
        if existing_owner is None:
            self.resolved_work_owners[identity] = state.owner
            return True
        if existing_owner == state.owner:
            return True

        state.completed = True
        self.admitted_works -= 1
        if self.admitted_works < 0:
            raise RuntimeError("negative admitted work count")
        log(f"SKIP duplicate work {state.shown_id}")
        self.active_works.pop(state.owner, None)
        self._fill_active_works()
        return False

    def _schedule_work_lock(self, state: WorkState) -> None:
        if self.args.dry_run:
            self._schedule_tracks(state)
            return
        root = Path(self.args.out) / work_folder_name(state.work)
        self._enqueue_work_task(
            state,
            label="work-lock",
            run=lambda: WorkLock.acquire(root),
            on_success=lambda lock: self._work_lock_ready(state, root, lock),
            retry_policy=LOCK_RETRY_POLICY,
            front=True,
        )

    def _work_lock_ready(
        self,
        state: WorkState,
        root: Path,
        work_lock: WorkLock,
    ) -> None:
        if state.completed:
            work_lock.close()
            return
        state.root = root
        state.work_lock = work_lock
        self._schedule_tracks(state)

    def _schedule_tracks(self, state: WorkState) -> None:
        work_id = state.work.get("id") or state.work.get("source_id")
        if work_id is None:
            self._fail_work(state, PayloadError("work is missing its id"))
            return
        self._enqueue_work_task(
            state,
            label="tracks",
            run=lambda work_id=work_id: self.client.tracks(work_id),
            on_success=lambda tracks: self._schedule_prepare(state, tracks),
            retry_policy=NETWORK_RETRY_POLICY,
            front=True,
        )

    def _schedule_prepare(self, state: WorkState, tracks: list[dict[str, Any]]) -> None:
        work = state.work
        self._enqueue_work_task(
            state,
            label="prepare-work",
            run=lambda: prepare_download_work(
                work,
                tracks,
                out=self.args.out,
                audio_format=self.args.format,
                include_subs=not self.args.no_subs,
                all_langs=not self.args.ja_only,
            ),
            on_success=lambda prepared: self._work_prepared(state, prepared),
            front=True,
        )

    def _work_prepared(self, state: WorkState, prepared: PreparedWork) -> None:
        if state.completed:
            return
        state.root = prepared.root
        state.manifest = prepared.manifest
        state.remaining_files = len(prepared.files)
        state.prepared = True
        prefix = "DRY ==" if self.args.dry_run else "=="
        log(f"{prefix} {state.shown_id}  {state.work.get('title')}  -> {prepared.root}")
        if prepared.manifest_keys_changed and not self.args.dry_run:
            self._mark_manifest_dirty(state)
        for context in prepared.files:
            self._enqueue_work_task(
                state,
                label=f"inspect {context.relative_path}",
                run=lambda context=context: inspect_local_file(
                    context, verify=getattr(self.args, "verify", False)
                ),
                on_success=lambda inspection, context=context: self._inspection_ready(
                    state, context, inspection
                ),
                on_error=lambda exc, context=context: self._file_error(
                    state, context, exc
                ),
            )
        self._maybe_complete_work(state)

    def _inspection_ready(
        self,
        state: WorkState,
        context: LocalFileContext,
        inspection: FileInspection,
    ) -> None:
        if inspection.plan is not None:
            self._handle_plan(state, context, inspection.plan)
            return
        self._enqueue_work_task(
            state,
            label=f"hash {context.relative_path}",
            run=lambda: hash_local_file(context),
            on_success=lambda hashed: self._hash_ready(
                state, context, inspection, hashed
            ),
            on_error=lambda exc: self._file_error(state, context, exc),
            front=True,
        )

    def _hash_ready(
        self,
        state: WorkState,
        context: LocalFileContext,
        inspection: FileInspection,
        hashed: HashedFile,
    ) -> None:
        plan, update = resolve_hashed_file(context, inspection, hashed)
        if update is not None and not self.args.dry_run:
            self._update_manifest_record(state, context.record_key, update)
        self._handle_plan(state, context, plan)

    def _handle_plan(
        self,
        state: WorkState,
        context: LocalFileContext,
        plan: LocalFilePlan,
    ) -> None:
        state.status_counts[plan.status] += 1
        if self.args.dry_run:
            size = expected_file_size(plan.file)
            log(
                f"  [{state.shown_id}] {plan.status:7} {plan.relative_path}  "
                f"{size if size is not None else '?'}"
            )
            if plan.status in {"valid", "adopt"}:
                state.skip += 1
            self._finish_file(state)
            return
        if plan.status == "valid":
            state.skip += 1
            self._finish_file(state)
            return
        if plan.status == "adopt":
            state.skip += 1
            log(f"  [{state.shown_id}] adopt  {plan.relative_path}")
            self._finish_file(state)
            return
        if not plan.needs_download:
            self._file_error(
                state, context, RuntimeError(f"unknown local status {plan.status}")
            )
            return
        self._schedule_download(state, context, plan)

    def _schedule_download(
        self,
        state: WorkState,
        context: LocalFileContext,
        plan: LocalFilePlan,
    ) -> None:
        if state.root is None:
            raise RuntimeError("download scheduled before work preparation")
        operation = DownloadOperation(
            self.client,
            state.work,
            self.args,
            state.root,
            context,
            plan,
        )
        self._enqueue_work_task(
            state,
            label=f"download {plan.relative_path}",
            run=operation,
            on_success=lambda outcome: self._download_ready(state, outcome),
            on_error=lambda exc: self._file_error(state, operation.context, exc),
            retry_policy=NETWORK_RETRY_POLICY,
            front=True,
        )

    def _download_ready(
        self,
        state: WorkState,
        outcome: DownloadOutcome,
    ) -> None:
        self._update_manifest_record(
            state,
            outcome.context.record_key,
            outcome.record,
        )
        state.ok += 1
        log(
            f"  [{state.shown_id}] {outcome.result.status:6} "
            f"{outcome.plan.relative_path}"
        )
        self._finish_file(state)

    def _file_error(
        self,
        state: WorkState,
        context: LocalFileContext,
        exc: BaseException,
    ) -> None:
        state.fail += 1
        log(f"  [{state.shown_id}] FAIL   {context.relative_path}  {exc}")
        self._finish_file(state)

    def _finish_file(self, state: WorkState) -> None:
        state.remaining_files -= 1
        if state.remaining_files < 0:
            raise RuntimeError(f"negative remaining file count for {state.shown_id}")
        self._maybe_complete_work(state)
        self._fill_active_works()

    def _update_manifest_record(
        self, state: WorkState, key: str, record: dict[str, Any]
    ) -> None:
        if state.manifest is None or state.manifest_failed:
            return
        state.manifest["files"][key] = record
        self._mark_manifest_dirty(state)

    def _mark_manifest_dirty(self, state: WorkState) -> None:
        if self.args.dry_run or state.manifest_failed:
            return
        state.manifest_generation += 1
        self._ensure_manifest_save(state)

    def _ensure_manifest_save(self, state: WorkState) -> None:
        if (
            state.save_inflight
            or state.manifest_failed
            or state.persisted_generation >= state.manifest_generation
        ):
            return
        if state.root is None or state.manifest is None:
            raise RuntimeError("manifest save requested before work preparation")
        generation = state.manifest_generation
        root = state.root
        snapshot = copy.deepcopy(state.manifest)
        state.save_inflight = True
        self._enqueue_work_task(
            state,
            label=f"save-manifest generation={generation}",
            run=lambda: save_checksum_manifest(root, snapshot),
            on_success=lambda _: self._manifest_saved(state, generation),
            on_error=lambda exc: self._manifest_error(state, exc),
            front=True,
        )

    def _manifest_saved(self, state: WorkState, generation: int) -> None:
        state.save_inflight = False
        state.persisted_generation = max(state.persisted_generation, generation)
        self._ensure_manifest_save(state)
        self._maybe_complete_work(state)
        self._fill_active_works()

    def _manifest_error(self, state: WorkState, exc: BaseException) -> None:
        state.save_inflight = False
        if not state.manifest_failed:
            state.manifest_failed = True
            state.fail += 1
            log(f"FAIL work {state.shown_id} manifest: {exc}")
        self._maybe_complete_work(state)

    def _maybe_complete_work(self, state: WorkState) -> None:
        if state.completed or not state.prepared or state.remaining_files != 0:
            return
        if state.save_inflight:
            return
        if (
            not self.args.dry_run
            and not state.manifest_failed
            and state.persisted_generation < state.manifest_generation
        ):
            self._ensure_manifest_save(state)
            return
        self._complete_work(state)

    def _complete_work(self, state: WorkState) -> None:
        state.completed = True
        summary = (
            " ".join(
                f"{status}={count}"
                for status, count in state.status_counts.items()
                if count
            )
            or "files=0"
        )
        log(
            f"== {state.shown_id} done ({summary}) "
            f"ok={state.ok} skipped={state.skip} failed={state.fail}"
        )
        self.outcomes.append(
            WorkOutcome(state.shown_id, state.ok, state.skip, state.fail)
        )
        self._release_work_lock(state)
        self.active_works.pop(state.owner, None)
        self._fill_active_works()

    def _fail_work(self, state: WorkState, exc: BaseException) -> None:
        if state.completed:
            return
        state.completed = True
        self.scheduler.discard_ready(state.owner)
        state.fail += 1
        log(f"FAIL work {state.shown_id}: {exc}")
        self.outcomes.append(
            WorkOutcome(state.shown_id, state.ok, state.skip, state.fail)
        )
        self._release_work_lock(state)
        self.active_works.pop(state.owner, None)
        self._fill_active_works()

    @staticmethod
    def _release_work_lock(state: WorkState) -> None:
        if state.work_lock is None:
            return
        state.work_lock.close()
        state.work_lock = None

    def _release_all_locks(self) -> None:
        for state in tuple(self.active_works.values()):
            self._release_work_lock(state)


def download_work(
    client: Client, work: dict[str, Any], args: argparse.Namespace
) -> tuple[str, int, int, int]:
    coordinator = DownloadCoordinator(client, args)
    coordinator.run_direct([work])
    outcome = coordinator.outcomes[0]
    return outcome.shown_id, outcome.ok, outcome.skip, outcome.fail


def cmd_download(args: argparse.Namespace) -> None:
    client = require_client(args, need_login=not args.works)
    t0 = time.time()
    summary = DownloadCoordinator(client, args).run_collection()
    elapsed = time.time() - t0
    log(
        f"done works={summary.works} files_ok={summary.ok} skipped={summary.skip} "
        f"failed={summary.fail} {elapsed:.1f}s"
    )
    if summary.fail:
        raise SystemExit(2)


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="asmr-one",
        description=(
            f"{APP_DISPLAY_NAME}: download your asmr.one favorites via the official "
            "per-work download API."
        ),
    )
    p.add_argument("--timeout", type=int, default=30)
    sub = p.add_subparsers(dest="cmd", required=True)

    login = sub.add_parser(
        "login", help="log in and store a token in ~/.config/asmr-one/"
    )
    login.add_argument("--name")
    login.add_argument(
        "--password", help="prefer env ASMR_PASSWORD; do not pass on shared shells"
    )
    login.set_defaults(func=cmd_login)

    who = sub.add_parser("whoami", help="show the saved session")
    who.set_defaults(func=cmd_whoami)

    playlists = sub.add_parser("playlists", help="list available asmr.one playlists")
    playlists.add_argument("--page-size", type=positive_int, default=50)
    playlists.set_defaults(func=cmd_playlists)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--source",
        choices=("auto", "playlists", "favorites", "review"),
        default="auto",
        help="default auto = playlists; favorites is a compatibility alias",
    )
    common.add_argument("--page-size", type=positive_int, default=50)
    common.add_argument("--limit", type=positive_int)
    collection = common.add_mutually_exclusive_group()
    collection.add_argument(
        "--work",
        dest="works",
        action="append",
        help="RJ/VJ/id; repeatable; skips collection",
    )
    collection.add_argument(
        "--playlist",
        dest="playlist_selectors",
        action="append",
        help="playlist name/id, liked, or marked; repeatable",
    )

    lst = sub.add_parser(
        "list", parents=[common], help="print playlist works or review list"
    )
    lst.set_defaults(func=cmd_list)

    dl = sub.add_parser("download", parents=[common], help="download the list")
    dl.add_argument("--out", default=str(Path.home() / "Music" / "asmr.one"))
    dl.add_argument(
        "--jobs",
        type=positive_int,
        default=4,
        help="global workers shared by discovery, hashing, and downloads",
    )
    dl.add_argument(
        "--format",
        choices=("all", "mp3", "wav", "flac", "best"),
        default="all",
        help="default all = every file (wav/mp3/video/subs). use mp3/wav to filter",
    )
    dl.add_argument(
        "--ja-only",
        action="store_true",
        help="keep only the Japanese language folder when editions exist",
    )
    dl.add_argument(
        "--no-subs", action="store_true", help="drop lrc/vtt/srt when filtering audio"
    )
    dl.add_argument("--dry-run", action="store_true")
    dl.add_argument(
        "--verify",
        action="store_true",
        help="recompute BLAKE3 for every existing selected file before downloading",
    )
    dl.set_defaults(func=cmd_download)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
