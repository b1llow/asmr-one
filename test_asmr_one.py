#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from collections import deque
from contextlib import redirect_stderr
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import asmr_one
from asmr_one import (
    ApiError,
    cmd_login,
    flatten_tracks,
    require_client,
    select_files,
    work_folder_name,
)


TRACKS = Path("/tmp/asmr-tracks.json")


class FakeResponse(io.BytesIO):
    def __init__(self, data: bytes, headers: dict[str, str] | None = None):
        super().__init__(data)
        self.headers = headers or {}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def download_args(out: str, **overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "out": out,
        "jobs": 2,
        "format": "all",
        "no_subs": False,
        "ja_only": False,
        "dry_run": False,
        "verify": False,
        "timeout": 30,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def coordinator_args(out: str, **overrides: object) -> argparse.Namespace:
    args = download_args(out)
    values = vars(args)
    values.update(
        {
            "works": None,
            "source": "auto",
            "page_size": 2,
            "limit": None,
            "playlist_selectors": None,
        }
    )
    values.update(overrides)
    return argparse.Namespace(**values)


def remote_file(
    title: str = "track.wav",
    *,
    size: int | None = 5,
    url: str = "https://cdn.example/work/track.wav?token=one",
    remote_id: str | None = "work/file",
    path: tuple[str, ...] | None = None,
) -> dict[str, object]:
    return {
        "title": title,
        "path": path or (title,),
        "url": url,
        "size": size,
        "hash": remote_id,
        "type": "audio",
        "ext": Path(title).suffix.lower(),
    }


def download_remote() -> dict[str, object]:
    return asmr_one.remote_fingerprint(remote_file())


def checkpoint_part(
    dest: Path,
    data: bytes,
    *,
    remote: dict[str, object] | None = None,
    etag: str | None = None,
) -> Path:
    part = asmr_one.part_file_path(dest)
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(data)
    asmr_one.save_partial_state(
        dest,
        asmr_one.PartialState(
            remote or download_remote(),
            len(data),
            asmr_one.checksum_file(part),
            etag,
        ),
    )
    return part


class BrandingTests(unittest.TestCase):
    def test_canonical_application_names(self) -> None:
        self.assertEqual(asmr_one.APP_NAME, "asmr-one")
        self.assertEqual(asmr_one.APP_DISPLAY_NAME, "ASMR One")
        self.assertEqual(asmr_one.CONFIG_HOME_ENV, "ASMR_ONE_HOME")
        self.assertEqual(asmr_one.WORK_LOCK_FILE_NAME, ".asmr-one.lock")

        parser = asmr_one.build_parser()
        self.assertEqual(parser.prog, "asmr-one")
        self.assertIn("ASMR One", parser.description or "")

    def test_new_config_home_environment_variable_is_used(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            configured = Path(tmp) / "configured"
            environment = os.environ.copy()
            environment[asmr_one.CONFIG_HOME_ENV] = str(configured)

            self.assertEqual(self._imported_config_dir(environment), configured)

    def test_legacy_config_home_environment_variable_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = os.environ.copy()
            environment.pop(asmr_one.CONFIG_HOME_ENV, None)
            environment["_".join(("ASMR", "FAV", "HOME"))] = str(
                Path(tmp) / "legacy"
            )
            environment["HOME"] = tmp

            self.assertEqual(
                self._imported_config_dir(environment),
                Path(tmp) / ".config" / "asmr-one",
            )

    @staticmethod
    def _imported_config_dir(environment: dict[str, str]) -> Path:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from asmr_one import CONFIG_DIR; print(CONFIG_DIR)",
            ],
            cwd=Path(__file__).parent,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        return Path(result.stdout.strip())


class MainTests(unittest.TestCase):
    def test_expected_operational_errors_are_reported_without_tracebacks(self) -> None:
        errors = (
            ApiError(401, "expired"),
            asmr_one.LocalStateError("unsafe local state"),
            asmr_one.PayloadError("bad payload"),
            asmr_one.RequestTransportError("network unavailable"),
            OSError("disk unavailable"),
        )
        for error in errors:
            handler = MagicMock(side_effect=error)
            parser = MagicMock()
            parser.parse_args.return_value = argparse.Namespace(func=handler)
            stderr = io.StringIO()
            with (
                self.subTest(error=error),
                patch("asmr_one.build_parser", return_value=parser),
                redirect_stderr(stderr),
                self.assertRaises(SystemExit) as raised,
            ):
                asmr_one.main([])

            self.assertEqual(raised.exception.code, 2)
            self.assertIn(f"error: {error}", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())

    def test_remote_error_text_cannot_forge_stderr_lines(self) -> None:
        error = ApiError(400, "first\nFORGED\x1b[31m\ud800")
        handler = MagicMock(side_effect=error)
        parser = MagicMock()
        parser.parse_args.return_value = argparse.Namespace(func=handler)
        stderr = io.StringIO()

        with (
            patch("asmr_one.build_parser", return_value=parser),
            redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            asmr_one.main([])

        output = stderr.getvalue()
        self.assertEqual(len(output.splitlines()), 1)
        self.assertNotIn("\x1b", output)
        self.assertNotRegex(output, r"[\ud800-\udfff]")
        self.assertIn("first FORGED [31m\ufffd", output)


class SelectTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not TRACKS.is_file():
            raise unittest.SkipTest("no captured tracks fixture")
        cls.files = flatten_tracks(json.loads(TRACKS.read_text()))

    def test_flatten_has_wav_and_mp3(self) -> None:
        exts = {f["ext"] for f in self.files}
        self.assertIn(".wav", exts)
        self.assertIn(".mp3", exts)
        self.assertGreater(len(self.files), 10)

    def test_default_keeps_everything(self) -> None:
        chosen = select_files(
            self.files, audio_format="all", include_subs=True, all_langs=True
        )
        self.assertEqual(len(chosen), len(self.files))

    def test_ja_mp3_filters(self) -> None:
        chosen = select_files(
            self.files, audio_format="mp3", include_subs=False, all_langs=False
        )
        self.assertTrue(chosen)
        self.assertTrue(all(f["ext"] == ".mp3" for f in chosen))
        self.assertTrue(any("日本語" in "".join(f["path"]) for f in chosen))
        self.assertFalse(any("英語" in "".join(f["path"]) for f in chosen))

    def test_folder_name(self) -> None:
        name = work_folder_name({"source_id": "RJ01657200", "title": "a/b:c"})
        self.assertTrue(name.startswith("RJ01657200 "))
        self.assertNotIn("/", name)
        self.assertNotIn(":", name)


class SelectBehaviorTests(unittest.TestCase):
    def test_all_without_subtitles_keeps_other_file_types(self) -> None:
        audio = remote_file("voice.wav")
        subtitle = remote_file("voice.srt")
        subtitle.update(type="file", ext=".srt")
        image = remote_file("cover.jpg")
        image.update(type="file", ext=".jpg")

        chosen = select_files(
            [audio, subtitle, image],
            audio_format="all",
            include_subs=False,
            all_langs=True,
        )

        self.assertEqual(
            [file["title"] for file in chosen], ["voice.wav", "cover.jpg"]
        )

    def test_explicit_format_does_not_fall_back(self) -> None:
        chosen = select_files(
            [remote_file("voice.wav"), remote_file("voice.flac")],
            audio_format="mp3",
            include_subs=False,
            all_langs=True,
        )

        self.assertEqual(chosen, [])

    def test_best_format_is_selected_per_logical_track(self) -> None:
        intro_wav = remote_file("intro.wav")
        intro_mp3 = remote_file("intro.mp3")
        chapter_mp3 = remote_file("chapter.mp3")

        chosen = select_files(
            [intro_mp3, chapter_mp3, intro_wav],
            audio_format="best",
            include_subs=False,
            all_langs=True,
        )

        self.assertEqual(chosen, [chapter_mp3, intro_wav])

    def test_ja_only_with_all_keeps_japanese_non_audio_files(self) -> None:
        japanese_audio = remote_file(
            "voice.wav", path=("日本語", "voice.wav")
        )
        japanese_video = remote_file(
            "scene.mp4", path=("日本語", "scene.mp4")
        )
        japanese_video.update(type="video", ext=".mp4")
        english_audio = remote_file(
            "voice.wav", path=("English", "voice.wav")
        )

        chosen = select_files(
            [japanese_audio, japanese_video, english_audio],
            audio_format="all",
            include_subs=True,
            all_langs=False,
        )

        self.assertEqual(
            [file["path"] for file in chosen],
            [("日本語", "voice.wav"), ("日本語", "scene.mp4")],
        )

    def test_ja_only_does_not_fall_back_to_other_languages(self) -> None:
        chosen = select_files(
            [
                remote_file("voice.wav", path=("English", "voice.wav")),
                remote_file("root.wav"),
            ],
            audio_format="all",
            include_subs=True,
            all_langs=False,
        )

        self.assertEqual(chosen, [])

    def test_subtitles_match_audio_within_the_same_directory(self) -> None:
        english_audio = remote_file(
            "voice.wav", path=("English", "voice.wav")
        )
        english_subtitle = remote_file(
            "voice.srt", path=("English", "voice.srt")
        )
        english_subtitle.update(type="file", ext=".srt")
        japanese_audio = remote_file(
            "voice.mp3", path=("日本語", "voice.mp3")
        )
        japanese_subtitle = remote_file(
            "voice.srt", path=("日本語", "voice.srt")
        )
        japanese_subtitle.update(type="file", ext=".srt")

        chosen = select_files(
            [
                english_audio,
                english_subtitle,
                japanese_audio,
                japanese_subtitle,
            ],
            audio_format="wav",
            include_subs=True,
            all_langs=True,
        )

        self.assertEqual(
            [file["path"] for file in chosen],
            [("English", "voice.wav"), ("English", "voice.srt")],
        )

    def test_subtitles_match_complete_stems_and_embedded_audio_suffixes(self) -> None:
        english_audio = remote_file("voice.en.wav")
        japanese_audio = remote_file("voice.ja.mp3")
        japanese_subtitle = remote_file("voice.ja.srt")
        japanese_subtitle.update(type="file", ext=".srt")
        embedded_subtitle = remote_file("voice.en.wav.srt")
        embedded_subtitle.update(type="file", ext=".srt")
        other_format_subtitle = remote_file("voice.en.flac.srt")
        other_format_subtitle.update(type="file", ext=".srt")

        chosen = select_files(
            [
                english_audio,
                japanese_audio,
                japanese_subtitle,
                embedded_subtitle,
                other_format_subtitle,
            ],
            audio_format="wav",
            include_subs=True,
            all_langs=True,
        )

        self.assertEqual(chosen, [english_audio, embedded_subtitle])

    def test_conflicting_remote_files_are_not_silently_deduplicated(self) -> None:
        first = remote_file("voice.wav", remote_id="work/first")
        conflicting = remote_file(
            "voice.wav",
            url="https://cdn.example/other.wav",
            remote_id="work/second",
        )

        chosen = select_files(
            [first, dict(first), conflicting],
            audio_format="all",
            include_subs=True,
            all_langs=True,
        )

        self.assertEqual(chosen, [first, conflicting])
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(asmr_one.LocalStateError, "collide"):
                asmr_one.prepare_local_files(
                    Path(tmp),
                    chosen,
                    asmr_one.empty_checksum_manifest(),
                )


class PathSafetyTests(unittest.TestCase):
    def test_work_folder_sanitizes_source_and_limits_utf8_bytes(self) -> None:
        name = work_folder_name(
            {"source_id": "../../escape", "title": "月" * 200}
        )

        self.assertNotIn("/", name)
        self.assertNotIn("\\", name)
        self.assertLessEqual(
            len(name.encode("utf-8")), asmr_one.MAX_WORK_FOLDER_BYTES
        )

    def test_track_components_leave_room_for_partial_state_suffix(self) -> None:
        relative = asmr_one.relative_file_path(
            remote_file("月" * 200 + ".wav")
        )

        self.assertTrue(
            all(
                len(part.encode("utf-8"))
                <= asmr_one.MAX_LOCAL_COMPONENT_BYTES
                for part in relative.parts
            )
        )
        self.assertLessEqual(
            len(asmr_one.part_state_path(relative).name.encode("utf-8")), 255
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / relative.name
            state = asmr_one.PartialState(
                asmr_one.remote_fingerprint(remote_file()),
                0,
                asmr_one.new_blake3_hasher().hexdigest(),
            )

            asmr_one.save_partial_state(dest, state)

            self.assertTrue(asmr_one.part_state_path(dest).is_file())

    def test_truncated_track_names_preserve_their_extensions(self) -> None:
        wav = asmr_one.relative_file_path(remote_file("月" * 81 + ".wav"))
        mp3 = asmr_one.relative_file_path(remote_file("月" * 81 + ".mp3"))

        self.assertEqual(wav.suffix, ".wav")
        self.assertEqual(mp3.suffix, ".mp3")
        self.assertNotEqual(wav, mp3)
        self.assertLessEqual(
            len(wav.name.encode("utf-8")), asmr_one.MAX_LOCAL_COMPONENT_BYTES
        )

    def test_local_names_replace_control_characters(self) -> None:
        relative = asmr_one.relative_file_path(
            remote_file("voice\nFAIL\x1b\x85\ud800.wav")
        )
        folder = work_folder_name(
            {"source_id": "RJ1\r", "title": "title\tspoof\x7f\udfff"}
        )

        for value in (*relative.parts, folder):
            self.assertFalse(
                any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value)
            )
            self.assertNotRegex(value, r"[\ud800-\udfff]")


class AuthTests(unittest.TestCase):
    def test_environment_token_precedes_saved_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "token.json"
            token_path.write_text('{"token": "saved-token"}', encoding="utf-8")
            with (
                patch.object(asmr_one, "TOKEN_PATH", token_path),
                patch.dict(os.environ, {"ASMR_TOKEN": " env-token "}, clear=True),
            ):
                self.assertEqual(asmr_one.load_token(), "env-token")

    def test_saved_token_is_loaded_without_environment_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "token.json"
            token_path.write_text('{"token": "saved-token"}', encoding="utf-8")
            with (
                patch.object(asmr_one, "TOKEN_PATH", token_path),
                patch.dict(os.environ, {}, clear=True),
            ):
                self.assertEqual(asmr_one.load_token(), "saved-token")

    def test_whitespace_environment_token_falls_back_to_saved_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "token.json"
            token_path.write_text('{"token": "saved-token"}', encoding="utf-8")
            with (
                patch.object(asmr_one, "TOKEN_PATH", token_path),
                patch.dict(os.environ, {"ASMR_TOKEN": "   "}, clear=True),
            ):
                self.assertEqual(asmr_one.load_token(), "saved-token")

    def test_saved_token_is_atomically_installed_with_mode_0600(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config"
            token_path = config / "token.json"
            installed_modes: list[int] = []
            real_replace = os.replace

            def replace(source: str | Path, destination: str | Path) -> None:
                installed_modes.append(Path(source).stat().st_mode & 0o777)
                real_replace(source, destination)

            with (
                patch.object(asmr_one, "CONFIG_DIR", config),
                patch.object(asmr_one, "TOKEN_PATH", token_path),
                patch("asmr_one.os.replace", side_effect=replace),
            ):
                asmr_one.save_token("secret", name="alice")

            self.assertEqual(installed_modes, [0o600])
            self.assertEqual(token_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                json.loads(token_path.read_text(encoding="utf-8")),
                {"name": "alice", "token": "secret"},
            )

    def test_non_object_saved_token_is_ignored(self) -> None:
        for payload in ("null", "[]", '"token"'):
            with (
                self.subTest(payload=payload),
                tempfile.TemporaryDirectory() as tmp,
            ):
                token_path = Path(tmp) / "token.json"
                token_path.write_text(payload, encoding="utf-8")
                with (
                    patch.object(asmr_one, "TOKEN_PATH", token_path),
                    patch.dict(os.environ, {}, clear=True),
                ):
                    self.assertIsNone(asmr_one.load_token())

    def test_invalid_utf8_saved_token_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "token.json"
            token_path.write_bytes(b"\xff")
            with (
                patch.object(asmr_one, "TOKEN_PATH", token_path),
                patch.dict(os.environ, {"ASMR_TOKEN": "   "}, clear=True),
            ):
                self.assertIsNone(asmr_one.load_token())

    def test_tokens_unsafe_for_http_headers_are_rejected(self) -> None:
        invalid_tokens = ("bad\nheader", "snowman-\u2603", "two words")
        for token in invalid_tokens:
            with self.subTest(token=token), tempfile.TemporaryDirectory() as tmp:
                token_path = Path(tmp) / "token.json"
                token_path.write_text(
                    json.dumps({"token": token}),
                    encoding="utf-8",
                )
                with (
                    patch.object(asmr_one, "TOKEN_PATH", token_path),
                    patch.dict(os.environ, {}, clear=True),
                ):
                    self.assertIsNone(asmr_one.load_token())
                with self.assertRaises(ValueError):
                    asmr_one.save_token(token)

        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "token.json"
            token_path.write_text('{"token": "saved-token"}', encoding="utf-8")
            with (
                patch.object(asmr_one, "TOKEN_PATH", token_path),
                patch.dict(
                    os.environ,
                    {"ASMR_TOKEN": "bad\nheader"},
                    clear=True,
                ),
            ):
                self.assertEqual(asmr_one.load_token(), "saved-token")

    def test_require_client_auto_logs_in_from_environment(self) -> None:
        args = argparse.Namespace(timeout=12)
        with (
            patch.dict(
                os.environ,
                {"ASMR_NAME": "alice1", "ASMR_PASSWORD": "secret1"},
                clear=True,
            ),
            patch("asmr_one.load_token", return_value=None),
            patch("asmr_one.Client") as client_class,
            patch("asmr_one.save_token") as save_token,
        ):
            client = client_class.return_value
            client.login.return_value = ("fresh-token", {"name": "alice1"})

            result = require_client(args)

            client_class.assert_called_once_with(timeout=12)
            client.login.assert_called_once_with("alice1", "secret1")
            self.assertEqual(client.token, "fresh-token")
            self.assertIs(result, client)
            save_token.assert_not_called()

    def test_require_client_prefers_existing_token(self) -> None:
        args = argparse.Namespace(timeout=8)
        with (
            patch.dict(
                os.environ,
                {"ASMR_NAME": "alice1", "ASMR_PASSWORD": "secret1"},
                clear=True,
            ),
            patch("asmr_one.load_token", return_value="existing-token"),
            patch("asmr_one.Client") as client_class,
        ):
            result = require_client(args)

            client_class.assert_called_once_with(token="existing-token", timeout=8)
            client_class.return_value.login.assert_not_called()
            self.assertIs(result, client_class.return_value)

    def test_require_client_rejects_partial_environment_credentials(self) -> None:
        args = argparse.Namespace(timeout=30)
        environments = (
            {"ASMR_NAME": "alice1"},
            {"ASMR_PASSWORD": "secret1"},
        )
        for environment in environments:
            with self.subTest(environment=environment):
                stderr = io.StringIO()
                with (
                    patch.dict(os.environ, environment, clear=True),
                    patch("asmr_one.load_token", return_value=None),
                    redirect_stderr(stderr),
                ):
                    with self.assertRaises(SystemExit):
                        require_client(args)
                self.assertIn(
                    "automatic login requires both ASMR_NAME and ASMR_PASSWORD",
                    stderr.getvalue(),
                )

    def test_require_client_does_not_login_when_auth_is_optional(self) -> None:
        args = argparse.Namespace(timeout=7)
        with (
            patch.dict(
                os.environ,
                {"ASMR_NAME": "alice1", "ASMR_PASSWORD": "secret1"},
                clear=True,
            ),
            patch("asmr_one.load_token", return_value=None),
            patch("asmr_one.Client") as client_class,
        ):
            result = require_client(args, need_login=False)

            client_class.assert_called_once_with(token=None, timeout=7)
            client_class.return_value.login.assert_not_called()
            self.assertIs(result, client_class.return_value)

    def test_explicit_login_still_saves_token(self) -> None:
        args = argparse.Namespace(name="alice1", password="secret1", timeout=15)
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("asmr_one.Client") as client_class,
            patch("asmr_one.save_token") as save_token,
            patch("asmr_one.log"),
        ):
            client_class.return_value.login.return_value = (
                "fresh-token",
                {"name": "alice1"},
            )

            cmd_login(args)

            client_class.assert_called_once_with(timeout=15)
            client_class.return_value.login.assert_called_once_with("alice1", "secret1")
            save_token.assert_called_once_with("fresh-token", name="alice1")


class ClientTests(unittest.TestCase):
    def test_login_requires_a_nonempty_string_token(self) -> None:
        client = asmr_one.Client()
        invalid_payloads = (
            {"token": 123},
            {"token": {"value": "abc"}},
            {"token": "   "},
            {"token": "bad\nheader"},
            {"token": "snowman-\u2603"},
            {"token": "two words"},
            {"user": {"token": 123}},
        )
        for payload in invalid_payloads:
            with (
                self.subTest(payload=payload),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError),
            ):
                client.login("alice", "secret")

        with patch.object(
            client,
            "request",
            return_value=(200, {"access_token": " valid ", "name": "alice"}),
        ):
            token, _ = client.login("alice", "secret")

        self.assertEqual(token, "valid")
        self.assertEqual(client.token, "valid")

    def test_client_rejects_an_unsafe_bearer_token_at_every_request(self) -> None:
        for token in ("bad\nheader", "snowman-\u2603", "two words", ""):
            with self.subTest(token=token), self.assertRaises(
                asmr_one.PayloadError
            ):
                asmr_one.Client(token=token)

        client = asmr_one.Client()
        client.token = "bad\nheader"
        with (
            patch.object(client._api_opener, "open") as open_request,
            self.assertRaises(asmr_one.PayloadError),
        ):
            client.request("GET", "/data")
        open_request.assert_not_called()

    def test_whoami_rejects_non_object_success_payloads(self) -> None:
        client = asmr_one.Client()
        for payload in (None, [], b"not-json"):
            with (
                self.subTest(payload=payload),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.whoami()
            self.assertFalse(
                asmr_one.is_retryable_network_error(raised.exception)
            )

    def test_bearer_token_is_limited_to_the_api_request(self) -> None:
        requests: list[asmr_one.urllib.request.Request] = []

        def urlopen(
            request: asmr_one.urllib.request.Request, **_kwargs: object
        ) -> FakeResponse:
            requests.append(request)
            return FakeResponse(b"{}")

        with patch.object(
            asmr_one, "DEFAULT_MIRRORS", ("https://api.example",)
        ):
            client = asmr_one.Client(token="secret")
        with (
            patch.object(
                client,
                "_open_request",
                side_effect=lambda request, **_kwargs: urlopen(request),
            ),
            patch(
                "asmr_one.socket.getaddrinfo",
                return_value=[
                    (
                        asmr_one.socket.AF_INET,
                        asmr_one.socket.SOCK_STREAM,
                        6,
                        "",
                        ("8.8.8.8", 443),
                    )
                ],
            ),
        ):
            _, response = client.request(
                "GET", "", raw_url="https://cdn.example/file", stream=True
            )
            response.close()
            client.request("GET", "/me")

        raw_request, api_request = requests
        self.assertIsNone(raw_request.get_header("Authorization"))
        self.assertEqual(api_request.get_header("Authorization"), "Bearer secret")
        handler = asmr_one.SameOriginAuthRedirectHandler()
        same_origin = handler.redirect_request(
            api_request,
            None,
            302,
            "Found",
            {},
            "https://api.example/redirected",
        )
        self.assertIsNotNone(same_origin)
        self.assertEqual(
            same_origin.get_header("Authorization"),
            "Bearer secret",
        )
        cross_origin = handler.redirect_request(
            api_request,
            None,
            302,
            "Found",
            {},
            "https://cdn.example/redirected",
        )
        self.assertIsNotNone(cross_origin)
        self.assertIsNone(cross_origin.get_header("Authorization"))

    def test_successful_fallback_mirror_is_promoted(self) -> None:
        mirrors = ("https://preferred.example", "https://fallback.example")
        attempts: list[str] = []
        responses: deque[object] = deque(
            [
                asmr_one.urllib.error.URLError("down"),
                FakeResponse(b"{}"),
                FakeResponse(b"{}"),
            ]
        )

        def urlopen(
            request: asmr_one.urllib.request.Request, **_kwargs: object
        ) -> FakeResponse:
            attempts.append(request.full_url)
            response = responses.popleft()
            if isinstance(response, BaseException):
                raise response
            return response

        with patch.object(asmr_one, "DEFAULT_MIRRORS", mirrors):
            client = asmr_one.Client()
            with patch.object(client._api_opener, "open", side_effect=urlopen):
                client.request("GET", "/first")
                self.assertEqual(client.mirror, mirrors[1])
                client.request("GET", "/second")

        self.assertEqual(
            attempts,
            [
                f"{mirrors[0]}/first",
                f"{mirrors[1]}/first",
                f"{mirrors[1]}/second",
            ],
        )

    def test_tracks_rejects_malformed_success_payloads(self) -> None:
        client = asmr_one.Client()
        for payload in (
            None,
            "bad",
            {},
            {"error": "bad"},
            {"tracks": None},
            [None],
            {"tracks": [None]},
        ):
            with (
                self.subTest(payload=payload),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.tracks(1)
            self.assertFalse(asmr_one.is_retryable_network_error(raised.exception))

        with patch.object(client, "request", return_value=(200, {"tracks": []})):
            self.assertEqual(client.tracks(1), [])

    def test_tracks_rejects_leaf_without_a_media_url(self) -> None:
        payload = [
            {
                "title": "voice.wav",
                "size": 5,
                "hash": "work/voice",
                "type": "audio",
            }
        ]
        client = asmr_one.Client()
        with (
            patch.object(client, "request", return_value=(200, payload)),
            self.assertRaises(asmr_one.PayloadError) as raised,
        ):
            client.tracks(1)

        self.assertFalse(asmr_one.is_retryable_network_error(raised.exception))

    def test_tracks_uses_later_nonempty_container(self) -> None:
        node = {
            "title": "track.wav",
            "mediaDownloadUrl": "https://cdn.example/track.wav",
            "size": 5,
            "hash": "work/track",
            "type": "audio",
        }
        client = asmr_one.Client()
        for children in (None, []):
            payload = {"children": children, "tracks": [node]}
            with (
                self.subTest(children=children),
                patch.object(client, "request", return_value=(200, payload)),
            ):
                tracks = client.tracks(1)

            self.assertEqual(len(tracks), 1)
            self.assertEqual(tracks[0]["title"], "track.wav")

    def test_track_title_supplies_extension_for_opaque_url(self) -> None:
        client = asmr_one.Client()
        for url in (
            "https://cdn.example/download/opaque",
            "https://cdn.example/download.php?id=1",
            "https://cdn.example/object.bin",
        ):
            payload = [
                {
                    "title": "voice.wav",
                    "mediaDownloadUrl": url,
                    "size": 5,
                    "hash": "work/voice",
                    "type": "audio",
                }
            ]
            with (
                self.subTest(url=url),
                patch.object(client, "request", return_value=(200, payload)),
            ):
                tracks = client.tracks(1)

            self.assertEqual(tracks[0]["ext"], ".wav")
            self.assertEqual(
                select_files(
                    tracks,
                    audio_format="wav",
                    include_subs=False,
                    all_langs=True,
                ),
                tracks,
            )

    def test_tracks_rejects_invalid_remote_sizes(self) -> None:
        client = asmr_one.Client()
        invalid_sizes = (
            True,
            False,
            -1,
            -1.0,
            5.0,
            5.9,
            float("inf"),
            "5.0",
            "-1",
        )
        for size in invalid_sizes:
            payload = [
                {
                    "title": "voice.wav",
                    "mediaDownloadUrl": "https://cdn.example/voice.wav",
                    "size": size,
                    "hash": "work/voice",
                    "type": "audio",
                }
            ]
            with (
                self.subTest(size=size),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.tracks(1)
            self.assertFalse(
                asmr_one.is_retryable_network_error(raised.exception)
            )

    def test_tracks_normalizes_valid_remote_sizes(self) -> None:
        client = asmr_one.Client()
        valid_sizes = ((None, None), (0, 0), (5, 5), ("5", 5), (" 5 ", 5))
        for size, expected in valid_sizes:
            payload = [
                {
                    "title": "voice.wav",
                    "mediaDownloadUrl": "https://cdn.example/voice.wav",
                    "size": size,
                    "hash": "work/voice",
                    "type": "audio",
                }
            ]
            with (
                self.subTest(size=size),
                patch.object(client, "request", return_value=(200, payload)),
            ):
                tracks = client.tracks(1)
            self.assertEqual(tracks[0]["size"], expected)

    def test_tracks_rejects_invalid_remote_hashes(self) -> None:
        client = asmr_one.Client()
        for remote_hash in (True, 1, {}, [], "   "):
            payload = [
                {
                    "title": "voice.wav",
                    "mediaDownloadUrl": "https://cdn.example/voice.wav",
                    "size": 5,
                    "hash": remote_hash,
                    "type": "audio",
                }
            ]
            with (
                self.subTest(remote_hash=remote_hash),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.tracks(1)
            self.assertFalse(
                asmr_one.is_retryable_network_error(raised.exception)
            )

    def test_work_lookups_preserve_rj_and_vj_namespaces(self) -> None:
        client = asmr_one.Client()
        with patch.object(
            client,
            "request",
            side_effect=(
                (200, {"id": 1, "source_id": "VJ000123"}),
                (200, {"id": 2, "source_id": "RJ000123"}),
                (200, []),
            ),
        ) as request:
            client.work_info("VJ000123")
            client.tracks("RJ000123")

        self.assertEqual(
            [call.args[1] for call in request.call_args_list],
            [
                "/api/workInfo/VJ000123",
                "/api/workInfo/RJ000123",
                "/api/tracks/2?v=2",
            ],
        )

    def test_work_info_rejects_a_mismatched_requested_identity(self) -> None:
        client = asmr_one.Client()
        cases = (
            ("RJ000123", {"id": 1, "source_id": "RJ000124"}),
            ("VJ000123", {"id": 1, "source_id": "RJ000123"}),
            (123, {"id": 124, "source_id": "RJ000123"}),
        )
        for requested, payload in cases:
            with (
                self.subTest(requested=requested, payload=payload),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.work_info(requested)
            self.assertFalse(
                asmr_one.is_retryable_network_error(raised.exception)
            )

        payload = {"id": 123, "source_id": "RJ000123"}
        with patch.object(client, "request", return_value=(200, payload)):
            self.assertEqual(client.work_info("000123"), payload)

    def test_stream_only_leaf_with_children_is_not_treated_as_a_folder(self) -> None:
        payload = [
            {
                "title": "voice.wav",
                "mediaStreamUrl": "https://cdn.example/voice.wav",
                "children": [],
                "size": 5,
                "hash": "work/voice",
                "type": "audio",
            }
        ]
        client = asmr_one.Client()
        with patch.object(client, "request", return_value=(200, payload)):
            tracks = client.tracks(1)

        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0]["url"], payload[0]["mediaStreamUrl"])

    def test_raw_auth_error_is_refreshable_but_api_auth_error_is_not(self) -> None:
        forbidden = asmr_one.urllib.error.HTTPError(
            "https://cdn.example/expired",
            403,
            "forbidden",
            {},
            io.BytesIO(b"expired"),
        )
        client = asmr_one.Client()
        with (
            patch.object(client, "_open_request", side_effect=forbidden),
            patch(
                "asmr_one.socket.getaddrinfo",
                return_value=[
                    (
                        asmr_one.socket.AF_INET,
                        asmr_one.socket.SOCK_STREAM,
                        6,
                        "",
                        ("8.8.8.8", 443),
                    )
                ],
            ),
        ):
            with self.assertRaises(
                asmr_one.DownloadAuthorizationError
            ) as raised:
                client.request(
                    "GET",
                    "",
                    raw_url="https://cdn.example/expired",
                    stream=True,
                )

        self.assertTrue(asmr_one.is_retryable_network_error(raised.exception))
        self.assertFalse(
            asmr_one.is_retryable_network_error(ApiError(403, "forbidden"))
        )

    def test_incomplete_json_response_becomes_retryable_transport_error(self) -> None:
        client = asmr_one.Client()
        with (
            patch.object(
                client,
                "_open_request",
                side_effect=asmr_one.http.client.IncompleteRead(b"{", 2),
            ),
            patch(
                "asmr_one.socket.getaddrinfo",
                return_value=[
                    (
                        asmr_one.socket.AF_INET,
                        asmr_one.socket.SOCK_STREAM,
                        6,
                        "",
                        ("8.8.8.8", 443),
                    )
                ],
            ),
            self.assertRaises(asmr_one.RequestTransportError) as raised,
        ):
            client.request("GET", "", raw_url="https://api.example/data")

        self.assertTrue(asmr_one.is_retryable_network_error(raised.exception))

    def test_non_stream_response_body_is_bounded(self) -> None:
        client = asmr_one.Client()
        response = FakeResponse(b"12345")
        with (
            patch.object(client._api_opener, "open", return_value=response),
            patch.object(asmr_one, "MAX_API_RESPONSE_BYTES", 4),
            self.assertRaises(asmr_one.PayloadError) as raised,
        ):
            client.request("GET", "/data")

        self.assertIn("exceeds 4 bytes", str(raised.exception))
        self.assertTrue(response.closed)

    def test_non_stream_response_rejects_invalid_utf8(self) -> None:
        client = asmr_one.Client()
        response = FakeResponse(b"\xff")
        with (
            patch.object(client._api_opener, "open", return_value=response),
            self.assertRaises(asmr_one.PayloadError) as raised,
        ):
            client.request("GET", "/data")

        self.assertFalse(asmr_one.is_retryable_network_error(raised.exception))
        self.assertTrue(response.closed)

    def test_json_decoder_resource_errors_are_payload_errors(self) -> None:
        decoder_errors = (
            ValueError("integer string conversion limit exceeded"),
            RecursionError("maximum recursion depth exceeded"),
        )
        for decoder_error in decoder_errors:
            client = asmr_one.Client()
            response = FakeResponse(b"{}")
            with (
                self.subTest(decoder_error=decoder_error),
                patch.object(client._api_opener, "open", return_value=response),
                patch("asmr_one.json.loads", side_effect=decoder_error),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.request("GET", "/data")
            self.assertIn("not valid JSON", str(raised.exception))
            self.assertFalse(
                asmr_one.is_retryable_network_error(raised.exception)
            )
            self.assertTrue(response.closed)

    def test_media_urls_reject_local_targets_and_unsafe_final_redirects(self) -> None:
        client = asmr_one.Client()
        with (
            patch.object(client, "_open_request") as open_request,
            self.assertRaises(asmr_one.PayloadError),
        ):
            client.request(
                "GET",
                "",
                raw_url="http://127.0.0.1/private",
                stream=True,
            )
        open_request.assert_not_called()

        with (
            patch.object(client, "_open_request") as open_request,
            patch(
                "asmr_one.socket.getaddrinfo",
                return_value=[
                    (
                        asmr_one.socket.AF_INET,
                        asmr_one.socket.SOCK_STREAM,
                        6,
                        "",
                        ("10.0.0.1", 443),
                    )
                ],
            ),
            self.assertRaises(asmr_one.PayloadError),
        ):
            client.request(
                "GET",
                "",
                raw_url="https://cdn.example/private",
                stream=True,
            )
        open_request.assert_not_called()

        response = FakeResponse(b"data")
        response.geturl = lambda: "https://127.0.0.1/private"  # type: ignore[attr-defined]
        with (
            patch.object(client, "_open_request", return_value=response),
            patch(
                "asmr_one.socket.getaddrinfo",
                return_value=[
                    (
                        asmr_one.socket.AF_INET,
                        asmr_one.socket.SOCK_STREAM,
                        6,
                        "",
                        ("8.8.8.8", 443),
                    )
                ],
            ),
            self.assertRaises(asmr_one.PayloadError),
        ):
            client.request(
                "GET",
                "",
                raw_url="https://cdn.example/file",
                stream=True,
            )
        self.assertTrue(response.closed)

        handler = asmr_one.SafeMediaRedirectHandler()
        with self.assertRaises(asmr_one.PayloadError):
            handler.redirect_request(
                asmr_one.urllib.request.Request("https://cdn.example/file"),
                None,
                302,
                "Found",
                {},
                "https://127.0.0.1/private",
            )

    def test_media_connection_uses_pinned_ip_and_original_tls_hostname(self) -> None:
        raw_socket = MagicMock()
        wrapped_socket = MagicMock()
        context = MagicMock()
        context.wrap_socket.return_value = wrapped_socket
        connection = asmr_one.PinnedHTTPSConnection(
            "cdn.example",
            timeout=5,
            context=context,
            pinned_addresses=("8.8.8.8",),
        )

        with (
            patch("asmr_one.socket.socket", return_value=raw_socket),
            patch(
                "asmr_one.socket.getaddrinfo",
                side_effect=AssertionError("connection performed a second DNS lookup"),
            ),
        ):
            connection.connect()

        raw_socket.connect.assert_called_once_with(("8.8.8.8", 443))
        context.wrap_socket.assert_called_once_with(
            raw_socket,
            server_hostname="cdn.example",
        )

    def test_media_dns_resolution_has_a_deadline(self) -> None:
        release = threading.Event()
        resolver_started = threading.Event()
        resolver_finished = threading.Event()
        validation_finished = threading.Event()
        errors: list[Exception] = []

        def stalled_resolution(*_args: object, **_kwargs: object) -> list[object]:
            resolver_started.set()
            release.wait()
            resolver_finished.set()
            return []

        def validate() -> None:
            try:
                asmr_one.validate_media_url(
                    "https://cdn.example/file",
                    timeout=0.02,
                )
            except Exception as exc:
                errors.append(exc)
            finally:
                validation_finished.set()

        validation_thread = threading.Thread(target=validate)

        try:
            with patch(
                "asmr_one.socket.getaddrinfo",
                side_effect=stalled_resolution,
            ):
                validation_thread.start()
                self.assertTrue(resolver_started.wait(timeout=0.5))
                self.assertTrue(validation_finished.wait(timeout=0.5))
                self.assertFalse(resolver_finished.is_set())
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(
                    errors[0], asmr_one.RequestTransportError
                )
                self.assertTrue(asmr_one.is_retryable_network_error(errors[0]))
        finally:
            release.set()
            validation_thread.join(timeout=0.5)

        self.assertFalse(validation_thread.is_alive())
        self.assertTrue(resolver_finished.wait(timeout=0.5))

    def test_successful_invalid_payload_is_not_retryable(self) -> None:
        client = asmr_one.Client()
        invalid_payloads = (
            [],
            {},
            {"error": "not found"},
            {"id": "", "source_id": " "},
            {"id": True},
        )
        for payload in invalid_payloads:
            with (
                self.subTest(payload=payload),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.work_info("RJ1")
            self.assertFalse(
                asmr_one.is_retryable_network_error(raised.exception)
            )

        successes = (
            (1, {"id": 1}),
            ("RJ1", {"id": 1, "source_id": "RJ000001"}),
        )
        for requested, payload in successes:
            with patch.object(client, "request", return_value=(200, payload)):
                self.assertEqual(client.work_info(requested), payload)

    def test_mirror_failures_preserve_longest_retry_after(self) -> None:
        mirrors = ("https://preferred.example", "https://fallback.example")
        rate_limited = asmr_one.urllib.error.HTTPError(
            f"{mirrors[0]}/data",
            429,
            "rate limited",
            {"Retry-After": "3600"},
            io.BytesIO(b"slow down"),
        )
        unavailable = asmr_one.urllib.error.HTTPError(
            f"{mirrors[1]}/data",
            503,
            "unavailable",
            {},
            io.BytesIO(b"unavailable"),
        )
        with patch.object(asmr_one, "DEFAULT_MIRRORS", mirrors):
            client = asmr_one.Client()
            with patch.object(
                client._api_opener,
                "open",
                side_effect=(rate_limited, unavailable),
            ):
                with self.assertRaises(ApiError) as raised:
                    client.request("GET", "/data")

        self.assertEqual(raised.exception.status, 503)
        self.assertEqual(asmr_one.retry_after_seconds(raised.exception), 3600.0)

    def test_retry_after_is_finite_and_capped(self) -> None:
        cases = (
            ("Infinity", None),
            ("NaN", None),
            ("1e100", asmr_one.MAX_RETRY_AFTER_SECONDS),
            (
                "Fri, 31 Dec 9999 23:59:59 GMT",
                asmr_one.MAX_RETRY_AFTER_SECONDS,
            ),
        )
        for value, expected in cases:
            with self.subTest(value=value):
                error = ApiError(429, "slow down", {"Retry-After": value})
                self.assertEqual(asmr_one.retry_after_seconds(error), expected)


class PlaylistTests(unittest.TestCase):
    def test_iter_playlists_paginates_using_total_count(self) -> None:
        client = asmr_one.Client()
        responses = (
            (
                200,
                {
                    "playlists": [{"id": "p1"}, {"id": "p2"}],
                    "pagination": {"page": 1, "pageSize": 2, "totalCount": 3},
                },
            ),
            (
                200,
                {
                    "playlists": [{"id": "p3"}],
                    "pagination": {"page": 2, "pageSize": 2, "totalCount": 3},
                },
            ),
        )
        with patch.object(client, "request", side_effect=responses) as request:
            playlists = list(client.iter_playlists(filter_by="all", page_size=2))

        self.assertEqual([playlist["id"] for playlist in playlists], ["p1", "p2", "p3"])
        self.assertEqual(request.call_count, 2)
        self.assertIn(
            "page=1&pageSize=2&filterBy=all", request.call_args_list[0].args[1]
        )
        self.assertIn(
            "page=2&pageSize=2&filterBy=all", request.call_args_list[1].args[1]
        )

    def test_iter_playlist_works_paginates(self) -> None:
        client = asmr_one.Client()
        responses = (
            (
                200,
                {
                    "works": [{"id": 1}, {"id": 2}],
                    "pagination": {"page": 1, "pageSize": 2, "totalCount": 3},
                },
            ),
            (
                200,
                {
                    "works": [{"id": 3}],
                    "pagination": {"page": 2, "pageSize": 2, "totalCount": 3},
                },
            ),
        )
        with patch.object(client, "request", side_effect=responses) as request:
            works = list(client.iter_playlist_works("playlist-id", page_size=2))

        self.assertEqual([work["id"] for work in works], [1, 2, 3])
        self.assertEqual(request.call_count, 2)
        self.assertIn(
            "id=playlist-id&page=2&pageSize=2", request.call_args_list[1].args[1]
        )

    def test_pages_reject_malformed_entries_before_counting(self) -> None:
        client = asmr_one.Client()
        cases = (
            (
                {"playlists": [{"id": "p1"}, None]},
                lambda: client.playlists_page(
                    filter_by="all", page=1, page_size=2
                ),
            ),
            (
                {"playlists": [{}]},
                lambda: client.playlists_page(
                    filter_by="all", page=1, page_size=2
                ),
            ),
            (
                {"playlists": [{"id": ""}]},
                lambda: client.playlists_page(
                    filter_by="all", page=1, page_size=2
                ),
            ),
            (
                {"works": [{"id": 1}, None]},
                lambda: client.playlist_works_page(
                    "playlist-id", page=1, page_size=2
                ),
            ),
            (
                {"works": [{"id": 1}, None]},
                lambda: client.review_page(page=1, page_size=2),
            ),
            (
                {"works": [{"work": None}]},
                lambda: client.playlist_works_page(
                    "playlist-id", page=1, page_size=2
                ),
            ),
            (
                {"works": [{}]},
                lambda: client.playlist_works_page(
                    "playlist-id", page=1, page_size=2
                ),
            ),
            (
                {"works": [{"work": {}}]},
                lambda: client.review_page(page=1, page_size=2),
            ),
        )
        for payload, fetch in cases:
            with (
                self.subTest(payload=payload),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError),
            ):
                fetch()

    def test_pages_reject_malformed_pagination_integers(self) -> None:
        client = asmr_one.Client()
        for field in (
            "currentPage",
            "page",
            "pageCount",
            "totalPages",
            "pageSize",
            "totalCount",
        ):
            for value in (
                "unknown",
                True,
                1.0,
                1.9,
                float("inf"),
                float("nan"),
            ):
                payload = {
                    "playlists": [],
                    "pagination": {field: value},
                }
                with (
                    self.subTest(field=field, value=value),
                    patch.object(client, "request", return_value=(200, payload)),
                    self.assertRaises(asmr_one.PayloadError) as raised,
                ):
                    client.playlists_page(filter_by="all", page=1, page_size=2)
                self.assertFalse(
                    asmr_one.is_retryable_network_error(raised.exception)
                )

    def test_pages_reject_a_reported_page_that_was_not_requested(self) -> None:
        client = asmr_one.Client()
        for field in ("currentPage", "page"):
            payload = {
                "playlists": [],
                "pagination": {field: 1, "pageCount": 2},
            }
            with (
                self.subTest(field=field),
                patch.object(client, "request", return_value=(200, payload)),
                self.assertRaises(asmr_one.PayloadError) as raised,
            ):
                client.playlists_page(filter_by="all", page=2, page_size=2)
            self.assertIn("does not match requested page 2", str(raised.exception))
            self.assertFalse(
                asmr_one.is_retryable_network_error(raised.exception)
            )

    def test_collect_works_uses_all_playlists_and_deduplicates(self) -> None:
        client = MagicMock()
        client.iter_playlists.return_value = iter([{"id": "p1"}, {"id": "p2"}])
        playlist_works = {
            "p1": [{"id": 1}, {"id": 2}],
            "p2": [{"id": 2}, {"id": 3}],
        }
        client.iter_playlist_works.side_effect = lambda playlist_id, *, page_size: iter(
            playlist_works[playlist_id]
        )
        args = argparse.Namespace(
            works=None,
            source="auto",
            page_size=2,
            limit=None,
            playlist_selectors=None,
        )

        with patch("asmr_one.log"):
            works = asmr_one.collect_works(client, args)

        self.assertEqual([work["id"] for work in works], [1, 2, 3])
        client.iter_playlists.assert_called_once_with(filter_by="all", page_size=2)
        self.assertEqual(
            [call.args[0] for call in client.iter_playlist_works.call_args_list],
            ["p1", "p2"],
        )

    def test_collection_limit_stops_fetching_after_unique_works(self) -> None:
        client = MagicMock()

        def playlists() -> Any:
            yield {"id": "p1"}
            self.fail("limit advanced the playlist listing")

        client.iter_playlists.return_value = playlists()
        client.iter_playlist_works.side_effect = lambda playlist_id, *, page_size: iter(
            [{"id": 1}, {"id": 1}, {"id": 2}]
            if playlist_id == "p1"
            else self.fail("limit fetched a later playlist")
        )
        args = argparse.Namespace(
            works=None,
            source="auto",
            page_size=2,
            limit=2,
            playlist_selectors=None,
        )

        with patch("asmr_one.log"):
            works = asmr_one.collect_works(client, args)

        self.assertEqual([work["id"] for work in works], [1, 2])
        client.iter_playlist_works.assert_called_once_with("p1", page_size=2)

    def test_review_limit_stops_collection_iteration(self) -> None:
        client = MagicMock()

        def review_works() -> Any:
            yield {"id": 1}
            yield {"id": 1}
            yield {"id": 2}
            self.fail("limit advanced past the requested unique works")

        client.iter_collection.return_value = review_works()
        args = argparse.Namespace(
            works=None,
            source="review",
            page_size=2,
            limit=2,
            playlist_selectors=None,
        )

        works = asmr_one.collect_works(client, args)

        self.assertEqual([work["id"] for work in works], [1, 2])

    def test_playlist_sources_do_not_fall_back_to_review(self) -> None:
        args = argparse.Namespace(
            works=None,
            page_size=50,
            limit=None,
            playlist_selectors=None,
        )
        for source in ("auto", "playlists", "favorites"):
            with self.subTest(source=source):
                client = MagicMock()
                client.iter_playlists.side_effect = ApiError(
                    500, "playlist unavailable"
                )
                args.source = source

                with self.assertRaises(ApiError):
                    asmr_one.collect_works(client, args)

                client.iter_collection.assert_not_called()

    def test_review_source_remains_explicit(self) -> None:
        client = MagicMock()
        client.iter_collection.return_value = iter([{"id": 9}])
        args = argparse.Namespace(
            works=None,
            source="review",
            page_size=25,
            limit=None,
            playlist_selectors=None,
        )

        works = asmr_one.collect_works(client, args)

        self.assertEqual(works, [{"id": 9}])
        client.iter_collection.assert_called_once_with("review", page_size=25)
        client.iter_playlists.assert_not_called()

    def test_explicit_work_list_honors_limit(self) -> None:
        client = MagicMock()
        client.work_info.side_effect = lambda code: {"source_id": code}
        args = argparse.Namespace(
            works=["RJ1", "RJ2"],
            source="auto",
            page_size=50,
            limit=1,
            playlist_selectors=None,
        )

        works = asmr_one.collect_works(client, args)

        self.assertEqual(works, [{"source_id": "RJ1"}])
        client.work_info.assert_called_once_with("RJ1")

    def test_explicit_work_list_deduplicates_before_applying_limit(self) -> None:
        client = MagicMock()

        def work_info(code: str) -> dict[str, object]:
            number = 2 if asmr_one.explicit_work_code_identity(code) == "RJ2" else 1
            return {"id": number, "source_id": f"RJ{number}"}

        client.work_info.side_effect = work_info
        args = argparse.Namespace(
            works=["rj0001", "RJ1", "1", "RJ2"],
            source="auto",
            page_size=50,
            limit=2,
            playlist_selectors=None,
        )

        works = asmr_one.collect_works(client, args)

        self.assertEqual([work["id"] for work in works], [1, 2])
        self.assertEqual(
            [call.args[0] for call in client.work_info.call_args_list],
            ["rj0001", "1", "RJ2"],
        )

    def test_select_playlists_supports_id_name_and_system_alias(self) -> None:
        playlists = [
            {"id": "liked-id", "name": "__SYS_PLAYLIST_LIKED"},
            {"id": "custom-id", "name": "舔耳魅魔\n"},
            {"id": "marked-id", "name": "__SYS_PLAYLIST_MARKED"},
        ]

        selected = asmr_one.select_playlists(
            playlists,
            ["LIKED", "舔耳魅魔", "marked-id", "liked"],
        )

        self.assertEqual(
            [playlist["id"] for playlist in selected],
            ["liked-id", "custom-id", "marked-id"],
        )

    def test_select_playlists_rejects_missing_and_ambiguous_names(self) -> None:
        playlists = [
            {"id": "one", "name": "duplicate"},
            {"id": "two", "name": "duplicate\n"},
        ]
        for selector, message in (
            ("missing", "playlist not found"),
            ("duplicate", "playlist name is ambiguous"),
        ):
            with self.subTest(selector=selector):
                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(SystemExit):
                        asmr_one.select_playlists(playlists, [selector])
                self.assertIn(message, stderr.getvalue())

    def test_playlist_selector_is_invalid_for_review_source(self) -> None:
        client = MagicMock()
        args = argparse.Namespace(
            works=None,
            source="review",
            page_size=50,
            limit=None,
            playlist_selectors=["liked"],
        )
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            with self.assertRaises(SystemExit):
                asmr_one.collect_works(client, args)

        self.assertIn(
            "--playlist cannot be used with --source review", stderr.getvalue()
        )
        client.iter_collection.assert_not_called()

    def test_playlists_command_outputs_single_line_names(self) -> None:
        client = MagicMock()
        client.iter_playlists.return_value = iter(
            [
                {"id": "liked-id", "name": "__SYS_PLAYLIST_LIKED", "works_count": 14},
                {
                    "id": "custom-id",
                    "name": "custom\nname\ud800",
                    "works_count": 9,
                },
            ]
        )
        messages: list[str] = []
        args = argparse.Namespace(timeout=30, page_size=50)

        with (
            patch("asmr_one.require_client", return_value=client),
            patch("asmr_one.log", side_effect=messages.append),
        ):
            asmr_one.cmd_playlists(args)

        self.assertEqual(
            messages,
            ["2 playlists", "liked-id\tLiked\t14", "custom-id\tcustom name\ufffd\t9"],
        )

    def test_work_list_command_outputs_one_record_per_line(self) -> None:
        client = MagicMock()
        client.work_info.return_value = {
            "source_id": "RJ1\nspoof",
            "title": "first\nsecond\tthird\udfff",
        }
        messages: list[str] = []
        args = argparse.Namespace(
            timeout=30,
            works=["RJ1"],
            source="auto",
            page_size=50,
            limit=None,
            playlist_selectors=None,
        )

        with (
            patch("asmr_one.require_client", return_value=client),
            patch("asmr_one.log", side_effect=messages.append),
        ):
            asmr_one.cmd_list(args)

        self.assertEqual(
            messages,
            ["1 works", "RJ1 spoof\tfirst second third\ufffd"],
        )

    def test_work_and_playlist_options_are_mutually_exclusive(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            with self.assertRaises(SystemExit):
                asmr_one.build_parser().parse_args(
                    ["list", "--work", "RJ123456", "--playlist", "liked"]
                )
        self.assertIn("not allowed with argument", stderr.getvalue())


class ChecksumTests(unittest.TestCase):
    def test_manifest_rejects_malformed_and_unsupported_data(self) -> None:
        bad_payloads = (
            "{",
            '{"version": 2, "algorithm": "blake3", "files": {}}',
            '{"version": 1, "algorithm": "sha256", "files": {}}',
            '{"version": 1, "algorithm": "blake3", "files": {"x": {}}}',
        )
        for payload in bad_payloads:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / asmr_one.CHECKSUM_FILE_NAME).write_text(
                    payload, encoding="utf-8"
                )
                with self.assertRaises(asmr_one.LocalStateError):
                    asmr_one.load_checksum_manifest(root)

    def test_manifest_wraps_invalid_utf8(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / asmr_one.CHECKSUM_FILE_NAME
            path.write_bytes(b"\xff\xfe")

            with self.assertRaisesRegex(asmr_one.LocalStateError, "cannot read"):
                asmr_one.load_checksum_manifest(Path(tmp))

    def test_manifest_accepts_negative_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = {
                "remote": {},
                "size": 0,
                "mtime_ns": -1,
                "blake3": "0" * 64,
            }
            asmr_one.save_checksum_manifest(root, manifest)

            loaded = asmr_one.load_checksum_manifest(root)

            self.assertEqual(loaded["files"]["track.wav"]["mtime_ns"], -1)

    def test_manifest_rejects_normalized_duplicate_paths(self) -> None:
        record = {
            "remote": {},
            "size": 0,
            "mtime_ns": 0,
            "blake3": "0" * 64,
        }
        duplicate_pairs = (
            ("A.wav", "a.wav"),
            ("é.wav", "e\u0301.wav"),
        )
        for first, second in duplicate_pairs:
            with (
                self.subTest(first=first, second=second),
                tempfile.TemporaryDirectory() as tmp,
            ):
                root = Path(tmp)
                manifest = asmr_one.empty_checksum_manifest()
                manifest["files"] = {first: record, second: record}
                asmr_one.save_checksum_manifest(root, manifest)

                with self.assertRaisesRegex(asmr_one.LocalStateError, "collide"):
                    asmr_one.load_checksum_manifest(root)

    def test_remote_query_does_not_change_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            first = remote_file()
            second = remote_file(url="https://cdn.example/work/track.wav?token=two")
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = asmr_one.checksum_record(
                first, dest, asmr_one.checksum_file(dest)
            )

            plans, updates, _ = asmr_one.classify_local_files(
                root, [second], manifest, verify=False
            )

            self.assertEqual(plans[0].status, "valid")
            self.assertEqual(updates, {})

    def test_stable_remote_id_ignores_refreshed_url_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            old = remote_file(url="https://cdn.example/old/track.wav")
            refreshed = remote_file(url="https://cdn.example/new/track.wav")
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = asmr_one.checksum_record(
                old, dest, asmr_one.checksum_file(dest)
            )

            plans, updates, _ = asmr_one.classify_local_files(
                root, [refreshed], manifest, verify=False
            )

            self.assertEqual(plans[0].status, "valid")
            self.assertEqual(updates, {})

    def test_manifest_without_stable_remote_id_is_never_a_fast_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            file = remote_file(remote_id=None)
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = asmr_one.checksum_record(
                file, dest, asmr_one.checksum_file(dest)
            )

            for verify in (False, True):
                with self.subTest(verify=verify):
                    plans, updates, _ = asmr_one.classify_local_files(
                        root, [file], manifest, verify=verify
                    )

                    self.assertEqual(plans[0].status, "stale")
                    self.assertEqual(updates, {})

    def test_reserved_and_sanitized_path_collisions_are_rejected(self) -> None:
        manifest = asmr_one.empty_checksum_manifest()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(asmr_one.LocalStateError, "reserved"):
                asmr_one.classify_local_files(
                    Path(tmp),
                    [remote_file("checksums.json")],
                    manifest,
                    verify=False,
                )

            with self.assertRaisesRegex(asmr_one.LocalStateError, "reserved"):
                asmr_one.classify_local_files(
                    Path(tmp),
                    [
                        remote_file(
                            "track.wav",
                            path=("checksums.json", "track.wav"),
                        )
                    ],
                    manifest,
                    verify=False,
                )

            with self.assertRaisesRegex(asmr_one.LocalStateError, "collide"):
                asmr_one.classify_local_files(
                    Path(tmp),
                    [remote_file("a:b.wav"), remote_file("a?b.wav")],
                    manifest,
                    verify=False,
                )

            with self.assertRaisesRegex(asmr_one.LocalStateError, "collide"):
                asmr_one.classify_local_files(
                    Path(tmp),
                    [remote_file("a.wav"), remote_file("a.wav.part")],
                    manifest,
                    verify=False,
                )

            with self.assertRaisesRegex(asmr_one.LocalStateError, "collide"):
                asmr_one.classify_local_files(
                    Path(tmp),
                    [remote_file("a.wav"), remote_file("a.wav.part.json")],
                    manifest,
                    verify=False,
                )

            ancestor = remote_file("audio_", path=("audio_",))
            descendant = remote_file(
                "track.wav", path=("audio_", "track.wav")
            )
            for files in ([ancestor, descendant], [descendant, ancestor]):
                with self.assertRaisesRegex(asmr_one.LocalStateError, "collide"):
                    asmr_one.classify_local_files(
                        Path(tmp), files, manifest, verify=False
                    )

            with self.assertRaisesRegex(asmr_one.LocalStateError, "reserved"):
                asmr_one.checksum_manifest_key(
                    asmr_one.WORK_LOCK_FILE_NAME,
                    Path(tmp) / asmr_one.CHECKSUM_FILE_NAME,
                )

    def test_hash_rejects_destination_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            context = asmr_one.LocalFileContext(
                remote_file(), dest, "track.wav", "track.wav", None
            )
            original_update = asmr_one.update_hasher_from_file

            def replace_after_hash(hasher: object, file_handle: object) -> None:
                original_update(hasher, file_handle)
                replacement = dest.with_name("replacement.wav")
                replacement.write_bytes(b"world")
                os.replace(replacement, dest)

            with (
                patch(
                    "asmr_one.update_hasher_from_file",
                    side_effect=replace_after_hash,
                ),
                self.assertRaisesRegex(
                    asmr_one.LocalStateError, "changed while hashing"
                ),
            ):
                asmr_one.hash_local_file(context)

    def test_checksum_open_does_not_follow_a_swapped_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            outside = root / "outside.wav"
            dest.write_bytes(b"hello")
            outside.write_bytes(b"private")
            real_open = os.open

            def swap_before_open(path: object, flags: int) -> int:
                self.assertEqual(Path(path), dest)
                dest.unlink()
                dest.symlink_to(outside)
                return real_open(path, flags)

            with (
                patch("asmr_one.os.open", side_effect=swap_before_open),
                self.assertRaises(asmr_one.LocalStateError),
            ):
                asmr_one.checksum_file(dest)

            self.assertEqual(outside.read_bytes(), b"private")

    def test_stale_destination_can_resume_matching_replacement_partial(self) -> None:
        old = remote_file(remote_id="work/old")
        current = remote_file(remote_id="work/current")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            checkpoint_part(
                dest,
                b"wo",
                remote=asmr_one.remote_fingerprint(current),
            )
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = asmr_one.checksum_record(
                old, dest, asmr_one.checksum_file(dest)
            )

            plans, _, _ = asmr_one.classify_local_files(
                root, [current], manifest, verify=False
            )

            self.assertEqual(plans[0].status, "stale")
            self.assertTrue(plans[0].resume)
            self.assertEqual(dest.read_bytes(), b"hello")

    def test_refreshed_url_path_keeps_matching_partial_resumable(self) -> None:
        old = remote_file(url="https://cdn.example/old/track.wav")
        refreshed = remote_file(url="https://cdn.example/new/track.wav")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            checkpoint_part(
                dest,
                b"he",
                remote=asmr_one.remote_fingerprint(old),
            )

            plans, _, _ = asmr_one.classify_local_files(
                root,
                [refreshed],
                asmr_one.empty_checksum_manifest(),
                verify=False,
            )

            self.assertEqual(plans[0].status, "partial")
            self.assertTrue(plans[0].resume)

    def test_normalized_manifest_lookup_redownloads_case_changed_remote(self) -> None:
        work = {"id": 1, "source_id": "RJ000001", "title": "Case"}
        old_file = remote_file(
            "A.wav",
            url="https://cdn.example/work/A.wav",
            path=("A.wav",),
        )
        new_file = remote_file(
            "a.wav",
            url="https://cdn.example/work/a.wav",
            path=("a.wav",),
        )
        client = MagicMock()
        client.tracks.return_value = [new_file]
        client.request.return_value = (
            200,
            FakeResponse(b"world", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            dest = root / "a.wav"
            dest.write_bytes(b"hello")
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["A.wav"] = asmr_one.checksum_record(
                old_file, dest, asmr_one.checksum_file(dest)
            )
            asmr_one.save_checksum_manifest(root, manifest)

            with patch("asmr_one.log"):
                _, ok, skipped, failed = asmr_one.download_work(
                    client, work, download_args(tmp)
                )

            self.assertEqual((ok, skipped, failed), (1, 0, 0))
            self.assertEqual(dest.read_bytes(), b"world")
            saved = asmr_one.load_checksum_manifest(root)
            self.assertEqual(set(saved["files"]), {"a.wav"})
            self.assertEqual(
                saved["files"]["a.wav"]["remote"],
                asmr_one.remote_fingerprint(new_file),
            )

    def test_legacy_file_is_adopted_then_uses_fast_path(self) -> None:
        work = {"id": 1, "source_id": "RJ000001", "title": "Test"}
        file = remote_file()
        client = MagicMock()
        client.tracks.return_value = [file]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            (root / "track.wav").write_bytes(b"hello")
            args = download_args(tmp)

            with patch("asmr_one.log"):
                _, ok, skipped, failed = asmr_one.download_work(client, work, args)

            self.assertEqual((ok, skipped, failed), (0, 1, 0))
            client.request.assert_not_called()
            manifest_path = root / asmr_one.CHECKSUM_FILE_NAME
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["files"]["track.wav"]["blake3"],
                asmr_one.checksum_file(root / "track.wav"),
            )

            with (
                patch(
                    "asmr_one.checksum_file",
                    side_effect=AssertionError("fast path hashed"),
                ),
                patch("asmr_one.log"),
            ):
                _, ok, skipped, failed = asmr_one.download_work(client, work, args)

            self.assertEqual((ok, skipped, failed), (0, 1, 0))
            client.request.assert_not_called()

    def test_verify_detects_same_size_corruption_even_with_preserved_mtime(
        self,
    ) -> None:
        file = remote_file()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            record = asmr_one.checksum_record(file, dest, asmr_one.checksum_file(dest))
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = record
            dest.write_bytes(b"jello")
            stat = dest.stat()
            os.utime(dest, ns=(stat.st_atime_ns, record["mtime_ns"]))

            fast, _, _ = asmr_one.classify_local_files(
                root, [file], manifest, verify=False
            )
            verified, _, _ = asmr_one.classify_local_files(
                root, [file], manifest, verify=True
            )

            self.assertEqual(fast[0].status, "valid")
            self.assertEqual(verified[0].status, "corrupt")

    def test_verify_redownloads_corrupt_file_and_updates_manifest(self) -> None:
        work = {"id": 1, "source_id": "RJ000001", "title": "Verify"}
        file = remote_file()
        client = MagicMock()
        client.tracks.return_value = [file]
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = asmr_one.checksum_record(
                file, dest, asmr_one.checksum_file(dest)
            )
            asmr_one.save_checksum_manifest(root, manifest)
            dest.write_bytes(b"jello")

            with patch("asmr_one.log"):
                _, ok, skipped, failed = asmr_one.download_work(
                    client, work, download_args(tmp, verify=True)
                )

            self.assertEqual((ok, skipped, failed), (1, 0, 0))
            self.assertEqual(dest.read_bytes(), b"hello")
            saved = asmr_one.load_checksum_manifest(root)
            self.assertEqual(
                saved["files"]["track.wav"]["blake3"],
                asmr_one.checksum_file(dest),
            )

    def test_failed_repair_preserves_old_file_and_checksum_record(self) -> None:
        work = {"id": 1, "source_id": "RJ000001", "title": "Verify"}
        file = remote_file()
        client = MagicMock()
        client.tracks.return_value = [file]
        client.request.return_value = (
            200,
            FakeResponse(b"bad", {"Content-Length": "3"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            manifest = asmr_one.empty_checksum_manifest()
            original_record = asmr_one.checksum_record(
                file, dest, asmr_one.checksum_file(dest)
            )
            manifest["files"]["track.wav"] = original_record
            asmr_one.save_checksum_manifest(root, manifest)
            dest.write_bytes(b"jello")

            no_retry = asmr_one.RetryPolicy(
                (),
                asmr_one.is_retryable_network_error,
            )
            with (
                patch("asmr_one.log"),
                patch("asmr_one.NETWORK_RETRY_POLICY", no_retry),
            ):
                _, ok, skipped, failed = asmr_one.download_work(
                    client, work, download_args(tmp, verify=True)
                )

            self.assertEqual((ok, skipped, failed), (0, 0, 1))
            self.assertEqual(dest.read_bytes(), b"jello")
            self.assertFalse(asmr_one.part_file_path(dest).exists())
            saved = asmr_one.load_checksum_manifest(root)
            self.assertEqual(saved["files"]["track.wav"], original_record)

    def test_changed_mtime_rehashes_and_refreshes_valid_record(self) -> None:
        file = remote_file()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            dest.write_bytes(b"hello")
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["track.wav"] = asmr_one.checksum_record(
                file, dest, asmr_one.checksum_file(dest)
            )
            stat = dest.stat()
            os.utime(dest, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

            with patch(
                "asmr_one.checksum_file", wraps=asmr_one.checksum_file
            ) as checksum:
                plans, updates, _ = asmr_one.classify_local_files(
                    root, [file], manifest, verify=False
                )

            self.assertEqual(plans[0].status, "valid")
            self.assertEqual(checksum.call_count, 1)
            self.assertIn("track.wav", updates)
            self.assertEqual(updates["track.wav"]["mtime_ns"], dest.stat().st_mtime_ns)

    def test_existing_file_without_size_or_record_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "track.wav").write_bytes(b"hello")

            plans, updates, _ = asmr_one.classify_local_files(
                root,
                [remote_file(size=None)],
                asmr_one.empty_checksum_manifest(),
                verify=False,
            )

            self.assertEqual(plans[0].status, "stale")
            self.assertTrue(plans[0].needs_download)
            self.assertEqual(updates, {})

    def test_download_one_creates_file_and_digest(self) -> None:
        client = MagicMock()
        response = FakeResponse(b"hello", {"Content-Length": "5"})
        client.request.return_value = (200, response)
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=False,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.digest, asmr_one.checksum_file(dest))
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertFalse(asmr_one.part_file_path(dest).exists())
            self.assertIsNone(client.request.call_args.kwargs["range_header"])
            self.assertTrue(response.closed)

    def test_download_record_rejects_destination_replacement(self) -> None:
        file = remote_file()
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        real_download = asmr_one.download_one

        def download_then_replace(*args: Any, **kwargs: Any) -> Any:
            result = real_download(*args, **kwargs)
            dest = Path(args[2])
            replacement = dest.with_name("replacement.tmp")
            replacement.write_bytes(b"world")
            os.replace(replacement, dest)
            return result

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            context = asmr_one.LocalFileContext(
                file,
                dest,
                "track.wav",
                "track.wav",
                None,
            )
            plan = asmr_one.LocalFilePlan(
                file,
                dest,
                "track.wav",
                "missing",
            )

            with (
                patch("asmr_one.download_one", side_effect=download_then_replace),
                self.assertRaisesRegex(
                    asmr_one.LocalStateError,
                    "changed after installation",
                ),
            ):
                asmr_one.download_file_and_record(client, context, plan)

    def test_download_checkpoints_progress_at_configured_intervals(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        committed_sizes: list[int] = []
        original_save = asmr_one.save_partial_state

        def capture(dest: Path, state: asmr_one.PartialState) -> None:
            committed_sizes.append(state.committed_size)
            original_save(dest, state)

        with (
            tempfile.TemporaryDirectory() as tmp,
            patch("asmr_one.DOWNLOAD_CHUNK_SIZE", 2),
            patch("asmr_one.save_partial_state", side_effect=capture),
        ):
            dest = Path(tmp) / "track.wav"
            asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=False,
                remote=download_remote(),
                checkpoint_size=2,
            )

        self.assertEqual(committed_sizes, [0, 2, 4, 5])

    def test_download_one_resumes_with_matching_content_range(self) -> None:
        client = MagicMock()
        response = FakeResponse(
            b"llo",
            {"Content-Range": "bytes 2-4/5", "Content-Length": "3"},
        )
        client.request.return_value = (206, response)
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"he")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "resume")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertEqual(result.digest, asmr_one.checksum_file(dest))
            self.assertEqual(
                client.request.call_args.kwargs["range_header"], "bytes=2-"
            )

    def test_initial_request_failure_closes_validated_partial_handle(self) -> None:
        client = MagicMock()
        client.request.side_effect = asmr_one.RequestTransportError("offline")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dest = root / "track.wav"
            opened = (root / "opened.part").open("w+b")
            hasher = asmr_one.new_blake3_hasher()
            state = asmr_one.PartialState(
                download_remote(),
                2,
                hasher.hexdigest(),
            )
            signature = asmr_one.stat_signature(os.fstat(opened.fileno()))
            try:
                with (
                    patch(
                        "asmr_one.open_validated_partial",
                        return_value=(opened, hasher, state, signature),
                    ),
                    self.assertRaises(asmr_one.RequestTransportError),
                ):
                    asmr_one.download_one(
                        client,
                        "https://cdn.example/track.wav",
                        dest,
                        5,
                        resume=True,
                        remote=download_remote(),
                    )
                self.assertTrue(opened.closed)
            finally:
                if not opened.closed:
                    opened.close()

    def test_range_body_length_mismatch_preserves_received_progress(self) -> None:
        client = MagicMock()
        response = FakeResponse(b"ll", {"Content-Range": "bytes 2-4/5"})
        client.request.return_value = (206, response)
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            part = asmr_one.part_file_path(dest)
            dest.write_bytes(b"older")
            checkpoint_part(dest, b"he")

            with self.assertRaisesRegex(
                asmr_one.IncompleteDownloadError, "response body length"
            ):
                asmr_one.download_one(
                    client,
                    "https://cdn.example/track.wav",
                    dest,
                    5,
                    resume=True,
                    remote=download_remote(),
                )

            self.assertEqual(dest.read_bytes(), b"older")
            self.assertEqual(part.read_bytes(), b"hell")
            state = asmr_one.load_partial_state(dest)
            self.assertIsNotNone(state)
            self.assertEqual(state.committed_size, 4)
            self.assertTrue(response.closed)

    def test_range_that_does_not_reach_eof_restarts_from_zero(self) -> None:
        client = MagicMock()
        response = FakeResponse(b"ll", {"Content-Range": "bytes 2-3/5"})
        client.request.side_effect = (
            (206, response),
            (200, FakeResponse(b"hello", {"Content-Length": "5"})),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            part = asmr_one.part_file_path(dest)
            checkpoint_part(dest, b"he")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertFalse(part.exists())
            self.assertEqual(client.request.call_count, 2)
            self.assertTrue(response.closed)

    def test_download_one_restarts_when_server_ignores_range(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"bad")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")

    def test_download_one_restarts_after_range_not_satisfiable(self) -> None:
        client = MagicMock()
        client.request.side_effect = (
            ApiError(416, "range not satisfiable"),
            (200, FakeResponse(b"hello", {"Content-Length": "5"})),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"he")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertEqual(
                client.request.call_args_list[0].kwargs["range_header"], "bytes=2-"
            )
            self.assertNotIn("range_header", client.request.call_args_list[1].kwargs)

    def test_oversized_partial_restarts_without_range(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            asmr_one.part_file_path(dest).write_bytes(b"too-long")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertIsNone(client.request.call_args.kwargs["range_header"])

    def test_invalid_content_range_restarts_and_preserves_old_until_success(
        self,
    ) -> None:
        client = MagicMock()
        response = FakeResponse(
            b"llo",
            {"Content-Range": "bytes 1-3/5", "Content-Length": "3"},
        )
        client.request.side_effect = (
            (206, response),
            (200, FakeResponse(b"hello", {"Content-Length": "5"})),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            part = asmr_one.part_file_path(dest)
            dest.write_bytes(b"older")
            checkpoint_part(dest, b"he")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertFalse(part.exists())
            self.assertTrue(response.closed)

    def test_exact_size_partial_is_promoted_without_network(self) -> None:
        client = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"hello")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "resume")
            self.assertEqual(dest.read_bytes(), b"hello")
            client.request.assert_not_called()

    def test_legacy_partial_without_state_restarts_from_zero(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            asmr_one.part_file_path(dest).write_bytes(b"he")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertIsNone(client.request.call_args.kwargs["range_header"])

    def test_changed_remote_identity_restarts_same_size_partial(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"world", {"Content-Length": "5"}),
        )
        changed = asmr_one.remote_fingerprint(remote_file(remote_id="work/new"))
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"he")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=changed,
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"world")
            self.assertIsNone(client.request.call_args.kwargs["range_header"])

    def test_http_protocol_read_error_is_checkpointed_and_retryable(self) -> None:
        response = MagicMock()
        response.headers = {"Content-Length": "5"}
        response.read.side_effect = asmr_one.http.client.LineTooLong("chunk line")
        client = MagicMock()
        client.request.return_value = (200, response)
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"

            with self.assertRaises(asmr_one.DownloadProtocolError) as raised:
                asmr_one.download_one(
                    client,
                    "https://cdn.example/track.wav",
                    dest,
                    5,
                    resume=False,
                    remote=download_remote(),
                )

            self.assertTrue(
                asmr_one.is_retryable_network_error(raised.exception)
            )
            self.assertTrue(asmr_one.part_file_path(dest).is_file())
            self.assertIsNotNone(asmr_one.load_partial_state(dest))
            response.close.assert_called_once_with()

    def test_malformed_full_content_length_is_retryable(self) -> None:
        for value in ("invalid", "-1"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as tmp:
                response = FakeResponse(b"hello", {"Content-Length": value})
                client = MagicMock()
                client.request.return_value = (200, response)

                with self.assertRaises(asmr_one.DownloadProtocolError) as raised:
                    asmr_one.download_one(
                        client,
                        "https://cdn.example/track.wav",
                        Path(tmp) / "track.wav",
                        5,
                        resume=False,
                        remote=download_remote(),
                    )

                self.assertTrue(
                    asmr_one.is_retryable_network_error(raised.exception)
                )
                self.assertTrue(response.closed)

    def test_stream_stops_at_and_rejects_the_first_excess_byte(self) -> None:
        read_sizes: list[int] = []

        class RecordingResponse(FakeResponse):
            def read(self, size: int = -1) -> bytes:
                read_sizes.append(size)
                return super().read(size)

        response = RecordingResponse(b"abcdef")
        client = MagicMock()
        client.request.return_value = (200, response)
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"

            with self.assertRaises(asmr_one.DownloadProtocolError) as raised:
                asmr_one.download_one(
                    client,
                    "https://cdn.example/track.wav",
                    dest,
                    5,
                    resume=False,
                    remote=download_remote(),
                )

            self.assertTrue(
                asmr_one.is_retryable_network_error(raised.exception)
            )
            self.assertEqual(read_sizes, [6])
            self.assertFalse(dest.exists())
            self.assertEqual(asmr_one.part_file_path(dest).stat().st_size, 0)
            state = asmr_one.load_partial_state(dest)
            self.assertIsNotNone(state)
            self.assertEqual(state.committed_size, 0)  # type: ignore[union-attr]
            self.assertTrue(response.closed)

    def test_stable_remote_id_resumes_after_url_path_refresh(self) -> None:
        old_file = remote_file(
            url="https://cdn.example/old/track.wav?token=one",
            remote_id="work/stable",
        )
        refreshed_file = remote_file(
            url="https://cdn.example/new/track.wav?token=two",
            remote_id="work/stable",
        )
        client = MagicMock()
        client.request.return_value = (
            206,
            FakeResponse(
                b"llo",
                {"Content-Range": "bytes 2-4/5", "Content-Length": "3"},
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(
                dest,
                b"he",
                remote=asmr_one.remote_fingerprint(old_file),
            )

            result = asmr_one.download_one(
                client,
                str(refreshed_file["url"]),
                dest,
                5,
                resume=True,
                remote=asmr_one.remote_fingerprint(refreshed_file),
            )

            self.assertEqual(result.status, "resume")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertEqual(
                client.request.call_args.kwargs["range_header"], "bytes=2-"
            )

    def test_partial_without_remote_id_or_etag_restarts_from_zero(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        remote = asmr_one.remote_fingerprint(remote_file(remote_id=None))
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"he", remote=remote)

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=remote,
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertIsNone(client.request.call_args.kwargs["range_header"])

    def test_exact_partial_with_only_etag_is_revalidated(self) -> None:
        client = MagicMock()
        client.request.side_effect = (
            ApiError(416, "range not satisfiable"),
            (200, FakeResponse(b"hello", {"Content-Length": "5"})),
        )
        remote = asmr_one.remote_fingerprint(remote_file(remote_id=None))
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"hello", remote=remote, etag='"version-one"')

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=remote,
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(client.request.call_count, 2)
            self.assertEqual(
                client.request.call_args_list[0].kwargs["range_header"], "bytes=5-"
            )
            self.assertEqual(
                client.request.call_args_list[0].kwargs["headers"],
                {"If-Range": '"version-one"'},
            )

    def test_same_size_partial_mutation_is_not_resumed(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            part = checkpoint_part(dest, b"he")
            part.write_bytes(b"XX")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertIsNone(client.request.call_args.kwargs["range_header"])

    def test_uncommitted_partial_tail_is_truncated_before_resume(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            206,
            FakeResponse(
                b"llo",
                {"Content-Range": "bytes 2-4/5", "Content-Length": "3"},
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            part = checkpoint_part(dest, b"he")
            with part.open("ab") as fh:
                fh.write(b"uncommitted")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "resume")
            self.assertEqual(dest.read_bytes(), b"hello")
            self.assertEqual(
                client.request.call_args.kwargs["range_header"], "bytes=2-"
            )

    def test_strong_etag_is_sent_with_if_range(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            206,
            FakeResponse(
                b"llo",
                {
                    "Content-Range": "bytes 2-4/5",
                    "Content-Length": "3",
                    "ETag": '"version-one"',
                },
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"he", etag='"version-one"')

            asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(
                client.request.call_args.kwargs["headers"],
                {"If-Range": '"version-one"'},
            )

    def test_changed_etag_restarts_from_zero(self) -> None:
        client = MagicMock()
        ranged = FakeResponse(
            b"llo",
            {
                "Content-Range": "bytes 2-4/5",
                "Content-Length": "3",
                "ETag": '"version-two"',
            },
        )
        client.request.side_effect = (
            (206, ranged),
            (
                200,
                FakeResponse(
                    b"world",
                    {"Content-Length": "5", "ETag": '"version-two"'},
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            checkpoint_part(dest, b"he", etag='"version-one"')

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"world")
            self.assertEqual(client.request.call_count, 2)
            self.assertTrue(ranged.closed)

    def test_exact_size_partial_with_bad_digest_is_redownloaded(self) -> None:
        client = MagicMock()
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            part = checkpoint_part(dest, b"hello")
            part.write_bytes(b"jello")

            result = asmr_one.download_one(
                client,
                "https://cdn.example/track.wav",
                dest,
                5,
                resume=True,
                remote=download_remote(),
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(dest.read_bytes(), b"hello")
            client.request.assert_called_once()

    def test_symlink_destination_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "work"
            root.mkdir()
            outside = Path(tmp) / "outside.wav"
            outside.write_bytes(b"hello")
            (root / "track.wav").symlink_to(outside)

            with self.assertRaisesRegex(asmr_one.LocalStateError, "symlink"):
                asmr_one.classify_local_files(
                    root,
                    [remote_file()],
                    asmr_one.empty_checksum_manifest(),
                    verify=False,
                )

            self.assertEqual(outside.read_bytes(), b"hello")

    def test_partial_symlink_created_during_request_is_never_followed(self) -> None:
        responses = (
            (200, {"Content-Length": "5"}),
            (
                206,
                {
                    "Content-Range": "bytes 0-4/5",
                    "Content-Length": "5",
                },
            ),
        )
        for status, headers in responses:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                dest = root / "track.wav"
                part = asmr_one.part_file_path(dest)
                outside = root / "outside.txt"
                outside.write_bytes(b"do not truncate")
                client = MagicMock()

                def request(*_args: object, **_kwargs: object) -> tuple[int, FakeResponse]:
                    part.symlink_to(outside)
                    return status, FakeResponse(b"hello", headers)

                client.request.side_effect = request
                with self.assertRaisesRegex(
                    asmr_one.LocalStateError,
                    "partial file must not be a symlink",
                ):
                    asmr_one.download_one(
                        client,
                        "https://cdn.example/track.wav",
                        dest,
                        5,
                        resume=False,
                        remote=download_remote(),
                    )

                self.assertEqual(outside.read_bytes(), b"do not truncate")

    def test_symlinked_directory_below_work_root_is_rejected(self) -> None:
        client = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "work"
            outside = Path(tmp) / "outside"
            root.mkdir()
            outside.mkdir()
            (root / "folder").symlink_to(outside, target_is_directory=True)
            dest = root / "folder" / "track.wav"

            with self.assertRaisesRegex(asmr_one.LocalStateError, "symlink"):
                asmr_one.download_one(
                    client,
                    "https://cdn.example/track.wav",
                    dest,
                    5,
                    resume=False,
                    remote=download_remote(),
                    work_root=root,
                )

            self.assertFalse((outside / "track.wav").exists())
            client.request.assert_not_called()

    def test_inspection_and_hash_reject_symlinked_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "work"
            outside = Path(tmp) / "outside"
            root.mkdir()
            outside.mkdir()
            (outside / "track.wav").write_bytes(b"hello")
            (root / "folder").symlink_to(outside, target_is_directory=True)
            context = asmr_one.LocalFileContext(
                remote_file(path=("folder", "track.wav")),
                root / "folder" / "track.wav",
                "folder/track.wav",
                "folder/track.wav",
                None,
            )

            for operation in (
                lambda: asmr_one.inspect_local_file(context, verify=False),
                lambda: asmr_one.hash_local_file(context),
            ):
                with (
                    self.subTest(operation=operation),
                    self.assertRaisesRegex(asmr_one.LocalStateError, "symlink"),
                ):
                    operation()

    def test_replaced_partial_is_not_installed_after_validation(self) -> None:
        client = MagicMock()
        response = FakeResponse(b"hello", {"Content-Length": "5"})
        client.request.return_value = (200, response)
        original_close = asmr_one.close_response
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "track.wav"
            part = asmr_one.part_file_path(dest)

            def replace_on_close(current: object) -> None:
                original_close(current)
                replacement = Path(tmp) / "replacement.part"
                replacement.write_bytes(b"jello")
                os.replace(replacement, part)

            with (
                patch("asmr_one.close_response", side_effect=replace_on_close),
                self.assertRaisesRegex(
                    asmr_one.LocalStateError, "changed before installation"
                ),
            ):
                asmr_one.download_one(
                    client,
                    "https://cdn.example/track.wav",
                    dest,
                    5,
                    resume=False,
                    remote=download_remote(),
                )

            self.assertFalse(dest.exists())
            self.assertEqual(part.read_bytes(), b"jello")

    def test_download_work_only_requests_missing_files_and_keeps_old_records(
        self,
    ) -> None:
        work = {"id": 1, "source_id": "RJ000001", "title": "Test"}
        existing = remote_file("existing.wav", size=5)
        missing = remote_file(
            "missing.wav",
            size=5,
            url="https://cdn.example/work/missing.wav",
            remote_id="work/missing",
        )
        old = remote_file(
            "old.wav",
            size=3,
            url="https://cdn.example/work/old.wav",
            remote_id="work/old",
        )
        client = MagicMock()
        client.tracks.return_value = [existing, missing]
        client.request.return_value = (
            200,
            FakeResponse(b"world", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            (root / "existing.wav").write_bytes(b"hello")
            (root / "old.wav").write_bytes(b"old")
            manifest = asmr_one.empty_checksum_manifest()
            manifest["files"]["old.wav"] = asmr_one.checksum_record(
                old, root / "old.wav", asmr_one.checksum_file(root / "old.wav")
            )
            asmr_one.save_checksum_manifest(root, manifest)

            with patch("asmr_one.log"):
                _, ok, skipped, failed = asmr_one.download_work(
                    client, work, download_args(tmp)
                )

            self.assertEqual((ok, skipped, failed), (1, 1, 0))
            self.assertEqual(client.request.call_count, 1)
            saved = asmr_one.load_checksum_manifest(root)
            self.assertEqual(
                set(saved["files"]), {"existing.wav", "missing.wav", "old.wav"}
            )
            self.assertEqual((root / "missing.wav").read_bytes(), b"world")

    def test_dry_run_classifies_without_writing(self) -> None:
        work = {"id": 1, "source_id": "RJ000001", "title": "Dry"}
        client = MagicMock()
        client.tracks.return_value = [remote_file()]
        messages: list[str] = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)

            with patch("asmr_one.log", side_effect=messages.append):
                asmr_one.download_work(client, work, download_args(tmp, dry_run=True))

            self.assertFalse(root.exists())
            client.request.assert_not_called()
            self.assertTrue(any("missing" in message for message in messages))

    def test_dry_run_does_not_save_legacy_adoption(self) -> None:
        work = {"id": 1, "source_id": "RJ000001", "title": "Dry"}
        client = MagicMock()
        client.tracks.return_value = [remote_file()]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            (root / "track.wav").write_bytes(b"hello")

            with patch("asmr_one.log"):
                asmr_one.download_work(
                    client,
                    work,
                    download_args(tmp, dry_run=True, verify=True),
                )

            self.assertFalse((root / asmr_one.CHECKSUM_FILE_NAME).exists())
            self.assertFalse((root / asmr_one.WORK_LOCK_FILE_NAME).exists())
            client.request.assert_not_called()

    def test_download_parser_supports_verify(self) -> None:
        args = asmr_one.build_parser().parse_args(
            ["download", "--verify", "--work", "RJ1"]
        )
        self.assertTrue(args.verify)


class GlobalTaskSchedulerTests(unittest.TestCase):
    @staticmethod
    def work(number: int) -> dict[str, object]:
        return {
            "id": number,
            "source_id": f"RJ{number:06d}",
            "title": f"Work {number}",
        }

    def test_reported_page_count_is_scheduled_through_a_bounded_window(self) -> None:
        scheduler = MagicMock()
        scheduler.jobs = 2
        tasks: list[asmr_one.ScheduledTask] = []
        scheduler.enqueue.side_effect = tasks.append
        emitted: list[int] = []
        stream = asmr_one.OrderedPageStream(
            scheduler,
            owner="pages",
            label="collection",
            fetch_page=lambda page: self.fail(f"unexpected fetch {page}"),
            on_items=lambda items: emitted.extend(int(item["id"]) for item in items),
            on_done=lambda: self.fail("huge stream finished"),
            on_error=lambda exc: self.fail(str(exc)),
            should_stop=lambda: False,
        )

        stream.start()
        self.assertEqual(len(tasks), 1)
        tasks[0].on_success(
            asmr_one.FetchedPage([{"id": 1}], 1_000_000, True)
        )

        self.assertEqual(emitted, [1])
        self.assertEqual([task.label for task in tasks], [
            "collection page=1",
            "collection page=2",
            "collection page=3",
        ])

        tasks[1].on_success(
            asmr_one.FetchedPage([{"id": 2}], 1_000_000, True)
        )
        self.assertEqual(emitted, [1, 2])
        self.assertEqual(tasks[-1].label, "collection page=4")
        self.assertEqual(len(tasks), 4)

    def test_short_page_does_not_replace_a_reported_final_page(self) -> None:
        scheduler = MagicMock()
        scheduler.jobs = 2
        tasks: list[asmr_one.ScheduledTask] = []
        scheduler.enqueue.side_effect = tasks.append
        done = MagicMock()
        failures: list[BaseException] = []
        stream = asmr_one.OrderedPageStream(
            scheduler,
            owner="pages",
            label="collection",
            fetch_page=lambda page: self.fail(f"unexpected fetch {page}"),
            on_items=lambda _items: None,
            on_done=done,
            on_error=failures.append,
            should_stop=lambda: False,
        )

        stream.start()
        tasks[0].on_success(asmr_one.FetchedPage([{"id": 1}], 5, True))
        tasks[1].on_success(asmr_one.FetchedPage([], None, False))

        self.assertEqual(
            [task.label for task in tasks],
            [
                "collection page=1",
                "collection page=2",
                "collection page=3",
                "collection page=4",
            ],
        )
        done.assert_not_called()
        self.assertEqual(failures, [])

    def test_scheduler_enforces_one_global_worker_limit(self) -> None:
        scheduler = asmr_one.TaskScheduler(2)
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        active = 0
        maximum = 0
        completed: list[int] = []

        def run_task(number: int) -> int:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                barrier.wait(timeout=3)
                return number
            finally:
                with lock:
                    active -= 1

        for number in range(4):
            scheduler.enqueue(
                asmr_one.ScheduledTask(
                    owner=f"owner-{number % 2}",
                    label=f"task-{number}",
                    run=lambda number=number: run_task(number),
                    on_success=completed.append,
                    on_error=lambda exc: self.fail(str(exc)),
                )
            )

        scheduler.run()

        self.assertEqual(maximum, 2)
        self.assertCountEqual(completed, range(4))

    def test_scheduler_retries_with_delays_without_occupying_worker(self) -> None:
        clock = FakeClock()
        scheduler = asmr_one.TaskScheduler(1, clock=clock, sleeper=clock.sleep)
        call_times: list[float] = []
        retries: list[tuple[int, float]] = []
        completed: list[str] = []

        def run() -> str:
            call_times.append(clock.now)
            if len(call_times) < 3:
                raise asmr_one.RequestTransportError("temporary")
            return "ok"

        scheduler.enqueue(
            asmr_one.ScheduledTask(
                owner="owner",
                label="network",
                run=run,
                on_success=completed.append,
                on_error=lambda exc: self.fail(str(exc)),
                retry_policy=asmr_one.RetryPolicy(
                    (1.0, 5.0),
                    asmr_one.is_retryable_network_error,
                ),
                on_retry=lambda _exc, attempt, _total, delay: retries.append(
                    (attempt, delay)
                ),
            )
        )

        scheduler.run()

        self.assertEqual(call_times, [0.0, 1.0, 6.0])
        self.assertEqual(clock.sleeps, [1.0, 5.0])
        self.assertEqual(retries, [(1, 1.0), (2, 5.0)])
        self.assertEqual(completed, ["ok"])

    def test_discarded_owner_cannot_be_revived_by_inflight_failure(self) -> None:
        clock = FakeClock()
        scheduler = asmr_one.TaskScheduler(2, clock=clock, sleeper=clock.sleep)
        victim_started = threading.Event()
        release_victim = threading.Event()
        attempts = 0
        retries: list[int] = []
        failures: list[BaseException] = []

        def victim() -> None:
            nonlocal attempts
            attempts += 1
            victim_started.set()
            self.assertTrue(release_victim.wait(timeout=3))
            raise asmr_one.RequestTransportError("late failure")

        def cancel_victim(_: object) -> None:
            scheduler.discard_ready("victim")
            release_victim.set()

        scheduler.enqueue(
            asmr_one.ScheduledTask(
                owner="victim",
                label="victim",
                run=victim,
                on_success=lambda _: self.fail("unexpected success"),
                on_error=failures.append,
                retry_policy=asmr_one.NETWORK_RETRY_POLICY,
                on_retry=lambda _exc, attempt, _total, _delay: retries.append(attempt),
            )
        )
        scheduler.enqueue(
            asmr_one.ScheduledTask(
                owner="control",
                label="cancel",
                run=lambda: victim_started.wait(timeout=3),
                on_success=cancel_victim,
                on_error=lambda exc: self.fail(str(exc)),
            )
        )

        scheduler.run()

        self.assertEqual(attempts, 1)
        self.assertEqual(retries, [])
        self.assertEqual(failures, [])
        self.assertEqual(clock.sleeps, [])

    def test_scheduler_runs_ready_work_before_future_task(self) -> None:
        clock = FakeClock()
        scheduler = asmr_one.TaskScheduler(1, clock=clock, sleeper=clock.sleep)
        order: list[tuple[str, float]] = []

        def task(name: str, eligible_at: float = 0.0) -> asmr_one.ScheduledTask:
            return asmr_one.ScheduledTask(
                owner=name,
                label=name,
                run=lambda: order.append((name, clock.now)),
                on_success=lambda _: None,
                on_error=lambda exc: self.fail(str(exc)),
                eligible_at=eligible_at,
            )

        scheduler.enqueue(task("later", 10.0))
        scheduler.enqueue(task("ready"))
        scheduler.run()

        self.assertEqual(order, [("ready", 0.0), ("later", 10.0)])
        self.assertEqual(clock.sleeps, [10.0])

    def test_retry_after_header_extends_policy_delay(self) -> None:
        clock = FakeClock()
        scheduler = asmr_one.TaskScheduler(1, clock=clock, sleeper=clock.sleep)
        attempts = 0

        def run() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ApiError(429, "slow down", {"Retry-After": "120"})

        scheduler.enqueue(
            asmr_one.ScheduledTask(
                owner="owner",
                label="rate-limited",
                run=run,
                on_success=lambda _: None,
                on_error=lambda exc: self.fail(str(exc)),
                retry_policy=asmr_one.RetryPolicy(
                    (1.0,),
                    asmr_one.is_retryable_network_error,
                    asmr_one.retry_after_seconds,
                ),
            )
        )

        scheduler.run()

        self.assertEqual(attempts, 2)
        self.assertEqual(clock.sleeps, [120.0])

    def test_network_retry_exhaustion_reports_one_final_error(self) -> None:
        clock = FakeClock()
        scheduler = asmr_one.TaskScheduler(1, clock=clock, sleeper=clock.sleep)
        attempts = 0
        failures: list[BaseException] = []

        def run() -> None:
            nonlocal attempts
            attempts += 1
            raise ApiError(503, "unavailable")

        scheduler.enqueue(
            asmr_one.ScheduledTask(
                owner="owner",
                label="network",
                run=run,
                on_success=lambda _: self.fail("unexpected success"),
                on_error=failures.append,
                retry_policy=asmr_one.NETWORK_RETRY_POLICY,
            )
        )

        scheduler.run()

        self.assertEqual(attempts, 6)
        self.assertEqual(clock.sleeps, list(asmr_one.RETRY_DELAYS))
        self.assertEqual(len(failures), 1)

    def test_permanent_http_error_is_not_retried(self) -> None:
        clock = FakeClock()
        scheduler = asmr_one.TaskScheduler(1, clock=clock, sleeper=clock.sleep)
        attempts = 0
        failures: list[BaseException] = []

        def run() -> None:
            nonlocal attempts
            attempts += 1
            raise ApiError(404, "missing")

        scheduler.enqueue(
            asmr_one.ScheduledTask(
                owner="owner",
                label="network",
                run=run,
                on_success=lambda _: self.fail("unexpected success"),
                on_error=failures.append,
                retry_policy=asmr_one.NETWORK_RETRY_POLICY,
            )
        )

        scheduler.run()

        self.assertEqual(attempts, 1)
        self.assertEqual(clock.sleeps, [])
        self.assertEqual(len(failures), 1)

    def test_work_lock_conflict_is_released_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "work"
            first = asmr_one.WorkLock.acquire(root)
            try:
                with self.assertRaises(asmr_one.WorkLockedError):
                    asmr_one.WorkLock.acquire(root)
            finally:
                first.close()

            second = asmr_one.WorkLock.acquire(root)
            second.close()

    def test_delayed_work_lock_does_not_block_later_work(self) -> None:
        works = [self.work(1), self.work(2)]
        client = MagicMock()
        client.tracks.return_value = []
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            locked_root = Path(tmp) / asmr_one.work_folder_name(works[0])
            held = asmr_one.WorkLock.acquire(locked_root)
            try:
                coordinator = asmr_one.DownloadCoordinator(
                    client,
                    download_args(tmp, jobs=1),
                )
                coordinator.scheduler = asmr_one.TaskScheduler(
                    1,
                    clock=clock,
                    sleeper=clock.sleep,
                )
                with patch("asmr_one.log"):
                    summary = coordinator.run_direct(works)
            finally:
                held.close()

        self.assertEqual((summary.works, summary.fail), (2, 1))
        client.tracks.assert_called_once_with(2)
        self.assertEqual(clock.sleeps, list(asmr_one.RETRY_DELAYS))

    def test_transient_outage_bounds_active_locked_works(self) -> None:
        works = [self.work(number) for number in range(1, 13)]
        client = MagicMock()
        client.tracks.side_effect = asmr_one.RequestTransportError("tracks unavailable")
        clock = FakeClock()
        peak_active = 0

        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            coordinator = asmr_one.DownloadCoordinator(
                client,
                download_args(tmp, jobs=2),
            )
            coordinator.scheduler = asmr_one.TaskScheduler(
                2,
                clock=clock,
                sleeper=clock.sleep,
            )
            original_start = coordinator._start_work

            def record_start(state: asmr_one.WorkState) -> None:
                nonlocal peak_active
                peak_active = max(peak_active, len(coordinator.active_works))
                original_start(state)

            with patch.object(coordinator, "_start_work", side_effect=record_start):
                summary = coordinator.run_direct(works)

        self.assertEqual((summary.works, summary.fail), (12, 12))
        self.assertEqual(coordinator.max_active_works, 4)
        self.assertEqual(peak_active, coordinator.max_active_works)
        self.assertEqual(coordinator.active_works, {})

    def test_work_info_and_tracks_requests_are_retried(self) -> None:
        client = MagicMock()
        client.work_info.side_effect = (
            ApiError(503, "work info unavailable"),
            self.work(1),
        )
        client.tracks.side_effect = (
            asmr_one.RequestTransportError("tracks unavailable"),
            [],
        )
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            coordinator = asmr_one.DownloadCoordinator(
                client,
                download_args(tmp, jobs=1, dry_run=True),
            )
            coordinator.scheduler = asmr_one.TaskScheduler(
                1,
                clock=clock,
                sleeper=clock.sleep,
            )
            summary = coordinator.run_direct([{"source_id": "RJ1"}])

        self.assertEqual((summary.works, summary.fail), (1, 0))
        self.assertEqual(client.work_info.call_count, 2)
        self.assertEqual(client.tracks.call_count, 2)
        self.assertEqual(clock.sleeps, [60.0, 60.0])

    def test_malformed_internal_id_is_resolved_through_source_id(self) -> None:
        client = MagicMock()
        client.work_info.return_value = self.work(1)
        client.tracks.return_value = []
        malformed = {"id": True, "source_id": "RJ1", "title": "Work 1"}

        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client,
                download_args(tmp, dry_run=True),
            ).run_direct([malformed])

        self.assertEqual((summary.works, summary.fail), (1, 0))
        client.work_info.assert_called_once_with("RJ1")
        client.tracks.assert_called_once_with(1)

    def test_refresh_does_not_rebind_missing_stable_id_by_path(self) -> None:
        work = self.work(1)
        original = remote_file("track.wav", remote_id="work/original")
        replacement = remote_file("track.wav", remote_id="work/replacement")
        client = MagicMock()
        client.tracks.return_value = [replacement]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            contexts, _ = asmr_one.prepare_local_files(
                root,
                [original],
                asmr_one.empty_checksum_manifest(),
            )
            context = contexts[0]
            operation = asmr_one.DownloadOperation(
                client,
                work,
                download_args(tmp),
                root,
                context,
                asmr_one.LocalFilePlan(
                    context.file,
                    context.dest,
                    context.relative_path,
                    "partial",
                    resume=True,
                ),
            )

            with self.assertRaisesRegex(
                asmr_one.RemoteFileUnavailableError,
                "no longer available",
            ):
                operation._refresh()

        self.assertIs(operation.context, context)
        client.request.assert_not_called()

    def test_refresh_can_use_path_when_original_has_no_stable_id(self) -> None:
        work = self.work(1)
        original = remote_file("track.wav", remote_id=None)
        replacement = remote_file("track.wav", remote_id="work/replacement")
        client = MagicMock()
        client.tracks.return_value = [replacement]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            contexts, _ = asmr_one.prepare_local_files(
                root,
                [original],
                asmr_one.empty_checksum_manifest(),
            )
            context = contexts[0]
            operation = asmr_one.DownloadOperation(
                client,
                work,
                download_args(tmp),
                root,
                context,
                asmr_one.LocalFilePlan(
                    context.file,
                    context.dest,
                    context.relative_path,
                    "partial",
                    resume=True,
                ),
            )

            operation._refresh()

        self.assertEqual(
            asmr_one.stable_remote_id(operation.context.file),
            "work/replacement",
        )

    def test_refresh_keeps_original_target_when_stable_id_moves(self) -> None:
        work = self.work(1)
        original_a = remote_file(
            "a.wav",
            url="https://cdn.example/original-a.wav",
            remote_id="work/a",
        )
        original_b = remote_file(
            "b.wav",
            url="https://cdn.example/original-b.wav",
            remote_id="work/b",
        )
        moved_a = remote_file(
            "b.wav",
            url="https://cdn.example/moved-a.wav",
            remote_id="work/a",
        )
        moved_b = remote_file(
            "c.wav",
            url="https://cdn.example/moved-b.wav",
            remote_id="work/b",
        )
        client = MagicMock()
        client.tracks.return_value = [moved_a, moved_b]
        client.request.return_value = (
            200,
            FakeResponse(b"AAAAA", {"Content-Length": "5"}),
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            sibling = root / "b.wav"
            sibling.write_bytes(b"BBBBB")
            contexts, _ = asmr_one.prepare_local_files(
                root,
                [original_a, original_b],
                asmr_one.empty_checksum_manifest(),
            )
            context = contexts[0]
            operation = asmr_one.DownloadOperation(
                client,
                work,
                download_args(tmp),
                root,
                context,
                asmr_one.LocalFilePlan(
                    context.file,
                    context.dest,
                    context.relative_path,
                    "missing",
                ),
            )
            operation.calls = 1

            outcome = operation()

            self.assertEqual(outcome.context.dest, root / "a.wav")
            self.assertEqual((root / "a.wav").read_bytes(), b"AAAAA")
            self.assertEqual(sibling.read_bytes(), b"BBBBB")
            self.assertFalse((root / "c.wav").exists())
            self.assertEqual(
                client.request.call_args.kwargs["raw_url"],
                "https://cdn.example/moved-a.wav",
            )

    def test_expired_signed_url_is_refreshed_before_retry(self) -> None:
        work = self.work(1)
        client = MagicMock()
        track_calls = 0

        def tracks(_work_id: int) -> list[dict[str, object]]:
            nonlocal track_calls
            track_calls += 1
            return [
                remote_file(
                    "track.wav",
                    url=f"https://cdn.example/track.wav?token={track_calls}",
                    remote_id="work/track",
                )
            ]

        urls: list[str] = []

        def request(*_args: object, **kwargs: object) -> tuple[int, FakeResponse]:
            url = str(kwargs["raw_url"])
            urls.append(url)
            if len(urls) == 1:
                raise asmr_one.DownloadAuthorizationError(403, "expired")
            return 200, FakeResponse(b"hello", {"Content-Length": "5"})

        client.tracks.side_effect = tracks
        client.request.side_effect = request
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            coordinator = asmr_one.DownloadCoordinator(
                client,
                download_args(tmp, jobs=1),
            )
            coordinator.scheduler = asmr_one.TaskScheduler(
                1,
                clock=clock,
                sleeper=clock.sleep,
            )

            summary = coordinator.run_direct([work])

        self.assertEqual((summary.ok, summary.fail), (1, 0))
        self.assertEqual(
            urls,
            [
                "https://cdn.example/track.wav?token=1",
                "https://cdn.example/track.wav?token=2",
            ],
        )
        self.assertEqual(track_calls, 2)
        self.assertEqual(clock.sleeps, [60.0])

    def test_delayed_download_does_not_block_later_work_and_refreshes_url(self) -> None:
        works = [self.work(1), self.work(2)]
        track_calls = {1: 0, 2: 0}
        client = MagicMock()

        def tracks(work_id: int) -> list[dict[str, object]]:
            track_calls[work_id] += 1
            token = track_calls[work_id]
            return [
                remote_file(
                    "track.wav",
                    url=f"https://cdn.example/{work_id}.wav?token={token}",
                    remote_id=f"work/{work_id}",
                )
            ]

        request_order: list[str] = []
        first_work_attempts = 0

        def request(*_args: object, **kwargs: object) -> tuple[int, FakeResponse]:
            nonlocal first_work_attempts
            url = str(kwargs["raw_url"])
            request_order.append(url)
            if "/1.wav" in url:
                first_work_attempts += 1
                if first_work_attempts == 1:
                    return 200, FakeResponse(b"he", {"Content-Length": "5"})
                return 206, FakeResponse(
                    b"llo",
                    {"Content-Range": "bytes 2-4/5", "Content-Length": "3"},
                )
            return 200, FakeResponse(b"hello", {"Content-Length": "5"})

        client.tracks.side_effect = tracks
        client.request.side_effect = request
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            coordinator = asmr_one.DownloadCoordinator(
                client,
                download_args(tmp, jobs=1),
            )
            coordinator.scheduler = asmr_one.TaskScheduler(
                1,
                clock=clock,
                sleeper=clock.sleep,
            )
            summary = coordinator.run_direct(works)

        self.assertEqual((summary.ok, summary.fail), (2, 0))
        self.assertEqual(
            request_order,
            [
                "https://cdn.example/1.wav?token=1",
                "https://cdn.example/2.wav?token=1",
                "https://cdn.example/1.wav?token=2",
            ],
        )
        self.assertEqual(clock.sleeps, [60.0])

    def test_single_file_works_download_concurrently(self) -> None:
        client = MagicMock()
        client.tracks.side_effect = lambda work_id: [
            remote_file(
                f"track-{work_id}.wav",
                url=f"https://cdn.example/{work_id}.wav",
                remote_id=f"work/{work_id}",
            )
        ]
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        active = 0
        maximum = 0

        def request(*_args: object, **_kwargs: object) -> tuple[int, FakeResponse]:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                barrier.wait(timeout=3)
                return 200, FakeResponse(b"hello", {"Content-Length": "5"})
            finally:
                with lock:
                    active -= 1

        client.request.side_effect = request
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client, download_args(tmp, jobs=2)
            ).run_direct([self.work(1), self.work(2)])

        self.assertEqual((summary.works, summary.ok, summary.fail), (2, 2, 0))
        self.assertEqual(maximum, 2)

    def test_work_info_tracks_and_local_checks_are_parallel_tasks(self) -> None:
        client = MagicMock()
        barriers = {
            stage: threading.Barrier(2) for stage in ("work-info", "tracks", "inspect")
        }
        lock = threading.Lock()
        active = {stage: 0 for stage in barriers}
        maximum = {stage: 0 for stage in barriers}
        worker_threads: set[int] = set()
        main_thread = threading.get_ident()
        original_inspect = asmr_one.inspect_local_file

        def run_stage(stage: str, operation: Callable[[], Any]) -> Any:
            with lock:
                active[stage] += 1
                maximum[stage] = max(maximum[stage], active[stage])
                worker_threads.add(threading.get_ident())
            try:
                barriers[stage].wait(timeout=3)
                return operation()
            finally:
                with lock:
                    active[stage] -= 1

        def work_info(code: str) -> dict[str, object]:
            number = int(code.removeprefix("RJ"))
            return run_stage("work-info", lambda: self.work(number))

        def tracks(work_id: int) -> list[dict[str, object]]:
            return run_stage(
                "tracks",
                lambda: [
                    remote_file(
                        f"track-{work_id}.wav",
                        url=f"https://cdn.example/{work_id}.wav",
                        remote_id=f"work/{work_id}",
                    )
                ],
            )

        def inspect(
            context: asmr_one.LocalFileContext, *, verify: bool
        ) -> asmr_one.FileInspection:
            return run_stage(
                "inspect", lambda: original_inspect(context, verify=verify)
            )

        client.work_info.side_effect = work_info
        client.tracks.side_effect = tracks
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch("asmr_one.inspect_local_file", side_effect=inspect),
            patch("asmr_one.log"),
        ):
            summary = asmr_one.DownloadCoordinator(
                client,
                coordinator_args(
                    tmp,
                    works=["RJ1", "RJ2"],
                    jobs=2,
                    dry_run=True,
                ),
            ).run_collection()

        self.assertEqual((summary.works, summary.fail), (2, 0))
        self.assertEqual(maximum, {"work-info": 2, "tracks": 2, "inspect": 2})
        self.assertNotIn(main_thread, worker_threads)

    def test_explicit_work_aliases_deduplicate_after_work_info(self) -> None:
        client = MagicMock()
        barrier = threading.Barrier(2)
        resolved = self.work(1)

        def work_info(_code: str) -> dict[str, object]:
            barrier.wait(timeout=3)
            return dict(resolved)

        client.work_info.side_effect = work_info
        client.tracks.return_value = []
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            coordinator = asmr_one.DownloadCoordinator(
                client,
                coordinator_args(
                    tmp,
                    works=["RJ000001", "000001"],
                    jobs=2,
                ),
            )
            summary = coordinator.run_collection()

        self.assertEqual((summary.works, summary.fail), (1, 0))
        self.assertEqual(client.work_info.call_count, 2)
        client.tracks.assert_called_once_with(1)
        self.assertEqual(len(coordinator.outcomes), 1)

    def test_limit_continues_after_resolved_aliases_reduce_the_count(self) -> None:
        client = MagicMock()
        barrier = threading.Barrier(2)

        def work_info(code: str) -> dict[str, object]:
            if code in {"RJ000001", "000001"}:
                barrier.wait(timeout=3)
                return self.work(1)
            return self.work(2)

        client.work_info.side_effect = work_info
        client.tracks.return_value = []
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            coordinator = asmr_one.DownloadCoordinator(
                client,
                coordinator_args(
                    tmp,
                    works=["RJ000001", "000001", "RJ000002"],
                    jobs=2,
                    limit=2,
                ),
            )
            summary = coordinator.run_collection()

        self.assertEqual((summary.works, summary.fail), (2, 0))
        self.assertCountEqual(
            [call.args[0] for call in client.work_info.call_args_list],
            ["RJ000001", "000001", "RJ000002"],
        )
        self.assertCountEqual(
            [call.args[0] for call in client.tracks.call_args_list],
            [1, 2],
        )

    def test_distinct_works_cannot_share_a_sanitized_output_root(self) -> None:
        works = [
            {"id": 1, "source_id": "A/B", "title": "title"},
            {"id": 2, "source_id": "A:B", "title": "title"},
        ]
        self.assertEqual(
            asmr_one.work_folder_name(works[0]),
            asmr_one.work_folder_name(works[1]),
        )
        client = MagicMock()
        client.tracks.return_value = []
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client,
                download_args(tmp, jobs=2, dry_run=True),
            ).run_direct(works)

        self.assertEqual((summary.works, summary.fail), (2, 1))
        client.tracks.assert_called_once_with(1)

    def test_explicit_rj_and_vj_codes_with_same_number_remain_distinct(self) -> None:
        client = MagicMock()

        def work_info(code: str) -> dict[str, object]:
            return {
                "id": 1 if code.upper().startswith("RJ") else 2,
                "source_id": code,
                "title": code,
            }

        client.work_info.side_effect = work_info
        client.tracks.return_value = []
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client,
                coordinator_args(
                    tmp,
                    works=["RJ000123", "VJ000123"],
                    jobs=2,
                    dry_run=True,
                ),
            ).run_collection()

        self.assertEqual((summary.works, summary.fail), (2, 0))
        self.assertCountEqual(
            [call.args[0] for call in client.work_info.call_args_list],
            ["RJ000123", "VJ000123"],
        )
        self.assertCountEqual(
            [call.args[0] for call in client.tracks.call_args_list],
            [1, 2],
        )

    def test_explicit_downloads_honor_limit(self) -> None:
        client = MagicMock()
        client.work_info.side_effect = lambda code: self.work(
            int(str(code).removeprefix("RJ"))
        )
        client.tracks.return_value = []
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client,
                coordinator_args(
                    tmp,
                    works=["RJ1", "RJ2"],
                    limit=1,
                ),
            ).run_collection()

        self.assertEqual((summary.works, summary.fail), (1, 0))
        client.work_info.assert_called_once_with("RJ1")
        client.tracks.assert_called_once_with(1)

    def test_files_in_one_work_download_concurrently_and_share_manifest(self) -> None:
        work = self.work(1)
        files = [
            remote_file(
                "a.wav",
                url="https://cdn.example/a.wav",
                remote_id="work/a",
            ),
            remote_file(
                "b.wav",
                url="https://cdn.example/b.wav",
                remote_id="work/b",
            ),
        ]
        client = MagicMock()
        client.tracks.return_value = files
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        active = 0
        maximum = 0

        def request(*_args: object, **_kwargs: object) -> tuple[int, FakeResponse]:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                barrier.wait(timeout=3)
                return 200, FakeResponse(b"hello", {"Content-Length": "5"})
            finally:
                with lock:
                    active -= 1

        client.request.side_effect = request
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client, download_args(tmp, jobs=2)
            ).run_direct([work])
            root = Path(tmp) / asmr_one.work_folder_name(work)
            manifest = asmr_one.load_checksum_manifest(root)

        self.assertEqual((summary.ok, summary.fail), (2, 0))
        self.assertEqual(maximum, 2)
        self.assertEqual(set(manifest["files"]), {"a.wav", "b.wav"})

    def test_one_file_failure_does_not_cancel_its_sibling(self) -> None:
        work = self.work(1)
        client = MagicMock()
        client.tracks.return_value = [
            remote_file(
                "bad.wav",
                url="https://cdn.example/bad.wav",
                remote_id="work/bad",
            ),
            remote_file(
                "good.wav",
                url="https://cdn.example/good.wav",
                remote_id="work/good",
            ),
        ]
        client.request.return_value = (
            200,
            FakeResponse(b"hello", {"Content-Length": "5"}),
        )
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            root = Path(tmp) / asmr_one.work_folder_name(work)
            root.mkdir()
            (root / "bad.wav").mkdir()

            summary = asmr_one.DownloadCoordinator(
                client, download_args(tmp, jobs=2)
            ).run_direct([work])

            self.assertEqual((summary.ok, summary.fail), (1, 1))
            self.assertEqual((root / "good.wav").read_bytes(), b"hello")
            self.assertEqual(client.request.call_count, 1)

    def test_verify_hash_uses_one_native_blake3_thread_per_worker(self) -> None:
        hasher = MagicMock()
        hasher.hexdigest.return_value = "0" * 64
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "track.wav"
            path.write_bytes(b"hello")
            with patch("asmr_one.blake3", return_value=hasher) as constructor:
                digest = asmr_one.checksum_file(path)

        self.assertEqual(digest, "0" * 64)
        constructor.assert_called_once_with(max_threads=1)
        hasher.update.assert_called_once_with(b"hello")

    def test_verify_hashes_are_offloaded_and_run_concurrently(self) -> None:
        works = [self.work(1), self.work(2)]
        files = {
            number: remote_file(
                "track.wav",
                url=f"https://cdn.example/{number}.wav",
                remote_id=f"work/{number}",
            )
            for number in (1, 2)
        }
        client = MagicMock()
        client.tracks.side_effect = lambda work_id: [files[work_id]]
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        main_thread = threading.get_ident()
        hash_threads: set[int] = set()
        active = 0
        maximum = 0
        original_hash = asmr_one.hash_local_file

        def hash_local_file(
            context: asmr_one.LocalFileContext,
        ) -> asmr_one.HashedFile:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                hash_threads.add(threading.get_ident())
            try:
                barrier.wait(timeout=3)
                return original_hash(context)
            finally:
                with lock:
                    active -= 1

        with tempfile.TemporaryDirectory() as tmp:
            for work in works:
                root = Path(tmp) / asmr_one.work_folder_name(work)
                root.mkdir()
                dest = root / "track.wav"
                dest.write_bytes(b"hello")
                manifest = asmr_one.empty_checksum_manifest()
                manifest["files"]["track.wav"] = asmr_one.checksum_record(
                    files[int(work["id"])], dest, asmr_one.checksum_file(dest)
                )
                asmr_one.save_checksum_manifest(root, manifest)

            with (
                patch("asmr_one.hash_local_file", side_effect=hash_local_file),
                patch("asmr_one.log"),
            ):
                summary = asmr_one.DownloadCoordinator(
                    client, download_args(tmp, jobs=2, verify=True)
                ).run_direct(works)

        self.assertEqual((summary.skip, summary.fail), (2, 0))
        self.assertEqual(maximum, 2)
        self.assertNotIn(main_thread, hash_threads)

    def test_review_pages_feed_work_tasks_before_discovery_finishes(self) -> None:
        first_work_started = threading.Event()
        client = MagicMock()

        def review_page(*, page: int, page_size: int) -> asmr_one.FetchedPage:
            self.assertEqual(page_size, 2)
            if page == 1:
                return asmr_one.FetchedPage([self.work(1)], 2, True)
            self.assertTrue(first_work_started.wait(timeout=3))
            return asmr_one.FetchedPage([self.work(2)], 2, False)

        def tracks(work_id: int) -> list[dict[str, object]]:
            if work_id == 1:
                first_work_started.set()
            return []

        client.review_page.side_effect = review_page
        client.tracks.side_effect = tracks
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client,
                coordinator_args(tmp, source="review", jobs=2),
            ).run_collection()

        self.assertEqual((summary.works, summary.fail), (2, 0))

    def test_limit_stops_queued_collection_pages(self) -> None:
        client = MagicMock()
        client.review_page.return_value = asmr_one.FetchedPage([self.work(1)], 2, True)
        client.tracks.return_value = []
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            summary = asmr_one.DownloadCoordinator(
                client,
                coordinator_args(tmp, source="review", jobs=1, limit=1),
            ).run_collection()

        self.assertEqual(summary.works, 1)
        self.assertEqual(client.review_page.call_count, 1)

    def test_playlist_content_streams_run_one_at_a_time(self) -> None:
        client = MagicMock()
        client.playlists_page.return_value = asmr_one.FetchedPage(
            [{"id": "first"}, {"id": "second"}], 1, False
        )
        first_finished = threading.Event()
        second_started = threading.Event()
        overlaps: list[bool] = []
        playlist_calls: list[str] = []

        def playlist_works_page(
            playlist_id: str, *, page: int, page_size: int
        ) -> asmr_one.FetchedPage:
            self.assertEqual((page, page_size), (1, 2))
            playlist_calls.append(playlist_id)
            if playlist_id == "first":
                self.assertFalse(first_finished.is_set())
                overlaps.append(second_started.wait(timeout=0.25))
                first_finished.set()
                return asmr_one.FetchedPage([self.work(1), self.work(2)], 1, False)
            second_started.set()
            self.assertTrue(first_finished.is_set())
            return asmr_one.FetchedPage([self.work(2), self.work(3)], 1, False)

        client.playlist_works_page.side_effect = playlist_works_page
        client.tracks.return_value = []
        with tempfile.TemporaryDirectory() as tmp, patch("asmr_one.log"):
            coordinator = asmr_one.DownloadCoordinator(
                client, coordinator_args(tmp, jobs=2)
            )
            admitted: list[int] = []
            original_admit = coordinator._admit_work

            def record_admit(
                work: dict[str, object], *, apply_limit: bool = True
            ) -> None:
                before = coordinator.admitted_works
                original_admit(work, apply_limit=apply_limit)
                if coordinator.admitted_works > before:
                    admitted.append(int(work["id"]))

            coordinator._admit_work = record_admit  # type: ignore[method-assign]
            summary = coordinator.run_collection()

        self.assertEqual(admitted, [1, 2, 3])
        self.assertEqual(playlist_calls, ["first", "second"])
        self.assertEqual(overlaps, [False])
        self.assertEqual(summary.works, 3)
        self.assertEqual(coordinator._active_playlist_streams, 0)
        self.assertFalse(coordinator._pending_playlists)
        self.assertTrue(
            all(
                isinstance(buffer, deque)
                for buffer in coordinator._playlist_buffers.values()
            )
        )

    def test_download_logs_normalize_remote_work_controls(self) -> None:
        client = MagicMock()
        client.tracks.return_value = []
        messages: list[str] = []
        work = {
            "id": 1,
            "source_id": "RJ1\nspoof",
            "title": "first\nFAIL\x1b[31m",
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch("asmr_one.log", side_effect=messages.append),
        ):
            summary = asmr_one.DownloadCoordinator(
                client,
                download_args(tmp, dry_run=True),
            ).run_direct([work])

        self.assertEqual((summary.works, summary.fail), (1, 0))
        self.assertTrue(
            any("RJ1 spoof  first FAIL [31m" in message for message in messages)
        )
        for message in messages:
            self.assertFalse(
                any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in message)
            )

    def test_download_parser_rejects_non_positive_jobs(self) -> None:
        for value in ("0", "-1"):
            with self.subTest(value=value), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    asmr_one.build_parser().parse_args(
                        ["download", "--jobs", value, "--work", "RJ1"]
                    )

    def test_parsers_reject_non_positive_page_sizes(self) -> None:
        commands = (
            ["playlists"],
            ["list", "--work", "RJ1"],
            ["download", "--work", "RJ1"],
        )
        for command in commands:
            for value in ("0", "-1"):
                with (
                    self.subTest(command=command[0], value=value),
                    redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    asmr_one.build_parser().parse_args(
                        [command[0], "--page-size", value, *command[1:]]
                    )

    def test_collection_parsers_reject_non_positive_limits(self) -> None:
        for command in ("list", "download"):
            for value in ("0", "-1"):
                with (
                    self.subTest(command=command, value=value),
                    redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    asmr_one.build_parser().parse_args(
                        [command, "--limit", value, "--work", "RJ1"]
                    )

    def test_parser_rejects_non_positive_timeout(self) -> None:
        for value in ("0", "-1"):
            with (
                self.subTest(value=value),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                asmr_one.build_parser().parse_args(
                    ["--timeout", value, "list", "--work", "RJ1"]
                )


if __name__ == "__main__":
    unittest.main()
