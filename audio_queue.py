import asyncio
import hashlib
import json
import logging
import os
import shutil
import wave
from collections import deque
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


logger = logging.getLogger(__name__)


class AudioQueue:
    """Manages audio download queue with caching and pre-roll silence."""

    def __init__(
        self,
        audio_dir: Path,
        auto_advance_buffer: float = 2.0,
        preroll_silence: float = 8.0,
        cache_mode: str = "size_lru",
        cache_max_bytes: int = 1_073_741_824,
        cache_delete_after_playback: bool = True,
        cache_delete_on_shutdown: bool = True,
        search_mode: str = "fast",
        search_timeout_seconds: float = 8.0,
        extractor_retries: int = 1,
    ):
        self.audio_dir = audio_dir
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        self.queue = deque()
        self.current = None
        self.loop_mode = False
        self.auto_advance_buffer = auto_advance_buffer
        self.preroll_silence = preroll_silence
        self.cache_mode = str(cache_mode or "size_lru").strip().lower()
        if self.cache_mode not in {"size_lru", "never_delete", "always_delete"}:
            self.cache_mode = "size_lru"
        self.cache_max_bytes = max(0, int(cache_max_bytes))
        self.cache_delete_after_playback = bool(cache_delete_after_playback)
        self.cache_delete_on_shutdown = bool(cache_delete_on_shutdown)
        self.search_mode = str(search_mode or "fast").strip().lower()
        if self.search_mode not in {"fast", "accurate"}:
            self.search_mode = "fast"
        self.search_timeout_seconds = max(0.0, float(search_timeout_seconds))
        self.extractor_retries = max(0, int(extractor_retries))

        # Cache shape: {url: {"file": str, "music_duration": Optional[float]}}
        self.download_cache = {}
        self.search_cache: dict[str, dict] = {}
        self._enforce_size_limit()

    def _in_use_files(self) -> set[str]:
        in_use: set[str] = set()
        if isinstance(self.current, dict):
            file_path = self.current.get("file")
            if isinstance(file_path, str) and file_path:
                in_use.add(file_path)
        for track in self.queue:
            if not isinstance(track, dict):
                continue
            file_path = track.get("file")
            if isinstance(file_path, str) and file_path:
                in_use.add(file_path)
        return in_use

    def _is_within_audio_dir(self, file_path: str) -> bool:
        try:
            resolved_file = Path(file_path).resolve()
            resolved_dir = self.audio_dir.resolve()
        except Exception:
            return False
        return resolved_dir == resolved_file or resolved_dir in resolved_file.parents

    def _drop_cache_entries_for_file(self, file_path: str):
        stale = [url for url, info in self.download_cache.items() if info.get("file") == file_path]
        for url in stale:
            self.download_cache.pop(url, None)

    def _delete_file(self, file_path: str) -> bool:
        if not file_path or not self._is_within_audio_dir(file_path):
            return False
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info("Deleted cached audio file: %s", file_path)
            self._drop_cache_entries_for_file(file_path)
            return True
        except Exception as exc:
            logger.warning("Failed deleting cached audio file %s: %s", file_path, exc)
            return False

    def _touch_file(self, file_path: str):
        try:
            os.utime(file_path, None)
        except Exception:
            pass

    def _enforce_size_limit(self):
        if self.cache_mode != "size_lru":
            return
        if self.cache_max_bytes <= 0:
            return

        files: list[tuple[Path, os.stat_result]] = []
        total_bytes = 0
        for path in self.audio_dir.glob("*.wav"):
            try:
                stat = path.stat()
            except OSError:
                continue
            if not path.is_file():
                continue
            files.append((path, stat))
            total_bytes += int(stat.st_size)

        if total_bytes <= self.cache_max_bytes:
            return

        in_use = self._in_use_files()
        candidates = sorted(files, key=lambda item: item[1].st_mtime)
        for path, stat in candidates:
            if total_bytes <= self.cache_max_bytes:
                break
            file_path = str(path)
            if file_path in in_use:
                continue
            if self._delete_file(file_path):
                total_bytes -= int(stat.st_size)

    def maybe_delete_track_file(self, track: Optional[dict], *, trigger: str):
        if not isinstance(track, dict):
            return

        should_delete = bool(track.get("non_cacheable"))
        if not should_delete:
            if self.cache_mode != "always_delete":
                return
            if trigger == "after_playback" and not self.cache_delete_after_playback:
                return
            if trigger == "shutdown" and not self.cache_delete_on_shutdown:
                return
            should_delete = True

        if not should_delete:
            return

        file_path = track.get("file")
        if not isinstance(file_path, str) or not file_path:
            return

        if file_path in self._in_use_files():
            return
        self._delete_file(file_path)

    def cleanup_on_shutdown(self):
        if self.cache_mode != "always_delete" or not self.cache_delete_on_shutdown:
            return

        for path in self.audio_dir.glob("*.wav"):
            self._delete_file(str(path))

    @staticmethod
    def normalize_title(title: str) -> str:
        return " ".join(title.split())

    @staticmethod
    def get_audio_duration(wav_file: str) -> Optional[float]:
        """Get duration of WAV file in seconds."""
        try:
            with wave.open(wav_file, "rb") as wav_file_handle:
                frames = wav_file_handle.getnframes()
                rate = wav_file_handle.getframerate()
                return frames / float(rate)
        except Exception as exc:
            logger.error(f"Error getting duration: {exc}")
            return None

    @staticmethod
    def add_silence_to_wav(input_file: str, output_file: str, silence_duration: float) -> bool:
        """Prepend silence to a WAV file."""
        try:
            with wave.open(input_file, "rb") as input_wav:
                params = input_wav.getparams()
                original_frames = input_wav.readframes(params.nframes)

            silence_frames_count = int(params.framerate * silence_duration)
            silence_data = b"\x00" * (silence_frames_count * params.nchannels * params.sampwidth)

            with wave.open(output_file, "wb") as output_wav:
                output_wav.setparams(params)
                output_wav.writeframes(silence_data + original_frames)

            logger.info(f"Added {silence_duration:.1f}s pre-roll silence to {output_file}")
            return True
        except Exception as exc:
            logger.error(f"Error adding silence: {exc}")
            return False

    @staticmethod
    def normalize_media_url(value: str) -> str:
        raw = (value or "").strip()
        if not raw:
            return raw

        try:
            parsed = urlparse(raw)
        except Exception:
            return raw

        host = (parsed.hostname or "").lower()
        if host != "music.youtube.com":
            return raw

        netloc = parsed.netloc
        if parsed.port:
            netloc = f"www.youtube.com:{parsed.port}"
        else:
            netloc = "www.youtube.com"

        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        rebuilt_query = urlencode(query_pairs, doseq=True)
        normalized = parsed._replace(netloc=netloc, query=rebuilt_query)
        return urlunparse(normalized)

    @staticmethod
    def looks_like_url(value: str) -> bool:
        try:
            parsed = urlparse(value)
        except Exception:
            return False
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

    @staticmethod
    async def _run_command(*cmd: str) -> tuple[int, str, str]:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return (process.returncode if process.returncode is not None else -1), stdout.decode("utf-8", errors="replace"), stderr.decode("utf-8", errors="replace")

    async def _resolve_media_info(self, dlp_cmd: str, query_or_url: str) -> tuple[bool, dict | str]:
        is_url = self.looks_like_url(query_or_url)
        target = query_or_url if is_url else f"ytsearch1:{query_or_url}"
        cmd = [dlp_cmd, "--no-playlist", "--dump-single-json", "--extractor-retries", str(self.extractor_retries)]
        if self.search_mode == "fast":
            cmd.extend(["--no-warnings", "--socket-timeout", str(max(3.0, self.search_timeout_seconds))])
        cmd.append(target)
        code, stdout, stderr = await self._run_command(*cmd)
        if code != 0:
            return False, (stderr.strip() or "Failed to resolve media info")

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError:
            return False, "Could not parse media metadata"

        entry = data
        entries = data.get("entries") if isinstance(data, dict) else None
        if isinstance(entries, list) and entries:
            for item in entries:
                if isinstance(item, dict) and item.get("webpage_url"):
                    entry = item
                    break
            else:
                if isinstance(entries[0], dict):
                    entry = entries[0]

        source_url = entry.get("webpage_url") or entry.get("original_url") or (query_or_url if is_url else None)
        if not source_url:
            return False, "No playable result found"
        source_url = self.normalize_media_url(str(source_url))

        raw_title = entry.get("title") or os.path.basename(source_url)
        title = self.normalize_title(str(raw_title))
        duration = entry.get("duration")
        uploader = entry.get("uploader") or entry.get("channel")
        stream_url = entry.get("url")
        if not isinstance(stream_url, str) or not stream_url.strip():
            stream_url = None

        out = {
            "source_url": source_url,
            "title": title,
            "duration": float(duration) if isinstance(duration, (int, float)) else None,
            "uploader": uploader,
            "stream_url": stream_url,
        }
        return True, out

    async def resolve_stream_source(self, query_or_url: str) -> tuple[bool, dict | str]:
        query_or_url = query_or_url.strip()
        if not query_or_url:
            return False, "Empty query"
        query_or_url = self.normalize_media_url(query_or_url)

        dlp_cmd = "yt-dlp" if shutil.which("yt-dlp") else "youtube-dlp"
        if not shutil.which(dlp_cmd):
            return False, "yt-dlp (or youtube-dlp) is not installed"

        target = query_or_url if self.looks_like_url(query_or_url) else f"ytsearch1:{query_or_url}"
        metadata_task = asyncio.create_task(self._resolve_media_info(dlp_cmd, query_or_url))
        stream_task = asyncio.create_task(self._resolve_direct_stream_url(dlp_cmd, target))
        (resolved_ok, resolved), (stream_ok, stream_url_or_error) = await asyncio.gather(metadata_task, stream_task)

        if not stream_ok:
            return False, stream_url_or_error

        stream_url = stream_url_or_error if isinstance(stream_url_or_error, str) else None
        if not isinstance(stream_url, str) or not stream_url:
            return False, "No stream source available"

        metadata: dict = {}
        if resolved_ok and isinstance(resolved, dict):
            metadata = resolved

        return True, {
            "stream_url": stream_url,
            "source_url": metadata.get("source_url") or query_or_url,
            "title": metadata.get("title") or os.path.basename(query_or_url) or "track",
            "duration": metadata.get("duration"),
            "uploader": metadata.get("uploader"),
        }

    async def _resolve_direct_stream_url(self, dlp_cmd: str, target: str) -> tuple[bool, str]:
        cmd = [
            dlp_cmd,
            "--no-playlist",
            "-f",
            "bestaudio",
            "--get-url",
            "--extractor-retries",
            str(self.extractor_retries),
        ]
        if self.search_mode == "fast":
            cmd.extend(["--no-warnings", "--socket-timeout", str(max(3.0, self.search_timeout_seconds))])
        cmd.append(target)
        code, stdout, stderr = await self._run_command(*cmd)
        if code != 0:
            return False, (stderr.strip() or "Failed to resolve stream URL")
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        if not lines:
            return False, "No direct stream URL returned"
        return True, lines[0]

    async def resolve_playlist_entries(self, url: str) -> tuple[bool, dict | str]:
        """Resolve playlist metadata and entry URLs for a playlist URL.

        Returns:
            (success, result)

            Success result shape:
            {
                "is_playlist": bool,
                "title": Optional[str],
                "entries": list[str],
            }
        """
        candidate = self.normalize_media_url((url or "").strip())
        if not self.looks_like_url(candidate):
            return True, {"is_playlist": False, "title": None, "entries": []}

        dlp_cmd = "yt-dlp" if shutil.which("yt-dlp") else "youtube-dlp"
        if not shutil.which(dlp_cmd):
            return False, "yt-dlp (or youtube-dlp) is not installed"

        cmd = [
            dlp_cmd,
            "--flat-playlist",
            "--dump-single-json",
            "--extractor-retries",
            str(self.extractor_retries),
        ]
        if self.search_mode == "fast":
            cmd.extend(["--no-warnings", "--lazy-playlist", "--socket-timeout", str(max(3.0, self.search_timeout_seconds))])
        cmd.append(candidate)
        code, stdout, stderr = await self._run_command(*cmd)
        if code != 0:
            return False, (stderr.strip() or "Failed to resolve playlist metadata")

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError:
            return False, "Could not parse playlist metadata"

        if not isinstance(data, dict):
            return True, {"is_playlist": False, "title": None, "entries": []}

        entries = data.get("entries")
        if not isinstance(entries, list) or not entries:
            return True, {"is_playlist": False, "title": None, "entries": []}

        urls: list[str] = []
        seen: set[str] = set()
        for item in entries:
            if not isinstance(item, dict):
                continue

            webpage_url = item.get("webpage_url")
            raw_url = webpage_url if isinstance(webpage_url, str) and webpage_url else item.get("url")
            if not isinstance(raw_url, str) or not raw_url.strip():
                continue

            normalized = self.normalize_media_url(raw_url.strip())
            if not self.looks_like_url(normalized):
                entry_id = item.get("id")
                if isinstance(entry_id, str) and entry_id:
                    normalized = f"https://www.youtube.com/watch?v={entry_id}"
                else:
                    continue

            if normalized in seen:
                continue
            seen.add(normalized)
            urls.append(normalized)

        title_value = data.get("title")
        title = self.normalize_title(str(title_value)) if isinstance(title_value, str) else None
        return True, {"is_playlist": True, "title": title, "entries": urls}

    async def download_audio(self, query_or_url: str) -> tuple[bool, dict | str]:
        """Download audio from URL/search with caching and pre-roll silence.

        Returns:
            (success, result)

            Success result shape:
            {
                "file": str,
                "duration": Optional[float],
                "title": str,
                "source_url": str,
                "uploader": Optional[str],
            }
        """
        query_or_url = query_or_url.strip()
        if not query_or_url:
            return False, "Empty query"
        query_or_url = self.normalize_media_url(query_or_url)

        dlp_cmd = "yt-dlp" if shutil.which("yt-dlp") else "youtube-dlp"
        if not shutil.which(dlp_cmd):
            return False, "yt-dlp (or youtube-dlp) is not installed"

        query_key = query_or_url.casefold()
        is_direct_url = self.looks_like_url(query_or_url)
        resolved: dict | str
        if self.search_mode == "fast" and not is_direct_url and query_key in self.search_cache:
            resolved_ok = True
            resolved = dict(self.search_cache[query_key])
        else:
            resolved_ok, resolved = await self._resolve_media_info(dlp_cmd, query_or_url)
        if not resolved_ok:
            return False, resolved
        if not isinstance(resolved, dict):
            return False, "Resolved metadata is invalid"

        if self.search_mode == "fast" and not is_direct_url:
            self.search_cache[query_key] = dict(resolved)

        source_url = resolved["source_url"]

        if source_url in self.download_cache:
            cached = self.download_cache[source_url]
            cached_file = cached.get("file")
            if isinstance(cached_file, str) and os.path.exists(cached_file):
                self._touch_file(cached_file)
            else:
                self.download_cache.pop(source_url, None)
                cached = None
        else:
            cached = None

        if cached:
            non_cacheable = False
            if self.cache_mode == "size_lru" and self.cache_max_bytes > 0:
                try:
                    non_cacheable = os.path.getsize(cached["file"]) > self.cache_max_bytes
                except OSError:
                    non_cacheable = False
            logger.info(f"Using cached file: {cached['file']}")
            return True, {
                "file": cached["file"],
                "duration": cached["music_duration"],
                "title": resolved["title"],
                "source_url": source_url,
                "uploader": resolved["uploader"],
                "non_cacheable": non_cacheable,
            }

        url_hash = hashlib.md5(source_url.encode(), usedforsecurity=False).hexdigest()[:8]
        temp_output = str(self.audio_dir / f"{url_hash}_temp.%(ext)s")
        temp_wav_file = self.audio_dir / f"{url_hash}_temp.wav"
        final_output = str(self.audio_dir / f"{url_hash}.wav")

        cmd = [
            dlp_cmd,
            "-x",
            "--audio-format",
            "wav",
            "--audio-quality",
            "0",
            "--extractor-retries",
            str(self.extractor_retries),
            "-o",
            temp_output,
            source_url,
        ]
        if self.search_mode == "fast":
            cmd[1:1] = ["--no-warnings", "--socket-timeout", str(max(3.0, self.search_timeout_seconds))]

        try:
            process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode().strip() if stderr else "Unknown error"
                logger.error(f"yt-dlp failed: {error_msg}")
                return False, error_msg

            if not temp_wav_file.exists():
                return False, "File not found after download"

            music_duration = self.get_audio_duration(str(temp_wav_file))

            if self.preroll_silence > 0:
                silence_ok = self.add_silence_to_wav(str(temp_wav_file), final_output, self.preroll_silence)
                if not silence_ok:
                    logger.warning("Failed to add pre-roll silence, using original file")
                    temp_wav_file.rename(final_output)
            else:
                temp_wav_file.rename(final_output)

            if temp_wav_file.exists():
                temp_wav_file.unlink()

            non_cacheable = False
            if self.cache_mode == "size_lru" and self.cache_max_bytes > 0:
                try:
                    non_cacheable = os.path.getsize(final_output) > self.cache_max_bytes
                except OSError:
                    non_cacheable = False

            if non_cacheable:
                logger.info(
                    "Downloaded file exceeds cache_max_bytes; will delete after playback: %s",
                    final_output,
                )
            else:
                self.download_cache[source_url] = {"file": final_output, "music_duration": music_duration}

            self._touch_file(final_output)
            self._enforce_size_limit()

            if music_duration is not None:
                logger.info(
                    f"Downloaded: {final_output} "
                    f"(music: {music_duration:.2f}s + pre-roll: {self.preroll_silence:.2f}s)"
                )
            else:
                logger.info(
                    f"Downloaded: {final_output} "
                    f"(music duration unknown + pre-roll: {self.preroll_silence:.2f}s)"
                )

            return True, {
                "file": final_output,
                "duration": music_duration,
                "title": resolved["title"],
                "source_url": source_url,
                "uploader": resolved["uploader"],
                "non_cacheable": non_cacheable,
            }
        except Exception as exc:
            logger.error(f"Download exception: {exc}")
            return False, str(exc)

    def add_to_queue(
        self,
        audio_file: str,
        title: Optional[str] = None,
        duration: Optional[float] = None,
        source_url: Optional[str] = None,
        non_cacheable: bool = False,
    ):
        """Add a track to queue."""
        track = {
            "file": audio_file,
            "title": title or os.path.basename(audio_file),
            "duration": duration,
            "source_url": source_url,
            "non_cacheable": bool(non_cacheable),
        }
        self.queue.append(track)
        if duration is not None:
            logger.info(f"Added to queue: {track['title']} (duration: {duration:.2f}s)")
        else:
            logger.info(f"Added to queue: {track['title']}")

    def get_next(self) -> Optional[dict]:
        """Pop next track from queue and set current."""
        if not self.queue:
            self.current = None
            return None
        self.current = self.queue.popleft()
        return self.current

    def get_current_or_next(self) -> Optional[dict]:
        """Return current track in loop mode, otherwise pop next."""
        if self.loop_mode and self.current:
            return self.current
        return self.get_next()

    def clear_queue(self):
        self.queue.clear()
        logger.info("Queue cleared")

    def has_source(self, source_url: str) -> bool:
        if not source_url:
            return False

        if self.current and self.current.get("source_url") == source_url:
            return True

        return any(item.get("source_url") == source_url for item in self.queue)

    def toggle_loop(self) -> bool:
        self.loop_mode = not self.loop_mode
        return self.loop_mode
