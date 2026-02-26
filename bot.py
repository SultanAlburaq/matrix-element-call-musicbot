import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
import logging
from pathlib import Path
import shutil
import subprocess
import time
from typing import Awaitable, Callable, Optional

from nio import AsyncClient, InviteMemberEvent, MatrixRoom, RoomMessageText

from config import Config
from audio_queue import AudioQueue
from call_worker_process import CallWorkerProcess
from saved_queues import SavedQueueStore


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OutboundMessage:
    room_id: str
    text: str
    html_body: Optional[str]
    priority: str


class IntegratedBot:
    """Matrix music bot core (commands, queue, call join, audio publish)."""

    def __init__(self, config: Config):
        self.config = config
        self.client = AsyncClient(config.MATRIX_HOMESERVER, config.MATRIX_USER_ID)
        self.client.access_token = config.MATRIX_ACCESS_TOKEN
        self.first_sync_done = False

        self.audio_queue = AudioQueue(
            config.AUDIO_DIR,
            config.AUTO_ADVANCE_BUFFER,
            config.PREROLL_SILENCE,
            cache_mode=config.AUDIO_CACHE_MODE,
            cache_max_bytes=config.AUDIO_CACHE_MAX_BYTES,
            cache_delete_after_playback=config.AUDIO_CACHE_DELETE_AFTER_PLAYBACK,
            cache_delete_on_shutdown=config.AUDIO_CACHE_DELETE_ON_SHUTDOWN,
            search_mode=config.SEARCH_MODE,
            search_timeout_seconds=config.SEARCH_TIMEOUT_SECONDS,
            extractor_retries=config.EXTRACTOR_RETRIES,
        )

        self._auto_advance_task: Optional[asyncio.Task] = None
        self._advance_watchdog_task: Optional[asyncio.Task] = None
        self._worker_playback_task: Optional[asyncio.Task] = None
        self._background_load_task: Optional[asyncio.Task] = None
        self._stream_prefetch_task: Optional[asyncio.Task] = None
        self._current_room_id: Optional[str] = None
        self._current_track_started_at: Optional[float] = None
        self._playback_generation = 0
        self._playback_lock = asyncio.Lock()
        self._play_request_lock = asyncio.Lock()
        self._last_skip_at = 0.0
        self._skip_cooldown_seconds = config.SKIP_COOLDOWN_SECONDS
        self._restart_failed_notified = False
        self._startup_warnings: list[str] = []
        self._tool_versions: dict[str, str] = {}
        self._play_history_by_room: dict[str, deque[dict]] = defaultdict(
            lambda: deque(maxlen=max(1, self.config.HISTORY_LIMIT))
        )
        self._message_queue: asyncio.PriorityQueue[tuple[int, int, OutboundMessage]] = asyncio.PriorityQueue()
        self._message_dispatch_task: Optional[asyncio.Task] = None
        self._message_seq = 0
        self._noisy_cooldown_until: dict[tuple[str, str], float] = {}
        self._message_priority_map = {"critical": 0, "normal": 1, "noisy": 2}
        self.call_worker = CallWorkerProcess(
            Path(__file__).resolve().parent,
            max_restart_attempts=config.WORKER_MAX_RESTART_ATTEMPTS,
            heartbeat_interval=config.WORKER_HEARTBEAT_INTERVAL,
            stop_timeout_restart_threshold=config.WORKER_STOP_TIMEOUT_RESTART_THRESHOLD,
            env_overrides={
                "MATRIX_HOMESERVER": config.MATRIX_HOMESERVER or "",
                "MATRIX_USER_ID": config.MATRIX_USER_ID or "",
                "MATRIX_ACCESS_TOKEN": config.MATRIX_ACCESS_TOKEN or "",
                "NORMALIZE_AUDIO": "true" if config.NORMALIZE_AUDIO else "false",
                "FADE_IN_MS": str(config.FADE_IN_MS),
                "VOLUME_PERCENT": str(config.VOLUME_PERCENT),
                "WORKER_LOG_MAX_BYTES": str(config.WORKER_LOG_MAX_BYTES),
                "WORKER_LOG_BACKUPS": str(config.WORKER_LOG_BACKUPS),
                "WORKER_MEMBERSHIP_MODE": config.WORKER_MEMBERSHIP_MODE,
            },
        )
        self.call_worker.set_event_handler(self._on_call_worker_event)
        self.saved_queues = SavedQueueStore(config.SAVED_QUEUES_FILE)

        self.client.add_event_callback(self.on_message, RoomMessageText)
        self.client.add_event_callback(self.on_invite, InviteMemberEvent)

        self._command_handlers: dict[str, Callable[[MatrixRoom, str], Awaitable[None]]] = {}
        self._register_command_handlers()

    def _cancel_auto_advance(self):
        if self._auto_advance_task and not self._auto_advance_task.done():
            self._auto_advance_task.cancel()
            logger.info("Cancelled auto-advance timer")
        self._auto_advance_task = None

    def _start_message_dispatcher(self):
        if self._message_dispatch_task and not self._message_dispatch_task.done():
            return
        self._message_dispatch_task = asyncio.create_task(self._message_dispatch_loop())

    async def _stop_message_dispatcher(self):
        task = self._message_dispatch_task
        if not task:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self._message_dispatch_task = None

    def _should_emit_message(self, text: str, priority: str) -> bool:
        if priority == "critical":
            return True
        if not self.config.QUIET_MODE:
            return True
        if priority == "noisy":
            return False
        return True

    async def _queue_message(self, room_id: str, text: str, *, html_body: Optional[str], priority: str):
        if not self._should_emit_message(text, priority):
            return
        if priority == "noisy":
            key = (room_id, text)
            now = asyncio.get_running_loop().time()
            cool_until = self._noisy_cooldown_until.get(key)
            if cool_until is not None and now < cool_until:
                return
            self._noisy_cooldown_until[key] = now + 2.5
        self._message_seq += 1
        msg = OutboundMessage(room_id=room_id, text=text, html_body=html_body, priority=priority)
        await self._message_queue.put((self._message_priority_map[priority], self._message_seq, msg))

    async def _message_dispatch_loop(self):
        try:
            while True:
                _, _, msg = await self._message_queue.get()
                try:
                    await self._send_message_now(msg.room_id, msg.text, html_body=msg.html_body)
                except Exception as exc:
                    logger.error("Error sending message: %s", exc)
                finally:
                    self._message_queue.task_done()
        except asyncio.CancelledError:
            raise

    async def _send_message_now(self, room_id: str, text: str, *, html_body: Optional[str] = None):
        content = {"msgtype": "m.text", "body": text}
        if self.config.RICH_FORMATTING and html_body:
            content["format"] = "org.matrix.custom.html"
            content["formatted_body"] = html_body
        await self.client.room_send(room_id, message_type="m.room.message", content=content)

    def _ensure_advance_watchdog(self):
        if self._advance_watchdog_task and not self._advance_watchdog_task.done():
            return
        self._advance_watchdog_task = asyncio.create_task(self._advance_watchdog_loop())

    def _cancel_advance_watchdog(self):
        if self._advance_watchdog_task and not self._advance_watchdog_task.done():
            self._advance_watchdog_task.cancel()
            logger.info("Cancelled advance watchdog")
        self._advance_watchdog_task = None

    async def _advance_watchdog_loop(self):
        try:
            while True:
                await asyncio.sleep(4.0)

                if self.audio_queue.loop_mode:
                    continue
                if not self.audio_queue.current or not self.audio_queue.queue:
                    continue
                if self._current_track_started_at is None:
                    continue

                duration = self.audio_queue.current.get("duration")
                if duration is None:
                    continue

                elapsed = asyncio.get_running_loop().time() - self._current_track_started_at
                threshold = float(duration) + float(self.audio_queue.auto_advance_buffer) + 6.0
                if elapsed < threshold:
                    continue

                room_id = self._current_room_id
                if not room_id:
                    continue
                if not self._is_joined_in_room_call(room_id):
                    continue

                logger.warning(
                    "Advance watchdog forcing next track after %.2fs (threshold %.2fs)",
                    elapsed,
                    threshold,
                )
                async with self._playback_lock:
                    if self.audio_queue.current and self.audio_queue.queue:
                        await self._advance_queue(room_id, force_next=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Advance watchdog error: %s", exc)

    @staticmethod
    def _format_duration(seconds: Optional[float]) -> str:
        if seconds is None:
            return "unknown"
        total = int(round(max(0.0, seconds)))
        hours, rem = divmod(total, 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours}h {minutes:02d}m"
        if minutes > 0:
            return f"{minutes}m {secs:02d}s"
        return f"{secs}s"

    @classmethod
    def _format_track_line(cls, prefix: str, track: dict) -> str:
        duration = track.get("duration")
        duration_str = f" [{cls._format_duration(duration)}]" if duration is not None else ""
        return f"{prefix}{track['title']}{duration_str}"

    @staticmethod
    def _format_bytes(num_bytes: int) -> str:
        size = float(max(0, int(num_bytes)))
        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        while size >= 1024.0 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        return f"{size:.1f} {units[unit_index]}"

    def _next_track_title(self) -> Optional[str]:
        if not self.audio_queue.queue:
            return None
        return self.audio_queue.queue[0]["title"]

    @staticmethod
    def _sum_known_durations(tracks: list[dict]) -> Optional[float]:
        total = 0.0
        for track in tracks:
            duration = track.get("duration") if isinstance(track, dict) else None
            if duration is None:
                return None
            total += float(duration)
        return total

    def _current_track_remaining_seconds(self) -> Optional[float]:
        if not self.audio_queue.current:
            return None

        duration = self.audio_queue.current.get("duration")
        if duration is None:
            return None

        if self._current_track_started_at is None:
            return None

        now = asyncio.get_running_loop().time()
        elapsed = max(0.0, now - self._current_track_started_at)
        return max(0.0, float(duration) - elapsed)

    def _help_text(self) -> str:
        return (
            "🎵 Commands\n\n"
            "Playback\n"
            "!help (!h) - show this help\n"
            "!join (!j) - join Element Call in this room\n"
            "!leave (!lv) - leave current Element Call\n"
            "!play (!p) <url-or-query> - add track and auto-join call if needed\n"
            "!queue (!q) - show queue with ETA\n"
            "!nowplaying (!np) - show current track\n"
            "!skip (!s) - skip current track\n"
            "!stop (!x) - stop playback and clear queue\n"
            "!loop (!lp) - toggle loop mode\n"
            "!history (!hist) - show recent playback history\n\n"
            "Saved Queues\n"
            "!save (!sv) <name> [--force] - save current+upcoming queue\n"
            "!load (!ld) <name> - load a saved queue and auto-join call if needed\n"
            "!queues (!qs) - list saved queues\n"
            "!deletequeue (!dq) <name> - delete a saved queue\n"
            "!renamequeue (!rq) <old> <new> - rename a saved queue\n\n"
            "Audio & Info\n"
            "!audio (!a) - show current audio settings\n"
            "!normalize (!norm) on|off - toggle normalization\n"
            "!fadein (!fi) <ms> - set fade-in (0-5000)\n"
            "!volume (!v) <0-200> - set playback volume percent\n"
            "!status (!st) - show bot status\n"
            "!diag (!d) - show diagnostics\n"
            "!config (!cfg) - show active config\n"
            "!defaults (!df) - show default config values"
        )

    def _safe_config_text(self) -> str:
        lines = [
            "⚙️ Active config",
            f"Config file: {self.config.config_file}",
            f"Audio dir: {self.config.AUDIO_DIR}",
            f"Saved queues: {self.config.SAVED_QUEUES_FILE}",
            f"Pre-roll silence: {self.config.PREROLL_SILENCE:.1f}s",
            f"Auto-advance buffer: {self.config.AUTO_ADVANCE_BUFFER:.1f}s",
            f"Normalize audio: {'On' if self.config.NORMALIZE_AUDIO else 'Off'}",
            f"Fade-in: {self.config.FADE_IN_MS}ms",
            f"Volume: {self.config.VOLUME_PERCENT}%",
            f"Cache mode: {self.config.AUDIO_CACHE_MODE}",
            f"Cache max size: {self._format_bytes(self.config.AUDIO_CACHE_MAX_BYTES)}",
            f"Delete after playback: {'On' if self.config.AUDIO_CACHE_DELETE_AFTER_PLAYBACK else 'Off'}",
            f"Delete on shutdown: {'On' if self.config.AUDIO_CACHE_DELETE_ON_SHUTDOWN else 'Off'}",
            f"Search mode: {self.config.SEARCH_MODE}",
            f"Search timeout: {self.config.SEARCH_TIMEOUT_SECONDS:.1f}s",
            f"Extractor retries: {self.config.EXTRACTOR_RETRIES}",
            f"Stream first when idle: {'On' if self.config.STREAM_FIRST_IDLE else 'Off'}",
            f"Stream prefetch current: {'On' if self.config.STREAM_PREFETCH_CURRENT else 'Off'}",
            (
                "Stream retry to file on fail: "
                f"{'On' if self.config.STREAM_RETRY_TO_FILE_ON_FAIL else 'Off'}"
            ),
            f"Skip cooldown: {self.config.SKIP_COOLDOWN_SECONDS:.1f}s",
            f"Worker restarts: {self.config.WORKER_MAX_RESTART_ATTEMPTS}",
            f"Worker heartbeat: {self.config.WORKER_HEARTBEAT_INTERVAL:.1f}s",
            f"Stop-timeout recovery: {self.config.WORKER_STOP_TIMEOUT_RESTART_THRESHOLD}",
            f"Worker membership mode: {self.config.WORKER_MEMBERSHIP_MODE}",
            f"Playlist max tracks/request: {self.config.PLAYLIST_MAX_TRACKS_PER_REQUEST}",
            f"Playlist background concurrency: {self.config.PLAYLIST_BACKGROUND_LOAD_CONCURRENCY}",
            f"History limit: {self.config.HISTORY_LIMIT}",
            f"Auto-accept invites: {'On' if self.config.AUTO_ACCEPT_INVITES else 'Off'}",
            f"Progress messages: {'On' if self.config.SHOW_PROGRESS_MESSAGES else 'Off'}",
            f"Quiet mode: {'On' if self.config.QUIET_MODE else 'Off'}",
            f"Log file: {self.config.LOG_FILE}",
            f"Clean log: {self.config.CLEAN_LOG_FILE if self.config.CLEAN_LOG_ENABLED else 'Off'}",
        ]
        if self.config.AUDIO_CACHE_MAX_BYTES_CLAMPED:
            lines.append(
                "Cache max was below 200MB, clamped to minimum 200MB for size_lru stability"
            )
        return "\n".join(lines)

    def _default_config_text(self) -> str:
        return Config.defaults_text()

    async def _apply_audio_settings_to_worker(self):
        if not self.call_worker.running:
            return
        await self.call_worker.set_audio_settings(
            normalize_audio=self.config.NORMALIZE_AUDIO,
            fade_in_ms=self.config.FADE_IN_MS,
            volume_percent=self.config.VOLUME_PERCENT,
        )

    @staticmethod
    def _track_source_ref(track: Optional[dict]) -> Optional[str]:
        if not isinstance(track, dict):
            return None
        active_source = track.get("active_source")
        if isinstance(active_source, str) and active_source:
            return active_source
        file_path = track.get("file")
        if isinstance(file_path, str) and file_path:
            return file_path
        stream_url = track.get("stream_url")
        if isinstance(stream_url, str) and stream_url:
            return stream_url
        return None

    async def _play_track_in_worker(self, track: dict, *, pre_stop: bool = True) -> tuple[int, Optional[str]]:
        should_wait_stop = pre_stop and self.call_worker.state == "playing"
        if should_wait_stop:
            await self.call_worker.stop_playback(wait_for_terminal=True, timeout=3.0)
        file_path = track.get("file")
        stream_url = track.get("stream_url")
        title = track.get("title")
        selected_source: Optional[str] = None
        if isinstance(file_path, str) and file_path:
            await self.call_worker.play(file_path, title)
            selected_source = file_path
        elif isinstance(stream_url, str) and stream_url:
            await self.call_worker.play_stream(stream_url, title)
            selected_source = stream_url
        else:
            raise RuntimeError("Track has neither file nor stream source")
        track["active_source"] = selected_source
        self._playback_generation += 1
        generation = self._playback_generation
        return generation, self._track_source_ref(track)

    def _cancel_stream_prefetch(self):
        if self._stream_prefetch_task and not self._stream_prefetch_task.done():
            self._stream_prefetch_task.cancel()
        self._stream_prefetch_task = None

    async def _prefetch_current_track_file(self, room_id: str, source_url: str, expected_generation: int):
        try:
            success, result = await self.audio_queue.download_audio(source_url)
        except asyncio.CancelledError:
            return
        if not success or not isinstance(result, dict):
            return

        replay_needed = False
        async with self._playback_lock:
            if expected_generation != self._playback_generation:
                return
            current = self.audio_queue.current
            if not isinstance(current, dict):
                return
            if current.get("source_url") != source_url:
                return
            current["file"] = result.get("file")
            current["non_cacheable"] = bool(result.get("non_cacheable"))
            replay_needed = self.audio_queue.loop_mode and self.call_worker.state != "playing"

        if replay_needed:
            try:
                await self._advance_queue(room_id, force_next=False, pre_stop=False)
            except Exception:
                logger.exception("Failed to resume loop playback after stream prefetch")

    async def _retry_stream_track_as_file(self, room_id: str, track: dict) -> bool:
        source_url = track.get("source_url") if isinstance(track, dict) else None
        if not isinstance(source_url, str) or not source_url:
            return False
        success, result = await self.audio_queue.download_audio(source_url)
        if not success or not isinstance(result, dict):
            return False

        track["file"] = result.get("file")
        track["non_cacheable"] = bool(result.get("non_cacheable"))
        try:
            generation, source_ref = await self._play_track_in_worker(track, pre_stop=False)
        except Exception:
            return False

        self._current_track_started_at = asyncio.get_running_loop().time()
        self._worker_playback_task = asyncio.create_task(
            self._wait_for_worker_playback(room_id, generation, source_ref)
        )
        self._arm_auto_advance(
            track.get("duration"),
            room_id,
            expected_generation=generation,
            expected_source=source_ref,
        )
        return True

    def _should_stream_first_idle(self) -> bool:
        if not self.config.STREAM_FIRST_IDLE:
            return False
        if self.audio_queue.current is not None:
            return False
        if self.audio_queue.queue:
            return False
        if self.call_worker.state == "playing":
            return False
        return True

    async def _try_stream_first_idle_play(self, room_id: str, args: str) -> bool:
        if not self._should_stream_first_idle():
            return False

        join_task = asyncio.create_task(self._join_call_for_room(room_id))
        resolve_task = asyncio.create_task(self.audio_queue.resolve_stream_source(args))
        join_ok = False
        try:
            join_ok, resolved = await asyncio.gather(join_task, resolve_task)
        except Exception as exc:
            logger.warning("Stream-first resolve failed, falling back to file download: %s", exc)
            return False

        if not join_ok:
            return False
        if not isinstance(resolved, tuple) or len(resolved) != 2:
            return False
        success, result = resolved
        if not success:
            return False
        if not isinstance(result, dict):
            return False

        source_url = result.get("source_url")
        if isinstance(source_url, str) and self.audio_queue.has_source(source_url):
            await self.send_message(
                room_id,
                "ℹ️ That track is already playing or queued. Use `!loop` if you want repeats.",
            )
            return True

        title = result.get("title") or "track"
        duration = result.get("duration")

        track = {
            "title": title,
            "duration": duration,
            "source_url": source_url,
            "stream_url": result.get("stream_url"),
            "non_cacheable": True,
        }

        previous_current = self.audio_queue.current
        previous_room_id = self._current_room_id
        previous_started_at = self._current_track_started_at
        self._cancel_stream_prefetch()
        generation = 0
        source_ref = None
        try:
            async with self._playback_lock:
                self.audio_queue.current = track
                self._current_room_id = room_id
                generation, source_ref = await self._play_track_in_worker(track, pre_stop=False)
                self._current_track_started_at = asyncio.get_running_loop().time()
                self._push_play_history(room_id, track)
                self._worker_playback_task = asyncio.create_task(
                    self._wait_for_worker_playback(room_id, generation, source_ref)
                )
                self._arm_auto_advance(
                    track.get("duration"),
                    room_id,
                    expected_generation=generation,
                    expected_source=source_ref,
                )
        except Exception as exc:
            logger.warning("Stream-first playback start failed, falling back to file download: %s", exc)
            async with self._playback_lock:
                if self.audio_queue.current is track:
                    self.audio_queue.current = previous_current
                    self._current_room_id = previous_room_id
                    self._current_track_started_at = previous_started_at
            return False

        await self.send_message(room_id, f"▶️ Now playing: {title}")

        if self.config.STREAM_PREFETCH_CURRENT and isinstance(source_url, str) and source_url:
            self._stream_prefetch_task = asyncio.create_task(
                self._prefetch_current_track_file(room_id, source_url, generation)
            )
        return True

    def _arm_auto_advance(
        self,
        duration: Optional[float],
        room_id: str,
        *,
        expected_generation: Optional[int] = None,
        expected_source: Optional[str] = None,
    ):
        if self.audio_queue.loop_mode:
            logger.info("Loop mode enabled - auto-advance disabled")
            return
        if duration is None:
            logger.warning("No duration available - auto-advance disabled for this track")
            return
        self._cancel_auto_advance()
        generation = self._playback_generation if expected_generation is None else expected_generation
        self._auto_advance_task = asyncio.create_task(
            self._auto_advance_timer(float(duration), room_id, generation, expected_source)
        )

    async def _sync_current_track_to_worker(self, room_id: str, announce: bool = True) -> bool:
        if not self.audio_queue.current:
            return False
        if self._current_room_id != room_id:
            return False
        if not self._is_joined_in_room_call(room_id):
            return False

        try:
            self._cancel_worker_playback_wait()
            await self.call_worker.stop_playback(wait_for_terminal=False)
            generation, source_ref = await self._play_track_in_worker(self.audio_queue.current, pre_stop=False)
            self._current_track_started_at = asyncio.get_running_loop().time()
            self._worker_playback_task = asyncio.create_task(
                self._wait_for_worker_playback(room_id, generation, source_ref)
            )
            self._arm_auto_advance(
                self.audio_queue.current.get("duration"),
                room_id,
                expected_generation=generation,
                expected_source=source_ref,
            )
            if announce:
                await self.send_message(room_id, f"▶️ Synced current track to call: {self.audio_queue.current['title']}")
            return True
        except Exception as exc:
            self._current_track_started_at = None
            logger.error("Failed to sync current track after join/recovery: %s", exc)
            if announce:
                await self.send_message(room_id, f"⚠️ Joined call, but could not sync current track: {exc}")
            return False

    def _push_play_history(self, room_id: str, track: dict):
        if not room_id or not isinstance(track, dict):
            return
        title = track.get("title")
        if not isinstance(title, str) or not title:
            return
        self._play_history_by_room[room_id].appendleft(
            {
                "title": title,
                "duration": track.get("duration"),
                "at": int(time.time()),
            }
        )

    async def _join_call_for_room(self, room_id: str, *, announce_if_already_joined: bool = False) -> bool:
        if self._is_joined_in_room_call(room_id):
            if announce_if_already_joined:
                await self.send_message(room_id, "ℹ️ Already joined this room call")
            return True

        if not self.call_worker.available:
            await self.send_message(
                room_id,
                "❌ Call worker not found. Expected file: call_worker/src/join_call.js",
            )
            return False

        try:
            await self.call_worker.start(room_id)
        except Exception as exc:
            await self.send_message(room_id, f"❌ Failed to join Element Call: {exc}")
            return False

        await self.send_message(room_id, "✅ Joined Element Call")
        await self._apply_audio_settings_to_worker()

        if self.audio_queue.current and self._current_room_id == room_id:
            await self._sync_current_track_to_worker(room_id, announce=True)

        return True

    def _is_joined_in_room_call(self, room_id: str) -> bool:
        return self.call_worker.running and self.call_worker.room_id == room_id

    async def _require_joined_in_room_call(self, room_id: str, command_name: str) -> bool:
        if self._is_joined_in_room_call(room_id):
            return True
        await self.send_message(
            room_id,
            f"ℹ️ Join call first with !join before {command_name}.",
        )
        return False

    def _cancel_worker_playback_wait(self):
        if self._worker_playback_task and not self._worker_playback_task.done():
            self._worker_playback_task.cancel()
            logger.info("Cancelled worker playback watcher")
        self._worker_playback_task = None

    def _cancel_background_load(self):
        if self._background_load_task and not self._background_load_task.done():
            self._background_load_task.cancel()
            logger.info("Cancelled background queue loader")
        self._background_load_task = None
        self._cancel_stream_prefetch()

    async def _wait_for_worker_playback(
        self,
        room_id: str,
        expected_generation: int,
        expected_source: Optional[str],
    ):
        try:
            event = await self.call_worker.wait_for_playback_terminal()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Call worker playback error: {exc}")
            if "InvalidState - failed to capture frame" not in str(exc):
                await self.send_message(room_id, f"❌ Playback worker error: {exc}")
            async with self._playback_lock:
                if expected_generation != self._playback_generation:
                    logger.info("Ignoring playback recovery (generation changed)")
                    return
                current_source = self._track_source_ref(self.audio_queue.current)
                if expected_source and current_source and current_source != expected_source:
                    logger.info("Ignoring playback recovery (track changed)")
                    return

                self._cancel_auto_advance()
                self._current_track_started_at = None

                if self.audio_queue.queue:
                    await self._advance_queue(room_id, force_next=True, pre_stop=False)
                    return

                if self.audio_queue.current is not None:
                    self.audio_queue.current = None
            return

        if expected_generation != self._playback_generation:
            logger.info("Ignoring stale worker terminal event (generation changed)")
            return

        event_source = event.get("source") or event.get("file") or event.get("url")
        current_source = self._track_source_ref(self.audio_queue.current)
        if expected_source and current_source and event_source and event_source != expected_source:
            logger.info("Ignoring stale worker terminal event (track changed)")
            return

        event_name = event.get("event")
        if event_name != "play_ended":
            return

        async with self._playback_lock:
            await self._advance_queue(room_id, force_next=not self.audio_queue.loop_mode)

    async def _download_first_available_source(self, sources: list[str]) -> tuple[Optional[dict], int, int]:
        first_item: Optional[dict] = None
        first_index = -1
        failures = 0
        for index, source_url in enumerate(sources):
            success, result = await self.audio_queue.download_audio(source_url)
            if success and isinstance(result, dict):
                first_item = result
                first_index = index
                break
            failures += 1
        return first_item, first_index, failures

    @staticmethod
    def _parse_int_arg(raw: str) -> Optional[int]:
        try:
            return int(raw)
        except ValueError:
            return None

    async def _load_remaining_tracks(
        self,
        room_id: str,
        collection_name: str,
        remaining_sources: list[str],
        *,
        dedupe_existing: bool,
        source_label: str,
    ):
        if not remaining_sources:
            return

        semaphore = asyncio.Semaphore(self.config.PLAYLIST_BACKGROUND_LOAD_CONCURRENCY)

        async def load_one(index: int, source_url: str):
            async with semaphore:
                ok, result = await self.audio_queue.download_audio(source_url)
                return index, ok, result

        try:
            tasks = [asyncio.create_task(load_one(idx, src)) for idx, src in enumerate(remaining_sources)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            return

        loaded: list[dict] = []
        failures = 0
        ordered: list[tuple[int, bool, object]] = []
        for item in results:
            if isinstance(item, Exception):
                failures += 1
                continue
            if not isinstance(item, tuple) or len(item) != 3:
                failures += 1
                continue
            ordered.append(item)

        ordered.sort(key=lambda x: x[0])
        for _, ok, result in ordered:
            if not ok or not isinstance(result, dict):
                failures += 1
                continue
            loaded.append(result)

        if not loaded and failures == 0:
            return

        added = 0
        skipped_existing = 0
        async with self._playback_lock:
            for track in loaded:
                source_url = track.get("source_url")
                if dedupe_existing and isinstance(source_url, str) and source_url and self.audio_queue.has_source(source_url):
                    skipped_existing += 1
                    continue
                self.audio_queue.add_to_queue(
                    track["file"],
                    track.get("title"),
                    track.get("duration"),
                    source_url=source_url,
                    non_cacheable=bool(track.get("non_cacheable")),
                )
                added += 1

        if added:
            message = f"✅ Added {added} more from {source_label} `{collection_name}`"
            if skipped_existing:
                message += f"\nSkipped already queued: {skipped_existing}"
            if failures:
                message += f"\nFailed: {failures}"
            await self.send_message(room_id, message)
        elif failures:
            await self.send_message(room_id, f"⚠️ Failed to load {failures} from {source_label} `{collection_name}`")

    async def _on_call_worker_event(self, event: dict):
        event_name = event.get("event")
        room_id = event.get("roomId") or self._current_room_id or self.call_worker.room_id or self.call_worker.desired_room_id
        if not room_id:
            return

        if event_name == "compatibility_notice":
            message = event.get("message")
            if isinstance(message, str) and message:
                await self.send_message(room_id, f"ℹ️ {message}")
            return

        if event_name == "worker_restart_attempt":
            self._restart_failed_notified = False
            attempt = event.get("attempt")
            backoff = event.get("backoff")
            await self.send_message(
                room_id,
                f"⚠️ Call worker disconnected. Reconnecting (attempt {attempt}, retry in {backoff}s)...",
                priority="critical",
            )
            return

        if event_name == "worker_restarted":
            self._restart_failed_notified = False
            await self.send_message(room_id, "✅ Call worker reconnected", priority="critical")
            await self._sync_current_track_to_worker(room_id, announce=False)
            return

        if event_name == "worker_restart_failed":
            if not self._restart_failed_notified:
                self._restart_failed_notified = True
                await self.send_message(room_id, "❌ Call worker could not recover. Use !join to reconnect.", priority="critical")
            return

        if event_name == "worker_heartbeat_timeout":
            logger.warning("Call worker heartbeat timeout detected")
            return

        if event_name == "worker_stop_timeout":
            count = event.get("count")
            threshold = event.get("threshold")
            logger.warning("Call worker stop timeout (%s/%s)", count, threshold)
            return

        if event_name == "worker_recovering":
            await self.send_message(room_id, "⚠️ Playback backend stalled. Recovering call worker...", priority="critical")
            return

        if event_name == "worker_recovered":
            await self.send_message(room_id, "✅ Call worker recovered", priority="critical")
            await self._sync_current_track_to_worker(room_id, announce=False)
            return

    async def _auto_advance_timer(
        self,
        duration: float,
        room_id: str,
        expected_generation: int,
        expected_source: Optional[str],
    ):
        try:
            total_wait = duration + self.audio_queue.auto_advance_buffer
            logger.info(
                f"Auto-advance timer set for {total_wait:.2f}s "
                f"(track: {duration:.2f}s + buffer: {self.audio_queue.auto_advance_buffer:.2f}s)"
            )
            await asyncio.sleep(total_wait)
            if expected_generation != self._playback_generation:
                logger.info("Skipping stale auto-advance timer (generation changed)")
                return
            current_source = self._track_source_ref(self.audio_queue.current)
            if expected_source and current_source and current_source != expected_source:
                logger.info("Skipping stale auto-advance timer (track changed)")
                return
            await self._advance_queue(room_id, from_timer=True)
        except asyncio.CancelledError:
            logger.info("Auto-advance timer cancelled")
            raise
        except Exception as exc:
            logger.error(f"Error in auto-advance timer: {exc}")

    async def _advance_queue(
        self,
        room_id: str,
        from_timer: bool = False,
        force_next: bool = False,
        pre_stop: bool = True,
    ):
        previous_current = self.audio_queue.current
        if from_timer:
            self._auto_advance_task = None
        else:
            self._cancel_auto_advance()
        self._cancel_worker_playback_wait()

        if force_next:
            next_track = self.audio_queue.get_next()
        else:
            next_track = self.audio_queue.get_current_or_next()

        if isinstance(previous_current, dict) and previous_current is not next_track:
            self.audio_queue.maybe_delete_track_file(previous_current, trigger="after_playback")

        if not next_track:
            self._current_track_started_at = None
            await self.send_message(room_id, "📭 Queue is empty")
            return

        self._current_room_id = room_id
        loop_indicator = " 🔁" if self.audio_queue.loop_mode else ""

        if not self._is_joined_in_room_call(room_id):
            logger.info("Playback paused: not joined in room call")
            self._current_track_started_at = None
            await self.send_message(
                room_id,
                "ℹ️ Playback paused because bot is not joined in this room call. Use `!join`.",
            )
            return

        try:
            generation, current_source = await self._play_track_in_worker(next_track, pre_stop=pre_stop)
            self._current_track_started_at = asyncio.get_running_loop().time()
            self._push_play_history(room_id, next_track)
            await self.send_message(room_id, f"▶️ Now playing: {next_track['title']}{loop_indicator}")
            self._worker_playback_task = asyncio.create_task(
                self._wait_for_worker_playback(room_id, generation, current_source)
            )
        except Exception as exc:
            logger.error(f"Failed to send track to call worker: {exc}")
            can_retry_stream = (
                self.config.STREAM_RETRY_TO_FILE_ON_FAIL
                and isinstance(next_track, dict)
                and not next_track.get("file")
                and isinstance(next_track.get("stream_url"), str)
            )
            if can_retry_stream:
                await self.send_message(room_id, "⚠️ Stream failed. Retrying from cached file...", priority="critical")
                fallback_ok = await self._retry_stream_track_as_file(room_id, next_track)
                if fallback_ok:
                    await self.send_message(room_id, f"▶️ Now playing: {next_track['title']}")
                    return
            self._current_track_started_at = None
            await self.send_message(room_id, f"❌ Failed to play in call: {exc}")
            return

        self._arm_auto_advance(
            next_track.get("duration"),
            room_id,
            expected_generation=generation,
            expected_source=current_source,
        )

    async def on_invite(self, room: MatrixRoom, event: InviteMemberEvent):
        if not self.config.AUTO_ACCEPT_INVITES:
            logger.info("Invite received for %s but auto-accept is disabled", room.room_id)
            return

        logger.info(f"Invited to {room.display_name}")
        await self.client.join(room.room_id)
        await self.send_message(
            room.room_id,
            "🎵 Music Bot ready. Use `!help` to see commands.",
        )

    def _register_command_handlers(self):
        commands = [
            "!help",
            "!config",
            "!defaults",
            "!join",
            "!leave",
            "!play",
            "!queue",
            "!nowplaying",
            "!diag",
            "!audio",
            "!normalize",
            "!fadein",
            "!volume",
            "!history",
            "!save",
            "!queues",
            "!deletequeue",
            "!renamequeue",
            "!load",
            "!skip",
            "!loop",
            "!stop",
            "!status",
        ]
        for command_name in commands:
            self._command_handlers[command_name] = (
                lambda room, args, cmd=command_name: self._handle_command_internal(room, cmd, args)
            )

    async def handle_command(self, room: MatrixRoom, message: str):
        parts = message.strip().split(maxsplit=1)
        command = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        aliases = {
            "!h": "!help",
            "!j": "!join",
            "!lv": "!leave",
            "!p": "!play",
            "!q": "!queue",
            "!np": "!nowplaying",
            "!d": "!diag",
            "!a": "!audio",
            "!norm": "!normalize",
            "!fi": "!fadein",
            "!v": "!volume",
            "!hist": "!history",
            "!s": "!skip",
            "!lp": "!loop",
            "!x": "!stop",
            "!st": "!status",
            "!sv": "!save",
            "!ld": "!load",
            "!qs": "!queues",
            "!dq": "!deletequeue",
            "!rq": "!renamequeue",
            "!cfg": "!config",
            "!df": "!defaults",
        }
        command = aliases.get(command, command)
        handler = self._command_handlers.get(command)
        if handler is None:
            await self.send_message(room.room_id, "❓ Unknown command. Use !help", priority="normal")
            return
        await handler(room, args)

    async def _handle_command_internal(self, room: MatrixRoom, command: str, args: str):

        if command == "!help":
            await self.send_message(room.room_id, self._help_text())
            return

        if command == "!config":
            await self.send_message(room.room_id, self._safe_config_text())
            return

        if command == "!defaults":
            await self.send_message(room.room_id, self._default_config_text())
            return

        if command == "!join":
            await self._join_call_for_room(room.room_id, announce_if_already_joined=True)
            return

        if command == "!leave":
            if not self.call_worker.running:
                await self.send_message(room.room_id, "ℹ️ Not currently in a call")
                return

            self._cancel_worker_playback_wait()
            self._cancel_background_load()
            self._current_track_started_at = None
            await self.call_worker.stop()
            await self.send_message(room.room_id, "✅ Left Element Call")
            return

        if command == "!play":
            if not args:
                await self.send_message(room.room_id, "❌ Usage: !play <audio-url-or-query>")
                return

            async with self._play_request_lock:
                self._cancel_background_load()

                playlist_ok = False
                playlist_info: dict | None = None
                if self.audio_queue.looks_like_url(args):
                    resolved_ok, resolved = await self.audio_queue.resolve_playlist_entries(args)
                    if resolved_ok and isinstance(resolved, dict) and resolved.get("is_playlist"):
                        playlist_ok = True
                        playlist_info = resolved

                if not playlist_ok:
                    streamed = await self._try_stream_first_idle_play(room.room_id, args)
                    if streamed:
                        return

                if not await self._join_call_for_room(room.room_id):
                    return

                if playlist_ok and isinstance(playlist_info, dict):
                    entries = playlist_info.get("entries")
                    if not isinstance(entries, list) or not entries:
                        await self.send_message(room.room_id, "❌ Could not find playable tracks in that playlist")
                        return

                    playlist_name_raw = playlist_info.get("title")
                    playlist_name = playlist_name_raw if isinstance(playlist_name_raw, str) and playlist_name_raw else "playlist"

                    unique_sources: list[str] = []
                    seen_sources: set[str] = set()
                    for source in entries:
                        if not isinstance(source, str) or not source:
                            continue
                        if source in seen_sources:
                            continue
                        seen_sources.add(source)
                        unique_sources.append(source)

                    over_limit_skipped = 0
                    max_tracks = self.config.PLAYLIST_MAX_TRACKS_PER_REQUEST
                    if len(unique_sources) > max_tracks:
                        over_limit_skipped = len(unique_sources) - max_tracks
                        unique_sources = unique_sources[:max_tracks]

                    filtered_sources = [src for src in unique_sources if not self.audio_queue.has_source(src)]
                    skipped_existing = len(unique_sources) - len(filtered_sources)
                    if not filtered_sources:
                        await self.send_message(
                            room.room_id,
                            f"ℹ️ All tracks from `{playlist_name}` are already playing or queued",
                        )
                        return

                    await self.send_message(room.room_id, f"📥 Loading playlist `{playlist_name}`...")

                    first_item, first_index, failures = await self._download_first_available_source(filtered_sources)

                    if first_item is None:
                        await self.send_message(room.room_id, f"❌ Could not load any tracks from `{playlist_name}`")
                        return

                    async with self._playback_lock:
                        had_current = self.audio_queue.current is not None or self.call_worker.state == "playing"
                        self.audio_queue.add_to_queue(
                            first_item["file"],
                            first_item.get("title"),
                            first_item.get("duration"),
                            source_url=first_item.get("source_url"),
                            non_cacheable=bool(first_item.get("non_cacheable")),
                        )
                        if not had_current:
                            await self._advance_queue(room.room_id, force_next=True)

                    loaded_count = 1
                    remaining_sources = [src for idx, src in enumerate(filtered_sources) if idx != first_index]
                    if remaining_sources:
                        self._background_load_task = asyncio.create_task(
                            self._load_remaining_tracks(
                                room.room_id,
                                playlist_name,
                                remaining_sources,
                                dedupe_existing=True,
                                source_label="playlist",
                            )
                        )

                    summary = [f"✅ Playlist `{playlist_name}`: added {loaded_count} track"]
                    if remaining_sources:
                        summary.append(f"ℹ️ Loading {len(remaining_sources)} more in background")
                    if skipped_existing:
                        summary.append(f"Skipped already queued: {skipped_existing}")
                    if over_limit_skipped:
                        summary.append(
                            f"Skipped by max_tracks_per_request ({self.config.PLAYLIST_MAX_TRACKS_PER_REQUEST}): {over_limit_skipped}"
                        )
                    if failures:
                        summary.append(f"Failed: {failures}")
                    await self.send_message(room.room_id, "\n".join(summary))
                    return

                if self.config.SHOW_PROGRESS_MESSAGES:
                    if not self.audio_queue.looks_like_url(args):
                        asyncio.create_task(self.send_message(room.room_id, f"🔎 Searching: {args}", priority="noisy"))
                    asyncio.create_task(self.send_message(room.room_id, "⬇️ Downloading...", priority="noisy"))

                success, result = await self.audio_queue.download_audio(args)
                if not success:
                    await self.send_message(room.room_id, f"❌ Download failed: {result}")
                    return

                if not isinstance(result, dict):
                    await self.send_message(room.room_id, "❌ Download failed: invalid download metadata")
                    return

                audio_file = result["file"]
                duration = result["duration"]
                title = result["title"]
                source_url = result.get("source_url")

                if isinstance(source_url, str) and self.audio_queue.has_source(source_url):
                    await self.send_message(
                        room.room_id,
                        "ℹ️ That track is already playing or queued. Use `!loop` if you want repeats.",
                    )
                    return

                queued_message = None
                should_advance = False
                async with self._playback_lock:
                    had_current = self.audio_queue.current is not None or self.call_worker.state == "playing"
                    self.audio_queue.add_to_queue(
                        audio_file,
                        title,
                        duration,
                        source_url=source_url,
                        non_cacheable=bool(result.get("non_cacheable")),
                    )

                    if had_current:
                        eta_known = True
                        eta_seconds = 0.0

                        current_remaining = self._current_track_remaining_seconds()
                        if current_remaining is None:
                            eta_known = False
                        else:
                            eta_seconds = current_remaining

                        # Sum durations of tracks ahead of the newly queued one.
                        for queued_item in list(self.audio_queue.queue)[:-1]:
                            item_duration = queued_item.get("duration") if isinstance(queued_item, dict) else None
                            if eta_known and item_duration is not None:
                                eta_seconds += float(item_duration)
                            else:
                                eta_known = False

                        eta_line = (
                            f"\nStarts in: {self._format_duration(eta_seconds)}"
                            if eta_known
                            else "\nStarts in: unknown"
                        )
                        next_title = self._next_track_title()
                        next_line = f"\nNext: {next_title}" if next_title else ""
                        queued_message = (
                            f"✅ Queued: {title}\n"
                            f"Position: {len(self.audio_queue.queue)}"
                            f"{eta_line}{next_line}"
                        )
                    else:
                        should_advance = True
                if queued_message:
                    await self.send_message(room.room_id, queued_message)
                if should_advance:
                    await self._advance_queue(room.room_id, force_next=True)
            return

        if command == "!queue":
            if not self.audio_queue.queue and not self.audio_queue.current:
                await self.send_message(room.room_id, "ℹ️ Queue empty")
                return

            lines = ["🎵 Queue", ""]
            eta_known = True
            eta_seconds = 0.0

            if self.audio_queue.current:
                loop_indicator = " 🔁 (looping)" if self.audio_queue.loop_mode else ""
                lines.append(self._format_track_line("Now: ", self.audio_queue.current) + loop_indicator)
                lines.append("")
                current_remaining = self._current_track_remaining_seconds()
                if current_remaining is None:
                    eta_known = False
                else:
                    eta_seconds = current_remaining

            for idx, item in enumerate(self.audio_queue.queue, 1):
                eta_label = f"in {self._format_duration(eta_seconds)}" if eta_known else "ETA unknown"
                lines.append(self._format_track_line(f"{idx}. ", item) + f" ({eta_label})")

                item_duration = item.get("duration")
                if eta_known and item_duration is not None:
                    eta_seconds += float(item_duration)
                else:
                    eta_known = False

            tracks_for_total = []
            if self.audio_queue.current:
                tracks_for_total.append(self.audio_queue.current)
            tracks_for_total.extend(list(self.audio_queue.queue))
            total_duration = self._sum_known_durations(tracks_for_total)
            if total_duration is not None and self.audio_queue.current:
                current_duration = self.audio_queue.current.get("duration")
                current_remaining = self._current_track_remaining_seconds()
                if current_duration is None or current_remaining is None:
                    total_duration = None
                else:
                    total_duration = max(0.0, total_duration - float(current_duration) + current_remaining)
            total_label = self._format_duration(total_duration) if total_duration is not None else "unknown"
            lines.append("")
            lines.append(f"Total: {total_label}")

            await self.send_message(room.room_id, "\n".join(lines))
            return

        if command == "!nowplaying":
            if not self.audio_queue.current:
                await self.send_message(room.room_id, "ℹ️ Nothing is currently playing")
                return

            line = self._format_track_line("▶️ Now: ", self.audio_queue.current)
            next_title = self._next_track_title()
            if next_title:
                line += f"\nNext: {next_title}"
            await self.send_message(room.room_id, line)
            return

        if command == "!diag":
            now = asyncio.get_running_loop().time()
            last_pong = self.call_worker.last_pong_ts
            pong_age = f"{(now - last_pong):.1f}s ago" if last_pong is not None else "never"
            lines = [
                "🛠️ Diagnostics",
                f"Worker state: {self.call_worker.state}",
                f"Worker running: {'yes' if self.call_worker.running else 'no'}",
                f"Room joined: {self.call_worker.room_id or 'none'}",
                f"Desired room: {self.call_worker.desired_room_id or 'none'}",
                f"Heartbeat pong: {pong_age}",
                f"Restart attempts: {self.call_worker.restart_attempts}/{self.call_worker.max_restart_attempts}",
                (
                    "Stop timeouts: "
                    f"{self.call_worker.consecutive_stop_timeouts}/{self.call_worker.stop_timeout_restart_threshold}"
                ),
                f"Queue size: {len(self.audio_queue.queue)}",
                "Startup checks: " + ("ok" if not self._startup_warnings else f"{len(self._startup_warnings)} warning(s)"),
            ]
            if self._tool_versions:
                tools = ", ".join(f"{k}={v}" for k, v in sorted(self._tool_versions.items()))
                lines.append(f"Tool versions: {tools}")
            if self._startup_warnings:
                lines.extend(f"- {w}" for w in self._startup_warnings[:3])
            await self.send_message(room.room_id, "\n".join(lines))
            return

        if command == "!audio":
            await self.send_message(
                room.room_id,
                "🎚️ Audio\n"
                f"Normalize: {'On' if self.config.NORMALIZE_AUDIO else 'Off'}\n"
                f"Fade-in: {self.config.FADE_IN_MS}ms\n"
                f"Volume: {self.config.VOLUME_PERCENT}%\n"
                f"Cache mode: {self.config.AUDIO_CACHE_MODE}\n"
                f"Cache max: {self._format_bytes(self.config.AUDIO_CACHE_MAX_BYTES)}\n"
                f"Delete after playback: {'On' if self.config.AUDIO_CACHE_DELETE_AFTER_PLAYBACK else 'Off'}\n"
                f"Delete on shutdown: {'On' if self.config.AUDIO_CACHE_DELETE_ON_SHUTDOWN else 'Off'}\n"
                f"Stream-first idle: {'On' if self.config.STREAM_FIRST_IDLE else 'Off'}\n"
                f"Stream prefetch current: {'On' if self.config.STREAM_PREFETCH_CURRENT else 'Off'}",
            )
            return

        if command == "!normalize":
            value = args.strip().lower()
            if value not in {"on", "off"}:
                await self.send_message(room.room_id, "❌ Usage: !normalize on|off")
                return
            self.config.NORMALIZE_AUDIO = value == "on"
            await self._apply_audio_settings_to_worker()
            await self.send_message(room.room_id, f"✅ Normalize audio: {'On' if self.config.NORMALIZE_AUDIO else 'Off'}")
            return

        if command == "!fadein":
            raw = args.strip()
            if not raw:
                await self.send_message(room.room_id, "❌ Usage: !fadein <milliseconds>")
                return
            ms = self._parse_int_arg(raw)
            if ms is None:
                await self.send_message(room.room_id, "❌ Fade-in must be an integer number of milliseconds")
                return
            if ms < 0 or ms > 5000:
                await self.send_message(room.room_id, "❌ Fade-in must be between 0 and 5000 ms")
                return
            self.config.FADE_IN_MS = ms
            await self._apply_audio_settings_to_worker()
            await self.send_message(room.room_id, f"✅ Fade-in set to {self.config.FADE_IN_MS}ms")
            return

        if command == "!volume":
            raw = args.strip()
            if not raw:
                await self.send_message(room.room_id, "❌ Usage: !volume <0-200>")
                return
            value = self._parse_int_arg(raw)
            if value is None:
                await self.send_message(room.room_id, "❌ Volume must be an integer between 0 and 200")
                return
            if value < 0 or value > 200:
                await self.send_message(room.room_id, "❌ Volume must be between 0 and 200")
                return
            self.config.VOLUME_PERCENT = value
            await self._apply_audio_settings_to_worker()
            if self.call_worker.running:
                await self.send_message(room.room_id, f"✅ Volume set to {self.config.VOLUME_PERCENT}% (applies immediately)")
            else:
                await self.send_message(room.room_id, f"✅ Volume set to {self.config.VOLUME_PERCENT}%")
            return

        if command == "!history":
            history = list(self._play_history_by_room.get(room.room_id, []))
            if not history:
                await self.send_message(room.room_id, "ℹ️ No playback history yet")
                return

            lines = ["🕘 Recent playback"]
            for idx, item in enumerate(history, 1):
                title = item.get("title", "unknown")
                duration = self._format_duration(item.get("duration"))
                at_ts = item.get("at")
                at_label = time.strftime("%H:%M:%S", time.localtime(at_ts)) if isinstance(at_ts, int) else "unknown"
                lines.append(f"{idx}. {title} [{duration}] at {at_label}")
            await self.send_message(room.room_id, "\n".join(lines))
            return

        if command == "!save":
            tokens = args.strip().split()
            force = False
            if "--force" in tokens:
                force = True
                tokens = [t for t in tokens if t != "--force"]
            name = " ".join(tokens).strip()
            if not name:
                await self.send_message(room.room_id, "❌ Usage: !save <name> [--force]")
                return

            if self.saved_queues.has_queue(room.room_id, name) and not force:
                await self.send_message(
                    room.room_id,
                    f"❌ Saved queue `{name}` already exists. Use `!save {name} --force` to overwrite.",
                )
                return

            snapshot = []
            skipped = 0

            if self.audio_queue.current:
                source_url = self.audio_queue.current.get("source_url")
                if source_url:
                    snapshot.append(
                        {
                            "source_url": source_url,
                            "title": self.audio_queue.current.get("title"),
                            "duration": self.audio_queue.current.get("duration"),
                        }
                    )
                else:
                    skipped += 1

            for item in self.audio_queue.queue:
                source_url = item.get("source_url")
                if source_url:
                    snapshot.append(
                        {
                            "source_url": source_url,
                            "title": item.get("title"),
                            "duration": item.get("duration"),
                        }
                    )
                else:
                    skipped += 1

            if not snapshot:
                await self.send_message(room.room_id, "❌ Nothing saveable in queue right now")
                return

            self.saved_queues.save_queue(room.room_id, name, snapshot)
            msg = f"✅ Saved queue `{name}` with {len(snapshot)} track(s)"
            if skipped:
                msg += f"\nSkipped {skipped} unsaveable track(s)"
            await self.send_message(room.room_id, msg)
            return

        if command == "!queues":
            names = self.saved_queues.list_names(room.room_id)
            if not names:
                await self.send_message(room.room_id, "📭 No saved queues in this room")
                return

            entries = []
            for name in names:
                tracks = self.saved_queues.load_queue(room.room_id, name) or []
                total = self._sum_known_durations(tracks)
                total_label = self._format_duration(total) if total is not None else "unknown"
                entries.append(f"- {name} ({len(tracks)} track(s), {total_label})")

            await self.send_message(room.room_id, "💾 Saved queues:\n" + "\n".join(entries))
            return

        if command == "!deletequeue":
            name = args.strip()
            if not name:
                await self.send_message(room.room_id, "❌ Usage: !deletequeue <name>")
                return
            deleted = self.saved_queues.delete_queue(room.room_id, name)
            if not deleted:
                await self.send_message(room.room_id, f"❌ Saved queue not found: {name}")
                return
            await self.send_message(room.room_id, f"🗑️ Deleted saved queue `{name}`")
            return

        if command == "!renamequeue":
            parts = args.strip().split(maxsplit=1)
            if len(parts) != 2:
                await self.send_message(room.room_id, "❌ Usage: !renamequeue <old> <new>")
                return

            old_name, new_name = parts[0].strip(), parts[1].strip()
            if not old_name or not new_name:
                await self.send_message(room.room_id, "❌ Usage: !renamequeue <old> <new>")
                return

            ok, reason = self.saved_queues.rename_queue(room.room_id, old_name, new_name)
            if not ok:
                if reason == "missing_old":
                    await self.send_message(room.room_id, f"❌ Saved queue not found: {old_name}")
                    return
                if reason == "new_exists":
                    await self.send_message(room.room_id, f"❌ A saved queue named `{new_name}` already exists")
                    return
                await self.send_message(room.room_id, "❌ Failed to rename saved queue")
                return

            await self.send_message(room.room_id, f"✏️ Renamed `{old_name}` to `{new_name}`")
            return

        if command == "!load":
            name = args.strip()
            if not name:
                await self.send_message(room.room_id, "❌ Usage: !load <name>")
                return

            tracks = self.saved_queues.load_queue(room.room_id, name)
            if tracks is None:
                await self.send_message(room.room_id, f"❌ Saved queue not found: {name}")
                return

            if not tracks:
                await self.send_message(room.room_id, f"❌ Saved queue `{name}` is empty")
                return

            if not await self._join_call_for_room(room.room_id):
                return

            self._cancel_background_load()
            await self.send_message(room.room_id, f"📥 Loading `{name}`...")

            sources: list[str] = []
            failures = 0

            for track in tracks:
                source_url = track.get("source_url") if isinstance(track, dict) else None
                if not source_url:
                    failures += 1
                    continue
                sources.append(source_url)

            first_item, first_index, download_failures = await self._download_first_available_source(sources)
            failures += download_failures

            if first_item is None:
                await self.send_message(room.room_id, f"❌ Could not load any tracks from `{name}`")
                return

            async with self._playback_lock:
                prior_tracks = []
                if self.audio_queue.current:
                    prior_tracks.append(self.audio_queue.current)
                prior_tracks.extend(list(self.audio_queue.queue))
                self._cancel_auto_advance()
                self._cancel_worker_playback_wait()
                await self.call_worker.stop_playback(wait_for_terminal=True)
                self.audio_queue.clear_queue()
                self.audio_queue.current = None
                for track in prior_tracks:
                    self.audio_queue.maybe_delete_track_file(track, trigger="after_playback")

                self.audio_queue.add_to_queue(
                    first_item["file"],
                    first_item.get("title"),
                    first_item.get("duration"),
                    source_url=first_item.get("source_url"),
                    non_cacheable=bool(first_item.get("non_cacheable")),
                )

                await self._advance_queue(room.room_id, force_next=True)

            remaining_sources = [src for idx, src in enumerate(sources) if idx != first_index]
            if remaining_sources:
                self._background_load_task = asyncio.create_task(
                    self._load_remaining_tracks(
                        room.room_id,
                        name,
                        remaining_sources,
                        dedupe_existing=False,
                        source_label="saved queue",
                    )
                )
                await self.send_message(room.room_id, f"ℹ️ Loading {len(remaining_sources)} more...")
            elif failures:
                await self.send_message(room.room_id, f"⚠️ Failed: {failures}")
            return

        if command == "!skip":
            if not await self._require_joined_in_room_call(room.room_id, "!skip"):
                return

            now = time.monotonic()
            if now - self._last_skip_at < self._skip_cooldown_seconds:
                await self.send_message(room.room_id, "ℹ️ Skip already in progress")
                return

            queue_empty = False
            async with self._playback_lock:
                if not self.audio_queue.current and not self.audio_queue.queue:
                    queue_empty = True
                else:
                    self._last_skip_at = now
                    asyncio.create_task(self.send_message(room.room_id, "⏭️ Skipping...", priority="noisy"))
                    self._cancel_worker_playback_wait()
                    await self.call_worker.stop_playback(wait_for_terminal=False)
                    await self._advance_queue(room.room_id, force_next=True, pre_stop=False)
            if queue_empty:
                await self.send_message(room.room_id, "📭 Queue is empty")
            return

        if command == "!loop":
            if not await self._require_joined_in_room_call(room.room_id, "!loop"):
                return

            loop_status = self.audio_queue.toggle_loop()
            if loop_status:
                self._cancel_auto_advance()
                await self.send_message(
                    room.room_id,
                    "✅ Loop on",
                )
            else:
                await self.send_message(room.room_id, "✅ Loop off")
                current = self.audio_queue.current
                if current and current.get("duration") is not None and self._current_room_id:
                    self._arm_auto_advance(
                        current["duration"],
                        self._current_room_id,
                        expected_generation=self._playback_generation,
                        expected_source=self._track_source_ref(current),
                    )
            return

        if command == "!stop":
            if not await self._require_joined_in_room_call(room.room_id, "!stop"):
                return

            self._cancel_auto_advance()
            self._cancel_worker_playback_wait()
            self._cancel_background_load()
            tracks_to_cleanup = []
            if self.audio_queue.current:
                tracks_to_cleanup.append(self.audio_queue.current)
            tracks_to_cleanup.extend(list(self.audio_queue.queue))
            self.audio_queue.clear_queue()
            self.audio_queue.current = None
            self._current_room_id = None
            self._current_track_started_at = None
            for track in tracks_to_cleanup:
                self.audio_queue.maybe_delete_track_file(track, trigger="after_playback")
            await self.call_worker.stop_playback(wait_for_terminal=True)
            await self.send_message(room.room_id, "⏹️ Stopped and cleared queue")
            return

        if command == "!status":
            timer_status = (
                "Active"
                if self._auto_advance_task and not self._auto_advance_task.done()
                else "Inactive"
            )
            lines = [
                "✅ Bot online",
                f"Worker: {self.call_worker.state}",
                f"Queue: {len(self.audio_queue.queue)} track(s)",
                f"Loop: {'On' if self.audio_queue.loop_mode else 'Off'}",
                f"Auto-advance: {timer_status}",
                f"Pre-roll: {self.audio_queue.preroll_silence:.1f}s",
                f"Normalize: {'On' if self.config.NORMALIZE_AUDIO else 'Off'}",
                f"Fade-in: {self.config.FADE_IN_MS}ms",
                f"Volume: {self.config.VOLUME_PERCENT}%",
            ]
            if self.call_worker.running and self.call_worker.room_id:
                lines.insert(2, f"Room: {self.call_worker.room_id}")
            if self.audio_queue.current:
                lines.insert(2, f"Current: {self.audio_queue.current['title']}")
                next_title = self._next_track_title()
                if next_title:
                    lines.insert(3, f"Next: {next_title}")
            await self.send_message(room.room_id, "\n".join(lines))
            return

        return

    async def send_message(
        self,
        room_id: str,
        text: str,
        *,
        html_body: Optional[str] = None,
        priority: str = "normal",
    ):
        try:
            resolved_priority = priority if priority in self._message_priority_map else "normal"
            if priority == "normal":
                if text.startswith("❌") or text.startswith("⚠️"):
                    resolved_priority = "critical"
            await self._queue_message(room_id, text, html_body=html_body, priority=resolved_priority)
        except Exception as exc:
            logger.error(f"Error sending message: {exc}")

    async def on_message(self, room: MatrixRoom, event: RoomMessageText):
        if event.sender == self.config.MATRIX_USER_ID or not self.first_sync_done:
            return

        if event.body.strip().startswith("!"):
            await self.handle_command(room, event.body)

    async def start(self):
        logger.info("=" * 60)
        logger.info("Music Bot Core Starting")
        logger.info(f"Auto-advance buffer: {self.config.AUTO_ADVANCE_BUFFER}s")
        logger.info(f"Pre-roll silence: {self.config.PREROLL_SILENCE}s")
        logger.info(f"Normalize audio: {self.config.NORMALIZE_AUDIO}")
        logger.info(f"Fade-in: {self.config.FADE_IN_MS}ms")
        logger.info(f"Volume: {self.config.VOLUME_PERCENT}%")
        logger.info("Voice backend: call worker enabled")
        self._run_startup_checks()
        logger.info("=" * 60)

        await self.client.sync(timeout=30000, full_state=True)
        self.first_sync_done = True
        self._start_message_dispatcher()
        self._ensure_advance_watchdog()
        logger.info("Bot ready")

        try:
            await self.client.sync_forever(timeout=30000, full_state=False)
        finally:
            self._cancel_auto_advance()
            self._cancel_advance_watchdog()
            self._cancel_worker_playback_wait()
            self._cancel_background_load()
            self.audio_queue.cleanup_on_shutdown()
            await self._stop_message_dispatcher()
            await self.call_worker.stop()
            await self.client.close()

    def _run_startup_checks(self):
        warnings: list[str] = []
        self._tool_versions = self._collect_tool_versions()

        if not self.call_worker.available:
            warnings.append("Call worker script not found (call features disabled)")

        if not shutil.which("ffmpeg"):
            warnings.append("ffmpeg not found in PATH")

        if not shutil.which("yt-dlp") and not shutil.which("youtube-dlp"):
            warnings.append("yt-dlp/youtube-dlp not found in PATH")

        try:
            self.audio_queue.audio_dir.mkdir(parents=True, exist_ok=True)
            probe = self.audio_queue.audio_dir / ".write_test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except Exception as exc:
            warnings.append(f"Audio directory is not writable ({self.audio_queue.audio_dir}): {exc}")

        if warnings:
            for warning in warnings:
                logger.warning("Startup check: %s", warning)
        else:
            logger.info("Startup checks passed")
        self._startup_warnings = warnings

    def _collect_tool_versions(self) -> dict[str, str]:
        result: dict[str, str] = {}

        def capture(name: str, cmd: list[str]):
            try:
                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=3).strip()
            except Exception:
                return
            if not output:
                return
            first = output.splitlines()[0].strip()
            if first:
                result[name] = first

        capture("python", ["python", "--version"])
        capture("node", ["node", "--version"])
        capture("ffmpeg", ["ffmpeg", "-version"])
        if shutil.which("yt-dlp"):
            capture("yt-dlp", ["yt-dlp", "--version"])
        elif shutil.which("youtube-dlp"):
            capture("youtube-dlp", ["youtube-dlp", "--version"])

        return result
