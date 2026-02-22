import os
import tomllib
from pathlib import Path
from typing import Any, Optional


MIN_AUDIO_CACHE_MAX_BYTES = 200 * 1024 * 1024


class Config:
    """Runtime configuration loaded from config.toml with env overrides."""

    DEFAULTS: dict[str, Any] = {
        "bot.name": "Music Bot",
        "bot.history_limit": 10,
        "bot.auto_accept_invites": False,
        "paths.audio_dir": "/tmp/musicbot_audio",
        "paths.saved_queues_file": "data/saved_queues.json",
        "audio.auto_advance_buffer": 2.0,
        "audio.preroll_silence": 1.0,
        "audio.normalize_audio": False,
        "audio.fade_in_ms": 120,
        "audio.volume_percent": 100,
        "audio.cache_mode": "size_lru",
        "audio.cache_max_bytes": 1_073_741_824,
        "audio.cache_delete_after_playback": False,
        "audio.cache_delete_on_shutdown": True,
        "worker.max_restart_attempts": 3,
        "worker.heartbeat_interval_seconds": 10.0,
        "worker.skip_cooldown_seconds": 1.0,
        "worker.stop_timeout_restart_threshold": 2,
        "worker.membership_mode": "legacy",
        "worker.log_max_bytes": 2_000_000,
        "worker.log_backups": 5,
        "logging.file": "logs/musicbot.log",
        "logging.clean_enabled": True,
        "logging.clean_file": "logs/musicbot.clean.log",
        "logging.clean_filter_matrixrtc_noise": True,
        "logging.max_bytes": 2_000_000,
        "logging.backups": 5,
        "ui.show_progress_messages": False,
        "ui.rich_formatting": False,
    }

    def __init__(self):
        self._load_dotenv_file()
        self.config_file = Path(os.environ.get("CONFIG_FILE", "config.toml"))
        self._toml = self._load_toml_file(self.config_file)

        self.MATRIX_HOMESERVER = self._get_str("MATRIX_HOMESERVER", "matrix", "homeserver")
        self.MATRIX_USER_ID = self._get_str("MATRIX_USER_ID", "matrix", "user_id")
        self.MATRIX_ACCESS_TOKEN = self._get_str("MATRIX_ACCESS_TOKEN", "matrix", "access_token")

        self.BOT_NAME = self._get_str("BOT_NAME", "bot", "name", default=self.DEFAULTS["bot.name"])
        self.HISTORY_LIMIT = self._get_nonnegative_int(
            "HISTORY_LIMIT", "bot", "history_limit", self.DEFAULTS["bot.history_limit"]
        )
        self.AUTO_ACCEPT_INVITES = self._get_bool(
            "AUTO_ACCEPT_INVITES",
            "bot",
            "auto_accept_invites",
            self.DEFAULTS["bot.auto_accept_invites"],
        )
        self.AUDIO_DIR = Path(
            self._get_str("AUDIO_DIR", "paths", "audio_dir", default=self.DEFAULTS["paths.audio_dir"])
            or self.DEFAULTS["paths.audio_dir"]
        )
        self.SAVED_QUEUES_FILE = Path(
            self._get_str("SAVED_QUEUES_FILE", "paths", "saved_queues_file", default=self.DEFAULTS["paths.saved_queues_file"])
            or self.DEFAULTS["paths.saved_queues_file"]
        )

        self.AUTO_ADVANCE_BUFFER = self._get_nonnegative_float(
            "AUTO_ADVANCE_BUFFER", "audio", "auto_advance_buffer", self.DEFAULTS["audio.auto_advance_buffer"]
        )
        self.PREROLL_SILENCE = self._get_nonnegative_float(
            "PREROLL_SILENCE", "audio", "preroll_silence", self.DEFAULTS["audio.preroll_silence"]
        )
        self.NORMALIZE_AUDIO = self._get_bool(
            "NORMALIZE_AUDIO", "audio", "normalize_audio", self.DEFAULTS["audio.normalize_audio"]
        )
        self.FADE_IN_MS = self._get_nonnegative_int("FADE_IN_MS", "audio", "fade_in_ms", self.DEFAULTS["audio.fade_in_ms"])
        self.VOLUME_PERCENT = self._get_nonnegative_int(
            "VOLUME_PERCENT", "audio", "volume_percent", self.DEFAULTS["audio.volume_percent"]
        )
        if self.VOLUME_PERCENT > 200:
            raise ValueError("VOLUME_PERCENT/audio.volume_percent must be between 0 and 200")
        self.AUDIO_CACHE_MODE = (
            self._get_str("AUDIO_CACHE_MODE", "audio", "cache_mode", default=self.DEFAULTS["audio.cache_mode"])
            or self.DEFAULTS["audio.cache_mode"]
        )
        self.AUDIO_CACHE_MAX_BYTES = self._get_nonnegative_int(
            "AUDIO_CACHE_MAX_BYTES", "audio", "cache_max_bytes", self.DEFAULTS["audio.cache_max_bytes"]
        )
        self.AUDIO_CACHE_DELETE_AFTER_PLAYBACK = self._get_bool(
            "AUDIO_CACHE_DELETE_AFTER_PLAYBACK",
            "audio",
            "cache_delete_after_playback",
            self.DEFAULTS["audio.cache_delete_after_playback"],
        )
        self.AUDIO_CACHE_DELETE_ON_SHUTDOWN = self._get_bool(
            "AUDIO_CACHE_DELETE_ON_SHUTDOWN",
            "audio",
            "cache_delete_on_shutdown",
            self.DEFAULTS["audio.cache_delete_on_shutdown"],
        )
        self.AUDIO_CACHE_MODE = self.AUDIO_CACHE_MODE.strip().lower()
        if self.AUDIO_CACHE_MODE not in {"size_lru", "never_delete", "always_delete"}:
            raise ValueError(
                "AUDIO_CACHE_MODE/audio.cache_mode must be one of: size_lru, never_delete, always_delete"
            )
        self.AUDIO_CACHE_MAX_BYTES_RAW = int(self.AUDIO_CACHE_MAX_BYTES)
        self.AUDIO_CACHE_MAX_BYTES_CLAMPED = False
        if self.AUDIO_CACHE_MODE == "size_lru" and self.AUDIO_CACHE_MAX_BYTES < MIN_AUDIO_CACHE_MAX_BYTES:
            self.AUDIO_CACHE_MAX_BYTES = MIN_AUDIO_CACHE_MAX_BYTES
            self.AUDIO_CACHE_MAX_BYTES_CLAMPED = True

        self.LOG_FILE = Path(
            self._get_str("LOG_FILE", "logging", "file", default=self.DEFAULTS["logging.file"]) or self.DEFAULTS["logging.file"]
        )
        self.CLEAN_LOG_ENABLED = self._get_bool(
            "CLEAN_LOG_ENABLED", "logging", "clean_enabled", self.DEFAULTS["logging.clean_enabled"]
        )
        self.CLEAN_LOG_FILE = Path(
            self._get_str("CLEAN_LOG_FILE", "logging", "clean_file", default=self.DEFAULTS["logging.clean_file"])
            or self.DEFAULTS["logging.clean_file"]
        )
        self.CLEAN_LOG_FILTER_MATRIXRTC_NOISE = self._get_bool(
            "CLEAN_LOG_FILTER_MATRIXRTC_NOISE",
            "logging",
            "clean_filter_matrixrtc_noise",
            self.DEFAULTS["logging.clean_filter_matrixrtc_noise"],
        )
        self.LOG_MAX_BYTES = self._get_nonnegative_int("LOG_MAX_BYTES", "logging", "max_bytes", self.DEFAULTS["logging.max_bytes"])
        self.LOG_BACKUPS = self._get_nonnegative_int("LOG_BACKUPS", "logging", "backups", self.DEFAULTS["logging.backups"])
        self.SHOW_PROGRESS_MESSAGES = self._get_bool(
            "SHOW_PROGRESS_MESSAGES", "ui", "show_progress_messages", self.DEFAULTS["ui.show_progress_messages"]
        )
        self.RICH_FORMATTING = self._get_bool("RICH_FORMATTING", "ui", "rich_formatting", self.DEFAULTS["ui.rich_formatting"])

        self.WORKER_LOG_MAX_BYTES = self._get_nonnegative_int(
            "WORKER_LOG_MAX_BYTES", "worker", "log_max_bytes", self.DEFAULTS["worker.log_max_bytes"]
        )
        self.WORKER_LOG_BACKUPS = self._get_nonnegative_int(
            "WORKER_LOG_BACKUPS", "worker", "log_backups", self.DEFAULTS["worker.log_backups"]
        )
        self.WORKER_MAX_RESTART_ATTEMPTS = self._get_nonnegative_int(
            "WORKER_MAX_RESTART_ATTEMPTS", "worker", "max_restart_attempts", self.DEFAULTS["worker.max_restart_attempts"]
        )
        self.WORKER_HEARTBEAT_INTERVAL = self._get_nonnegative_float(
            "WORKER_HEARTBEAT_INTERVAL",
            "worker",
            "heartbeat_interval_seconds",
            self.DEFAULTS["worker.heartbeat_interval_seconds"],
        )
        self.SKIP_COOLDOWN_SECONDS = self._get_nonnegative_float(
            "SKIP_COOLDOWN_SECONDS", "worker", "skip_cooldown_seconds", self.DEFAULTS["worker.skip_cooldown_seconds"]
        )
        self.WORKER_STOP_TIMEOUT_RESTART_THRESHOLD = self._get_nonnegative_int(
            "WORKER_STOP_TIMEOUT_RESTART_THRESHOLD",
            "worker",
            "stop_timeout_restart_threshold",
            self.DEFAULTS["worker.stop_timeout_restart_threshold"],
        )
        self.WORKER_MEMBERSHIP_MODE = (
            self._get_str(
                "WORKER_MEMBERSHIP_MODE",
                "worker",
                "membership_mode",
                default=self.DEFAULTS["worker.membership_mode"],
            )
            or self.DEFAULTS["worker.membership_mode"]
        )
        self.WORKER_MEMBERSHIP_MODE = self.WORKER_MEMBERSHIP_MODE.strip().lower()
        if self.WORKER_MEMBERSHIP_MODE not in {"matrix2_auto", "matrix2", "legacy"}:
            raise ValueError(
                "WORKER_MEMBERSHIP_MODE/worker.membership_mode must be one of: matrix2_auto, matrix2, legacy"
            )

        missing = [
            key
            for key, value in [
                ("matrix.homeserver", self.MATRIX_HOMESERVER),
                ("matrix.user_id", self.MATRIX_USER_ID),
                ("matrix.access_token", self.MATRIX_ACCESS_TOKEN),
            ]
            if not value
        ]
        if missing:
            raise ValueError(
                "Missing required configuration values: "
                + ", ".join(missing)
                + ". Create config.toml (see config/config.example.toml)."
            )

    @staticmethod
    def _load_toml_file(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("rb") as handle:
            data = tomllib.load(handle)
        if not isinstance(data, dict):
            return {}
        return data

    def _toml_get(self, *keys: str) -> Optional[Any]:
        cur: Any = self._toml
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
            if cur is None:
                return None
        return cur

    def _get_str(self, env_name: str, section: str, key: str, default: Optional[str] = None) -> Optional[str]:
        from_env = os.environ.get(env_name)
        if from_env is not None:
            return from_env
        from_toml = self._toml_get(section, key)
        if from_toml is None:
            return default
        return str(from_toml)

    def _get_nonnegative_float(self, env_name: str, section: str, key: str, default: float) -> float:
        raw = os.environ.get(env_name)
        if raw is None:
            from_toml = self._toml_get(section, key)
            raw = str(from_toml) if from_toml is not None else None
        if raw is None:
            return default
        try:
            value = float(raw)
        except ValueError as exc:
            raise ValueError(f"{env_name}/{section}.{key} must be a float, got: {raw!r}") from exc
        if value < 0:
            raise ValueError(f"{env_name}/{section}.{key} must be >= 0, got: {value}")
        return value

    def _get_nonnegative_int(self, env_name: str, section: str, key: str, default: int) -> int:
        raw = os.environ.get(env_name)
        if raw is None:
            from_toml = self._toml_get(section, key)
            raw = str(from_toml) if from_toml is not None else None
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{env_name}/{section}.{key} must be an integer, got: {raw!r}") from exc
        if value < 0:
            raise ValueError(f"{env_name}/{section}.{key} must be >= 0, got: {value}")
        return value

    def _get_bool(self, env_name: str, section: str, key: str, default: bool) -> bool:
        raw = os.environ.get(env_name)
        if raw is None:
            from_toml = self._toml_get(section, key)
            raw = str(from_toml) if from_toml is not None else None
        if raw is None:
            return default
        norm = raw.strip().lower()
        if norm in {"1", "true", "yes", "on"}:
            return True
        if norm in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"{env_name}/{section}.{key} must be a boolean-like value, got: {raw!r}")

    @staticmethod
    def _load_dotenv_file():
        """Load key=value pairs from local .env into os.environ if unset."""
        dotenv_path = Path(".env")
        if not dotenv_path.exists():
            return

        for line in dotenv_path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue

            key, value = raw.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue

            if (
                (value.startswith('"') and value.endswith('"'))
                or (value.startswith("'") and value.endswith("'"))
            ):
                value = value[1:-1]

            os.environ.setdefault(key, value)

    @classmethod
    def defaults_text(cls) -> str:
        lines = [
            "⚙️ Default config values",
            f"bot.name = {cls.DEFAULTS['bot.name']}",
            f"bot.history_limit = {cls.DEFAULTS['bot.history_limit']}",
            f"bot.auto_accept_invites = {str(cls.DEFAULTS['bot.auto_accept_invites']).lower()}",
            f"paths.audio_dir = {cls.DEFAULTS['paths.audio_dir']}",
            f"paths.saved_queues_file = {cls.DEFAULTS['paths.saved_queues_file']}",
            f"audio.auto_advance_buffer = {cls.DEFAULTS['audio.auto_advance_buffer']}",
            f"audio.preroll_silence = {cls.DEFAULTS['audio.preroll_silence']}",
            f"audio.normalize_audio = {str(cls.DEFAULTS['audio.normalize_audio']).lower()}",
            f"audio.fade_in_ms = {cls.DEFAULTS['audio.fade_in_ms']}",
            f"audio.volume_percent = {cls.DEFAULTS['audio.volume_percent']}",
            "audio.cache_mode = size_lru | always_delete | never_delete",
            f"audio.cache_max_bytes = {cls.DEFAULTS['audio.cache_max_bytes']} (1GB)",
            f"audio.cache_delete_after_playback = {str(cls.DEFAULTS['audio.cache_delete_after_playback']).lower()}",
            f"audio.cache_delete_on_shutdown = {str(cls.DEFAULTS['audio.cache_delete_on_shutdown']).lower()}",
            f"worker.max_restart_attempts = {cls.DEFAULTS['worker.max_restart_attempts']}",
            f"worker.heartbeat_interval_seconds = {cls.DEFAULTS['worker.heartbeat_interval_seconds']}",
            f"worker.skip_cooldown_seconds = {cls.DEFAULTS['worker.skip_cooldown_seconds']}",
            f"worker.stop_timeout_restart_threshold = {cls.DEFAULTS['worker.stop_timeout_restart_threshold']}",
            "worker.membership_mode = matrix2_auto | matrix2 | legacy",
            f"worker.log_max_bytes = {cls.DEFAULTS['worker.log_max_bytes']}",
            f"worker.log_backups = {cls.DEFAULTS['worker.log_backups']}",
            f"logging.file = {cls.DEFAULTS['logging.file']}",
            f"logging.clean_enabled = {str(cls.DEFAULTS['logging.clean_enabled']).lower()}",
            f"logging.clean_file = {cls.DEFAULTS['logging.clean_file']}",
            (
                "logging.clean_filter_matrixrtc_noise = "
                f"{str(cls.DEFAULTS['logging.clean_filter_matrixrtc_noise']).lower()}"
            ),
            f"logging.max_bytes = {cls.DEFAULTS['logging.max_bytes']}",
            f"logging.backups = {cls.DEFAULTS['logging.backups']}",
            f"ui.show_progress_messages = {str(cls.DEFAULTS['ui.show_progress_messages']).lower()}",
            f"ui.rich_formatting = {str(cls.DEFAULTS['ui.rich_formatting']).lower()}",
            f"audio.cache_max_bytes minimum in size_lru mode = {MIN_AUDIO_CACHE_MAX_BYTES} (200MB)",
        ]
        return "\n".join(lines)
