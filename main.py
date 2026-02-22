import asyncio
import logging
from logging.handlers import RotatingFileHandler

from bot import IntegratedBot
from config import Config

logger = logging.getLogger(__name__)


class CleanLogNoiseFilter(logging.Filter):
    def __init__(self, enable_matrixrtc_filter: bool):
        super().__init__()
        self._enable_matrixrtc_filter = bool(enable_matrixrtc_filter)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._enable_matrixrtc_filter:
            return True

        message = record.getMessage()
        noisy_parts = (
            "[MatrixRTCSession",
            "MembershipManager",
            "RestartDelayedEvent",
            "Date.now:",
            "Queue: [",
        )
        if any(part in message for part in noisy_parts):
            return False
        return True


def setup_logging(config: Config):
    config.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        filename=config.LOG_FILE,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUPS,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    clean_file_handler = None
    if config.CLEAN_LOG_ENABLED:
        config.CLEAN_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        clean_file_handler = RotatingFileHandler(
            filename=config.CLEAN_LOG_FILE,
            maxBytes=config.LOG_MAX_BYTES,
            backupCount=config.LOG_BACKUPS,
            encoding="utf-8",
        )
        clean_file_handler.setFormatter(formatter)
        clean_file_handler.addFilter(CleanLogNoiseFilter(config.CLEAN_LOG_FILTER_MATRIXRTC_NOISE))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(stream_handler)
    root.addHandler(file_handler)
    if clean_file_handler is not None:
        root.addHandler(clean_file_handler)


async def main():
    config = Config()
    setup_logging(config)
    bot = IntegratedBot(config)

    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
