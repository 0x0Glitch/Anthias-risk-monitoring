"""Logging configuration for Hyperliquid Position Monitoring System."""
import logging
import logging.handlers
from pathlib import Path
from typing import Optional

from config.constants import LogConfig


class LoggingSetup:

    @staticmethod
    def setup_logging(log_dir: Path, log_level: str = "INFO", module_name: Optional[str] = None) -> logging.Logger:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger(module_name) if module_name else logging.getLogger()
        if logger.handlers:
            return logger
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        all_logs_handler = logging.handlers.RotatingFileHandler(
            log_dir / "monitor.log",
            maxBytes=LogConfig.MAX_LOG_SIZE,
            backupCount=LogConfig.LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        all_logs_handler.setLevel(logging.DEBUG)
        all_logs_handler.setFormatter(detailed_formatter)

        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / "errors.log",
            maxBytes=LogConfig.MAX_ERROR_LOG_SIZE,
            backupCount=LogConfig.ERROR_LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)

        logger.addHandler(all_logs_handler)
        logger.addHandler(error_handler)
        logger.addHandler(console_handler)

        logging.getLogger('asyncio').setLevel(logging.WARNING)
        logging.getLogger('asyncpg').setLevel(logging.WARNING)
        logging.getLogger('aiohttp').setLevel(logging.WARNING)

        return logger

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)

    @staticmethod
    def set_level(level: str, logger_name: Optional[str] = None):
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    @staticmethod
    def add_file_handler(
        logger: logging.Logger,
        file_path: Path,
        level: str = "DEBUG",
        max_bytes: int = LogConfig.MAX_LOG_SIZE,
        backup_count: int = LogConfig.LOG_BACKUP_COUNT
    ):
        """
        Add an additional file handler to a logger.

        Args:
            logger: Logger instance
            file_path: Path to log file
            level: Log level for this handler
            max_bytes: Maximum size before rotation
            backup_count: Number of backup files to keep
        """
        file_handler = logging.handlers.RotatingFileHandler(
            file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, level.upper(), logging.DEBUG))

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)


# Convenience function for backward compatibility
def setup_logging(log_dir: Path, log_level: str = "INFO") -> logging.Logger:
    """
    Convenience wrapper for LoggingSetup.setup_logging().

    Args:
        log_dir: Directory for log files
        log_level: Logging level

    Returns:
        Configured root logger
    """
    return LoggingSetup.setup_logging(log_dir, log_level)