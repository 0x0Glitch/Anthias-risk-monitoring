"""
Logging configuration for Hyperliquid Position Monitoring System.
Provides centralized logging setup with rotation, formatting, and multiple handlers.
"""
import logging
import logging.handlers
from pathlib import Path
from typing import Optional

from config.constants import LogConfig


class LoggingSetup:
    """Centralized logging configuration manager."""

    @staticmethod
    def setup_logging(
        log_dir: Path,
        log_level: str = "INFO",
        module_name: Optional[str] = None
    ) -> logging.Logger:
        """
        Setup comprehensive logging with file rotation and console output.

        Args:
            log_dir: Directory for log files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            module_name: Name for the logger (defaults to root if None)

        Returns:
            Configured logger instance
        """
        # Create logs directory
        log_dir.mkdir(parents=True, exist_ok=True)

        # Get logger
        logger = logging.getLogger(module_name) if module_name else logging.getLogger()

        # Only configure if not already configured (avoid duplicate handlers)
        if logger.handlers:
            return logger

        # Set log level
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # File handler for all logs (with rotation)
        all_logs_handler = logging.handlers.RotatingFileHandler(
            log_dir / "monitor.log",
            maxBytes=LogConfig.MAX_LOG_SIZE,
            backupCount=LogConfig.LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        all_logs_handler.setLevel(logging.DEBUG)
        all_logs_handler.setFormatter(detailed_formatter)

        # File handler for errors only (with rotation)
        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / "errors.log",
            maxBytes=LogConfig.MAX_ERROR_LOG_SIZE,
            backupCount=LogConfig.ERROR_LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)

        # Console handler for user-facing logs
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)

        # Add handlers to logger
        logger.addHandler(all_logs_handler)
        logger.addHandler(error_handler)
        logger.addHandler(console_handler)

        # Reduce noise from third-party libraries
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        logging.getLogger('asyncpg').setLevel(logging.WARNING)
        logging.getLogger('aiohttp').setLevel(logging.WARNING)

        return logger

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Get a logger for a specific module.

        Args:
            name: Module name (typically __name__)

        Returns:
            Logger instance
        """
        return logging.getLogger(name)

    @staticmethod
    def set_level(level: str, logger_name: Optional[str] = None):
        """
        Dynamically change log level.

        Args:
            level: New log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            logger_name: Specific logger to update (None for root)
        """
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