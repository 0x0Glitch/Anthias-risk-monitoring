"""Centralized logging configuration for the monitoring system."""
import logging
import logging.handlers
from pathlib import Path
from typing import Optional
import sys


class LoggingSetup:
    """Centralized logging configuration."""
    
    @staticmethod
    def setup_logging(log_dir: Path, log_level: str = "INFO") -> logging.Logger:
        """Setup comprehensive logging configuration."""
        
        # Ensure log directory exists
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # Console handler for real-time monitoring
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # Main log file handler with rotation
        main_log_file = log_dir / "monitor.log"
        file_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)
        
        # Error log file handler
        error_log_file = log_dir / "error.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(error_handler)
        
        # Performance log for timing critical operations
        perf_log_file = log_dir / "performance.log"
        perf_handler = logging.handlers.RotatingFileHandler(
            perf_log_file,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=2,
            encoding='utf-8'
        )
        perf_handler.setLevel(logging.INFO)
        perf_handler.setFormatter(detailed_formatter)
        
        # Create performance logger
        perf_logger = logging.getLogger('performance')
        perf_logger.addHandler(perf_handler)
        perf_logger.setLevel(logging.INFO)
        perf_logger.propagate = False
        
        # Configure third-party loggers
        logging.getLogger('asyncpg').setLevel(logging.WARNING)
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        # Return main logger
        main_logger = logging.getLogger('hyperliquid_monitor')
        main_logger.info("Logging system initialized")
        main_logger.info(f"Log directory: {log_dir}")
        main_logger.info(f"Log level: {log_level}")
        
        return main_logger
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get a logger with the specified name."""
        return logging.getLogger(name)
    
    @staticmethod
    def log_performance(operation: str, duration_ms: float, details: Optional[dict] = None):
        """Log performance metrics."""
        perf_logger = logging.getLogger('performance')
        message = f"{operation}: {duration_ms:.2f}ms"
        if details:
            detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
            message += f" | {detail_str}"
        perf_logger.info(message)
    
    @staticmethod
    def log_system_event(event_type: str, message: str, extra_data: Optional[dict] = None):
        """Log system events with structured data."""
        logger = logging.getLogger('system_events')
        log_message = f"[{event_type}] {message}"
        if extra_data:
            log_message += f" | Data: {extra_data}"
        logger.info(log_message)
