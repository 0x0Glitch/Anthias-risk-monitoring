"""Performance monitoring and metrics collection."""
import time
import asyncio
import logging
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class PerformanceMetrics:
    """Performance metrics for operations."""
    operation_name: str
    total_calls: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0.0
    error_count: int = 0
    last_call_time: Optional[float] = None
    
    def add_measurement(self, duration_ms: float, success: bool = True):
        """Add a performance measurement."""
        self.total_calls += 1
        self.total_duration_ms += duration_ms
        self.min_duration_ms = min(self.min_duration_ms, duration_ms)
        self.max_duration_ms = max(self.max_duration_ms, duration_ms)
        self.last_call_time = time.time()
        
        if not success:
            self.error_count += 1
    
    @property
    def average_duration_ms(self) -> float:
        """Calculate average duration."""
        if self.total_calls == 0:
            return 0.0
        return self.total_duration_ms / self.total_calls
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate as percentage."""
        if self.total_calls == 0:
            return 0.0
        return (self.error_count / self.total_calls) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'operation_name': self.operation_name,
            'total_calls': self.total_calls,
            'total_duration_ms': round(self.total_duration_ms, 2),
            'average_duration_ms': round(self.average_duration_ms, 2),
            'min_duration_ms': round(self.min_duration_ms, 2) if self.min_duration_ms != float('inf') else 0,
            'max_duration_ms': round(self.max_duration_ms, 2),
            'error_count': self.error_count,
            'error_rate': round(self.error_rate, 2),
            'last_call_time': self.last_call_time
        }


class PerformanceMonitor:
    """Global performance monitoring system."""
    
    def __init__(self):
        self.metrics: Dict[str, PerformanceMetrics] = {}
        self.logger = logging.getLogger('performance')
        
    def get_or_create_metric(self, operation_name: str) -> PerformanceMetrics:
        """Get or create a performance metric."""
        if operation_name not in self.metrics:
            self.metrics[operation_name] = PerformanceMetrics(operation_name)
        return self.metrics[operation_name]
    
    @asynccontextmanager
    async def measure_async(self, operation_name: str):
        """Async context manager for measuring operation performance."""
        start_time = time.time()
        success = True
        
        try:
            yield
        except Exception as e:
            success = False
            raise
        finally:
            duration_ms = (time.time() - start_time) * 1000
            metric = self.get_or_create_metric(operation_name)
            metric.add_measurement(duration_ms, success)
            
            # Log slow operations
            if duration_ms > 1000:  # > 1 second
                self.logger.warning(f"Slow operation {operation_name}: {duration_ms:.2f}ms")
            elif duration_ms > 100:  # > 100ms
                self.logger.info(f"Operation {operation_name}: {duration_ms:.2f}ms")
    
    @contextmanager
    def measure_sync(self, operation_name: str):
        """Sync context manager for measuring operation performance."""
        start_time = time.time()
        success = True
        
        try:
            yield
        except Exception as e:
            success = False
            raise
        finally:
            duration_ms = (time.time() - start_time) * 1000
            metric = self.get_or_create_metric(operation_name)
            metric.add_measurement(duration_ms, success)
    
    def record_timing(self, operation_name: str, duration_ms: float, success: bool = True):
        """Manually record a timing measurement."""
        metric = self.get_or_create_metric(operation_name)
        metric.add_measurement(duration_ms, success)
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get all performance metrics."""
        return {name: metric.to_dict() for name, metric in self.metrics.items()}
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of performance metrics."""
        total_operations = sum(metric.total_calls for metric in self.metrics.values())
        total_errors = sum(metric.error_count for metric in self.metrics.values())
        
        # Find slowest operations
        slowest_ops = sorted(
            [(name, metric.max_duration_ms) for name, metric in self.metrics.items()],
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        # Find most error-prone operations
        error_prone_ops = sorted(
            [(name, metric.error_rate) for name, metric in self.metrics.items() if metric.error_rate > 0],
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        return {
            'total_operations': total_operations,
            'total_errors': total_errors,
            'overall_error_rate': (total_errors / max(1, total_operations)) * 100,
            'tracked_operations': len(self.metrics),
            'slowest_operations': slowest_ops,
            'error_prone_operations': error_prone_ops
        }
    
    def reset_metrics(self):
        """Reset all performance metrics."""
        self.metrics.clear()


# Global performance monitor instance
performance_monitor = PerformanceMonitor()


def measure_performance(operation_name: str):
    """Decorator for measuring function performance."""
    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                async with performance_monitor.measure_async(operation_name):
                    return await func(*args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                with performance_monitor.measure_sync(operation_name):
                    return func(*args, **kwargs)
            return sync_wrapper
    return decorator
