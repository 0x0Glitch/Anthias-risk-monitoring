"""System resource monitoring using psutil."""
import psutil
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from ..config.constants import (
    MEMORY_WARNING_THRESHOLD_MB,
    MEMORY_CRITICAL_THRESHOLD_MB,
    CPU_WARNING_THRESHOLD_PERCENT,
    CPU_CRITICAL_THRESHOLD_PERCENT
)


class SystemMonitor:
    """Monitor system resources and performance."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.process = psutil.Process()
        self.start_time = datetime.now()
        
    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics."""
        try:
            # Memory statistics
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            # CPU statistics
            cpu_percent = self.process.cpu_percent()
            
            # System-wide stats
            system_memory = psutil.virtual_memory()
            system_cpu = psutil.cpu_percent(interval=1)
            
            # Disk usage for current directory
            disk_usage = psutil.disk_usage('.')
            
            # Network statistics
            net_io = psutil.net_io_counters()
            
            uptime = datetime.now() - self.start_time
            
            return {
                'process': {
                    'memory_mb': round(memory_mb, 2),
                    'memory_percent': round(self.process.memory_percent(), 2),
                    'cpu_percent': round(cpu_percent, 2),
                    'num_threads': self.process.num_threads(),
                    'open_files': len(self.process.open_files()),
                    'connections': len(self.process.connections()),
                    'uptime_seconds': int(uptime.total_seconds())
                },
                'system': {
                    'memory_total_gb': round(system_memory.total / 1024 / 1024 / 1024, 2),
                    'memory_available_gb': round(system_memory.available / 1024 / 1024 / 1024, 2),
                    'memory_percent': system_memory.percent,
                    'cpu_percent': system_cpu,
                    'cpu_count': psutil.cpu_count(),
                    'disk_total_gb': round(disk_usage.total / 1024 / 1024 / 1024, 2),
                    'disk_free_gb': round(disk_usage.free / 1024 / 1024 / 1024, 2),
                    'disk_percent': round((disk_usage.used / disk_usage.total) * 100, 2)
                },
                'network': {
                    'bytes_sent': net_io.bytes_sent,
                    'bytes_recv': net_io.bytes_recv,
                    'packets_sent': net_io.packets_sent,
                    'packets_recv': net_io.packets_recv
                } if net_io else None
            }
            
        except Exception as e:
            self.logger.error(f"Error getting system stats: {e}")
            return {}
    
    def check_resource_health(self) -> Dict[str, Any]:
        """Check if system resources are within healthy limits."""
        health_status = {
            'healthy': True,
            'warnings': [],
            'critical': []
        }
        
        try:
            # Check memory usage
            memory_mb = self.process.memory_info().rss / 1024 / 1024
            
            if memory_mb > MEMORY_CRITICAL_THRESHOLD_MB:
                health_status['healthy'] = False
                health_status['critical'].append(f"Memory usage critical: {memory_mb:.1f}MB")
            elif memory_mb > MEMORY_WARNING_THRESHOLD_MB:
                health_status['warnings'].append(f"Memory usage high: {memory_mb:.1f}MB")
            
            # Check CPU usage
            cpu_percent = self.process.cpu_percent()
            
            if cpu_percent > CPU_CRITICAL_THRESHOLD_PERCENT:
                health_status['healthy'] = False
                health_status['critical'].append(f"CPU usage critical: {cpu_percent:.1f}%")
            elif cpu_percent > CPU_WARNING_THRESHOLD_PERCENT:
                health_status['warnings'].append(f"CPU usage high: {cpu_percent:.1f}%")
            
            # Check system memory
            system_memory = psutil.virtual_memory()
            if system_memory.percent > 90:
                health_status['critical'].append(f"System memory critical: {system_memory.percent:.1f}%")
                health_status['healthy'] = False
            elif system_memory.percent > 80:
                health_status['warnings'].append(f"System memory high: {system_memory.percent:.1f}%")
            
        except Exception as e:
            health_status['healthy'] = False
            health_status['critical'].append(f"Failed to check resources: {e}")
        
        return health_status
