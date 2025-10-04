"""
Prometheus metrics for user position monitoring system.
Non-intrusive monitoring for user tracking and position updates.
"""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
from functools import wraps
import time
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Create a dedicated registry for user monitoring metrics
USER_REGISTRY = CollectorRegistry()

# =============================================================================
# USER POSITION METRICS
# =============================================================================

USER_SNAPSHOTS_PROCESSED = Counter(
    'user_snapshots_processed_total',
    'User position snapshots processed',
    ['status'],  # success, failed, skipped
    registry=USER_REGISTRY
)

USER_SNAPSHOT_DURATION = Histogram(
    'user_snapshot_duration_seconds',
    'Time to process user snapshots',
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    registry=USER_REGISTRY
)

USER_POSITIONS_UPDATED = Counter(
    'user_positions_updated_total', 
    'User positions updated in database',
    ['market'],  # LINK, BTC, ETH, etc.
    registry=USER_REGISTRY
)

USER_ADDRESSES_DISCOVERED = Gauge(
    'user_addresses_discovered_count',
    'Number of unique user addresses discovered',
    registry=USER_REGISTRY
)

USER_ADDRESSES_REMOVED = Counter(
    'user_addresses_removed_total',
    'User addresses removed from tracking',
    registry=USER_REGISTRY
)

ACTIVE_POSITIONS = Gauge(
    'active_positions_count',
    'Number of active positions',
    ['market'],
    registry=USER_REGISTRY
)

POSITION_VALUE_TOTAL = Gauge(
    'position_value_total_usd',
    'Total position value in USD',
    ['market', 'side'],  # long, short
    registry=USER_REGISTRY
)

# =============================================================================
# COMPONENT HEALTH METRICS
# =============================================================================

COMPONENT_HEALTH = Gauge(
    'component_health_status',
    'Health status of monitoring components (1=healthy, 0=unhealthy)',
    ['component'],  # snapshot_processor, address_manager, position_updater
    registry=USER_REGISTRY
)

COMPONENT_ERRORS = Counter(
    'component_errors_total',
    'Errors in monitoring components',
    ['component', 'error_type'],
    registry=USER_REGISTRY
)

CONSECUTIVE_COMPONENT_ERRORS = Gauge(
    'consecutive_component_errors',
    'Consecutive errors in components',
    ['component'],
    registry=USER_REGISTRY
)

COMPONENT_LAST_SUCCESS = Gauge(
    'component_last_success_timestamp',
    'Unix timestamp of last successful operation',
    ['component'],
    registry=USER_REGISTRY
)

# =============================================================================
# API METRICS
# =============================================================================

USER_API_QUERIES = Counter(
    'user_api_queries_total',
    'User monitoring API queries',  
    ['query_type', 'status'],  # query_type: snapshot, clearinghouse, user_states
    registry=USER_REGISTRY
)

USER_API_DURATION = Histogram(
    'user_api_duration_seconds',
    'User API query duration',
    ['query_type'],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=USER_REGISTRY
)

USER_API_ERRORS = Counter(
    'user_api_errors_total',
    'User API errors',
    ['query_type', 'error_code'],
    registry=USER_REGISTRY
)

# =============================================================================
# DATABASE METRICS
# =============================================================================

USER_DB_OPERATIONS = Counter(
    'user_db_operations_total',
    'User database operations',
    ['operation', 'table', 'status'],
    registry=USER_REGISTRY
)

USER_DB_OPERATION_DURATION = Histogram(
    'user_db_operation_duration_seconds',
    'User database operation duration',
    ['operation', 'table'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
    registry=USER_REGISTRY
)

USER_DB_BATCH_SIZE = Histogram(
    'user_db_batch_size',
    'Database batch operation size',
    ['operation', 'table'],
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
    registry=USER_REGISTRY
)

# =============================================================================
# PROCESSING METRICS
# =============================================================================

PROCESSING_CYCLE_DURATION = Histogram(
    'processing_cycle_duration_seconds',
    'Duration of complete processing cycle',
    ['cycle_type'],  # snapshot, position_update, address_discovery
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
    registry=USER_REGISTRY
)

PROCESSING_QUEUE_SIZE = Gauge(
    'processing_queue_size',
    'Size of processing queue',
    ['queue_type'],
    registry=USER_REGISTRY
)

PROCESSING_LAG = Gauge(
    'processing_lag_seconds',
    'Processing lag behind real-time',
    registry=USER_REGISTRY
)

# =============================================================================
# MONITORING STATISTICS
# =============================================================================

MONITOR_STATISTICS = Gauge(
    'monitor_statistics',
    'Monitoring statistics',
    ['metric'],  # snapshots_failed, addresses_discovered, positions_updated, etc.
    registry=USER_REGISTRY
)

MONITOR_UPTIME = Gauge(
    'monitor_uptime_seconds',
    'Monitor uptime in seconds',
    registry=USER_REGISTRY
)

# =============================================================================
# DECORATOR FUNCTIONS
# =============================================================================

def monitor_snapshot_processing(func):
    """Monitor snapshot processing operations."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start_time
            
            USER_SNAPSHOTS_PROCESSED.labels(status='success').inc()
            USER_SNAPSHOT_DURATION.observe(duration)
            COMPONENT_LAST_SUCCESS.labels(component='snapshot_processor').set(time.time())
            CONSECUTIVE_COMPONENT_ERRORS.labels(component='snapshot_processor').set(0)
            
            return result
        except Exception as e:
            USER_SNAPSHOTS_PROCESSED.labels(status='failed').inc()
            COMPONENT_ERRORS.labels(
                component='snapshot_processor',
                error_type=type(e).__name__
            ).inc()
            
            # Increment consecutive errors
            try:
                current = CONSECUTIVE_COMPONENT_ERRORS.labels(component='snapshot_processor')._value.get()
                CONSECUTIVE_COMPONENT_ERRORS.labels(component='snapshot_processor').set(current + 1)
            except:
                CONSECUTIVE_COMPONENT_ERRORS.labels(component='snapshot_processor').set(1)
            
            raise
    return wrapper

def monitor_position_update(func):
    """Monitor position update operations."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start_time
            
            PROCESSING_CYCLE_DURATION.labels(cycle_type='position_update').observe(duration)
            COMPONENT_LAST_SUCCESS.labels(component='position_updater').set(time.time())
            CONSECUTIVE_COMPONENT_ERRORS.labels(component='position_updater').set(0)
            
            return result
        except Exception as e:
            COMPONENT_ERRORS.labels(
                component='position_updater',
                error_type=type(e).__name__
            ).inc()
            
            # Increment consecutive errors
            try:
                current = CONSECUTIVE_COMPONENT_ERRORS.labels(component='position_updater')._value.get()
                CONSECUTIVE_COMPONENT_ERRORS.labels(component='position_updater').set(current + 1)
            except:
                CONSECUTIVE_COMPONENT_ERRORS.labels(component='position_updater').set(1)
            
            raise
    return wrapper

def monitor_api_query(query_type: str):
    """Monitor API query operations."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                USER_API_QUERIES.labels(query_type=query_type, status='success').inc()
                USER_API_DURATION.labels(query_type=query_type).observe(duration)
                
                return result
            except Exception as e:
                USER_API_QUERIES.labels(query_type=query_type, status='error').inc()
                
                error_code = 'timeout' if 'timeout' in str(e).lower() else 'unknown'
                if '429' in str(e):
                    error_code = 'rate_limit'
                elif '500' in str(e) or '502' in str(e) or '503' in str(e):
                    error_code = 'server_error'
                
                USER_API_ERRORS.labels(query_type=query_type, error_code=error_code).inc()
                raise
        return wrapper
    return decorator

def monitor_db_operation(operation: str, table: str):
    """Monitor database operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                USER_DB_OPERATIONS.labels(
                    operation=operation,
                    table=table,
                    status='success'
                ).inc()
                USER_DB_OPERATION_DURATION.labels(
                    operation=operation,
                    table=table
                ).observe(duration)
                
                # Track batch size if applicable
                if operation in ['bulk_insert', 'bulk_update'] and isinstance(result, (list, tuple)):
                    USER_DB_BATCH_SIZE.labels(
                        operation=operation,
                        table=table
                    ).observe(len(result))
                
                return result
            except Exception as e:
                USER_DB_OPERATIONS.labels(
                    operation=operation,
                    table=table,
                    status='error'
                ).inc()
                raise
        return wrapper
    return decorator

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def update_component_health(component: str, healthy: bool, error_count: int = 0):
    """Update component health status."""
    try:
        COMPONENT_HEALTH.labels(component=component).set(1 if healthy else 0)
        if healthy:
            CONSECUTIVE_COMPONENT_ERRORS.labels(component=component).set(0)
        else:
            CONSECUTIVE_COMPONENT_ERRORS.labels(component=component).set(error_count)
    except Exception as e:
        logger.error(f"Error updating component health for {component}: {e}")

def update_monitor_statistics(stats: Dict[str, Any]):
    """Update monitoring statistics from MonitorStatistics object."""
    try:
        if 'snapshots_processed' in stats:
            MONITOR_STATISTICS.labels(metric='snapshots_processed').set(stats['snapshots_processed'])
        if 'snapshots_failed' in stats:
            MONITOR_STATISTICS.labels(metric='snapshots_failed').set(stats['snapshots_failed'])
        if 'addresses_discovered' in stats:
            MONITOR_STATISTICS.labels(metric='addresses_discovered').set(stats['addresses_discovered'])
            USER_ADDRESSES_DISCOVERED.set(stats['addresses_discovered'])
        if 'addresses_removed' in stats:
            MONITOR_STATISTICS.labels(metric='addresses_removed').set(stats['addresses_removed'])
        if 'positions_updated' in stats:
            MONITOR_STATISTICS.labels(metric='positions_updated').set(stats['positions_updated'])
        if 'api_queries' in stats:
            MONITOR_STATISTICS.labels(metric='api_queries').set(stats['api_queries'])
        if 'api_errors' in stats:
            MONITOR_STATISTICS.labels(metric='api_errors').set(stats['api_errors'])
        if 'db_operations' in stats:
            MONITOR_STATISTICS.labels(metric='db_operations').set(stats['db_operations'])
        if 'db_errors' in stats:
            MONITOR_STATISTICS.labels(metric='db_errors').set(stats['db_errors'])
    except Exception as e:
        logger.error(f"Error updating monitor statistics: {e}")

def update_position_metrics(positions: list, market: str = None):
    """Update position-related metrics."""
    try:
        if market:
            # Count active positions for specific market
            active_count = len([p for p in positions if p.get('size', 0) != 0])
            ACTIVE_POSITIONS.labels(market=market).set(active_count)
            
            # Calculate total position values
            long_value = sum(float(p.get('notional', 0)) for p in positions if float(p.get('size', 0)) > 0)
            short_value = abs(sum(float(p.get('notional', 0)) for p in positions if float(p.get('size', 0)) < 0))
            
            POSITION_VALUE_TOTAL.labels(market=market, side='long').set(long_value)
            POSITION_VALUE_TOTAL.labels(market=market, side='short').set(short_value)
            
            # Update positions counter
            USER_POSITIONS_UPDATED.labels(market=market).inc(len(positions))
    except Exception as e:
        logger.error(f"Error updating position metrics: {e}")

def update_processing_lag(lag_seconds: float):
    """Update processing lag metric."""
    try:
        PROCESSING_LAG.set(lag_seconds)
    except Exception as e:
        logger.error(f"Error updating processing lag: {e}")

def update_monitor_uptime(start_time: float):
    """Update monitor uptime metric."""
    try:
        uptime = time.time() - start_time
        MONITOR_UPTIME.set(uptime)
    except Exception as e:
        logger.error(f"Error updating monitor uptime: {e}")

# =============================================================================
# SYSTEM METRICS FOR USER MONITORING
# =============================================================================

def update_user_system_metrics(process_name: str = 'user_monitoring'):
    """Update system-level metrics for user monitoring."""
    try:
        import psutil
        import threading
        
        # Get current process
        process = psutil.Process()
        
        # Memory metrics - reuse from market metrics if available
        from metrics_market import PYTHON_MEMORY_USAGE, CPU_USAGE_PERCENT, THREAD_COUNT
        
        memory_info = process.memory_info()
        PYTHON_MEMORY_USAGE.labels(process_name=process_name).set(memory_info.rss)
        
        # CPU metrics
        cpu_percent = process.cpu_percent(interval=0.1)
        CPU_USAGE_PERCENT.labels(process_name=process_name).set(cpu_percent)
        
        # Thread count
        THREAD_COUNT.labels(process_name=process_name).set(threading.active_count())
        
    except ImportError:
        # If market metrics not available, skip
        pass
    except Exception as e:
        logger.error(f"Error updating user system metrics: {e}")
