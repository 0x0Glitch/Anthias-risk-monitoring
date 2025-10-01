"""
Production-grade Hyperliquid Position Monitor with enterprise-level reliability.
"""
import asyncio
import signal
import sys
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List, Any
from dataclasses import dataclass, field
from pathlib import Path
import traceback

from config.config import MonitorConfig
from config.logging_config import LoggingSetup
from config.constants import (
    SystemState,
    ProcessingIntervals,
    MonitoringThresholds
)
from core.snapshot_processor import SnapshotProcessor
from core.address_manager import AddressManager
from core.position_updater import PositionUpdater
from db.db_manager import DatabaseManager


@dataclass
class ComponentHealth:
    """Health status of a component."""
    name: str
    healthy: bool
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    error_count: int = 0
    consecutive_errors: int = 0


@dataclass
class MonitorStatistics:
    """Comprehensive monitoring statistics."""
    start_time: datetime = field(default_factory=datetime.now)
    snapshots_processed: int = 0
    snapshots_failed: int = 0
    addresses_discovered: int = 0
    addresses_removed: int = 0
    positions_updated: int = 0
    api_queries: int = 0
    api_errors: int = 0
    db_operations: int = 0
    db_errors: int = 0
    last_snapshot_time: Optional[datetime] = None
    last_position_update: Optional[datetime] = None
    
    def uptime(self) -> timedelta:
        return datetime.now() - self.start_time
    
    def to_dict(self) -> dict:
        return {
            'uptime_seconds': self.uptime().total_seconds(),
            'snapshots_processed': self.snapshots_processed,
            'snapshots_failed': self.snapshots_failed,
            'addresses_discovered': self.addresses_discovered,
            'addresses_removed': self.addresses_removed,
            'positions_updated': self.positions_updated,
            'api_queries': self.api_queries,
            'api_errors': self.api_errors,
            'api_error_rate': (self.api_errors / max(1, self.api_queries)) * 100,
            'db_operations': self.db_operations,
            'db_errors': self.db_errors,
            'last_snapshot': self.last_snapshot_time.isoformat() if self.last_snapshot_time else None,
            'last_position_update': self.last_position_update.isoformat() if self.last_position_update else None
        }


class HyperliquidMonitor:
    """
    Enterprise-grade monitor with:
    - Comprehensive error handling and recovery
    - Health monitoring for all components  
    - Graceful degradation
    - Automatic recovery mechanisms
    - Detailed statistics and metrics
    - Production logging
    """
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        
        # Setup logging using centralized configuration
        log_dir = config.data_dir / "logs"
        self.logger = LoggingSetup.setup_logging(log_dir, log_level="INFO")
        
        self.state = SystemState.INITIALIZING
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.force_shutdown = False
        
        # Components
        self.db_manager = DatabaseManager(config)
        self.snapshot_processor = SnapshotProcessor(config)
        self.address_manager = AddressManager(config)
        self.position_updater = PositionUpdater(config, self.db_manager)
        # Removed system_monitor as it doesn't exist in HIP3-users
        
        # Health monitoring
        self.component_health = {
            'database': ComponentHealth('database', True),
            'snapshot_processor': ComponentHealth('snapshot_processor', True),
            'position_updater': ComponentHealth('position_updater', True),
            'address_manager': ComponentHealth('address_manager', True),
        }
        
        # Statistics
        self.stats = MonitorStatistics()
        self.stats_file = config.data_dir / "monitor_stats.json"
        
        # Task management
        self.tasks: List[asyncio.Task] = []
        self.task_errors: Dict[str, int] = {}
        self.max_task_errors = MonitoringThresholds.MAX_TASK_ERRORS
        
        # Rate limiting
        self.snapshot_cooldown = timedelta(seconds=ProcessingIntervals.SNAPSHOT_COOLDOWN)
        self.last_snapshot_attempt = datetime.min
        
        self.logger.info("Monitor initialized")
    
    async def start(self) -> None:
        """Start the monitoring system with comprehensive initialization."""
        
        self.logger.info("="*80)
        self.logger.info("HYPERLIQUID POSITION MONITOR v2.0 - PRODUCTION")
        self.logger.info(f"Target Markets: {', '.join(self.config.target_markets)}")
        self.logger.info(f"Min Position Size: ${self.config.min_position_size_usd:,.2f}")
        self.logger.info(f"Environment: {os.getenv('ENV', 'production')}")
        self.logger.info("="*80)
        
        try:
            # Initialize all components
            await self._initialize_components()
            
            # Setup signal handlers
            self._setup_signal_handlers()
            
            # Perform initial seeding
            await self._initial_seeding()
            
            # Start monitoring tasks
            self.running = True
            self.state = SystemState.RUNNING
            await self._start_tasks()
            
            self.logger.info("Monitor started successfully")
            
            # Main event loop
            await self._main_loop()
            
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.critical(f"Fatal error in monitor: {e}", exc_info=True)
            self.state = SystemState.FATAL
            raise
        finally:
            await self.stop()
    
    async def _initialize_components(self) -> None:
        """Initialize all components with proper error handling."""
        
        self.logger.info("Initializing components...")
        
        # Initialize database
        try:
            await self.db_manager.initialize()
            self.component_health['database'].healthy = True
            self.component_health['database'].last_success = datetime.now()
            self.logger.info("✓ Database initialized")
        except Exception as e:
            self.logger.error(f"✗ Database initialization failed: {e}")
            self.component_health['database'].healthy = False
            self.component_health['database'].last_error = str(e)
            raise
        
        # Initialize position updater
        try:
            await self.position_updater.start()
            self.component_health['position_updater'].healthy = True
            self.logger.info("✓ Position updater initialized")
        except Exception as e:
            self.logger.warning(f"⚠ Position updater initialization warning: {e}")
            self.component_health['position_updater'].healthy = False
        
        # Validate configuration
        self._validate_configuration()
        
        self.state = SystemState.READY
        self.logger.info("All components initialized")
    
    def _validate_configuration(self) -> None:
        """Validate configuration and environment."""
        
        issues = []
        
        # Check RMP path
        if not self.config.rmp_base_path.exists():
            issues.append(f"RMP path does not exist: {self.config.rmp_base_path}")
        
        # Check node binary
        if not self.config.node_binary_path.exists():
            issues.append(f"hl-node binary not found: {self.config.node_binary_path}")
        
        # Check data directory
        if not self.config.data_dir.exists():
            self.config.data_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created data directory: {self.config.data_dir}")
        
        if issues:
            self.logger.warning("Configuration issues detected:")
            for issue in issues:
                self.logger.warning(f"  - {issue}")
            self.state = SystemState.DEGRADED
    
    async def _initial_seeding(self) -> None:
        """Perform initial snapshot processing for seeding."""
        
        self.logger.info("Performing initial seeding from snapshots...")
        
        try:
            success, users_by_market = await self.snapshot_processor.process_latest_snapshot()
            
            if success and users_by_market:
                # Replace all addresses with those from snapshot (only active positions)
                stats = await self.address_manager.replace_addresses(users_by_market)
                self.stats.addresses_discovered = stats['total']
                self.logger.info(f"Address manager updated: {stats['total']} addresses with active positions")
                
                # Comprehensive position update for ALL addresses from snapshot
                if users_by_market:
                    total_addrs = sum(len(addrs) for addrs in users_by_market.values())
                    self.logger.info(f"Performing comprehensive initial position update for {total_addrs} addresses...")
                    updated = await self.position_updater.update_positions(users_by_market)
                    # Handle both int and dict return types
                    if isinstance(updated, dict):
                        self.stats.positions_updated += len(updated)
                        self.logger.info(f"✓ Initial positions updated: {len(updated)} addresses processed")
                    else:
                        self.stats.positions_updated += updated
                        self.logger.info(f"✓ Initial positions updated: {updated} addresses processed")
                    
                self.stats.snapshots_processed += 1
                self.stats.last_snapshot_time = datetime.now()
                self.logger.info("✓ Initial seeding completed")
            else:
                self.logger.warning("⚠ No data from initial snapshot")
                
        except Exception as e:
            self.logger.error(f"Initial seeding error: {e}", exc_info=True)
            # Continue anyway - system can recover
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        
        def signal_handler(sig, frame):
            if self.force_shutdown:
                self.logger.warning("Force shutdown requested - terminating")
                os._exit(1)
            
            if not self.running:
                self.logger.info("Already shutting down, force quit with another signal")
                self.force_shutdown = True
                return
            
            self.logger.info(f"Received signal {sig}, initiating graceful shutdown...")
            self.running = False
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def _start_tasks(self) -> None:
        """Start all monitoring tasks."""
        
        self.tasks = [
            asyncio.create_task(self._snapshot_processor_task(), name="snapshot_processor"),
            asyncio.create_task(self._position_updater_task(), name="position_updater"),
            asyncio.create_task(self._health_monitor_task(), name="health_monitor"),
            asyncio.create_task(self._stats_reporter_task(), name="stats_reporter"),
            asyncio.create_task(self._cleanup_task(), name="cleanup")
        ]
        
        self.logger.info(f"Started {len(self.tasks)} monitoring tasks")
    
    async def _main_loop(self) -> None:
        """Main event loop with task monitoring."""
        
        while self.running:
            try:
                # Check task health
                failed_tasks = []
                for task in self.tasks:
                    if task.done():
                        task_name = task.get_name()
                        if task.cancelled():
                            self.logger.warning(f"Task {task_name} was cancelled")
                        elif exception := task.exception():
                            self.logger.error(f"Task {task_name} failed: {exception}")
                            self.task_errors[task_name] = self.task_errors.get(task_name, 0) + 1
                            
                            # Restart task if not too many errors
                            if self.task_errors[task_name] < self.max_task_errors:
                                self.logger.info(f"Restarting task {task_name}")
                                new_task = await self._restart_task(task_name)
                                if new_task:
                                    self.tasks.remove(task)
                                    self.tasks.append(new_task)
                            else:
                                failed_tasks.append(task_name)
                
                # Check if too many tasks have failed
                if len(failed_tasks) > 2:
                    self.logger.critical("Too many tasks have failed, entering error state")
                    self.state = SystemState.ERROR
                    break
                
                # Wait for shutdown signal or timeout
                await asyncio.wait_for(
                    self.shutdown_event.wait(),
                    timeout=1.0
                )
                
            except asyncio.TimeoutError:
                # Normal timeout, continue loop
                pass
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
    
    async def _restart_task(self, task_name: str) -> Optional[asyncio.Task]:
        """Restart a failed task."""
        
        task_map = {
            "snapshot_processor": self._snapshot_processor_task,
            "position_updater": self._position_updater_task,
            "health_monitor": self._health_monitor_task,
            "stats_reporter": self._stats_reporter_task,
            "cleanup": self._cleanup_task
        }
        
        if task_func := task_map.get(task_name):
            return asyncio.create_task(task_func(), name=task_name)
        
        return None
    
    async def _snapshot_processor_task(self) -> None:
        """Task for processing RMP snapshots."""
        
        while self.running:
            try:
                # Rate limiting
                time_since_last = datetime.now() - self.last_snapshot_attempt
                if time_since_last < self.snapshot_cooldown:
                    await asyncio.sleep((self.snapshot_cooldown - time_since_last).total_seconds())
                
                await asyncio.sleep(self.config.snapshot_check_interval)
                
                if not self.running:
                    break
                
                self.last_snapshot_attempt = datetime.now()
                
                # Process snapshot
                success, users_by_market = await self.snapshot_processor.process_latest_snapshot()
                
                if success:
                    self.stats.snapshots_processed += 1
                    self.component_health['snapshot_processor'].last_success = datetime.now()
                    self.component_health['snapshot_processor'].consecutive_errors = 0
                    
                    if users_by_market:
                        # Replace addresses with those from latest snapshot
                        stats = await self.address_manager.replace_addresses(users_by_market)
                        self.stats.addresses_discovered = stats['total']
                        self.stats.last_snapshot_time = datetime.now()
                        
                        self.logger.info(f"Snapshot processed: {stats['total']} addresses with positions "
                                       f"(+{stats['added']}/-{stats['removed']})")
                        
                        # Immediately update ALL positions from the new snapshot
                        self.logger.info(f"Performing comprehensive position update for {stats['total']} addresses from new snapshot...")
                        updated = await self.position_updater.update_positions(users_by_market)
                        total_updated = len(updated) if isinstance(updated, dict) else updated
                        self.logger.info(f"✓ Updated positions for {total_updated} addresses after snapshot load")
                else:
                    self.stats.snapshots_failed += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Snapshot processor error: {e}", exc_info=True)
                self.component_health['snapshot_processor'].consecutive_errors += 1
                self.component_health['snapshot_processor'].last_error = str(e)
                await asyncio.sleep(30)  # Backoff on error
    
    async def _position_updater_task(self) -> None:
        """Task for updating positions."""
        
        while self.running:
            try:
                await asyncio.sleep(self.config.position_refresh_interval)
                
                if not self.running:
                    break
                
                # Get addresses to update
                addresses_by_market = self.address_manager.get_addresses_by_market()
                
                if not addresses_by_market:
                    continue
                
                # Update positions
                updated = await self.position_updater.update_positions(addresses_by_market)
                total_updated = len(updated) if isinstance(updated, dict) else updated
                
                if total_updated > 0:
                    self.stats.positions_updated += total_updated
                    self.stats.last_position_update = datetime.now()
                    self.component_health['position_updater'].last_success = datetime.now()
                    self.component_health['position_updater'].consecutive_errors = 0
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Position updater error: {e}")
                self.component_health['position_updater'].consecutive_errors += 1
                await asyncio.sleep(10)
    
    async def _health_monitor_task(self) -> None:
        """Monitor component health and system state."""
        
        while self.running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                if not self.running:
                    break
                
                # Check component health
                unhealthy = []
                for name, health in self.component_health.items():
                    if health.consecutive_errors > MonitoringThresholds.MAX_CONSECUTIVE_ERRORS:
                        health.healthy = False
                        unhealthy.append(name)
                    elif health.consecutive_errors == 0 and not health.healthy:
                        health.healthy = True
                        self.logger.info(f"Component {name} recovered")
                
                # Update system state based on health
                if len(unhealthy) == 0:
                    if self.state == SystemState.DEGRADED:
                        self.state = SystemState.RUNNING
                        self.logger.info("System recovered to healthy state")
                elif len(unhealthy) <= MonitoringThresholds.MAX_DEGRADED_COMPONENTS:
                    if self.state != SystemState.DEGRADED:
                        self.state = SystemState.DEGRADED
                        self.logger.warning(f"System degraded, unhealthy components: {unhealthy}")
                else:
                    self.state = SystemState.ERROR
                    self.logger.error(f"System error, multiple failures: {unhealthy}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Health monitor error: {e}")
    
    async def _stats_reporter_task(self) -> None:
        """Report statistics periodically."""
        
        while self.running:
            try:
                await asyncio.sleep(300)  # Report every 5 minutes
                
                if not self.running:
                    break
                
                # Save stats to file
                self._save_stats()
                
                # Log stats
                stats_dict = self.stats.to_dict()
                self.logger.info("="*60)
                self.logger.info("SYSTEM STATISTICS")
                self.logger.info(f"State: {self.state.value}")
                self.logger.info(f"Uptime: {timedelta(seconds=int(stats_dict['uptime_seconds']))}")
                self.logger.info(f"Snapshots: {stats_dict['snapshots_processed']} processed, {stats_dict['snapshots_failed']} failed")
                self.logger.info(f"Addresses: {stats_dict['addresses_discovered']} discovered, {stats_dict['addresses_removed']} removed")
                self.logger.info(f"Positions: {stats_dict['positions_updated']} updated")
                self.logger.info(f"API: {stats_dict['api_queries']} queries, {stats_dict['api_error_rate']:.1f}% error rate")
                
                # Log database stats
                try:
                    db_stats = await self.db_manager.get_stats()
                    self.logger.info(f"Database: {db_stats.get('total_positions', 0)} positions, "
                                   f"${db_stats.get('total_value_usd', 0):,.2f} total value")
                except Exception:
                    pass
                
                self.logger.info("="*60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Stats reporter error: {e}")
    
    async def _cleanup_task(self) -> None:
        """Periodic cleanup tasks."""
        
        while self.running:
            try:
                await asyncio.sleep(3600)  # Every hour
                
                if not self.running:
                    break
                
                self.logger.info("Running cleanup tasks...")
                
                # Clean up closed positions
                try:
                    cleaned = await self.db_manager.cleanup_closed_positions(max_age_hours=24)
                    if cleaned:
                        self.logger.info(f"Cleaned up {len(cleaned)} closed positions")
                except Exception as e:
                    self.logger.error(f"Database cleanup error: {e}")
                
                # Reset task error counts for recovered tasks
                for task_name in list(self.task_errors.keys()):
                    if self.task_errors[task_name] < self.max_task_errors / 2:
                        del self.task_errors[task_name]
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Cleanup task error: {e}")
    
    def _save_stats(self) -> None:
        """Save statistics to file."""
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats.to_dict(), f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save stats: {e}")
    
    async def stop(self) -> None:
        """Gracefully stop the monitor."""
        
        if self.state == SystemState.STOPPED:
            return
        
        self.logger.info("Initiating graceful shutdown...")
        self.state = SystemState.STOPPING
        self.running = False
        self.shutdown_event.set()
        
        # Cancel all tasks with timeout
        if self.tasks:
            self.logger.info("Cancelling tasks...")
            for task in self.tasks:
                task.cancel()
            
            # Wait for tasks with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some tasks did not shut down gracefully")
        
        # Stop components
        self.logger.info("Stopping components...")
        
        try:
            await self.position_updater.stop()
        except Exception as e:
            self.logger.error(f"Error stopping position updater: {e}")
        
        try:
            await self.db_manager.close()
        except Exception as e:
            self.logger.error(f"Error closing database: {e}")
        
        # Save final stats
        self._save_stats()
        
        self.state = SystemState.STOPPED
        self.logger.info("Monitor stopped successfully")


async def main():
    """Main entry point."""
    
    try:
        # Load configuration
        config = MonitorConfig.from_env()
        
        # Create and run monitor
        monitor = HyperliquidMonitor(config)
        await monitor.start()
        
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
