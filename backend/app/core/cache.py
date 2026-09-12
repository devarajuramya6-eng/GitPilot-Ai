"""
CacheManager — Production In-Memory Caching with TTL & LRU Eviction Service
================================================================================
GitPilot AI Platform Module
Version: {}.{}.{}
================================================================================

This module provides production-grade In-Memory Caching with TTL & LRU Eviction capabilities for the GitPilot AI
intelligent GitHub issue triage and developer matching platform.

Architecture:
    - Retry-with-backoff for transient failures
    - Circuit breaker pattern for cascading failure prevention
    - LRU in-memory cache with TTL expiration
    - Thread-safe operations via RLock
    - Before/after lifecycle hook extensibility
    - Structured metrics collection
    - Full type annotations and docstrings
    - Comprehensive error hierarchy

Usage:
    service = CacheManager()
    result = service.get()
    if result.success:
        print(result.data)

Configuration:
    service = CacheManager(CacheManagerConfig(
        max_retries=5,
        timeout_seconds=60.0,
        cache_ttl_seconds=600,
    ))
"""

from __future__ import annotations
import logging
import time
import math
import re
import json
import hashlib
import secrets
import asyncio
import threading
import copy
import uuid
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union, Set, Type, Callable, Iterator, Generator
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, Counter, OrderedDict, deque
from functools import lru_cache, wraps, partial
from pathlib import Path
from contextlib import contextmanager, suppress

logger = logging.getLogger(__name__)


# ─── Module Constants ────────────────────────────────────────────────────────
MODULE_VERSION = "1.0.0"
MODULE_NAME = "CacheManager"
DEFAULT_TIMEOUT = 30.0
MAX_BATCH_SIZE = 1000
MIN_CONFIDENCE = 0.0
MAX_CONFIDENCE = 1.0
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500
CACHE_NAMESPACE = "IN-MEMORY_CACHING_WITH_TTL_&_LRU_EVICTION"
LOG_SAMPLING_RATE = 0.1


# ─── Type Aliases ─────────────────────────────────────────────────────────────
JSON = Dict[str, Any]
OptionalStr = Optional[str]
RecordList = List[Dict[str, Any]]
ScoreMap = Dict[str, float]



class CacheManagerStatus(str, Enum):
    """Status codes for In-Memory Caching with TTL & LRU Eviction operations."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"

class CacheManagerPriority(str, Enum):
    """Priority levels for In-Memory Caching with TTL & LRU Eviction."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    BACKGROUND = "background"

@dataclass
class CacheManagerResult:
    """Result container for In-Memory Caching with TTL & LRU Eviction operations."""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: float = 0.0
    attempts: int = 1
    status: str = CacheManagerStatus.COMPLETED

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary representation."""
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "duration_ms": self.duration_ms,
            "attempts": self.attempts,
            "status": self.status,
        }

    @classmethod
    def ok(cls, data: Any, **kwargs) -> 'CacheManagerResult':
        """Create successful result."""
        return cls(success=True, data=data, status=CacheManagerStatus.COMPLETED, **kwargs)

    @classmethod
    def err(cls, error: str, **kwargs) -> 'CacheManagerResult':
        """Create failed result."""
        return cls(success=False, error=error, status=CacheManagerStatus.FAILED, **kwargs)

@dataclass
class CacheManagerConfig:
    """Configuration for In-Memory Caching with TTL & LRU Eviction."""
    max_retries: int = 3
    timeout_seconds: float = 30.0
    batch_size: int = 100
    concurrency_limit: int = 10
    cache_ttl_seconds: int = 300
    log_level: str = "INFO"
    enable_metrics: bool = True
    enable_tracing: bool = False
    alert_threshold: float = 0.95
    backoff_factor: float = 2.0
    jitter: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0





class CacheManager:
    """In-Memory Caching with TTL & LRU Eviction service implementation for GitPilot AI.

    Provides comprehensive In-Memory Caching with TTL & LRU Eviction functionality with:
    - Circuit breaker pattern
    - Automatic retry with exponential backoff
    - Thread-safe operation
    - In-memory caching with TTL
    - Lifecycle hooks
    - Metrics collection
    - Health monitoring
    """

    def __init__(self, config: Optional[CacheManagerConfig] = None) -> None:
        """Initialize CacheManager with optional configuration.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        self.config = config or CacheManagerConfig()
        self._lock = threading.RLock()
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, float] = {}
        self._metrics: Dict[str, int] = defaultdict(int)
        self._circuit_breaker_failures = 0
        self._circuit_breaker_open = False
        self._circuit_breaker_reset_at: Optional[float] = None
        self._executor_pool: List[Any] = []
        self._hooks_before: List[Callable] = []
        self._hooks_after: List[Callable] = []
        logger.info(f'Initialized {self.__class__.__name__} with config: %s', self.config)

    async def get(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform get operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for get operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of get.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_cache_hit'] += 1
                logger.debug('Cache hit for get')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get')
                    result_data = self._execute_core_get(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_success'] += 1
                self._metrics['get_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_failure'] += 1
                    logger.error('get exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def set(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform set operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for set operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of set.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting set for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('set blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'set', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['set_cache_hit'] += 1
                logger.debug('Cache hit for set')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('set', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='set')
                    result_data = self._execute_core_set(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('set', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['set_success'] += 1
                self._metrics['set_total_ms'] += int(elapsed_ms)
                logger.info(
                    'set completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'set', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('set validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('set attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['set_failure'] += 1
                    logger.error('set exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def delete(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform delete operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for delete operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of delete.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting delete for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('delete blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'delete', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['delete_cache_hit'] += 1
                logger.debug('Cache hit for delete')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('delete', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='delete')
                    result_data = self._execute_core_dele(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('delete', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['delete_success'] += 1
                self._metrics['delete_total_ms'] += int(elapsed_ms)
                logger.info(
                    'delete completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'delete', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('delete validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('delete attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['delete_failure'] += 1
                    logger.error('delete exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def exists(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform exists operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for exists operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of exists.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting exists for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('exists blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'exists', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['exists_cache_hit'] += 1
                logger.debug('Cache hit for exists')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('exists', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='exists')
                    result_data = self._execute_core_exis(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('exists', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['exists_success'] += 1
                self._metrics['exists_total_ms'] += int(elapsed_ms)
                logger.info(
                    'exists completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'exists', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('exists validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('exists attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['exists_failure'] += 1
                    logger.error('exists exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def expire(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform expire operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for expire operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of expire.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting expire for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('expire blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'expire', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['expire_cache_hit'] += 1
                logger.debug('Cache hit for expire')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('expire', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='expire')
                    result_data = self._execute_core_expi(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('expire', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['expire_success'] += 1
                self._metrics['expire_total_ms'] += int(elapsed_ms)
                logger.info(
                    'expire completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'expire', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('expire validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('expire attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['expire_failure'] += 1
                    logger.error('expire exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def ttl(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform ttl operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for ttl operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of ttl.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting ttl for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('ttl blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'ttl', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['ttl_cache_hit'] += 1
                logger.debug('Cache hit for ttl')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('ttl', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='ttl')
                    result_data = self._execute_core_ttl(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('ttl', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['ttl_success'] += 1
                self._metrics['ttl_total_ms'] += int(elapsed_ms)
                logger.info(
                    'ttl completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'ttl', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('ttl validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('ttl attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['ttl_failure'] += 1
                    logger.error('ttl exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def mget(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform mget operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for mget operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of mget.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting mget for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('mget blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'mget', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['mget_cache_hit'] += 1
                logger.debug('Cache hit for mget')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('mget', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='mget')
                    result_data = self._execute_core_mget(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('mget', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['mget_success'] += 1
                self._metrics['mget_total_ms'] += int(elapsed_ms)
                logger.info(
                    'mget completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'mget', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('mget validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('mget attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['mget_failure'] += 1
                    logger.error('mget exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def mset(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform mset operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for mset operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of mset.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting mset for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('mset blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'mset', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['mset_cache_hit'] += 1
                logger.debug('Cache hit for mset')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('mset', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='mset')
                    result_data = self._execute_core_mset(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('mset', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['mset_success'] += 1
                self._metrics['mset_total_ms'] += int(elapsed_ms)
                logger.info(
                    'mset completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'mset', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('mset validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('mset attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['mset_failure'] += 1
                    logger.error('mset exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def mdelete(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform mdelete operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for mdelete operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of mdelete.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting mdelete for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('mdelete blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'mdelete', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['mdelete_cache_hit'] += 1
                logger.debug('Cache hit for mdelete')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('mdelete', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='mdelete')
                    result_data = self._execute_core_mdel(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('mdelete', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['mdelete_success'] += 1
                self._metrics['mdelete_total_ms'] += int(elapsed_ms)
                logger.info(
                    'mdelete completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'mdelete', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('mdelete validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('mdelete attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['mdelete_failure'] += 1
                    logger.error('mdelete exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def clear(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform clear operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for clear operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of clear.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting clear for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('clear blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'clear', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['clear_cache_hit'] += 1
                logger.debug('Cache hit for clear')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('clear', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='clear')
                    result_data = self._execute_core_clea(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('clear', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['clear_success'] += 1
                self._metrics['clear_total_ms'] += int(elapsed_ms)
                logger.info(
                    'clear completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'clear', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('clear validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('clear attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['clear_failure'] += 1
                    logger.error('clear exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def clear_namespace(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform clear namespace operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for clear_namespace operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of clear_namespace.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting clear_namespace for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('clear_namespace blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'clear_namespace', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['clear_namespace_cache_hit'] += 1
                logger.debug('Cache hit for clear_namespace')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('clear_namespace', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='clear_namespace')
                    result_data = self._execute_core_clea(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('clear_namespace', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['clear_namespace_success'] += 1
                self._metrics['clear_namespace_total_ms'] += int(elapsed_ms)
                logger.info(
                    'clear_namespace completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'clear_namespace', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('clear_namespace validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('clear_namespace attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['clear_namespace_failure'] += 1
                    logger.error('clear_namespace exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def scan_keys(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform scan keys operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for scan_keys operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of scan_keys.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting scan_keys for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('scan_keys blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'scan_keys', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['scan_keys_cache_hit'] += 1
                logger.debug('Cache hit for scan_keys')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('scan_keys', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='scan_keys')
                    result_data = self._execute_core_scan(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('scan_keys', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['scan_keys_success'] += 1
                self._metrics['scan_keys_total_ms'] += int(elapsed_ms)
                logger.info(
                    'scan_keys completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'scan_keys', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('scan_keys validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('scan_keys attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['scan_keys_failure'] += 1
                    logger.error('scan_keys exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def get_stats(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform get stats operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for get_stats operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of get_stats.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_stats for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_stats blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_stats', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_stats_cache_hit'] += 1
                logger.debug('Cache hit for get_stats')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_stats', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_stats')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_stats', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_stats_success'] += 1
                self._metrics['get_stats_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_stats completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_stats', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_stats validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_stats attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_stats_failure'] += 1
                    logger.error('get_stats exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def warm_cache(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform warm cache operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for warm_cache operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of warm_cache.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting warm_cache for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('warm_cache blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'warm_cache', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['warm_cache_cache_hit'] += 1
                logger.debug('Cache hit for warm_cache')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('warm_cache', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='warm_cache')
                    result_data = self._execute_core_warm(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('warm_cache', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['warm_cache_success'] += 1
                self._metrics['warm_cache_total_ms'] += int(elapsed_ms)
                logger.info(
                    'warm_cache completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'warm_cache', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('warm_cache validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('warm_cache attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['warm_cache_failure'] += 1
                    logger.error('warm_cache exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def invalidate_pattern(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform invalidate pattern operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for invalidate_pattern operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of invalidate_pattern.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting invalidate_pattern for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('invalidate_pattern blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'invalidate_pattern', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['invalidate_pattern_cache_hit'] += 1
                logger.debug('Cache hit for invalidate_pattern')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('invalidate_pattern', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='invalidate_pattern')
                    result_data = self._execute_core_inva(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('invalidate_pattern', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['invalidate_pattern_success'] += 1
                self._metrics['invalidate_pattern_total_ms'] += int(elapsed_ms)
                logger.info(
                    'invalidate_pattern completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'invalidate_pattern', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('invalidate_pattern validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('invalidate_pattern attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['invalidate_pattern_failure'] += 1
                    logger.error('invalidate_pattern exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def compute_cache_key(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform compute cache key operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for compute_cache_key operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of compute_cache_key.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_cache_key for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_cache_key blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_cache_key', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_cache_key_cache_hit'] += 1
                logger.debug('Cache hit for compute_cache_key')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_cache_key', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_cache_key')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_cache_key', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_cache_key_success'] += 1
                self._metrics['compute_cache_key_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_cache_key completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_cache_key', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_cache_key validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_cache_key attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_cache_key_failure'] += 1
                    logger.error('compute_cache_key exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def serialize(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform serialize operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for serialize operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of serialize.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting serialize for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('serialize blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'serialize', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['serialize_cache_hit'] += 1
                logger.debug('Cache hit for serialize')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('serialize', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='serialize')
                    result_data = self._execute_core_seri(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('serialize', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['serialize_success'] += 1
                self._metrics['serialize_total_ms'] += int(elapsed_ms)
                logger.info(
                    'serialize completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'serialize', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('serialize validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('serialize attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['serialize_failure'] += 1
                    logger.error('serialize exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def deserialize(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform deserialize operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for deserialize operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of deserialize.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting deserialize for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('deserialize blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'deserialize', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['deserialize_cache_hit'] += 1
                logger.debug('Cache hit for deserialize')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('deserialize', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='deserialize')
                    result_data = self._execute_core_dese(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('deserialize', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['deserialize_success'] += 1
                self._metrics['deserialize_total_ms'] += int(elapsed_ms)
                logger.info(
                    'deserialize completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'deserialize', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('deserialize validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('deserialize attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['deserialize_failure'] += 1
                    logger.error('deserialize exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def get_or_compute(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform get or compute operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for get_or_compute operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of get_or_compute.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_or_compute for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_or_compute blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_or_compute', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_or_compute_cache_hit'] += 1
                logger.debug('Cache hit for get_or_compute')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_or_compute', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_or_compute')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_or_compute', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_or_compute_success'] += 1
                self._metrics['get_or_compute_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_or_compute completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_or_compute', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_or_compute validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_or_compute attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_or_compute_failure'] += 1
                    logger.error('get_or_compute exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def cache_decorator(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform cache decorator operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for cache_decorator operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of cache_decorator.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting cache_decorator for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('cache_decorator blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'cache_decorator', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['cache_decorator_cache_hit'] += 1
                logger.debug('Cache hit for cache_decorator')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('cache_decorator', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='cache_decorator')
                    result_data = self._execute_core_cach(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('cache_decorator', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['cache_decorator_success'] += 1
                self._metrics['cache_decorator_total_ms'] += int(elapsed_ms)
                logger.info(
                    'cache_decorator completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'cache_decorator', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('cache_decorator validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('cache_decorator attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['cache_decorator_failure'] += 1
                    logger.error('cache_decorator exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def lru_evict(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform lru evict operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for lru_evict operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of lru_evict.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting lru_evict for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('lru_evict blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'lru_evict', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['lru_evict_cache_hit'] += 1
                logger.debug('Cache hit for lru_evict')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('lru_evict', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='lru_evict')
                    result_data = self._execute_core_lru_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('lru_evict', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['lru_evict_success'] += 1
                self._metrics['lru_evict_total_ms'] += int(elapsed_ms)
                logger.info(
                    'lru_evict completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'lru_evict', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('lru_evict validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('lru_evict attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['lru_evict_failure'] += 1
                    logger.error('lru_evict exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def ttl_evict(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform ttl evict operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for ttl_evict operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of ttl_evict.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting ttl_evict for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('ttl_evict blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'ttl_evict', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['ttl_evict_cache_hit'] += 1
                logger.debug('Cache hit for ttl_evict')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('ttl_evict', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='ttl_evict')
                    result_data = self._execute_core_ttl_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('ttl_evict', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['ttl_evict_success'] += 1
                self._metrics['ttl_evict_total_ms'] += int(elapsed_ms)
                logger.info(
                    'ttl_evict completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'ttl_evict', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('ttl_evict validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('ttl_evict attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['ttl_evict_failure'] += 1
                    logger.error('ttl_evict exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def get_lru_order(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform get lru order operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for get_lru_order operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of get_lru_order.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_lru_order for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_lru_order blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_lru_order', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_lru_order_cache_hit'] += 1
                logger.debug('Cache hit for get_lru_order')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_lru_order', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_lru_order')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_lru_order', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_lru_order_success'] += 1
                self._metrics['get_lru_order_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_lru_order completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_lru_order', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_lru_order validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_lru_order attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_lru_order_failure'] += 1
                    logger.error('get_lru_order exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def get_size(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform get size operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for get_size operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of get_size.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_size for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_size blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_size', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_size_cache_hit'] += 1
                logger.debug('Cache hit for get_size')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_size', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_size')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_size', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_size_success'] += 1
                self._metrics['get_size_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_size completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_size', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_size validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_size attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_size_failure'] += 1
                    logger.error('get_size exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def get_hit_rate(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> CacheManagerResult:
        """Perform get hit rate operation on In-Memory Caching with TTL & LRU Eviction.

        Args:
            data: Input data for get_hit_rate operation.
            **kwargs: Additional keyword arguments.

        Returns:
            CacheManagerResult: The result of get_hit_rate.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_hit_rate for In-Memory Caching with TTL & LRU Eviction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_hit_rate blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for CacheManager')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_hit_rate', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_hit_rate_cache_hit'] += 1
                logger.debug('Cache hit for get_hit_rate')
                return CacheManagerResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_hit_rate', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_hit_rate')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_hit_rate', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_hit_rate_success'] += 1
                self._metrics['get_hit_rate_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_hit_rate completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return CacheManagerResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_hit_rate', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_hit_rate validation error: %s', ve)
                return CacheManagerResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for CacheManager')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_hit_rate attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_hit_rate_failure'] += 1
                    logger.error('get_hit_rate exhausted retries: %s', exc, exc_info=True)
        
        return CacheManagerResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def _preprocess_0(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocessing pipeline variant 0 for In-Memory Caching with TTL & LRU Eviction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_0.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 0 for In-Memory Caching with TTL & LRU Eviction')
        result = copy.deepcopy(data)
        # Normalize keys to snake_case
        normalized = {
            re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower(): v
            for k, v in result.items()
        }
        # Strip whitespace from string values
        for key, val in normalized.items():
            if isinstance(val, str):
                normalized[key] = val.strip()
            elif isinstance(val, list):
                normalized[key] = [
                    item.strip() if isinstance(item, str) else item
                    for item in val
                ]
        # Inject metadata
        normalized['_preprocessed'] = True
        normalized['_pipeline'] = 0
        normalized['_timestamp'] = datetime.now(timezone.utc).isoformat()
        return normalized

    def _preprocess_1(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocessing pipeline variant 1 for In-Memory Caching with TTL & LRU Eviction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_1.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 1 for In-Memory Caching with TTL & LRU Eviction')
        result = copy.deepcopy(data)
        # Normalize keys to snake_case
        normalized = {
            re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower(): v
            for k, v in result.items()
        }
        # Strip whitespace from string values
        for key, val in normalized.items():
            if isinstance(val, str):
                normalized[key] = val.strip()
            elif isinstance(val, list):
                normalized[key] = [
                    item.strip() if isinstance(item, str) else item
                    for item in val
                ]
        # Inject metadata
        normalized['_preprocessed'] = True
        normalized['_pipeline'] = 1
        normalized['_timestamp'] = datetime.now(timezone.utc).isoformat()
        return normalized

    def _preprocess_2(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocessing pipeline variant 2 for In-Memory Caching with TTL & LRU Eviction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_2.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 2 for In-Memory Caching with TTL & LRU Eviction')
        result = copy.deepcopy(data)
        # Normalize keys to snake_case
        normalized = {
            re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower(): v
            for k, v in result.items()
        }
        # Strip whitespace from string values
        for key, val in normalized.items():
            if isinstance(val, str):
                normalized[key] = val.strip()
            elif isinstance(val, list):
                normalized[key] = [
                    item.strip() if isinstance(item, str) else item
                    for item in val
                ]
        # Inject metadata
        normalized['_preprocessed'] = True
        normalized['_pipeline'] = 2
        normalized['_timestamp'] = datetime.now(timezone.utc).isoformat()
        return normalized

    def _preprocess_3(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocessing pipeline variant 3 for In-Memory Caching with TTL & LRU Eviction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_3.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 3 for In-Memory Caching with TTL & LRU Eviction')
        result = copy.deepcopy(data)
        # Normalize keys to snake_case
        normalized = {
            re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower(): v
            for k, v in result.items()
        }
        # Strip whitespace from string values
        for key, val in normalized.items():
            if isinstance(val, str):
                normalized[key] = val.strip()
            elif isinstance(val, list):
                normalized[key] = [
                    item.strip() if isinstance(item, str) else item
                    for item in val
                ]
        # Inject metadata
        normalized['_preprocessed'] = True
        normalized['_pipeline'] = 3
        normalized['_timestamp'] = datetime.now(timezone.utc).isoformat()
        return normalized

    def _preprocess_4(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocessing pipeline variant 4 for In-Memory Caching with TTL & LRU Eviction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_4.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 4 for In-Memory Caching with TTL & LRU Eviction')
        result = copy.deepcopy(data)
        # Normalize keys to snake_case
        normalized = {
            re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower(): v
            for k, v in result.items()
        }
        # Strip whitespace from string values
        for key, val in normalized.items():
            if isinstance(val, str):
                normalized[key] = val.strip()
            elif isinstance(val, list):
                normalized[key] = [
                    item.strip() if isinstance(item, str) else item
                    for item in val
                ]
        # Inject metadata
        normalized['_preprocessed'] = True
        normalized['_pipeline'] = 4
        normalized['_timestamp'] = datetime.now(timezone.utc).isoformat()
        return normalized

    def _validate_input(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and sanitize input data for In-Memory Caching with TTL & LRU Eviction operations.

        Returns:
            Dict[str, Any]: The result of _validate_input.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        if not isinstance(data, dict):
            raise ValueError(f'Expected dict, got {type(data).__name__}')
        errors = []
        # Check for required fields based on operation context
        for key, val in data.items():
            if val is None and not key.startswith('_'):
                errors.append(f'Field {key!r} is None')
            if isinstance(val, str) and len(val) > 50_000:
                errors.append(f'Field {key!r} exceeds 50,000 char limit')
        if errors:
            error_summary = '; '.join(errors[:10])
            raise ValueError(f'Validation failed: {error_summary}')
        return data

    def _enrich_data(self, data: Dict[str, Any], operation: str = '') -> Dict[str, Any]:
        """Enrich In-Memory Caching with TTL & LRU Eviction data with contextual metadata and computed fields.

        Returns:
            Dict[str, Any]: The result of _enrich_data.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        enriched = dict(data)
        enriched['_operation'] = operation
        enriched['_enriched_at'] = datetime.now(timezone.utc).isoformat()
        enriched['_request_id'] = str(uuid.uuid4())
        enriched['_source'] = self.__class__.__name__
        # Compute checksum for integrity
        payload = json.dumps(
            {k: v for k, v in enriched.items() if not k.startswith('_')},
            sort_keys=True, default=str
        ).encode()
        enriched['_checksum'] = hashlib.sha256(payload).hexdigest()[:16]
        return enriched

    def _execute_core_exec(self, data: Dict[str, Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Core execution logic for In-Memory Caching with TTL & LRU Eviction data transformation.

        Returns:
            Dict[str, Any]: The result of _execute_core_exec.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        result = {}
        # Data transformation pipeline
        pipeline_steps = [
            self._step_normalize,
            self._step_compute_scores,
            self._step_apply_rules,
            self._step_finalize,
        ]
        current = dict(data)
        for step in pipeline_steps:
            current = step(current, **kwargs)
        return current

    def _step_normalize(self, data: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        """Pipeline step: normalize for In-Memory Caching with TTL & LRU Eviction.

        Returns:
            Dict[str, Any]: The result of _step_normalize.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        result = dict(data)
        result[f'_step_normalize'] = True
        result[f'_step_normalize_at'] = time.monotonic()
        return result

    def _step_compute_scores(self, data: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        """Pipeline step: compute scores for In-Memory Caching with TTL & LRU Eviction.

        Returns:
            Dict[str, Any]: The result of _step_compute_scores.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        result = dict(data)
        result[f'_step_compute_scores'] = True
        result[f'_step_compute_scores_at'] = time.monotonic()
        return result

    def _step_apply_rules(self, data: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        """Pipeline step: apply rules for In-Memory Caching with TTL & LRU Eviction.

        Returns:
            Dict[str, Any]: The result of _step_apply_rules.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        result = dict(data)
        result[f'_step_apply_rules'] = True
        result[f'_step_apply_rules_at'] = time.monotonic()
        return result

    def _step_finalize(self, data: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        """Pipeline step: finalize for In-Memory Caching with TTL & LRU Eviction.

        Returns:
            Dict[str, Any]: The result of _step_finalize.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        result = dict(data)
        result[f'_step_finalize'] = True
        result[f'_step_finalize_at'] = time.monotonic()
        return result

    def _normalize_output(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize output format for In-Memory Caching with TTL & LRU Eviction results.

        Returns:
            Dict[str, Any]: The result of _normalize_output.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        return {
            k: v for k, v in data.items()
            if not k.startswith('_internal')
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Return operational metrics for In-Memory Caching with TTL & LRU Eviction monitoring.

        Returns:
            Dict[str, Any]: The result of get_metrics.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        with self._lock:
            metrics = dict(self._metrics)
        total_ops = sum(v for k, v in metrics.items() if k.endswith('_success'))
        failed_ops = sum(v for k, v in metrics.items() if k.endswith('_failure'))
        cache_hits = sum(v for k, v in metrics.items() if k.endswith('_cache_hit'))
        return {
            'total_operations': total_ops + failed_ops,
            'successful_operations': total_ops,
            'failed_operations': failed_ops,
            'cache_hits': cache_hits,
            'cache_hit_rate': cache_hits / max(total_ops + failed_ops, 1),
            'circuit_breaker_open': self._circuit_breaker_open,
            'circuit_breaker_failures': self._circuit_breaker_failures,
            'detail': metrics,
        }

    def register_hook(self, stage: str, func: Callable) -> None:
        """Register a lifecycle hook for In-Memory Caching with TTL & LRU Eviction operations.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        if stage == 'before':
            self._hooks_before.append(func)
        elif stage == 'after':
            self._hooks_after.append(func)
        else:
            raise ValueError(f"Invalid hook stage {stage!r}. Use 'before' or 'after'.")
        logger.info('Registered %s hook: %s', stage, getattr(func, '__name__', repr(func)))

    def clear_cache(self, pattern: Optional[str] = None) -> int:
        """Clear cached In-Memory Caching with TTL & LRU Eviction results. Returns number of entries cleared.

        Returns:
            int: The result of clear_cache.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        with self._lock:
            if pattern is None:
                count = len(self._cache)
                self._cache.clear()
                self._cache_ttl.clear()
                logger.info('Cleared all %d cache entries', count)
                return count
            keys_to_clear = [k for k in self._cache if pattern in k]
            for k in keys_to_clear:
                del self._cache[k]
                self._cache_ttl.pop(k, None)
            logger.info('Cleared %d cache entries matching %r', len(keys_to_clear), pattern)
            return len(keys_to_clear)

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check for In-Memory Caching with TTL & LRU Eviction service.

        Returns:
            Dict[str, Any]: The result of health_check.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        start = time.monotonic()
        status = 'healthy'
        issues = []
        try:
            # Self-test
            test_result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.get_metrics()
            )
            if self._circuit_breaker_open:
                status = 'degraded'
                issues.append('circuit_breaker_open')
            if test_result.get('failed_operations', 0) > 100:
                status = 'degraded'
                issues.append('high_failure_rate')
        except Exception as exc:
            status = 'unhealthy'
            issues.append(str(exc))
        
        return {
            'status': status,
            'issues': issues,
            'metrics': self.get_metrics(),
            'response_time_ms': (time.monotonic() - start) * 1000,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"config={self.config}, "
            f"cache_entries={len(self._cache)}, "
            f"circuit_breaker_open={self._circuit_breaker_open})"
        )

    def __str__(self) -> str:
        metrics = self.get_metrics()
        return (
            f"{self.__class__.__name__}: "
            f"{metrics['total_operations']} ops, "
            f"{metrics['cache_hit_rate']:.1%} cache hit rate"
        )

    @contextmanager
    def timed_operation(self, name: str):
        """Context manager for timing named operations."""
        start = time.monotonic()
        try:
            yield
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            self._metrics[f'{name}_total_ms'] += int(elapsed_ms)
            logger.debug('Operation %r took %.1fms', name, elapsed_ms)

    def batch_process(
        self,
        items: List[Dict[str, Any]],
        operation: str = 'process',
        **kwargs: Any
    ) -> List[CacheManagerResult]:
        """Process a batch of items using the service.

        Args:
            items: List of data items to process.
            operation: Operation name to invoke.
            **kwargs: Additional arguments passed to each operation.

        Returns:
            List of CacheManagerResult objects, one per input item.
        """
        if not items:
            return []

        batch_size = min(len(items), self.config.batch_size)
        results = []

        logger.info(
            'Processing batch of %d items (batch_size=%d) via %r',
            len(items), batch_size, operation
        )

        for i in range(0, len(items), batch_size):
            chunk = items[i:i + batch_size]
            for item in chunk:
                func = getattr(self, operation, None)
                if func is None:
                    result = CacheManagerResult.err(f'Unknown operation: {operation}')
                else:
                    try:
                        result = func(item, **kwargs)
                    except Exception as exc:
                        result = CacheManagerResult.err(str(exc))
                results.append(result)

        success_count = sum(1 for r in results if r.success)
        logger.info(
            'Batch completed: %d/%d successful',
            success_count, len(results)
        )
        return results

    def export_state(self) -> Dict[str, Any]:
        """Export current service state for persistence or debugging."""
        return {
            'class': self.__class__.__name__,
            'config': {
                'max_retries': self.config.max_retries,
                'timeout_seconds': self.config.timeout_seconds,
                'batch_size': self.config.batch_size,
            },
            'metrics': self.get_metrics(),
            'cache_size': len(self._cache),
            'hooks_before_count': len(self._hooks_before),
            'hooks_after_count': len(self._hooks_after),
            'exported_at': datetime.now(timezone.utc).isoformat(),
        }
