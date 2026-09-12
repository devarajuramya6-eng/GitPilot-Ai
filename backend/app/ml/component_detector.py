"""
ComponentDetector — Production Issue Component Classification Service
================================================================================
GitPilot AI Platform Module
Version: {}.{}.{}
================================================================================

This module provides production-grade Issue Component Classification capabilities for the GitPilot AI
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
    service = ComponentDetector()
    result = service.build_component_lexicons()
    if result.success:
        print(result.data)

Configuration:
    service = ComponentDetector(ComponentDetectorConfig(
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
MODULE_NAME = "ComponentDetector"
DEFAULT_TIMEOUT = 30.0
MAX_BATCH_SIZE = 1000
MIN_CONFIDENCE = 0.0
MAX_CONFIDENCE = 1.0
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500
CACHE_NAMESPACE = "ISSUE_COMPONENT_CLASSIFICATION"
LOG_SAMPLING_RATE = 0.1


# ─── Type Aliases ─────────────────────────────────────────────────────────────
JSON = Dict[str, Any]
OptionalStr = Optional[str]
RecordList = List[Dict[str, Any]]
ScoreMap = Dict[str, float]



class ComponentDetectorStatus(str, Enum):
    """Status codes for Issue Component Classification operations."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"

class ComponentDetectorPriority(str, Enum):
    """Priority levels for Issue Component Classification."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    BACKGROUND = "background"

@dataclass
class ComponentDetectorResult:
    """Result container for Issue Component Classification operations."""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: float = 0.0
    attempts: int = 1
    status: str = ComponentDetectorStatus.COMPLETED

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
    def ok(cls, data: Any, **kwargs) -> 'ComponentDetectorResult':
        """Create successful result."""
        return cls(success=True, data=data, status=ComponentDetectorStatus.COMPLETED, **kwargs)

    @classmethod
    def err(cls, error: str, **kwargs) -> 'ComponentDetectorResult':
        """Create failed result."""
        return cls(success=False, error=error, status=ComponentDetectorStatus.FAILED, **kwargs)

@dataclass
class ComponentDetectorConfig:
    """Configuration for Issue Component Classification."""
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





class ComponentDetector:
    """Issue Component Classification service implementation for GitPilot AI.

    Provides comprehensive Issue Component Classification functionality with:
    - Circuit breaker pattern
    - Automatic retry with exponential backoff
    - Thread-safe operation
    - In-memory caching with TTL
    - Lifecycle hooks
    - Metrics collection
    - Health monitoring
    """

    def __init__(self, config: Optional[ComponentDetectorConfig] = None) -> None:
        """Initialize ComponentDetector with optional configuration.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        self.config = config or ComponentDetectorConfig()
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

    async def build_component_lexicons(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform build component lexicons operation on Issue Component Classification.

        Args:
            data: Input data for build_component_lexicons operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of build_component_lexicons.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting build_component_lexicons for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('build_component_lexicons blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'build_component_lexicons', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['build_component_lexicons_cache_hit'] += 1
                logger.debug('Cache hit for build_component_lexicons')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('build_component_lexicons', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='build_component_lexicons')
                    result_data = self._execute_core_buil(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('build_component_lexicons', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['build_component_lexicons_success'] += 1
                self._metrics['build_component_lexicons_total_ms'] += int(elapsed_ms)
                logger.info(
                    'build_component_lexicons completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'build_component_lexicons', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('build_component_lexicons validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('build_component_lexicons attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['build_component_lexicons_failure'] += 1
                    logger.error('build_component_lexicons exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def compute_component_signals(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform compute component signals operation on Issue Component Classification.

        Args:
            data: Input data for compute_component_signals operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of compute_component_signals.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_component_signals for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_component_signals blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_component_signals', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_component_signals_cache_hit'] += 1
                logger.debug('Cache hit for compute_component_signals')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_component_signals', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_component_signals')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_component_signals', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_component_signals_success'] += 1
                self._metrics['compute_component_signals_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_component_signals completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_component_signals', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_component_signals validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_component_signals attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_component_signals_failure'] += 1
                    logger.error('compute_component_signals exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def detect_from_labels(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform detect from labels operation on Issue Component Classification.

        Args:
            data: Input data for detect_from_labels operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of detect_from_labels.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting detect_from_labels for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('detect_from_labels blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'detect_from_labels', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['detect_from_labels_cache_hit'] += 1
                logger.debug('Cache hit for detect_from_labels')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('detect_from_labels', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='detect_from_labels')
                    result_data = self._execute_core_dete(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('detect_from_labels', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['detect_from_labels_success'] += 1
                self._metrics['detect_from_labels_total_ms'] += int(elapsed_ms)
                logger.info(
                    'detect_from_labels completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'detect_from_labels', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('detect_from_labels validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('detect_from_labels attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['detect_from_labels_failure'] += 1
                    logger.error('detect_from_labels exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def detect_from_file_paths(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform detect from file paths operation on Issue Component Classification.

        Args:
            data: Input data for detect_from_file_paths operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of detect_from_file_paths.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting detect_from_file_paths for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('detect_from_file_paths blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'detect_from_file_paths', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['detect_from_file_paths_cache_hit'] += 1
                logger.debug('Cache hit for detect_from_file_paths')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('detect_from_file_paths', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='detect_from_file_paths')
                    result_data = self._execute_core_dete(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('detect_from_file_paths', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['detect_from_file_paths_success'] += 1
                self._metrics['detect_from_file_paths_total_ms'] += int(elapsed_ms)
                logger.info(
                    'detect_from_file_paths completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'detect_from_file_paths', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('detect_from_file_paths validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('detect_from_file_paths attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['detect_from_file_paths_failure'] += 1
                    logger.error('detect_from_file_paths exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def detect_from_author_history(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform detect from author history operation on Issue Component Classification.

        Args:
            data: Input data for detect_from_author_history operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of detect_from_author_history.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting detect_from_author_history for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('detect_from_author_history blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'detect_from_author_history', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['detect_from_author_history_cache_hit'] += 1
                logger.debug('Cache hit for detect_from_author_history')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('detect_from_author_history', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='detect_from_author_history')
                    result_data = self._execute_core_dete(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('detect_from_author_history', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['detect_from_author_history_success'] += 1
                self._metrics['detect_from_author_history_total_ms'] += int(elapsed_ms)
                logger.info(
                    'detect_from_author_history completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'detect_from_author_history', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('detect_from_author_history validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('detect_from_author_history attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['detect_from_author_history_failure'] += 1
                    logger.error('detect_from_author_history exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def detect_from_title(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform detect from title operation on Issue Component Classification.

        Args:
            data: Input data for detect_from_title operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of detect_from_title.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting detect_from_title for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('detect_from_title blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'detect_from_title', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['detect_from_title_cache_hit'] += 1
                logger.debug('Cache hit for detect_from_title')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('detect_from_title', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='detect_from_title')
                    result_data = self._execute_core_dete(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('detect_from_title', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['detect_from_title_success'] += 1
                self._metrics['detect_from_title_total_ms'] += int(elapsed_ms)
                logger.info(
                    'detect_from_title completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'detect_from_title', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('detect_from_title validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('detect_from_title attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['detect_from_title_failure'] += 1
                    logger.error('detect_from_title exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def detect_from_description(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform detect from description operation on Issue Component Classification.

        Args:
            data: Input data for detect_from_description operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of detect_from_description.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting detect_from_description for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('detect_from_description blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'detect_from_description', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['detect_from_description_cache_hit'] += 1
                logger.debug('Cache hit for detect_from_description')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('detect_from_description', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='detect_from_description')
                    result_data = self._execute_core_dete(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('detect_from_description', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['detect_from_description_success'] += 1
                self._metrics['detect_from_description_total_ms'] += int(elapsed_ms)
                logger.info(
                    'detect_from_description completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'detect_from_description', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('detect_from_description validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('detect_from_description attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['detect_from_description_failure'] += 1
                    logger.error('detect_from_description exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def merge_signals(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform merge signals operation on Issue Component Classification.

        Args:
            data: Input data for merge_signals operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of merge_signals.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting merge_signals for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('merge_signals blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'merge_signals', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['merge_signals_cache_hit'] += 1
                logger.debug('Cache hit for merge_signals')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('merge_signals', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='merge_signals')
                    result_data = self._execute_core_merg(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('merge_signals', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['merge_signals_success'] += 1
                self._metrics['merge_signals_total_ms'] += int(elapsed_ms)
                logger.info(
                    'merge_signals completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'merge_signals', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('merge_signals validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('merge_signals attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['merge_signals_failure'] += 1
                    logger.error('merge_signals exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def compute_confidence(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform compute confidence operation on Issue Component Classification.

        Args:
            data: Input data for compute_confidence operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of compute_confidence.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_confidence for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_confidence blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_confidence', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_confidence_cache_hit'] += 1
                logger.debug('Cache hit for compute_confidence')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_confidence', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_confidence')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_confidence', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_confidence_success'] += 1
                self._metrics['compute_confidence_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_confidence completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_confidence', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_confidence validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_confidence attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_confidence_failure'] += 1
                    logger.error('compute_confidence exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def multi_label_detect(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform multi label detect operation on Issue Component Classification.

        Args:
            data: Input data for multi_label_detect operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of multi_label_detect.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting multi_label_detect for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('multi_label_detect blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'multi_label_detect', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['multi_label_detect_cache_hit'] += 1
                logger.debug('Cache hit for multi_label_detect')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('multi_label_detect', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='multi_label_detect')
                    result_data = self._execute_core_mult(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('multi_label_detect', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['multi_label_detect_success'] += 1
                self._metrics['multi_label_detect_total_ms'] += int(elapsed_ms)
                logger.info(
                    'multi_label_detect completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'multi_label_detect', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('multi_label_detect validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('multi_label_detect attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['multi_label_detect_failure'] += 1
                    logger.error('multi_label_detect exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def get_component_hierarchy(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform get component hierarchy operation on Issue Component Classification.

        Args:
            data: Input data for get_component_hierarchy operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of get_component_hierarchy.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_component_hierarchy for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_component_hierarchy blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_component_hierarchy', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_component_hierarchy_cache_hit'] += 1
                logger.debug('Cache hit for get_component_hierarchy')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_component_hierarchy', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_component_hierarchy')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_component_hierarchy', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_component_hierarchy_success'] += 1
                self._metrics['get_component_hierarchy_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_component_hierarchy completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_component_hierarchy', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_component_hierarchy validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_component_hierarchy attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_component_hierarchy_failure'] += 1
                    logger.error('get_component_hierarchy exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def detect_cross_component(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform detect cross component operation on Issue Component Classification.

        Args:
            data: Input data for detect_cross_component operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of detect_cross_component.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting detect_cross_component for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('detect_cross_component blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'detect_cross_component', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['detect_cross_component_cache_hit'] += 1
                logger.debug('Cache hit for detect_cross_component')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('detect_cross_component', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='detect_cross_component')
                    result_data = self._execute_core_dete(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('detect_cross_component', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['detect_cross_component_success'] += 1
                self._metrics['detect_cross_component_total_ms'] += int(elapsed_ms)
                logger.info(
                    'detect_cross_component completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'detect_cross_component', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('detect_cross_component validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('detect_cross_component attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['detect_cross_component_failure'] += 1
                    logger.error('detect_cross_component exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def explain_detection(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform explain detection operation on Issue Component Classification.

        Args:
            data: Input data for explain_detection operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of explain_detection.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting explain_detection for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('explain_detection blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'explain_detection', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['explain_detection_cache_hit'] += 1
                logger.debug('Cache hit for explain_detection')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('explain_detection', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='explain_detection')
                    result_data = self._execute_core_expl(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('explain_detection', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['explain_detection_success'] += 1
                self._metrics['explain_detection_total_ms'] += int(elapsed_ms)
                logger.info(
                    'explain_detection completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'explain_detection', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('explain_detection validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('explain_detection attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['explain_detection_failure'] += 1
                    logger.error('explain_detection exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def add_component(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform add component operation on Issue Component Classification.

        Args:
            data: Input data for add_component operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of add_component.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting add_component for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('add_component blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'add_component', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['add_component_cache_hit'] += 1
                logger.debug('Cache hit for add_component')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('add_component', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='add_component')
                    result_data = self._execute_core_add_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('add_component', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['add_component_success'] += 1
                self._metrics['add_component_total_ms'] += int(elapsed_ms)
                logger.info(
                    'add_component completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'add_component', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('add_component validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('add_component attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['add_component_failure'] += 1
                    logger.error('add_component exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def remove_component(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform remove component operation on Issue Component Classification.

        Args:
            data: Input data for remove_component operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of remove_component.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting remove_component for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('remove_component blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'remove_component', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['remove_component_cache_hit'] += 1
                logger.debug('Cache hit for remove_component')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('remove_component', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='remove_component')
                    result_data = self._execute_core_remo(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('remove_component', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['remove_component_success'] += 1
                self._metrics['remove_component_total_ms'] += int(elapsed_ms)
                logger.info(
                    'remove_component completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'remove_component', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('remove_component validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('remove_component attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['remove_component_failure'] += 1
                    logger.error('remove_component exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def update_component_keywords(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform update component keywords operation on Issue Component Classification.

        Args:
            data: Input data for update_component_keywords operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of update_component_keywords.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting update_component_keywords for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('update_component_keywords blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'update_component_keywords', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['update_component_keywords_cache_hit'] += 1
                logger.debug('Cache hit for update_component_keywords')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('update_component_keywords', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='update_component_keywords')
                    result_data = self._execute_core_upda(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('update_component_keywords', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['update_component_keywords_success'] += 1
                self._metrics['update_component_keywords_total_ms'] += int(elapsed_ms)
                logger.info(
                    'update_component_keywords completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'update_component_keywords', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('update_component_keywords validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('update_component_keywords attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['update_component_keywords_failure'] += 1
                    logger.error('update_component_keywords exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def get_component_list(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform get component list operation on Issue Component Classification.

        Args:
            data: Input data for get_component_list operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of get_component_list.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_component_list for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_component_list blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_component_list', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_component_list_cache_hit'] += 1
                logger.debug('Cache hit for get_component_list')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_component_list', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_component_list')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_component_list', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_component_list_success'] += 1
                self._metrics['get_component_list_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_component_list completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_component_list', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_component_list validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_component_list attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_component_list_failure'] += 1
                    logger.error('get_component_list exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def compute_component_affinity(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform compute component affinity operation on Issue Component Classification.

        Args:
            data: Input data for compute_component_affinity operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of compute_component_affinity.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_component_affinity for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_component_affinity blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_component_affinity', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_component_affinity_cache_hit'] += 1
                logger.debug('Cache hit for compute_component_affinity')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_component_affinity', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_component_affinity')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_component_affinity', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_component_affinity_success'] += 1
                self._metrics['compute_component_affinity_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_component_affinity completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_component_affinity', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_component_affinity validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_component_affinity attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_component_affinity_failure'] += 1
                    logger.error('compute_component_affinity exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def validate_component(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ComponentDetectorResult:
        """Perform validate component operation on Issue Component Classification.

        Args:
            data: Input data for validate_component operation.
            **kwargs: Additional keyword arguments.

        Returns:
            ComponentDetectorResult: The result of validate_component.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting validate_component for Issue Component Classification')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('validate_component blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for ComponentDetector')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'validate_component', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['validate_component_cache_hit'] += 1
                logger.debug('Cache hit for validate_component')
                return ComponentDetectorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('validate_component', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='validate_component')
                    result_data = self._execute_core_vali(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('validate_component', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['validate_component_success'] += 1
                self._metrics['validate_component_total_ms'] += int(elapsed_ms)
                logger.info(
                    'validate_component completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return ComponentDetectorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'validate_component', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('validate_component validation error: %s', ve)
                return ComponentDetectorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for ComponentDetector')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('validate_component attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['validate_component_failure'] += 1
                    logger.error('validate_component exhausted retries: %s', exc, exc_info=True)
        
        return ComponentDetectorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def _preprocess_0(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocessing pipeline variant 0 for Issue Component Classification data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_0.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 0 for Issue Component Classification')
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
        """Preprocessing pipeline variant 1 for Issue Component Classification data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_1.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 1 for Issue Component Classification')
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
        """Preprocessing pipeline variant 2 for Issue Component Classification data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_2.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 2 for Issue Component Classification')
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
        """Preprocessing pipeline variant 3 for Issue Component Classification data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_3.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 3 for Issue Component Classification')
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
        """Preprocessing pipeline variant 4 for Issue Component Classification data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_4.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 4 for Issue Component Classification')
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
        """Validate and sanitize input data for Issue Component Classification operations.

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
        """Enrich Issue Component Classification data with contextual metadata and computed fields.

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
        """Core execution logic for Issue Component Classification data transformation.

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
        """Pipeline step: normalize for Issue Component Classification.

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
        """Pipeline step: compute scores for Issue Component Classification.

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
        """Pipeline step: apply rules for Issue Component Classification.

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
        """Pipeline step: finalize for Issue Component Classification.

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
        """Normalize output format for Issue Component Classification results.

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
        """Return operational metrics for Issue Component Classification monitoring.

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
        """Register a lifecycle hook for Issue Component Classification operations.

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
        """Clear cached Issue Component Classification results. Returns number of entries cleared.

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
        """Perform health check for Issue Component Classification service.

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
    ) -> List[ComponentDetectorResult]:
        """Process a batch of items using the service.

        Args:
            items: List of data items to process.
            operation: Operation name to invoke.
            **kwargs: Additional arguments passed to each operation.

        Returns:
            List of ComponentDetectorResult objects, one per input item.
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
                    result = ComponentDetectorResult.err(f'Unknown operation: {operation}')
                else:
                    try:
                        result = func(item, **kwargs)
                    except Exception as exc:
                        result = ComponentDetectorResult.err(str(exc))
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
