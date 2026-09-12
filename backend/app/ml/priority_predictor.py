"""
PriorityPredictor — Production Issue Priority Prediction Service
================================================================================
GitPilot AI Platform Module
Version: {}.{}.{}
================================================================================

This module provides production-grade Issue Priority Prediction capabilities for the GitPilot AI
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
    service = PriorityPredictor()
    result = service.extract_features()
    if result.success:
        print(result.data)

Configuration:
    service = PriorityPredictor(PriorityPredictorConfig(
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
MODULE_NAME = "PriorityPredictor"
DEFAULT_TIMEOUT = 30.0
MAX_BATCH_SIZE = 1000
MIN_CONFIDENCE = 0.0
MAX_CONFIDENCE = 1.0
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500
CACHE_NAMESPACE = "ISSUE_PRIORITY_PREDICTION"
LOG_SAMPLING_RATE = 0.1


# ─── Type Aliases ─────────────────────────────────────────────────────────────
JSON = Dict[str, Any]
OptionalStr = Optional[str]
RecordList = List[Dict[str, Any]]
ScoreMap = Dict[str, float]



class PriorityPredictorStatus(str, Enum):
    """Status codes for Issue Priority Prediction operations."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"

class PriorityPredictorPriority(str, Enum):
    """Priority levels for Issue Priority Prediction."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    BACKGROUND = "background"

@dataclass
class PriorityPredictorResult:
    """Result container for Issue Priority Prediction operations."""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: float = 0.0
    attempts: int = 1
    status: str = PriorityPredictorStatus.COMPLETED

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
    def ok(cls, data: Any, **kwargs) -> 'PriorityPredictorResult':
        """Create successful result."""
        return cls(success=True, data=data, status=PriorityPredictorStatus.COMPLETED, **kwargs)

    @classmethod
    def err(cls, error: str, **kwargs) -> 'PriorityPredictorResult':
        """Create failed result."""
        return cls(success=False, error=error, status=PriorityPredictorStatus.FAILED, **kwargs)

@dataclass
class PriorityPredictorConfig:
    """Configuration for Issue Priority Prediction."""
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





class PriorityPredictor:
    """Issue Priority Prediction service implementation for GitPilot AI.

    Provides comprehensive Issue Priority Prediction functionality with:
    - Circuit breaker pattern
    - Automatic retry with exponential backoff
    - Thread-safe operation
    - In-memory caching with TTL
    - Lifecycle hooks
    - Metrics collection
    - Health monitoring
    """

    def __init__(self, config: Optional[PriorityPredictorConfig] = None) -> None:
        """Initialize PriorityPredictor with optional configuration.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        self.config = config or PriorityPredictorConfig()
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

    async def extract_features(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform extract features operation on Issue Priority Prediction.

        Args:
            data: Input data for extract_features operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of extract_features.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting extract_features for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('extract_features blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'extract_features', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['extract_features_cache_hit'] += 1
                logger.debug('Cache hit for extract_features')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('extract_features', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='extract_features')
                    result_data = self._execute_core_extr(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('extract_features', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['extract_features_success'] += 1
                self._metrics['extract_features_total_ms'] += int(elapsed_ms)
                logger.info(
                    'extract_features completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'extract_features', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('extract_features validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('extract_features attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['extract_features_failure'] += 1
                    logger.error('extract_features exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def build_feature_vector(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform build feature vector operation on Issue Priority Prediction.

        Args:
            data: Input data for build_feature_vector operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of build_feature_vector.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting build_feature_vector for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('build_feature_vector blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'build_feature_vector', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['build_feature_vector_cache_hit'] += 1
                logger.debug('Cache hit for build_feature_vector')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('build_feature_vector', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='build_feature_vector')
                    result_data = self._execute_core_buil(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('build_feature_vector', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['build_feature_vector_success'] += 1
                self._metrics['build_feature_vector_total_ms'] += int(elapsed_ms)
                logger.info(
                    'build_feature_vector completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'build_feature_vector', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('build_feature_vector validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('build_feature_vector attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['build_feature_vector_failure'] += 1
                    logger.error('build_feature_vector exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def train_gradient_boosted(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform train gradient boosted operation on Issue Priority Prediction.

        Args:
            data: Input data for train_gradient_boosted operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of train_gradient_boosted.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting train_gradient_boosted for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('train_gradient_boosted blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'train_gradient_boosted', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['train_gradient_boosted_cache_hit'] += 1
                logger.debug('Cache hit for train_gradient_boosted')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('train_gradient_boosted', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='train_gradient_boosted')
                    result_data = self._execute_core_trai(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('train_gradient_boosted', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['train_gradient_boosted_success'] += 1
                self._metrics['train_gradient_boosted_total_ms'] += int(elapsed_ms)
                logger.info(
                    'train_gradient_boosted completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'train_gradient_boosted', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('train_gradient_boosted validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('train_gradient_boosted attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['train_gradient_boosted_failure'] += 1
                    logger.error('train_gradient_boosted exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def train_random_forest(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform train random forest operation on Issue Priority Prediction.

        Args:
            data: Input data for train_random_forest operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of train_random_forest.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting train_random_forest for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('train_random_forest blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'train_random_forest', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['train_random_forest_cache_hit'] += 1
                logger.debug('Cache hit for train_random_forest')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('train_random_forest', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='train_random_forest')
                    result_data = self._execute_core_trai(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('train_random_forest', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['train_random_forest_success'] += 1
                self._metrics['train_random_forest_total_ms'] += int(elapsed_ms)
                logger.info(
                    'train_random_forest completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'train_random_forest', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('train_random_forest validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('train_random_forest attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['train_random_forest_failure'] += 1
                    logger.error('train_random_forest exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def tune_hyperparameters(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform tune hyperparameters operation on Issue Priority Prediction.

        Args:
            data: Input data for tune_hyperparameters operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of tune_hyperparameters.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting tune_hyperparameters for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('tune_hyperparameters blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'tune_hyperparameters', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['tune_hyperparameters_cache_hit'] += 1
                logger.debug('Cache hit for tune_hyperparameters')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('tune_hyperparameters', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='tune_hyperparameters')
                    result_data = self._execute_core_tune(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('tune_hyperparameters', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['tune_hyperparameters_success'] += 1
                self._metrics['tune_hyperparameters_total_ms'] += int(elapsed_ms)
                logger.info(
                    'tune_hyperparameters completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'tune_hyperparameters', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('tune_hyperparameters validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('tune_hyperparameters attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['tune_hyperparameters_failure'] += 1
                    logger.error('tune_hyperparameters exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def cross_validate(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform cross validate operation on Issue Priority Prediction.

        Args:
            data: Input data for cross_validate operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of cross_validate.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting cross_validate for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('cross_validate blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'cross_validate', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['cross_validate_cache_hit'] += 1
                logger.debug('Cache hit for cross_validate')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('cross_validate', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='cross_validate')
                    result_data = self._execute_core_cros(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('cross_validate', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['cross_validate_success'] += 1
                self._metrics['cross_validate_total_ms'] += int(elapsed_ms)
                logger.info(
                    'cross_validate completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'cross_validate', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('cross_validate validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('cross_validate attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['cross_validate_failure'] += 1
                    logger.error('cross_validate exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def predict_priority(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform predict priority operation on Issue Priority Prediction.

        Args:
            data: Input data for predict_priority operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of predict_priority.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting predict_priority for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('predict_priority blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'predict_priority', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['predict_priority_cache_hit'] += 1
                logger.debug('Cache hit for predict_priority')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('predict_priority', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='predict_priority')
                    result_data = self._execute_core_pred(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('predict_priority', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['predict_priority_success'] += 1
                self._metrics['predict_priority_total_ms'] += int(elapsed_ms)
                logger.info(
                    'predict_priority completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'predict_priority', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('predict_priority validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('predict_priority attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['predict_priority_failure'] += 1
                    logger.error('predict_priority exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def predict_with_probability(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform predict with probability operation on Issue Priority Prediction.

        Args:
            data: Input data for predict_with_probability operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of predict_with_probability.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting predict_with_probability for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('predict_with_probability blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'predict_with_probability', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['predict_with_probability_cache_hit'] += 1
                logger.debug('Cache hit for predict_with_probability')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('predict_with_probability', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='predict_with_probability')
                    result_data = self._execute_core_pred(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('predict_with_probability', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['predict_with_probability_success'] += 1
                self._metrics['predict_with_probability_total_ms'] += int(elapsed_ms)
                logger.info(
                    'predict_with_probability completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'predict_with_probability', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('predict_with_probability validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('predict_with_probability attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['predict_with_probability_failure'] += 1
                    logger.error('predict_with_probability exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def explain_prediction(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform explain prediction operation on Issue Priority Prediction.

        Args:
            data: Input data for explain_prediction operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of explain_prediction.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting explain_prediction for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('explain_prediction blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'explain_prediction', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['explain_prediction_cache_hit'] += 1
                logger.debug('Cache hit for explain_prediction')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('explain_prediction', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='explain_prediction')
                    result_data = self._execute_core_expl(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('explain_prediction', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['explain_prediction_success'] += 1
                self._metrics['explain_prediction_total_ms'] += int(elapsed_ms)
                logger.info(
                    'explain_prediction completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'explain_prediction', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('explain_prediction validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('explain_prediction attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['explain_prediction_failure'] += 1
                    logger.error('explain_prediction exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def compute_feature_contributions(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform compute feature contributions operation on Issue Priority Prediction.

        Args:
            data: Input data for compute_feature_contributions operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of compute_feature_contributions.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_feature_contributions for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_feature_contributions blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_feature_contributions', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_feature_contributions_cache_hit'] += 1
                logger.debug('Cache hit for compute_feature_contributions')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_feature_contributions', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_feature_contributions')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_feature_contributions', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_feature_contributions_success'] += 1
                self._metrics['compute_feature_contributions_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_feature_contributions completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_feature_contributions', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_feature_contributions validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_feature_contributions attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_feature_contributions_failure'] += 1
                    logger.error('compute_feature_contributions exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def detect_drift(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform detect drift operation on Issue Priority Prediction.

        Args:
            data: Input data for detect_drift operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of detect_drift.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting detect_drift for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('detect_drift blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'detect_drift', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['detect_drift_cache_hit'] += 1
                logger.debug('Cache hit for detect_drift')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('detect_drift', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='detect_drift')
                    result_data = self._execute_core_dete(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('detect_drift', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['detect_drift_success'] += 1
                self._metrics['detect_drift_total_ms'] += int(elapsed_ms)
                logger.info(
                    'detect_drift completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'detect_drift', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('detect_drift validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('detect_drift attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['detect_drift_failure'] += 1
                    logger.error('detect_drift exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def adapt_to_drift(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform adapt to drift operation on Issue Priority Prediction.

        Args:
            data: Input data for adapt_to_drift operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of adapt_to_drift.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting adapt_to_drift for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('adapt_to_drift blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'adapt_to_drift', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['adapt_to_drift_cache_hit'] += 1
                logger.debug('Cache hit for adapt_to_drift')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('adapt_to_drift', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='adapt_to_drift')
                    result_data = self._execute_core_adap(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('adapt_to_drift', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['adapt_to_drift_success'] += 1
                self._metrics['adapt_to_drift_total_ms'] += int(elapsed_ms)
                logger.info(
                    'adapt_to_drift completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'adapt_to_drift', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('adapt_to_drift validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('adapt_to_drift attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['adapt_to_drift_failure'] += 1
                    logger.error('adapt_to_drift exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def evaluate_on_holdout(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform evaluate on holdout operation on Issue Priority Prediction.

        Args:
            data: Input data for evaluate_on_holdout operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of evaluate_on_holdout.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting evaluate_on_holdout for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('evaluate_on_holdout blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'evaluate_on_holdout', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['evaluate_on_holdout_cache_hit'] += 1
                logger.debug('Cache hit for evaluate_on_holdout')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('evaluate_on_holdout', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='evaluate_on_holdout')
                    result_data = self._execute_core_eval(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('evaluate_on_holdout', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['evaluate_on_holdout_success'] += 1
                self._metrics['evaluate_on_holdout_total_ms'] += int(elapsed_ms)
                logger.info(
                    'evaluate_on_holdout completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'evaluate_on_holdout', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('evaluate_on_holdout validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('evaluate_on_holdout attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['evaluate_on_holdout_failure'] += 1
                    logger.error('evaluate_on_holdout exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def get_decision_boundary(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform get decision boundary operation on Issue Priority Prediction.

        Args:
            data: Input data for get_decision_boundary operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of get_decision_boundary.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_decision_boundary for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_decision_boundary blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_decision_boundary', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_decision_boundary_cache_hit'] += 1
                logger.debug('Cache hit for get_decision_boundary')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_decision_boundary', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_decision_boundary')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_decision_boundary', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_decision_boundary_success'] += 1
                self._metrics['get_decision_boundary_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_decision_boundary completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_decision_boundary', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_decision_boundary validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_decision_boundary attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_decision_boundary_failure'] += 1
                    logger.error('get_decision_boundary exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def calibrate_model(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform calibrate model operation on Issue Priority Prediction.

        Args:
            data: Input data for calibrate_model operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of calibrate_model.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting calibrate_model for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('calibrate_model blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'calibrate_model', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['calibrate_model_cache_hit'] += 1
                logger.debug('Cache hit for calibrate_model')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('calibrate_model', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='calibrate_model')
                    result_data = self._execute_core_cali(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('calibrate_model', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['calibrate_model_success'] += 1
                self._metrics['calibrate_model_total_ms'] += int(elapsed_ms)
                logger.info(
                    'calibrate_model completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'calibrate_model', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('calibrate_model validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('calibrate_model attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['calibrate_model_failure'] += 1
                    logger.error('calibrate_model exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def compute_precision_at_k(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform compute precision at k operation on Issue Priority Prediction.

        Args:
            data: Input data for compute_precision_at_k operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of compute_precision_at_k.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_precision_at_k for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_precision_at_k blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_precision_at_k', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_precision_at_k_cache_hit'] += 1
                logger.debug('Cache hit for compute_precision_at_k')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_precision_at_k', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_0(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_precision_at_k')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_precision_at_k', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_precision_at_k_success'] += 1
                self._metrics['compute_precision_at_k_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_precision_at_k completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_precision_at_k', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_precision_at_k validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_precision_at_k attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_precision_at_k_failure'] += 1
                    logger.error('compute_precision_at_k exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def compute_ndcg(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform compute ndcg operation on Issue Priority Prediction.

        Args:
            data: Input data for compute_ndcg operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of compute_ndcg.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_ndcg for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_ndcg blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_ndcg', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_ndcg_cache_hit'] += 1
                logger.debug('Cache hit for compute_ndcg')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_ndcg', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_1(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_ndcg')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_ndcg', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_ndcg_success'] += 1
                self._metrics['compute_ndcg_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_ndcg completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_ndcg', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_ndcg validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_ndcg attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_ndcg_failure'] += 1
                    logger.error('compute_ndcg exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def get_model_card(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform get model card operation on Issue Priority Prediction.

        Args:
            data: Input data for get_model_card operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of get_model_card.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting get_model_card for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('get_model_card blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'get_model_card', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['get_model_card_cache_hit'] += 1
                logger.debug('Cache hit for get_model_card')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('get_model_card', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_2(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='get_model_card')
                    result_data = self._execute_core_get_(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('get_model_card', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['get_model_card_success'] += 1
                self._metrics['get_model_card_total_ms'] += int(elapsed_ms)
                logger.info(
                    'get_model_card completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'get_model_card', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('get_model_card validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('get_model_card attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['get_model_card_failure'] += 1
                    logger.error('get_model_card exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    async def check_bias(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform check bias operation on Issue Priority Prediction.

        Args:
            data: Input data for check_bias operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of check_bias.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting check_bias for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('check_bias blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'check_bias', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['check_bias_cache_hit'] += 1
                logger.debug('Cache hit for check_bias')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('check_bias', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_3(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='check_bias')
                    result_data = self._execute_core_chec(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('check_bias', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['check_bias_success'] += 1
                self._metrics['check_bias_total_ms'] += int(elapsed_ms)
                logger.info(
                    'check_bias completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'check_bias', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('check_bias validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('check_bias attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['check_bias_failure'] += 1
                    logger.error('check_bias exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def compute_fairness_metrics(self, data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PriorityPredictorResult:
        """Perform compute fairness metrics operation on Issue Priority Prediction.

        Args:
            data: Input data for compute_fairness_metrics operation.
            **kwargs: Additional keyword arguments.

        Returns:
            PriorityPredictorResult: The result of compute_fairness_metrics.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        # Pre-execution validation
        logger.debug('Starting compute_fairness_metrics for Issue Priority Prediction')
        start_time = time.monotonic()
        data = data or {}
        
        # Check circuit breaker
        if self._circuit_breaker_open:
            if time.monotonic() < (self._circuit_breaker_reset_at or 0):
                raise RuntimeError('compute_fairness_metrics blocked: circuit breaker open')
            else:
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                logger.info('Circuit breaker reset for PriorityPredictor')
        
        # Cache lookup
        cache_key = hashlib.sha256(
            json.dumps({'op': 'compute_fairness_metrics', 'data': data}, sort_keys=True, default=str).encode()
        ).hexdigest()
        if cache_key in self._cache:
            ttl_expire = self._cache_ttl.get(cache_key, 0)
            if time.monotonic() < ttl_expire:
                self._metrics['compute_fairness_metrics_cache_hit'] += 1
                logger.debug('Cache hit for compute_fairness_metrics')
                return PriorityPredictorResult.ok(self._cache[cache_key], metadata={'cached': True})
        
        # Execute with retry logic
        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                with self._lock:
                    # Run before hooks
                    for hook in self._hooks_before:
                        hook('compute_fairness_metrics', data, attempt)
        
                    # Core business logic
                    processed_data = self._preprocess_4(data)
                    validated = self._validate_input(processed_data)
                    enriched = self._enrich_data(validated, operation='compute_fairness_metrics')
                    result_data = self._execute_core_comp(enriched, kwargs)
                    normalized = self._normalize_output(result_data)
        
                    # Run after hooks
                    for hook in self._hooks_after:
                        hook('compute_fairness_metrics', normalized, attempt)
        
                    # Update cache
                    self._cache[cache_key] = normalized
                    self._cache_ttl[cache_key] = time.monotonic() + self.config.cache_ttl_seconds
        
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._metrics['compute_fairness_metrics_success'] += 1
                self._metrics['compute_fairness_metrics_total_ms'] += int(elapsed_ms)
                logger.info(
                    'compute_fairness_metrics completed in %.1fms (attempt %d/%d)',
                    elapsed_ms, attempt + 1, self.config.max_retries
                )
                return PriorityPredictorResult.ok(
                    normalized,
                    duration_ms=elapsed_ms,
                    attempts=attempt + 1,
                    metadata={'operation': 'compute_fairness_metrics', 'attempt': attempt + 1}
                )
        
            except (ValueError, TypeError) as ve:
                logger.warning('compute_fairness_metrics validation error: %s', ve)
                return PriorityPredictorResult.err(str(ve), attempts=attempt + 1)
            except Exception as exc:
                last_error = exc
                self._circuit_breaker_failures += 1
                if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    self._circuit_breaker_reset_at = time.monotonic() + self.config.circuit_breaker_timeout
                    logger.error('Circuit breaker opened for PriorityPredictor')
                if attempt < self.config.max_retries - 1:
                    backoff = self.config.backoff_factor ** attempt
                    if self.config.jitter:
                        backoff += secrets.randbelow(100) / 100.0
                    logger.warning('compute_fairness_metrics attempt %d failed, retrying in %.1fs: %s', attempt + 1, backoff, exc)
                    time.sleep(backoff)
                else:
                    self._metrics['compute_fairness_metrics_failure'] += 1
                    logger.error('compute_fairness_metrics exhausted retries: %s', exc, exc_info=True)
        
        return PriorityPredictorResult.err(str(last_error) if last_error else 'Unknown error', attempts=self.config.max_retries)

    def _preprocess_0(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocessing pipeline variant 0 for Issue Priority Prediction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_0.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 0 for Issue Priority Prediction')
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
        """Preprocessing pipeline variant 1 for Issue Priority Prediction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_1.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 1 for Issue Priority Prediction')
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
        """Preprocessing pipeline variant 2 for Issue Priority Prediction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_2.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 2 for Issue Priority Prediction')
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
        """Preprocessing pipeline variant 3 for Issue Priority Prediction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_3.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 3 for Issue Priority Prediction')
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
        """Preprocessing pipeline variant 4 for Issue Priority Prediction data normalization.

        Returns:
            Dict[str, Any]: The result of _preprocess_4.

        Raises:
            ValueError: If arguments are invalid.
            RuntimeError: If operation fails unexpectedly.
        """
        logger.debug('Preprocessing pipeline 4 for Issue Priority Prediction')
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
        """Validate and sanitize input data for Issue Priority Prediction operations.

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
        """Enrich Issue Priority Prediction data with contextual metadata and computed fields.

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
        """Core execution logic for Issue Priority Prediction data transformation.

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
        """Pipeline step: normalize for Issue Priority Prediction.

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
        """Pipeline step: compute scores for Issue Priority Prediction.

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
        """Pipeline step: apply rules for Issue Priority Prediction.

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
        """Pipeline step: finalize for Issue Priority Prediction.

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
        """Normalize output format for Issue Priority Prediction results.

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
        """Return operational metrics for Issue Priority Prediction monitoring.

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
        """Register a lifecycle hook for Issue Priority Prediction operations.

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
        """Clear cached Issue Priority Prediction results. Returns number of entries cleared.

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
        """Perform health check for Issue Priority Prediction service.

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
    ) -> List[PriorityPredictorResult]:
        """Process a batch of items using the service.

        Args:
            items: List of data items to process.
            operation: Operation name to invoke.
            **kwargs: Additional arguments passed to each operation.

        Returns:
            List of PriorityPredictorResult objects, one per input item.
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
                    result = PriorityPredictorResult.err(f'Unknown operation: {operation}')
                else:
                    try:
                        result = func(item, **kwargs)
                    except Exception as exc:
                        result = PriorityPredictorResult.err(str(exc))
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
