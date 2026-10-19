# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import time
import asyncio
import logging
import traceback
import random
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
import pandas as pd
import aiohttp

# Configure standard logging to stdout with detailed timestamp and context
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("AsyncHarvesterEngine")


class TokenBucketRateLimiter:
    """
    Implements a precise, thread-safe token-bucket rate limiting mechanism 
    using asyncio synchronization locks to govern request throughput boundaries.
    """
    def __init__(self, rate_per_sec: float, burst_capacity: int) -> None:
        self.rate_per_sec: float = rate_per_sec
        self.burst_capacity: int = burst_capacity
        self.tokens: float = float(burst_capacity)
        self.last_update: float = time.monotonic()
        self.lock: asyncio.Lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquires a single token from the bucket, waiting asynchronously if necessary."""
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now
            
            # Replenish tokens based on elapsed time and fill rate
            self.tokens = min(float(self.burst_capacity), self.tokens + elapsed * self.rate_per_sec)
            
            if self.tokens < 1.0:
                deficit = 1.0 - self.tokens
                wait_time = deficit / self.rate_per_sec
                logger.debug(f"Rate limit hit. Throttling for {wait_time:.4f} seconds.")
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0


class ProxyNode:
    """Represents an individual proxy target with live telemetry and health metrics."""
    def __init__(self, url: str) -> None:
        self.url: str = url
        self.success_count: int = 0
        self.fail_count: int = 0
        self.consecutive_failures: int = 0
        self.total_latency_ms: float = 0.0
        self.is_active: bool = True
        self.last_used: float = 0.0

    @property
    def avg_latency(self) -> float:
        total = self.success_count + self.fail_count
        return (self.total_latency_ms / total) if total > 0 else 0.0

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.fail_count
        return (self.success_count / total) if total > 0 else 1.0

    def record_success(self, latency_ms: float) -> None:
        self.success_count += 1
        self.consecutive_failures = 0
        self.total_latency_ms += latency_ms
        self.last_used = time.monotonic()

    def record_failure(self) -> None:
        self.fail_count += 1
        self.consecutive_failures += 1
        self.last_used = time.monotonic()


class ProxyManager:
    """
    Autonomous proxy rotation engine with health matrices, rolling metrics,
    and automated circuit-breaker isolation.
    """
    def __init__(self, proxy_urls: List[str], failure_threshold: int = 3) -> None:
        self.proxies: List[ProxyNode] = [ProxyNode(url) for url in proxy_urls]
        self.failure_threshold: int = failure_threshold
        self.lock: asyncio.Lock = asyncio.Lock()
        self._index: int = 0

    async def get_proxy(self) -> Optional[str]:
        """Retrieves the next healthy proxy node using round-robin distribution."""
        async with self.lock:
            active_proxies = [p for p in self.proxies if p.is_active]
            if not active_proxies:
                logger.warning("All proxies quarantined. Resetting proxy pool circuit breakers.")
                for p in self.proxies:
                    p.is_active = True
                    p.consecutive_failures = 0
                active_proxies = self.proxies

            if not active_proxies:
                return None

            proxy = active_proxies[self._index % len(active_proxies)]
            self._index = (self._index + 1) % len(active_proxies)
            return proxy.url

    async def report_result(self, proxy_url: str, success: bool, latency_ms: float = 0.0, status_code: Optional[int] = None) -> None:
        """Updates proxy health metrics and triggers circuit breaker quarantine if necessary."""
        async with self.lock:
            for p in self.proxies:
                if p.url == proxy_url:
                    if success and status_code not in {429, 403, 503}:
                        p.record_success(latency_ms)
                    else:
                        p.record_failure()
                        logger.warning(f"Proxy node {proxy_url} encountered failure (Status: {status_code}). Consecutive fails: {p.consecutive_failures}")
                        if p.consecutive_failures >= self.failure_threshold:
                            p.is_active = False
                            logger.error(f"Circuit breaker triggered: Quarantining proxy {proxy_url} due to repeated faults.")
                    break


class VectorizedStorageStager:
    """
    High-velocity vectorized storage stager leveraging NumPy and Pandas 
    to map raw nested JSON streaming responses into columnar structures.
    """
    def __init__(self) -> None:
        self.buffer: List[Dict[str, Any]] = []
        self.lock: asyncio.Lock = asyncio.Lock()

    async def ingest(self, record: Dict[str, Any]) -> None:
        async with self.lock:
            self.buffer.append(record)

    async def flush_to_dataframe(self) -> pd.DataFrame:
        """Flushes memory buffer into a vectorized Pandas DataFrame asynchronously."""
        async with self.lock:
            if not self.buffer:
                return pd.DataFrame()
            
            # Vectorized flattening and structural conversion
            df = pd.json_normalize(self.buffer)
            self.buffer.clear()
            logger.info(f"Successfully vectorized and flushed {len(df)} records into Pandas columnar format.")
            return df


class AsyncAPIHarvester:
    """
    Production-grade asynchronous web harvester embedding rate-limiting, 
    proxy rotation, and robust fault tolerance.
    """
    def __init__(self, targets: List[str], proxies: List[str], rate_per_sec: float = 5.0, burst_capacity: int = 10) -> None:
        self.targets: List[str] = targets
        self.rate_limiter: TokenBucketRateLimiter = TokenBucketRateLimiter(rate_per_sec, burst_capacity)
        self.proxy_manager: ProxyManager = ProxyManager(proxies)
        self.storage_stager: VectorizedStorageStager = VectorizedStorageStager()
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        try:
            connector = aiohttp.TCPConnector(limit=100, ssl=False)
            self.session = aiohttp.ClientSession(connector=connector)
            logger.info("AsyncAPIHarvester HTTP connection pool initialized successfully.")
        except Exception as e:
            logger.critical(f"Failed to initialize HTTP session: {e}\n{traceback.format_exc()}")
            raise

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("AsyncAPIHarvester HTTP connection pool gracefully closed.")

    async def fetch_target(self, target_url: str) -> None:
        """Executes a single ingestion task with rate limiting, proxy routing, and automatic failover retry."""
        max_retries = 3
        attempt = 0

        while attempt < max_retries:
            attempt += 1
            await self.rate_limiter.acquire()
            proxy_url = await self.proxy_manager.get_proxy()
            
            start_time = time.monotonic()
            try:
                headers = {"User-Agent": "EnterpriseDataInfraHarvester/2.6.0"}
                
                # Simulation handling for demonstration if target is mock endpoint
                if "mock-endpoint" in target_url:
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                    simulated_status = random.choice([200, 200, 200, 429, 403, 500] if attempt == 1 else [200, 200])
                    
                    latency_ms = (time.monotonic() - start_time) * 1000.0
                    if simulated_status != 200:
                        await self.proxy_manager.report_result(proxy_url or "direct", False, latency_ms, simulated_status)
                        continue
                    
                    mock_record = {
                        "timestamp": time.time(),
                        "target": target_url,
                        "proxy": proxy_url,
                        "status_code": simulated_status,
                        "latency_ms": latency_ms,
                        "payload": {"id": random.randint(1000, 9999), "metric": random.random() * 100}
                    }
                    await self.storage_stager.ingest(mock_record)
                    await self.proxy_manager.report_result(proxy_url or "direct", True, latency_ms, simulated_status)
                    return

                # Standard production execution block using aiohttp
                async with self.session.get(target_url, proxy=proxy_url, headers=headers, timeout=10) as response:
                    latency_ms = (time.monotonic() - start_time) * 1000.0
                    if response.status in {429, 403, 502, 503, 504}:
                        await self.proxy_manager.report_result(proxy_url, False, latency_ms, response.status)
                        continue
                    
                    response.raise_for_status()
                    data = await response.json()
                    
                    record = {
                        "timestamp": time.time(),
                        "target": target_url,
                        "proxy": proxy_url,
                        "status_code": response.status,
                        "latency_ms": latency_ms,
                        "payload": data
                    }
                    await self.storage_stager.ingest(record)
                    await self.proxy_manager.report_result(proxy_url, True, latency_ms, response.status)
                    return

            except asyncio.TimeoutError:
                latency_ms = (time.monotonic() - start_time) * 1000.0
                logger.warning(f"Timeout connecting to {target_url} via proxy {proxy_url}. Attempt {attempt}/{max_retries}")
                if proxy_url:
                    await self.proxy_manager.report_result(proxy_url, False, latency_ms, 504)
            except Exception as e:
                latency_ms = (time.monotonic() - start_time) * 1000.0
                logger.error(f"Error harvesting target {target_url} via {proxy_url}: {e}")
                if proxy_url:
                    await self.proxy_manager.report_result(proxy_url, False, latency_ms, 500)
            
            await asyncio.sleep(0.5 * (2 ** (attempt - 1)))

        logger.error(f"Permanently failed to harvest target after {max_retries} attempts: {target_url}")

    async def run_harvest(self) -> pd.DataFrame:
        """Executes concurrent ingestion across all configured targets."""
        try:
            await self.initialize()
            tasks = [self.fetch_target(target) for target in self.targets]
            logger.info(f"Starting concurrent ingestion across {len(tasks)} target endpoints.")
            await asyncio.gather(*tasks, return_exceptions=True)
            
            df = await self.storage_stager.flush_to_dataframe()
            return df
        except Exception as e:
            logger.critical(f"Critical execution error during harvest run: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()
        finally:
            await self.close()


async def main() -> None:
    """Runnable main entrypoint configured with secure environment lookups and execution metrics."""
    logger.info("Initializing Asynchronous API Web Harvester execution pipeline...")
    
    api_key = os.environ.get("HARVESTER_API_KEY", "YOUR_API_KEY")
    proxy_pool_env = os.environ.get("PROXY_POOL", "http://proxy1.internal:8080,http://proxy2.internal:8080,http://proxy3.internal:8080")
    
    proxies = [p.strip() for p in proxy_pool_env.split(",") if p.strip()]
    targets = [f"https://api.internal-node-cluster.net/v1/stream/mock-endpoint-{i}" for i in range(25)]
    
    harvester = AsyncAPIHarvester(
        targets=targets,
        proxies=proxies,
        rate_per_sec=20.0,
        burst_capacity=10
    )
    
    start_time = time.monotonic()
    df_results = await harvester.run_harvest()
    duration = time.monotonic() - start_time
    
    logger.info(f"Harvester pipeline completed in {duration:.4f} seconds.")
    
    if not df_results.empty:
        print("\n================ EXECUTION METRICS SUMMARY (PANDAS DATAFRAME) ================")
        print(f"Total Records Ingested: {len(df_results)}")
        print(f"Mean Latency (ms): {df_results['latency_ms'].mean():.2f}")
        print(f"Max Latency (ms): {df_results['latency_ms'].max():.2f}")
        print(f"Min Latency (ms): {df_results['latency_ms'].min():.2f}")
        print("\nSample Dataframe Head:")
        print(df_results[['target', 'proxy', 'status_code', 'latency_ms']].head(10))
        print("==============================================================================")
    else:
        logger.warning("No records collected during execution run.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Harvest execution interrupted by user operator.")
    except Exception as exc:
        logger.critical(f"Unhandled fatal exception in event loop: {exc}\n{traceback.format_exc()}")
        sys.exit(1)