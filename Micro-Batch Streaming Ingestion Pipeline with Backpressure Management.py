# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import time
import threading
import queue
import logging
import traceback
from typing import Optional, Tuple, List, Dict, Any
import numpy as np
import pandas as pd

# Configure standard logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("StreamingPipeline")


class VectorizedRingBuffer:
    """
    High-speed, thread-safe circular ring buffer backed by pre-allocated NumPy arrays.
    Handles high-throughput concurrent writes and reads with zero dynamic memory allocation overhead.
    """
    def __init__(self, capacity: int, dtype: np.dtype = np.float64):
        self.capacity = capacity
        self.dtype = dtype
        self.buffer = np.zeros(capacity, dtype=dtype)
        self.head = 0
        self.tail = 0
        self.size = 0
        self.lock = threading.Lock()
        
    def push(self, data: np.ndarray) -> int:
        """Atomically push a 1D numpy array of items into the ring buffer."""
        with self.lock:
            try:
                n = len(data)
                if n == 0:
                    return 0
                
                space_available = self.capacity - self.size
                if n > space_available:
                    # Truncate incoming data if it exceeds remaining buffer capacity
                    data = data[-space_available:]
                    n = len(data)
                
                if n <= 0:
                    return 0

                first_chunk = min(n, self.capacity - self.tail)
                self.buffer[self.tail:self.tail + first_chunk] = data[:first_chunk]
                
                if first_chunk < n:
                    second_chunk = n - first_chunk
                    self.buffer[0:second_chunk] = data[first_chunk:]
                
                self.tail = (self.tail + n) % self.capacity
                self.size += n
                return n
            except Exception as e:
                logger.error(f"Critical error in ring buffer push transaction: {e}\n{traceback.format_exc()}")
                raise

    def pop(self, max_batch_size: int) -> np.ndarray:
        """Atomically pop up to max_batch_size items from the ring buffer."""
        with self.lock:
            try:
                if self.size == 0:
                    return np.array([], dtype=self.dtype)
                
                n = min(max_batch_size, self.size)
                result = np.empty(n, dtype=self.dtype)
                
                first_chunk = min(n, self.capacity - self.head)
                result[:first_chunk] = self.buffer[self.head:self.head + first_chunk]
                
                if first_chunk < n:
                    second_chunk = n - first_chunk
                    result[first_chunk:] = self.buffer[0:second_chunk]
                
                self.head = (self.head + n) % self.capacity
                self.size -= n
                return result
            except Exception as e:
                logger.error(f"Critical error in ring buffer pop transaction: {e}\n{traceback.format_exc()}")
                return np.array([], dtype=self.dtype)

    def get_saturation(self) -> float:
        """Return current buffer saturation percentage (0.0 to 100.0)."""
        with self.lock:
            try:
                return (self.size / self.capacity) * 100.0
            except Exception as e:
                logger.error(f"Error calculating buffer saturation: {e}\n{traceback.format_exc()}")
                return 0.0


class BackpressureController:
    """
    Reactive backpressure engine monitoring buffer saturation percentiles in real time.
    Enforces high-watermark throttling and low-watermark resumption.
    """
    def __init__(self, buffer: VectorizedRingBuffer, high_watermark: float = 85.0, low_watermark: float = 40.0):
        self.buffer = buffer
        self.high_watermark = high_watermark
        self.low_watermark = low_watermark
        self.throttle_event = threading.Event()
        self.throttle_event.set() # Open by default (producers unblocked)
        self.lock = threading.Lock()
        
    def check_and_update(self) -> bool:
        """Evaluate saturation state and transition throttle flags."""
        with self.lock:
            try:
                saturation = self.buffer.get_saturation()
                is_throttled = not self.throttle_event.is_set()
                
                if not is_throttled and saturation >= self.high_watermark:
                    logger.warning(f"[BACKPRESSURE TRIGGERED] Saturation at {saturation:.2f}% (>= {self.high_watermark}% High Watermark). Throttling incoming producer threads...")
                    self.throttle_event.clear()
                    return True
                elif is_throttled and saturation <= self.low_watermark:
                    logger.info(f"[BACKPRESSURE RELEASED] Saturation cleared to {saturation:.2f}% (<= {self.low_watermark}% Low Watermark). Resuming producer throughput...")
                    self.throttle_event.set()
                    return False
                return is_throttled
            except Exception as e:
                logger.error(f"Error in backpressure evaluation: {e}\n{traceback.format_exc()}")
                return False

    def wait_if_throttled(self) -> None:
        """Block producer execution thread if backpressure throttle is active."""
        try:
            self.throttle_event.wait()
        except Exception as e:
            logger.error(f"Error waiting on backpressure throttle event: {e}\n{traceback.format_exc()}")


class MicroBatchExecutionEngine:
    """
    Consumer daemon extracting micro-batches from the buffer and processing data 
    using Pandas vectorized matrix operations for rolling metrics and quality audits.
    """
    def __init__(self, buffer: VectorizedRingBuffer, batch_size: int = 500, poll_interval: float = 0.01):
        self.buffer = buffer
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.running = threading.Event()
        self.metrics_history: List[Dict[str, Any]] = []
        self.consumer_thread: Optional[threading.Thread] = None
        
    def start(self) -> None:
        try:
            self.running.set()
            self.consumer_thread = threading.Thread(target=self._consume_loop, name="ConsumerDaemon", daemon=True)
            self.consumer_thread.start()
            logger.info("Micro-Batch Execution Engine started successfully.")
        except Exception as e:
            logger.error(f"Failed to start consumer daemon: {e}\n{traceback.format_exc()}")
            raise

    def stop(self) -> None:
        try:
            self.running.clear()
            if self.consumer_thread and self.consumer_thread.is_alive():
                self.consumer_thread.join(timeout=3.0)
            logger.info("Micro-Batch Execution Engine stopped cleanly.")
        except Exception as e:
            logger.error(f"Error stopping consumer daemon: {e}\n{traceback.format_exc()}")

    def _consume_loop(self) -> None:
        while self.running.is_set():
            try:
                raw_data = self.buffer.pop(self.batch_size)
                if len(raw_data) == 0:
                    time.sleep(self.poll_interval)
                    continue
                
                self._process_micro_batch(raw_data)
            except Exception as e:
                logger.error(f"Exception encountered in consumer loop: {e}\n{traceback.format_exc()}")
                time.sleep(self.poll_interval)

    def _process_micro_batch(self, data: np.ndarray) -> None:
        try:
            timestamp = time.time()
            df = pd.DataFrame({"telemetry_value": data})
            
            # Vectorized metrics calculation & quality checks via Pandas/NumPy
            metrics = {
                "timestamp": timestamp,
                "batch_count": len(df),
                "mean": float(df["telemetry_value"].mean()),
                "std": float(df["telemetry_value"].std(ddof=0)),
                "min": float(df["telemetry_value"].min()),
                "max": float(df["telemetry_value"].max()),
                "null_count": int(df["telemetry_value"].isna().sum())
            }
            self.metrics_history.append(metrics)
            
            # Simulate downstream sink latency (e.g., columnar DB write)
            time.sleep(0.015)
            
            logger.debug(f"Micro-batch processed: {metrics['batch_count']} records | Mean: {metrics['mean']:.2f}")
        except Exception as e:
            logger.error(f"Error during vectorized batch processing: {e}\n{traceback.format_exc()}")

    def get_metrics_dataframe(self) -> pd.DataFrame:
        try:
            return pd.DataFrame(self.metrics_history)
        except Exception as e:
            logger.error(f"Error compiling metrics DataFrame: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()


class StreamingPipelineOrchestrator:
    """
    Orchestrates the end-to-end streaming architecture, managing producers, buffer, and consumer lifecycle.
    """
    def __init__(self, buffer_capacity: int = 15000, high_watermark: float = 85.0, low_watermark: float = 40.0):
        self.buffer = VectorizedRingBuffer(capacity=buffer_capacity)
        self.backpressure = BackpressureController(self.buffer, high_watermark=high_watermark, low_watermark=low_watermark)
        self.engine = MicroBatchExecutionEngine(self.buffer, batch_size=400, poll_interval=0.005)
        self.producers_running = threading.Event()
        self.producer_threads: List[threading.Thread] = []

    def start_producers(self, num_producers: int = 3) -> None:
        try:
            self.producers_running.set()
            for i in range(num_producers):
                t = threading.Thread(target=self._producer_task, args=(i,), name=f"Producer-{i+1}", daemon=True)
                self.producer_threads.append(t)
                t.start()
            logger.info(f"Initialized and started {num_producers} streaming producer threads.")
        except Exception as e:
            logger.error(f"Failed to start producer threads: {e}\n{traceback.format_exc()}")
            raise

    def _producer_task(self, producer_id: int) -> None:
        while self.producers_running.is_set():
            try:
                # Check and obey backpressure throttle signals
                self.backpressure.wait_if_throttled()
                
                # Generate high-speed synthetic sensor telemetry
                batch_size = np.random.randint(30, 100)
                data = np.random.normal(loc=50.0, scale=10.0, size=batch_size)
                
                # Push data into ring buffer
                self.buffer.push(data)
                
                # Update backpressure status continuously
                self.backpressure.check_and_update()
                
                # High-velocity generation cadence
                time.sleep(0.002)
            except Exception as e:
                logger.error(f"Error in producer worker {producer_id}: {e}\n{traceback.format_exc()}")
                time.sleep(0.05)

    def stop_producers(self) -> None:
        try:
            self.producers_running.clear()
            for t in self.producer_threads:
                if t.is_alive():
                    t.join(timeout=1.0)
            logger.info("All streaming producers stopped.")
        except Exception as e:
            logger.error(f"Error stopping producers: {e}\n{traceback.format_exc()}")

    def run(self, duration_seconds: float = 10.0) -> pd.DataFrame:
        logger.info("Starting Streaming Ingestion Pipeline Orchestration...")
        self.engine.start()
        self.start_producers(num_producers=3)
        
        try:
            start_time = time.time()
            while time.time() - start_time < duration_seconds:
                self.backpressure.check_and_update()
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Pipeline execution interrupted by user signal.")
        finally:
            self.stop_producers()
            self.engine.stop()
            logger.info("Streaming pipeline shutdown sequence completed.")
            
        return self.engine.get_metrics_dataframe()


if __name__ == "__main__":
    # Security: load configurable thresholds securely via environment variables with safe defaults
    buffer_cap = int(os.environ.get("STREAM_BUFFER_CAPACITY", "20000"))
    high_wm = float(os.environ.get("STREAM_HIGH_WATERMARK", "85.0"))
    low_wm = float(os.environ.get("STREAM_LOW_WATERMARK", "40.0"))
    run_duration = float(os.environ.get("STREAM_RUN_DURATION", "8.0"))

    orchestrator = StreamingPipelineOrchestrator(
        buffer_capacity=buffer_cap,
        high_watermark=high_wm,
        low_watermark=low_wm
    )
    
    metrics_df = orchestrator.run(duration_seconds=run_duration)
    
    # Print Consolidated Pipeline Metrics Summary
    print("\n" + "="*80)
    print("CONSOLIDATED PIPELINE METRICS SUMMARY (PANDAS DATAFRAME)")
    print("="*80)
    if not metrics_df.empty:
        print(metrics_df.describe().to_string())
        print("\nLast 5 Processed Micro-Batches:")
        print(metrics_df.tail().to_string())
    else:
        print("No metrics collected during execution.")
    print("="*80)