#Author: Rayaan tasnim, 13 aged prodigy 
 
import os
import sys
import time
import re
import logging
import traceback
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import pandas as pd

# Configure logging to stdout with a detailed format
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("VectorizedLogEngine")

class LogParserEngine:
    """
    Vectorized Regular Expression Parsing Engine.
    Compiles optimized regular expressions and tokenizes raw logs into dense arrays.
    """
    def __init__(self, pattern: Optional[str] = None) -> None:
        # Safe default pattern using os.environ or fallback pattern
        default_pattern = (
            r"^\[(?P<timestamp>[^\]]+)\]\s+"
            r"(?P<service_id>\S+)\s+"
            r"status=(?P<status_code>\d+)\s+"
            r"latency_ms=(?P<latency>[0-9.]+)\s+"
            r"bytes=(?P<payload_size>\d+)$"
        )
        reg_pattern = os.environ.get("LOG_PARSER_REGEX", pattern or default_pattern)
        
        try:
            logger.info("Initializing and pre-compiling regular expression pattern.")
            self.compiled_regex = re.compile(reg_pattern)
        except Exception as e:
            logger.error("Failed to compile regular expression pattern: %s", str(e))
            logger.error(traceback.format_exc())
            raise

    def parse_stream(self, raw_logs: List[str]) -> pd.DataFrame:
        """
        Tokenizes raw log string buffers into structured tabular format using vectorized mapping.
        """
        try:
            logger.info("Starting vectorized log stream tokenization for %d entries.", len(raw_logs))
            
            # Convert list to Pandas Series for vectorized string operations
            series = pd.Series(raw_logs)
            
            # Extract matches using the pre-compiled regex
            extracted = series.str.extract(self.compiled_regex)
            
            if extracted.isnull().all().all():
                logger.warning("Extraction resulted in empty dataframe. Check log formats against regex.")
                return pd.DataFrame(columns=["timestamp", "service_id", "status_code", "latency", "payload_size"])

            # Vectorized data type casting and memory alignment
            extracted["timestamp"] = pd.to_datetime(extracted["timestamp"], errors="coerce")
            extracted["service_id"] = extracted["service_id"].astype("category")
            extracted["status_code"] = pd.to_numeric(extracted["status_code"], errors="coerce").fillna(0).astype(np.int32)
            extracted["latency"] = pd.to_numeric(extracted["latency"], errors="coerce").fillna(0.0).astype(np.float64)
            extracted["payload_size"] = pd.to_numeric(extracted["payload_size"], errors="coerce").fillna(0).astype(np.int64)
            
            # Drop rows with null timestamps
            extracted = extracted.dropna(subset=["timestamp"])
            
            logger.info("Successfully parsed and vectorized %d log records.", len(extracted))
            return extracted

        except Exception as e:
            logger.error("Error during log stream parsing and tokenization: %s", str(e))
            logger.error(traceback.format_exc())
            raise


class RealTimeFeatureStore:
    """
    Low-Latency In-Memory Multi-Index Feature Store.
    Manages multi-index partitioning and pure matrix sliding-window calculations.
    """
    def __init__(self) -> None:
        self.store: Optional[pd.DataFrame] = None
        logger.info("Initialized RealTimeFeatureStore.")

    def ingest_and_compute_windows(self, parsed_df: pd.DataFrame) -> pd.DataFrame:
        """
        Constructs MultiIndex structure partitioned by Service_ID and Timestamp,
        and computes sliding window metrics via pure matrix operations.
        """
        try:
            logger.info("Allocating MultiIndex memory structures and calculating sliding window metrics.")
            
            if parsed_df.empty:
                logger.warning("Ingested DataFrame is empty. Returning empty feature store frame.")
                return pd.DataFrame()

            # Set up working frame sorted by timestamp for time-based operations
            df = parsed_df.copy()
            df = df.sort_values(by=["service_id", "timestamp"])
            
            # Create a helper binary column for 5xx errors
            df["is_5xx"] = ((df["status_code"] >= 500) & (df["status_code"] < 600)).astype(np.int8)

            # Group by service_id and apply sliding window calculations efficiently using pandas rolling
            # Window specification: 60-second rolling window per service
            features_list = []
            
            for service_id, group in df.groupby("service_id", observed=True):
                # Set index to timestamp for time-based rolling window calculations
                g_indexed = group.set_index("timestamp")
                
                # Vectorized rolling computations over 60 seconds
                rolling_window = g_indexed.rolling(window="60s")
                
                g_features = pd.DataFrame(index=g_indexed.index)
                g_features["service_id"] = service_id
                g_features["rolling_error_count_60s"] = rolling_window["is_5xx"].sum()
                g_features["rolling_latency_p95_60s"] = rolling_window["latency"].quantile(0.95)
                g_features["rolling_mean_latency_60s"] = rolling_window["latency"].mean()
                g_features["rolling_total_bytes_60s"] = rolling_window["payload_size"].sum()
                
                features_list.append(g_features.reset_index())

            if not features_list:
                return pd.DataFrame()

            combined_features = pd.concat(features_list, ignore_index=True)
            
            # Construct MultiIndex (Service_ID, Timestamp)
            combined_features = combined_features.set_index(["service_id", "timestamp"])
            combined_features = combined_features.sort_index()
            
            self.store = combined_features
            logger.info("Successfully computed feature store matrices with MultiIndex.")
            return self.store

        except Exception as e:
            logger.error("Error during multi-index feature store construction and windowing: %s", str(e))
            logger.error(traceback.format_exc())
            raise


class MLFeatureExporter:
    """
    Downstream Machine Learning Feature Exporter.
    Handles missing feature imputation via high-speed forward-filling interpolation
    and outputs synchronized numerical feature matrices.
    """
    def __init__(self, feature_store: RealTimeFeatureStore) -> None:
        self.feature_store = feature_store
        logger.info("Initialized MLFeatureExporter.")

    def export_matrix(self) -> np.ndarray:
        """
        Interpolates missing features and exports a clean synchronized numerical feature matrix.
        """
        try:
            logger.info("Preparing and interpolating feature matrices for ML pipeline export.")
            
            if self.feature_store.store is None or self.feature_store.store.empty:
                logger.warning("Feature store is empty. Returning empty feature matrix.")
                return np.empty((0, 0))

            # Reset index temporarily to perform vectorized interpolation across levels if needed
            df = self.feature_store.store.copy()
            
            # High-speed forward-filling interpolation arrays for handling missing values
            df_interpolated = df.groupby(level="service_id").ffill().bfill().fillna(0.0)
            
            # Extract pure numerical values matrix
            feature_matrix = df_interpolated.to_numpy(dtype=np.float64)
            
            logger.info("Successfully exported clean numerical feature matrix of shape: %s", feature_matrix.shape)
            return feature_matrix

        except Exception as e:
            logger.error("Error during ML feature matrix extraction and interpolation: %s", str(e))
            logger.error(traceback.format_exc())
            raise


if __name__ == "__main__":
    logger.info("=== Starting Real-Time Event Stream Log Parser & Feature Store Simulation ===")
    
    # Simulate high-frequency stream of multi-service raw application logs
    simulated_raw_logs = [
        "[2026-10-19T10:00:00.123456Z] auth-service status=200 latency_ms=12.4 bytes=512",
        "[2026-10-19T10:00:01.234567Z] payment-service status=500 latency_ms=450.2 bytes=1024",
        "[2026-10-19T10:00:02.345678Z] auth-service status=200 latency_ms=9.1 bytes=256",
        "[2026-10-19T10:00:03.456789Z] payment-service status=503 latency_ms=890.5 bytes=2048",
        "[2026-10-19T10:00:04.567890Z] order-service status=201 latency_ms=45.0 bytes=4096",
        "[2026-10-19T10:00:05.678901Z] payment-service status=200 latency_ms=85.3 bytes=512",
        "[2026-10-19T10:00:10.789012Z] auth-service status=401 latency_ms=15.0 bytes=128",
        "[2026-10-19T10:00:15.890123Z] order-service status=500 latency_ms=300.1 bytes=1024"
    ]
    
    try:
        # Instantiate architectural components
        parser = LogParserEngine()
        feature_store = RealTimeFeatureStore()
        exporter = MLFeatureExporter(feature_store)
        
        # 1. Vectorized Regular Expression Parsing
        parsed_df = parser.parse_stream(simulated_raw_logs)
        logger.info("Parsed DataFrame Preview:\n%s", parsed_df.head())
        
        # 2. Low-Latency In-Memory Multi-Index Feature Store Computation
        computed_features = feature_store.ingest_and_compute_windows(parsed_df)
        logger.info("Feature Store MultiIndex Snapshot:\n%s", computed_features.head(10))
        
        # 3. Downstream Machine Learning Feature Exporter
        final_matrix = exporter.export_matrix()
        logger.info("Exported ML Feature Matrix Shape: %s", final_matrix.shape)
        logger.info("Exported ML Feature Matrix Sample Data:\n%s", final_matrix[:5])
        
        logger.info("=== Simulation Completed Successfully ===")
        
    except Exception as exc:
        logger.critical("Fatal error encountered in main simulation workflow: %s", str(exc))
        logger.critical(traceback.format_exc())
        sys.exit(1)