"""
Vectorized Multi-Dimensional Signal Processing ETL Engine
--------------------------------------------------------
Production-grade, highly optimized single-file Python script implementing a 
high-frequency multi-channel signal processing and transformation pipeline.
"""

import os
import sys
import time
import logging
import traceback
from typing import Dict, Tuple, Optional, Any, Union

import numpy as np
import pandas as pd
from scipy import signal
from scipy.stats import entropy

# Configure Logging to stdout
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("SignalProcessingETLEngine")


class SignalProcessingETLEngine:
    """
    High-frequency multi-dimensional signal processing and feature extraction ETL engine.
    Performs vectorized timestamp synchronization, digital filtering, spectral analysis,
    and columnar tabular transformation.
    """

    def __init__(
        self,
        sampling_rate_hz: float = 1000.0,
        window_size: int = 256,
        overlap: int = 128,
        cutoff_freq_hz: float = 50.0,
        filter_order: int = 4,
    ) -> None:
        self.sampling_rate_hz = sampling_rate_hz
        self.window_size = window_size
        self.overlap = overlap
        self.cutoff_freq_hz = cutoff_freq_hz
        self.filter_order = filter_order
        
        # Pre-design Butterworth low-pass filter coefficients for vectorized application
        try:
            nyquist = 0.5 * self.sampling_rate_hz
            normal_cutoff = self.cutoff_freq_hz / nyquist
            self.b, self.a = signal.butter(self.filter_order, normal_cutoff, btype='low', analog=False)
            logger.info("Successfully initialized digital filter coefficients (Butterworth Low-Pass).")
        except Exception as e:
            logger.error(f"Failed to initialize digital filter coefficients: {e}")
            logger.debug(traceback.format_exc())
            raise

    def ingest_and_align(
        self, timestamps: np.ndarray, raw_signals: np.ndarray
    ) -> pd.DataFrame:
        """
        Ingests high-frequency raw multi-dimensional signal arrays and timestamps,
        handles out-of-order sequence flags, aligns parallel streams, and executes
        vectorized forward-filling.
        """
        try:
            logger.info("Starting high-frequency data ingestion and timestamp synchronization...")
            start_time = time.time()

            if timestamps.shape[0] != raw_signals.shape[0]:
                raise ValueError("Length mismatch between timestamp array and signal matrix rows.")

            # Construct structured DataFrame for alignment
            num_channels = raw_signals.shape[1]
            column_names = [f"channel_{i}" for i in range(num_channels)]
            
            df = pd.DataFrame(raw_signals, columns=column_names)
            df.insert(0, "timestamp", timestamps)

            # Sort by timestamp to handle out-of-order data segments
            df = df.sort_values(by="timestamp").reset_index(drop=True)

            # Handle missing or NaN entries using vectorized forward-fill with back-fill fallback
            df[column_names] = df[column_names].ffill().bfill()

            # Ensure uniform indexing / chronological continuity check
            if df.isnull().any().any():
                logger.warning("Null values detected post-fill; imputing remaining NaNs with zeros.")
                df.fillna(0.0, inplace=True)

            logger.info(f"Ingestion and alignment completed successfully in {time.time() - start_time:.4f}s. Shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Error during high-frequency ingestion and alignment: {e}")
            logger.error(traceback.format_exc())
            raise

    def apply_digital_filter(self, signal_matrix: np.ndarray) -> np.ndarray:
        """
        Applies a zero-phase digital Butterworth filter across multi-dimensional signal arrays 
        using vectorized operations via SciPy.
        """
        try:
            logger.info("Applying vectorized digital filtering across multi-channel matrix...")
            start_time = time.time()

            # signal.filtfilt operates along the first dimension (axis=0) by default
            filtered_matrix = signal.filtfilt(self.b, self.a, signal_matrix, axis=0)

            logger.info(f"Digital filtering completed in {time.time() - start_time:.4f}s.")
            return filtered_matrix

        except Exception as e:
            logger.error(f"Error during vectorized digital filtering execution: {e}")
            logger.error(traceback.format_exc())
            raise

    def compute_spectral_and_statistical_features(
        self, aligned_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Extracts time-domain and frequency-domain metrics (sliding window rolling means,
        spectral entropy, peak-to-peak amplitude bounds, and power spectral density profiles)
        via purely vectorized matrix computations.
        """
        try:
            logger.info("Starting vectorized feature extraction pipeline (Time & Frequency domains)...")
            start_time = time.time()

            timestamp_col = aligned_df["timestamp"]
            signal_cols = [c for c in aligned_df.columns if c != "timestamp"]
            raw_data = aligned_df[signal_cols].to_numpy()

            # 1. Apply digital filter on raw data channels
            filtered_data = self.apply_digital_filter(raw_data)

            # 2. Time-domain metrics: Rolling Statistics (Vectorized via Pandas/NumPy windowing)
            rolling_window = min(self.window_size, max(2, len(aligned_df) // 10))
            
            df_features = pd.DataFrame()
            df_features["timestamp"] = timestamp_col

            for idx, col in enumerate(signal_cols):
                channel_series = pd.Series(filtered_data[:, idx])
                
                # Time domain feature extractions
                df_features[f"{col}_rolling_mean"] = channel_series.rolling(window=rolling_window, min_periods=1).mean()
                df_features[f"{col}_rolling_std"] = channel_series.rolling(window=rolling_window, min_periods=1).std().fillna(0.0)
                df_features[f"{col}_peak_to_peak"] = (
                    channel_series.rolling(window=rolling_window, min_periods=1).max() -
                    channel_series.rolling(window=rolling_window, min_periods=1).min()
                )

            # 3. Frequency-domain metrics via Sliding Window FFT / PSD (Vectorized block processing)
            n_samples, n_channels = filtered_data.shape
            step_size = max(1, self.window_size - self.overlap)
            
            # Compute sliding spectral features
            spectral_entropy_list = []
            dominant_freq_list = []

            for start_idx in range(0, n_samples, step_size):
                end_idx = min(start_idx + self.window_size, n_samples)
                window_chunk = filtered_data[start_idx:end_idx, :]
                
                if len(window_chunk) < 2:
                    # Pad or fallback for tiny trailing windows
                    spec_ent = np.zeros(n_channels)
                    dom_freq = np.zeros(n_channels)
                else:
                    # Compute FFT power spectral density (PSD)
                    fft_vals = np.fft.rfft(window_chunk, axis=0)
                    psd = np.abs(fft_vals) ** 2
                    
                    # Normalize PSD for spectral entropy calculation
                    psd_sum = np.sum(psd, axis=0, keepdims=True)
                    psd_sum[psd_sum == 0.0] = 1.0 # Prevent division by zero
                    psd_norm = psd / psd_sum
                    
                    # Spectral Entropy across frequency bins per channel
                    spec_ent = entropy(psd_norm, axis=0) / np.log(max(2, psd_norm.shape[0]))
                    
                    # Dominant Frequency per channel
                    freqs = np.fft.rfftfreq(len(window_chunk), d=1.0 / self.sampling_rate_hz)
                    dom_freq_idx = np.argmax(psd, axis=0)
                    dom_freq = freqs[dom_freq_idx]

                spectral_entropy_list.append((end_idx, spec_ent))
                dominant_freq_list.append((end_idx, dom_freq))

            # Map chunk-level frequency metrics back to frame-level array via broadcast/interpolation
            for idx, col in enumerate(signal_cols):
                channel_spec_ent = np.zeros(n_samples)
                channel_dom_freq = np.zeros(n_samples)
                
                last_end = 0
                for end_idx, spec_ent_arr in spectral_entropy_list:
                    channel_spec_ent[last_end:end_idx] = spec_ent_arr[idx]
                    channel_dom_freq[last_end:end_idx] = dominant_freq_list[last_end // step_size][1][idx]
                    last_end = end_idx

                df_features[f"{col}_spectral_entropy"] = channel_spec_ent
                df_features[f"{col}_dominant_frequency_hz"] = channel_dom_freq

            logger.info(f"Feature extraction pipeline completed successfully in {time.time() - start_time:.4f}s.")
            return df_features

        except Exception as e:
            logger.error(f"Error during feature extraction computation: {e}")
            logger.error(traceback.format_exc())
            raise

    def execute_pipeline(self, timestamps: np.ndarray, raw_signals: np.ndarray) -> pd.DataFrame:
        """
        Orchestrates the end-to-end signal processing ETL pipeline with robust telemetry tracking.
        """
        pipeline_start = time.time()
        logger.info("=== INITIALIZING SIGNAL PROCESSING PIPELINE EXECUTION ===")

        try:
            # Step 1: High-Frequency Ingestion & Alignment
            aligned_df = self.ingest_and_align(timestamps, raw_signals)

            # Step 2: Vectorized Mathematical Feature Transformation & Spectral Analysis
            feature_df = self.compute_spectral_and_statistical_features(aligned_df)

            total_duration = time.time() - pipeline_start
            logger.info(f"=== PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN {total_duration:.4f}s ===")
            
            return feature_df

        except Exception as e:
            logger.critical(f"Pipeline execution aborted due to unhandled exception: {e}")
            logger.critical(traceback.format_exc())
            raise


if __name__ == "__main__":
    try:
        logger.info("Provisioning mock high-frequency multi-channel sensor array...")
        
        # Configuration parameters from safe environment variables or defaults
        num_records = 2000
        num_channels = 3
        sampling_rate = float(os.environ.get("SENSOR_SAMPLING_RATE", 1000.0))

        # Generate synthetic high-frequency timestamps with out-of-order elements to test sorting/alignment
        np.random.seed(42)
        base_time = 1718000000.0
        time_deltas = np.random.exponential(scale=1.0 / sampling_rate, size=num_records)
        timestamps = base_time + np.cumsum(time_deltas)
        
        # Shuffle a subset of timestamps to test out-of-order alignment logic
        shuffle_indices = np.random.permutation(num_records)[:50]
        timestamps[shuffle_indices] = timestamps[shuffle_indices][::-1]

        # Generate synthetic multi-dimensional noisy signal arrays (e.g., sine waves + high frequency noise)
        t_vector = np.linspace(0, 2, num_records)
        raw_signals = np.zeros((num_records, num_channels))
        
        raw_signals[:, 0] = np.sin(2 * np.pi * 5 * t_vector) + 0.5 * np.random.randn(num_records) # 5 Hz signal + noise
        raw_signals[:, 1] = np.cos(2 * np.pi * 15 * t_vector) + 0.3 * np.random.randn(num_records) # 15 Hz signal + noise
        raw_signals[:, 2] = np.sin(2 * np.pi * 40 * t_vector) + 0.2 * np.random.randn(num_records) # 40 Hz signal + noise

        # Introduce intentional missing values (NaNs) to test vectorized forward-filling
        nan_indices_ch0 = np.random.choice(num_records, size=10, replace=False)
        raw_signals[nan_indices_ch0, 0] = np.nan

        # Instantiate ETL Engine
        engine = SignalProcessingETLEngine(
            sampling_rate_hz=sampling_rate,
            window_size=128,
            overlap=64,
            cutoff_freq_hz=30.0,
            filter_order=4
        )

        # Run ETL Pipeline
        processed_features_df = engine.execute_pipeline(timestamps, raw_signals)

        # Output summarized diagnostic feature trace snapshot
        print("\n--- PROCESSED FEATURE TRACE DIAGNOSTIC SNAPSHOT ---")
        print(processed_features_df.head(10).to_string())
        print(f"\nTotal Output DataFrame Shape: {processed_features_df.shape}")
        print(f"Columns Extracted: {list(processed_features_df.columns)}")

    except Exception as main_err:
        logger.critical(f"Critical execution failure in main block: {main_err}")
        logger.critical(traceback.format_exc())
        sys.exit(1)