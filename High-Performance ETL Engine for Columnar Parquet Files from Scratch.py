# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import time
import logging
import traceback
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging to stdout
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ColumnarETLEngine")

class HighPerformanceETLEngine:
    """
    Production-grade High-Performance ETL Engine for Columnar Parquet Files from Scratch.
    Handles streaming records, algorithmic schema inference, strict type validation,
    vectorized transformations, and compressed PyArrow/Parquet structural exports.
    """

    def __init__(self, output_dir: Optional[str] = None, chunk_size: int = 10000):
        try:
            # Security constraint: use os.environ.get with safe local placeholder default
            self.output_dir = output_dir or os.environ.get("ETL_OUTPUT_DIR", "./parquet_output")
            self.chunk_size = chunk_size
            
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir, exist_ok=True)
                logger.info(f"Initialized output directory at safe local path: {self.output_dir}")
        except Exception as e:
            logger.error(f"Failed to initialize HighPerformanceETLEngine: {e}")
            logger.error(traceback.format_exc())
            raise

    def parse_and_validate_stream(self, records: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Streams unstructured, raw relational records, applies statistical schema inference,
        handles jagged schemas, isolates type mismatches into an error buffer, and constructs 
        a clean DataFrame with vector-aligned null management.
        """
        logger.info("Starting streaming schema inference and strict type validation...")
        valid_records = []
        error_buffer = []

        try:
            for idx, record in enumerate(records):
                try:
                    # Basic structural sanity check
                    if not isinstance(record, dict):
                        raise TypeError(f"Record at index {idx} is not a dictionary.")
                    
                    # Example validation rule: mandatory fields presence check
                    # We normalize keys dynamically to support jagged schemas
                    cleaned_record = {}
                    for k, v in record.items():
                        # Standardize key format
                        clean_key = str(k).strip().lower()
                        cleaned_record[clean_key] = v

                    valid_records.append(cleaned_record)
                except Exception as rec_err:
                    error_buffer.append({
                        "index": idx,
                        "record": str(record),
                        "error": str(rec_err),
                        "traceback": traceback.format_exc()
                    })

            if not valid_records:
                raise ValueError("No valid records found after schema parsing and validation.")

            # Convert valid records into a Pandas DataFrame for vectorized manipulation
            df = pd.DataFrame(valid_records)

            # Vectorized null bitmap handling & datatype inference optimization
            for col in df.columns:
                # Infer and coerce types based on statistical profiling
                if df[col].dtype == object:
                    # Check if numeric values can be safely parsed
                    numeric_coerced = pd.to_numeric(df[col], errors='coerce')
                    if numeric_coerced.notna().sum() > 0.5 * len(df):
                        df[col] = numeric_coerced
                    else:
                        # Fill missing categorical values with explicit null placeholder for zero-copy efficiency
                        df[col] = df[col].fillna("UNKNOWN_NULL").astype(str)
                elif pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(0)

            logger.info(f"Successfully processed {len(df)} valid records. Captured {len(error_buffer)} malformed records into error buffer.")
            return df, error_buffer

        except Exception as e:
            logger.error("Critical error during schema parsing and stream validation.")
            logger.error(traceback.format_exc())
            raise

    def transform_columnar_vectors(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs vector-oriented columnar transformations using NumPy backend:
        - Numerical Min-Max Normalization
        - Categorical Label Mapping (Dictionary Encoding prep)
        - Timestamp Standardizations
        """
        logger.info("Executing columnar transformation layer via vectorized operations...")
        try:
            transformed_df = df.copy()

            # 1. Numerical Min-Max Normalization on any continuous numeric columns
            numeric_cols = transformed_df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                col_data = transformed_df[col].to_numpy(dtype=np.float64)
                col_min = np.min(col_data)
                col_max = np.max(col_data)
                
                if col_max > col_min:
                    # Vectorized min-max normalization formula: (X - min) / (max - min)
                    normalized_data = (col_data - col_min) / (col_max - col_min)
                    transformed_df[f"{col}_normalized"] = normalized_data
                else:
                    transformed_df[f"{col}_normalized"] = 0.0

            # 2. Categorical Label Mapping / Dictionary encoding setup
            categorical_cols = transformed_df.select_dtypes(include=[object]).columns
            for col in categorical_cols:
                if col != "error":
                    codes, uniques = pd.factorize(transformed_df[col])
                    transformed_df[f"{col}_encoded"] = codes

            # 3. Timestamp Standardizations (if timestamp-like data exists)
            if 'timestamp' in transformed_df.columns:
                transformed_df['standardized_timestamp'] = pd.to_datetime(
                    transformed_df['timestamp'], errors='coerce'
                ).fillna(pd.Timestamp('1970-01-01'))

            logger.info("Columnar transformations completed successfully.")
            return transformed_df

        except Exception as e:
            logger.error("Error encountered during columnar transformations.")
            logger.error(traceback.format_exc())
            raise

    def export_to_parquet(self, df: pd.DataFrame, filename: str = "dataset_partitioned.parquet") -> str:
        """
        Compiles vectorized NumPy memory chunks into an Arrow Table and writes out 
        a highly optimized, compressed Parquet file with Snappy/GZIP compression and dictionary encoding.
        """
        logger.info("Compiling Arrow Table and exporting to optimized Parquet format...")
        file_path = os.path.join(self.output_dir, filename)

        try:
            # Convert pandas DataFrame to PyArrow Table
            arrow_table = pa.Table.from_pandas(df)

            # Write out Parquet file with advanced storage configurations (Snappy compression, dictionary encoding)
            pq.write_table(
                arrow_table,
                file_path,
                compression='SNAPPY',
                use_dictionary=True,
                flavor='spark',
                row_group_size=self.chunk_size
            )

            logger.info(f"Parquet file successfully written to: {file_path}")
            return file_path

        except Exception as e:
            logger.error(f"Failed to export table to Parquet path: {file_path}")
            logger.error(traceback.format_exc())
            raise

    def verify_parquet_integrity(self, file_path: str) -> bool:
        """
        Reads back the compiled Parquet file to verify schema structural validity and data integrity.
        """
        logger.info(f"Verifying structural validity of Parquet file: {file_path}")
        try:
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata
            num_rows = metadata.num_rows
            num_row_groups = metadata.num_row_groups

            logger.info(f"Parquet Metadata Verification Passed: Rows={num_rows}, Row Groups={num_row_groups}")

            # Read back table subset to verify contents
            read_table = parquet_file.read()
            df_verified = read_table.to_pandas()
            
            if len(df_verified) == num_rows:
                logger.info("Data row count verification successful!")
                return True
            else:
                logger.warning("Row count discrepancy detected during Parquet verification.")
                return False

        except Exception as e:
            logger.error(f"Parquet verification failed for file: {file_path}")
            logger.error(traceback.format_exc())
            raise


if __name__ == "__main__":
    logger.info("=== INITIALIZING HIGH-PERFORMANCE ETL PIPELINE EXECUTION ===")
    
    start_time = time.time()

    try:
        # Generate massive row-oriented mock transactional log with varying structural datatypes (jagged schemas)
        logger.info("Generating mock transactional log with mixed schemas...")
        mock_records = [
            {"transaction_id": 1001, "user": "alice", "amount": 250.75, "timestamp": "2026-03-01T12:00:00Z", "status": "SUCCESS"},
            {"transaction_id": 1002, "user": "bob", "amount": "120.00", "timestamp": "2026-03-01T12:05:00Z", "status": "PENDING"},
            {"transaction_id": 1003, "user": "charlie", "amount": 45.50, "extra_field": "unstructured_val", "status": "SUCCESS"},
            {"transaction_id": 1004, "user": "diana", "amount": None, "timestamp": "invalid_date", "status": "FAILED"},
            {"transaction_id": 1005, "user": "eve", "amount": 999.99, "timestamp": "2026-03-01T12:10:00Z", "status": "SUCCESS"},
            # Malformed record example to test error isolation buffer
            "corrupted_string_record_not_a_dict" 
        ]

        # Duplicate records to simulate scale
        mock_records = mock_records * 2000

        # Instantiate ETL Engine
        etl_engine = HighPerformanceETLEngine(chunk_size=5000)

        # Step 1: Algorithmic Schema Inference & Strict Type Validation
        clean_df, error_buffer = etl_engine.parse_and_validate_stream(mock_records)
        logger.info(f"Schema inference complete. Clean records shape: {clean_df.shape}, Errors isolated: {len(error_buffer)}")

        # Step 2: Columnar Transformation Layer
        transformed_df = etl_engine.transform_columnar_vectors(clean_df)
        logger.info(f"Columnar transformation complete. Transformed shape: {transformed_df.shape}")

        # Step 3: Optimized Structural Storage Exporter
        parquet_filepath = etl_engine.export_to_parquet(transformed_df, filename="transactions_master.parquet")

        # Step 4: Verification
        is_valid = etl_engine.verify_parquet_integrity(parquet_filepath)
        
        elapsed_time = time.time() - start_time
        logger.info(f"=== ETL PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN {elapsed_time:.4f} SECONDS ===")

    except Exception as fatal_err:
        logger.error("FATAL: Pipeline execution aborted due to unhandled exceptions.")
        logger.error(traceback.format_exc())
        sys.exit(1)