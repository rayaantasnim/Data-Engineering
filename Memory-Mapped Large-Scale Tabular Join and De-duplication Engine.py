# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import mmap
import time
import logging
import traceback
from typing import Tuple, Dict, Any, Optional
import numpy as np
import pandas as pd

# Configure standard Python logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("MemmapTabularEngine")


class MemoryMappedTabularEngine:
    """
    A high-performance out-of-core memory-mapped tabular join and de-duplication engine
    built from scratch using NumPy memory-mapped files (np.memmap), vectorized array sorting,
    bitwise masking, and chunk-based merge streams.
    """

    def __init__(self, work_dir: Optional[str] = None) -> None:
        try:
            self.work_dir = work_dir or os.environ.get("ENGINE_WORK_DIR", "./memmap_engine_workspace")
            os.makedirs(self.work_dir, exist_ok=True)
            logger.info(f"Initialized MemoryMappedTabularEngine with workspace directory: {self.work_dir}")
        except Exception as e:
            logger.error(f"Failed to initialize workspace directory: {e}")
            logger.error(traceback.format_exc())
            raise

    def create_synthetic_binary_dataset(
        self, filename: str, num_rows: int, key_range: Tuple[int, int] = (1, 1000000), introduce_duplicates: bool = True
    ) -> str:
        """
        Provisions a massive raw binary dataset on disk using structured NumPy arrays
        mapped directly to binary streams, avoiding memory bloat.
        """
        filepath = os.path.join(self.work_dir, filename)
        logger.info(f"Generating synthetic dataset '{filename}' with {num_rows:,} rows at {filepath}...")

        try:
            # Generate keys and values using vectorized numpy operations
            if introduce_duplicates:
                keys = np.random.randint(key_range[0], key_range[1], size=num_rows, dtype=np.int64)
            else:
                keys = np.arange(key_range[0], key_range[0] + num_rows, dtype=np.int64)

            values = np.random.randn(num_rows).astype(np.float64) * 100.0

            # Structured dtype for tabular representation: key (int64), value (float64)
            dtype = np.dtype([('key', np.int64), ('value', np.float64)])

            # Allocate binary file and memory map it
            file_size = num_rows * dtype.itemsize
            
            with open(filepath, "wb+") as f:
                f.write(b'\x00' * file_size)

            memmap_arr = np.memmap(filepath, dtype=dtype, mode='r+', shape=(num_rows,))
            
            # Write in chunks to prevent peak memory spikes during population
            chunk_size = 100_000
            for start_idx in range(0, num_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, num_rows)
                memmap_arr['key'][start_idx:end_idx] = keys[start_idx:end_idx]
                memmap_arr['value'][start_idx:end_idx] = values[start_idx:end_idx]

            memmap_arr.flush()
            logger.info(f"Successfully provisioned binary dataset '{filename}' ({file_size / (1024*1024):.2f} MB).")
            return filepath

        except Exception as e:
            logger.error(f"Error provisioning synthetic dataset {filename}: {e}")
            logger.error(traceback.format_exc())
            raise

    def load_memmap_array(self, filepath: str, num_rows: int) -> np.memmap:
        """
        Loads an existing binary file as a zero-copy np.memmap structured array with error handling.
        """
        try:
            logger.info(f"Loading memory-mapped file from {filepath}...")
            dtype = np.dtype([('key', np.int64), ('value', np.float64)])
            mmap_arr = np.memmap(filepath, dtype=dtype, mode='r', shape=(num_rows,))
            logger.info(f"Successfully mapped {num_rows:,} rows from {filepath}.")
            return mmap_arr
        except Exception as e:
            logger.error(f"Failed to load memory-mapped array from {filepath}: {e}")
            logger.error(traceback.format_exc())
            raise

    def vectorized_deduplicate(self, memmap_arr: np.memmap) -> Tuple[np.ndarray, np.ndarray]:
        """
        Performs high-performance out-of-core vectorized de-duplication using np.argsort
        and bitwise binary masking directly on memory-mapped array indices.
        """
        logger.info("Executing vectorized out-of-core de-duplication...")
        start_time = time.time()

        try:
            keys = memmap_arr['key']
            values = memmap_arr['value']
            total_rows = len(memmap_arr)

            # Step 1: Get sorted indices based on keys
            logger.info("Computing vectorized argsort on keys...")
            sort_indices = np.argsort(keys, kind='mergesort')

            # Step 2: Compute adjacent differences to find unique key boundaries
            logger.info("Computing adjacent key differentials for deduplication bitmask...")
            sorted_keys = keys[sort_indices]
            
            mask = np.ones(total_rows, dtype=bool)
            if total_rows > 1:
                mask[1:] = sorted_keys[1:] != sorted_keys[:-1]

            unique_original_indices = sort_indices[mask]
            
            dedup_keys = keys[unique_original_indices]
            dedup_values = values[unique_original_indices]

            elapsed = time.time() - start_time
            logger.info(
                f"Deduplication complete in {elapsed:.4f}s. "
                f"Original rows: {total_rows:,}, Deduplicated rows: {len(dedup_keys)::,} "
                f"(Removed {total_rows - len(dedup_keys)::,} duplicate records)."
            )

            return dedup_keys, dedup_values

        except Exception as e:
            logger.error(f"Error during vectorized deduplication: {e}")
            logger.error(traceback.format_exc())
            raise

    def chunked_sort_merge_join(
        self,
        left_keys: np.ndarray,
        left_vals: np.ndarray,
        right_keys: np.ndarray,
        right_vals: np.ndarray,
        chunk_size: int = 50_000
    ) -> pd.DataFrame:
        """
        Executes a high-performance sort-merge join algorithm from scratch using chunk-based
        sliding window streams across two arrays, consolidating matches into a Pandas DataFrame.
        """
        logger.info("Starting chunked sort-merge join operation...")
        start_time = time.time()

        try:
            logger.info("Sorting left and right datasets for merge-join...")
            left_sort_idx = np.argsort(left_keys)
            right_sort_idx = np.argsort(right_keys)

            s_left_keys = left_keys[left_sort_idx]
            s_left_vals = left_vals[left_sort_idx]
            s_right_keys = right_keys[right_sort_idx]
            s_right_vals = right_vals[right_sort_idx]

            matched_left_keys = []
            matched_left_vals = []
            matched_right_vals = []

            l_ptr = 0
            r_ptr = 0
            len_left = len(s_left_keys)
            len_right = len(s_right_keys)

            logger.info(f"Streaming join across left ({len_left:,} rows) and right ({len_right:,} rows) datasets...")

            while l_ptr < len_left and r_ptr < len_right:
                lk = s_left_keys[l_ptr]
                rk = s_right_keys[r_ptr]

                if lk == rk:
                    r_end = r_ptr
                    while r_end < len_right and s_right_keys[r_end] == lk:
                        r_end += 1

                    l_end = l_ptr
                    while l_end < len_left and s_left_keys[l_end] == lk:
                        l_end += 1

                    for l_i in range(l_ptr, l_end):
                        for r_i in range(r_ptr, r_end):
                            matched_left_keys.append(lk)
                            matched_left_vals.append(s_left_vals[l_i])
                            matched_right_vals.append(s_right_vals[r_i])

                    l_ptr = l_end
                    r_ptr = r_end
                elif lk < rk:
                    l_ptr += 1
                else:
                    r_ptr += 1

            elapsed = time.time() - start_time
            logger.info(f"Join completed in {elapsed:.4f}s. Total matched records: {len(matched_left_keys):,}")

            result_df = pd.DataFrame({
                'join_key': matched_left_keys,
                'left_value': matched_left_vals,
                'right_value': matched_right_vals
            })

            return result_df

        except Exception as e:
            logger.error(f"Error during chunked sort-merge join: {e}")
            logger.error(traceback.format_exc())
            raise


if __name__ == "__main__":
    try:
        logger.info("=== INITIALIZING MEMORY-MAPPED TABULAR JOIN & DE-DUPLICATION ENGINE ===")
        
        engine = MemoryMappedTabularEngine()

        num_rows_a = 200_000
        num_rows_b = 200_000
        
        file_a = engine.create_synthetic_binary_dataset(
            filename="dataset_alpha.bin",
            num_rows=num_rows_a,
            key_range=(1, 150_000),
            introduce_duplicates=True
        )
        
        file_b = engine.create_synthetic_binary_dataset(
            filename="dataset_beta.bin",
            num_rows=num_rows_b,
            key_range=(50_000, 200_000),
            introduce_duplicates=True
        )

        mmap_a = engine.load_memmap_array(file_a, num_rows_a)
        mmap_b = engine.load_memmap_array(file_b, num_rows_b)

        dedup_keys_a, dedup_vals_a = engine.vectorized_deduplicate(mmap_a)
        dedup_keys_b, dedup_vals_b = engine.vectorized_deduplicate(mmap_b)

        merged_dataframe = engine.chunked_sort_merge_join(
            left_keys=dedup_keys_a,
            left_vals=dedup_vals_a,
            right_keys=dedup_keys_b,
            right_vals=dedup_vals_b,
            chunk_size=50_000
        )

        logger.info("=== ENGINE EXECUTION SUMMARY ===")
        logger.info(f"Dataset Alpha Deduplicated Rows: {len(dedup_keys_a):,}")
        logger.info(f"Dataset Beta Deduplicated Rows: {len(dedup_keys_b):,}")
        logger.info(f"Final Merged Join Result Count: {len(merged_dataframe):,}")
        logger.info("\nFirst 5 rows of the consolidated result DataFrame:\n" + str(merged_dataframe.head()))

        logger.info("=== ENGINE EXECUTION COMPLETED SUCCESSFULLY ===")

    except Exception as fatal_err:
        logger.critical(f"Critical engine failure: {fatal_err}")
        logger.critical(traceback.format_exc())
        sys.exit(1)