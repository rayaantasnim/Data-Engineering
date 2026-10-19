# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import json
import time
import logging
import traceback
from typing import Dict, List, Any, Optional, Tuple, Union
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, ValidationError

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SchemaAuditorEngine")

# Environment configuration with safe placeholders
DEFAULT_DEAD_LETTER_PATH = os.environ.get("SCHEMA_DEAD_LETTER_PATH", "./dead_letter_registry.json")
DEFAULT_BASELINE_PATH = os.environ.get("SCHEMA_BASELINE_PATH", "./baseline_schema.json")


class ColumnSchema(BaseModel):
    name: str
    inferred_type: str
    nullable: bool = True
    sample_values: List[Any] = Field(default_factory=list)


class BaselineSchema(BaseModel):
    version: int = 1
    columns: Dict[str, ColumnSchema] = Field(default_factory=dict)


class MigrationManifest(BaseModel):
    timestamp: float = Field(default_factory=time.time)
    added_columns: List[str] = Field(default_factory=list)
    missing_columns: List[str] = Field(default_factory=list)
    breaking_type_changes: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    non_breaking_type_changes: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    is_compatible: bool = True


class TypeInferenceEngine:
    """
    Mathematical and structural type inference engine processing tabular data streams
    without relying on hardcoded type maps or naive string parsing.
    """
    
    @staticmethod
    def infer_column_type(series: pd.Series) -> str:
        try:
            non_null = series.dropna()
            if non_null.empty:
                return "string"
            
            # Check for Boolean matrix/series
            if pd.api.types.is_bool_dtype(non_null) or set(non_null.unique()).issubset({True, False, 0, 1, 0.0, 1.0}):
                if len(non_null.unique()) <= 2 and not pd.api.types.is_float_dtype(non_null):
                    return "boolean"

            # Check for structural JSON substructures (dicts or lists)
            sample_val = non_null.iloc[0]
            if isinstance(sample_val, (dict, list)):
                return "json_struct"
            
            # Check for numeric types (Integer vs Float)
            if pd.api.types.is_numeric_dtype(non_null):
                if pd.api.types.is_integer_dtype(non_null):
                    return "integer"
                else:
                    # Check if floats represent whole numbers exclusively
                    float_arr = non_null.to_numpy(dtype=float)
                    if np.all(np.mod(float_arr, 1) == 0):
                        return "integer"
                    return "float"

            # Check for ISO timestamps via string pattern or datetime dtype
            if pd.api.types.is_datetime64_any_dtype(non_null):
                return "timestamp"
            
            if pd.api.types.is_string_dtype(non_null) or isinstance(sample_val, str):
                # Attempt strict ISO timestamp pattern matching on sample
                date_parsed = pd.to_datetime(non_null, errors='coerce', utc=True)
                if date_parsed.notna().mean() >= 0.95:
                    return "timestamp"

            return "string"
        except Exception as e:
            logger.error(f"Error during type inference for series {series.name}: {e}")
            logger.debug(traceback.format_exc())
            return "string"


class SchemaEvolutionAuditor:
    """
    Evaluates schema mutations, manages compatibility checks, and constructs migration manifests.
    """
    def __init__(self, baseline: Optional[BaselineSchema] = None):
        try:
            self.baseline = baseline if baseline else BaselineSchema()
            logger.info("SchemaEvolutionAuditor initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize SchemaEvolutionAuditor: {e}")
            raise

    def set_baseline(self, df: pd.DataFrame) -> None:
        try:
            columns_meta = {}
            for col in df.columns:
                inf_type = TypeInferenceEngine.infer_column_type(df[col])
                columns_meta[col] = ColumnSchema(
                    name=col,
                    inferred_type=inf_type,
                    nullable=df[col].isnull().any(),
                    sample_values=df[col].dropna().head(3).tolist()
                )
            self.baseline = BaselineSchema(version=self.baseline.version, columns=columns_meta)
            logger.info(f"Baseline schema successfully established with {len(columns_meta)} columns.")
        except Exception as e:
            logger.error(f"Error setting baseline schema: {e}")
            logger.debug(traceback.format_exc())
            raise

    def audit_batch(self, df: pd.DataFrame) -> Tuple[MigrationManifest, pd.DataFrame, pd.DataFrame]:
        try:
            incoming_types = {}
            for col in df.columns:
                incoming_types[col] = TypeInferenceEngine.infer_column_type(df[col])

            baseline_cols = set(self.baseline.columns.keys())
            incoming_cols = set(df.columns)

            added = list(incoming_cols - baseline_cols)
            missing = list(baseline_cols - incoming_cols)
            common = baseline_cols.intersection(incoming_cols)

            breaking_changes = {}
            non_breaking_changes = {}

            for col in common:
                base_type = self.baseline.columns[col].inferred_type
                inc_type = incoming_types[col]

                if base_type != inc_type:
                    # Define breaking vs non-breaking transitions
                    # e.g., integer -> float is non-breaking (widening), string -> integer is breaking
                    if base_type == "integer" and inc_type == "float":
                        non_breaking_changes[col] = {"baseline": base_type, "incoming": inc_type}
                    else:
                        breaking_changes[col] = {"baseline": base_type, "incoming": inc_type}

            is_compatible = len(breaking_changes) == 0 and len(missing) == 0

            manifest = MigrationManifest(
                added_columns=added,
                missing_columns=missing,
                breaking_type_changes=breaking_changes,
                non_breaking_type_changes=non_breaking_changes,
                is_compatible=is_compatible
            )

            # Automated Data Repair Matrix & Dead-Letter Isolation
            repaired_df, dead_letter_df = self._apply_repair_matrix(df, manifest)

            return manifest, repaired_df, dead_letter_df
        except Exception as e:
            logger.error(f"Error auditing incoming batch: {e}")
            logger.debug(traceback.format_exc())
            raise

    def _apply_repair_matrix(self, df: pd.DataFrame, manifest: MigrationManifest) -> Tuple[pd.DataFrame, pd.DataFrame]:
        try:
            work_df = df.copy()
            dead_letter_records = pd.DataFrame()

            # Handle non-breaking type variations via vectorized casting
            for col, types in manifest.non_breaking_type_changes.items():
                if types["baseline"] == "integer" and types["incoming"] == "float":
                    logger.info(f"Applying vectorized normalization for column '{col}': float -> integer conversion.")
                    work_df[col] = work_df[col].fillna(0).astype(np.int64)

            # Handle breaking mutations by routing invalid/corrupted rows to dead-letter registry
            if manifest.breaking_type_changes:
                mask_invalid = pd.Series([False] * len(work_df))
                for col, types in manifest.breaking_type_changes.items():
                    # Example check: if expected integer but incoming is string/object that cannot parse
                    if types["baseline"] == "integer":
                        converted = pd.to_numeric(work_df[col], errors='coerce')
                        bad_mask = converted.isna() & work_df[col].notna()
                        mask_invalid = mask_invalid | bad_mask
                        work_df[col] = converted.fillna(0).astype(np.int64)

                if mask_invalid.any():
                    dead_letter_records = work_df[mask_invalid].copy()
                    dead_letter_records["isolation_reason"] = "Breaking schema type mutation violation"
                    work_df = work_df[~mask_invalid].reset_index(drop=True)
                    logger.warning(f"Isolated {len(dead_letter_records)} rows to dead-letter registry due to breaking type mutations.")

            return work_df, dead_letter_records
        except Exception as e:
            logger.error(f"Error in automated structural data repair matrix: {e}")
            logger.debug(traceback.format_exc())
            raise


if __name__ == "__main__":
    try:
        logger.info("Starting Automated Schema Evolution and Type Inference Auditor Pipeline...")

        # 1. Provision Baseline Streaming Batch
        baseline_data = {
            "transaction_id": [1001, 1002, 1003, 1004],
            "amount": [45.50, 100.00, 12.30, 99.99],
            "is_active": [True, False, True, True],
            "event_time": ["2026-10-01T10:00:00Z", "2026-10-01T10:05:00Z", "2026-10-01T10:10:00Z", "2026-10-01T10:15:00Z"]
        }
        df_baseline = pd.DataFrame(baseline_data)
        
        auditor = SchemaEvolutionAuditor()
        auditor.set_baseline(df_baseline)

        # 2. Provision Evolving Streaming Batches
        logger.info("Simulating Streaming Batch 1: Forward Compatible (Added Column) & Non-Breaking Type Shift")
        batch_1_data = {
            "transaction_id": [2001.0, 2002.0, 2003.0],  # Float representation of integer ID (non-breaking)
            "amount": [150.00, 200.50, 5.00],
            "is_active": [False, True, False],
            "event_time": ["2026-10-01T11:00:00Z", "2026-10-01T11:05:00Z", "2026-10-01T11:10:00Z"],
            "region": ["NA", "EU", "APAC"]  # Newly added feature (Forward Compatible)
        }
        df_batch_1 = pd.DataFrame(batch_1_data)

        manifest_1, repaired_1, dead_letter_1 = auditor.audit_batch(df_batch_1)
        logger.info(f"Batch 1 Audit Manifest: {manifest_1.model_dump_json(indent=2)}")

        logger.info("Simulating Streaming Batch 2: Breaking Type Mutation & Missing Feature")
        batch_2_data = {
            "transaction_id": ["INVALID_ID", "3002", "3003"],  # String type breaking mutation for integer column
            "amount": [75.00, 12.00, 44.10],
            "is_active": [True, False, True]
            # 'event_time' is missing (Backward Compatible gap or missing feature)
        }
        df_batch_2 = pd.DataFrame(batch_2_data)

        manifest_2, repaired_2, dead_letter_2 = auditor.audit_batch(df_batch_2)
        logger.info(f"Batch 2 Audit Manifest: {manifest_2.model_dump_json(indent=2)}")

        # 3. Output Final Structural Snapshot Table Layout
        logger.info("Generating Final Structural Snapshot Summary Table...")
        final_summary_rows = []
        for col_name, col_obj in auditor.baseline.columns.items():
            final_summary_rows.append({
                "Column Name": col_name,
                "Inferred Baseline Type": col_obj.inferred_type,
                "Nullable": col_obj.nullable,
                "Sample Values": str(col_obj.sample_values)
            })
        
        df_summary = pd.DataFrame(final_summary_rows)
        print("\n" + "="*80)
        print("FINAL STRUCTURAL SNAPSHOT SCHEMA BASELINE")
        print("="*80)
        print(df_summary.to_string(index=False))
        print("="*80 + "\n")

        # Handle Dead Letter Registry Persistence Safely
        if not dead_letter_2.empty:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(DEFAULT_DEAD_LETTER_PATH)), exist_ok=True)
                dead_letter_2.to_json(DEFAULT_DEAD_LETTER_PATH, orient="records", indent=2)
                logger.info(f"Successfully persisted dead-letter registry to {DEFAULT_DEAD_LETTER_PATH}")
            except Exception as io_err:
                logger.error(f"File I/O error while persisting dead-letter registry: {io_err}")
                logger.debug(traceback.format_exc())

        logger.info("Automated Schema Evolution and Type Inference Auditor Pipeline completed successfully.")

    except Exception as global_err:
        logger.critical(f"Critical pipeline failure: {global_err}")
        logger.debug(traceback.format_exc())
        sys.exit(1)