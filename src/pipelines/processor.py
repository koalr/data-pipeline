"""Data pipeline processing module."""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    source_table: str
    destination_table: str
    batch_size: int = 1000
    max_retries: int = 3
    timeout_seconds: int = 300


class DataProcessor:
    """Processes and transforms engineering metrics data."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._processed_count = 0

    def process_batch(self, records: list[dict]) -> list[dict]:
        """Process a batch of records with transformation."""
        results = []
        for record in records:
            try:
                transformed = self._transform(record)
                if self._validate(transformed):
                    results.append(transformed)
                    self._processed_count += 1
            except Exception as e:
                logger.error(f"Failed to process record: {e}")
        return results

    def _transform(self, record: dict) -> dict:
        return {
            "id": record["id"],
            "timestamp": datetime.fromisoformat(record["created_at"]),
            "value": float(record.get("value", 0)),
            "source": self.config.source_table,
        }

    def _validate(self, record: dict) -> bool:
        return record.get("value", 0) >= 0

    @property
    def stats(self) -> dict:
        return {"processed": self._processed_count}
