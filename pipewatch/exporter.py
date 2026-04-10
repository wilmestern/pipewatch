"""Exporter module for serializing pipeline reports to various output formats."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import asdict
from typing import List

from pipewatch.reporter import PipelineReport


class Exporter:
    """Serializes a list of PipelineReport objects to JSON or CSV format."""

    def to_json(self, reports: List[PipelineReport], indent: int = 2) -> str:
        """Return a JSON string representation of the given reports."""
        payload = [self._report_to_dict(r) for r in reports]
        return json.dumps(payload, indent=indent, default=str)

    def to_csv(self, reports: List[PipelineReport]) -> str:
        """Return a CSV string representation of the given reports."""
        if not reports:
            return ""

        fieldnames = [
            "source",
            "status",
            "is_healthy",
            "latency_ms",
            "error_rate",
            "throughput",
            "active_alerts",
            "collected_at",
        ]

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for report in reports:
            writer.writerow(self._report_to_dict(report))

        return output.getvalue()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _report_to_dict(self, report: PipelineReport) -> dict:
        metric = report.metric_result.metric
        return {
            "source": report.source,
            "status": report.status_label,
            "is_healthy": report.is_healthy,
            "latency_ms": metric.latency_ms,
            "error_rate": metric.error_rate,
            "throughput": metric.throughput,
            "active_alerts": len(report.active_alerts),
            "collected_at": metric.collected_at,
        }
