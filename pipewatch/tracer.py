"""Execution tracer for tracking pipeline run lineage."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
import uuid


@dataclass
class TraceSpan:
    source_name: str
    trace_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    success: Optional[bool] = None
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    def summary(self) -> str:
        status = "ok" if self.success else ("error" if self.success is False else "running")
        dur = f"{self.duration_seconds:.3f}s" if self.duration_seconds is not None else "?"
        return f"[{self.trace_id[:8]}] {self.source_name} {status} ({dur})"


class Tracer:
    def __init__(self) -> None:
        self._spans: Dict[str, List[TraceSpan]] = {}

    def start(self, source_name: str) -> TraceSpan:
        span = TraceSpan(
            source_name=source_name,
            trace_id=str(uuid.uuid4()),
            started_at=datetime.utcnow(),
        )
        self._spans.setdefault(source_name, []).append(span)
        return span

    def finish(self, span: TraceSpan, success: bool, error: Optional[str] = None) -> TraceSpan:
        span.ended_at = datetime.utcnow()
        span.success = success
        span.error = error
        return span

    def spans_for(self, source_name: str) -> List[TraceSpan]:
        return list(self._spans.get(source_name, []))

    def latest(self, source_name: str) -> Optional[TraceSpan]:
        spans = self._spans.get(source_name, [])
        return spans[-1] if spans else None

    def all_sources(self) -> List[str]:
        return list(self._spans.keys())
