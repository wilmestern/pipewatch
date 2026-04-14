"""Annotator: attach free-form notes to pipeline metric results."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from pipewatch.metrics import MetricResult


@dataclass
class Annotation:
    source_name: str
    note: str
    author: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def summary(self) -> str:
        ts = self.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"[{ts}] {self.author} on '{self.source_name}': {self.note}"


@dataclass
class AnnotatedResult:
    result: MetricResult
    annotations: list[Annotation] = field(default_factory=list)

    def has_annotations(self) -> bool:
        return len(self.annotations) > 0

    def latest_annotation(self) -> Optional[Annotation]:
        if not self.annotations:
            return None
        return max(self.annotations, key=lambda a: a.created_at)


class Annotator:
    """Stores and retrieves annotations keyed by source name."""

    def __init__(self) -> None:
        self._store: dict[str, list[Annotation]] = {}

    def add(self, source_name: str, note: str, author: str) -> Annotation:
        annotation = Annotation(source_name=source_name, note=note, author=author)
        self._store.setdefault(source_name, []).append(annotation)
        return annotation

    def get(self, source_name: str) -> list[Annotation]:
        return list(self._store.get(source_name, []))

    def annotate_result(self, result: MetricResult) -> AnnotatedResult:
        notes = self.get(result.source_name)
        return AnnotatedResult(result=result, annotations=notes)

    def clear(self, source_name: str) -> int:
        removed = self._store.pop(source_name, [])
        return len(removed)
