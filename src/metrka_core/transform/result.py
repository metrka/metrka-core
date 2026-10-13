"""Structured results returned by transformation operations."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from metrka_core.lineage.transformation.models import TransformationEvidence


@dataclass(frozen=True)
class TransformationResult:
    """
    Data produced by a transformation together with its evidence.

    Operations return evidence explicitly instead of mutating an
    observations list supplied by their caller.
    """

    data: pd.DataFrame
    evidence: tuple[TransformationEvidence, ...] = ()
