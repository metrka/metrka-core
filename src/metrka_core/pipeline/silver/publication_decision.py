"""Pure decision logic for Silver publication eligibility."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from metrka_core.catalog.publication_candidate_models import SilverPublicationChangeKind
from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.pipeline.silver.fingerprints import SilverDatasetFingerprint


class SilverPublicationDecisionStatus(StrEnum):
    """Outcome of comparing a Silver build with its publication."""

    VERIFIED_EQUIVALENT = "verified_equivalent"
    AWAITING_APPROVAL = "awaiting_approval"


@dataclass(frozen=True)
class SilverPublicationDecision:
    """Decision made after a successful Silver materialization."""

    status: SilverPublicationDecisionStatus
    change_kind: SilverPublicationChangeKind
    baseline_publication_id: str | None

    def __post_init__(self) -> None:
        if self.status is SilverPublicationDecisionStatus.VERIFIED_EQUIVALENT:
            if self.change_kind is not SilverPublicationChangeKind.NONE:
                raise ValueError("An equivalent build cannot contain a publication change")

            if self.baseline_publication_id is None:
                raise ValueError("An equivalent build must reference the matched publication")

        if (
            self.status is SilverPublicationDecisionStatus.AWAITING_APPROVAL
            and self.change_kind is SilverPublicationChangeKind.NONE
        ):
            raise ValueError("A build awaiting publication approval must describe what changed")

    @property
    def verified_equivalent(self) -> bool:
        """Return whether this build reproduced a publication."""

        return self.status is SilverPublicationDecisionStatus.VERIFIED_EQUIVALENT

    @property
    def requires_approval(self) -> bool:
        """Return whether publication requires human approval."""

        return self.status is SilverPublicationDecisionStatus.AWAITING_APPROVAL


def decide_silver_publication(
    *,
    current_publication: DatasetPublication | None,
    candidate_fingerprint: SilverDatasetFingerprint,
) -> SilverPublicationDecision:
    """
    Compare one successful Silver build with the active publication.

    No active publication means that the first publication requires
    explicit approval. Identical logical data and schema produce a
    verification rather than another public revision.
    """

    if current_publication is None:
        return SilverPublicationDecision(
            status=SilverPublicationDecisionStatus.AWAITING_APPROVAL,
            change_kind=SilverPublicationChangeKind.INITIAL_PUBLICATION,
            baseline_publication_id=None,
        )

    if current_publication.fingerprint_version != candidate_fingerprint.fingerprint_version:
        return SilverPublicationDecision(
            status=SilverPublicationDecisionStatus.AWAITING_APPROVAL,
            change_kind=SilverPublicationChangeKind.FINGERPRINT_VERSION_CHANGED,
            baseline_publication_id=current_publication.publication_id,
        )

    if (
        current_publication.logical_hash_algorithm != candidate_fingerprint.logical_hash_algorithm
        or current_publication.schema_hash_algorithm != candidate_fingerprint.schema_hash_algorithm
    ):
        return SilverPublicationDecision(
            status=SilverPublicationDecisionStatus.AWAITING_APPROVAL,
            change_kind=SilverPublicationChangeKind.FINGERPRINT_ALGORITHM_CHANGED,
            baseline_publication_id=current_publication.publication_id,
        )

    logical_data_changed = (
        current_publication.logical_data_hash != candidate_fingerprint.logical_data_hash
    )

    schema_changed = current_publication.schema_hash != candidate_fingerprint.schema_hash

    if not logical_data_changed and not schema_changed:
        return SilverPublicationDecision(
            status=SilverPublicationDecisionStatus.VERIFIED_EQUIVALENT,
            change_kind=SilverPublicationChangeKind.NONE,
            baseline_publication_id=current_publication.publication_id,
        )

    if logical_data_changed and schema_changed:
        change_kind = SilverPublicationChangeKind.LOGICAL_DATA_AND_SCHEMA_CHANGED
    elif logical_data_changed:
        change_kind = SilverPublicationChangeKind.LOGICAL_DATA_CHANGED
    else:
        change_kind = SilverPublicationChangeKind.SCHEMA_CHANGED

    return SilverPublicationDecision(
        status=SilverPublicationDecisionStatus.AWAITING_APPROVAL,
        change_kind=change_kind,
        baseline_publication_id=current_publication.publication_id,
    )
