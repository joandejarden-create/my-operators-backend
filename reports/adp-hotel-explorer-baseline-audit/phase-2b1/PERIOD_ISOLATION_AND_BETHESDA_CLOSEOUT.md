# Phase 2B.1 closeout — period isolation and Bethesda periods

## Period isolation

`readPublishedEvidence(propertyId, periodId)` and `loadPublishedEvidenceIndex(propertyId, periodId)` now open only `evidence-<requestedPeriodId>.json` when a period is requested.

The period snapshot manifest must name that same period and property. The file checksum must match `evidenceSha256`. A mismatch throws `ADP_DURABLE_INTEGRITY_FAILURE`. A missing snapshot, a period id for another property, or a path escape returns `ADP_DURABLE_EVIDENCE_PERIOD_UNAVAILABLE` and does not fall through to the current pointer.

An omitted period id still reads the current pointer. A checksum-valid `items: []` index remains readable and is not treated as complete supporting evidence (`isDurableEvidenceSupportingComplete` is false).

## Bethesda periods preserved

| Period | Label | What exists | Certification |
|--------|--------|-------------|---------------|
| `…20260909091016_9f3a60` | September lock vector | Filesystem report, business subject rate 46.4 | Still the customer-surface expectation. Not re-pinned by this change |
| `…20261002142154_62428a` | October filesystem published current | Report plus a large evidence index. Evidence-link audit passed on this package (68 links) | Candidate historical baseline only. Not approved |
| `…20261105213117_f695d6` | November durable current | Durable report (business 30) and runtime file with 192 parsed observations in the authorized backup. Durable evidence index is an empty `items: []` stub | Not certified |

November research can be reconstructed without a paid rerun: the runtime observations and the durable report already exist. The published evidence index for that period was not baked, so reconstruction is not the same as a certified customer package. This change does not rebuild that index, does not approve certification, and does not publish.

October is the stronger historical certification candidate because its filesystem package already carries the evidence index. It stays labeled as the October measurement. `certifiedCount` stays 0. Territory locks were not edited.
