# Publication Monitor Data Model

Schema: `gdi_publication_monitor_v1`

Store: `data/group-demand-intelligence/hotels/{hotelId}/publication-monitors.json`

Audit: `data/group-demand-intelligence/hotels/{hotelId}/publication-monitor-audit.jsonl`

## Fields

| Field | Purpose |
|-------|---------|
| monitorId | Stable id |
| campaignId | Linked demand campaign |
| hotelId | Hotel scope |
| triggerType | Primary watched artifact type |
| watchForTypes | All detected artifact types that fire |
| triggerSourceUrl | Known official / affiliated URL |
| sourceLanguage | es / gl / en |
| currentSourceState | LIST_NOT_YET_PUBLISHED / LIST_PARTIAL / … |
| lastCheckedAt | Last fetch |
| lastContentHash | Normalized content hash |
| lastMeaningfulChangeAt | Last MEANINGFUL / ARTIFACT_PUBLISHED |
| nextCheckAt | Schedule |
| expectedPublicationWindowStart/End | Publication-window scheduling |
| monitoringStatus | ACTIVE / PAUSED / TRIGGERED / COMPLETED / EXPIRED |
| detectedArtifactType | Last fired type |
| reDecompositionStatus | IDLE / QUEUED / COMPLETED / SKIPPED_NO_NEW_EVIDENCE / FAILED |
| previouslySeenEntityIds | Dedup for reprocess |
| customerMonitoringFor | Customer-safe labels |
| priorityRank | 1 = CIELO near-term |
| autoAmericasSpecial | Official hotel ≠ Radisson opportunity |

## Statuses

ACTIVE · PAUSED · TRIGGERED · COMPLETED · EXPIRED
