# Accor protected-46 baseline reconciliation — failure extraction

Generated: 2026-07-28T19:49:06.774Z

## Verdict

Wave 13 Accor Active brands were dropped from identity maps when FACTORY_PREVIEW became Wave-14-only. Without resolveActiveUniverseRecordId(slug), PVQL/footnote/quality fall back to slug-as-name → brand_not_found → PVQL blocker → quality remediation_required (40 vs 46). Not a Wave 14 Stage 5 write defect.

- Quality freeze on disk: **45** / 46 (gap 1)
- Classification: **B** recordId↔slug resolver gap + **C** alias mismatch (+ **E** footnote identity, **A** stale reports)
- Airtable writes required: **false**

## Failure table

| Brand | Slug | Record ID | Failure Source | Failure Type | Current Resolver | Expected | Root Cause | Airtable Write? |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Mama Shelter | mama-shelter | recXCZCK05XXYX7Q8 | pvql|footnote_audit|protected_46_quality_freeze_count | brand_not_found_resolver_identity | recXCZCK05XXYX7Q8 | recXCZCK05XXYX7Q8 | B_C_resolver_identity_alias | false |
| Mercure | mercure | recevrLJ3m6rIug3S | 24_tab_quality_audit|pvql|footnote_audit|protected_46_quality_freeze_count | brand_not_found_resolver_identity | recevrLJ3m6rIug3S | recevrLJ3m6rIug3S | B_C_resolver_identity_alias | false |
| Novotel | novotel | recQE2lSSSSyuUrMQ | pvql|footnote_audit|protected_46_quality_freeze_count | brand_not_found_resolver_identity | recQE2lSSSSyuUrMQ | recQE2lSSSSyuUrMQ | B_C_resolver_identity_alias | false |
| Pullman | pullman | recFW9kfqKfOjv7Z1 | pvql|footnote_audit|protected_46_quality_freeze_count | brand_not_found_resolver_identity | recFW9kfqKfOjv7Z1 | recFW9kfqKfOjv7Z1 | B_C_resolver_identity_alias | false |
| Fairmont | fairmont-hotels-and-resorts | recJhPaDVU3YUDQUt | pvql|footnote_audit|protected_46_quality_freeze_count | brand_not_found_resolver_identity | recJhPaDVU3YUDQUt | recJhPaDVU3YUDQUt | B_C_resolver_identity_alias | false |
| SO/ | so-hotels-and-resorts | recTJdPlr4mDs9app | pvql|footnote_audit|protected_46_quality_freeze_count | brand_not_found_resolver_identity | recTJdPlr4mDs9app | recTJdPlr4mDs9app | B_C_resolver_identity_alias | false |

## Proposed fix

Restore Wave 13 Accor durable identity anchors (recordId↔slug + fairmont/so aliases) in EXTRA_ACTIVE_IDENTITY_ANCHORS; stop reading Accor ids from Wave-14-only FACTORY_PREVIEW_CANDIDATE_IDENTITIES; refresh Accor PVQL/quality/footnote reports only.

