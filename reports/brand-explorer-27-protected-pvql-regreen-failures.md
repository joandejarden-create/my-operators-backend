# Protected 27 PVQL Re-Green — Failure Extraction

Version: `27-protected-pvql-regreen-v1` · Generated: 2026-07-24T12:18:19.861Z
Read-only. Exact failures only (no guessed fields).

## Summary

| Metric | Count |
| --- | ---: |
| Brands | 3 |
| Failure rows | 6 |
| Brands needing fix | 3 |

## Failure table

| Brand | Tab | Section | Record ID | Field | Failure Type | Current Value | Why It Fails | Proposed Fix |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Preferred Hotels & Resorts | Overview | Brand Positioning / Audience | `recwl5JOYxlChuCAr` | Target Guest Segments | generic_copy_scan / generic_audience_prose | Luxury / Discerning, Leisure, Experience-Oriented, International Inbound | Rendered Audience joins multi-select as "Luxury / Discerning, Leisure…", which matches golden GENERIC_AUDIENCE_PROSE and fails PVQL generic_copy_scan. Tab Factory also fails because golden_content_quality is a hard gate (failFindings may still be 0). | Replace Target Guest Segments with: Experience-Oriented, Leisure, International Inbound (drop Luxury / Discerning while keeping Leisure / Experience-Oriented / International Inbound as applicable). |
| Preferred Hotels & Resorts | Tab Factory (aggregate) | golden_content_quality gate | `recwl5JOYxlChuCAr` | Target Guest Segments (via rendered Audience) | tab_factory_audit | Luxury / Discerning, Leisure, Experience-Oriented, International Inbound | tab_factory_audit.pass requires golden_content_quality.pass. Completeness failFindings can be 0 while audit still fails on generic_audience_prose. | Same Target Guest Segments adjacency remediation clears the golden gate and re-greens tab_factory_audit. |
| Radisson Individuals by Choice | Overview | Brand Positioning / Audience | `recRyvM8OmLlDj9G7` | Target Guest Segments | generic_copy_scan / generic_audience_prose | Luxury / Discerning, Leisure, Experience-Oriented | Rendered Audience joins multi-select as "Luxury / Discerning, Leisure…", which matches golden GENERIC_AUDIENCE_PROSE and fails PVQL generic_copy_scan. Tab Factory also fails because golden_content_quality is a hard gate (failFindings may still be 0). | Replace Target Guest Segments with: Experience-Oriented, Leisure (drop Luxury / Discerning while keeping Leisure / Experience-Oriented / International Inbound as applicable). |
| Radisson Individuals by Choice | Tab Factory (aggregate) | golden_content_quality gate | `recRyvM8OmLlDj9G7` | Target Guest Segments (via rendered Audience) | tab_factory_audit | Luxury / Discerning, Leisure, Experience-Oriented | tab_factory_audit.pass requires golden_content_quality.pass. Completeness failFindings can be 0 while audit still fails on generic_audience_prose. | Same Target Guest Segments adjacency remediation clears the golden gate and re-greens tab_factory_audit. |
| Small Luxury Hotels of the World | Overview | Brand Positioning / Audience | `recjjSnY2opb8P4DG` | Target Guest Segments | generic_copy_scan / generic_audience_prose | Luxury / Discerning, Leisure, Experience-Oriented, International Inbound | Rendered Audience joins multi-select as "Luxury / Discerning, Leisure…", which matches golden GENERIC_AUDIENCE_PROSE and fails PVQL generic_copy_scan. Tab Factory also fails because golden_content_quality is a hard gate (failFindings may still be 0). | Replace Target Guest Segments with: Experience-Oriented, Leisure, International Inbound (drop Luxury / Discerning while keeping Leisure / Experience-Oriented / International Inbound as applicable). |
| Small Luxury Hotels of the World | Tab Factory (aggregate) | golden_content_quality gate | `recjjSnY2opb8P4DG` | Target Guest Segments (via rendered Audience) | tab_factory_audit | Luxury / Discerning, Leisure, Experience-Oriented, International Inbound | tab_factory_audit.pass requires golden_content_quality.pass. Completeness failFindings can be 0 while audit still fails on generic_audience_prose. | Same Target Guest Segments adjacency remediation clears the golden gate and re-greens tab_factory_audit. |

## Root cause

All three brands share Brand Basics multi-select `Target Guest Segments` containing both `Luxury / Discerning` and `Leisure`. Public HTML Audience rendering creates the golden `generic_audience_prose` adjacency. That fails `generic_copy_scan` and, transitively, `tab_factory_audit`.

No Presentation Title/Body/Case Summary offenders were required for these PVQL failures once rows include Case Summary Overview.

## Out of scope

- Wave 12 brands
- Tapestry / Dazzler / Trademark / other protected 27
- Company Validated / Source Library / Registry / Brand Status / release / images

