# GDI Field Authority Audit V1

## Original local presentation intent (contacts)

Tile footer shows **one** name + **one** subtitle, in order:

1. Verified professional / activity-specific title → `displayRole` (`displayRoleKind=professional_title`)
2. Else opportunity-specific role enum → `opportunityRole` (Event Organizer, Group Travel Coordinator, …)
3. Else `Role Not Yet Verified`

`buyerEligibility` / contact relevance are **not** tile subtitles.

**Lorna Kipfer fixture:** `displayRole=RIC Conference Contact`, `opportunityRole=Group Travel Coordinator`. Card must show RIC Conference Contact.

## Package fingerprint

- Approved: `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66`
- Package: `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66`
- Match: true

## Audit results

- Opportunities: 67
- Material substitution / drift findings: **0**
- Lorna fixture: PASS

## Matrix

See `FIELD_AUTHORITY_MATRIX.json` (13 fields).

## Publication content

No fingerprint change in this audit. Mapper enforces authority at read/attach time only.
