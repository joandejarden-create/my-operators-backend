# Operator P1 Profile Governance Fields Setup

Generated: 2026-07-06T08:07:21.274Z
Mode: apply
Base: `appvtnDurnMSjINP6`

> Schema-only. No records read or modified.

**P1 scope:** Operator Setup - Master, Operator Setup - Explorer Materials

**Excluded from Setup roots (Partner Intelligence SSOT):** Source URL / File Path, Source Date

## Summary

| Metric | Count |
|--------|-------|
| Fields already present (exact) | 1 |
| Fields satisfied by alias (no create) | 1 |
| Fields would create / created | 26 |
| Fields failed | 0 |

## Skipped — alias equivalent (no duplicate column)

| Table | Expected | Live column | Reason |
|-------|----------|-------------|--------|
| Operator Setup - Master | `Confidence Level` | `Data Confidence Level` | Data Confidence Level exists on Operator Master (OAS/admin semantics). Treat as partial equivalent; map in read paths until unified or options aligned. |

## Created

- **Operator Setup - Master** — `Validation Status` (singleSelect, fldjPzojnBDtl9nOE)
- **Operator Setup - Master** — `Usage Permission` (singleSelect, fldJgEouqbWELmDsI)
- **Operator Setup - Master** — `Source Region` (singleSelect, fldDU0zlp0I03jo8J)
- **Operator Setup - Master** — `Last Reviewed Date` (date, fldswv9VfYXs1NKan)
- **Operator Setup - Master** — `Refresh Due Date` (date, fldim8gujXiAI0ZBo)
- **Operator Setup - Master** — `Evidence Notes` (multilineText, fld2SFm3PCaHwdWlb)
- **Operator Setup - Master** — `Missing Data Flags` (multilineText, fld6gW2wNZXZVjLeB)
- **Operator Setup - Master** — `Company Validated` (checkbox, fld2ohCAcD8r9sneV)
- **Operator Setup - Master** — `Company Validation Date` (date, fldIBW51yy1lFradw)
- **Operator Setup - Master** — `Reviewed By` (singleLineText, fldbaBDttBwSVH85I)
- **Operator Setup - Master** — `External Display Status` (singleSelect, fldnGJjgXCtqVukbQ)
- **Operator Setup - Master** — `Internal Notes` (multilineText, fldiLTtKbrN0V2gXA)
- **Operator Setup - Explorer Materials** — `Validation Status` (singleSelect, fldJl3Ad7Kr4Jt2Lm)
- **Operator Setup - Explorer Materials** — `Usage Permission` (singleSelect, fldtPnDXjYgH4nKl5)
- **Operator Setup - Explorer Materials** — `Source Type` (singleSelect, fld9ZWtN9M6MqUKe5)
- **Operator Setup - Explorer Materials** — `Source Region` (singleSelect, flds5ipnAgSKbA2Mg)
- **Operator Setup - Explorer Materials** — `Last Reviewed Date` (date, fldQwPTXQpak5pgOW)
- **Operator Setup - Explorer Materials** — `Refresh Due Date` (date, fldYmO8Tz2kGdEKYE)
- **Operator Setup - Explorer Materials** — `Confidence Level` (singleSelect, fldiD6W9nDnYTaVdx)
- **Operator Setup - Explorer Materials** — `Evidence Notes` (multilineText, fldV4j3bIZVP02UxJ)
- **Operator Setup - Explorer Materials** — `Missing Data Flags` (multilineText, flddZ2dA926RQkLw0)
- **Operator Setup - Explorer Materials** — `Company Validated` (checkbox, fld9y8X1Zu5j6KiTX)
- **Operator Setup - Explorer Materials** — `Company Validation Date` (date, fldO7m8luoZUrUcvE)
- **Operator Setup - Explorer Materials** — `Reviewed By` (singleLineText, fldMtvNgYfsT7kT43)
- **Operator Setup - Explorer Materials** — `External Display Status` (singleSelect, fldvbpuX6kCyvojiR)
- **Operator Setup - Explorer Materials** — `Internal Notes` (multilineText, fldEPBHhsU3UbEhv1)

## Already present (exact name)

- **Operator Setup - Master** — `Source Type`

## Per-table detail

### Operator Setup - Master

| Field | Status | Live / notes |
|-------|--------|--------------|
| `Validation Status` | created | — |
| `Usage Permission` | created | — |
| `Source Type` | present | `Source Type` |
| `Source Region` | created | — |
| `Last Reviewed Date` | created | — |
| `Refresh Due Date` | created | — |
| `Confidence Level` | skipped_alias | `Data Confidence Level` |
| `Evidence Notes` | created | — |
| `Missing Data Flags` | created | — |
| `Company Validated` | created | — |
| `Company Validation Date` | created | — |
| `Reviewed By` | created | — |
| `External Display Status` | created | — |
| `Internal Notes` | created | — |

### Operator Setup - Explorer Materials

| Field | Status | Live / notes |
|-------|--------|--------------|
| `Validation Status` | created | — |
| `Usage Permission` | created | — |
| `Source Type` | created | — |
| `Source Region` | created | — |
| `Last Reviewed Date` | created | — |
| `Refresh Due Date` | created | — |
| `Confidence Level` | created | — |
| `Evidence Notes` | created | — |
| `Missing Data Flags` | created | — |
| `Company Validated` | created | — |
| `Company Validation Date` | created | — |
| `Reviewed By` | created | — |
| `External Display Status` | created | — |
| `Internal Notes` | created | — |
