# Founder approval request — Bethesda local-master publication

## Authority
Local authenticated GDI at `http://127.0.0.1:8080` (`?live=1`) is the master.

## Candidate (do not activate without your OK)

| Field | Value |
|-------|-------|
| Hotel | Bethesda Marriott `recLuxvwwxID7U2B8` |
| Version | `gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v1` |
| **Fingerprint to approve** | `b45106b7cd82e92e41423a698ac739bb2d67db6ab2bcf12850ef6b7f5d2a0165` |
| Inventory | All 67 / Hotel 61 / Competitive 10 |
| Prior live package (still CURRENT) | `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f` |

## Example content retained from local master

| Opportunity | Local master (candidate) | Prior package (still live externally) |
|-------------|--------------------------|----------------------------------------|
| Potomac Memorial | Build Account | Qualify Opportunity |
| Bethesda Premier Cup | Build Account | Qualify Opportunity |
| Regulatory Information Conference | Monitor | Qualify Opportunity |

## What approval authorizes
Switch Bethesda `CURRENT` pointer from prior fingerprint → candidate fingerprint, then deploy/verify production against the frozen local master.

## What is already done without activation
- Local master snapshot frozen
- Difference register vs prior package
- Candidate package written + `STAGED`
- Local master ↔ candidate contract parity: **0 mismatches**
- **CURRENT unchanged** (customers still on prior package)

Reply with exact fingerprint approval to proceed past this boundary.
