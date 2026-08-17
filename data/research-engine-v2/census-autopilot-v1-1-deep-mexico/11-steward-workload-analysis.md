# Steward Workload Analysis (V1.1 Live Deep)

## After live deep research (365 hotels)

| Bucket | Count |
|--------|------:|
| Production Candidate (autonomous staging) | 153 |
| Material remediation (batch steward) | 194 |
| Partial | 18 |
| Deep research / hold | ~0 |
| Image rights review (parallel, not data-blocking) | 365 |
| Run failures | 0 |

## Human exception rate

- **Promoted without Joan:** 54 of 244 prior remediation → Production Candidate (~22%)
- **Steward-touch share:** ~53% still in remediation/partial queues
- **Research operations:** Autopilot ran unattended (priority, ladder, resume, continue-on-failure)

Routine Unknown on owner/operator/flags is **not** a Joan research task — accept Unknown or first-party pack.

## Target assessment

**Joan does not manage research operations.** Remaining human work: exception routing, write approval, first-party packs, Brand Completion activation decisions.
