# Operator Setup Demo-Readiness Execution Checklist

Generated: 2026-06-02  
Purpose: practical execution checklist for demo go/no-go (no new implementation scope).

---

## 1) Final deployment/environment confirmation

Confirm runtime values exactly:

- `NODE_ENV=production`
- `OPERATOR_SETUP_WRITE_MODE=canonical`
- `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`
- `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0`
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=0`
- `OPERATOR_SETUP_CONTRACT_DIAGNOSTICS=0`

Execution check:
- Capture env snapshot before demo run.
- Confirm no temporary overrides in shell/session startup scripts.

---

## 2) Exact pre-demo QA steps

Run in order and mark pass/fail:

1. My Operator create/update save in canonical mode.
2. Reload/prefill includes `serviceModelsSupported`.
3. Detail readback confirms `readPath=new_base`.
4. Explorer detail confirms canonical `serviceModelsSupported`.
5. Non-rec operator request remains mock-blocked.
6. Deal-linked OAS companies validation for exact demo pair.
7. Score breakdown stable across duplicate runs.
8. No shadow/fail-open behavior in logs.
9. Authenticated OAS UI/API smoke pass.
10. Sparse-data completeness scan for selected demo operators.

---

## 3) Demo data checklist (fill before demo)

For each demo pair, capture:

- Confirmed demo deal ID
- Confirmed linked operator ID(s)
- Operator name(s)
- Logo present (`yes/no`)
- Complete profile content (`yes/no`)
- OAS page works for pair (`yes/no`)
- Score page works for pair (`yes/no`)
- Explorer profile presentable (`yes/no`)
- Notes / cleanup owner

---

## 4) Go / No-Go decision table

| Status | When to use | Action |
|---|---|---|
| **Ready** | All env checks pass; all 10 QA steps pass; demo data complete for selected pairs | Proceed with demo |
| **Ready with cleanup** | Core path passes; only minor data presentation cleanup remains | Proceed after closing listed cleanup items |
| **Hold** | Env mismatch, canonical/save/read regressions, unstable score, mock leak, or broken deal-linked OAS pair | Do not demo; fix blockers first |

---

## 5) Explicitly deferred items

- `explorerProfileJson` browser-only mirror diagnostics
- Legacy/alias cleanup
- Batch 4 select/multi-select normalization
- Scoring methodology changes
- Airtable schema changes

