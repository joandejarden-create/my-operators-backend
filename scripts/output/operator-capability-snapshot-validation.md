# Operator Capability Snapshot — patched deals validation

Generated: 2026-05-22T23:12:55.962Z

## Confirmation checks

1. Project Type canonical only (stored value): **PASS**
2. No acquisition in Project Type: **PASS**
3. Snapshot logic uses Project Type + operating fields: **see per-deal rules triggered**
4. Other/TBC avoids project-type capability inference: **PASS**
5. Exactly one deal flagged for manual review (Needs Review / uncertain backfill): **PASS** — **Xavier v2.0**

## Summary

- Allowed: 2
- Limited: 7
- Blocked: 0

## Deals

| Deal Name | Project Type | Current Operating Model | Opening / Transition Phase | Primary Market Region | Preferred Future Operating Model | Operator Capability Priorities | Owner Reporting Frequency | Top inferred areas | Rules triggered | Missing / uncertain | Snapshot | Manual review? | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Hampton by Hilton Lima Centro | New Build | Owner-operated (unbranded) | N/A (stabilized operating) | CALA | Undecided / exploring | Accounting & owner reporting | Weekly | Pre-opening / opening support; Design / renovation PM; Development complexity / permitting; Local market / CALA execution | generic_context_blob, project_type:New Build, project_type_kind, stated_priorities | — | allowed | false | No manual review required beyond routine validation. |
| Courtyard by Marriott Buenos Aires | New Build | Third-party managed (branded) | N/A (stabilized operating) | CALA | Brand-managed | Revenue management & distribution; HR & training; Full hotel management; Sales & marketing | Weekly | Pre-opening / opening support; Design / renovation PM; Development complexity / permitting; Local market / CALA execution | generic_context_blob, project_type:New Build, project_type_kind, stated_priorities | — | limited | false | No manual review required beyond routine validation. |
| DoubleTree by Hilton Santiago Centro | Conversion / Reflag | Owner-operated (unbranded) | Rebranding in place | CALA | Third-party management only | HR & training; Accounting & owner reporting; Full hotel management | Ad hoc | Conversion & PIP execution; Brand standards alignment; Operator transition / handover; Pre-opening / opening support; Local market / CALA execution | generic_context_blob, project_type:Conversion / Reflag, project_type_kind, stated_priorities | — | limited | false | Resolve clarifications before sharing snapshot externally. Reconcile: Current model is owner-operated but preferred future targets third-party management |
| Hilton Bogota Corferias | New Build | Owner-operated (unbranded) | Construction | CALA | Owner-operated | Full hotel management; HR & training; Revenue management & distribution | Weekly | Pre-opening / opening support; Design / renovation PM; Development complexity / permitting; Local market / CALA execution | generic_context_blob, project_type:New Build, project_type_kind, stated_priorities | — | allowed | false | No manual review required beyond routine validation. |
| Hilton Garden Inn Medellin | New Build | Owner-operated (unbranded) | Construction | CALA | Undecided / exploring | Revenue management & distribution; HR & training; Sales & marketing | Weekly | Pre-opening / opening support; Design / renovation PM; Development complexity / permitting; Local market / CALA execution | generic_context_blob, project_type:New Build, project_type_kind, stated_priorities | — | limited | false | No manual review required beyond routine validation. |
| Courtyard by Marriott Amsterdam Airport | New Build | Owner-operated (unbranded) | Planning / entitlement | CALA | Brand-managed | Full hotel management | Monthly | Pre-opening / opening support; Design / renovation PM; Development complexity / permitting; Local market / CALA execution | generic_context_blob, project_type:New Build, project_type_kind, stated_priorities | — | limited | false | No manual review required beyond routine validation. |
| Marriott Mexico City Centro Historico | Conversion / Reflag | Third-party managed (branded) | Rebranding in place | CALA | Undecided / exploring | Full hotel management | Ad hoc | Conversion & PIP execution; Brand standards alignment; Operator transition / handover; Pre-opening / opening support; Local market / CALA execution | generic_context_blob, project_type:Conversion / Reflag, project_type_kind, stated_priorities | — | limited | false | No manual review required beyond routine validation. |
| Holiday Inn Express Sao Paulo Centro | Conversion / Reflag | Third-party managed (branded) | Rebranding in place | CALA | Undecided / exploring | Accounting & owner reporting | Weekly | Conversion & PIP execution; Brand standards alignment; Operator transition / handover; Pre-opening / opening support; Local market / CALA execution | generic_context_blob, project_type:Conversion / Reflag, project_type_kind, stated_priorities | — | limited | false | No manual review required beyond routine validation. |
| Xavier v2.0 | Renovation / Repositioning | Needs Review | Needs Review | CALA | Undecided / exploring | Full hotel management; HR & training | Monthly | Conversion & PIP execution; Asset management / capex planning; Commercial repositioning; Operating-while-renovating coordination; Revenue management & distribution; Local market / CALA execution | generic_context_blob, project_type:Renovation / Repositioning, project_type_kind, stated_priorities | current_operating_model_needs_review; opening_transition_needs_review | limited | true | PRIORITY: manual review — Needs Review or uncertain backfill on operating fields. Resolve clarifications before sharing snapshot externally. |
