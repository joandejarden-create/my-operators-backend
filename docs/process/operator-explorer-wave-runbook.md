# Operator Explorer — Wave Runbook

## Intended input

```text
Wave Name
+
List of Operator Names
```

## Pipeline stages

1. Entity resolution (Master / alias / provisional) — **exception on ambiguity**
2. Current-data audit (Claims, Presence, Assignments, Brand Rel)
3. Source discovery → PI Source Library dedupe
4. Assignment research (named properties)
5. Market Presence (geo SoT)
6. Brand Relationships (typed; BMC scoped)
7. Claims (only non-structured leftovers)
8. Evidence + publication classification
9. Conflict detection → holdouts
10. Write plan + backup
11. Apply + validate
12. Explorer readiness
13. Exception report for founder

## Automation readiness

| Stage | Class |
| ----- | ----- |
| Entity resolution | Automatable with exception handling |
| Assignment harvest | Automatable with exception handling |
| Publication of routine facts | Fully automatable |
| Master create | Periodic human / founder approval |
| Approval claims / duplicate Masters | Founder approval required |

Founder reviews **exceptions**, not routine fields.