# UI QA — Customer filter cleanup

## Top-level nav contract

| Check | Result |
|-------|--------|
| Only All / Ready / Watching | PASS (browser: AC, Radisson, YOTEL, Bethesda) |
| No Active Pursuits | PASS |
| No Follow-Up Due | PASS |
| No Hotel Selection | PASS |
| No Closed | PASS |
| No duplicate filter chips | PASS |
| Active-state styling | PASS (uses `.chain-scale-legend-item.active`) |
| Spacing / alignment | PASS (`.gdi-workflow-filter-row` + shared legend) |
| No blank gaps / separators | PASS |

## Filter behavior

| Filter | Expected | Notes |
|--------|----------|-------|
| All | Customer-visible Ready + Watching | Unchanged |
| Ready | Ready / readiness READY / maturity ACTIONABLE (pre-existing) | Unchanged logic; pursuit status does not gate |
| Watching | FUTURE_WATCH / WATCH facing states | Unchanged; pursuit status does not gate |

## Card actions

| Action | Preserved |
|--------|-----------|
| Start Pursuit | YES (`pursuitActionButtonHtml`) |
| View Pursuit | YES |
| Pursuit panel in detail | YES (`pursuitPanelHtml`) |

## Hotels browsed

1. AC Hotel A Coruña (`rec2PVBDavppGpenm`) — filters OK; Watch card visible
2. Radisson Santo Domingo (`recUOyzOXn2Zdp98I`) — filters OK (list empty on this API session; pre-existing data/API, not filter regression)
3. YOTEL (`recrPQcZg7SFARRb2`) — filters OK; opportunities load
4. Bethesda (`recLuxvwwxID7U2B8`) — filters OK; All (28)
