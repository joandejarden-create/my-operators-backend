# GDI 10 Bases of Demand — Taxonomy

Demand engine answers: **WHAT TYPE OF DEMAND?**  
Base of demand answers: **HOW DID WE FIND / GENERATE THE OPPORTUNITY?**

Both are persisted.

| # | Base | Meaning |
|---|------|---------|
| 1 | `PUBLISHED_EVENT_DECOMPOSITION` | 1. Published Event Decomposition |
| 2 | `INTERNATIONAL_ORG_RECURRING_GROUPS` | 2. International Org / Recurring Groups |
| 3 | `PARTICIPANT_EXHIBITOR_SPONSOR_MINING` | 3. Participant / Exhibitor / Sponsor Mining |
| 4 | `HISTORIC_ROTATION_PREDICTION` | 4. Historic Rotation Prediction |
| 5 | `RECURRING_CORPORATE_MEETINGS` | 5. Recurring Corporate Meetings |
| 6 | `CORPORATE_TRIGGER_DEMAND` | 6. Corporate Trigger Demand |
| 7 | `PHARMA_MEDICAL_ECOSYSTEM` | 7. Pharma / Medical Ecosystem |
| 8 | `PROJECT_WORKFORCE_DEMAND` | 8. Project / Workforce Demand |
| 9 | `SPORTS_ENTERTAINMENT_PRODUCTION` | 9. Sports / Entertainment / Production |
| 10 | `HOTEL_HISTORY_LOOKALIKE` | 10. Hotel History + Lookalike |

## Maturity states

- `CONFIRMED_OPPORTUNITY` — canonical customer-ready gate passed
- `PREDICTED_OPPORTUNITY` — rotation/recurrence inferred; **NOT confirmed business**
- `VALID_FUTURE_WATCH` — canonical watch gate
- `RESEARCH_LEAD` — pursuing evidence
- `GENERATOR_INTELLIGENCE` — retained under generator; not auto-promoted
- `REJECTED`

## Coverage states

NOT_RESEARCHED | LIGHT | ADEQUATE | DEEP | SATURATED | NOT_APPLICABLE

## Hard rules

- Attendance ≠ room demand
- Phone co-occurrence ≠ lodging proof
- Apify = SIGNAL until page-validated
- Jev cannot create truth / admit / promote / change thresholds
- GDI thresholds unchanged
