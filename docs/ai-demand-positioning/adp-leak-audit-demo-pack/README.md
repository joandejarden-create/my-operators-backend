# AI Demand Leak Audit — Demo Pack (Phase 1.5)

Sales-ready package for founder outreach, LinkedIn testing, and client conversations.

## Buyer takeaway (under 2 minutes)

> AI may be recommending competitors instead of your hotel. This audit shows where that is happening, why it may be happening, and what to fix first.

## Pack contents

| Asset | Path / URL |
|-------|------------|
| Sample report (live page) | `/adp-leak-audit/sample` |
| Portfolio sample | `/adp-leak-audit/sample-portfolio` |
| Sample report fixture | `fixtures/ai-demand-positioning/leak-audit-sample-report-v1.json` |
| Portfolio fixture | `fixtures/ai-demand-positioning/leak-audit-portfolio-sample-v1.json` |
| Founder demo script | [FOUNDER_DEMO_SCRIPT.md](./FOUNDER_DEMO_SCRIPT.md) |
| Admin workflow checklist | `/admin/adp-leak-audits/workflow` · [ADMIN_WORKFLOW_CHECKLIST.md](./ADMIN_WORKFLOW_CHECKLIST.md) |
| Outreach copy | [OUTREACH_COPY.md](./OUTREACH_COPY.md) |
| Intake qualification questions | [INTAKE_QUALIFICATION_QUESTIONS.md](./INTAKE_QUALIFICATION_QUESTIONS.md) |

## Safety

- No proprietary full prompts
- No production ADP / Census / Airtable IDs in the sample
- Limited diagnostic framing only — does not give away full ADP monitoring

## Gates

```bash
npm run test:adp-leak-audit-client-report-safety-v1
npm run test:adp-leak-audit-demo-pack-v1
```
