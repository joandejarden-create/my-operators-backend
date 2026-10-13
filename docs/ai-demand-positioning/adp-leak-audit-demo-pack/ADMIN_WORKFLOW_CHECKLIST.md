# Admin Workflow Checklist — AI Demand Leak Audit

For Joan or a future team member. Live page: `/admin/adp-leak-audits/workflow`

## Checklist

1. **Create request** — `/admin/adp-leak-audits/new`  
   Capture hotel, website, market, contact, demand segment, source.  
   Status → `requested`. No production hotel writes.

2. **Approve request** — `/admin/adp-leak-audits`  
   Confirm qualification answers. Click **Approve**.  
   Status → `approved`.

3. **Run audit**  
   Click **Run Audit**. Phase 1 stores run + observations only in the leak-audit store.  
   Status → `running` → `completed`.

4. **Review observations**  
   Confirm mentions, competitors, territories, and any run error notes.  
   Failures stay on the leak-audit run only.

5. **Generate / open report**  
   Report is created on run completion. Open **View Report** and sanity-check owner readability.  
   Sales conversations can use `/adp-leak-audit/sample`.

6. **Mark sent**  
   After sharing with the prospect, click **Mark Sent**.  
   Status → `sent`.

7. **Promote to pilot**  
   Only with explicit confirmation modal. Phase 1.5 creates a promotion stub and sets status → `converted`.  
   Does **not** create production ADP monitoring records yet.

## Safety reminders

- No full proprietary prompts in client materials
- No Census / Brand Explorer / Operator Explorer / paid ADP writes from this workflow
- Cautious language only
- Free audit remains separate from paid ADP
