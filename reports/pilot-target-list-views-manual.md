# Pilot Target List — manual Airtable view setup

The Airtable Meta API **cannot create or configure views** (filters, sorts, visible fields) in this environment.
Create each grid view manually in Airtable using the steps below.

Base: `appKZuK006BWIVjNW`
Table: **Pilot Target List** (`tblgsKWuI25MWohAP`)

## Quick steps (repeat per view)

1. Open **Pilot Target List** in Airtable.
2. Click **+** next to the view tabs → **Grid view**.
3. Rename the view to the exact name below.
4. Open **Filter** → **Add condition** → switch to **Formula** and paste the filter.
5. Open **Sort** and add the sort fields in order.
6. Hide all columns, then show only the listed fields **in order** (drag to reorder).
7. Save the view.

## Views already present

- **Pilot Outreach Pipeline** (`viwoSZExYPJbLVOBW`) — verify filter, sort, and visible fields match below
- **Drafting Queue** (`viwJBIUdRkKqrjV9X`) — verify filter, sort, and visible fields match below
- **Approved for Send / Mail Merge** (`viw37u0BOTiS2quqi`) — verify filter, sort, and visible fields match below
- **Follow-Up Needed** (`viwm530SJEcqlqvac`) — verify filter, sort, and visible fields match below

---

## Pilot Outreach Pipeline

**Purpose:** Daily working view for active outreach tracking.

### Filter (formula)

```
AND(
  NOT({Do Not Contact}),
  OR(
    {Outreach Status} = BLANK(),
    AND(
      {Outreach Status} != 'Archived',
      {Outreach Status} != 'Not Interested'
    )
  )
)
```

*Includes rows with blank Outreach Status. Excludes Do Not Contact, Archived, and Not Interested.*

### Sort

1. **Priority** — ascending (P1 → P2 → P3 (ascending works alphabetically))
1. **Next Follow-Up Date** — ascending
1. **Company** — ascending
1. **Name** — ascending

### Visible fields (in order)

- Name
- Company
- Role
- Category
- Outreach Segment
- Pilot Region
- Region
- Priority
- Pilot Fit
- Pilot Relevance
- Warm Intro?
- Relationship Strength
- Likely Contribution
- Why They Matter
- Outreach Message Angle
- Outreach Status
- Next Action
- Last Contact Date
- Next Follow-Up Date
- Reply Notes
- Notes

*Optional fields omitted (not on table):*
- Message Angle

### Notes

- Hide Do Not Contact from this view or keep it hidden — filter already excludes checked rows.
- First wave outreach is manual/founder-led; this view does not send email.

---

## Drafting Queue

**Purpose:** Rows where messaging needs to be written, reviewed, or approved.

### Filter (formula)

```
AND(
  NOT({Do Not Contact}),
  OR(
    {Outreach Status} = 'Draft Needed',
    {Outreach Status} = 'Drafted',
    {Outreach Status} = 'Needs Review'
  )
)
```

### Sort

1. **Priority** — ascending
1. **Outreach Segment** — ascending
1. **Company** — ascending
1. **Name** — ascending

### Visible fields (in order)

- Name
- Company
- Role
- Outreach Segment
- Pilot Region
- Priority
- Pilot Fit
- Why They Matter
- Outreach Message Angle
- Personalization Line
- Email Subject
- Email Draft
- Final Approved Email
- LinkedIn DM Draft
- Follow-Up Draft
- Outreach Status
- Notes

### Notes

- Use Final Approved Email as the send/export copy once reviewed.
- Email Draft is working copy only.

---

## Approved for Send / Mail Merge

**Purpose:** Rows approved for manual sending or CSV export.

### Filter (formula)

```
AND(
  {Outreach Status} = 'Approved',
  {Ready for Mail Merge},
  NOT({Do Not Contact}),
  LEN(TRIM({Email Subject} & '')) > 0,
  LEN(TRIM({Final Approved Email} & '')) > 0,
  OR(
    {Send Channel} != 'Email',
    LEN(TRIM({Email} & '')) > 0
  )
)
```

*Use this view before running export-owner-targets-mail-merge.mjs. Requires Email when Send Channel = Email.*

### Sort

1. **Mail Merge Batch** — ascending
1. **Send Channel** — ascending
1. **Company** — ascending
1. **Name** — ascending

### Visible fields (in order)

- Name
- Email
- Company
- Role
- Pilot Region
- LinkedIn URL
- Send Channel
- Mail Merge Batch
- Email Subject
- Final Approved Email
- LinkedIn DM Draft
- Ready for Mail Merge
- Outreach Status
- Last Contact Date
- Next Follow-Up Date
- Do Not Contact
- Do Not Contact Reason

### Notes

- Export script: node scripts/export-owner-targets-mail-merge.mjs
- Do Not Contact columns shown for verification — filter excludes checked rows.

---

## Follow-Up Needed

**Purpose:** Rows requiring a follow-up.

### Filter (formula)

```
AND(
  NOT({Do Not Contact}),
  OR(
    {Outreach Status} = 'Follow-Up Needed',
    IS_BEFORE({Next Follow-Up Date}, TODAY()),
    IS_SAME({Next Follow-Up Date}, TODAY(), 'day')
  )
)
```

### Sort

1. **Next Follow-Up Date** — ascending
1. **Priority** — ascending
1. **Company** — ascending
1. **Name** — ascending

### Visible fields (in order)

- Name
- Email
- Company
- Role
- Pilot Region
- Send Channel
- Outreach Status
- Last Contact Date
- Next Follow-Up Date
- Follow-Up Draft
- Reply Notes
- Notes
- Do Not Contact

### Notes

- Review Follow-Up Draft and Reply Notes before contacting again.
- Manual follow-up only — no automated sending.

---

## Reminders

- These are **Airtable views only** — they do not send email.
- **Approved for Send / Mail Merge** is the pre-export checklist view.
- **Do Not Contact** excludes targets from outreach and CSV export.
- First wave should remain **manual / founder-led**.

