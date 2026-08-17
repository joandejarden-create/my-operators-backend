# Pilot Target List dropdown options — manual fallback

Use this if Airtable Meta API option updates fail or are blocked by permissions.

Base: `appKZuK006BWIVjNW`
Table: **Pilot Target List** (`tblgsKWuI25MWohAP`)

## Region dropdown

- Legacy field: **Region** (undefined)
- Structured dropdown field: **Pilot Region**
- If missing, create it as Single select with these options:
  - CALA
  - Mexico
  - Caribbean
  - Central America
  - South America
  - Latin America
  - United States / Canada
  - Europe / Spain
  - Global / Multi-Region
  - Other
  - Unknown / TBD

## Single-select option updates

### Outreach Segment

- Current type: single_select
- Add options (if missing): Brand / Referral Source

### Pilot Fit

- Current type: single_select
- Add options (if missing): Strong Pilot Candidate, Possible Pilot Candidate, Feedback / Referral Only, Follow-Up Later, Weak Fit, Not A Fit

### Priority

- Current type: single_select
- Add options (if missing): (none)

### Outreach Status

- Current type: single_select
- Add options (if missing): Follow-Up Later, Converted To Pilot

### Send Channel

- Current type: single_select
- Add options (if missing): (none)

### Outreach Message Angle

- Current type: single_select
- Add options (if missing): Feedback / Perspective, Warm Intro / Referral, Brand Criteria Input, Operator Perspective, Owner-Opt-In Referral Only, Pilot Update / Re-Engage Later, Non-CALA Workflow Feedback, Other

### Relationship Strength

- Current type: single_select
- Add options (if missing): Strong Warm Relationship, Known Contact, Met Once, LinkedIn / Light Connection, Dormant

### Pilot Relevance

- Current type: single_select
- Add options (if missing): Unknown

## Value migration (manual if needed)

- Review report JSON `migration.proposedUpdates` before changing existing values.

