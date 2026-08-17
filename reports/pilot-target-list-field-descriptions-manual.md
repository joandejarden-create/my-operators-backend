# Pilot Target List — manual field descriptions

Use if Airtable Meta API description updates are unavailable.

Base: `appKZuK006BWIVjNW`
Table: **Pilot Target List** (`tblgsKWuI25MWohAP`)

## Fields to add or update

### Company

Company associated with the target. Used for context, filtering, and mail merge company name.

### Role

Contact's current role or title.

### Category

Original high-level category for the target. Kept for existing reporting and list segmentation.

### Region

Primary market, region, or geography relevant to this target.

### Email

Email address used for manual outreach or mail merge export. Required for email-based sends.

### LinkedIn URL

LinkedIn profile or company URL used for research and manual LinkedIn outreach.

### Warm Intro?

Indicates whether this target may be reachable through a warm introduction.

### Intro Source

Person, relationship, or channel that may be able to provide an introduction.

### Warm Intro Contact

Specific person who may be asked to make the introduction.

### Relationship Strength

Current strength of the relationship or connection to this target.

### Priority

Outreach priority for pilot execution. Use P1 for highest-priority targets.

### Pilot Relevance

How relevant this target is to the owner/advisor pilot based on likely fit or usefulness.

### Likely Contribution

What this person may contribute to the pilot, such as a real deal, owner feedback, advisor feedback, operator perspective, or referral.

### Why They Matter

Narrative explanation of why this target matters and why we may contact them.

### Outreach Message Angle

High-level message angle to use for this target, such as Owner Pilot, Advisor Partner, Referral, or Operator/Brand perspective.

### Email Subject

Subject line for the email or mail merge export.

### LinkedIn DM Draft

Short message draft for LinkedIn outreach.

### Follow-Up Draft

Draft follow-up message to use if there is no response or if a reply requires a next step.

### Status

Legacy or existing status field. Keep for historical tracking if already used.

### Next Action

Immediate next manual action for this target.

### Last Contact Date

Date this target was last contacted.

### Next Follow-Up Date

Date when the next follow-up should occur.

### Reply Notes

Notes from replies, conversations, objections, or follow-up context.

### Notes

General notes about the target, relationship, or outreach context.

### Ready for Mail Merge

Check only when the row is approved for export. Requires Outreach Status = Approved, Email Subject, Final Approved Email, and valid Email for email sends.

### Send Channel

Primary channel for outreach, such as Email, LinkedIn, WhatsApp, Warm Intro, Manual Call, or Other.

### Do Not Contact

Check to exclude this target from outreach and mail merge exports.

### Do Not Contact Reason

Reason this target should not be contacted.

## Fields skipped (existing descriptions)

- **Name**: Primary contact name for the pilot outreach target.
- **Outreach Segment**: Pilot outreach segment (broader than Category picklist).
- **Pilot Fit**: Pilot fit for this wave (complements Pilot Relevance).
- **Personalization Line**: Custom first line or founder-led context.
- **Email Draft**: Draft email body — not necessarily approved.
- **Final Approved Email**: Final approved copy for manual send or mail merge export.
- **Outreach Status**: Pilot outreach workflow status (separate from legacy Status field).
- **Mail Merge Batch**: e.g. Pilot Wave 1, Pilot Wave 2

## Fields in map but not on table

- Last Contacted Date — Date this target was last contacted. Use this if the table uses this field name instead of Last Contact Date.
- Message Angle — Narrative version of the message angle, if this field exists separately. Use for a short explanation of why the outreach is relevant.

