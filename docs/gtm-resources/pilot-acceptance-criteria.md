# Pilot Acceptance Criteria

**Status:** Final — Jul 2026  
**Use:** Decide on calls and in follow-up whether a contact becomes a **real pilot opportunity** or stays **feedback / referral only**.  
**Related:** Pilot Target List `Pilot Fit` field · `docs/gtm-resources/reply-playbook.md` · pilot call script (mt-06)

---

## Three outcomes (pick one per contact)

| Outcome | Meaning | Platform invite? |
|---------|---------|------------------|
| **Real pilot opportunity** | Owner-opt-in deal or realistic scenario worth structuring in Dealality | Yes — after access hygiene + QA |
| **Feedback / referral only** | Useful perspective, criteria input, or potential future intro — no active deal | No platform invite |
| **Not a fit / defer** | Wrong segment, timing, or geography for Wave 1 | No — log and follow up later if appropriate |

Map to **Pilot Fit** in Pilot Target List:

| Outcome | Pilot Fit value |
|---------|-----------------|
| Real pilot opportunity (high confidence) | `Strong Pilot Candidate` |
| Real pilot opportunity (needs validation) | `Possible Pilot Candidate` |
| Feedback / referral only | `Feedback / Referral Only` |
| Defer | `Follow-Up Later` |
| Wrong fit | `Weak Fit` or `Not A Fit` |

---

## Real pilot opportunity — acceptance checklist

Accept when **all required** items are true and **at least two** supporting signals are present.

### Required (all must be true)

1. **Owner opt-in** — The owner (or authorized decision-maker) has agreed to explore Dealality for a specific opportunity or realistic scenario.
2. **Defined subject** — There is a identifiable hotel project: location (city/country), asset type or scale, and stage (existing, conversion, ground-up, or realistic hypothetical).
3. **Pilot-appropriate scope** — One opportunity (or one primary scenario), not a portfolio-wide rollout or vague "exploring the space."
4. **Confidentiality comfort** — Participant understands what will be shared, who sees it, and that nothing moves to brands/operators without their control.
5. **Joan capacity** — You can provision, support, and review outputs within the current pilot wave (typically ≤3 active real opportunities at once in Wave 1).

### Supporting signals (at least two)

- Timing: decision, LOI, RFP, or brand/operator conversation within ~12 months.
- Objective: reflag, new build, operator search, brand comparison, or restructuring — not generic curiosity only.
- Information depth: willing to share enough for readiness assessment (location, keys, stage, constraints) even if financials come later.
- Advisor alignment: lawyer/advisor supports structured comparison (or owner is direct and self-advised).
- CALA / pilot geography fit: aligns with Wave 1 focus (CALA and adjacent markets) unless explicitly prioritized otherwise.

### Disqualifiers (any one → not a real pilot opportunity yet)

- No owner opt-in; advisor-only curiosity with no client engagement path.
- Request to use Dealality as a broker/intermediary to source buyers or confidential pipelines.
- Brand or operator asking for owner lead flow (use feedback / criteria path only).
- Participant unwilling to share even high-level opportunity parameters.
- Purely theoretical with no path to a real or realistic scenario in 12 months.

---

## Feedback / referral only — when to use

Use this path when the conversation is valuable but **does not** meet real pilot acceptance.

**Typical cases**

- Lawyer/advisor likes the concept but has no active client deal.
- Operator or brand offers **criteria input** or market perspective — not a client opportunity.
- Referral source willing to intro **later** with owner opt-in.
- Non-CALA contact useful for workflow feedback (see Outreach Message Angle: `Non-CALA Workflow Feedback`).

**What you still deliver**

- Thank-you and clear close.
- Optional: one-pager, sample output pack (sanitized), or 20-minute feedback call.
- Warm intro blurb if they offer introductions.

**What you do not deliver**

- Memberstack / platform login.
- Deal record creation with their confidential data.
- Implied brand or operator outreach on their behalf.

---

## Decision flow (on call or within 24h after)

```
Start: First conversation completed
  │
  ├─ Owner opt-in + defined subject?
  │     No → Feedback / Referral Only (or Follow-Up Later)
  │     Yes ↓
  │
  ├─ Required checklist (5/5)?
  │     No → Possible Pilot Candidate + list gaps in Reply Notes
  │     Yes ↓
  │
  ├─ ≥2 supporting signals?
  │     No → Feedback / Referral Only; revisit in 90 days
  │     Yes ↓
  │
  ├─ Any disqualifier?
  │     Yes → Feedback / Referral Only or Not A Fit
  │     No ↓
  │
  └─ Strong Pilot Candidate → schedule provisioning QA → invite
```

---

## After acceptance — next steps

### Real pilot opportunity

1. Log in PTL: Pilot Fit → `Strong Pilot Candidate` or `Possible Pilot Candidate`.
2. Outreach Status → `Converted To Pilot` when invite sent.
3. Run **access hygiene** and **pilot opportunity QA** (mt-14, mt-22) before production invite.
4. Use **pilot intake questions** (mt-10) — not full deal-setup intake on first call.
5. Provision per `owner-pilot-provisioning-runbook` (Company Profile workspace access, Memberstack sync).

### Feedback / referral only

1. Pilot Fit → `Feedback / Referral Only`.
2. Capture learnings in Reply Notes / pilot learning log (mt-23).
3. Set Next Follow-Up Date if referral may mature (+60–90 days).
4. No platform provisioning.

---

## Wave 1 success metrics (founder view)

| Metric | Target (Wave 1) |
|--------|-----------------|
| Real pilot opportunities accepted | 1–3 |
| Feedback / referral conversations | Unlimited; prioritize quality notes |
| Platform invites sent | Only accepted real opportunities |
| Owner-opt-in introductions from advisors | Track via Warm Intro Contact |

---

## Alignment with call script

On every pilot call, confirm explicitly:

1. "Is there a specific opportunity or realistic scenario you would want to structure — or is this mainly perspective / referral?"
2. "If we proceed, anything shared moves forward only with the owner's opt-in — does that work?"
3. "What would make this a useful use of 30 minutes for you?"

If answers point to feedback only, **do not** push platform access. Close with gratitude and one clear next step (referral blurb, follow-up date, or one-pager).
