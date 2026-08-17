# Reply Playbook — Outreach Response Templates

**Status:** Final — Jul 2026  
**Use:** First-wave lawyer/advisor/owner outreach replies (email and LinkedIn).  
**Tone:** Personal, concise, low-pressure — match `lib/gtm-owner-target/pilot-outreach-draft-templates.js`.  
**Related:** `docs/gtm-resources/warm-intro-blurb.md`, `docs/gtm-resources/pilot-acceptance-criteria.md`

---

## How to use

1. Read the reply; classify using the **Reply type** below.
2. Pick the matching template; personalize `{{first_name}}` and one specific line from their message.
3. Log in **Pilot Target List → Reply Notes**; update **Outreach Status** and **Pilot Fit** per the table at the end.
4. Do not send attachments or platform access until **pilot acceptance criteria** are met.

---

## Reply types

### 1. Happy to chat / open to a call

**Signals:** "Happy to connect," "let's find time," "open to a conversation."

```
Hi {{first_name}},

Thank you — I appreciate you getting back to me.

I'd welcome a short conversation. I'm keeping the pilot group small, so the call is simply to see whether Dealality might be useful for a real or realistic opportunity you are advising on — or whether your perspective alone would be valuable.

Would any of these work for a 20-minute call?
- [Option A]
- [Option B]

If easier, feel free to send a couple of times that work for you.

At your service,
Joan
```

**PTL:** Outreach Status → `Replied` or `Meeting Scheduled` · Pilot Fit → assess on call

---

### 2. Send more info

**Signals:** "Tell me more," "what is Dealality," "send materials."

```
Hi {{first_name}},

Happy to share a bit more.

Dealality is a small pilot I'm running with owners and advisors to structure hotel opportunities before brand/operator conversations — assessing readiness, comparing alignment options, and clarifying what information may still be missing.

I'm attaching / sharing the [Advisor/Consultant | Owner/Developer | Lawyer/Referral] overview — one page, no obligation.

If after reading it a short conversation still seems relevant, I'd welcome that. If not, no worries at all.

At your service,
Joan
```

**Attach:** Correct pilot overview one-pager for their segment.  
**PTL:** Outreach Status → `Replied` · Pilot Fit → `Possible Pilot Candidate` or `Feedback / Referral Only` until call

---

### 3. May know someone / referral offer

**Signals:** "I might know an owner," "happy to intro if relevant," "let me think about who."

```
Hi {{first_name}},

That would be very helpful — thank you.

I'm only looking for introductions where the owner has opted in and a conversation would genuinely be welcome. No need to share confidential details in advance.

If you are open to making an intro, here is a short forwardable blurb you can paste (also happy to draft a tighter version for a specific person):

[paste from docs/gtm-resources/warm-intro-blurb.md — Standard forward]

And if it is easier to start with a quick call between us first, I'm happy to do that too.

At your service,
Joan
```

**PTL:** Warm Intro? → Yes · Outreach Message Angle → `Warm Intro / Referral` · Pilot Fit → `Feedback / Referral Only` until owner engages

---

### 4. Confidentiality / data / client concerns

**Signals:** "Is this confidential," "what happens to client data," "can I share deal details."

```
Hi {{first_name}},

Completely fair question.

For this pilot, anything shared moves forward only with the owner's opt-in. I am not asking you to pass along confidential client materials. A short conversation can stay high-level until everyone is comfortable.

If we proceed with a real opportunity, we would agree on what is shared, who sees it, and whether the owner wants to use the platform for structured comparison — or simply use the conversation for feedback.

Happy to walk through that on a brief call if useful.

At your service,
Joan
```

**PTL:** Reply Notes → capture specific concern · Pilot Fit → do not upgrade until concern resolved

---

### 5. Advisory / brokerage role clarification

**Signals:** "Are you a broker," "how do you work with advisors," "fee model," "conflict with my role."

```
Hi {{first_name}},

Good question — I want to be clear.

Dealality is not a brokerage and is not replacing your advisory role. The pilot is about helping owners and advisors structure and compare options before brand/operator conversations — not about intermediating your client relationships.

If there is a real opportunity and the owner opts in, you remain in control of how you advise and whether anything moves forward.

If a short call would help clarify fit, I'm happy to do that.

At your service,
Joan
```

**PTL:** Pilot Fit → often `Feedback / Referral Only` unless active deal emerges · Outreach Message Angle → `Feedback / Perspective`

---

### 6. No active deal right now

**Signals:** "No deals at the moment," "nothing active," "maybe later."

```
Hi {{first_name}},

Understood — thank you for letting me know.

If timing changes, I'd still welcome your perspective on whether this kind of readiness workflow is useful in practice. No pressure either way.

I'll check back in a few months unless you'd prefer I don't — just let me know.

At your service,
Joan
```

**PTL:** Outreach Status → `Follow-Up Later` · Next Follow-Up Date → +90 days · Pilot Fit → `Follow-Up Later`

---

### 7. Not relevant / wrong fit

**Signals:** "Not my area," "not interested," "wrong person," "please remove me."

```
Hi {{first_name}},

Thank you for the quick reply — I appreciate it.

I'll close the loop on my side. If I misread fit, apologies for the noise.

If you ever come across an owner or advisor who might find this useful, a light introduction is always welcome — but absolutely no obligation.

At your service,
Joan
```

**PTL:** Outreach Status → `Not Interested` (or `Archived` if hard no) · Do Not Contact → only if they ask · Pilot Fit → `Not A Fit`

---

### 8. No response (follow-up)

**Timing:** 4–6 business days after first touch; align with **Next Follow-Up Date** in PTL.

```
Hi {{first_name}} — just wanted to follow up on my note about the Dealality pilot. No rush at all — I'm keeping the first group small and would still really value your perspective if relevant.

At your service,
Joan
```

**Rules:** One follow-up per wave unless they re-engage. No third touch without new signal.  
**PTL:** Outreach Status → `Follow-Up Needed` then `Follow-Up Sent` after send

---

## Quick classification → PTL fields

| Reply type | Outreach Status | Pilot Fit (initial) | Next action |
|------------|-----------------|---------------------|-------------|
| Happy to chat | Replied / Meeting Scheduled | TBD on call | Book call; use call script |
| Send more info | Replied | Possible Pilot Candidate | Send one-pager; offer call |
| Referral offer | Replied | Feedback / Referral Only | Send warm intro blurb |
| Confidentiality | Replied | TBD | Address on call |
| Advisory clarification | Replied | Feedback / Referral Only | Clarify role; offer call |
| No active deal | Follow-Up Later | Follow-Up Later | Set +90d follow-up |
| Not relevant | Not Interested | Not A Fit | Close loop |
| No response | Follow-Up Sent | unchanged | Wait; one follow-up only |

---

## Tone guardrails

- One clear CTA per message.
- No defensive disclaimers (see `BANNED_DRAFT_PHRASES` in outreach templates).
- Do not promise brand introductions, funding, or guaranteed matches.
- Joan sends manually — agents draft only.
