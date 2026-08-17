# Brand Activation Research Mode

`researchMode = "brand_activation"`

## Workflow

Brand target → existence/status → parent/regional → positioning → development model → hotel census (MX→CALA→Americas) → pipeline → owner/operator → BE claims → contradiction search → image integrity → completeness/gates → **activation readiness recommendation**

## Statuses

- Ready for Activation Review
- Targeted Remediation Required
- Deep Research Required
- Hold — Conflicting Evidence
- Hold — Insufficient Current Evidence
- Brand Appears Inactive / Discontinued

## Hard gates (override %)

- Current brand identity
- Parent company
- Brand currently exists (**only** with strong discontinuation language — HTTP 403/bot-block is **not** discontinuation)
- Source authority (official site OK **or** census Open/Pipeline **or** official directory rows)
- Mexico/CALA census when claimed

Existence can be corroborated by Dealality census Open/Pipeline hotels or official directory rows when homepage fetches are blocked.

**Never activates automatically.**

## Brand Activation Candidate

When verified census hotels exist for a brand with no Active/Live Brand Explorer profile → flag `brandActivationCandidate: true` (Avani-class detection).
