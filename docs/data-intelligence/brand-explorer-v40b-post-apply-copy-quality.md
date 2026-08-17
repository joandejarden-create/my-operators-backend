# v40B — Post-Apply Copy Quality Audit + Founder Review Packet

After v40 Presentation owner-copy scrub for Everhome Suites, Kimpton, and Radisson Individuals by Choice, verify forbidden language is gone and scrubbed copy still reads naturally — then produce founder review packets.

```bash
npm run brand-explorer-v40b-post-apply-copy-quality -- --brands everhome-suites,kimpton,radisson-individuals-by-choice --dry-run
```

## Source of truth (important)

v38 quality lock hides these profiles externally until active release. **External DOM lock PASS only proves Profile in Preparation remains hidden.**

v40B audits full content via:

1. Brand Explorer Presentation rows (Title / Body / Case Summary*)
2. Live Brand Library API blocks
3. Internal preview renderer (`?beInternalPreview=1`) — full profile tabs

Founder packets are based on internal preview / full profile, not the locked external shell.

## Checks

| Check | Pass criteria |
|-------|----------------|
| Forbidden language | Zero LOI, FDD, Item 19, franchise disclosure, fee stack, net contribution, raw URLs, Sources:/Source:, disclosure document, performance representation, ADR, RevPAR |
| Mechanical copy | Flag awkward scrub replacements (owner economics, participation cost categories, repeated diligence boilerplate, vague emptied rows) |
| Brand-specific | Everhome extended-stay cues; Kimpton lifestyle/F&B; Radisson soft-brand/conversion — no franchise/LOI boilerplate |
| External DOM lock | Still Profile in Preparation only |
| Founder decision | `founder_visual_review_ready` \| `more_remediation_required` \| `not_owner_ready` |

## Guardrails

- Read-only / dry-run only (`--apply` refused)
- No active-profile approval
- No Company Validated / Source Library / Registry / image-field changes
- No unlock

## Deliverables

- `reports/brand-explorer-v40b-post-apply-copy-quality.json`
- `reports/brand-explorer-v40b-post-apply-copy-quality.md`
- `reports/brand-explorer-v40b-founder-review-{slug}.md` (per brand)

## Known residual failure modes (post-v40)

1. **Presentation leftovers** — ADR/RevPAR tokens, visible `https://…` in Body, mechanical “participation cost categories” / LOI rewrites.
2. **Renderer / Brand Setup chrome** — Economics “Typical Economics at a Glance” still injects FDD Item 7 / disclosure document / LOI language from Brand Setup fields (outside Presentation scrub scope).
3. External quality lock remaining PASS only means Profile in Preparation is still the external shell.

Until Presentation leftovers **and** economics chrome are clean under `?beInternalPreview=1`, founder decision stays `not_owner_ready` or `more_remediation_required`.
