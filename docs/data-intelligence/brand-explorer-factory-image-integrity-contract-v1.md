# Brand Explorer Factory — Image Integrity Contract V1

**Contract:** `BE_FACTORY_IMAGE_INTEGRITY_V1`  
**Status:** Permanent factory gate (awaiting founder merge review)  
**Module:** `lib/brand-explorer/factory-2/be-factory-image-integrity-v1.js`  
**Recovery:** `lib/brand-explorer/factory-2/be-factory-image-integrity-recovery-v1.js`

## Rule

**A populated image URL is not a successful image.**

Only status `CERTIFIED` raises **Certified Image Completion**.

```
Certified Image Completion = Certified Required Image Slots / Total Required Image Slots
```

Required slots (9):

- `materials.gallery.1` … `materials.gallery.6`
- `overview.scenario.1` … `overview.scenario.3`

## Status vocabulary

| Status | Meaning |
|--------|---------|
| `MISSING` | No URL / asset reference |
| `URL_PRESENT_UNVERIFIED` | URL recorded; not yet fetched |
| `FETCH_FAILED` | HTTP / network / content-type failure |
| `DECODE_FAILED` | Bytes present but not a valid image / zero dimensions |
| `BROWSER_RENDER_FAILED` | Failed to render in Brand Explorer UI |
| `RENDER_VALID` | Fetch+decode OK; awaiting browser + visual certify |
| `VISUAL_REVIEW_REQUIRED` | Technical OK; content/provenance unsuitable or weak |
| `CERTIFIED` | Technical + visual + browser (or bounded fresh prior evidence) |

## Pipeline (canonical factory stages)

**Discover → Select → Fetch → Decode → Browser Render → Visual Validate → Certify → Score → Approval**

Integrated into:

- Image discovery (`be-factory-2-hotel-image-discovery-v1.js` inventory)
- Orchestrator gap + stage certification (`be-factory-2-orchestrator-v1.js`)
- Batch approval gate (`be-factory-2-batch-approval-v1.js`) — blocks brand approval below threshold
- Portfolio dashboard (`be-factory-2-staging-store-v1.js` → `imageIntegrity` section)

## Approval threshold

`FACTORY_IMAGE_APPROVAL_THRESHOLD = 1.0` (100% of required slots `CERTIFIED`).

Failing image slots block **that brand’s** factory approval only — not unrelated brands or previously approved protected content.

## Recovery

Bounded, $0 paid:

1. Record rejection (URL fingerprint ledger)
2. Try authorized image-pool replacements
3. Optional targeted discovery (`--allow-discovery`)
4. Re-validate through the same pipeline
5. Preserve unresolved gaps — never fabricate completion

Never re-research the same rejected URL without new evidence.

## Freshness

Previously `CERTIFIED` evidence may be reused for ≤14 days when the URL fingerprint is unchanged. Revalidate when the asset reference changes, evidence expires, or a render regression is detected.

## Correction disclosure

Earlier Factory 2 scoring treated **URL population** as gallery/scenario completion. That metric is retained as `legacyUrlPopulationCompletion` for before/after comparison and **must not** gate approval.

## Commands

```bash
# Unit / permanent regression
npm run test:be-factory-image-integrity-v1

# Portfolio audit (no writes)
node scripts/be-factory-image-integrity-audit-recovery-v1.mjs --dry-run
node scripts/be-factory-image-integrity-audit-recovery-v1.mjs --dry-run --slugs ac-hotels-by-marriott

# Bounded recovery from authorized pools (still no Airtable / no publish)
node scripts/be-factory-image-integrity-audit-recovery-v1.mjs --apply-recovery --limit 10

# Authenticated :8080 browser certification
node scripts/be-factory-image-integrity-browser-cert-v1.mjs --slugs ac-hotels-by-marriott
```

## STOP

No merge, deployment, paid research, destructive persistence, or customer publication without founder review.
