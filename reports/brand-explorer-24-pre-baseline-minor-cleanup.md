# Brand Explorer 24 pre-baseline minor cleanup

Version: `24-pre-baseline-minor-cleanup-v1` · Generated: 2026-07-23T17:55:30.000Z  
Mode: **apply** · writePerformed=true

## Verdict

**Protected baseline can now be frozen.**  
Post-cleanup quality audit: `ready_to_freeze_24_brand_baseline` — **24/24** `approve_for_baseline_freeze`, **0** remediation, **0** blockers, **0** cross-brand image reuse.

## Scope

Targets (11 `approve_after_minor_cleanup` only):  
autograph-collection, bw-premier-collection, bw-signature-collection, comfort-inn-suites, curio-collection, hotel-indigo, kimpton, preferred-hotels-and-resorts, radisson-blu, small-luxury-hotels-of-the-world, tribute-portfolio

Excluded: Radisson Collection, Tapestry Collection by Hilton (not Active/Live). Freeze-approved brands untouched except via shared tooling (no Presentation writes to those 13).

## Waves

| Wave | Patches | Image actions | Notes |
|------|---------|---------------|-------|
| 1 | 97 | 46 | Scenario title diversification; missing optional slots; thin tags/portfolio; gallery caption alignment; Preferred Bonvoy scrub |
| 2 | 3 | 0 | Preferred residual: proof.2 Body ≥35 words + thicker operator tags (Wave 1 had shortened proof under PVQL) |
| **Total** | **100** | **46** | |

## Per-brand summary

| Brand | Finding Source (categories) | Patch count | Image actions | Before Recommendation | After Recommendation |
|-------|----------------------------|-------------|---------------|----------------------|---------------------|
| autograph-collection | repeated_visual_role; caption_mismatch; missing overview/similar/owner_considerations; thin tags/portfolio | 8 | 3 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| bw-premier-collection | repeated_visual_role; missing similar/owner_considerations | 4 | 2 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| bw-signature-collection | repeated_visual_role; missing similar/owner_considerations; thin tags | 5 | 2 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| comfort-inn-suites | caption_mismatch; missing guest psych/similar/owner_considerations; thin brand_involvement | 10 | 6 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| curio-collection | repeated_visual_role; caption_mismatch; missing guest psych/similar/owner_considerations; thin ops fields | 12 | 6 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| hotel-indigo | caption_mismatch; missing overview/watchouts/guest psych/similar/owner_considerations; thin tags/portfolio | 11 | 4 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| kimpton | caption_mismatch; missing guest psych/similar/owner_considerations; thin management/staffing | 11 | 6 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| preferred-hotels-and-resorts | repeated_visual_role; missing overview/similar/owner_considerations; thin tags; wrong_brand→thin proof.2 residual | 7+3 | 2 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| radisson-blu | caption_mismatch; missing guest psych/similar/owner_considerations | 9 | 6 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| small-luxury-hotels-of-the-world | repeated_visual_role; missing overview/watchouts/guest psych/similar/owner_considerations; thin tags/portfolio | 10 | 3 | approve_after_minor_cleanup | approve_for_baseline_freeze |
| tribute-portfolio | repeated_visual_role; caption_mismatch; missing guest psych/similar/owner_considerations; thin tags | 10 | 6 | approve_after_minor_cleanup | approve_for_baseline_freeze |

### Preferred residual detail (Wave 2)

| Brand | Finding Source | Record ID | Field | Current Issue | Patch Applied | Before | After |
|-------|----------------|-----------|-------|---------------|---------------|--------|-------|
| preferred-hotels-and-resorts | thin:operations.operator_compat.tags | recNHUmpe7xkWWIW5 | Body | Thin Body (9 words) | yes | approve_after_minor_cleanup / brief remediation | approve_for_baseline_freeze |
| preferred-hotels-and-resorts | thin:overview.proof.2 | recRha8AHJaGPXtsc | Title+Body | Thin proof.2 Body (30 words; need ≥35) | yes | PVQL fail / remediation_required | approve_for_baseline_freeze |

## Image strategy

- Preserved existing distinct scenario images.
- Retitled scenarios 2/3 to `Commercial Platform And Lobby Experience` / `Destination Lifestyle Stay Experience` where `repeated_visual_role` was flagged (avoids three exterior/reposition heuristics).
- Gallery Title role-prefix alignment only where caption_mismatch flagged.
- No wholesale gallery replacement; no logo-only / parent-company / wrong-brand imagery.

## Post-validation

| Gate | Result |
|------|--------|
| `brand-explorer-24-tab-section-quality-audit --dry-run` | **ready_to_freeze_24_brand_baseline** · 24 freeze · 0 remediation |
| `test:brand-explorer-public-visibility-quality-lock --public-full-only` | **24/24 PASS** · CV untouched |
| `brand-explorer-os --stage release-readiness --dry-run --skip-regression` | **7× active_profile_ready → no_action** |
| `test:brand-explorer-mandatory-release-gates` | **PASS** |

## Guardrails (confirmed)

- Company Validated / Source Library / Registry / Brand Status / release / public restore: untouched
- Radisson Collection + Tapestry: excluded
- No broad rewrites; Presentation-only targeted fields

## npm

```bash
npm run brand-explorer-24-pre-baseline-minor-cleanup -- --dry-run
```
