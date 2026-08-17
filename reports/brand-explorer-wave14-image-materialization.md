# Wave 14 Stage 5 — Image / Visual Materialization

- Generated: 2026-07-28T17:25:39.511Z
- Mode: **APPLY**
- Ready: **9/9** · Blocked: **0**
- Patches planned: **103**

## Brand results

- **Marriott Hotels**: ready · g6/6 s3/3 o3/3
- **Sheraton**: ready · g6/6 s3/3 o3/3
- **Westin**: ready · g6/6 s3/3 o3/3
- **Residence Inn by Marriott**: ready · g6/6 s3/3 o3/3
- **SpringHill Suites by Marriott**: ready · g6/6 s3/3 o3/3
- **TownePlace Suites by Marriott**: ready · g6/6 s3/3 o3/3
- **Aloft Hotels**: ready · g6/6 s3/3 o3/3
- **Four Points Flex by Sheraton**: ready · g4/6 s3/3 o0/3 (documented holds)
- **StudioRes**: ready · g6/6 s3/3 o3/3

## Four Points Flex — cleanly unavailable holds

- Live brand page is Getty/stock-only; official Flex imagery limited to Marriott development gallery + one Scene7 Flex property asset (`xf-lonfb-the-hub`).
- Gallery: **4/6** distinct official images; remaining gallery slots cleanly unavailable (no Four Points by Sheraton substitute).
- Openings/property: **0/3** — named Flex property overview URLs not steward-matched; Stage 4 openings row set to **Do Not Display**.
- Scenario: **3/3** cleared (primary Stage 5 goal).
- Post-apply hygiene: `scripts/wave14-flex-hold-hygiene.mjs` (Do Not Display openings + F&B caption repair).

## Guardrails confirmed

- Exactly nine Wave 14 brands imaged
- All remain Under Review / factory preview only
- No Brand Status / release / CV / Source Library / Registry writes
- No protected 46 / Accor Wave 13 Active / House of Originals / Morgans / Radisson Collection writes
- Accor 24-tab watch note: `reports/brand-explorer-wave14-active-baseline-watch-note.md`

## Post-apply validation (partial)

- Image uniqueness: **8/9** pass; Flex fails slot-count gates only (documented holds; no duplicate URLs among written images)
- Image role-match: **8/9** pass; Flex fails galleryCount&lt;6 only (unresolved wrong-role = 0 after hygiene)
- Tab-factory: field failFindings=0 / empty=0; non-image pattern gates remain false (pre-existing Stage 4 / Momentum pattern debt)
- Golden content quality: **9/9 PASS**
- No-empty rendered components: **9/9 PASS**
- Rendered field completeness: **8/9**; Flex `footprint.openings:missing` = accepted openings hold
- PVQL public-full-only: **PASS** (`overallPass=true`, public-full cohort 40)
- Protected 46 baseline: **FAIL** on Accor Wave 13 Active brands only (`quality_freeze_count:40_expected_46`) — see watch note; **not** Wave 14 Stage 5 writes
- AI-Assisted footnote: Accor slug/`brand_not_found` noise on same watch set; Wave 14 factory-preview brands not in Active footnote universe

## Ready

**`wave14_image_materialization_ready_for_post_image_cleanup`**

## Apply flags

- `--approve-wave14-image-materialization`
- `--confirm-nine-brand-stage5-scope`
- `--confirm-target-brands-only`
- `--confirm-no-protected-46-brand-changes`
- `--confirm-no-accor-wave13-active-brand-writes`
- `--confirm-no-house-of-originals-writes`
- `--confirm-no-morgans-originals-writes`
- `--confirm-no-radisson-collection-changes`
- `--confirm-no-company-validation-changes`
- `--confirm-no-source-library-status-changes`
- `--confirm-no-registry-approval-changes`
- `--confirm-no-brand-status-changes`
- `--confirm-no-release-field-writes`
- `--confirm-no-content-rewrites`
- `--confirm-image-uniqueness`
- `--confirm-image-role-match`
- `--confirm-no-wrong-brand-images`
- `--confirm-no-sibling-brand-images`
- `--confirm-marriott-brand-family-separated`
- `--confirm-four-points-flex-not-four-points`
- `--confirm-studiores-not-residence-inn-or-towneplace`
- `--confirm-cala-first-openings-priority`
- `--confirm-international-reference-labels-where-needed`
- `--confirm-property-url-matches-required-for-named-gallery`
- `--confirm-cleanly-unavailable-for-unsupported-property-images`
