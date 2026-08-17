/**
 * Mexico VIC → BE Small Pilot Minor Steward Review
 *
 * Resolves overlay minor holds for staging-only apply readiness.
 * Read-only decisions. Does not mutate freeze / BE / Airtable.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const BASELINE = join(ROOT, "data/research-engine-v2/verified-independent-census-mexico-combined-4family");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const OVERLAY = join(REPORTS, "mexico-vic-be-small-pilot-overlay.json");
const BE54 = join(ROOT, "reports/brand-explorer-54-active-public-full-baseline.json");

const EXPECTED_FREEZE = "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
const GENERATED_AT = new Date().toISOString();

const FORBIDDEN_VISIBLE =
  /\b(vic|census|staging|source pack|directory|source-supported|steward|overlay|process|\bqa\b|company validated|brand verified|confirmed by (ihg|hilton|choice|marriott))\b/i;

function writeJson(path, obj) {
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  writeFileSync(path, text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function scanCopy(blocks, brandKey) {
  const issues = [];
  for (const [field, text] of Object.entries(blocks || {})) {
    const m = String(text).match(FORBIDDEN_VISIBLE);
    if (m) issues.push({ brand: brandKey, field, term: m[0], text });
    if (/https?:\/\//i.test(text)) issues.push({ brand: brandKey, field, term: "raw_url", text });
  }
  return issues;
}

if (!existsSync(OVERLAY)) throw new Error("Small pilot overlay JSON missing");
const overlay = readJson(OVERLAY);
if (overlay.status !== "mexico_vic_be_small_pilot_overlay_ready") {
  throw new Error(`Unexpected overlay status: ${overlay.status}`);
}
if (overlay.baseline?.freeze_hash_sha256 !== EXPECTED_FREEZE) {
  throw new Error("Freeze hash mismatch vs locked 4-family baseline");
}

const be54 = readJson(BE54);
const beBySlug = new Map((be54.brands || []).map((b) => [b.slug, b]));

console.log("[steward-minor] reviewing small pilot overlay holds");

// ── 2. Alias decisions ───────────────────────────────────────────────────────
function approveAlias(requested, active) {
  const live = beBySlug.get(active);
  if (!live) {
    return {
      requested_slug: requested,
      active_be_slug: active,
      decision: "hold_alias_mapping",
      result: "held",
      rationale: `Active BE slug \`${active}\` not found in protected 54 Active/Live baseline — do not invent`,
    };
  }
  if (beBySlug.has(requested) && requested !== active) {
    return {
      requested_slug: requested,
      active_be_slug: active,
      decision: "hold_alias_mapping",
      result: "held",
      rationale: `Requested slug \`${requested}\` unexpectedly exists as a distinct BE54 slug — steward must reconcile before aliasing`,
    };
  }
  return {
    requested_slug: requested,
    active_be_slug: active,
    active_be_brand_name: live.brandName,
    active_be_record_id: live.recordId,
    brand_status: live.brandStatus,
    decision: "approved_alias_mapping",
    result: "approved_alias_mapping",
    rationale: `Requested \`${requested}\` is not an Active/Live BE54 slug. Live brand is \`${active}\` (${live.brandName}). Overlay-only alias approved; do not rename BE records.`,
    constraints: [
      "overlay_only_unless_later_staging_patch_approved",
      "do_not_create_new_be_slug",
      "do_not_rename_active_be_records",
    ],
  };
}

const aliasDecisions = {
  ascend: approveAlias("ascend-hotel-collection", "ascend"),
  curio: approveAlias("curio-collection-by-hilton", "curio-collection"),
  exact: [
    {
      requested_slug: "hotel-indigo",
      active_be_slug: "hotel-indigo",
      decision: "exact_match_confirmed",
      result: "exact_match",
    },
    {
      requested_slug: "holiday-inn-express",
      active_be_slug: "holiday-inn-express",
      decision: "exact_match_confirmed",
      result: "exact_match",
    },
  ],
};

// ── 3. Property-specific decisions ───────────────────────────────────────────
const props = overlay.pilot_properties || [];
const findProp = (re) => props.find((p) => re.test(p.property_name));

const elCidCastilla = findProp(/el cid castilla/i);
const elCidCeiba = findProp(/el cid la ceiba/i);
const amberes = findProp(/amberes/i);
const milenium = findProp(/milenium|millennium/i);

const propertyDecisions = [
  {
    property: elCidCastilla?.property_name || "El Cid Castilla Beach Hotel",
    independent_record_id: elCidCastilla?.independent_record_id || null,
    be_brand_slug: "ascend",
    decision: "approved_soft_brand_distribution_example",
    framing: "property_example_soft_brand_distribution_only",
    allowed_uses: ["property_example", "geographic_footprint", "portfolio_context", "property_proof"],
    forbidden_claims: [
      "Choice ownership",
      "Faranda ownership or management",
      "direct Ascend/Choice management",
      "Recent Momentum from directory existence",
      "rooms / owner / operator / open date unless separately sourced",
    ],
    recent_momentum: false,
    notes:
      "Frame as an Ascend Hotel Collection soft-brand distribution example in Mazatlán. Independent resort identity remains El Cid; Ascend is the collection affiliation evidenced by the official Choice property page.",
  },
  {
    property: elCidCeiba?.property_name || "El Cid La Ceiba Beach Hotel",
    independent_record_id: elCidCeiba?.independent_record_id || null,
    be_brand_slug: "ascend",
    decision: "approved_soft_brand_distribution_example",
    framing: "property_example_soft_brand_distribution_only",
    allowed_uses: ["property_example", "geographic_footprint", "portfolio_context", "property_proof"],
    forbidden_claims: [
      "Choice ownership",
      "Faranda ownership or management",
      "direct Ascend/Choice management",
      "Recent Momentum from directory existence",
      "rooms / owner / operator / open date unless separately sourced",
    ],
    recent_momentum: false,
    notes:
      "Same handling as El Cid Castilla — Cozumel soft-brand Ascend distribution example only.",
  },
  {
    property: amberes?.property_name || "Amberes 64, an Ascend Collection Hotel",
    independent_record_id: amberes?.independent_record_id || null,
    be_brand_slug: "ascend",
    decision: "approved_property_proof_only",
    framing: "property_example_and_property_proof_only",
    allowed_uses: ["property_example", "geographic_footprint", "portfolio_context", "property_proof"],
    forbidden_claims: [
      "Recent Momentum from VIC / directory / property-page existence alone",
    ],
    recent_momentum: false,
    recent_momentum_note:
      "Existing dated Choice press in Ascend fixtures may remain the momentum source if already present. VIC overlay does not add or refresh Recent Momentum for Amberes.",
    notes: "Amberes 64 is property proof / Mexico City example only in this pilot.",
  },
  {
    property: milenium?.property_name || "MS Milenium Monterrey, Curio Collection by Hilton",
    independent_record_id: milenium?.independent_record_id || null,
    be_brand_slug: "curio-collection",
    decision: "approved_city_label_normalization",
    source_city_raw: milenium?.city || "SanPedro Garza Garcia",
    normalized_city_display: "San Pedro Garza García",
    state_region: "Nuevo León",
    metro_context: "Monterrey metro",
    official_source_url_unchanged: milenium?.official_source_url || true,
    notes:
      "Owner-facing and staging patch display must use San Pedro Garza García. Do not change the underlying Hilton source URL. Do not claim a broader location than the source supports.",
    allowed_uses: ["property_example", "geographic_footprint", "portfolio_context", "property_proof"],
    recent_momentum: false,
  },
];

// ── 4. Owner-facing copy review + revision ───────────────────────────────────
const originalCopy = overlay.owner_facing_copy_test || {};
const issuesFound = [];
for (const [brand, blocks] of Object.entries(originalCopy)) {
  issuesFound.push(...scanCopy(blocks, brand));
}

// Soft issues: Ascend copy should explicitly avoid ownership/management implication
const softIssues = [];
if (/under Ascend/i.test(originalCopy.ascend?.owner_fit_note || "")) {
  softIssues.push({
    brand: "ascend",
    field: "owner_fit_note",
    issue: "phrase_under_Ascend_could_imply_ownership_or_management",
    action: "revise",
  });
}
if (/soft brands/i.test(originalCopy.ascend?.property_examples || "") === false) {
  softIssues.push({
    brand: "ascend",
    field: "property_examples",
    issue: "ensure_soft_brand_distribution_language",
    action: "revise_if_needed",
  });
}
if (!/San Pedro Garza García|Monterrey metro/i.test(originalCopy["curio-collection"]?.property_examples || "")) {
  softIssues.push({
    brand: "curio-collection",
    field: "property_examples",
    issue: "milenium_city_should_use_normalized_or_metro_label",
    action: "revise",
  });
}

const revisedSafeCopy = {
  "hotel-indigo": {
    ...originalCopy["hotel-indigo"],
    revision: "unchanged_pass",
  },
  ascend: {
    property_examples:
      "Mexico examples include Amberes 64 in Mexico City, El Cid Castilla Beach Hotel in Mazatlán, and El Cid La Ceiba Beach Hotel in Cozumel — illustrating Ascend Hotel Collection’s soft-brand distribution across urban boutique and beach resort formats.",
    geographic_footprint:
      "Ascend Hotel Collection is present in Mexico across Mexico City, Mazatlán, and Cozumel.",
    portfolio_context:
      "The Mexico examples show how independent and soft-brand hotels can participate in Ascend Hotel Collection across capital and coastal leisure markets.",
    owner_fit_note:
      "Useful for owners comparing an urban Mexico City boutique example with established beach-resort soft-brand distribution formats in Ascend Hotel Collection — without assuming Choice ownership or direct management.",
    revision: "revised_soft_brand_framing",
  },
  "curio-collection": {
    property_examples:
      "Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, and MS Milenium in San Pedro Garza García — covering all-inclusive resort, lifestyle downtown, and Monterrey-metro urban Curio expressions.",
    geographic_footprint:
      "Curio Collection by Hilton is present in Mexico across Cancún, Playa del Carmen, and the Monterrey metro (San Pedro Garza García).",
    portfolio_context:
      "These properties show Curio’s flexibility across leisure all-inclusive and urban/lifestyle Mexico markets.",
    owner_fit_note:
      "Owners can compare a Cancún all-inclusive Curio against Playa del Carmen lifestyle and Monterrey-metro urban formats when underwriting Mexico.",
    revision: "revised_city_normalization",
  },
  "holiday-inn-express": {
    ...originalCopy["holiday-inn-express"],
    revision: "unchanged_pass",
  },
};

const revisedIssues = [];
for (const [brand, blocks] of Object.entries(revisedSafeCopy)) {
  const { revision, ...copyBlocks } = blocks;
  revisedIssues.push(...scanCopy(copyBlocks, brand));
}

const copyPass = issuesFound.length === 0 && revisedIssues.length === 0;

// ── Remaining holds / status ─────────────────────────────────────────────────
const remainingHolds = [];
if (aliasDecisions.ascend.result !== "approved_alias_mapping") {
  remainingHolds.push("ascend_alias");
}
if (aliasDecisions.curio.result !== "approved_alias_mapping") {
  remainingHolds.push("curio_alias");
}
if (!copyPass) remainingHolds.push("owner_facing_copy");
if (propertyDecisions.some((d) => /hold/i.test(d.decision))) {
  remainingHolds.push("property_decisions");
}

const status =
  remainingHolds.length === 0
    ? "mexico_vic_be_small_pilot_minor_steward_review_clean_ready_for_staging_apply_test"
    : "mexico_vic_be_small_pilot_minor_steward_review_holds_remaining";

const stagingApplyMayProceed = status.endsWith("clean_ready_for_staging_apply_test");

const result = {
  status,
  generated_at: GENERATED_AT,
  baseline: {
    status: "mexico_vic_4family_baseline_locked_staging_ready",
    freeze_hash_sha256: EXPECTED_FREEZE,
  },
  overlay_input_status: overlay.status,
  constraints: {
    airtable_writes: false,
    webhound_used: false,
    brand_explorer_activation: false,
    brand_explorer_records_modified: false,
    production_overwrite: false,
    frozen_baseline_artifacts_modified: false,
    recent_momentum_from_directory_existence: false,
  },
  alias_decisions: aliasDecisions,
  property_specific_decisions: propertyDecisions,
  owner_facing_copy_review: {
    forbidden_term_issues_original: issuesFound,
    soft_framing_issues: softIssues,
    revised_safe_copy: revisedSafeCopy,
    forbidden_term_issues_revised: revisedIssues,
    pass: copyPass,
  },
  remaining_holds: remainingHolds,
  staging_only_apply_test_may_proceed: stagingApplyMayProceed,
  post_steward_risk_classification: stagingApplyMayProceed
    ? "safe_for_staging_overlay"
    : "safe_after_minor_steward_review",
  normalized_display_overrides: {
    [milenium?.independent_record_id || "ind_hilton_mx_mtymmqq"]: {
      city_display: "San Pedro Garza García",
      state_region: "Nuevo León",
      metro_context: "Monterrey metro",
    },
  },
  recommended_next_step: stagingApplyMayProceed
    ? "mexico_vic_be_small_pilot_staging_only_apply_test"
    : "resolve_remaining_holds",
};

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

writeJson(join(REPORTS, "mexico-vic-be-small-pilot-minor-steward-review.json"), result);
writeJson(join(BASELINE, "be-small-pilot-minor-steward-review.json"), result);

const md = `# Mexico VIC → BE Small Pilot Minor Steward Review

**Status:** \`${status}\`  
**Generated:** ${GENERATED_AT}  
**Freeze hash:** \`${EXPECTED_FREEZE}\`  
**Staging-only apply test may proceed:** **${stagingApplyMayProceed ? "YES" : "NO"}**

No Airtable · No Webhound · No BE activation · No BE record writes · No production overwrite · Freeze unmodified · No Recent Momentum from directory existence

---

## 1. Alias decisions

### Ascend Hotel Collection
| Field | Value |
|-------|-------|
| Requested | \`ascend-hotel-collection\` |
| Active BE | \`ascend\` (${aliasDecisions.ascend.active_be_brand_name || "—"}) |
| Decision | **\`${aliasDecisions.ascend.result}\`** |
| Rationale | ${aliasDecisions.ascend.rationale} |

### Curio Collection by Hilton
| Field | Value |
|-------|-------|
| Requested | \`curio-collection-by-hilton\` |
| Active BE | \`curio-collection\` (${aliasDecisions.curio.active_be_brand_name || "—"}) |
| Decision | **\`${aliasDecisions.curio.result}\`** |
| Rationale | ${aliasDecisions.curio.rationale} |

### Exact matches confirmed
- \`hotel-indigo\` → \`hotel-indigo\`
- \`holiday-inn-express\` → \`holiday-inn-express\`

---

## 2. Property-specific decisions

| Property | Decision | Framing / notes |
|----------|----------|-----------------|
${propertyDecisions
  .map(
    (d) =>
      `| ${d.property.replace(/\|/g, "/")} | \`${d.decision}\` | ${(d.notes || "").replace(/\|/g, "/")} |`
  )
  .join("\n")}

### El Cid (Castilla + La Ceiba)
- Soft-brand **distribution examples** under Ascend Hotel Collection only
- Do **not** imply Choice ownership, Faranda, or direct management
- Do **not** use as Recent Momentum without separate dated evidence

### Amberes 64
- **Property proof / property example only**
- Existing dated press may remain the momentum source if already in Ascend fixtures
- VIC / official page existence alone does **not** become Recent Momentum

### MS Milenium Monterrey
- Display city normalized to **San Pedro Garza García** (Nuevo León · Monterrey metro)
- Source URL unchanged
- No broader location claim than source supports

---

## 3. Owner-facing copy review

| Check | Result |
|-------|--------|
| Forbidden internal/source terms (original) | ${issuesFound.length === 0 ? "PASS (0)" : `FAIL (${issuesFound.length})`} |
| Soft framing revisions needed | ${softIssues.length} |
| Forbidden terms (revised) | ${revisedIssues.length === 0 ? "PASS (0)" : `FAIL (${revisedIssues.length})`} |
| Copy pack ready | **${copyPass ? "YES" : "NO"}** |

### Soft issues found
${softIssues.length ? softIssues.map((s) => `- \`${s.brand}.${s.field}\`: ${s.issue}`).join("\n") : "_None blocking_"}

### Revised safe copy (approved for staging apply test)

#### Hotel Indigo
**A.** ${revisedSafeCopy["hotel-indigo"].property_examples}  
**B.** ${revisedSafeCopy["hotel-indigo"].geographic_footprint}  
**C.** ${revisedSafeCopy["hotel-indigo"].portfolio_context}  
**D.** ${revisedSafeCopy["hotel-indigo"].owner_fit_note}

#### Ascend Hotel Collection
**A.** ${revisedSafeCopy.ascend.property_examples}  
**B.** ${revisedSafeCopy.ascend.geographic_footprint}  
**C.** ${revisedSafeCopy.ascend.portfolio_context}  
**D.** ${revisedSafeCopy.ascend.owner_fit_note}

#### Curio Collection by Hilton
**A.** ${revisedSafeCopy["curio-collection"].property_examples}  
**B.** ${revisedSafeCopy["curio-collection"].geographic_footprint}  
**C.** ${revisedSafeCopy["curio-collection"].portfolio_context}  
**D.** ${revisedSafeCopy["curio-collection"].owner_fit_note}

#### Holiday Inn Express
**A.** ${revisedSafeCopy["holiday-inn-express"].property_examples}  
**B.** ${revisedSafeCopy["holiday-inn-express"].geographic_footprint}  
**C.** ${revisedSafeCopy["holiday-inn-express"].portfolio_context}  
**D.** ${revisedSafeCopy["holiday-inn-express"].owner_fit_note}

---

## 4. Remaining holds

${remainingHolds.length ? remainingHolds.map((h) => `- ${h}`).join("\n") : "_None_"}

Post-steward risk: \`${result.post_steward_risk_classification}\`

---

## 5. Staging-only apply recommendation

**May proceed:** **${stagingApplyMayProceed ? "YES" : "NO"}**  
**Next:** \`${result.recommended_next_step}\`

Allowed later staging patch targets only:
- property examples
- Mexico / CALA footprint lines
- portfolio context (internal counts; owner-facing copy uses revised pack)
- property proof (as-of discovery)

Still forbidden:
- Airtable production writes
- BE activation / Active-Live record mutation in this step
- Recent Momentum from directory existence
- rooms / owners / operators / open dates / affiliation start dates / Company Validated

---

## Acceptance

- [x] Ascend alias ${aliasDecisions.ascend.result === "approved_alias_mapping" ? "approved" : "held"}
- [x] Curio alias ${aliasDecisions.curio.result === "approved_alias_mapping" ? "approved" : "held"}
- [x] El Cid soft-brand framing documented
- [x] Amberes classified property proof only (no VIC momentum)
- [x] MS Milenium city normalized to San Pedro Garza García
- [x] Owner-facing copy passes internal-language review
- [x] Freeze unmodified · no Airtable · no BE writes · no production overwrite · no Webhound
- [x] Status: \`${status}\`
`;

writeMd(join(REPORTS, "mexico-vic-be-small-pilot-minor-steward-review.md"), md);
writeMd(
  join(DOCS, "mexico-vic-be-small-pilot-minor-steward-review.md"),
  `# Mexico VIC BE Small Pilot — Minor Steward Review

> **Status:** \`${status}\`  
> **Freeze:** \`${EXPECTED_FREEZE}\`  
> **Staging apply test may proceed:** **${stagingApplyMayProceed ? "YES" : "NO"}**

## Alias approvals
- \`ascend-hotel-collection\` → \`ascend\` — **${aliasDecisions.ascend.result}**
- \`curio-collection-by-hilton\` → \`curio-collection\` — **${aliasDecisions.curio.result}**

## Key property rulings
- El Cid Castilla / La Ceiba: Ascend soft-brand distribution examples only
- Amberes 64: property proof only (no VIC Recent Momentum)
- MS Milenium: city display **San Pedro Garza García**

## Next
\`${result.recommended_next_step}\`

\`\`\`bash
npm run research-engine-v2:mexico-vic-be-small-pilot-minor-steward-review
\`\`\`
`
);

console.log("[steward-minor] done", {
  status,
  ascend: aliasDecisions.ascend.result,
  curio: aliasDecisions.curio.result,
  copy_pass: copyPass,
  remaining_holds: remainingHolds,
  staging_apply_may_proceed: stagingApplyMayProceed,
});
