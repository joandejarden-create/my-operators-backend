/**
 * The only code path that may return READY_FOR_FOUNDER_REVIEW.
 * Subsystem passes are evidence. They do not grant readiness.
 */
import { CONTENT_CONTRACT, CANONICAL_TAB_IDS } from "./product-shell.js";
import { evaluateLessons } from "./lessons.js";
import { validateLiveTabs } from "./live-tabs.js";
import { classifyPropertyImage } from "./image-evidence.js";

const VISIBLE = new Set(CONTENT_CONTRACT.visibleDispositions);

function gate(name, pass, detail = []) {
  return { name, pass: Boolean(pass), detail };
}

export function evaluateMasterRelease(profile = {}) {
  const findings = evaluateLessons(profile);
  const items = profile.items || [];
  const byId = new Map(items.map((item) => [item.itemId, item]));
  const missingStatic = CONTENT_CONTRACT.staticRequired.filter((item) => !byId.has(item.itemId)).map((item) => item.itemId);
  const researchRequired = items.filter((item) => item.disposition === "RESEARCH_REQUIRED").map((item) => item.itemId);
  const blocked = items.filter((item) => item.disposition === "BLOCKED").map((item) => item.itemId);
  const unsourced = items
    .filter((item) => VISIBLE.has(item.disposition))
    .filter((item) => !Array.isArray(item.sourceIds) || item.sourceIds.length === 0)
    .map((item) => item.itemId);
  const imageProblems = (profile.properties || [])
    .map((property) => ({ name: property.name, ...classifyPropertyImage(property) }))
    .filter((row) => !row.acceptable);
  const live = validateLiveTabs(profile.liveValidation);
  const counts = profile.reconciliation || null;
  const reconciliationOk = !counts || (counts.planned === counts.written && counts.written === counts.rendered);

  const gates = [
    gate("identity", Boolean(profile.identity?.brandName && profile.identity?.slug), []),
    gate("foundation", profile.foundation?.buildBriefPresent === true, []),
    gate("item_completion", missingStatic.length === 0, missingStatic),
    gate("research_required", researchRequired.length === 0, researchRequired),
    gate("blocked", blocked.length === 0, blocked),
    gate("collection_cardinality", !findings.some((f) => f.validator === "collection_underpopulation"), []),
    gate("suppression_integrity", !findings.some((f) => f.validator === "invalid_suppression" || f.validator === "hidden_negative"), []),
    gate("source_support", unsourced.length === 0, unsourced),
    gate("fact_consistency", !findings.some((f) => f.validator === "fact_contradiction"), []),
    gate("metric_semantics", !findings.some((f) => ["blank_metric", "missing_metric_scope", "invalid_zero"].includes(f.validator)), []),
    gate("structure", (profile.tabIds || CANONICAL_TAB_IDS).join("|") === CANONICAL_TAB_IDS.join("|"), []),
    gate("canonical_labels", !findings.some((f) => f.validator === "wrong_geography_label" || f.validator === "noncanonical_flexibility_level"), []),
    gate("presentation", !findings.some((f) => f.validator === "generic_template" || f.validator === "empty_item"), []),
    gate("visual_geometry", !findings.some((f) => f.validator === "wrong_grid_cardinality" || f.validator === "partial_width_geometry"), []),
    gate("property_atomicity", !findings.some((f) => f.validator === "property_multi_identity"), []),
    gate("property_image_evidence", imageProblems.length === 0, imageProblems.map((row) => `${row.name}:${row.state}`)),
    gate("loyalty_tiers", !findings.some((f) => f.validator === "missing_loyalty_tier"), []),
    gate("loyalty_benefits", !findings.some((f) => f.detail?.includes?.("loyaltyBenefits") && f.validator === "collection_underpopulation"), []),
    gate("momentum", !findings.some((f) => f.validator === "momentum_missing_url"), []),
    gate("similar_brands", !((profile.collections?.similarBrands || []).length < 2), []),
    gate("geography", (profile.regions || []).length === 5 && !findings.some((f) => f.validator === "wrong_geography_label"), []),
    gate("planned_written_rendered_reconciliation", reconciliationOk, []),
    gate("live_application", live.pass, live.blockers),
  ];

  const lessonPass = findings.length === 0;
  const gatePass = gates.every((row) => row.pass);
  const subsystemBypass = profile.readyForFounderReview === true && profile.masterReleasePass !== true;
  const MASTER_RELEASE_CONTRACT_PASS = lessonPass && gatePass && !subsystemBypass;

  return {
    MASTER_RELEASE_CONTRACT_PASS,
    status: MASTER_RELEASE_CONTRACT_PASS ? "READY_FOR_FOUNDER_REVIEW" : "NOT_READY",
    readyForFounderReview: MASTER_RELEASE_CONTRACT_PASS,
    findings,
    gates,
    live,
    writes: 0,
  };
}
