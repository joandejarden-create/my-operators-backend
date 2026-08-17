/**
 * Autopilot V1 mode registry — maps product modes to existing RE2 surfaces.
 */

export const MODE_REGISTRY = Object.freeze({
  discovery: {
    mode: "discovery",
    aliases: ["DISCOVERY"],
    description: "Independently build hotel universe from approved structured / official sources.",
    reuses: [
      "clean-census/independent-discovery.js",
      "clean-census/*-mexico-discovery.js",
      "census-autopilot-*-cala-discovery-adapter.js",
      "census-autopilot-family-directory-adapters.js",
      "census-autopilot-preferred-directory-discovery-adapter.js",
    ],
    writes_airtable: false,
    calls_webhound: false,
  },
  reconstruction: {
    mode: "reconstruction",
    aliases: ["RECONSTRUCTION", "reconstruct"],
    description: "Create independently researched record with no legacy seed (clean-room).",
    reuses: [
      "clean-census/wave-engine.js",
      "clean-census/research-firewall.js",
      "clean-census/independent-record.js",
      "clean-census/verified-record.js",
      "clean-census/property-identity.js",
      "clean-census/temporal-affiliation.js",
    ],
    writes_airtable: false,
    calls_webhound: false,
  },
  full_record: {
    mode: "full_record",
    aliases: ["FULL_RECORD", "full-record"],
    description: "Attempt every researchable Census field via field routing registry.",
    reuses: [
      "production-census-field-contract-v111.js",
      "clean-census/field-research.js",
      "census-autopilot-key-field-completion.js",
      "census-autopilot-commercial-fields-and-description-v1.js",
    ],
    writes_airtable: false,
    calls_webhound: false,
  },
  freshness: {
    mode: "freshness",
    aliases: ["FRESHNESS"],
    description: "Re-check known independent records for affiliation/status/temporal drift.",
    reuses: [
      "clean-census/temporal-affiliation.js",
      "census-autopilot-coverage-reconciliation.js",
      "shadow monitoring / activation mode surfaces",
    ],
    writes_airtable: false,
    calls_webhound: false,
  },
  reconciliation: {
    mode: "reconciliation",
    aliases: ["RECONCILIATION"],
    description: "Compare independent evidence vs Verified Census / Brand Explorer / Operator Explorer staging.",
    reuses: [
      "clean-census/legacy-reconcile.js",
      "census-autopilot-coverage-reconciliation.js",
      "census-autopilot-brand-census-matcher.js",
      "cross-family-identity.js",
    ],
    writes_airtable: false,
    calls_webhound: false,
  },
  activation: {
    mode: "activation",
    aliases: ["ACTIVATION"],
    description:
      "Prepare Brand Explorer completion / activation research packs for Under Review / incomplete brands. Does NOT activate.",
    reuses: [
      "Brand Explorer Tab Factory gates",
      "PVQL / protected baseline",
      "verified-independent-census brand aggregation",
    ],
    writes_airtable: false,
    calls_webhound: false,
    activates_brands: false,
  },
  image_integrity: {
    mode: "image_integrity",
    aliases: ["IMAGE_INTEGRITY", "image-integrity"],
    description: "Validate imagery metadata / rights / entity match. No download/rehost/replace.",
    reuses: ["image uniqueness / source-rights patterns", "family directory image signals"],
    writes_airtable: false,
    auto_download: false,
    auto_replace: false,
  },
  escalation: {
    mode: "escalation",
    aliases: ["ESCALATION"],
    description:
      "Package unresolved material claims for Lane C (Webhound candidate / human / specialist / first-party). Does NOT call Webhound.",
    reuses: [
      "census-autopilot-queue-router.js routeWebhoundCandidates",
      "steward queue",
      "first-party validation packs",
    ],
    writes_airtable: false,
    calls_webhound: false,
  },
  unified_benchmark: {
    mode: "unified_benchmark",
    aliases: ["UNIFIED", "benchmark", "mexico-benchmark"],
    description:
      "Single operating job: discovery→identity→full-record→freshness signals→reconcile→activation candidates→images→escalation (dry).",
    reuses: ["all of the above"],
    writes_airtable: false,
    calls_webhound: false,
  },
});

/**
 * @param {string} raw
 */
export function resolveMode(raw) {
  const key = String(raw || "unified_benchmark")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  if (MODE_REGISTRY[key]) return MODE_REGISTRY[key];
  for (const m of Object.values(MODE_REGISTRY)) {
    if ((m.aliases || []).some((a) => String(a).toLowerCase().replace(/-/g, "_") === key)) {
      return m;
    }
  }
  return MODE_REGISTRY.unified_benchmark;
}

export function listModes() {
  return Object.values(MODE_REGISTRY);
}
