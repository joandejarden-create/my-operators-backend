/**
 * Packet 2.8C-1 — Research quality gate for LangChain-native outputs.
 * Plausible model text without evidence = SEARCH LEAD, not claim.
 */

export const NATIVE_QUALITY_GATE_VERSION = "native-langchain-quality-gate-v1";

const CRITICAL_RELATIONSHIPS = new Set([
  "OWNED_BY",
  "CONTROLLED_BY",
  "OPERATED_BY",
  "BRANDED_BY",
  "SPONSORED_BY",
  "FORMER_BRAND",
  "ANNOUNCED_BRAND_PROJECT",
]);

/**
 * @param {{ claims?: object[], leads?: object[] }} extracted
 */
export function gateNativeResearchOutput(extracted = {}, opts = {}) {
  const accepted = [];
  const rejected = [];
  const leads = [];
  const contradictions = [];

  for (const claim of extracted.claims || []) {
    const issues = [];
    const sources = Array.isArray(claim.source_urls) ? claim.source_urls.filter(Boolean) : [];
    if (!sources.length) issues.push("missing_source_url");
    if (!claim.relationship_type && !claim.claim_text) issues.push("missing_claim_body");
    if (CRITICAL_RELATIONSHIPS.has(String(claim.relationship_type || "").toUpperCase()) && !sources.length) {
      issues.push("critical_without_evidence");
    }
    if (claim.from_model_memory_only === true) issues.push("unsourced_model_memory");
    if (!claim.temporal_status) issues.push("missing_temporal_status");

    // Negative screens
    const text = `${claim.claim_text || ""} ${claim.entities?.join(" ") || ""}`.toLowerCase();
    if (opts.hotel_name && /wrong property|adjacent resort as same/i.test(claim.claim_text || "")) {
      issues.push("wrong_property_risk");
    }
    if (/breathless/.test(text) && String(claim.temporal_status || "").toUpperCase() === "CURRENT") {
      if (!/announced|pending|project/i.test(claim.claim_text || "")) {
        issues.push("announced_as_current_risk");
        contradictions.push({
          type: "ANNOUNCED_CURRENT_CONFUSION",
          claim_id: claim.claim_id,
        });
      }
    }

    if (issues.length) {
      rejected.push({ ...claim, reject_reasons: issues, status: "REJECTED" });
      if (sources.length === 0 && claim.claim_text) {
        leads.push({
          lead_text: claim.claim_text,
          reason: "useful_unsourced_lead",
          status: "SEARCH_LEAD",
        });
      }
      continue;
    }

    accepted.push({
      ...claim,
      status: "ACCEPTED",
      confidence: claim.confidence || (sources.length >= 2 ? "HIGH" : "PROBABLE"),
    });
  }

  for (const lead of extracted.leads || []) {
    leads.push({ ...lead, status: "SEARCH_LEAD" });
  }

  return {
    version: NATIVE_QUALITY_GATE_VERSION,
    accepted_claims: accepted,
    rejected_claims: rejected,
    search_leads: leads,
    contradictions,
    false_confident_critical: contradictions.filter((c) => c.type === "ANNOUNCED_CURRENT_CONFUSION").length,
    ok: contradictions.length === 0,
  };
}
