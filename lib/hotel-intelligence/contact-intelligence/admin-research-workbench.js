/**
 * Admin ownership/contact research workbench — presentation over existing
 * runFullResearchWorkflow cases. Staging only; never promotes canonical data.
 */

import { CONTACT_ROUTE, analyzeContactAttribution } from "./research-evidence.js";
import {
  filterBlockedSources,
  isSupportedOwnerClassification,
} from "./handoff-result-adapter.js";

export const ADMIN_RESEARCH_WORKBENCH_VERSION = "admin-research-workbench-v1.1";

const REVIEW_DECISIONS = new Set([
  "NEEDS_MORE_RESEARCH",
  "ACCEPT_STAGED",
  "REJECT_STAGED",
  "ESCALATE",
  "HOLD",
]);

const OPERATOR_RELATIONSHIP_RE = /^(OPERATED_BY|OPERATOR)$/i;
const OWNER_SPONSOR_RELATIONSHIP_RE =
  /^(OWNED_BY|OWNS|SPONSORS|PROPERTY_OWNER|ECONOMIC_OWNER|ECONOMIC_OWNER_OR_SPONSOR|ECONOMIC_SPONSOR|OWNER)$/i;

/**
 * True only for supported owner/sponsor hotel relationships.
 * OPERATED_BY / operator edges never count as ownership support.
 */
export function isSupportedOwnerSponsorRelationship(rel = {}) {
  if (!rel || rel.supported !== true || rel.hotel_specific !== true) return false;
  const kind = String(rel.relationship || rel.classification || "").toUpperCase();
  if (!kind || OPERATOR_RELATIONSHIP_RE.test(kind) || /OPERATOR|BRAND|MANAGER/.test(kind)) {
    return false;
  }
  if (/PERSON_AFFILIATED|CANDIDATE/.test(kind)) return false;
  if (OWNER_SPONSOR_RELATIONSHIP_RE.test(kind)) return true;
  return isSupportedOwnerClassification({ classification: kind });
}

export function isOperatorRelationship(rel = {}) {
  if (!rel || rel.hotel_specific !== true) return false;
  const kind = String(rel.relationship || "").toUpperCase();
  return OPERATOR_RELATIONSHIP_RE.test(kind) || kind === "OPERATOR";
}

/**
 * Human-readable contact route line. Holder and reach-target stay distinct.
 * Example: "Contact Pat Lee, assistant to Alex Rivera."
 */
export function formatContactRouteDisplay({ holder, reach_target, route, person } = {}) {
  const h = holder || null;
  const t = reach_target || null;
  if (route === CONTACT_ROUTE.ASSISTANT_MEDIATED && h && t && h !== t) {
    return `Contact ${h}, assistant to ${t}.`;
  }
  if (route === CONTACT_ROUTE.ASSISTANT_MEDIATED && h) {
    return `Contact ${h}, assistant${t ? ` to ${t}` : ""}.`.replace(/,\s*$/, ".");
  }
  if (route === CONTACT_ROUTE.ORGANIZATIONAL && h) {
    return `Contact ${h} (organizational).`;
  }
  if (h) return `Contact ${h}.`;
  if (person && route === CONTACT_ROUTE.DIRECT) return `Contact ${person}.`;
  return null;
}

/**
 * Channel-level attribution from evidence for this value only.
 * Returns true / false / null (unknown — no evidence for this channel value).
 */
export function channelAttributionFromEvidence({
  value,
  personName,
  sources = [],
  assistantMediatedRoutes = [],
} = {}) {
  if (!value) {
    return {
      route: CONTACT_ROUTE.UNKNOWN,
      holder: null,
      reach_target: null,
      personally_attributable: null,
      evidence_found: false,
    };
  }

  const assist = (assistantMediatedRoutes || []).find((r) => r && r.value === value);
  if (assist) {
    return {
      route: CONTACT_ROUTE.ASSISTANT_MEDIATED,
      holder: assist.holder || null,
      reach_target: personName || assist.reach_target || null,
      personally_attributable: false,
      evidence_found: true,
    };
  }

  let best = null;
  for (const s of sources || []) {
    const text = s.excerpt || s.source_text || s.text || "";
    if (!text || !String(text).includes(String(value))) continue;
    const a = analyzeContactAttribution(text, value, personName);
    best = {
      route: a.route || CONTACT_ROUTE.UNKNOWN,
      holder: a.holder || null,
      reach_target: a.reach_target || null,
      personally_attributable:
        typeof a.personally_attributable === "boolean" ? a.personally_attributable : null,
      evidence_found: true,
    };
    if (a.holder || a.route !== CONTACT_ROUTE.UNKNOWN) break;
  }

  if (!best) {
    return {
      route: CONTACT_ROUTE.UNKNOWN,
      holder: null,
      reach_target: null,
      personally_attributable: null,
      evidence_found: false,
    };
  }
  return best;
}

/**
 * Build a workbench-facing projection from a research case / workflow result.
 * Does not invent supported edges or attributable contacts.
 */
export function buildAdminResearchWorkbenchView(caseOrResult = {}) {
  const c = caseOrResult.case || caseOrResult;
  const hotel =
    c.hotel_seed ||
    c.hotel ||
    {
      hotel_id: c.hotel_id || null,
      hotel_name: c.hotel_name || null,
    };
  const identity = c.physical_identity || c.identity || null;
  const ownership = c.staged_result?.ownership || c.ownership || c.research_snapshot?.ownership || null;
  const relationships = Array.isArray(c.relationships) ? c.relationships : [];
  const supportedOwner = relationships.find((r) => isSupportedOwnerSponsorRelationship(r)) || null;
  const operatorRels = relationships.filter((r) => isOperatorRelationship(r) && r.supported);
  const ownershipSupported = Boolean(
    supportedOwner ||
      (c.ownership_supported === true && isSupportedOwnerClassification(ownership))
  );
  const contactStatuses = c.contact_statuses || {};
  const people = Array.isArray(c.people) ? c.people : [];
  const sources = filterBlockedSources(c.sources || []);
  const contradictions = c.contradictions || [];
  const unresolved = c.unresolved_reasons || [];
  const budget = c.budget_usage || {};
  const humanReview = c.human_review || null;

  const hpcRecordLoaded = Boolean(
    identity?.hpc_record_loaded === true || c.hpc_record_loaded === true
  );
  const physicalSupported = Boolean(
    identity?.physical_identity_sufficiently_supported === true ||
      c.physical_identity_sufficiently_supported === true
  );
  const identityStatus =
    identity?.identity_status || c.identity_status || null;

  const contactRoutes = [];
  for (const p of people) {
    const per = (contactStatuses.per_person || []).find(
      (x) => String(x.display_name || "").toLowerCase() === String(p.display_name || "").toLowerCase()
    );
    for (const ch of p.channels || []) {
      const attr = channelAttributionFromEvidence({
        value: ch.value,
        personName: p.display_name,
        sources,
        assistantMediatedRoutes: per?.assistant_mediated_routes || [],
      });
      // Verified only from explicit verification_status — never raw unchecked flags
      const verified =
        String(ch.verification_status || "").toUpperCase() === "VERIFIED" &&
        attr.personally_attributable === true;

      contactRoutes.push({
        person: p.display_name || null,
        kind: ch.kind || null,
        value_present: Boolean(ch.value),
        value: ch.value || null,
        attribution_label: ch.attribution || null,
        route: attr.route,
        holder: attr.holder,
        reach_target: attr.reach_target,
        display_label: formatContactRouteDisplay({
          holder: attr.holder,
          reach_target: attr.reach_target,
          route: attr.route,
          person: p.display_name,
        }),
        // null = unknown (no channel-level evidence); never copy person-level email_attributable
        personally_attributable: attr.evidence_found ? attr.personally_attributable : null,
        found: Boolean(ch.value),
        verified,
      });
    }
    for (const ar of per?.assistant_mediated_routes || []) {
      if (!contactRoutes.some((r) => r.value === ar.value && r.route === CONTACT_ROUTE.ASSISTANT_MEDIATED)) {
        const route = CONTACT_ROUTE.ASSISTANT_MEDIATED;
        const holder = ar.holder || null;
        const reachTarget = p.display_name || ar.reach_target || null;
        contactRoutes.push({
          person: p.display_name || null,
          kind: ar.kind || "PERSON_EMAIL",
          value: ar.value || null,
          value_present: Boolean(ar.value),
          attribution_label: "ASSISTANT_MEDIATED",
          route,
          holder,
          reach_target: reachTarget,
          display_label: formatContactRouteDisplay({
            holder,
            reach_target: reachTarget,
            route,
            person: p.display_name,
          }),
          personally_attributable: false,
          found: Boolean(ar.value),
          verified: false,
        });
      }
    }
  }

  const finalStatus = c.final_status || c.status || null;
  const usefulPartial =
    /PARTIAL|UNRESOLVED|INTERRUPTED|STAGED_PARTIAL/i.test(String(finalStatus || "")) ||
    (Boolean(ownership?.owner_display_name) && !ownershipSupported);

  return {
    workbench_version: ADMIN_RESEARCH_WORKBENCH_VERSION,
    staging: true,
    canonical_writes: false,
    customer_publication: "BLOCKED",
    case_id: c.case_id || null,
    final_status: finalStatus,
    useful_partial: usefulPartial,
    hotel_identity: {
      hotel_id: hotel.hotel_id || c.hotel_id || null,
      hotel_name: hotel.hotel_name || identity?.hotel?.hotel_name || identity?.resolved_name || null,
      city: hotel.city || identity?.hotel?.city || identity?.city || null,
      country: hotel.country || identity?.hotel?.country || identity?.country || null,
      hpc_record_loaded: hpcRecordLoaded,
      physical_identity_sufficiently_supported: physicalSupported,
      identity_status: identityStatus,
      // Alias of physical support only — never conflated with record-loaded
      identity_supported: physicalSupported,
      physical_identity: identity,
    },
    ownership: {
      owner_display_name: ownershipSupported
        ? ownership?.owner_display_name || supportedOwner?.object || null
        : ownership?.owner_display_name || null,
      classification: ownership?.classification || supportedOwner?.relationship || null,
      temporal_status: ownership?.temporal_status || supportedOwner?.current_or_historical || null,
      supported: ownershipSupported,
      evidence_refs: ownership?.evidence_refs || supportedOwner?.evidence_refs || [],
      note: ownershipSupported
        ? null
        : ownership?.owner_display_name
          ? "Owner candidate present without supported owner/sponsor relationship"
          : operatorRels.length
            ? "Operator relationship present — not ownership support"
            : "No ownership finding",
    },
    operators: operatorRels.map((r) => ({
      name: r.object || null,
      relationship: r.relationship || "OPERATED_BY",
      supported: Boolean(r.supported),
      temporal_status: r.current_or_historical || null,
      evidence_refs: r.evidence_refs || [],
    })),
    people: people.map((p) => ({
      display_name: p.display_name || null,
      title: p.title || null,
      publication_label: p.publication_label || null,
      affiliation_status: p.affiliation_status || null,
      relevant:
        (contactStatuses.per_person || []).find(
          (x) => String(x.display_name || "").toLowerCase() === String(p.display_name || "").toLowerCase()
        )?.relevant_person || false,
    })),
    contact_routes: contactRoutes,
    contact_statuses: {
      person_found: Boolean(contactStatuses.person_found),
      affiliation_evidenced: Boolean(contactStatuses.affiliation_evidenced),
      relevant_person: Boolean(contactStatuses.relevant_person),
      email_found: Boolean(contactStatuses.email_found),
      email_attributable: Boolean(contactStatuses.email_attributable),
      phone_found: Boolean(contactStatuses.phone_found),
      phone_attributable: Boolean(contactStatuses.phone_attributable),
    },
    sources: sources.slice(0, 40).map((s) => ({
      url: s.url || s.source_url || null,
      excerpt: String(s.excerpt || s.source_text || "").slice(0, 500),
      kind: s.kind || s.source_class || s.source_type || null,
      usage_rights: s.usage_rights || null,
    })),
    contradictions,
    unresolved_reasons: unresolved,
    limitations: [
      "Staging only — not canonical ownership or contact promotion",
      "Customer publication BLOCKED",
      "Paid enrichment disabled on HTTP workbench",
      ...(c.limitations || []),
    ],
    budget_usage: {
      context_dev_calls: Number(budget.context_dev_calls || 0),
      context_dev_spent: Number(budget.context_dev_spent || 0),
      serpapi_calls: Number(budget.serpapi_calls || 0),
      serpapi_usd: Number(budget.serpapi_usd || 0),
      serpapi_reserved: Number(budget.serpapi_reserved || 0),
      open_reservations: Number(budget.open_reservations || 0),
      enrichment_calls: Number(budget.enrichment_calls || 0),
    },
    stop_reason: c.human_review_reason || c.stop_reason || unresolved[0] || finalStatus || null,
    recommended_next_action:
      c.next_action || deriveWorkbenchNextAction(c, usefulPartial, ownershipSupported),
    human_review: humanReview,
    resume: {
      can_resume:
        Boolean(c.case_id) &&
        /INTERRUPTED|PARTIAL|UNRESOLVED|IN_PROGRESS/i.test(String(finalStatus || "")),
      hotel_id: hotel.hotel_id || c.hotel_id || null,
      iterative_resume_state: c.iterative_resume_state || null,
      pending_unknown_keys: c.pending_unknown_keys || c.iterative_resume_state?.pending_unknown_keys || [],
    },
  };
}

function deriveWorkbenchNextAction(c, usefulPartial, ownershipSupported) {
  const status = String(c.final_status || "");
  if (status === "STAGED_COMPLETE") {
    return { action: "HUMAN_REVIEW", reason: "Complete staged chain ready for human review (no auto-promote)" };
  }
  if (status === "INTERRUPTED") {
    return { action: "RESUME", reason: "Interrupted — reopen case to resume permitted pending work" };
  }
  if (!ownershipSupported) {
    return { action: "CONTINUE_OWNERSHIP_RESEARCH", reason: "Ownership not supported — gather clearer ownership evidence" };
  }
  if (usefulPartial) {
    return { action: "HUMAN_REVIEW", reason: "Useful partial result — review unresolved gaps before next spend" };
  }
  return { action: "HUMAN_REVIEW", reason: c.human_review_reason || "Review staged findings" };
}

/**
 * Resolve reviewer from authenticated server context only.
 */
export function resolveTrustedReviewer(dealalityUser = null) {
  if (!dealalityUser || typeof dealalityUser !== "object") return "admin";
  const email = String(dealalityUser.email || "").trim();
  if (email) return email.slice(0, 120);
  const id = String(dealalityUser.memberstackId || dealalityUser.id || "").trim();
  if (id) return id.slice(0, 120);
  return "admin";
}

/**
 * Validate and sanitize a human review note/decision. Never promotes canonical data.
 * When trustedReviewer is provided, client reviewer/actor fields are ignored.
 */
export function buildHumanReviewUpdate(input = {}, prior = null, opts = {}) {
  const decision = String(input.decision || "").toUpperCase();
  if (decision && !REVIEW_DECISIONS.has(decision)) {
    return { ok: false, error: "invalid_review_decision", allowed: [...REVIEW_DECISIONS] };
  }
  const note = String(input.note || input.notes || "").trim().slice(0, 4000);
  const reviewer =
    opts.trustedReviewer != null
      ? String(opts.trustedReviewer).trim().slice(0, 120) || "admin"
      : String(input.reviewer || input.actor || "admin").trim().slice(0, 120);
  const entry = {
    at: new Date().toISOString(),
    reviewer,
    decision: decision || null,
    note: note || null,
    canonical_promotion: false,
    staging_only: true,
  };
  const history = Array.isArray(prior?.history) ? prior.history.slice() : [];
  history.push(entry);
  return {
    ok: true,
    human_review: {
      latest: entry,
      history: history.slice(-50),
      canonical_promotion: false,
    },
  };
}

export { REVIEW_DECISIONS, CONTACT_ROUTE };

