/**
 * Versioned adapter: ownership handoff / workflow result → case coordinator shape.
 * Preserves evidence, conflicts, unresolved reasons, budget ledgers, resume state.
 * Does not fabricate supported edges from display names alone.
 */

import {
  mentionsHotel,
  currentOwnershipPassageSupport,
  contactValuePersonBound,
  analyzeContactAttribution,
  CONTACT_ROUTE,
} from "./research-evidence.js";
import { isUsageRightsBlocked } from "./research-operation-journal.js";

export const HANDOFF_RESULT_ADAPTER_VERSION = "handoff-result-adapter-v3";

const SUPPORTED_OWNER_CLASSES = new Set([
  "PROPERTY_OWNER",
  "ECONOMIC_OWNER_OR_SPONSOR",
  "ECONOMIC_OWNER",
  "OWNER",
]);

/** Extractor / hypothesis kinds never establish independent document provenance. */
const EXTRACTOR_SOURCE_KIND_RE =
  /context_extract(?!_attested)|extract_output|hypothesis|claims_bag|biography|serp_/i;

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function hasEvidenceRefs(refs) {
  return asArray(refs).some((r) => r && (r.source_url || r.excerpt || r.source_text || r.claim_id));
}

function normBlob(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
}

/**
 * Drop BLOCKED / restricted rows before evidence collection or staging.
 */
export function filterBlockedSources(items = []) {
  return asArray(items).filter((item) => item && !isUsageRightsBlocked(item));
}

/**
 * True when a source row is an independently captured document (scrape / document-read),
 * not extractor-labeled page_text / biography / claim bags. Field names alone do not
 * establish provenance.
 */
export function isCapturedDocumentSource(s = {}) {
  if (!s || isUsageRightsBlocked(s)) return false;
  if (s.provenance === "extractor_output" || s.extractor_output === true) return false;
  const kind = String(s.kind || "");
  if (EXTRACTOR_SOURCE_KIND_RE.test(kind)) return false;
  if (s.capture_attested === true) return true;
  if (s.document_id && (s.content_hash || s.retrieval_time || s.retrieved_at || s.observed_at)) {
    return true;
  }
  if (s.retrieval_provider || s.retrieved_via) return true;
  if (/scrape|inspect|document_read|retrieved_document|LIVE_RESEARCH/i.test(kind)) return true;
  if (s.markdown && (s.url || s.source_url)) return true;
  // Fixture / staged rows: URL + excerpt from non-extractor kinds (not page_text masquerade)
  if (
    (s.url || s.source_url) &&
    (s.excerpt || s.source_text || s.passage) &&
    !/extract/i.test(kind) &&
    !s.page_text &&
    !s.source_document_text
  ) {
    return true;
  }
  // Explicit retrieved_text with capture metadata
  if (s.retrieved_text && (s.content_hash || s.retrieval_time || s.retrieved_at || s.provider)) {
    return true;
  }
  return false;
}

/**
 * Collect permitted source document bodies from research (not channel-fabricated excerpts).
 * Only independently retrieved document text qualifies — BLOCKED sources are excluded.
 * Extractor page_text / source_document_text / retrieved_text labels are not provenance.
 */
export function collectPermittedSourceBodies(adaptedOrResearch = {}) {
  const research = adaptedOrResearch.research || adaptedOrResearch;
  const bodies = [];

  function retrievedTextFromSource(s) {
    if (!isCapturedDocumentSource(s)) return "";
    const direct =
      s.retrieved_text ||
      s.markdown ||
      (typeof s.data === "string" ? s.data : "") ||
      "";
    // page_text / source_document_text only when capture_attested (never bare extractor labels)
    const attestedPage =
      s.capture_attested === true
        ? s.page_text || s.source_document_text || ""
        : "";
    const fromDirect = String(direct || attestedPage || "").trim();
    if (fromDirect.length >= 20) return fromDirect;
    if (Array.isArray(s.passages) && s.passages.length) {
      const joined = s.passages
        .map((p) => p?.excerpt || p?.text || p?.passage || p?.content || "")
        .filter(Boolean)
        .join("\n");
      if (joined.trim().length >= 20) return joined;
    }
    const kind = String(s.kind || "");
    if (/search|serp_|organic|claims|hypothesis/i.test(kind)) return "";
    return String(s.excerpt || s.source_text || s.passage || "");
  }

  for (const s of asArray(research.sources)) {
    const text = retrievedTextFromSource(s);
    if (String(text).trim().length >= 20) {
      bodies.push({
        url: s.url || s.source_url || null,
        text: String(text),
        document_id: s.document_id || null,
        content_hash: s.content_hash || null,
        provider: s.provider || s.retrieval_provider || null,
        retrieval_time: s.retrieval_time || s.retrieved_at || s.observed_at || null,
        usage_rights: s.usage_rights || null,
      });
    }
  }
  for (const r of asArray(research.ownership?.evidence_refs)) {
    if (isUsageRightsBlocked(r)) continue;
    if (r.source_type === "generated_fallback" || r.extractor_output === true) continue;
    // Independently retrieved passage — require retrieved-document types or attested capture
    const retrievedType = /RETRIEVED|LIVE_RESEARCH|DOCUMENT|SCRAPE/i.test(
      String(r.source_type || r.source_class || "")
    );
    if (!retrievedType && !r.capture_attested && !r.document_id) {
      // Allow URL-bound excerpt only when source_text validates the excerpt (not invent)
      if (!r.source_text && !r.retrieved_text) continue;
    }
    const text =
      r.retrieved_text ||
      r.source_text ||
      (retrievedType || r.capture_attested ? r.excerpt || r.passage || "" : "") ||
      "";
    // Reject page_text-only extractor masquerade on evidence refs
    if (!text && (r.page_text || r.source_document_text) && !r.capture_attested) continue;
    if (String(text).trim().length >= 20) {
      // Supporting excerpt must appear in captured source_text when both present
      if (r.excerpt && r.source_text && !String(r.source_text).includes(String(r.excerpt).slice(0, 40))) {
        continue;
      }
      bodies.push({
        url: r.source_url || null,
        text: String(text),
        document_id: r.document_id || null,
        content_hash: r.content_hash || null,
        provider: r.provider || null,
        retrieval_time: r.retrieval_time || r.observed_at || null,
        usage_rights: r.usage_rights || null,
      });
    }
  }
  return bodies;
}

/**
 * Contact value is person-attributable only when bound to that person in a
 * permitted source body (same evidence unit after email/URL/phone protection).
 */
export function contactValueSupportedBySourceBody(value, personName, sourceBodies = []) {
  const v = String(value || "").trim().toLowerCase();
  if (!v || v.length < 3) return false;
  for (const body of sourceBodies) {
    if (isUsageRightsBlocked(body)) continue;
    const blob = normBlob(body.text);
    if (contactValuePersonBound(blob, v, personName)) return true;
  }
  return false;
}

/**
 * Hotel-specific current ownership evidence: subject → relation → object with
 * present/current temporal status. Operator, sold/former, and negation fail.
 * `hotel_specific: true` alone is never sufficient. Co-occurrence of an ownership
 * word elsewhere on the page is never sufficient.
 */
export function hasHotelSpecificOwnershipEvidence(ownership, hotelName, hotelId) {
  const refs = asArray(ownership?.evidence_refs);
  if (!refs.length) return false;
  const ownerName = ownership?.owner_display_name || ownership?.owner_name || null;
  if (!ownerName || !hotelName) return false;

  return refs.some((r) => {
    if (isUsageRightsBlocked(r)) return false;
    // Do not treat claim-only bags as passage support without excerpt/source_text
    const blob = `${r.excerpt || ""} ${r.source_text || ""}`.trim();
    if (!blob) return false;
    if (!mentionsHotel(blob, hotelName)) return false;
    return currentOwnershipPassageSupport(blob, hotelName, ownerName);
  });
}

export function isSupportedOwnerClassification(ownership) {
  const cls = String(
    ownership?.classification || ownership?.owner_role || ownership?.relationship_primary || ""
  ).toUpperCase();
  if (!cls || cls === "OWNER_CANDIDATE" || cls === "STAGED" || cls === "UNRESOLVED") return false;
  // Operator / brand / shareholder are not property-owner support classes
  if (/OPERATOR|BRAND|SHAREHOLDER|MANAGER|ASSET_MANAGER/.test(cls)) return false;
  return SUPPORTED_OWNER_CLASSES.has(cls);
}

/**
 * Temporal field adjudication. Absent temporal status is unresolved (null) —
 * callers must not infer currentness from the missing field alone.
 * Explicit HISTORICAL/FORMER/SOLD rejects; CURRENT accepts.
 */
export function isCurrentOwnership(ownership) {
  const t = String(ownership?.temporal_status || ownership?.historical_vs_current || "").toUpperCase();
  if (!t) return null;
  if (/HISTORICAL|FORMER|PRIOR|SOLD/.test(t) && !/CURRENT/.test(t)) return false;
  if (/CURRENT|PRESENT/.test(t)) return true;
  return null;
}

/**
 * Map handoff/workflow research object into durable case fields.
 */
export function adaptHandoffResearchResult(workflowResult = {}) {
  const research = workflowResult.research || workflowResult;
  const loop = research.iterative_ownership_loop || {};
  const budgets = research.budgets || {};
  const serp = budgets.serpapi || {};
  const contextDev = budgets.context_dev || {};
  const modelUsd = budgets.model_usd || loop.model_usd || null;

  const conflicts = [
    ...asArray(research.contradictions),
    ...asArray(research.conflicts),
    ...asArray(loop.conflicts),
  ];

  const unresolved = [
    ...asArray(research.unresolved_reasons),
    ...asArray(research.open_questions),
    ...asArray(research.unresolved_questions),
  ].filter(Boolean);

  const sources = filterBlockedSources(asArray(research.sources));
  const domainEvidence = filterBlockedSources(
    asArray(research.domain_evidence).length > 0
      ? asArray(research.domain_evidence)
      : sources.filter((s) => s && (s.domain_evidence || s.kind === "DOMAIN" || s.role === "owner_domain"))
  );

  const ownershipRaw = research.ownership || null;
  const ownership = ownershipRaw
    ? {
        ...ownershipRaw,
        evidence_refs: filterBlockedSources(asArray(ownershipRaw.evidence_refs)),
      }
    : null;
  const hotelName = research.hotel_name || null;
  const hotelId = research.hotel_id || null;

  // Passage must bind current ownership; absent temporal field is unresolved (null),
  // not an automatic current inference. Explicit HISTORICAL/FORMER rejects.
  const temporalOk = isCurrentOwnership(ownership) !== false;
  const ownershipSupported = Boolean(
    ownership?.owner_display_name &&
      isSupportedOwnerClassification(ownership) &&
      hasHotelSpecificOwnershipEvidence(ownership, hotelName, hotelId) &&
      temporalOk &&
      ownership.supported !== false
  );

  const iterative_resume_state =
    research.iterative_resume_state ||
    loop.research_state ||
    research.research_state ||
    null;

  // Billed Context.dev calls: prefer ledger entries; never treat orchestration
  // call-log rows (skips, deferred, resume markers) as provider dispatches.
  const ledgerEntries = asArray(contextDev.entries).filter((e) => Number(e?.cost) > 0);
  const orchestrationContextRows = asArray(research.calls).filter(
    (c) => c.provider === "context_dev" && !/skip|deferred|resume|blocked/i.test(String(c.kind || ""))
  );
  const budget_delta = {
    serpapi_calls: Number(serp.used ?? research.serpapi_calls ?? 0) || 0,
    serpapi_usd: Number(serp.usd ?? 0) || 0,
    context_dev_calls:
      Number(
        contextDev.used ??
          (ledgerEntries.length
            ? ledgerEntries.length
            : contextDev.calls ?? orchestrationContextRows.length)
      ) || 0,
    context_dev_spent_credits: Number(contextDev.spent_credits ?? contextDev.spent ?? 0) || 0,
    model_calls: Number(
      (typeof modelUsd === "object" && modelUsd?.calls) || research.model_calls || 0
    ) || 0,
    model_usd: Number((typeof modelUsd === "object" ? modelUsd?.usd_used ?? modelUsd?.usd : modelUsd) || 0) || 0,
    external_usd: Number(serp.usd || 0) + Number((typeof modelUsd === "object" ? modelUsd?.usd_used ?? modelUsd?.usd : modelUsd) || 0),
    enrichment_calls: Number(workflowResult.contact_enrichment?.attempted || 0) || 0,
  };

  return {
    adapter_version: HANDOFF_RESULT_ADAPTER_VERSION,
    ok: Boolean(research.ok),
    research,
    ownership,
    ownership_supported: ownershipSupported,
    people: asArray(research.people),
    sources,
    domain_evidence: domainEvidence,
    contradictions: conflicts,
    unresolved_reasons: [...new Set(unresolved.map(String))],
    pending_questions: unresolved.map((u) => (typeof u === "string" ? u : u?.question || u?.reason || String(u))),
    iterative_resume_state,
    budget_delta,
    qualifies_for_fullenrich: Boolean(research.qualifies_for_fullenrich),
    operator: research.operator || ownership?.operator || null,
    confirmed_company_domain_host: research.confirmed_company_domain_host || null,
    confirmed_company_domain: research.confirmed_company_domain || null,
  };
}

/**
 * Evidence-bearing contact status derivation.
 * Channel booleans alone do not establish attribution or relevance.
 */
export function deriveEvidenceContactStatuses(adapted, workflowResult = {}) {
  const statuses = {
    person_found: false,
    affiliation_evidenced: false,
    relevant_person: false,
    email_found: false,
    email_attributable: false,
    email_verified: false,
    phone_found: false,
    phone_attributable: false,
    phone_verified: false,
    per_person: [],
  };

  const sourceBodies = collectPermittedSourceBodies(adapted);
  // Workflow may pass additional permitted document bodies (e.g. scrape markdown)
  for (const extra of asArray(workflowResult.permitted_source_bodies)) {
    if (extra?.text) sourceBodies.push(extra);
  }

  const subjects = asArray(workflowResult.enrichment_subjects);
  const gatedOk = subjects.filter((s) => s?.gate?.ok);
  if (subjects.length) statuses.person_found = true;
  if (gatedOk.length) {
    statuses.affiliation_evidenced = true;
    statuses.relevant_person = true;
  }

  for (const p of adapted.people || []) {
    const personId = p.person_id || p.display_name;
    const gate = p.provenance?.affiliation_gate;
    const gateFailed = gate && gate.ok === false;
    const gatePassed =
      gate?.ok === true ||
      (gate == null &&
        p.provenance?.affiliation_independent === true &&
        p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE");
    // Failed/unresolved affiliation adjudication overrides stale CORROBORATED flags
    const hasAffEvidence =
      !gateFailed &&
      (gatePassed ||
        ((p.provenance?.independently_corroborated === true ||
          p.affiliation_status === "CORROBORATED") &&
          p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" &&
          hasEvidenceRefs(p.evidence)));
    const relevant =
      !gateFailed &&
      (gatedOk.some(
        (s) =>
          String(s.person?.display_name || s.full_name || "").toLowerCase() ===
          String(p.display_name || "").toLowerCase()
      ) ||
        (p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" && hasAffEvidence));

    if (p.display_name) statuses.person_found = true;
    if (hasAffEvidence) statuses.affiliation_evidenced = true;
    if (relevant) statuses.relevant_person = true;

    const chain = {
      person_id: personId,
      display_name: p.display_name,
      relevant_person: Boolean(relevant),
      affiliation_evidenced: Boolean(hasAffEvidence),
      affiliation_gate_ok: gate ? gate.ok === true : null,
      affiliation_rejection_reason: gateFailed ? gate.reason || null : null,
      email_found: false,
      email_attributable: false,
      email_verified: false,
      phone_found: false,
      phone_attributable: false,
      phone_verified: false,
      attributable_emails: [],
      attributable_phones: [],
      assistant_mediated_routes: [],
    };

    for (const ch of asArray(p.channels)) {
      const kind = String(ch.kind || "");
      const labelOk =
        ch.attribution === "PERSON_ATTRIBUTED" || ch.attribution === "NAMED_PERSON";
      // Labels alone are insufficient — value must appear in permitted source body
      const sourceOk = contactValueSupportedBySourceBody(ch.value, p.display_name, sourceBodies);
      let assistantRoute = null;
      for (const body of sourceBodies) {
        const analysis = analyzeContactAttribution(body.text, ch.value, p.display_name);
        if (
          analysis.route === CONTACT_ROUTE.ASSISTANT_MEDIATED &&
          analysis.person_is_reach_target &&
          !analysis.person_is_holder
        ) {
          assistantRoute = {
            holder: analysis.holder,
            reach_target: p.display_name,
            value: ch.value,
            kind,
            route: CONTACT_ROUTE.ASSISTANT_MEDIATED,
          };
          break;
        }
      }
      if (assistantRoute) {
        chain.assistant_mediated_routes.push(assistantRoute);
      }
      // Attribution is value-specific; label + source support + relevant person
      const attributed = Boolean(labelOk && sourceOk && relevant && ch.value);
      const verified =
        ch.verification_status === "VERIFIED" && attributed && relevant;

      if (/EMAIL/i.test(kind) && ch.value) {
        chain.email_found = true;
        statuses.email_found = true;
        if (attributed) {
          chain.email_attributable = true;
          chain.attributable_emails.push(String(ch.value).toLowerCase());
          statuses.email_attributable = true;
        }
        if (verified) {
          chain.email_verified = true;
          statuses.email_verified = true;
        }
      }
      if (/PHONE/i.test(kind) && ch.value) {
        chain.phone_found = true;
        statuses.phone_found = true;
        if (attributed) {
          chain.phone_attributable = true;
          chain.attributable_phones.push(String(ch.value));
          statuses.phone_attributable = true;
        }
        if (verified) {
          chain.phone_verified = true;
          statuses.phone_verified = true;
        }
      }
    }
    statuses.per_person.push(chain);
  }

  // Provider enrichment results: found ≠ attributable
  for (const row of asArray(workflowResult.contact_enrichment?.results)) {
    const r = row.result || row;
    const emails = r?.surfe?.normalized?.emails || r?.pdl?.emails || [];
    const phones = r?.surfe?.normalized?.phones || r?.pdl?.phones || [];
    if (emails.length) statuses.email_found = true;
    if (phones.length) statuses.phone_found = true;
  }

  return statuses;
}

/**
 * Build relationships only from supported evidence — never from name alone.
 */
export function buildSupportedRelationships(adapted) {
  const rels = [];
  const research = adapted.research || {};
  const hotel = research.hotel_name || research.hotel_id;
  const ownership = adapted.ownership;

  if (adapted.ownership_supported && ownership?.owner_display_name) {
    rels.push({
      subject: hotel,
      relationship: ownership.classification || "OWNED_BY",
      object: ownership.owner_display_name,
      current_or_historical: ownership.temporal_status || "CURRENT_OR_UNKNOWN",
      evidence_refs: ownership.evidence_refs || [],
      observation_date: ownership.observation_date || null,
      relationship_established_date: ownership.relationship_date || null,
      confidence: ownership.confidence || null,
      review_status: "STAGED",
      usage_rights: "INTERNAL_ONLY",
      hotel_specific: true,
      supported: true,
    });
  } else if (ownership?.owner_display_name) {
    rels.push({
      subject: hotel,
      relationship: ownership.classification || "OWNED_BY",
      object: ownership.owner_display_name,
      evidence_refs: ownership.evidence_refs || [],
      review_status: "HYPOTHESIS",
      usage_rights: "INTERNAL_ONLY",
      hotel_specific: true,
      supported: false,
      note: "Display name present without supporting evidence refs — not a supported edge",
    });
  }

  const operator = adapted.operator || research.operator;
  if (operator?.name && hasEvidenceRefs(operator.evidence_refs)) {
    rels.push({
      subject: hotel,
      relationship: "OPERATED_BY",
      object: operator.name,
      current_or_historical: operator.temporal_status || "CURRENT_OR_UNKNOWN",
      evidence_refs: operator.evidence_refs || [],
      review_status: "STAGED",
      usage_rights: "INTERNAL_ONLY",
      hotel_specific: true,
      supported: true,
    });
  }

  for (const p of adapted.people || []) {
    if (!p.display_name) continue;
    const gate = p.provenance?.affiliation_gate;
    const gateFailed = gate && gate.ok === false;
    const affiliationOk =
      !gateFailed &&
      (gate?.ok === true ||
        (p.provenance?.affiliation_independent === true &&
          p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE"));
    const hasRefs = hasEvidenceRefs(p.evidence);
    // Preserve rejected/historical evidence for review with truthful supported:false
    if (!hasRefs && !gate && !p.provenance?.affiliation_rejection_reason) continue;
    rels.push({
      subject: ownership?.owner_display_name || "owner",
      relationship: "PERSON_AFFILIATED_WITH",
      object: p.display_name,
      role: p.title || null,
      evidence_refs: p.evidence || [],
      review_status: affiliationOk ? "STAGED" : "REJECTED_OR_UNRESOLVED",
      usage_rights: "INTERNAL_ONLY",
      hotel_specific: false,
      supported: Boolean(affiliationOk && hasRefs),
      affiliation_gate: gate || null,
      note: gateFailed
        ? `Affiliation gate rejected: ${gate.reason || "UNRESOLVED"}`
        : affiliationOk
          ? null
          : "Person present without independently supported current affiliation",
    });
  }
  return rels;
}

/**
 * Objective-specific completion. Partial contact ≠ complete.
 */
export function deriveObjectiveCompletion({
  objective,
  adapted,
  contactStatuses,
  relationships,
}) {
  const needsContact =
    objective === "HOTEL_OWNERSHIP_CONTACT" || objective === "CONTACT_INTELLIGENCE";
  const openConflicts = (adapted.contradictions || []).filter(
    (c) => !c.status || c.status === "OPEN" || c.status === "UNRESOLVED"
  );
  const blockingUnresolved = (adapted.unresolved_reasons || []).filter((r) =>
    /NO_OWNERSHIP|IDENTITY|INSUFFICIENT|CONTRADICTION|OWNER_DOMAIN_UNRESOLVED/i.test(String(r))
  );

  const supportedOwnerEdge = relationships.some(
    (r) =>
      r.supported &&
      r.hotel_specific &&
      /OWNER|OWNED/i.test(String(r.relationship)) &&
      !/CANDIDATE/i.test(String(r.relationship))
  ) || adapted.ownership_supported;

  if (!adapted.ok) {
    return {
      final_status: "FAILED",
      stop_reason: adapted.research?.error || "RESEARCH_FAILED",
      complete: false,
    };
  }

  if (!supportedOwnerEdge && objective !== "CONTACT_INTELLIGENCE") {
    return {
      final_status: "STAGED_UNRESOLVED",
      stop_reason: "INSUFFICIENT_OWNERSHIP_EVIDENCE",
      complete: false,
    };
  }

  if (openConflicts.length || blockingUnresolved.length) {
    return {
      final_status: "STAGED_PARTIAL",
      stop_reason: openConflicts.length ? "OPEN_CONTRADICTIONS" : "UNRESOLVED_REASONS_REMAIN",
      complete: false,
    };
  }

  if (needsContact) {
    const anyPersonComplete = (contactStatuses.per_person || []).some(
      (p) =>
        p.relevant_person &&
        p.affiliation_evidenced &&
        (p.email_attributable || p.phone_attributable)
    );
    // Aggregate flags alone are insufficient — require same-person chain
    if (!anyPersonComplete) {
      return {
        final_status: "STAGED_PARTIAL",
        stop_reason: contactStatuses.relevant_person
          ? "NO_ATTRIBUTABLE_CONTACT"
          : "NO_QUALIFIED_PERSON_CONTACT_CHAIN",
        complete: false,
      };
    }
  }

  return {
    final_status: "STAGED_COMPLETE",
    stop_reason: "SUFFICIENT_STAGED_EVIDENCE",
    complete: true,
  };
}

export function deriveNextActionFromAdapted({
  stage,
  adapted,
  contactStatuses,
  objective,
  stopReason,
  completion,
}) {
  if (stopReason === "IDENTITY_UNRESOLVED") {
    return {
      action: "HUMAN_REVIEW",
      reason: "Physical hotel identity unresolved — ownership/contact work blocked",
      pending_question: "Confirm hotel name, city, country (and address if available)",
      pending_questions: ["Confirm hotel name, city, country (and address if available)"],
    };
  }
  if (stopReason === "BUDGET_EXHAUSTED") {
    return {
      action: "RESUME_WITH_BUDGET",
      reason: "Research budget exhausted",
      pending_question: "Increase context_dev_max / search budget or accept unresolved",
      pending_questions: ["Increase context_dev_max / search budget or accept unresolved"],
    };
  }
  if (!adapted?.ok) {
    return {
      action: "RETRY_OR_ESCALATE",
      reason: adapted?.research?.error || "Research failed",
      pending_question: "Inspect provider limitations and retry with permitted fallback",
      pending_questions: ["Inspect provider limitations and retry with permitted fallback"],
    };
  }

  const pending = [...(adapted.pending_questions || [])];
  const needsContact =
    objective === "HOTEL_OWNERSHIP_CONTACT" || objective === "CONTACT_INTELLIGENCE";

  if (!adapted.ownership_supported) {
    pending.unshift("Who is the current economic owner or sponsor of this exact hotel?");
    return {
      action: "CONTINUE_OWNERSHIP_RESEARCH",
      reason: "No supported owner/sponsor relationship yet",
      pending_question: pending[0],
      pending_questions: [...new Set(pending)],
    };
  }

  if (needsContact) {
    if (!contactStatuses.person_found || !contactStatuses.relevant_person) {
      pending.unshift("Who is a current decision-relevant person at the owner/sponsor?");
      return {
        action: "CONTINUE_PERSON_DISCOVERY",
        reason: "Owner evidenced; no qualified relevant person yet",
        pending_question: pending[0],
        pending_questions: [...new Set(pending)],
      };
    }
    if (!contactStatuses.affiliation_evidenced) {
      pending.unshift("Confirm current role and affiliation with first-party or filing evidence");
      return {
        action: "CORROBORATE_AFFILIATION",
        reason: "Person found but affiliation not independently evidenced",
        pending_question: pending[0],
        pending_questions: [...new Set(pending)],
      };
    }
    if (!contactStatuses.email_attributable && !contactStatuses.phone_attributable) {
      pending.unshift("Is there an independently attributable professional email or phone?");
      return {
        action: "DISCOVER_ATTRIBUTABLE_CONTACT",
        reason: "Qualified person without attributable contact route",
        pending_question: pending[0],
        pending_questions: [...new Set(pending)],
      };
    }
  }

  if (completion?.final_status === "STAGED_PARTIAL" && pending.length) {
    return {
      action: "RESOLVE_PENDING_QUESTIONS",
      reason: "Staged result has unresolved work",
      pending_question: pending[0],
      pending_questions: [...new Set(pending)],
    };
  }

  return {
    action: "ACCEPT_STAGED_RESULT",
    reason: "Evidence-backed staged chain complete for requested objective (publication still blocked)",
    pending_question: null,
    pending_questions: [],
  };
}
