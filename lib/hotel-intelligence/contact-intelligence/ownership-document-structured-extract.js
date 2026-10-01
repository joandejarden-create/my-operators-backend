/**
 * Ownership document-understanding extract (text-in → structured claims) v2.
 * Reuses Partner Intelligence OpenAI JSON + evidenceAppearsInSource.
 * Does NOT use contextDevExtract (URL crawl ≠ saved-document replay).
 *
 * Model output never authorizes enrichment or publication.
 */
import { evidenceAppearsInSource } from "../../partner-intelligence/merge-extraction-candidates.js";
import {
  classifyOwnershipLead,
  hotelCoreName,
  mentions,
  mentionsHotel,
  ownershipPassageSupport,
} from "./research-evidence.js";
import {
  ownershipCandidates,
  ownershipDocumentPassages,
  selectOwnershipDocumentSectionsForReader,
} from "./ownership-research-planning.js";

export const OWNERSHIP_DOC_UNDERSTANDING_VERSION = "ownership-doc-understanding-v2";

export const CLAIM_RELATIONSHIPS = Object.freeze([
  "ACQUIRED",
  "OWNS",
  "OPERATES",
  "BRANDS",
  "FAMILY_OWNS_UNNAMED",
  "PARENT_ACQUIRED",
  "OTHER",
]);

export const CLAIM_SCOPES = Object.freeze(["HOTEL_ASSET", "COMPANY", "GENERAL_PORTFOLIO"]);

export const CURRENTNESS = Object.freeze([
  "HISTORICAL",
  "PROPOSED",
  "CURRENT_AS_OF_STATED_DATE",
  "UNRESOLVED",
]);

export const TARGET_HOTEL_LINK_STATUS = Object.freeze(["EXPLICIT", "NONE", "AMBIGUOUS"]);

/**
 * Structured claim contract:
 * - what source says (subject/relationship/object + evidence)
 * - which entity/asset (scope)
 * - whether it links to target hotel (target_hotel_link)
 * - currentness separately from relationship type
 */
export const OWNERSHIP_DOCUMENT_CLAIM_SCHEMA = Object.freeze({
  type: "object",
  properties: {
    hotel_identity: {
      type: "object",
      properties: {
        name_as_stated: { type: "string" },
        aliases_in_document: { type: "array", items: { type: "string" } },
      },
      required: ["name_as_stated"],
      additionalProperties: false,
    },
    claims: {
      type: "array",
      items: {
        type: "object",
        properties: {
          subject: { type: "string", description: "Entity performing the relationship, or empty for unnamed family." },
          relationship: { type: "string", enum: [...CLAIM_RELATIONSHIPS] },
          object: { type: "string", description: "Entity or asset the relationship concerns as stated." },
          scope: { type: "string", enum: [...CLAIM_SCOPES] },
          event_date: { type: ["string", "null"] },
          source_date: { type: ["string", "null"], description: "Publication/document date if stated." },
          currentness: { type: "string", enum: [...CURRENTNESS] },
          evidence_span: {
            type: "string",
            description: "Exact contiguous substring copied from SOURCE TEXT (verbatim).",
          },
          target_hotel_link: {
            type: "object",
            properties: {
              status: { type: "string", enum: [...TARGET_HOTEL_LINK_STATUS] },
              supporting_passage: {
                type: ["string", "null"],
                description: "Verbatim span that links this claim to the target hotel, or null if none.",
              },
              establishes_relationship_to_target: { type: "boolean" },
            },
            required: ["status", "supporting_passage", "establishes_relationship_to_target"],
            additionalProperties: false,
          },
          contradictions_or_ambiguity: { type: "string" },
          next_evidence_needed: { type: "string" },
        },
        required: [
          "subject",
          "relationship",
          "object",
          "scope",
          "currentness",
          "evidence_span",
          "target_hotel_link",
        ],
        additionalProperties: false,
      },
    },
    document_notes: { type: "string" },
  },
  required: ["hotel_identity", "claims"],
  additionalProperties: false,
});

const SYSTEM_PROMPT = `You extract ownership-related claims from ONE supplied hotel document for Dealality.

SEPARATE four things for every claim:
1) What the source explicitly says (subject, relationship, object) — copy only stated facts.
2) Which entity or asset the statement concerns (scope).
3) Whether it establishes a relationship to the TARGET HOTEL (target_hotel_link).
4) Currentness: HISTORICAL | PROPOSED | CURRENT_AS_OF_STATED_DATE | UNRESOLVED.

SCOPE RULES:
- HOTEL_ASSET: statement is about a specific hotel/property (named or clearly the target).
- COMPANY: statement is about a corporate parent/group transaction (e.g. company A acquiring company B).
- GENERAL_PORTFOLIO: generic "owns and operates hotels/resorts" or brand portfolio wording with NO explicit link tying that ownership/operation to the target hotel.

CRITICAL:
- evidence_span MUST be an exact contiguous substring from SOURCE TEXT. Never paraphrase.
- target_hotel_link.supporting_passage MUST be exact substring or null. Matching quotation ≠ target-hotel ownership.
- GENERAL_PORTFOLIO may be retained, but establishes_relationship_to_target MUST be false unless a separate supporting_passage explicitly connects the party to the target hotel.
- Do NOT upgrade GENERAL_PORTFOLIO into OWNS or OPERATES of the target hotel without that explicit link.
- Do NOT invent an operator relationship for the target from portfolio boilerplate.
- Parent-company deals (company acquires company) use relationship PARENT_ACQUIRED + scope COMPANY — never HOTEL_ASSET alone.
- Historical hotel acquisition uses ACQUIRED + HOTEL_ASSET + currentness HISTORICAL or UNRESOLVED — never promote to present OWNS.
- Unnamed family ownership: relationship FAMILY_OWNS_UNNAMED, subject="", named parties none, do not invent a person/company.
- Prefer EXPLICIT target_hotel_link only when the hotel/core name (or clear core alias) appears in the linking passage with the claim.`;

function nz(v) {
  return String(v == null ? "" : v).trim();
}

function findSpanOffsets(sourceText, span) {
  const raw = String(sourceText || "");
  const s = nz(span);
  if (!s) return { found: false, start: -1, end: -1 };
  const start = raw.indexOf(s);
  if (start >= 0) return { found: true, start, end: start + s.length };
  const norm = (t) => t.replace(/\s+/g, " ");
  const nRaw = norm(raw);
  const nSpan = norm(s);
  const ni = nRaw.indexOf(nSpan);
  if (ni < 0) return { found: false, start: -1, end: -1 };
  let collapsed = 0;
  let origStart = 0;
  for (let i = 0; i < raw.length; i++) {
    if (/\s/.test(raw[i]) && i > 0 && /\s/.test(raw[i - 1])) continue;
    if (collapsed === ni) {
      origStart = i;
      break;
    }
    collapsed++;
  }
  return { found: true, start: origStart, end: Math.min(raw.length, origStart + s.length) };
}

function entityAppearsInSpan(span, entity) {
  const e = nz(entity);
  if (!e) return true;
  return mentions(span, e) || evidenceAppearsInSource(e, span);
}

/**
 * Structural admission: quotation grounding ≠ relationship correctness.
 * Returns { admitted, reasons[], flags }.
 */
export function admitOwnershipClaim(claim, sourceText, hotelName = "") {
  const reasons = [];
  const span = nz(claim.evidence_span);
  const link = claim.target_hotel_link || {};
  const linkPassage = link.supporting_passage == null ? null : nz(link.supporting_passage);
  const scope = nz(claim.scope);
  const relationship = nz(claim.relationship);
  const subject = nz(claim.subject);
  const object = nz(claim.object);
  const phraseFlags = classifyOwnershipLead(span, hotelName);

  const spanOk = evidenceAppearsInSource(span, sourceText) && findSpanOffsets(sourceText, span).found;
  if (!spanOk) reasons.push("EVIDENCE_SPAN_NOT_IN_DOCUMENT");

  if (linkPassage) {
    if (!evidenceAppearsInSource(linkPassage, sourceText) || !findSpanOffsets(sourceText, linkPassage).found) {
      reasons.push("TARGET_LINK_PASSAGE_NOT_IN_DOCUMENT");
    }
  }

  if (relationship === "FAMILY_OWNS_UNNAMED" && subject) {
    reasons.push("NAMED_SUBJECT_ON_UNNAMED_FAMILY");
  }

  // Subject/object must appear in evidence (except empty subject on unnamed family)
  if (relationship !== "FAMILY_OWNS_UNNAMED" && subject && !entityAppearsInSpan(span, subject)) {
    reasons.push("SUBJECT_NOT_IN_EVIDENCE_SPAN");
  }
  if (object && !entityAppearsInSpan(span, object) && !mentionsHotel(span, hotelName)) {
    // object may be the hotel under a core alias — check core
    if (!mentions(span, hotelCoreName(hotelName)) && !mentions(span, object)) {
      reasons.push("OBJECT_NOT_IN_EVIDENCE_SPAN");
    }
  }

  const hotelInEvidence = mentionsHotel(span, hotelName) || (linkPassage && mentionsHotel(linkPassage, hotelName));
  const explicitLinkRequested = link.status === "EXPLICIT" || link.establishes_relationship_to_target === true;

  // Contextual two-span: relationship span + separate hotel identity span (both exact)
  const identitySpan = nz(link.hotel_identity_span);
  let contextualLinkOk = false;
  if (identitySpan && evidenceAppearsInSource(identitySpan, sourceText) && mentionsHotel(identitySpan, hotelName)) {
    contextualLinkOk = true;
  }

  if (scope === "GENERAL_PORTFOLIO") {
    if (link.establishes_relationship_to_target === true && !hotelInEvidence && !contextualLinkOk) {
      reasons.push("GENERAL_PORTFOLIO_CLAIMED_TARGET_LINK_WITHOUT_HOTEL_MENTION");
    }
    // Owning/operating the target hotel cannot be established from portfolio scope alone
    if (
      link.establishes_relationship_to_target === true &&
      (relationship === "OWNS" || relationship === "OPERATES") &&
      !(linkPassage && mentionsHotel(linkPassage, hotelName) && entityAppearsInSpan(linkPassage, subject)) &&
      !contextualLinkOk
    ) {
      reasons.push("GENERAL_PORTFOLIO_CANNOT_ESTABLISH_TARGET_OWN_OR_OPERATE");
    }
  }

  if (explicitLinkRequested && !hotelInEvidence && !contextualLinkOk) {
    reasons.push("TARGET_LINK_EXPLICIT_WITHOUT_HOTEL_IN_PASSAGE");
  }

  if (scope === "COMPANY" && (relationship === "ACQUIRED" || relationship === "PARENT_ACQUIRED") && link.establishes_relationship_to_target === true) {
    // Company acquisition of another company is not automatically hotel-asset ownership
    if (!mentionsHotel(span, hotelName) && !contextualLinkOk) {
      reasons.push("COMPANY_TXN_MARKED_AS_TARGET_HOTEL_RELATIONSHIP");
    }
  }

  // Decor metaphor: keep claim for review but flag — do not invent ownership
  if (phraseFlags.reasons.includes("DECOR_OR_MARKETING_METAPHOR") && (relationship === "OWNS" || relationship === "ACQUIRED")) {
    reasons.push("DECOR_METAPHOR_NOT_OWNERSHIP");
  }

  const hardReject = reasons.some((r) =>
    [
      "EVIDENCE_SPAN_NOT_IN_DOCUMENT",
      "TARGET_LINK_PASSAGE_NOT_IN_DOCUMENT",
      "NAMED_SUBJECT_ON_UNNAMED_FAMILY",
      "GENERAL_PORTFOLIO_CANNOT_ESTABLISH_TARGET_OWN_OR_OPERATE",
      "DECOR_METAPHOR_NOT_OWNERSHIP",
    ].includes(r)
  );

  return {
    admitted: !hardReject && spanOk,
    reasons,
    phrase_rule_flags: phraseFlags.reasons,
    phrase_rule_allow_owner_candidate: phraseFlags.allow_owner_candidate,
    hotel_in_evidence: hotelInEvidence || contextualLinkOk,
    textual_grounding_only: spanOk,
    contextual_hotel_link: contextualLinkOk,
    // Separate: even if admitted, target relationship may be false
    establishes_relationship_to_target: Boolean(
      (link.establishes_relationship_to_target || contextualLinkOk) &&
        (hotelInEvidence || contextualLinkOk) &&
        scope !== "GENERAL_PORTFOLIO" &&
        !reasons.includes("COMPANY_TXN_MARKED_AS_TARGET_HOTEL_RELATIONSHIP")
    ),
  };
}

/**
 * Validate model claims with separate grounding vs relationship admission.
 * Grounded but relationship-rejected claims are retained for review (not silently erased).
 */
export function validateOwnershipDocumentClaims(parsed, sourceText, hotelName = "") {
  const claims = Array.isArray(parsed?.claims) ? parsed.claims : [];
  const grounded = [];
  const retained_for_review = [];
  const rejected = [];

  for (const c of claims) {
    const span = nz(c.evidence_span);
    const offsets = findSpanOffsets(sourceText, span);
    const quoteOk = evidenceAppearsInSource(span, sourceText) && offsets.found;
    const admission = admitOwnershipClaim(c, sourceText, hotelName);
    const link = c.target_hotel_link || {};
    const linkPassage = link.supporting_passage == null ? null : nz(link.supporting_passage);
    const linkOffsets = linkPassage ? findSpanOffsets(sourceText, linkPassage) : { found: false, start: -1, end: -1 };

    const repairedCurrentness =
      nz(c.currentness) === "CURRENT_AS_OF_STATED_DATE" &&
      !(c.event_date || c.source_date)
        ? {
            currentness: "UNRESOLVED",
            currentness_repaired: "CURRENT_REQUIRES_EVIDENCED_DATE",
            historical_vs_current: "CURRENTNESS_UNRESOLVED_NO_STATED_DATE",
          }
        : {
            currentness: nz(c.currentness) || "UNRESOLVED",
            currentness_repaired: null,
            historical_vs_current: null,
          };

    const enriched = {
      subject: nz(c.subject),
      relationship: nz(c.relationship),
      object: nz(c.object),
      scope: nz(c.scope),
      event_date: c.event_date ?? null,
      source_date: c.source_date ?? null,
      currentness: repairedCurrentness.currentness,
      currentness_repaired: repairedCurrentness.currentness_repaired,
      historical_vs_current: repairedCurrentness.historical_vs_current,
      evidence_span: span,
      evidence_start: offsets.start,
      evidence_end: offsets.end,
      evidence_grounded: quoteOk,
      target_hotel_link: {
        status: nz(link.status) || "NONE",
        supporting_passage: linkPassage,
        supporting_passage_start: linkOffsets.start,
        supporting_passage_end: linkOffsets.end,
        establishes_relationship_to_target: Boolean(link.establishes_relationship_to_target),
        // Code-computed — do not trust model flag alone
        establishes_relationship_to_target_validated: admission.establishes_relationship_to_target,
      },
      contradictions_or_ambiguity: nz(c.contradictions_or_ambiguity),
      next_evidence_needed: nz(c.next_evidence_needed),
      admission_reasons: admission.reasons,
      phrase_rule_flags: admission.phrase_rule_flags,
      phrase_rule_allow_owner_candidate: admission.phrase_rule_allow_owner_candidate,
      // Regex support is advisory only — disagreements stay visible
      deterministic_transaction_support:
        nz(c.subject) && hotelName
          ? ownershipPassageSupport(span, hotelName, c.subject)
          : false,
      promotes_to_current_ownership: false,
      enrichment_authorized: false,
      publication_authorized: false,
    };

    if (!quoteOk) {
      rejected.push({ reason: "EVIDENCE_SPAN_NOT_IN_DOCUMENT", claim: enriched });
      continue;
    }

    if (!admission.admitted) {
      // Quotation may be grounded but relationship/admission failed — keep for review
      retained_for_review.push({
        reason: admission.reasons.join("|") || "ADMISSION_FAILED",
        claim: enriched,
      });
      continue;
    }

    grounded.push(enriched);
  }

  return {
    hotel_identity: parsed?.hotel_identity || { name_as_stated: hotelName },
    grounded_claims: grounded,
    retained_for_review,
    rejected_claims: rejected,
    document_notes: nz(parsed?.document_notes),
  };
}

/**
 * Follow-up research questions from unresolved but valid claims (not executed).
 */
export function proposeFollowUpQueries(claims = [], hotel = {}) {
  const hotelName = hotel.hotel_name || "";
  const core = hotelCoreName(hotelName);
  const out = [];
  for (const c of claims) {
    // Deterministic OWNERSHIP_STATEMENT_EVIDENCED → currency corroboration (not enrichment)
    if (
      (c.classification === "OWNERSHIP_STATEMENT_EVIDENCED" ||
        c.classification === "NAMED_OWNER_STATEMENT_LEAD") &&
      (c.name || c.subject) &&
      c.implies_current_ownership !== true
    ) {
      const who = c.name || c.subject;
      out.push({
        kind: "OWNERSHIP_STATEMENT_CURRENCY",
        question: `Is ${who} still the owner/sponsor as of a stated recent date, or has ownership transferred?`,
        proposed_query: `"${core}" (owner OR "owned by" OR proprietor OR sold OR sale) "${who}"`.trim(),
        based_on: {
          subject: who,
          classification: c.classification,
          currentness: c.currentness || "UNRESOLVED",
          staging_only: true,
          implies_current_ownership: false,
          requires_currency_research: true,
        },
      });
      continue;
    }
    if (!c.evidence_grounded) continue;
    const est = c.target_hotel_link?.establishes_relationship_to_target_validated;
    const subject = c.subject || "";
    if (c.relationship === "ACQUIRED" && est && (c.currentness === "HISTORICAL" || c.currentness === "UNRESOLVED")) {
      out.push({
        kind: "SUBSEQUENT_SALE",
        question: `Has the historical acquirer subsequently sold this hotel?`,
        proposed_query: `"${core}" (sold OR sale OR acquired by OR divested) after:${c.event_date || ""} ${subject}`.trim(),
        based_on: { subject, relationship: c.relationship, event_date: c.event_date },
      });
    }
    if (
      (c.relationship === "OWNS" || c.relationship === "OWNED_BY") &&
      subject &&
      (c.currentness === "HISTORICAL" || c.currentness === "UNRESOLVED" || c.party_role === "HISTORICAL_OWNER")
    ) {
      out.push({
        kind: "HISTORICAL_OWNER_CURRENCY",
        question: `Is ${subject} still the owner as of a stated recent date, or has ownership transferred?`,
        proposed_query: `"${core}" (owner OR "owned by" OR sold OR sale) "${subject}"`.trim(),
        based_on: {
          subject,
          relationship: c.relationship,
          currentness: c.currentness,
          source_date: c.source_date,
          staging_only: true,
        },
      });
    }
    if (c.scope === "GENERAL_PORTFOLIO" || (c.relationship === "OPERATES" && !est) || (c.relationship === "BRANDS" && !est)) {
      out.push({
        kind: "ASSET_OWNER_BEHIND_OPERATOR",
        question: `Who is the named asset owner behind this operator/brand?`,
        proposed_query: `"${core}" (owner OR owned by OR acquisition OR investor OR proprietor OR proprietário OR propietario) ${subject}`.trim(),
        based_on: { subject, relationship: c.relationship, scope: c.scope },
      });
    }
    if (c.relationship === "FAMILY_OWNS_UNNAMED") {
      out.push({
        kind: "NAMED_FAMILY_OR_COMPANY",
        question: `Which named family/company owns this property?`,
        proposed_query: `"${core}" (owned by OR proprietor OR family OR investor OR company)`,
        based_on: { relationship: c.relationship },
      });
    }
    if (c.relationship === "PARENT_ACQUIRED" && c.scope === "COMPANY") {
      out.push({
        kind: "ASSET_AFTER_PARENT_DEAL",
        question: `After the parent-company transaction, did this hotel remain with the same owner group or was it sold separately?`,
        proposed_query: `"${core}" (sold OR ownership OR owner) ${subject}`,
        based_on: { subject, object: c.object, relationship: c.relationship },
      });
    }
  }
  // Dedupe by proposed_query
  const seen = new Set();
  return out.filter((q) => {
    if (seen.has(q.proposed_query)) return false;
    seen.add(q.proposed_query);
    return true;
  });
}

export function deterministicOwnershipDocumentArm(sourceText, hotel = {}) {
  const hotelName = hotel.hotel_name || "";
  const document = ownershipDocumentPassages(sourceText, hotel);
  const candidates = ownershipCandidates(document, hotel);
  const leads = (document.passages || []).map((p) => ({
    start: p.start,
    end: p.end,
    excerpt: p.excerpt,
    ...classifyOwnershipLead(p.excerpt, hotelName),
  }));
  return {
    arm: "A_DETERMINISTIC",
    hotel_core: hotelCoreName(hotelName),
    document_sha256: document.sha256,
    characters: document.characters,
    passages: document.passages,
    leads,
    candidates: candidates.map((c) => ({
      ...c,
      supported_transaction_claim: c.supported_transaction_claim ?? c.supported_ownership ?? false,
      implies_current_ownership: false,
    })),
    enrichment_authorized: false,
    publication_authorized: false,
  };
}

export const OWNERSHIP_DOC_LOCKED_MODEL = "gpt-4o-mini";

export const GPT4O_MINI_PRICE_USD_PER_M = Object.freeze({
  input: 0.15,
  output: 0.6,
  source: "https://developers.openai.com/api/docs/models/gpt-4o-mini",
});

export function resolveOwnershipDocModel({ allowOverride = false } = {}) {
  const configured =
    nz(process.env.OWNERSHIP_DOC_LLM_MODEL) ||
    nz(process.env.PARTNER_INTELLIGENCE_LLM_MODEL) ||
    OWNERSHIP_DOC_LOCKED_MODEL;
  if (configured !== OWNERSHIP_DOC_LOCKED_MODEL && !allowOverride) {
    return {
      ok: false,
      model: configured,
      error: `model_locked_to_${OWNERSHIP_DOC_LOCKED_MODEL}_got_${configured}`,
    };
  }
  return { ok: true, model: configured, error: null };
}

export function requireFullSourceText(text, maxChars = 24000) {
  const t = String(text ?? "");
  if (t.length > maxChars) {
    throw new Error(`source_exceeds_allowance_${t.length}_gt_${maxChars}_truncation_forbidden`);
  }
  return t;
}

export function estimateTokensFromChars(chars) {
  return Math.ceil(Number(chars || 0) / 3);
}

export function costUsdFromUsage(usage = {}, prices = GPT4O_MINI_PRICE_USD_PER_M) {
  const prompt = Number(usage.prompt_tokens || 0);
  const completion = Number(usage.completion_tokens || 0);
  return Number(((prompt / 1e6) * prices.input + (completion / 1e6) * prices.output).toFixed(6));
}

export function computeConservativeMaxCostUsd({
  documents = [],
  systemPromptChars = 0,
  userPromptOverheadChars = 2200,
  maxCompletionTokens = 2500,
  hardCapUsd = 0.25,
} = {}) {
  const prices = GPT4O_MINI_PRICE_USD_PER_M;
  const perDoc = documents.map((d) => {
    const sourceChars = Number(d.characters ?? String(d.text || "").length);
    const inputChars = Math.min(sourceChars, 24000) + systemPromptChars + userPromptOverheadChars;
    const promptTokens = estimateTokensFromChars(inputChars);
    const maxUsd = (promptTokens / 1e6) * prices.input + (maxCompletionTokens / 1e6) * prices.output;
    return {
      doc_id: d.id || null,
      source_chars: sourceChars,
      prompt_tokens_max: promptTokens,
      completion_tokens_max: maxCompletionTokens,
      max_usd: Number(maxUsd.toFixed(6)),
    };
  });
  const totalMaxUsd = Number(perDoc.reduce((s, r) => s + r.max_usd, 0).toFixed(6));
  return {
    model: OWNERSHIP_DOC_LOCKED_MODEL,
    prices,
    hard_cap_usd: hardCapUsd,
    max_completion_tokens: maxCompletionTokens,
    retries: 0,
    documents: perDoc,
    total_max_usd: totalMaxUsd,
    within_hard_cap: totalMaxUsd <= hardCapUsd,
  };
}

async function callOpenAiJson(system, user, { maxTokens = 2500, model = OWNERSHIP_DOC_LOCKED_MODEL } = {}) {
  const started = Date.now();
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      temperature: 0.1,
      max_tokens: maxTokens,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    }),
  });
  const latencyMs = Date.now() - started;
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(json.error?.message || `OpenAI API error (${res.status})`);
    err.status = res.status;
    err.latency_ms = latencyMs;
    throw err;
  }
  const content = json.choices?.[0]?.message?.content;
  if (!content) {
    const err = new Error("OpenAI returned empty response.");
    err.latency_ms = latencyMs;
    throw err;
  }
  const usage = json.usage || {};
  return {
    parsed: JSON.parse(content),
    raw_content: content,
    model: json.model || model,
    model_requested: model,
    usage: {
      prompt_tokens: usage.prompt_tokens ?? null,
      completion_tokens: usage.completion_tokens ?? null,
      total_tokens: usage.total_tokens ?? null,
    },
    cost_usd: costUsdFromUsage(usage),
    latency_ms: latencyMs,
    http_status: res.status,
  };
}

export async function modelOwnershipDocumentArm({
  sourceText,
  hotel = {},
  applyModel = false,
  allowModelOverride = false,
  maxTokens = 2500,
} = {}) {
  const hotelName = hotel.hotel_name || "";
  const modelRes = resolveOwnershipDocModel({ allowOverride: allowModelOverride });
  const prepared = {
    arm: "B_STRUCTURED_MODEL",
    version: OWNERSHIP_DOC_UNDERSTANDING_VERSION,
    reuse_path:
      "lib/partner-intelligence/llm-extract-operator-facts.js pattern → OpenAI chat/completions JSON + evidenceAppearsInSource",
    model: modelRes.model,
    schema: OWNERSHIP_DOCUMENT_CLAIM_SCHEMA,
    context_dev_extract_used: false,
    enrichment_authorized: false,
    publication_authorized: false,
    retries: 0,
  };

  if (!applyModel) {
    return { ...prepared, status: "PREPARED_NOT_EXECUTED", paid_call: false };
  }
  if (!modelRes.ok) {
    return { ...prepared, status: "BLOCKED_MODEL_LOCK", paid_call: false, error: modelRes.error };
  }
  if (!nz(process.env.OPENAI_API_KEY)) {
    return { ...prepared, status: "BLOCKED_MISSING_OPENAI_API_KEY", paid_call: false };
  }

  let fullSource;
  let sectionSelection = null;
  try {
    // Prefer bounded section selection over hard-blocking oversized docs.
    sectionSelection = selectOwnershipDocumentSectionsForReader(sourceText, hotel, { maxChars: 22000 });
    if (sectionSelection.status === "EMPTY") {
      return { ...prepared, status: "BLOCKED_EMPTY_SOURCE", paid_call: false };
    }
    fullSource = sectionSelection.text_for_reader;
    // Keep legacy guard for accidental silent truncation paths.
    requireFullSourceText(fullSource, 24000);
  } catch (err) {
    return { ...prepared, status: "BLOCKED_TRUNCATION_FORBIDDEN", paid_call: false, error: err.message };
  }

  const unreadNote =
    sectionSelection?.unread_may_contain_evidence === true
      ? `\n\nDOCUMENT SECTION NOTE: Selected ${sectionSelection.sections_selected?.length || 0} section(s) from original ${sectionSelection.original_length} chars. Unread sections (original offsets): ${JSON.stringify(sectionSelection.sections_unread || [])}. Unread text may still contain ownership evidence — do not conclude absence of evidence from selected sections alone.`
      : "";

  const systemPrompt = SYSTEM_PROMPT;
  const userPrompt = `TARGET HOTEL: ${hotelName}
TARGET HOTEL CORE: ${hotelCoreName(hotelName)}
SOURCE URL (provenance only; do not fetch): ${hotel.source_url || "n/a"}
${unreadNote}

SOURCE TEXT:
${fullSource}

Return JSON:
{
  "hotel_identity": { "name_as_stated": "…", "aliases_in_document": [] },
  "claims": [
    {
      "subject": "…",
      "relationship": "ACQUIRED|OWNS|OPERATES|BRANDS|FAMILY_OWNS_UNNAMED|PARENT_ACQUIRED|OTHER",
      "object": "…",
      "scope": "HOTEL_ASSET|COMPANY|GENERAL_PORTFOLIO",
      "event_date": null,
      "source_date": null,
      "currentness": "HISTORICAL|PROPOSED|CURRENT_AS_OF_STATED_DATE|UNRESOLVED",
      "evidence_span": "verbatim",
      "target_hotel_link": {
        "status": "EXPLICIT|NONE|AMBIGUOUS",
        "supporting_passage": null,
        "establishes_relationship_to_target": false
      },
      "contradictions_or_ambiguity": "…",
      "next_evidence_needed": "…"
    }
  ],
  "document_notes": "…"
}`;

  try {
    const call = await callOpenAiJson(systemPrompt, userPrompt, { maxTokens, model: modelRes.model });
    const validated = validateOwnershipDocumentClaims(call.parsed, fullSource, hotelName);
    const follow_ups = proposeFollowUpQueries(
      [...validated.grounded_claims, ...validated.retained_for_review.map((r) => r.claim)],
      hotel
    );
    return {
      ...prepared,
      status: "EXECUTED",
      paid_call: true,
      model: call.model,
      model_requested: call.model_requested,
      usage: call.usage,
      cost_usd: call.cost_usd,
      latency_ms: call.latency_ms,
      http_status: call.http_status,
      section_selection: sectionSelection
        ? {
            status: sectionSelection.status,
            original_length: sectionSelection.original_length,
            sections_selected: sectionSelection.sections_selected,
            sections_unread: sectionSelection.sections_unread,
            unread_may_contain_evidence: sectionSelection.unread_may_contain_evidence,
            note: sectionSelection.note || null,
          }
        : null,
      exact_input: {
        system_prompt: systemPrompt,
        user_prompt: userPrompt,
        source_chars: fullSource.length,
        source_truncated: false,
        original_source_chars: sectionSelection?.original_length ?? fullSource.length,
      },
      raw_model_output: call.parsed,
      raw_model_content: call.raw_content,
      follow_up_queries_proposed: follow_ups,
      ...validated,
    };
  } catch (err) {
    return {
      ...prepared,
      status: "FAILED",
      paid_call: true,
      error: err.message || String(err),
      latency_ms: err.latency_ms ?? null,
      section_selection: sectionSelection
        ? {
            status: sectionSelection.status,
            original_length: sectionSelection.original_length,
            unread_may_contain_evidence: sectionSelection.unread_may_contain_evidence,
          }
        : null,
      exact_input: {
        system_prompt: systemPrompt,
        user_prompt: userPrompt,
        source_chars: fullSource.length,
        source_truncated: false,
      },
    };
  }
}

export function estimateModelArmCostUsd({ promptTokens = 8000, completionTokens = 2000, docs = 3 } = {}) {
  const perDoc = (promptTokens / 1e6) * GPT4O_MINI_PRICE_USD_PER_M.input + (completionTokens / 1e6) * GPT4O_MINI_PRICE_USD_PER_M.output;
  return {
    model: OWNERSHIP_DOC_LOCKED_MODEL,
    assumed_prompt_tokens_per_doc: promptTokens,
    assumed_completion_tokens_per_doc: completionTokens,
    docs,
    estimated_usd: Number((perDoc * docs).toFixed(4)),
    hard_cap_usd: 0.25,
    prices: GPT4O_MINI_PRICE_USD_PER_M,
  };
}

export { SYSTEM_PROMPT as OWNERSHIP_DOC_SYSTEM_PROMPT };

// Back-compat aliases used by earlier runners
export const CLAIM_TYPES = Object.freeze([
  "ACQUISITION",
  "OWNERSHIP",
  "OPERATION",
  "BRAND",
  "UNNAMED_FAMILY",
  "COMPANY_PARENT_TRANSACTION",
  "OTHER",
]);
export const RELATIONSHIP_LEVELS = Object.freeze(["ASSET", "COMPANY", "AMBIGUOUS", "UNKNOWN"]);
