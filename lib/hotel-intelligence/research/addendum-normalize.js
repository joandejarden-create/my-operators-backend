/**
 * Packet 2.6C-R2 — normalize Webhound (or other) raw research into Dealality structures.
 * Provider prose is not dumped raw into the PDF — structured sections + findings.
 */

import crypto from "node:crypto";
import { assertKgpvPropertyIdentity } from "./archive-integrity.js";
import { KGPV_HOTEL_ID } from "./backfill-kgpv.js";
import {
  derivePublisherFromUrl,
  deriveTitleFromUrl,
  deriveSourceTypeFromUrl,
} from "../dossier/client-safe/source-normalize.js";

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function textFromUnknown(v) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  if (typeof v === "object") {
    return String(
      v.content_markdown ||
        v.text ||
        v.content ||
        v.markdown ||
        v.body ||
        v.summary ||
        ""
    ).trim();
  }
  return String(v);
}

function extractMarkdown(raw) {
  if (!raw) return "";
  // Prefer assembled working documents when present (richer than slim output export).
  const docs =
    raw.evidence_pack?.documents ||
    raw.evidence_pack?.evidence?.documents ||
    raw.output?.documents ||
    raw.output?.available_documents ||
    [];
  const parts = [];
  for (const d of asArray(docs)) {
    const t = textFromUnknown(
      d.content_markdown || d.working_doc_snapshot || d.content || d.markdown || d
    );
    if (t && t.length > 40) {
      const name = d.doc_name || d.name || "Section";
      // Avoid duplicating heading if body already starts with it.
      if (/^#{1,3}\s+/.test(t.trim())) parts.push(t.trim());
      else parts.push(`## ${name}\n\n${t}`);
    }
  }
  if (parts.length) {
    const assembled = parts.join("\n\n");
    if (assembled.length > 500) return assembled;
  }

  const candidates = [
    raw.output?.content_markdown,
    raw.output?.markdown,
    raw.output?.content,
    raw.output?.text,
    raw.evidence_pack?.output?.markdown,
    raw.evidence_pack?.output?.content_markdown,
    raw.evidence_pack?.final_output,
    typeof raw.output === "string" ? raw.output : null,
  ];
  for (const c of candidates) {
    const t = textFromUnknown(c);
    if (t && t.length > 40) return t;
  }
  return parts.join("\n\n") || "";
}

function extractSources(raw) {
  const lists = [
    raw.sources?.sources,
    raw.sources,
    raw.evidence_pack?.sources,
    raw.output?.sources,
    raw.watch?.sources,
  ];
  const out = [];
  const seen = new Set();
  for (const list of lists) {
    for (const s of asArray(list)) {
      if (typeof s === "string") {
        const url = s.trim();
        if (!url || seen.has(url)) continue;
        seen.add(url);
        out.push({
          source_id: `src_${out.length + 1}`,
          url,
          title: deriveTitleFromUrl(url),
          publisher: derivePublisherFromUrl(url),
          source_type: deriveSourceTypeFromUrl(url),
          date: null,
        });
        continue;
      }
      if (!s || typeof s !== "object") continue;
      const url = String(s.url || s.href || s.link || "").trim();
      let title = String(s.title || s.name || "").trim();
      if (!title || /^www\./i.test(title) || title === s.domain) {
        title = url ? deriveTitleFromUrl(url) : s.domain || "Source";
      }
      const key = url || title;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push({
        source_id: s.id || s.source_id || `src_${out.length + 1}`,
        url: url || null,
        title,
        publisher: s.publisher || derivePublisherFromUrl(url) || s.domain || null,
        source_type: s.source_type || deriveSourceTypeFromUrl(url) || null,
        date: s.date || s.published_at || null,
        snippet: s.snippet || s.excerpt || null,
      });
    }
  }
  return out;
}

function extractClaims(raw) {
  const lists = [raw.claims?.claims, raw.claims, raw.evidence_pack?.claims];
  const out = [];
  for (const list of lists) {
    for (const c of asArray(list)) {
      if (!c) continue;
      if (typeof c === "string") {
        out.push({ claim_id: `cl_${out.length + 1}`, text: c, status: "CANDIDATE" });
        continue;
      }
      out.push({
        claim_id: c.id || c.claim_id || `cl_${out.length + 1}`,
        text: c.text || c.claim || c.statement || "",
        evidence: c.evidence || null,
        status: "CANDIDATE",
        sources: c.sources || c.source_ids || [],
        date: c.date || null,
      });
    }
  }
  return out.filter((c) => c.text);
}

/** Full finding text — never mid-word hard-clip for customer headlines. */
function headlineFromClaimText(text, max = 280) {
  const t = String(text || "").replace(/\s+/g, " ").trim();
  if (t.length <= max) return t;
  const cut = t.slice(0, max);
  const sp = cut.lastIndexOf(" ");
  const base = (sp > 80 ? cut.slice(0, sp) : cut).replace(/[,:;.\s]+$/, "");
  return `${base}…`;
}

function splitSectionsFromMarkdown(md) {
  const text = String(md || "").trim();
  if (!text) return [];
  const parts = text.split(/\n(?=#{1,3}\s+)/);
  const sections = [];
  for (const part of parts) {
    const m = part.match(/^#{1,3}\s+(.+)\n?([\s\S]*)$/);
    if (m) {
      sections.push({ heading: m[1].trim(), body: m[2].trim() });
    } else if (part.trim()) {
      sections.push({ heading: "Investigation", body: part.trim() });
    }
  }
  // Also promote ### subsections under a collapsed Investigation blob.
  if (sections.length === 1 && /###\s+/.test(sections[0].body)) {
    const sub = sections[0].body.split(/\n(?=#{2,3}\s+)/);
    const expanded = [];
    for (const part of sub) {
      const m = part.match(/^#{2,3}\s+(.+)\n?([\s\S]*)$/);
      if (m) expanded.push({ heading: m[1].trim(), body: m[2].trim() });
    }
    if (expanded.length >= 2) return expanded;
  }
  return sections;
}

function findingsFromClaimsAndSections(claims, sections, sources) {
  const findings = [];
  for (const c of claims.slice(0, 40)) {
    const full = String(c.text || "").trim();
    const body =
      c.evidence && String(c.evidence).trim().length > 40
        ? `${full}\n\n${String(c.evidence).trim()}`
        : full;
    findings.push({
      finding_id: c.claim_id,
      finding_type: "RESEARCH_FINDING",
      headline: headlineFromClaimText(full, 280),
      body,
      status: "RESEARCH_FINDING",
      sources: c.sources || [],
      confidence: null,
      temporal_relevance: null,
      theme_frequency: null,
    });
  }
  // Prefer claim-derived findings when present, but cap customer key findings at 8 later.
  // If claims are sparse, fall back to section headlines.
  if (!findings.length) {
    for (const s of sections.slice(0, 12)) {
      if (s.body.length < 80) continue;
      if (/^sources?/i.test(s.heading)) continue;
      findings.push({
        finding_id: `fnd_${crypto.randomBytes(3).toString("hex")}`,
        finding_type: "RESEARCH_FINDING",
        headline: s.heading.slice(0, 160),
        body: s.body.slice(0, 4000),
        status: "RESEARCH_FINDING",
        sources: sources.slice(0, 3).map((x) => x.source_id),
      });
    }
  }
  // Deduplicate near-identical headlines; keep strongest bodies.
  const deduped = [];
  const seenH = new Set();
  for (const f of findings) {
    const key = String(f.headline || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .slice(0, 100);
    if (!key || seenH.has(key)) continue;
    seenH.add(key);
    deduped.push(f);
  }
  return deduped;
}

function openQuestionsFromMarkdown(md, raw) {
  const fromRaw = asArray(raw.open_questions || raw.output?.open_questions);
  if (fromRaw.length) {
    return fromRaw.map((q, i) =>
      typeof q === "string"
        ? { open_question_id: `oq_${i + 1}`, title: q, question: q }
        : {
            open_question_id: q.id || `oq_${i + 1}`,
            title: q.title || q.question || q.text,
            question: q.question || q.text || q.title,
            why_it_matters: q.why_it_matters || null,
            what_would_resolve: q.what_would_resolve || null,
          }
    );
  }
  const sections = splitSectionsFromMarkdown(md);
  const oqSec = sections.find((s) => /open\s*question/i.test(s.heading));
  const verifySec = sections.find((s) => /verify\s*next|what\s*to\s*verify|diligence/i.test(s.heading));
  const lines = [];
  const pushLines = (body, why) => {
    for (const l of String(body || "").split(/\n+/)) {
      const t = l.replace(/^[-*\d.)\s]+/, "").trim();
      if (t.length > 20 && !/^#{1,3}\s/.test(t)) lines.push({ question: t, why_it_matters: why || null });
    }
  };
  if (oqSec) pushLines(oqSec.body, "Unresolved item from this Change & Opportunity investigation.");
  if (verifySec) pushLines(verifySec.body, "Verification needed before commercial action.");
  // Fallback: scan full markdown when section splitter collapsed (single Investigation blob).
  if (!lines.length && md) {
    const verifyBlock = md.match(
      /(?:^|\n)#{1,3}\s+[^\n]*(?:verify|open\s*question|diligence)[^\n]*\n([\s\S]{80,4000}?)(?=\n#{1,3}\s+|$)/i
    );
    if (verifyBlock) pushLines(verifyBlock[1], "Verification needed before commercial action.");
    const bullets = md.match(/^[-*]\s+.{30,200}$/gm) || [];
    for (const b of bullets.slice(0, 8)) {
      const t = b.replace(/^[-*]\s+/, "").trim();
      if (/confirm|verify|whether|timeline|capex|agreement|status/i.test(t)) {
        lines.push({
          question: t,
          why_it_matters: "Material uncertainty for commercial pursuit.",
        });
      }
    }
  }
  const seen = new Set();
  return lines
    .filter((row) => {
      const key = row.question.slice(0, 80).toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .slice(0, 12)
    .map((row, i) => ({
      open_question_id: `oq_${i + 1}`,
      title: row.question,
      question: row.question,
      why_it_matters: row.why_it_matters,
      what_would_resolve: "Primary-source confirmation from owner, brand, or filing.",
    }));
}

function changeOpportunitySchemaFromSections(sections, templateId) {
  if (templateId !== "CHANGE_OPPORTUNITY") return undefined;
  const find = (re) => sections.find((s) => re.test(s.heading))?.body || null;
  return {
    executive_answer: {
      why_now: find(/why\s*now/i),
      what_could_derail: find(/derail|risk/i),
      what_looks_stable: find(/looks\s*stable|what\s*is\s*stable/i),
      what_needs_verification: find(/verify|verification/i),
    },
    opportunity_thesis: find(/opportunity\s*thesis|why\s*this\s*hotel/i),
    asset_product_risk: {
      assessment: "INSUFFICIENT_EVIDENCE",
      narrative: find(/asset|product\s*risk|physical|condition/i),
      themes: [],
      product_investment: find(/product\s*investment|capex|renovation/i),
      deferred_capex: null,
    },
    operating_quality: {
      assessment: "INSUFFICIENT_EVIDENCE",
      narrative: find(/operating\s*quality|operations|service/i),
      themes: [],
      management_response: null,
    },
    operator_stability: {
      assessment: "INSUFFICIENT_EVIDENCE",
      narrative: find(/operator|management\s*stability/i),
      signals: [],
    },
    deal_risk_flags: [],
    what_to_verify_next: find(/verify\s*next|what\s*to\s*verify/i),
  };
}

/**
 * @param {object} input
 * @param {object} input.raw
 * @param {object} input.template
 * @param {object} input.request
 * @param {object} [input.hotel_seed]
 */
export function normalizeResearchArtifact(input = {}) {
  const raw = input.raw || {};
  const template = input.template || {};
  const request = input.request || {};
  const hotelId = request.hotel_id || input.hotel_seed?.hotel_id;

  if (hotelId === KGPV_HOTEL_ID) {
    const idCheck = assertKgpvPropertyIdentity(
      input.hotel_seed?.hotel_name || request.hotel_name || "Krystal Grand Puerto Vallarta"
    );
    if (!idCheck.ok && input.strict_identity) {
      const err = new Error("property_identity_guard_failed");
      err.code = "property_identity_guard_failed";
      throw err;
    }
  }

  const markdown = extractMarkdown(raw);
  const sources = extractSources(raw);
  const claims = extractClaims(raw);
  const sections = splitSectionsFromMarkdown(markdown);
  const findings = findingsFromClaimsAndSections(claims, sections, sources);
  const open_questions = openQuestionsFromMarkdown(markdown, raw);
  const people = asArray(raw.people || raw.output?.people || raw.evidence_pack?.people);
  const organizations = asArray(
    raw.organizations || raw.output?.organizations || raw.evidence_pack?.organizations
  );
  const events = asArray(raw.events || raw.output?.events);
  const relationships = asArray(raw.relationships || raw.output?.relationships);

  const explicitNoEvidence =
    raw.explicit_no_evidence_validated === true ||
    (sources.length === 0 &&
      findings.length === 0 &&
      String(raw.completion_reason || "").includes("empty"));

  const normalized = {
    normalized_research_id: `norm_${crypto.randomBytes(6).toString("hex")}`,
    simulated: false,
    is_simulation: false,
    template_id: template.template_id,
    template_version: template.version,
    hotel_id: hotelId,
    request_id: request.request_id,
    run_id: request.run_id || null,
    provider: "WEBHOUND",
    provider_run_id: raw.session_id || raw.provider_run_id || request.provider_run_id || null,
    markdown,
    sections,
    sources,
    findings,
    claims: claims.map((c) => ({ ...c, claim_handoff: { auto_promote: false } })),
    organizations,
    people,
    relationships,
    events,
    open_questions,
    contradictions: asArray(raw.contradictions),
    superseded_findings: asArray(raw.superseded_findings),
    research_gaps: asArray(raw.research_gaps),
    source_count: sources.length,
    finding_count: findings.length,
    open_question_count: open_questions.length,
    explicit_no_evidence_validated: explicitNoEvidence,
    claim_handoff: { auto_promote: false },
    change_opportunity_schema: changeOpportunitySchemaFromSections(sections, template.template_id),
    addendum_structure: {
      cover: true,
      research_question: template.customer_question,
      executive_answer:
        sections.find((s) => /executive/i.test(s.heading))?.body ||
        markdown.slice(0, 1200) ||
        null,
      key_findings: findings.slice(0, 8).map((f) => f.headline),
      investigation: sections,
      open_questions,
      sources_evidence: sources,
    },
  };

  return normalized;
}
