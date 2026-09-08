/**
 * Packet 2.6C-R2 / R4 / R4.2 — build a Research Addendum dossier from normalized research.
 * Change & Opportunity uses ChangeOpportunityAnalysis → populated section objects.
 */

import crypto from "node:crypto";
import {
  createEmptyDossier,
  refreshDossierCounts,
} from "../dossier/schema.js";
import {
  DOSSIER_TYPE_RESEARCH_ADDENDUM,
  DOSSIER_TITLE_RESEARCH_ADDENDUM,
  DOSSIER_SECTION_TITLES,
} from "../dossier/statuses.js";
import { reportTypeLabel } from "./report-export-contract.js";
import { persistResearchAddendum } from "./addendum-store.js";
import { buildChangeOpportunityAnalysis } from "./change-opportunity-analysis.js";
import {
  normalizeResearchSources,
  derivePublisherFromUrl,
  deriveTitleFromUrl,
} from "../dossier/client-safe/source-normalize.js";
import {
  relocateLeadingCitations,
  dedupeInlineCitations,
} from "../dossier/client-safe/citation-placement.js";
import {
  normalizeUnicodeText,
  stripRawMarkdownArtifacts,
  rewriteResearchInstructionLeaks,
} from "../dossier/client-safe/text-normalize.js";
import { buildHotelIdentitySeed } from "./hotel-seed.js";
import {
  enrichReportForPublishing,
  resolveReportHotelIdentity,
} from "../dossier/report-hotel-identity.js";

const NONE_EVIDENCE =
  "No material recurring evidence identified in the reviewed sources for this diligence lane.";

function cleanText(input) {
  return rewriteResearchInstructionLeaks(
    stripRawMarkdownArtifacts(
      normalizeUnicodeText(relocateLeadingCitations(String(input || "")))
    )
  )
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/** Renderer-compatible paragraph blocks (never type:"para"). */
function blocksFromText(text) {
  const body = cleanText(text);
  if (!body) {
    return [{ type: "paragraphs", paragraphs: [NONE_EVIDENCE] }];
  }
  const paras = body
    .split(/\n{2,}/)
    .map((p) => dedupeInlineCitations(p.trim()))
    .filter(Boolean)
    .slice(0, 64);
  return paras.map((p) => ({ type: "paragraphs", paragraphs: [p] }));
}

function section(id, bodyText, titleOverride) {
  return {
    id,
    title: titleOverride || DOSSIER_SECTION_TITLES[id] || id,
    blocks: blocksFromText(bodyText),
  };
}

function pickFromSections(byHeading, ...res) {
  for (const re of res) {
    for (const [h, body] of byHeading) {
      if (re.test(h) && body && String(body).trim().length > 40) {
        return cleanText(body);
      }
    }
  }
  return null;
}

function publisherFromUrl(url) {
  return derivePublisherFromUrl(url) || null;
}

function sourcesBibliographyBlocks(sources) {
  const pack = normalizeResearchSources(sources || []);
  const rows = pack.normalized;
  if (!rows.length) {
    return [{ type: "paragraphs", paragraphs: ["No sources captured."] }];
  }
  return [
    {
      type: "table",
      table_kind: "sources",
      headers: ["#", "Title", "Publisher", "Type", "URL"],
      rows: rows.map((s) => [
        String(s.number),
        s.title || deriveTitleFromUrl(s.url) || `Untitled source ${s.number}`,
        s.publisher && s.publisher !== "—" ? s.publisher : publisherFromUrl(s.url) || "Publisher unavailable",
        s.source_type || "Other",
        s.url || "",
      ]),
    },
  ];
}

function openQuestionBlocks(openQuestions) {
  if (!openQuestions.length) {
    return [
      {
        type: "paragraphs",
        paragraphs: [
          "Priority open items from this investigation should be confirmed with primary sources before commercial action.",
        ],
      },
    ];
  }
  const blocks = [];
  openQuestions.forEach((q, i) => {
    blocks.push({
      type: "heading",
      level: 3,
      text: `${i + 1}. ${q.question || q.title}`,
    });
    blocks.push({
      type: "paragraphs",
      paragraphs: [
        `Why it matters: ${q.why_it_matters || "Material diligence uncertainty."}`,
        `Best next evidence: ${q.best_next_evidence || q.what_would_resolve || "Primary-source confirmation."}`,
      ],
    });
  });
  return blocks;
}

function keyFindingBlocks(keyFindings) {
  if (!keyFindings.length) {
    return [{ type: "paragraphs", paragraphs: ["No structured findings extracted."] }];
  }
  return keyFindings.map((f) => {
    const cites = (f.citations || []).map((n) => `[${n}]`).join("");
    const bodyParts = [
      f.why_it_matters ? `Why it matters: ${f.why_it_matters}` : "",
      cleanText(f.body || f.headline || ""),
      cites ? `Evidence: ${cites}` : "",
    ].filter(Boolean);
    return {
      type: "finding_callout",
      headline: cleanText(f.headline || "Finding"),
      body: bodyParts.join("\n\n"),
      status: f.status || "RESEARCH_FINDING",
    };
  });
}

function mapChangeOpportunitySections(template, normalized, analysis) {
  return [
    section("executive_answer", analysis.executiveAnswer),
    section("research_question", analysis.researchQuestion || template?.customer_question || ""),
    {
      id: "key_findings",
      title: DOSSIER_SECTION_TITLES.key_findings || "Key Findings",
      blocks: keyFindingBlocks(analysis.keyFindings),
    },
    section("why_now", analysis.whyNow),
    section("recent_change_triggers", analysis.recentChangeTriggers),
    section("asset_product_risk", analysis.assetProductRisk),
    section("product_investment_capex", analysis.productInvestmentCapex),
    section("operating_quality", analysis.operatingQuality),
    section("operator_management_stability", analysis.managementOperatorStability),
    section("what_could_derail", analysis.derailmentRisks),
    section("deal_risk_flags", analysis.dealRiskFlags),
    section("what_looks_stable", analysis.stableSignals),
    section("what_to_verify_next", analysis.verifyNext),
    {
      id: "open_questions",
      title: DOSSIER_SECTION_TITLES.open_questions || "10. Open Questions & Research Priorities",
      blocks: openQuestionBlocks(analysis.openQuestions),
    },
    {
      id: "sources_evidence",
      title: DOSSIER_SECTION_TITLES.sources_evidence || "11. Sources & Evidence",
      blocks: sourcesBibliographyBlocks(analysis.sources),
    },
  ];
}

function mapGenericTemplateSections(template, normalized) {
  const tid = template?.template_id;
  const byHeading = new Map(
    (normalized.sections || []).map((s) => [String(s.heading || "").toLowerCase(), s.body])
  );
  const pick = (...res) => pickFromSections(byHeading, ...res);
  const findingsBlock = (normalized.findings || [])
    .slice(0, 12)
    .map((f, i) => `${i + 1}. ${cleanText(f.headline)}${f.body ? `\n${cleanText(f.body)}` : ""}`)
    .join("\n\n");

  let sections;
  if (tid === "DECISION_AUTHORITY") {
    sections = [
      section("executive_answer", pick(/executive/) || cleanText((normalized.markdown || "").slice(0, 1500))),
      section("research_question", template?.customer_question || ""),
      section("key_findings", findingsBlock || NONE_EVIDENCE),
      section("decision_map", pick(/decision\s*map|authority/) || cleanText((normalized.markdown || "").slice(0, 2000))),
      section("people_decision_authority", pick(/people|key\s*people|profiles/) || NONE_EVIDENCE),
      section("authority_evidence", pick(/evidence|authority|signator/) || NONE_EVIDENCE),
      section("professional_profiles", pick(/linkedin|professional\s*profile/) || NONE_EVIDENCE),
      section("contact_paths", pick(/contact/) || NONE_EVIDENCE),
    ];
  } else if (tid === "BRAND_OPERATOR_AGREEMENT") {
    sections = [
      section("executive_answer", pick(/executive/) || cleanText((normalized.markdown || "").slice(0, 1500))),
      section("research_question", template?.customer_question || ""),
      section("key_findings", findingsBlock || NONE_EVIDENCE),
      section("current_structure", pick(/current\s*structure|structure/) || NONE_EVIDENCE),
      section("agreement_evidence", pick(/agreement|franchise|management\s*agreement/) || NONE_EVIDENCE),
      section("brand_history", pick(/brand/) || NONE_EVIDENCE),
      section("operator_history", pick(/operator/) || NONE_EVIDENCE),
      section("change_signals", pick(/change|conversion|reflag/) || NONE_EVIDENCE),
    ];
  } else if (tid === "OWNERSHIP_CAPITAL_EVENTS") {
    sections = [
      section("executive_answer", pick(/executive/) || cleanText((normalized.markdown || "").slice(0, 1500))),
      section("research_question", template?.customer_question || ""),
      section("key_findings", findingsBlock || NONE_EVIDENCE),
      section("current_ownership", pick(/ownership|owner/) || NONE_EVIDENCE),
      section("recent_ownership_events", pick(/ownership\s*event|transaction|sale/) || NONE_EVIDENCE),
      section("financing_capital_events", pick(/financ|capital|debt/) || NONE_EVIDENCE),
      section("transaction_signals", pick(/transaction|deal/) || NONE_EVIDENCE),
      section("entity_control_changes", pick(/entity|control|ubo/) || NONE_EVIDENCE),
    ];
  } else {
    sections = [
      section(
        "executive_answer",
        pick(/executive/) || cleanText((normalized.markdown || "").slice(0, 1500))
      ),
      section("research_question", template?.customer_question || ""),
      section("key_findings", findingsBlock || NONE_EVIDENCE),
      section(
        "investigation",
        cleanText(
          (normalized.sections || [])
            .map((s) => `## ${s.heading}\n\n${s.body}`)
            .join("\n\n")
            .slice(0, 20000) || normalized.markdown
        )
      ),
    ];
  }

  const oq = (normalized.open_questions || []).map((q, i) => ({
    open_question_id: q.open_question_id || `oq_${i + 1}`,
    question: cleanText(q.question || q.title),
    title: cleanText(q.title || q.question),
    why_it_matters: cleanText(q.why_it_matters || "Material diligence uncertainty."),
    best_next_evidence: cleanText(
      q.what_would_resolve || "Primary-source confirmation from owner, brand, or filing."
    ),
  }));

  sections.push(
    {
      id: "open_questions",
      title: DOSSIER_SECTION_TITLES.open_questions || "Open Questions",
      blocks: openQuestionBlocks(oq),
    },
    {
      id: "sources_evidence",
      title: DOSSIER_SECTION_TITLES.sources_evidence || "Sources & Evidence",
      blocks: sourcesBibliographyBlocks(normalized.sources || []),
    }
  );
  return sections;
}

function resolveHotelLocation(request, hotelSeed) {
  const seed =
    hotelSeed ||
    buildHotelIdentitySeed({
      hotel_id: request.hotel_id,
      hotel_name: request.hotel_name,
    });
  const identity = resolveReportHotelIdentity({
    hotel_id: seed.hotel_id || request.hotel_id,
    hotel_airtable_record_id: request.hotel_id,
    hotel_name: request.hotel_name || seed.hotel_name,
  });
  return {
    label: identity.display_location,
    city: identity.city || identity.locality || null,
    locality: identity.locality || null,
    state_region: identity.state_region || null,
    country: identity.country || null,
    address: seed.address || null,
  };
}

/**
 * Build + optionally persist Research Addendum.
 */
export function buildResearchAddendumDossier(input = {}) {
  const template = input.template || {};
  const request = input.request || {};
  const normalized = input.normalized || {};
  const hotelSeed =
    input.hotel_seed ||
    buildHotelIdentitySeed({
      hotel_id: request.hotel_id,
      hotel_name: request.hotel_name,
    });
  const labels = reportTypeLabel("RESEARCH_ADDENDUM", template.display_name);
  const dossierId =
    input.dossier_id ||
    `addendum_${String(template.template_id || "research").toLowerCase()}_${crypto
      .randomBytes(4)
      .toString("hex")}`;

  const completedAt = input.completed_at || request.completed_at || new Date().toISOString();
  const isChangeOpportunity = template.template_id === "CHANGE_OPPORTUNITY";

  let analysis = null;
  let sections;
  if (isChangeOpportunity) {
    analysis = buildChangeOpportunityAnalysis({
      normalized,
      template,
      completed_at: completedAt,
    });
    sections = mapChangeOpportunitySections(template, normalized, analysis);
  } else {
    sections = mapGenericTemplateSections(template, normalized);
  }

  const openQs = isChangeOpportunity
    ? analysis.openQuestions
    : (normalized.open_questions || []).map((q) => ({
        id: q.open_question_id || q.id,
        question: cleanText(q.question || q.title),
        title: cleanText(q.title || q.question),
        why_it_matters: cleanText(q.why_it_matters || null),
        what_would_resolve: cleanText(q.what_would_resolve || null),
        best_next_evidence: cleanText(q.best_next_evidence || q.what_would_resolve || null),
      }));

  const sources = isChangeOpportunity
    ? analysis.sources
    : normalizeResearchSources(normalized.sources || []).normalized;

  const keyFindings = isChangeOpportunity
    ? analysis.keyFindings.map((f, i) => ({
        id: f.finding_id || `finding_${i + 1}`,
        finding_id: f.finding_id || `finding_${i + 1}`,
        theme: f.theme || null,
        headline: f.headline,
        explanation: f.why_it_matters,
        why_it_matters: f.why_it_matters,
        summary: f.body,
        body: f.body,
        status: f.status || "RESEARCH_FINDING",
        citations: f.citations || [],
        lane: f.lane || null,
      }))
    : (normalized.findings || []).slice(0, 8).map((f, i) => ({
        id: `finding_${i + 1}`,
        headline: cleanText(f.headline),
        explanation: null,
        summary: cleanText(f.body || f.headline),
        body: cleanText(f.body || f.headline),
        status: f.status || "RESEARCH_FINDING",
        lane: f.lane || null,
      }));

  const execParas = isChangeOpportunity
    ? String(analysis.executiveAnswer)
        .split(/\n{2,}/)
        .map((p) => cleanText(p))
        .filter(Boolean)
        .slice(0, 8)
    : String(
        normalized.addendum_structure?.executive_answer ||
          keyFindings
            .slice(0, 4)
            .map((f) => f.headline)
            .join(" ") ||
          `${template.display_name} research completed.`
      )
        .split(/\n{2,}/)
        .map((p) => cleanText(p))
        .filter(Boolean)
        .slice(0, 8);

  const hotelLocation = resolveHotelLocation(request, hotelSeed);

  const dossier = createEmptyDossier({
    dossier_id: dossierId,
    hotel_id: request.hotel_id,
    hotel_airtable_record_id: request.hotel_id,
    hotel_name: request.hotel_name || hotelSeed.hotel_name,
    title: `${labels.report_type_label}: ${template.display_name}`,
    status: openQs.length > 0 ? "COMPLETED_WITH_OPEN_QUESTIONS" : "COMPLETED",
    research_provider: "EXTERNAL_RESEARCH",
    research_run_id: input.run_id || request.run_id || null,
    completed_at: completedAt,
    started_at: request.started_at || null,
    research_cost_usd: null,
    executive_summary: { paragraphs: execParas },
    key_findings: keyFindings,
    sections,
    findings: keyFindings.map((f, i) => ({
      finding_id: f.id || `finding_${i + 1}`,
      headline: f.headline,
      body: f.body || f.summary || null,
      status: f.status || "RESEARCH_FINDING",
      lane: f.lane || null,
      source_refs: f.citations || [],
    })),
    people: normalized.people || [],
    entities: normalized.organizations || [],
    relationships: normalized.relationships || [],
    events: normalized.events || [],
    open_questions: openQs.map((q, i) => ({
      id: q.open_question_id || q.id || `oq_${i + 1}`,
      question: q.question || q.title,
      title: q.title || q.question,
      why_it_matters: q.why_it_matters || null,
      what_would_resolve: q.what_would_resolve || q.best_next_evidence || null,
      best_next_evidence: q.best_next_evidence || q.what_would_resolve || null,
    })),
    sources: sources.map((s, i) => ({
      id: s.id || `src_${i + 1}`,
      number: s.number || i + 1,
      title: s.title || deriveTitleFromUrl(s.url) || `Untitled source ${i + 1}`,
      url: s.url || null,
      publisher:
        s.publisher && s.publisher !== "—"
          ? s.publisher
          : publisherFromUrl(s.url) || "Publisher unavailable",
      date: s.published_date || s.date || null,
      source_type: s.source_type || "Other",
      source_class: s.source_type || s.authority_class || "Other",
    })),
    claim_handoff: { auto_promote: false, candidates: [] },
    raw_artifact_reference: normalized.raw_ref || request.raw_artifact_id || null,
  });

  dossier.dossier_type = DOSSIER_TYPE_RESEARCH_ADDENDUM;
  dossier.report_type = "RESEARCH_ADDENDUM";
  dossier.product_line = labels.product_line;
  dossier.report_type_label = labels.report_type_label;
  dossier.template_id = template.template_id;
  dossier.template_version = template.version;
  dossier.research_request_id = request.request_id;
  dossier.research_run_id = input.run_id || request.run_id || dossier.research_run_id;
  dossier.parent_report_id = request.parent_report_id || null;
  dossier.lineage = request.parent_report_id
    ? {
        follow_up_to: {
          report_id: request.parent_report_id,
          label: "Full Hotel Intelligence Investigation",
          completed_display: "Sep 4, 2026",
        },
      }
    : null;
  dossier.subtitle = DOSSIER_TITLE_RESEARCH_ADDENDUM;
  dossier.cover = {
    document_type: "RESEARCH ADDENDUM",
    investigation_name: template.display_name || "Research Addendum",
    parent_label: dossier.lineage?.follow_up_to?.label || null,
    methodology_note:
      "This Research Addendum provides focused follow-up intelligence based on reviewed public and proprietary research sources. Findings should be validated against primary legal, contractual or property-level documents where indicated.",
  };
  if (hotelLocation) dossier.hotel_location = hotelLocation;
  dossier.change_opportunity_analysis = isChangeOpportunity
    ? {
        meta: analysis._meta,
        research_question: analysis.researchQuestion,
        key_findings_count: analysis.keyFindings.length,
        sources_count: analysis.sources.length,
        open_questions_count: analysis.openQuestions.length,
      }
    : null;
  dossier.internal_accounting = {
    historical_provider_cost_usd:
      request.provider_actual_cost_usd != null ? Number(request.provider_actual_cost_usd) : null,
    recovered_legacy_orphan: Boolean(request.recovered_legacy_orphan),
    provider_run_id_internal: request.provider_run_id || null,
    analysis_word_count: analysis?._meta?.analysis_word_count || null,
  };

  refreshDossierCounts(dossier);

  const published = enrichReportForPublishing(dossier);
  Object.assign(dossier, published);

  if (input.persist !== false) {
    persistResearchAddendum(dossier, input.env);
  }
  return dossier;
}
