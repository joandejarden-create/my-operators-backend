/**
 * Bethesda Marriott — 26-opportunity demand-segment audit (offline, read-only).
 * Population: demandStatus ∈ {CONFIRMED_DEMAND, ACTIVE, EMERGING_DEMAND}
 * Canonical hotel: recLuxvwwxID7U2B8
 * Does not mutate opportunities.json / Airtable / publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SUPERVISOR_EXPECTED_SEGMENTS,
  canonicalizeSegmentId,
} from "./research-constants-v1.js";
import {
  RESEARCH_SEGMENT,
  RESEARCH_SEGMENT_LABEL,
  classifyOpportunitySegment,
} from "../workflow/hotel-segment-research-plan-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";

export const BETHESDA_26_SEGMENT_AUDIT_V1 = "gdi-bethesda-26-opportunity-segment-audit-v1";
export const REPORTS_DIR = "reports/gdi-bethesda-26-opportunity-segment-audit-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

/** Active demand statuses that define the intended 26-opportunity commercial population. */
export const BETHESDA_26_DEMAND_STATUSES = Object.freeze([
  "CONFIRMED_DEMAND",
  "ACTIVE",
  "EMERGING_DEMAND",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

export function loadBethesdaCanonicalOpportunities({ root = ROOT } = {}) {
  const p = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) {
    return { path: p, exists: false, hotelId: null, opportunities: [], total: 0 };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const opportunities = Array.isArray(doc.opportunities) ? doc.opportunities : [];
  return {
    path: p.replace(/\\/g, "/"),
    exists: true,
    hotelId: doc.hotelId || BETHESDA_CANONICAL_HOTEL_ID,
    updatedAt: doc.updatedAt || null,
    opportunities,
    total: opportunities.length,
  };
}

/**
 * Establish the exact 26-opportunity population and reconcile against full bag.
 */
export function resolveBethesda26OpportunityPopulation({ root = ROOT } = {}) {
  const bag = loadBethesdaCanonicalOpportunities({ root });
  const all = bag.opportunities || [];
  const population = all.filter((o) =>
    BETHESDA_26_DEMAND_STATUSES.includes(String(o.demandStatus || "")),
  );
  const excluded = all.filter((o) =>
    !BETHESDA_26_DEMAND_STATUSES.includes(String(o.demandStatus || "")),
  );

  const byExcludedStatus = {};
  for (const o of excluded) {
    const k = String(o.demandStatus || "unset");
    byExcludedStatus[k] = (byExcludedStatus[k] || 0) + 1;
  }

  const populationVerified = population.length === 26;

  return {
    version: BETHESDA_26_SEGMENT_AUDIT_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    sourcePath: bag.path,
    sourceUpdatedAt: bag.updatedAt,
    canonicalBagTotal: bag.total,
    populationDefinition: {
      rule: "demandStatus ∈ CONFIRMED_DEMAND | ACTIVE | EMERGING_DEMAND",
      statuses: [...BETHESDA_26_DEMAND_STATUSES],
      rationale: "Active/confirmed/emerging commercial demand set — excludes speculative watch, recurring-predicted-only, and unset demandStatus rows. Distinct from the 54-record full bag, the ~103 historical inventory, and the 10 live NET_NEW discovery candidates.",
    },
    populationCount: population.length,
    populationVerified,
    discrepancy: populationVerified
      ? null
      : {
        expected: 26,
        actual: population.length,
        message: population.length === 0
          ? "Could not locate a 26-record population — do not manufacture records."
          : `Population filter yielded ${population.length}, not 26.`,
      },
    excludedFromPopulationCount: excluded.length,
    excludedByDemandStatus: byExcludedStatus,
    notSubstituted: {
      fullCanonicalBag: bag.total,
      historicalInventoryNote: "Do not use 103-record baseline inventory as this population",
      liveNetNewCandidatesNote: "Do not use the 10 housing-page NET_NEW discoveries as this population",
    },
    populationIds: population.map((o) => o.id || o.opportunityId).filter(Boolean),
    population,
  };
}

/**
 * Map free-text / legacy segment labels to a canonical id when possible.
 */
export function mapCurrentSegmentToCanonical(rawSegment = "") {
  const raw = text(rawSegment);
  if (!raw) {
    return { canonical: null, mapped: false, note: "missing_current_segment" };
  }
  const canon = canonicalizeSegmentId(raw);
  const isCanonical = SUPERVISOR_EXPECTED_SEGMENTS.includes(canon);
  // "Overflow Housing" incorrectly aliases to exhibitor_sponsor_teams — flag
  if (/^overflow\s*housing$/i.test(raw)) {
    return {
      canonical: null,
      mapped: false,
      raw,
      note: "OVERFLOW_HOUSING_IS_OPPORTUNITY_TYPE_NOT_DEMAND_SEGMENT",
    };
  }
  if (/university\s*\/\s*education/i.test(raw) && !/medical|healthcare|scientific/i.test(raw)) {
    // Ambiguous: could be association (AGB) or tours — leave unmapped for audit override
    return { canonical: null, mapped: false, raw, note: "AMBIGUOUS_UNIVERSITY_EDUCATION_LABEL" };
  }
  if (/training\s*program/i.test(raw)) {
    return {
      canonical: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      mapped: true,
      raw,
      note: "training_program_label_treated_as_gov_conference_pending_mechanism",
    };
  }
  if (/event\s*\/\s*association/i.test(raw)) {
    return { canonical: null, mapped: false, raw, note: "GENERIC_EVENT_ASSOCIATION_LABEL" };
  }
  if (isCanonical) {
    return { canonical: canon, mapped: true, raw };
  }
  // Fuzzy via canonicalize that returned non-canonical string
  return { canonical: null, mapped: false, raw, note: "UNMAPPED_LEGACY_LABEL", canonicalizeResult: canon };
}

/**
 * Recommend primary (+ optional secondary) from demand mechanism — not organizer type alone.
 */
export function recommendDemandSegment(opp = {}) {
  const id = opp.id || opp.opportunityId;
  const org = text(opp.organizationName);
  const title = text(opp.title || opp.canonicalEventName || opp.normalizedEventName);
  const blob = `${org} ${title} ${opp.summaryWhat || ""} ${opp.segment || ""} ${opp.demandSignalType || ""}`.toLowerCase();
  const auto = classifyOpportunitySegment(opp);

  /** Curated mechanism overrides — evidence-backed, not organizer-label defaults. */
  const OVERRIDES = {
    gdi_opp_ahima_advocacy_2027: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      confidence: "HIGH",
      evidence: "Advocacy Summit / Hill-policy lodging demand — association advocacy mechanism; health-info domain is secondary.",
    },
    gdi_opp_acc_legislative_2027: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      confidence: "HIGH",
      evidence: "Legislative Conference Cap-Hill advocacy format — lodging for advocacy attendees, not clinical sessions.",
    },
    gdi_opp_ndss_advocacy_2027: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      confidence: "HIGH",
      evidence: "Down Syndrome Advocacy Conference — advocacy/association demand; healthcare affinity secondary.",
    },
    gdi_opp_nar_gad_institute_2027: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: null,
      confidence: "HIGH",
      evidence: "NAR GAD Institute — association advocacy/government-affairs institute.",
    },
    gdi_opp_nado_ddaa_washcon_2028: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: null,
      confidence: "HIGH",
      evidence: "NADO/DDAA Washington Conference — association + Hill Day lodging.",
    },
    gdi_opp_nice_2027: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
      confidence: "MEDIUM",
      evidence: "NIST/NICE cybersecurity education conference — government-hosted professional conference; training flavor secondary.",
    },
    gdi_opp_regulatory_information_conference_20260930: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
      confidence: "HIGH",
      evidence: "NRC Regulatory Information Conference — federal agency conference (not a hotel training-product segment).",
    },
    gdi_opp_asae_annual_2029: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
      confidence: "HIGH",
      evidence: "ASAE Annual Meeting & Exposition — association annual; exhibitor teams are secondary demand path.",
    },
    gdi_opp_agb_2027_disqualified: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST,
      confidence: "HIGH",
      evidence: "AGB National Conference on Trusteeship — association of university boards conference, not campus tour demand.",
    },
    gdi_opp_ntca_2027_disqualified: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: null,
      confidence: "HIGH",
      evidence: "NTCA Legislative and Policy Conference — rural broadband association advocacy.",
    },
    gdi_opp_ptab_2027_disqualified: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: null,
      confidence: "HIGH",
      evidence: "PTAB Bar Association Annual Conference — professional association.",
    },
    gdi_opp_fba_qui_tam_2027_disqualified: {
      primary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      secondary: null,
      confidence: "HIGH",
      evidence: "Federal Bar Association Qui Tam Conference — bar/association legal conference.",
    },
    gdi_opp_show_2026_nih: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: null,
      confidence: "HIGH",
      evidence: "NIH/NHLBI research conference on campus — medical/scientific demand.",
    },
    gdi_opp_nih_research_festival_2026: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
      confidence: "HIGH",
      evidence: "NIH Research Festival — scientific campus event; vendor/attendee overflow is secondary exhibitor path.",
    },
    gdi_opp_aan_2027_disqualified: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
      confidence: "HIGH",
      evidence: "AAN Annual Meeting — medical specialty scientific meeting (convention-scale).",
    },
    gdi_opp_ada_2027_disqualified: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
      confidence: "HIGH",
      evidence: "ADA Scientific Sessions — medical scientific sessions.",
    },
    gdi_opp_accp_annual_meeting_20261001: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: null,
      confidence: "HIGH",
      evidence: "ACCP Annual Meeting — clinical pharmacy professional/scientific meeting.",
    },
    gdi_opp_amwa_2027_annual: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      confidence: "HIGH",
      evidence: "AMWA Annual Meeting — medical professional association annual (medical content primary).",
    },
    gdi_opp_acts_ts27: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: null,
      confidence: "HIGH",
      evidence: "ACTS Translational Science — clinical/translational science meeting.",
    },
    gdi_opp_bebpa_usb_2027: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: null,
      confidence: "HIGH",
      evidence: "BEBPA US Bioassay Conference — biopharma scientific conference.",
    },
    gdi_opp_asa_advance_2027_disqualified: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: null,
      confidence: "HIGH",
      evidence: "ASA ADVANCE — anesthesiology medical meeting (already contracted elsewhere).",
    },
    gdi_opp_acvnu_renal_week_20270412: {
      primary: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      secondary: null,
      confidence: "HIGH",
      evidence: "ACVNU Renal Week — veterinary/clinical nephrology scientific meeting. Current 'Overflow Housing' is opportunity type, not segment.",
    },
    gdi_opp_loudoun_college_showcase_2027: {
      primary: RESEARCH_SEGMENT.SPORTS,
      secondary: null,
      confidence: "HIGH",
      evidence: "Soccer college showcase tournament — stay-to-play sports lodging (not educational tour).",
    },
    gdi_opp_arlington_spring_tournament_2027: {
      primary: RESEARCH_SEGMENT.SPORTS,
      secondary: null,
      confidence: "HIGH",
      evidence: "Youth soccer tournament — sports lodging.",
    },
    gdi_opp_bethesda_premier_cup_2026: {
      primary: RESEARCH_SEGMENT.SPORTS,
      secondary: null,
      confidence: "HIGH",
      evidence: "Bethesda Premier Cup soccer tournament — sports.",
    },
    gdi_opp_alexandria_soccer_kickoff_2027: {
      primary: RESEARCH_SEGMENT.SPORTS,
      secondary: null,
      confidence: "HIGH",
      evidence: "Alexandria Soccer Kickoff — stay-to-play sports lodging.",
    },
  };

  if (OVERRIDES[id]) {
    const o = OVERRIDES[id];
    return {
      primary: o.primary,
      secondary: o.secondary,
      confidence: o.confidence,
      evidence: o.evidence,
      method: "demand_mechanism_override",
      autoClassify: auto,
    };
  }

  if (auto && SUPERVISOR_EXPECTED_SEGMENTS.includes(auto)) {
    let secondary = null;
    if (auto === RESEARCH_SEGMENT.MEDICAL_ACADEMIC && /advocacy|legislative|hill/i.test(blob)) {
      secondary = RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
    }
    if (auto === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT && /exhibit|exposition|expo/i.test(blob)) {
      secondary = RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS;
    }
    return {
      primary: auto,
      secondary,
      confidence: "MEDIUM",
      evidence: `classifyOpportunitySegment → ${auto} from title/org/demand fields.`,
      method: "classifyOpportunitySegment",
      autoClassify: auto,
    };
  }

  return {
    primary: null,
    secondary: null,
    confidence: "MANUAL_REVIEW",
    evidence: "Insufficient demand-mechanism evidence to assign a canonical segment without guessing.",
    method: "unresolved",
    autoClassify: auto,
  };
}

export function auditOneOpportunity(opp = {}, index = 0) {
  const id = opp.id || opp.opportunityId;
  const currentRaw = text(opp.segment);
  const currentMapped = mapCurrentSegmentToCanonical(currentRaw);
  const rec = recommendDemandSegment(opp);
  const changeRequired = Boolean(
    rec.primary
    && (
      !currentMapped.mapped
      || currentMapped.canonical !== rec.primary
      || currentMapped.note
    ),
  );

  const contactRoute = Boolean(
    opp.primaryContactEmail
    || opp.primaryContact?.email
    || opp.contactOfficialUrl
    || (Array.isArray(opp.contacts) && opp.contacts.some((c) => c?.email || c?.url)),
  );

  return {
    row: index + 1,
    opportunityId: id,
    organization: text(opp.organizationName),
    title: text(opp.title || opp.canonicalEventName),
    demandMechanism: text(opp.title || opp.canonicalEventName || opp.summaryWhat),
    demandStatus: opp.demandStatus || null,
    opportunityType: opp.opportunityType || null,
    eventDateDisplay: opp.eventDateDisplay || opp.eventStartDate || null,
    currentSegmentRaw: currentRaw || null,
    currentSegmentCanonical: currentMapped.canonical,
    currentSegmentMappingNote: currentMapped.note || null,
    recommendedPrimary: rec.primary,
    recommendedPrimaryLabel: rec.primary ? RESEARCH_SEGMENT_LABEL[rec.primary] || rec.primary : null,
    secondarySegment: rec.secondary,
    secondarySegmentLabel: rec.secondary ? RESEARCH_SEGMENT_LABEL[rec.secondary] || rec.secondary : null,
    confidence: rec.confidence,
    changeRequired,
    evidence: rec.evidence,
    classificationMethod: rec.method,
    autoClassify: rec.autoClassify || null,
    contactRoutePresent: contactRoute,
    customerVisible: opp.customerVisible === true,
    commercialRelevanceNote: null, // filled in commercial pass
    deepResearchCandidate: false,
    manualReviewRequired: rec.confidence === "MANUAL_REVIEW" || !rec.primary,
  };
}

function enrichCommercialNotes(row, opp) {
  const primary = row.recommendedPrimary;
  let relevance = "UNKNOWN";
  if (primary === RESEARCH_SEGMENT.MEDICAL_ACADEMIC) {
    relevance = "HIGH — Bethesda NIH / medical corridor alignment";
  } else if (primary === RESEARCH_SEGMENT.SPORTS) {
    relevance = "MEDIUM-HIGH — local/regional tournament stay-to-play; Premier Cup especially local";
  } else if (primary === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT) {
    relevance = /legislative|advocacy|hill|gad|washington/i.test(row.title)
      ? "MEDIUM — DC advocacy/overflow; downtown often preferred"
      : "MEDIUM — association/government lodging in DMV";
  }
  if (/disqualified|CLOSED|Gaylord|Hilton Capitol|Ritz-Carlton|Washington Hilton|Hyatt Regency Washington/i.test(
    `${row.title} ${opp.opportunityType || ""} ${opp.alreadySourcedToHotelDisplay || ""}`,
  )) {
    relevance += " | CURRENT CYCLE LARGELY COMMITTED ELSEWHERE — future-cycle / overflow only";
  }

  row.commercialRelevanceNote = relevance;
  row.deepResearchCandidate = row.changeRequired === false
    && row.contactRoutePresent === false
    && !/disqualified|CLOSED_DISQUALIFIED/i.test(String(opp.opportunityType || ""))
    && /EMERGING|ACTIVE/i.test(String(opp.demandStatus || ""));
  row.groupDemandCredible = ["CONFIRMED_DEMAND", "ACTIVE", "EMERGING_DEMAND"].includes(opp.demandStatus);
  return row;
}

export function buildSegmentDistribution(audits = []) {
  const current = {};
  const corrected = {};
  for (const s of SUPERVISOR_EXPECTED_SEGMENTS) {
    current[s] = 0;
    corrected[s] = 0;
  }
  let currentUnmapped = 0;
  let manualReview = 0;
  let reclassCount = 0;

  for (const a of audits) {
    if (a.currentSegmentCanonical && current[a.currentSegmentCanonical] !== undefined) {
      current[a.currentSegmentCanonical] += 1;
    } else {
      currentUnmapped += 1;
    }
    if (a.recommendedPrimary && corrected[a.recommendedPrimary] !== undefined) {
      corrected[a.recommendedPrimary] += 1;
    } else {
      manualReview += 1;
    }
    if (a.changeRequired) reclassCount += 1;
  }

  const n = audits.length;
  const rows = SUPERVISOR_EXPECTED_SEGMENTS.map((s) => ({
    segment: s,
    label: RESEARCH_SEGMENT_LABEL[s] || s,
    currentCount: current[s],
    correctedCount: corrected[s],
    percentOfPortfolio: n ? Number(((corrected[s] / n) * 100).toFixed(1)) : 0,
    reclassificationIntoSegment: corrected[s] - current[s],
  }));

  const correctedSum = rows.reduce((a, r) => a + r.correctedCount, 0);
  const emptySegments = rows.filter((r) => r.correctedCount === 0).map((r) => r.segment);
  const disproportionate = rows
    .filter((r) => r.percentOfPortfolio >= 25)
    .map((r) => ({ segment: r.segment, percent: r.percentOfPortfolio, count: r.correctedCount }));

  return {
    populationSize: n,
    currentUnmapped,
    manualReview,
    reclassificationRequired: reclassCount,
    correctedSum,
    correctedSumEqualsPopulation: correctedSum === n && manualReview === 0,
    emptySegments,
    disproportionate,
    bySegment: rows,
  };
}

export function detectDuplicatesAndIdentityIssues(population = []) {
  const byOrg = new Map();
  for (const o of population) {
    const key = text(o.organizationName).toLowerCase();
    if (!key) continue;
    if (!byOrg.has(key)) byOrg.set(key, []);
    byOrg.get(key).push(o.id || o.opportunityId);
  }
  const multiProgramOrgs = [...byOrg.entries()]
    .filter(([, ids]) => ids.length > 1)
    .map(([org, ids]) => ({ organization: org, opportunityIds: ids, note: "Multiple programs — verify not duplicates" }));

  const idSet = new Set();
  const duplicateIds = [];
  for (const o of population) {
    const id = o.id || o.opportunityId;
    if (idSet.has(id)) duplicateIds.push(id);
    idSet.add(id);
  }

  return {
    duplicateOpportunityIds: duplicateIds,
    organizationsWithMultipleOpportunities: multiProgramOrgs,
    note: "Multiple opportunities under one org can be valid distinct programs/cycles.",
  };
}

export function buildCommercialAssessmentBySegment(audits = [], distribution = null) {
  const dist = distribution || buildSegmentDistribution(audits);
  return SUPERVISOR_EXPECTED_SEGMENTS.map((seg) => {
    const rows = audits.filter((a) => a.recommendedPrimary === seg);
    const withContact = rows.filter((a) => a.contactRoutePresent).length;
    return {
      segment: seg,
      label: RESEARCH_SEGMENT_LABEL[seg] || seg,
      volume: rows.length,
      percentOf26: dist.bySegment.find((r) => r.segment === seg)?.percentOfPortfolio || 0,
      commercialRelevanceToBethesda: rows[0]?.commercialRelevanceNote
        || (rows.length === 0 ? "GAP — no opportunities in population" : "See opportunity-level notes"),
      groupDemandCredibility: rows.filter((a) => a.groupDemandCredible).length,
      contactRouteCompleteness: rows.length
        ? `${withContact}/${rows.length}`
        : "0/0",
      deepResearchCandidates: rows.filter((a) => a.deepResearchCandidate).map((a) => a.opportunityId),
      netNewDiscoveryGap: rows.length === 0
        ? "Segment empty in active 26 — prioritize fresh discovery if commercially relevant to Bethesda"
        : rows.length <= 2
          ? "Thin coverage — discovery should not ignore this segment"
          : "Represented — still seek NET_NEW rather than only refreshing these",
      opportunityIds: rows.map((a) => a.opportunityId),
    };
  });
}

export function buildDiscoveryRecommendations(distribution = {}, identity = {}) {
  return {
    skewObserved: distribution.disproportionate,
    emptySegments: distribution.emptySegments,
    recommendations: [
      "Do not treat free-text labels like 'Overflow Housing', 'Training Program', or 'Event / Association' as canonical segments.",
      "Classify by demand mechanism (advocacy lodging vs scientific sessions vs tournament stay-to-play), not organizer industry alone.",
      "Medical/academic is disproportionately represented — valid for Bethesda NIH corridor, but discovery must still cover sports, local catering, DMC, corporate, and private events.",
      "Empty segments in the 26 (corporate meetings, local catering, weddings/social, entertainment, tours, planners/DMC, scaling companies, exhibitor-primary) are coverage gaps for weekly NET_NEW discovery — not proof of zero market demand.",
      "Exhibitor/sponsor demand often appears as secondary on large medical/association expos — keep as secondary intelligence; seek exhibitor-primary programs separately.",
      "Preserve distinct programs under the same organization (e.g. NIH SHOW vs NIH Research Festival) as separate opportunities.",
      ...(identity.organizationsWithMultipleOpportunities || []).slice(0, 3).map((m) =>
        `Verify distinct cycles for ${m.organization}: ${m.opportunityIds.join(", ")}`),
    ],
  };
}

/**
 * Run full audit and write reports/gdi-bethesda-26-opportunity-segment-audit-v1/
 */
export function runBethesda26OpportunitySegmentAudit({ root = ROOT } = {}) {
  const resolved = resolveBethesda26OpportunityPopulation({ root });
  const outDir = path.join(root, REPORTS_DIR);

  if (!resolved.populationVerified) {
    const fail = {
      version: BETHESDA_26_SEGMENT_AUDIT_V1,
      ok: false,
      discrepancy: resolved.discrepancy,
      populationCount: resolved.populationCount,
      canonicalBagTotal: resolved.canonicalBagTotal,
      message: "26-record population could not be verified — not manufacturing missing records.",
    };
    writeJson(path.join(outDir, "00-population-discrepancy.json"), fail);
    writeMd(path.join(outDir, "REPORT.md"), `# Bethesda 26-Opportunity Segment Audit — DISCREPANCY\n\nExpected 26; found **${resolved.populationCount}** under rule \`${resolved.populationDefinition.rule}\`.\nCanonical bag total: ${resolved.canonicalBagTotal}.\n\nDo not manufacture records.\n`);
    return fail;
  }

  const audits = resolved.population.map((opp, i) =>
    enrichCommercialNotes(auditOneOpportunity(opp, i), opp));
  const distribution = buildSegmentDistribution(audits);
  const identity = detectDuplicatesAndIdentityIssues(resolved.population);
  const commercial = buildCommercialAssessmentBySegment(audits, distribution);
  const discovery = buildDiscoveryRecommendations(distribution, identity);

  const table = audits.map((a) => ({
    "#": a.row,
    "Organization / Opportunity": `${a.organization} — ${a.title}`,
    "Current Segment": a.currentSegmentRaw || "(none)",
    "Recommended Segment": a.recommendedPrimary || "MANUAL_REVIEW",
    "Secondary Segment": a.secondarySegment || "—",
    Confidence: a.confidence,
    "Change Required": a.changeRequired ? "YES" : "NO",
  }));

  writeJson(path.join(outDir, "00-population-reconciliation.json"), {
    ...resolved,
    population: undefined, // IDs only in this file to keep size down
    populationIds: resolved.populationIds,
  });
  writeJson(path.join(outDir, "01-audit-table.json"), { count: table.length, table, audits });
  writeJson(path.join(outDir, "02-eleven-segment-distribution.json"), distribution);
  writeJson(path.join(outDir, "03-individual-classification-evidence.json"), {
    count: audits.length,
    items: audits.map((a) => ({
      opportunityId: a.opportunityId,
      organization: a.organization,
      demandMechanism: a.demandMechanism,
      currentSegmentRaw: a.currentSegmentRaw,
      recommendedPrimary: a.recommendedPrimary,
      secondarySegment: a.secondarySegment,
      confidence: a.confidence,
      evidence: a.evidence,
      changeRequired: a.changeRequired,
      method: a.classificationMethod,
    })),
  });
  writeJson(path.join(outDir, "04-reclassification-recommendations.json"), {
    count: audits.filter((a) => a.changeRequired).length,
    recommendations: audits.filter((a) => a.changeRequired).map((a) => ({
      opportunityId: a.opportunityId,
      from: a.currentSegmentRaw,
      to: a.recommendedPrimary,
      secondary: a.secondarySegment,
      confidence: a.confidence,
      evidence: a.evidence,
      applyToCanonical: false,
      note: "Audit only — do not modify canonical opportunities yet",
    })),
  });
  writeJson(path.join(outDir, "05-duplicates-and-identity-concerns.json"), identity);
  writeJson(path.join(outDir, "06-segment-coverage-gaps.json"), {
    emptySegments: distribution.emptySegments,
    emptyLabels: distribution.emptySegments.map((s) => RESEARCH_SEGMENT_LABEL[s] || s),
    thinSegments: distribution.bySegment.filter((r) => r.correctedCount > 0 && r.correctedCount <= 2),
    disproportionate: distribution.disproportionate,
  });
  writeJson(path.join(outDir, "07-commercial-implications.json"), { bySegment: commercial });
  writeJson(path.join(outDir, "08-discovery-recommendations.json"), discovery);

  const summary = {
    version: BETHESDA_26_SEGMENT_AUDIT_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    populationVerified: true,
    populationCount: 26,
    canonicalBagTotal: resolved.canonicalBagTotal,
    reclassificationRequired: distribution.reclassificationRequired,
    correctedSum: distribution.correctedSum,
    emptySegmentCount: distribution.emptySegments.length,
    correctedDistribution: Object.fromEntries(
      distribution.bySegment.map((r) => [r.segment, r.correctedCount]),
    ),
    paidSpend: 0,
    canonicalRecordsModified: false,
  };
  writeJson(path.join(outDir, "SUMMARY.json"), summary);

  const mdTable = [
    "| # | Organization / Opportunity | Current Segment | Recommended Segment | Secondary Segment | Confidence | Change Required |",
    "|---|---------------------------|-----------------|---------------------|-------------------|------------|-----------------|",
    ...table.map((r) =>
      `| ${r["#"]} | ${String(r["Organization / Opportunity"]).replace(/\|/g, "/")} | ${r["Current Segment"]} | ${r["Recommended Segment"]} | ${r["Secondary Segment"]} | ${r.Confidence} | ${r["Change Required"]} |`),
  ].join("\n");

  const distMd = distribution.bySegment.map((r) =>
    `| ${r.label} (\`${r.segment}\`) | ${r.currentCount} | ${r.correctedCount} | ${r.percentOfPortfolio}% |`).join("\n");

  writeMd(path.join(outDir, "REPORT.md"), `# Bethesda Marriott — 26-Opportunity Demand Segment Audit

**Hotel:** ${BETHESDA_CANONICAL_HOTEL_NAME} (\`${BETHESDA_CANONICAL_HOTEL_ID}\`)  
**Population verified:** YES — **26** opportunities  
**Definition:** \`demandStatus ∈ CONFIRMED_DEMAND | ACTIVE | EMERGING_DEMAND\`  
**Canonical bag total:** ${resolved.canonicalBagTotal} (not this population)  
**Canonical records modified:** No · **Paid research:** None

## Population reconciliation

- Full \`opportunities.json\`: **${resolved.canonicalBagTotal}**
- Active demand population (this audit): **26**
- Excluded (speculative / recurring-predicted / unset / other): **${resolved.excludedFromPopulationCount}** — ${JSON.stringify(resolved.excludedByDemandStatus)}
- Not used: 103-record historical inventory; 10 live NET_NEW housing-page candidates

## Full 26-row audit table

${mdTable}

## Eleven-segment distribution (primary only; secondaries not double-counted)

| Segment | Current (mapped) | Corrected | % of 26 |
|---------|------------------|-----------|---------|
${distMd}
| **Unmapped current labels** | **${distribution.currentUnmapped}** | — | — |
| **Corrected sum** | — | **${distribution.correctedSum}** | ${distribution.correctedSumEqualsPopulation ? "✓ = 26" : "CHECK"} |

**Reclassification required:** ${distribution.reclassificationRequired} / 26  
**Empty segments (corrected):** ${distribution.emptySegments.map((s) => RESEARCH_SEGMENT_LABEL[s] || s).join("; ") || "(none)"}  
**Disproportionate (≥25%):** ${distribution.disproportionate.map((d) => `${d.segment} ${d.percent}%`).join("; ") || "(none)"}

## Classification issues

- Many current labels are free-text (\`Medical\`, \`Healthcare\`, \`Overflow Housing\`, \`Training Program\`) rather than the 11 canonical ids.
- \`Overflow Housing\` is an **opportunity type**, not a demand segment (ACVNU was mislabeled).
- Advocacy conferences (AHIMA, ACC Legislative, NDSS) were often tagged medical/healthcare; **primary demand mechanism is associations/government advocacy lodging**.
- Medical/academic concentration reflects Bethesda NIH corridor strength **and** discovery skew — empty segments remain real coverage gaps for weekly NET_NEW work.

## Commercial implications (summary)

See \`07-commercial-implications.json\`. Qualification, publication eligibility, and contact verification are **unchanged** by this audit.

## Discovery recommendations

${discovery.recommendations.map((x) => `- ${x}`).join("\n")}

## Deliverables

1. \`00-population-reconciliation.json\`
2. \`01-audit-table.json\`
3. \`02-eleven-segment-distribution.json\`
4. \`03-individual-classification-evidence.json\`
5. \`04-reclassification-recommendations.json\`
6. \`05-duplicates-and-identity-concerns.json\`
7. \`06-segment-coverage-gaps.json\`
8. \`07-commercial-implications.json\`
9. \`08-discovery-recommendations.json\`
10. \`SUMMARY.json\` + this \`REPORT.md\`
`);

  return {
    ok: true,
    outDir: outDir.replace(/\\/g, "/"),
    summary,
    table,
    distribution,
    audits,
  };
}
