/**
 * DealSignal Opportunity Drawer V2 — commercial intelligence from HIF findings
 * + OPEW/Enrichment research overlays + DRAWER_HANDOFF structured sections.
 * Read-only. CUSTOMER_BLOCKED. No schema invention / no Surfe / no Airtable writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assignResearchQueueTier } from "../dealsignal/dealsignal-cala-research-priority-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

// Later paths win — prefer repo-local research packages over Track A snapshots.
const CANDIDATE_PATHS = [
  path.join(ROOT, "../ws-hif-v6-track-a/reports/dealsignal-enrichment-v1/ENRICHMENT_RESEARCH_PACKAGE.json"),
  path.join(ROOT, "../ws-hif-v6-track-a/reports/dealsignal-owner-portfolio-early-warning-v2/RESEARCH_CANDIDATES.json"),
  path.join(ROOT, "../ws-hif-v6-track-a/reports/dealsignal-owner-portfolio-early-warning-v2/RESEARCH_CANDIDATES_CONTINUATION.json"),
  path.join(ROOT, "../ws-hif-v6-track-a/reports/dealsignal-global-source-cala-discovery-v1/RESEARCH_CANDIDATES.json"),
  path.join(ROOT, "reports/dealsignal-enrichment-v1/ENRICHMENT_RESEARCH_PACKAGE.json"),
  path.join(ROOT, "reports/dealsignal-owner-portfolio-early-warning-v2/RESEARCH_CANDIDATES.json"),
  path.join(ROOT, "reports/dealsignal-owner-portfolio-early-warning-v2/RESEARCH_CANDIDATES_CONTINUATION.json"),
  path.join(ROOT, "reports/dealsignal-global-source-cala-discovery-v1/RESEARCH_CANDIDATES.json"),
];

// Later paths win on name/id collisions — keep repo-local handoffs last so IC
// integrations (status verification, drawer copy) override Track A research snapshots.
const HANDOFF_PATHS = [
  path.join(ROOT, "../ws-hif-v6-track-a/reports/dealsignal-owner-portfolio-early-warning-v2/DRAWER_HANDOFF.json"),
  path.join(ROOT, "../ws-hif-v6-track-a/reports/dealsignal-owner-portfolio-early-warning-v2/DRAWER_HANDOFF_CONTINUATION.json"),
  path.join(ROOT, "reports/dealsignal-owner-portfolio-early-warning-v2/DRAWER_HANDOFF.json"),
  path.join(ROOT, "reports/dealsignal-owner-portfolio-early-warning-v2/DRAWER_HANDOFF_CONTINUATION.json"),
];

let overlayCache = null;
let handoffCache = null;

function safeReadJson(filePath) {
  try {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function candidateRows(json) {
  if (!json) return [];
  if (Array.isArray(json.candidates)) return json.candidates;
  if (Array.isArray(json.deepened)) return json.deepened;
  if (Array.isArray(json.opportunities)) return json.opportunities;
  return [];
}

function loadOverlays() {
  if (overlayCache) return overlayCache;
  const byFindingId = new Map();
  const byDiscoveryId = new Map();
  const byHotelName = new Map();

  for (const p of CANDIDATE_PATHS) {
    const json = safeReadJson(p);
    if (!json) continue;
    for (const row of candidateRows(json)) {
      const findingId = row.findingId || row.id || null;
      const disc = row.discoveryPropertyId || null;
      const name = String(row.hotelName || row.name || "").trim().toLowerCase();
      const packed = { ...row, _overlayFile: path.basename(path.dirname(p)) };
      if (findingId) byFindingId.set(String(findingId), packed);
      if (disc) byDiscoveryId.set(String(disc), packed);
      if (name) byHotelName.set(name, packed);
    }
  }
  overlayCache = { byFindingId, byDiscoveryId, byHotelName };
  return overlayCache;
}

function loadHandoffs() {
  if (handoffCache) return handoffCache;
  const byHotelName = new Map();
  const byHandoffId = new Map();
  const committed = [];

  for (const p of HANDOFF_PATHS) {
    const json = safeReadJson(p);
    if (!json) continue;
    for (const row of json.opportunities || []) {
      const name = String(row.A_opportunityOverview?.hotelOrPortfolio || "").trim().toLowerCase();
      const packed = { ...row, _handoffFile: path.basename(p) };
      if (row.handoffId) byHandoffId.set(String(row.handoffId), packed);
      if (name) byHotelName.set(name, packed);
    }
    for (const c of json.committedControlsForDrawer || []) committed.push(c);
  }
  handoffCache = { byHotelName, byHandoffId, committed };
  return handoffCache;
}

function pickOverlay(finding, inventory, opportunity) {
  const overlays = loadOverlays();
  return (
    overlays.byFindingId.get(finding?.findingId) ||
    overlays.byDiscoveryId.get(inventory?.discoveryPropertyId) ||
    overlays.byHotelName.get(String(opportunity?.name || inventory?.hotelName || "").trim().toLowerCase()) ||
    null
  );
}

function pickHandoff(finding, inventory, opportunity, overlay) {
  const handoffs = loadHandoffs();
  const name = String(
    opportunity?.name || inventory?.hotelName || overlay?.hotelName || ""
  )
    .trim()
    .toLowerCase();
  if (name && handoffs.byHotelName.has(name)) return handoffs.byHotelName.get(name);
  // Fuzzy: handoff hotel name contained in opportunity name or vice versa
  for (const [hn, row] of handoffs.byHotelName) {
    if (!hn || !name) continue;
    if (name.includes(hn) || hn.includes(name)) return row;
  }
  return null;
}

function parsePipeNotes(notes) {
  const text = String(notes || "");
  const out = {
    owners: [],
    decisionMakers: [],
    timing: null,
    gaps: [],
    recommended: null,
    researchMaturity: null,
    commercialStatus: null,
    engagementRoute: null,
  };
  const owners = text.match(/Owners?:\s*([^|]+)/i);
  if (owners) {
    out.owners = owners[1]
      .split(/;|,/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  const dms = text.match(/Decision-makers?:\s*([^|]+)/i);
  if (dms) {
    out.decisionMakers = dms[1]
      .split(/;/)
      .map((s) => s.trim())
      .filter(Boolean)
      .map((chunk) => {
        const m = chunk.match(/^(.+?)\s*\((.+)\)\s*$/);
        if (m) return { name: m[1].trim(), role: m[2].trim(), organization: null, source: "Airtable finding notes" };
        return { name: chunk, role: null, organization: null, source: "Airtable finding notes" };
      });
  }
  const timing = text.match(/Timing:\s*([^|]+)/i);
  if (timing) out.timing = timing[1].trim();
  const gaps = text.match(/Gaps?:\s*([^|]+)/i);
  if (gaps) {
    out.gaps = gaps[1]
      .split(/;|,/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  const rec = text.match(/Recommended:\s*([^|]+)/i);
  if (rec) out.recommended = rec[1].trim();
  const mat = text.match(/researchMaturity=([A-Z_]+)/i);
  if (mat) out.researchMaturity = mat[1].toUpperCase();
  const st = text.match(/commercialStatus=([A-Z_]+)/i);
  if (st) out.commercialStatus = st[1].toUpperCase();
  const route = text.match(/engagementRoute[=:]\s*([A-Z_]+)/i);
  if (route) out.engagementRoute = route[1].toUpperCase();
  return out;
}

function mapEvidenceLabel(raw) {
  const v = String(raw || "").toUpperCase();
  if (v === "HIGH" || v === "STRONG") return "Strong";
  if (v === "MEDIUM" || v === "MODERATE") return "Moderate";
  if (v === "LOW" || v === "LIMITED" || v === "UNVERIFIED") return "Limited";
  return null;
}

function evidenceStrength({ confidence, sources, group, hasPrimaryOwnerSource, overlayStrength }) {
  const mapped = mapEvidenceLabel(overlayStrength) || mapEvidenceLabel(confidence);
  if (mapped) return mapped;
  const conf = String(confidence || "").toUpperCase();
  const n = Array.isArray(sources) ? sources.filter((s) => s?.url).length : 0;
  if (conf === "HIGH" && (hasPrimaryOwnerSource || n >= 1) && group === "qualified") return "Strong";
  if (conf === "HIGH" || (conf === "MEDIUM" && n >= 1) || group === "qualified") return "Moderate";
  if (conf === "LOW" || conf === "UNVERIFIED" || group === "watch") return "Limited";
  return n ? "Moderate" : "Limited";
}

function engagementPriority({ group, timingHorizon, overlay, handoff, opportunity }) {
  // Historical / executed dispositions must never receive ENGAGE NOW.
  if (
    opportunity?.commercialDisposition === "historical" ||
    /HISTORICAL/i.test(String(opportunity?.narrativeHints?.commercialStatus || "")) ||
    /HISTORICAL|EXECUTED|RETAINED|COMMITTED/i.test(String(opportunity?.status || ""))
  ) {
    return "MONITOR";
  }
  if (opportunity?.narrativeHints?.engagementPriority) {
    return opportunity.narrativeHints.engagementPriority;
  }
  const raw =
    handoff?.A_opportunityOverview?.engagementPriority ||
    overlay?.engagementPriority ||
    null;
  if (raw) {
    const p = String(raw).toUpperCase().replace(/_/g, " ");
    if (p.includes("ENGAGE")) return "ENGAGE NOW";
    if (p.includes("BUILD")) return "BUILD RELATIONSHIP";
    if (p.includes("MONITOR")) return "MONITOR";
  }
  if (group === "qualified" && (timingHorizon === "0_6_MONTHS" || timingHorizon === "NEAR_TERM")) {
    return "ENGAGE NOW";
  }
  if (group === "qualified") return "BUILD RELATIONSHIP";
  return "MONITOR";
}

function labelFact(value, kind = "FACT") {
  if (value == null || value === "" || value === "Unavailable" || value === "Requires research") {
    return { value: "Unknown", kind: "UNKNOWN" };
  }
  return { value: String(value), kind };
}

function isPrimarySource(url, publisher, authority) {
  const u = String(url || "").toLowerCase();
  const p = String(publisher || "").toLowerCase();
  const a = String(authority || "").toUpperCase();
  if (a.includes("PRIMARY")) return true;
  return /sec\.gov|sedar|otcmarkets|globenewswire|edgar|10-k|10-q|earnings|reit|embassy|ahip|diamondrock|pebblebrook|sunstone|rlj/i.test(
    u + " " + p
  );
}

/**
 * Build drawerV2 payload for an opportunity using finding + inventory + optional overlay/handoff.
 */
export function buildDrawerV2({ finding, inventory, opportunity, relatedHotels = [] }) {
  const notesParsed = parsePipeNotes(`${finding?.notes || ""} ${inventory?.notesQuarantine || ""}`);
  const overlay = pickOverlay(finding, inventory, opportunity);
  const handoff = pickHandoff(finding, inventory, opportunity, overlay);
  const thesis = overlay?.commercialThesis || null;
  const overview = handoff?.A_opportunityOverview || null;
  const ownerBg = handoff?.C_ownerBackground || null;
  const whyAlt = handoff?.E_whyOwnerMightConsiderAlternatives || null;

  const ownerFromOverlay = overlay?.owner || overlay?.decisionParty || overview?.owner || null;
  const owners = [
    ...notesParsed.owners,
    ...(ownerFromOverlay ? [ownerFromOverlay] : []),
    ...(ownerBg?.identity ? [ownerBg.identity] : []),
    ...(finding?.relatedOrgName ? [finding.relatedOrgName] : []),
  ].filter((v, i, a) => v && a.indexOf(v) === i);

  const operator =
    overlay?.currentOperator ||
    (overview?.currentBrandOperator && /\/|\bmanagement\b/i.test(overview.currentBrandOperator)
      ? String(overview.currentBrandOperator).split(/\/|,/).slice(-1)[0].trim()
      : null) ||
    (/Aimbridge|ONE Lodging/i.test(`${finding?.valueObserved || ""} ${finding?.notes || ""}`)
      ? "ONE Lodging Management LLC (Aimbridge)"
      : null);

  const brand =
    inventory?.currentBrand ||
    opportunity?.sub ||
    overlay?.currentBrand ||
    (overview?.currentBrandOperator ? String(overview.currentBrandOperator).split(/\/|,/)[0].trim() : null) ||
    null;

  const executives = [];
  const pushExec = (e, source) => {
    if (!e?.name) return;
    if (executives.some((x) => x.name === e.name)) return;
    executives.push({
      name: e.name,
      role: e.role || e.title || null,
      organization: e.organization || ownerFromOverlay || null,
      relevance: e.relevance || "Owner decision pathway",
      verification: e.verification || null,
      source: e.source || source || "Research handoff",
      publicProfileUrl: e.publicProfileUrl || null,
      contact: e.contact || null,
    });
  };
  if (Array.isArray(overlay?.executives)) {
    for (const e of overlay.executives) pushExec(e, overlay._overlayFile || "Research package");
  }
  if (Array.isArray(handoff?.F_decisionMakers)) {
    for (const e of handoff.F_decisionMakers) pushExec(e, handoff._handoffFile || "Drawer handoff");
  }
  for (const dm of notesParsed.decisionMakers) pushExec(dm, "Airtable finding notes");

  const sources = [];
  const pushSource = (s) => {
    if (!s) return;
    const url = s.url || (String(s.title || "").startsWith("http") ? s.title : null);
    const title = s.title && !String(s.title).startsWith("http") ? s.title : s.publisher || s.claim || s.supportedClaim || "Source";
    const publisher = s.publisher || null;
    const authority = s.authority || s.independence || s.type || null;
    const primary = isPrimarySource(url, publisher, authority);
    const role = String(s.role || "").toUpperCase() || null;
    // Related/context refs stay visible but must not masquerade as direct claim support.
    const directClaimSupport =
      role === "TRIGGER" || role === "CORROBORATION" || (!role && Boolean(s.claim || s.supportedClaim));
    const claim = s.claim || s.caption || s.supportedClaim || null;
    const next = {
      title: title || url || "Source",
      publisher,
      date: s.date || s.publicationDate || finding?.reportingDate || finding?.effectiveDate || null,
      claim,
      url,
      language: s.language || null,
      role: role || null,
      directClaimSupport,
      primarySecondary: primary ? "PRIMARY" : "SECONDARY",
      category: authority,
      limitations:
        s.limitation ||
        s.limitations ||
        (role === "CONTEXT"
          ? "Context / related reference — not direct claim support for the commercial trigger"
          : primary
            ? null
            : "Syndicated or secondary coverage — verify against primary disclosure"),
    };
    if (url) {
      const existing = sources.find((x) => x.url === url);
      if (existing) {
        // Upgrade a thin Airtable URL stub with claim/language/role from research overlays.
        if (!existing.claim && next.claim) existing.claim = next.claim;
        if (!existing.language && next.language) existing.language = next.language;
        if (!existing.role && next.role) existing.role = next.role;
        if (!existing.publisher && next.publisher) existing.publisher = next.publisher;
        if (existing.directClaimSupport !== true && next.directClaimSupport) {
          existing.directClaimSupport = true;
        }
        if (existing.title === "Source" || existing.title === url) existing.title = next.title;
        return;
      }
    }
    sources.push(next);
  };
  for (const s of opportunity?.sources || []) pushSource(s);
  for (const s of overlay?.sources || []) pushSource(s);
  for (const s of handoff?.I_supportingEvidence || []) pushSource(s);
  if (finding?.sourceUrl) pushSource({ url: finding.sourceUrl, title: "Finding source URL", publisher: null });

  const hasPrimary = sources.some((s) => s.primarySecondary === "PRIMARY");
  const strength = evidenceStrength({
    confidence: finding?.confidence || overlay?.evidenceStrength || overview?.evidenceStrength,
    sources,
    group: opportunity?.group,
    hasPrimaryOwnerSource: hasPrimary,
    overlayStrength: overlay?.evidenceStrength || overview?.evidenceStrength,
  });

  const rooms = overlay?.rooms || overlay?.keys || null;
  const hotelCount =
    relatedHotels?.length > 1
      ? relatedHotels.length
      : overlay?.hotels || (opportunity?.type?.toLowerCase().includes("portfolio") ? null : 1);

  const whatChanging =
    thesis?.whatIsHappening ||
    handoff?.B_whyThisMatters ||
    String(finding?.valueObserved || opportunity?.why || "").split("|")[0].trim() ||
    "Commercial signal retained; catalyst narrative requires enrichment.";
  const hints = opportunity?.narrativeHints || {};
  const isHistorical =
    opportunity?.commercialDisposition === "historical" ||
    /HISTORICAL/i.test(String(hints.commercialStatus || opportunity?.status || ""));
  const whyMatters =
    hints.whyItMatters ||
    thesis?.whyItMatters ||
    (isHistorical
      ? "Historical commercial intelligence for chronology, owner/brand/operator context, and source provenance. Do not approach as if the decision remains open."
      : opportunity?.kind === "BCT"
        ? "An upcoming brand or franchise decision could create a pathway for alternative brand discussion."
        : opportunity?.kind === "MCT"
          ? "A management or operator change could create a pathway for alternative operator discussion."
          : "A commercial change may create engagement relevance for brands or operators.");
  const confirmedOpen =
    hints.confirmedProspectiveDecision ||
    (/COMMITTED|CLOSED|HISTORICAL/i.test(
      String(overview?.commercialStatus || overlay?.commercialStatus || opportunity?.status || ""),
    )
      ? "Documented as committed/closed/historical — not an open conversion opportunity."
      : opportunity?.group === "qualified"
        ? "Prospective decision evidence is present; treat as commercially relevant, not as a guaranteed open RFP."
        : "Open competitive process is not confirmed — emerging / watch posture.");

  const claimClasses = whyAlt?.claimClasses || null;

  const uncertainties = [
    ...(notesParsed.gaps || []),
    ...(overlay?.gaps || []),
    ...(handoff?.J_unansweredQuestions || []),
    ...(thesis?.limits ? [thesis.limits] : []),
    ...(whyAlt?.limits ? [whyAlt.limits] : []),
  ].filter((v, i, a) => v && a.indexOf(v) === i);

  if (!owners.length) uncertainties.push("Ownership verification still required");
  if (!executives.length) uncertainties.push("Named decision-maker / executive still required");
  if (!opportunity?.window || /unknown|timing to verify/i.test(opportunity.window)) {
    uncertainties.push("Decision timing remains indicative or unverified");
  }
  if (!opportunity?.he?.ready) uncertainties.push("Canonical Hotel Explorer / HPC identity not yet linked");

  const contractual = [];
  const pushContract = (label, value, kind) => {
    contractual.push({ label, ...labelFact(value, kind) });
  };
  pushContract(
    "Agreement / milestone type",
    finding?.agreementType || overlay?.agreementType || overlay?.timingClass || opportunity?.fieldName || null,
    finding?.agreementType || overlay?.timingClass ? "FACT" : "SUPPORTED_INFERENCE"
  );
  pushContract("Effective / event date", finding?.effectiveDate || overlay?.nextDecisionDate || null, "FACT");
  pushContract("Reporting date", finding?.reportingDate || null, "FACT");
  pushContract(
    "Decision window",
    overview?.potentialTiming || opportunity?.window || notesParsed.timing || overlay?.commercialTiming || null,
    opportunity?.window && !/unknown|to verify|timing to verify/i.test(opportunity.window)
      ? "FACT"
      : overview?.potentialTiming
        ? "SUPPORTED_INFERENCE"
        : "UNKNOWN"
  );
  pushContract(
    "Current operator",
    operator,
    operator ? (overlay?.currentOperator ? "FACT" : "SUPPORTED_INFERENCE") : "UNKNOWN"
  );
  pushContract("Current brand / affiliation", brand, brand ? "FACT" : "UNKNOWN");
  if (thesis?.ownerStrategy || ownerBg?.strategy) {
    pushContract("Owner strategy (documented)", thesis?.ownerStrategy || ownerBg?.strategy, "SUPPORTED_INFERENCE");
  }
  for (const cat of handoff?.D_whatCouldTriggerChange || []) {
    pushContract(
      cat.catalyst || "Catalyst",
      cat.type || null,
      /VERIFIED/i.test(cat.type || "") ? "FACT" : "SUPPORTED_INFERENCE"
    );
  }

  const whyPartner =
    hints.whyPartner ||
    (isHistorical
      ? "Do not recommend competing for a completed selection. Preserve this record for historical diligence only unless a separate, independently evidenced future opportunity exists."
      : whyAlt?.rationale ||
        thesis?.whyAlternativesPlausible ||
        thesis?.discussionAngle ||
        (opportunity?.kind === "BCT"
          ? "Franchise or brand milestone timing can create a discussion window for alternative brand positioning — not a prediction of conversion."
          : opportunity?.kind === "MCT"
            ? "Documented management transition or evaluation can create a discussion window for alternative operators — not a prediction of award."
            : "Commercial milestone evidence may justify relationship development with the verified owner."));

  const nextResearch = [];
  const nextEngage = [];
  if (uncertainties.some((u) => /ownership/i.test(u))) {
    nextResearch.push("Verify ownership / sponsor identity against primary disclosures.");
  }
  if (!executives.length) {
    nextResearch.push("Identify the owner’s asset-management or development executive pathway.");
  }
  if (!opportunity?.he?.ready) {
    nextResearch.push("Resolve Hotel Property Census identity before deep Hotel Explorer diligence.");
  }
  if (opportunity?.group !== "qualified") {
    nextResearch.push("Watch for affirmative open-process or owner-led selection evidence.");
  }
  if (isHistorical) {
    nextResearch.push(
      "Treat as historical intelligence only — do not invent a new open selection from this completed event.",
    );
  }

  const recommended = isHistorical
    ? hints.nextStep ||
      "Retain as historical intelligence. Do not outreach as an open conversion/management opportunity unless independent prospective evidence appears."
    : handoff?.G_potentialEngagementApproach ||
      notesParsed.recommended ||
      overlay?.recommendedAction ||
      opportunity?.next ||
      null;
  if (recommended) nextEngage.push(recommended);
  if (!isHistorical && opportunity?.group === "qualified" && owners.length) {
    nextEngage.push(
      "Begin relationship development with the verified owner organization ahead of the indicated decision window."
    );
  }
  if (relatedHotels?.length > 1) {
    nextEngage.push(
      "Review related portfolio hotels individually in Hotel Explorer where HPC identities exist."
    );
  }
  if (!nextEngage.length) {
    nextEngage.push(
      "Monitor commercial disclosures; do not treat as outreach-ready until timing and access are confirmed."
    );
  }

  const maturity =
    notesParsed.researchMaturity ||
    overlay?.researchMaturity ||
    (opportunity?.group === "qualified" ? "VETTED" : "INVESTIGATED");
  const commercialStatus =
    hints.commercialStatus ||
    overview?.commercialStatus ||
    notesParsed.commercialStatus ||
    overlay?.commercialStatus ||
    (isHistorical
      ? "HISTORICAL_EXECUTED"
      : opportunity?.group === "qualified"
        ? "VERIFIED"
        : opportunity?.status || "EMERGING");

  const priority = engagementPriority({
    group: opportunity?.group,
    timingHorizon: opportunity?.timingHorizon || overlay?.commercialTiming,
    overlay,
    handoff,
    opportunity,
  });

  const keyDates = (handoff?.H_keyDates || []).map((d) => ({
    label: d.label,
    date: d.date,
    class: d.class || "UNKNOWN",
  }));

  const isPortfolio =
    relatedHotels?.length > 1 ||
    /portfolio/i.test(opportunity?.type || "") ||
    /portfolio/i.test(overview?.hotelOrPortfolio || "") ||
    /portfolio/i.test(opportunity?.name || "");

  const portfolioOpportunity = isPortfolio
    ? {
        ownerOrganization: owners[0] || ownerBg?.identity || null,
        relatedHotels: (relatedHotels || []).map((h) => ({
          name: h.name || h.hotelName,
          brand: h.brand || h.currentBrand || null,
          market: [h.city, h.stateRegion, h.country].filter(Boolean).join(", ") || null,
          hpcRecordId: h.hpcRecordId || null,
        })),
        hotelsLinked: relatedHotels?.length || hotelCount || null,
        angle:
          ownerBg?.commercialObjectives ||
          thesis?.discussionAngle ||
          "Portfolio-level engagement — one commercial mandate, multiple properties for diligence.",
        note: "Do not count every linked hotel as an independent open conversion opportunity.",
      }
    : relatedHotels?.length
      ? {
          ownerOrganization: owners[0] || null,
          relatedHotels: relatedHotels.map((h) => ({
            name: h.name,
            brand: h.brand || null,
            market: [h.city, h.stateRegion, h.country].filter(Boolean).join(", ") || null,
            hpcRecordId: h.hpcRecordId || null,
          })),
          hotelsLinked: relatedHotels.length,
          angle: null,
          note: null,
        }
      : null;

  const engagementReason =
    keyDates.find((d) => /RECOMMENDED|ENGAGE/i.test(d.class || ""))?.label ||
    overview?.potentialTiming ||
    (priority === "ENGAGE NOW"
      ? "Near-term contractual or selection milestone supports early relationship development."
      : priority === "BUILD RELATIONSHIP"
        ? "Documented commercial pathway exists; timing supports relationship development ahead of a later decision."
        : "Evidence does not yet support active outreach — monitor disclosures.");

  const hotelCountry =
    opportunity?.country ||
    inventory?.country ||
    overlay?.physicalLocation?.country ||
    overview?.location ||
    null;
  const researchQueueTier = assignResearchQueueTier({
    hotelCountry,
    country: hotelCountry,
    timeCritical: priority === "ENGAGE NOW",
    engagementPriority: priority === "ENGAGE NOW" ? "ENGAGE_NOW" : null,
    highValueCalaOwnerSignal: Boolean(
      overlay?.calaPriorityTier === "T2_HIGH_VALUE_CALA" || overlay?.earlyEngagement || handoff?.earlyEngagement,
    ),
    earlyEngagement: Boolean(overlay?.earlyEngagement),
  });

  return {
    version: "dealsignal-drawer-v2",
    publicationState: "CUSTOMER_BLOCKED",
    evidenceStrength: strength,
    engagementPriority: priority,
    researchMaturity: maturity,
    commercialStatus,
    researchQueueTier,
    researchQueueNote:
      "CALA research-processing priority only — does not change commercial status, evidence strength, or eligibility.",
    overlayUsed: overlay
      ? { file: overlay._overlayFile || "local-research", findingId: overlay.findingId || overlay.id || null }
      : null,
    handoffUsed: handoff
      ? { file: handoff._handoffFile || "drawer-handoff", handoffId: handoff.handoffId || null }
      : null,
    executiveSummary: {
      name: overview?.hotelOrPortfolio || opportunity?.name || inventory?.hotelName || "Unnamed opportunity",
      market: opportunity?.city || overview?.location || null,
      country: opportunity?.country || inventory?.country || null,
      currentBrand: brand || "Unknown",
      currentOperator: operator || "Unknown",
      ownerSponsor: owners[0] || "Unknown",
      opportunityType: opportunity?.type || overview?.commercialOpportunityType || null,
      commercialStatus,
      researchMaturity: maturity,
      engagementPriority: priority,
      decisionWindow: overview?.potentialTiming || opportunity?.window || "Unknown",
      timingHorizon: opportunity?.timingHorizon || overlay?.commercialTiming || null,
      hotelsAffected: hotelCount,
      roomsAffected: rooms,
      headline: String(whatChanging).slice(0, 280),
    },
    whyExists: {
      whatIsChanging: whatChanging,
      whyItMatters: whyMatters,
      conversationAngle:
        handoff?.G_potentialEngagementApproach ||
        thesis?.discussionAngle ||
        recommended ||
        null,
      evidenceSupports:
        finding?.evidenceSnippet ||
        thesis?.evidenceBasis ||
        sources[0]?.claim ||
        null,
      remainsUncertain: uncertainties.slice(0, 8),
      confirmedProspectiveDecision: confirmedOpen,
      claimClasses,
    },
    commercialStatusVerification: handoff?.commercialStatusVerification || overlay?.commercialStatusVerification || null,
    ownerBackground: {
      owners,
      assetManagementOrg: overlay?.assetManager || null,
      ownerProfile: thesis?.ownerStrategy || ownerBg?.strategy || null,
      holdingsNote:
        relatedHotels?.length > 1
          ? `${relatedHotels.length} related hotels linked on this portfolio mandate.`
          : ownerBg?.relevantPortfolio ||
            (owners.length
              ? "Single-asset or limited holdings disclosed on this finding — expand via Hotel Explorer / owner research."
              : "Owner holdings not yet verified on this record."),
      brandOperatorRelationships: brand || operator ? [brand, operator].filter(Boolean).join(" · ") : null,
      recentCapitalDevelopments: ownerBg?.commercialObjectives || null,
      hotelExplorer: opportunity?.he || null,
    },
    contractualContext: contractual,
    whyNewPartnerRelevant: {
      perspective: whyPartner,
      limits:
        handoff?.E_whyOwnerMightConsiderAlternatives?.limits ||
        "Commercial discussion opportunity — not a prediction of conversion or award. Contractual flexibility ≠ confirmed intention to replace the current partner.",
      posture: "Commercial discussion opportunity — not a prediction of conversion or award.",
      claimClasses,
    },
    decisionMakers: {
      people: executives,
      organizations: owners.map((o) => ({ name: o, role: "Owner / decision party" })),
      contactStatus: executives.some((e) => e.contact)
        ? "AUTHORIZED_CONTACT_PRESENT"
        : "Contact research required",
      note: "No Surfe auto-enrichment. Personal emails/phones are never invented.",
    },
    whenToEngage: {
      verifiedAgreementDates: [
        finding?.effectiveDate,
        overlay?.nextDecisionDate,
        ...keyDates.filter((d) => /VERIFIED/i.test(d.class || "")).map((d) => d.date),
      ].filter(Boolean),
      noticeDeadlines: [],
      capitalMilestones: keyDates.filter((d) => /CAPITAL|PIP/i.test(d.label || "")).map((d) => d.date).filter(Boolean),
      indicativeDecisionPeriod: overview?.potentialTiming || opportunity?.window || notesParsed.timing || "Unknown",
      timingHorizon: opportunity?.timingHorizon || overlay?.commercialTiming || null,
      keyDates,
      priority,
      reason: engagementReason,
      caveat: "Evidence-grounded recommendation only — not a guarantee the opportunity is open.",
    },
    portfolioOpportunity,
    supportingEvidence: sources,
    stillNeedToKnow: uncertainties.slice(0, 10),
    recommendedNextSteps: {
      researchTasks: nextResearch,
      engagementActions: nextEngage,
    },
  };
}
