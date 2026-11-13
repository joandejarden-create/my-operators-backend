#!/usr/bin/env node
/**
 * Sheraton Guadalajara Expo — recover historical Exa evidence packs and rebuild
 * the original 62 opportunities. No new paid research. No CUSTOMER_PUBLISHED.
 *
 *   npm run gdi:sheraton-exa-historical-research-recovery-v1
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { rehydratePersistedExaEvidence } from "../lib/group-demand-intelligence/workflow/gdi-persisted-exa-evidence-rehydrate-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import { planCustomerTileBadges } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { INVENTORY_POPULATION } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const V1_FP =
  "60f2d7ef732ac9578e33e159988092b90f49ec8372b8b174c7630296240e6211";
const V1_VERSION = "gdi_pub_recCAGqkERSXgiku4_20261113_full_e2e_v1";
const V2_FP =
  "6f9c0416be28c2409cf9fcccfb78cc45dd3a5178b27d4e0009f2622a21efd092";
const V2_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261113_commercial_e2e_remediation_v2";
const REVIEW_DATE = "2026-11-13";
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-exa-historical-research-recovery-v1",
);
const RESEARCH_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunity-research-state",
);
const AIMBRIDGE = path.join(
  ROOT,
  "..",
  "my-operators-backend-aimbridge-three-hotel-e2e",
);
const AIMBRIDGE_LIVE = path.join(
  AIMBRIDGE,
  "reports/gdi-aimbridge-full-e2e-live-v1",
);

function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function shaFile(p) {
  return crypto
    .createHash("sha256")
    .update(fs.readFileSync(p))
    .digest("hex");
}
function isGeneric(s) {
  return /Documented local expansion\s*\/\s*growth activity|investigate hotel-relevant group/i.test(
    text(s),
  );
}
function mapSegment(seg) {
  const s = text(seg).toLowerCase().replace(/-/g, "_");
  const map = {
    association_events: "association_conference_group",
    sports_entertainment: "sports_entertainment",
    government_public_sector: "government_public_sector",
    corporate_meetings_incentives: "corporate_meetings_incentives",
    education_scientific: "education_scientific",
    projects_infrastructure: "projects_infrastructure",
    social_special_events: "social_special_events",
    local_catering_day_meetings: "local_catering_day_meetings",
    corporate_celebrations_private_events: "corporate_celebrations_private_events",
    scaling_companies: "scaling_company",
    preferred_lodging: "preferred_lodging_partnership",
  };
  return map[s] || s || null;
}
function parseIso(raw) {
  const s = text(raw);
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (m) return m[1];
  const y = s.match(/\b(20\d{2})\b/);
  return y ? `${y[1]}-01-01` : null;
}

fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(path.join(OUT, "snapshots"), { recursive: true });
fs.mkdirSync(path.join(OUT, "indexes"), { recursive: true });

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const v1 = store.loadPackage(HOTEL_ID, V1_VERSION);
const v2 = store.loadPackage(HOTEL_ID, V2_VERSION);
if (!v1.ok || v1.package.contentFingerprintSha256 !== V1_FP) {
  console.error("BLOCKED: V1 baseline package missing/mismatched");
  process.exit(3);
}
fs.writeFileSync(
  path.join(OUT, "snapshots", "v1-baseline-package.json"),
  JSON.stringify(v1.package, null, 2) + "\n",
);
if (v2.ok) {
  fs.writeFileSync(
    path.join(OUT, "snapshots", "v2-remediation-package.json"),
    JSON.stringify(v2.package, null, 2) + "\n",
  );
}

const baseline62 = v1.package.opportunities;
const baselineIds = new Set(baseline62.map((o) => o.id));

// --- Artifact inventory ---
const artifacts = [];
function addArtifact(a) {
  artifacts.push({
    artifactId: a.artifactId,
    location: a.location,
    kind: a.kind,
    provider: a.provider || null,
    collectedAt: a.collectedAt || null,
    hotelId: HOTEL_ID,
    relatedOpportunityIds: a.relatedOpportunityIds || [],
    organization: a.organization || null,
    eventOrProject: a.eventOrProject || null,
    sourceUrls: a.sourceUrls || [],
    sourceType: a.sourceType || null,
    integritySha256: a.integritySha256 || null,
    originalIntact: a.originalIntact !== false,
    notes: a.notes || null,
  });
}

// Dossiers
const dossierFiles = fs.existsSync(RESEARCH_DIR)
  ? fs.readdirSync(RESEARCH_DIR).filter((f) => f.endsWith(".json"))
  : [];
const dossierById = new Map();
for (const f of dossierFiles) {
  const p = path.join(RESEARCH_DIR, f);
  let d;
  try {
    d = loadJson(p);
  } catch {
    continue;
  }
  const id = text(d.opportunityId || d.id || f.replace(/\.json$/, ""));
  dossierById.set(id, d);
  const pack = d.exaAgentEvidencePack;
  const urls = [
    ...(pack?.sources || []).map((s) => s.url).filter(Boolean),
    ...(d.evidence || []).map((e) => e.url).filter(Boolean),
    ...(d.sources || []).map((s) => (typeof s === "string" ? s : s.url)).filter(Boolean),
  ];
  addArtifact({
    artifactId: `dossier:${id}`,
    location: path.relative(ROOT, p).replace(/\\/g, "/"),
    kind: "RESEARCH_DOSSIER",
    provider: pack || d.exaAgentDeepResearch ? "exa_agent" : "gdi_research",
    collectedAt:
      d.exaAgentDeepResearch?.at ||
      d.sourcePublicationDate ||
      d.updatedAt ||
      null,
    relatedOpportunityIds: [id],
    organization: d.organizationName || null,
    eventOrProject: d.title || d.activityHeadline || null,
    sourceUrls: [...new Set(urls)].slice(0, 40),
    sourceType: pack ? "exa_evidence_pack_embedded" : "dossier_summary",
    integritySha256: shaFile(p),
    originalIntact: true,
    notes: pack
      ? `exaAgentEvidencePack claims=${(pack.claims || []).length} sources=${(pack.sources || []).length}`
      : d.exaAgentDeepResearch
        ? "exaAgentDeepResearch present without full pack"
        : "no exa pack",
  });
  if (pack) {
    addArtifact({
      artifactId: `exa_pack:${id}`,
      location: path.relative(ROOT, p).replace(/\\/g, "/") + "#exaAgentEvidencePack",
      kind: "EXA_EVIDENCE_PACK",
      provider: "exa_agent",
      collectedAt: d.exaAgentDeepResearch?.at || null,
      relatedOpportunityIds: [id],
      organization: d.organizationName || null,
      eventOrProject: d.title || null,
      sourceUrls: (pack.sources || []).map((s) => s.url).filter(Boolean),
      sourceType: "embedded_pack_not_raw_http",
      integritySha256: crypto
        .createHash("sha256")
        .update(JSON.stringify(pack))
        .digest("hex"),
      originalIntact: true,
      notes:
        "Intact Exa evidence pack (claims+sources+excerpts). Raw HTTP provider response body not separately archived on disk.",
    });
  }
}

// Aimbridge reports
const reportFiles = [
  "aimbridge-paid-exa-agent-provenance-reconciled-v1.json",
  "DEALALITY_SHERATON_GDI_COMMERCIAL_RESEARCH_RESULTS.json",
  "DEALALITY_GDI_SHERATON_STAGE_B_LIVE_CERTIFICATION_RESULTS.json",
  "DEALALITY_SHERATON_FULL_E2E_REMEDIATION_COMPLETE.json",
  "DEALALITY_ORIGINAL_14_SEGMENT_EXA_AGENT_FORENSIC_VERIFICATION.json",
  "publication-candidate-manifest-v1.json",
].map((n) => path.join(AIMBRIDGE_LIVE, n));
for (const p of reportFiles) {
  if (!fs.existsSync(p)) continue;
  const j = loadJson(p);
  addArtifact({
    artifactId: `report:${path.basename(p)}`,
    location: p,
    kind: "AIMBRIDGE_RESEARCH_REPORT",
    provider: "exa_agent_ledger",
    collectedAt: j.at || j.evaluatedAt || j.generatedAt || null,
    relatedOpportunityIds: [],
    sourceUrls: [],
    sourceType: "ledger_or_manifest",
    integritySha256: shaFile(p),
    notes: `keys=${Object.keys(j).slice(0, 12).join(",")}`,
  });
}

const orsDir = path.join(
  AIMBRIDGE_LIVE,
  "commercial-research-sheraton/ors-checkpoints",
);
let orsCount = 0;
if (fs.existsSync(orsDir)) {
  for (const f of fs.readdirSync(orsDir).filter((x) => x.endsWith(".json"))) {
    orsCount += 1;
    const id = f.replace(/\.json$/, "");
    addArtifact({
      artifactId: `ors_checkpoint:${id}`,
      location: path.join(orsDir, f),
      kind: "ORS_CHECKPOINT",
      provider: "gdi_ors",
      relatedOpportunityIds: [id],
      sourceType: "checkpoint",
      integritySha256: shaFile(path.join(orsDir, f)),
    });
  }
}

const provenancePath = path.join(
  AIMBRIDGE_LIVE,
  "aimbridge-paid-exa-agent-provenance-reconciled-v1.json",
);
const provenance = fs.existsSync(provenancePath) ? loadJson(provenancePath) : null;
const provenanceRows = Array.isArray(provenance?.rows) ? provenance.rows : [];
const provenanceByOpp = new Map();
for (const row of provenanceRows) {
  if (row.hotelId && row.hotelId !== HOTEL_ID) continue;
  const id = text(row.opportunityId);
  if (!id) continue;
  if (!provenanceByOpp.has(id)) provenanceByOpp.set(id, []);
  provenanceByOpp.get(id).push(row);
  addArtifact({
    artifactId: `provenance:${id}:${row.providerRunId || "unknown"}`,
    location: provenancePath,
    kind: "PAID_EXA_PROVENANCE_ROW",
    provider: "exa_agent",
    collectedAt: provenance.at || null,
    relatedOpportunityIds: [id],
    organization: row.organizationName || null,
    eventOrProject: row.title || row.activity || null,
    sourceUrls: row.evidenceUrls || [],
    sourceType: "ledger_row",
    originalIntact: Boolean(row.providerRunId),
    notes: `class=${row.class || row.provenanceClass || "?"} run=${row.providerRunId || "none"}`,
  });
}

writeJson("01-HISTORICAL_EXA_ARTIFACT_INVENTORY.json", {
  hotelId: HOTEL_ID,
  reviewedAt: new Date().toISOString(),
  reviewDatePolicy: REVIEW_DATE,
  counts: {
    artifacts: artifacts.length,
    dossiers: dossierFiles.length,
    dossiersWithExaPack: artifacts.filter((a) => a.kind === "EXA_EVIDENCE_PACK")
      .length,
    orsCheckpoints: orsCount,
    provenanceRows: provenanceRows.filter((r) => r.hotelId === HOTEL_ID || !r.hotelId)
      .length,
    rawHttpProviderBodiesFoundOnDisk: 0,
  },
  rawHttpNote:
    "Intact Exa evidence packs (claims/sources/excerpts) are embedded in research dossiers. Separate raw HTTP Exa API response bodies were not found as standalone files for Sheraton.",
  paidSpendUsdFromProvenance: provenance?.paidSpendUsd ?? null,
  commerciallyUsefulAfterRepair:
    provenance?.recoverable?.commerciallyUsefulAfterRepair ?? null,
  sampleArtifacts: artifacts.slice(0, 25),
});

writeJson(
  "02-RESEARCH_PROVENANCE_INDEX.json",
  {
    schema: "gdi-sheraton-exa-provenance-index-v1",
    artifactCount: artifacts.length,
    artifacts,
  },
);
// Also durable index under data for reference without giant git churn of raw packs
const indexDir = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "exa-historical-recovery-v1",
);
fs.mkdirSync(indexDir, { recursive: true });
fs.writeFileSync(
  path.join(indexDir, "provenance-index.json"),
  JSON.stringify(
    {
      schema: "gdi-sheraton-exa-provenance-index-v1",
      createdAt: new Date().toISOString(),
      artifactCount: artifacts.length,
      artifacts: artifacts.map((a) => ({
        ...a,
        // keep paths; no secrets
      })),
    },
    null,
    2,
  ) + "\n",
);

// --- Map to 62 ---
const mappings = [];
const customerOps = [];
const researchStage = [];
const freshnessRows = [];
const thesisRows = [];
const contactRows = [];
const marketRows = [];
const salesRows = [];
const actionRows = [];
const qualityRows = [];

for (const base of baseline62) {
  const id = text(base.id);
  const dossier = dossierById.get(id) || null;
  const prov = provenanceByOpp.get(id) || [];
  const orsPath = path.join(orsDir, `${id}.json`);
  const ors = fs.existsSync(orsPath) ? loadJson(orsPath) : null;

  const matched = [];
  if (dossier) matched.push(`dossier:${id}`);
  if (dossier?.exaAgentEvidencePack) matched.push(`exa_pack:${id}`);
  if (ors) matched.push(`ors_checkpoint:${id}`);
  for (const p of prov) {
    matched.push(`provenance:${id}:${p.providerRunId || "unknown"}`);
  }

  const rehydrated = rehydratePersistedExaEvidence(dossier || {});
  const d = rehydrated.opportunity;
  const pack = d.exaAgentEvidencePack || {};
  const claims = Array.isArray(pack.claims) ? pack.claims : [];
  const packSources = Array.isArray(pack.sources) ? pack.sources : [];
  const evidence = [
    ...(d.evidence || []),
    ...packSources.map((s) => ({
      url: s.url,
      statement: text(s.establishes || s.title || s.sourceRelevance || ""),
    })),
  ].filter((e) => e.url);

  const claimFacts = claims
    .filter((c) => text(c.factClass || "") === "fact" || !c.factClass)
    .map((c) => text(c.claimText || c.statement))
    .filter(Boolean);

  let coverage =
    packSources.length >= 3 && claims.length >= 2
      ? "COMPLETE_HISTORICAL_PACK"
      : packSources.length || claims.length || (d.evidence || []).length
        ? "PARTIAL"
        : matched.length
          ? "DOSSIER_ONLY"
          : "NO_RECOVERABLE_EXA";

  // Narratives from Exa claims first
  const title = text(base.displayTitle || base.title || d.title || id);
  const org = text(d.organizationName || base.organizationName || "");
  let what = text(
    claimFacts[0] ||
      d.whatIsHappening ||
      d.activityHeadline ||
      base.whatIsHappening ||
      "",
  );
  if (!what || isGeneric(what)) {
    const extras = claimFacts.slice(0, 3).join(" ");
    what = extras
      ? `${org || "Organization"} — ${title}. Recovered Exa facts: ${extras}`
      : text(
          `${org || "Organization"} — ${title}. Historical research retained; demand thesis still requires qualification.`,
        );
  }
  // Prefer non-generic dossier potential + claim support
  let potential = text(d.potentialHotelBusiness || base.potentialHotelBusiness || "");
  if (!potential || potential.length < 40) {
    const lodgingHints = claimFacts.filter((c) =>
      /hotel|lodging|room|meeting|catering|overnight|attendee|delegate|housing|venue/i.test(
        c,
      ),
    );
    potential = lodgingHints.length
      ? `Recovered Exa evidence supports plausible hotel-relevant demand: ${lodgingHints.slice(0, 2).join(" ")} Volume and decision path remain to be confirmed.`
      : `Plausible guestroom/meetings demand around ${title} for ${HOTEL_NAME} remains inferred from public research; not a confirmed room block.`;
  }
  let whyHotel = text(d.whyThisHotel || d.hotelOpportunityThesis || base.whyThisHotel || "");
  if (!whyHotel || /same commercial market \(the market\)/i.test(whyHotel) || isGeneric(whyHotel)) {
    whyHotel = `${HOTEL_NAME} is Expo Guadalajara–adjacent full-service lodging (~216 keys, meeting inventory). Fit depends on whether this demand is capturable in the Plaza del Sol / Expo corridor versus venue-hosted or competitor inventory.`;
  }
  let whyNow = text(
    d.honestSalesBriefV41?.reasonToContactNow ||
      d.salesBrief?.whyActNow ||
      base.whyActNow ||
      "",
  );
  const startDate =
    parseIso(d.originalActivityDate) ||
    parseIso(d.growthEvidenceDate) ||
    parseIso(base.tileEventStartDate) ||
    parseIso(base.datePillDisplayLabel);
  const year = startDate ? Number(startDate.slice(0, 4)) : null;
  let freshness = "HISTORICAL_FACT_STILL_CONTEXTUAL";
  if (year != null && year < 2026 && !/2026|2027|2028|recurring|edition|ciclo/i.test(title + what)) {
    freshness = "EXPIRED_WITHOUT_CLEAR_FUTURE_CYCLE";
  } else if (year != null && year >= 2026) {
    freshness = "FUTURE_ACTIVITY_SIGNAL";
  } else if (/2026|2027|2028/i.test(title + what)) {
    freshness = "FUTURE_ACTIVITY_SIGNAL";
  }

  if (!whyNow || whyNow.length < 30) {
    whyNow =
      freshness === "FUTURE_ACTIVITY_SIGNAL"
        ? `Timing signal ${startDate || year || "forward cycle"} — establish lodging/meetings owner before sourcing closes.`
        : `Evidence recovered from prior Exa research (${d.exaAgentDeepResearch?.at || "historical"}). Re-confirm activity status before outreach.`;
  }

  const pc = d.primaryContact || base.primaryContact || {};
  const hasContact = Boolean(text(pc.name));
  const partnership =
    /preferred lodging partnership|venue partnership|overnight guest referral|independent event venue/i.test(
      title,
    ) || d.preferredLodgingPartnershipV1 === true;
  const catering =
    /banquet|catering|gala|private dining|celebration|wedding|networking/i.test(
      `${title} ${potential} ${what}`,
    );
  const relevance = /cancún|cancun|las vegas/i.test(`${title} ${what}`)
    ? "OUTSIDE"
    : "HOTEL";

  const qualifyStance =
    hasContact &&
    evidence.length > 0 &&
    relevance === "HOTEL" &&
    !partnership &&
    /lodging|room|hotel|overnight|attendee|delegate|housing|group|congreso|congress|torneo|expo|meeting/i.test(
      `${potential} ${what}`,
    );

  let disposition = "CUSTOMER_VISIBLE";
  if (!hasContact || evidence.length === 0) disposition = "RESEARCH_STAGE";
  if (relevance === "OUTSIDE") disposition = "QUARANTINE_WRONG_MARKET";
  if (freshness === "EXPIRED_WITHOUT_CLEAR_FUTURE_CYCLE" && coverage === "NO_RECOVERABLE_EXA") {
    disposition = "RESEARCH_STAGE";
  }

  let draft = {
    ...base,
    displayTitle: title,
    title,
    organizationName: org || base.organizationName || null,
    displayCompanyName: org || base.displayCompanyName || null,
    whatIsHappening: what,
    potentialHotelBusiness: potential,
    potentialBusiness: potential,
    whyThisHotel: whyHotel,
    whyActNow: whyNow,
    summaryWhat: what,
    cardWhyNowLine: whyNow.slice(0, 160),
    sources: evidence.map((e) => ({
      url: e.url,
      statement: text(e.statement || e.quote || title),
    })),
    hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
    demandSegmentId: mapSegment(d.researchSegment || base.opportunityType),
    researchSegment: d.researchSegment || base.opportunityType,
    opportunityType: mapSegment(d.researchSegment || base.opportunityType),
    preferredLodgingPartnershipV1: partnership || undefined,
    demandOpportunitySubtype: partnership
      ? "PREFERRED_LODGING_PARTNERSHIP"
      : undefined,
    revenueStreamClass: catering
      ? "COMBINED_GUESTROOMS_EVENT_FNB"
      : "GUESTROOMS_ONLY",
    eventFnbThesis: catering
      ? `Recovered research suggests possible banquet/meeting components around ${title}; not confirmed catering RFP.`
      : undefined,
    guestroomThesis: potential,
    hasVerifiedNamedRelevantContact: hasContact,
    hasCredibleOutreachRoute: hasContact,
    eventStartDate: startDate,
    tileEventStartDate: startDate,
    datePillDisplayLabel:
      startDate ||
      (year ? String(year) : null) ||
      base.datePillDisplayLabel ||
      "Timing TBD",
    primaryContact: hasContact
      ? {
          name: text(pc.name),
          title: text(pc.title || pc.role || pc.professionalTitle) || null,
          professionalTitle: text(pc.title || pc.role) || null,
          organization: pc.organization || org || null,
          sourceUrl: pc.sourceUrl || null,
          contactClass:
            pc.commercialClass === "hotel_sourcing_decision_maker"
              ? "DIRECT_BUYER"
              : "RELEVANT_COMMERCIAL_ENTRY_POINT",
        }
      : null,
    cardContactSubtitle:
      text(pc.title || pc.role) || (hasContact ? "Role Not Yet Verified" : null),
    tileContactPreview: hasContact
      ? {
          displayRole: text(pc.title || pc.role) || "Role Not Yet Verified",
          displayRoleKind: text(pc.title || pc.role)
            ? "professional_title"
            : "role_not_yet_verified",
          professionalTitle: text(pc.title || pc.role) || null,
        }
      : base.tileContactPreview,
    inventoryPopulations:
      disposition === "CUSTOMER_VISIBLE"
        ? [INVENTORY_POPULATION.SALES_OPPORTUNITIES]
        : [],
    hotelMarketScopeEligible: disposition === "CUSTOMER_VISIBLE",
    competitiveMarketScopeEligible: false,
    commercialStageKey: partnership
      ? "RELATIONSHIP_DEVELOPMENT"
      : qualifyStance
        ? "QUALIFICATION_NEEDED"
        : year >= 2026
          ? "FUTURE_CAPTURE"
          : "QUALIFICATION_NEEDED",
    inventoryLane: null,
    bookingWindowStatus: qualifyStance ? "QUALIFY_NOW" : "WATCH",
    exaRecoveryV1: {
      coverage,
      matchedArtifacts: matched,
      rehydratedEvidence: rehydrated.evidenceAdded,
      rehydratedClaims: rehydrated.claimsAdded,
      packClaims: claims.length,
      packSources: packSources.length,
      freshness,
      paidProvenanceRows: prov.length,
    },
  };
  draft.inventoryLane = draft.commercialStageKey;

  if (qualifyStance) {
    draft.recommendedNextStep = `Qualify the lodging/meetings decision path for ${title} with ${text(pc.name)}. Lodging demand path not established for ${HOTEL_NAME} — do not claim open RFP; confirm whether hotel sourcing is open before pursuit.`;
  } else if (partnership) {
    draft.recommendedNextStep = `Open a preferred-lodging / referral partnership conversation for overflow and overnight guests tied to ${title}.`;
  } else {
    draft.recommendedNextStep = text(
      d.recommendedAction ||
        d.recommendedNextSalesAction ||
        base.recommendedNextStep ||
        "Identify a named commercially relevant contact and verify lodging decision authority.",
    );
  }

  const typeKey = classifyOpportunityTypeFilter(draft);
  draft.opportunityTypeFilterCategory = typeKey;
  draft.opportunityTypeFilterLabel =
    OPPORTUNITY_TYPE_FILTER_LABEL[typeKey] || typeKey;
  draft.opportunityTypeLabel = draft.opportunityTypeFilterLabel;
  draft.segmentDisplayLabel = draft.opportunityTypeFilterLabel;

  const sales = resolveCustomerSalesAction(draft);
  draft.salesActionKey = sales.key;
  draft.salesActionPillLabel = sales.label;
  draft.salesActionReason = sales.reason;
  draft.recommendedAction = draft.recommendedNextStep;

  const tilePlan = planCustomerTileBadges(draft);
  draft.tileBadgePlan = tilePlan;
  draft.priorityFilterKey = tilePlan.priorityFilterKey || draft.priority;
  draft.priorityPillLabel = tilePlan.priorityPillLabel || null;
  draft.commercialStageKey =
    tilePlan.commercialStageKey || draft.commercialStageKey;
  draft.datePillDisplayLabel =
    tilePlan.datePillDisplayLabel || draft.datePillDisplayLabel;
  draft = attachSalesOpportunityFilterFields(draft);

  draft.sharedTenSectionDrawer = {
    sections: [
      { id: "what_is_happening", title: "What is happening", body: what },
      {
        id: "potential_hotel_business",
        title: "Potential hotel business",
        body: potential,
      },
      { id: "why_this_hotel", title: "Why this hotel", body: whyHotel },
      { id: "why_now", title: "Why now", body: whyNow },
      {
        id: "who_to_contact",
        title: "Who to contact",
        body: hasContact
          ? `${pc.name}${pc.title ? ` — ${pc.title}` : ""}. Verify lodging decision authority before outreach.`
          : "Named contact not yet verified.",
      },
      {
        id: "recommended_next_step",
        title: "Recommended next step",
        body: draft.recommendedNextStep,
      },
      {
        id: "supporting_evidence",
        title: "Supporting evidence",
        body: evidence.length
          ? evidence
              .slice(0, 12)
              .map((e) => `${e.statement || ""} ${e.url || ""}`.trim())
              .join("\n")
          : "No recoverable Exa source URLs for this opportunity.",
      },
      {
        id: "research_uncertainties",
        title: "Research uncertainties",
        body: `Coverage: ${coverage}. Freshness: ${freshness}. Sourcing unconfirmed. Sales Ready not claimed.`,
      },
    ],
  };

  mappings.push({
    opportunityId: id,
    title,
    matchedArtifacts: matched,
    coverage,
    packClaims: claims.length,
    packSources: packSources.length,
    evidenceUrls: evidence.length,
    provenanceRows: prov.length,
    orsCheckpoint: Boolean(ors),
    disposition,
  });
  freshnessRows.push({ id, title, freshness, startDate, year });
  thesisRows.push({
    id,
    coverage,
    whatImproved: isGeneric(base.whatIsHappening) && !isGeneric(what),
    usedExaClaims: claimFacts.length > 0,
  });
  contactRows.push({
    id,
    contact: hasContact ? text(pc.name) : null,
    title: text(pc.title || pc.role) || null,
    class: draft.primaryContact?.contactClass || null,
  });
  marketRows.push({
    id,
    hotel: draft.hotelMarketScopeEligible,
    competitive: false,
    rationale:
      disposition === "CUSTOMER_VISIBLE"
        ? "Hotel-market relevant with recovered evidence"
        : disposition,
  });
  salesRows.push({
    id,
    keys: draft.salesOpportunityQuickFilterKeys,
  });
  actionRows.push({
    id,
    action: draft.salesActionPillLabel,
    reason: draft.salesActionReason,
  });
  qualityRows.push({
    id,
    title,
    coverage,
    freshness,
    disposition,
    salesAction: draft.salesActionPillLabel,
    narrativeImproved: isGeneric(base.whatIsHappening) && !isGeneric(what),
    contactUseful: hasContact,
    evidenceCount: evidence.length,
    salesReady: false,
  });

  if (disposition === "CUSTOMER_VISIBLE") customerOps.push(draft);
  else researchStage.push({ id, title, disposition, coverage });
}

const seen = new Set();
const uniqueCustomer = [];
for (const o of customerOps) {
  if (seen.has(o.id)) continue;
  seen.add(o.id);
  uniqueCustomer.push(o);
}

const publicationVersion = `gdi_pub_${HOTEL_ID}_${new Date()
  .toISOString()
  .slice(0, 10)
  .replace(/-/g, "")}_exa_historical_recovery_v1`;
const pkg = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: uniqueCustomer,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    recovery: "gdi-sheraton-exa-historical-research-recovery-v1",
    baselineV1Fingerprint: V1_FP,
    priorV2Fingerprint: V2_FP,
    paidResearchUsd: 0,
    templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  },
});
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error("PACKAGE_INTEGRITY_FAILED", integrity.fails);
  process.exit(2);
}
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: V2_VERSION,
    contentFingerprintSha256: V2_FP,
    alsoPreservesV1: { publicationVersion: V1_VERSION, fingerprint: V1_FP },
  },
});

fs.writeFileSync(
  path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json",
  ),
  JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      updatedAt: new Date().toISOString(),
      source: "gdi-sheraton-exa-historical-research-recovery-v1",
      opportunities: uniqueCustomer,
    },
    null,
    2,
  ) + "\n",
);

let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: uniqueCustomer,
  });
  airtablePlan = {
    configured: true,
    dryRun: true,
    writes: 0,
    ok: sync.ok,
    airtableCountExisting: sync.airtableCount,
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
    authorizationRequired: true,
  };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}

const actionCounts = {};
const filterCounts = {};
for (const o of uniqueCustomer) {
  actionCounts[o.salesActionPillLabel] =
    (actionCounts[o.salesActionPillLabel] || 0) + 1;
  for (const k of o.salesOpportunityQuickFilterKeys || []) {
    filterCounts[k] = (filterCounts[k] || 0) + 1;
  }
}

const coverageCounts = mappings.reduce((m, x) => {
  m[x.coverage] = (m[x.coverage] || 0) + 1;
  return m;
}, {});

writeJson("03-62_OPPORTUNITY_EVIDENCE_MAPPING.json", {
  baselineFingerprint: V1_FP,
  count: mappings.length,
  coverageCounts,
  mappings,
});
writeJson("04-HISTORICAL_SOURCE_FRESHNESS.json", {
  reviewDate: REVIEW_DATE,
  rows: freshnessRows,
});
writeJson("05-DEMAND_THESIS_RECONSTRUCTION.json", { rows: thesisRows });
writeJson("06-CONTACT_AND_BUYER_REVIEW.json", {
  withContact: contactRows.filter((r) => r.contact).length,
  rows: contactRows,
  salesReady: 0,
  note: "Zero Sales Ready — recovered Exa packs improve evidence but rarely prove lodging buyer + open sourcing.",
});
writeJson("07-MARKET_SCOPE_CLASSIFICATIONS.json", {
  hotel: uniqueCustomer.filter((o) => o.hotelMarketScopeEligible).length,
  competitive: 0,
  rows: marketRows,
});
writeJson("08-SALES_OPPORTUNITY_CLASSIFICATIONS.json", {
  filterCounts,
  rows: salesRows,
});
writeJson("09-COMMERCIAL_ACTION_REVIEW.json", {
  actionCounts,
  rows: actionRows,
});
writeJson("10-SEMANTIC_QUALITY_REVIEW.json", {
  assessed: 62,
  customerVisible: uniqueCustomer.length,
  researchStage: researchStage.length,
  rows: qualityRows,
  researchStage,
});
writeJson("11-AIRTABLE_UPSERT_DRY_RUN.json", airtablePlan);
writeJson("12-BEFORE_AFTER_COMMERCIAL_SCORECARD.json", {
  v1: {
    fingerprint: V1_FP,
    customerVisible: 62,
    marketScopeHotel: 0,
    salesFilters: false,
    buildAccountOnly: 62,
  },
  v2Remediation: {
    fingerprint: V2_FP,
    customerVisible: v2.ok ? v2.package.opportunityCount : null,
  },
  v3ExaRecovery: {
    fingerprint: pkg.contentFingerprintSha256,
    customerVisible: uniqueCustomer.length,
    researchStage: researchStage.length,
    coverageCounts,
    narrativesImproved: thesisRows.filter((r) => r.whatImproved).length,
    usedExaClaims: thesisRows.filter((r) => r.usedExaClaims).length,
    marketScopeHotel: uniqueCustomer.filter((o) => o.hotelMarketScopeEligible)
      .length,
    actionCounts,
    filterCounts,
    salesReady: 0,
    inventoryCounts: pkg.inventoryCounts,
  },
});
writeJson("13-NEW_STAGED_PUBLICATION.json", {
  status: "STAGED_FOR_FOUNDER_REVIEW",
  publicationVersion,
  fingerprint: pkg.contentFingerprintSha256,
  opportunityCount: uniqueCustomer.length,
  inventoryCounts: pkg.inventoryCounts,
  preservesV1: { version: V1_VERSION, fingerprint: V1_FP },
  supersedesV2: { version: V2_VERSION, fingerprint: V2_FP },
  packagePath: written.path,
});
writeJson("14-LOCAL_BROWSER_VERIFICATION.json", {
  status: "PENDING_BROWSER_SCRIPT",
  previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
  expectedFingerprint: pkg.contentFingerprintSha256,
  expectedCount: uniqueCustomer.length,
});
writeJson("15-REMAINING_RESEARCH_GAPS_AND_BUDGET.json", {
  paidResearchExecutedUsd: 0,
  historicalPaidAlreadySpentUsd: provenance?.paidSpendUsd ?? null,
  rawHttpBodiesMissing: true,
  gaps: [
    "Standalone raw Exa HTTP response archives not on disk — packs embedded in dossiers only",
    "Competitive set still unknown",
    "Need dates unknown",
    "Sales Ready buyers / open RFPs still rare",
    ...mappings
      .filter((m) => m.coverage === "NO_RECOVERABLE_EXA" || m.coverage === "DOSSIER_ONLY")
      .slice(0, 15)
      .map((m) => ({ opportunityId: m.id, coverage: m.coverage, title: m.title })),
  ],
  suggestedAdditionalBudgetUsd: 35,
  authorizationRequired: true,
});
writeJson("16-GITHUB_STATUS.json", {
  branch: "rem/gdi-sheraton-exa-historical-research-recovery-v1",
  headSha: sh("git rev-parse HEAD"),
  noBethesdaMutation: true,
  noCustomerPublished: true,
  noPaidResearch: true,
});

const founder = `# Sheraton Guadalajara Expo — Exa Historical Research Recovery V1

**Template:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\`  
**V1 baseline (preserved):** \`${V1_FP}\`  
**V2 remediation (preserved):** \`${V2_FP}\`  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**Version:** \`${publicationVersion}\`  
**Paid research this run:** **$0**

## Can historical Exa alone reach Bethesda quality?

**PARTIALLY — structural + evidence-backed narrative upgrades yes; full Bethesda commercial depth not yet.**

Recoverable intelligence is strong in **embedded Exa evidence packs** (claims/sources/excerpts) inside research dossiers (**${artifacts.filter((a) => a.kind === "EXA_EVIDENCE_PACK").length}** packs indexed).  
**Standalone raw Exa HTTP response bodies were not found** on disk — provenance ledgers reference \`providerRunId\`s but response blobs are not separately archived.

### Coverage of original 62
${Object.entries(coverageCounts)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

### Before → After (customer surface)
| Metric | V1 | V3 (this recovery) |
|--------|---:|-------------------:|
| Customer-visible | 62 | ${uniqueCustomer.length} |
| Research-stage | 0 | ${researchStage.length} |
| Market Scope Hotel | 0 | ${uniqueCustomer.filter((o) => o.hotelMarketScopeEligible).length} |
| Narratives improved from generic | — | ${thesisRows.filter((r) => r.whatImproved).length} |
| Used Exa claims in thesis | — | ${thesisRows.filter((r) => r.usedExaClaims).length} |
| Sales Ready | 0 | **0** |

### Commercial actions
${Object.entries(actionCounts)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

### Sales Opportunities keys
${Object.entries(filterCounts)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Sales Ready

Still **0**. Recovered Exa packs improve evidence and Qualify/Partnership differentiation, but do not generally prove lodging buyer authority + open hotel sourcing. This remains an **evidence limitation**, not a silent UI bottleneck.

## Preview
\`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged\`

Restart local server if it still serves V2 fingerprint.

## Airtable
Dry-run only — authorize before writes.

## Remaining blockers / budget
See \`15-REMAINING_RESEARCH_GAPS_AND_BUDGET.json\`. Suggested **$35** not authorized. Prefer verifying contacts/sourcing for COMPLETE_HISTORICAL_PACK opportunities before new discovery.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder, "utf8");

console.log(
  JSON.stringify(
    {
      v1Fp: V1_FP,
      newFp: pkg.contentFingerprintSha256,
      customerVisible: uniqueCustomer.length,
      researchStage: researchStage.length,
      coverageCounts,
      actionCounts,
      filterCounts,
      exaPacks: artifacts.filter((a) => a.kind === "EXA_EVIDENCE_PACK").length,
      out: OUT,
    },
    null,
    2,
  ),
);
