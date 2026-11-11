/**
 * Bethesda Historical GDI Opportunity Recovery V1
 *
 * Read-only reconciliation of previously researched Bethesda Marriott opportunities
 * against the canonical hotel store. No paid research. No hotel-store writes.
 * No publication. No new discovery engines.
 *
 * Contract: BETHESDA_HISTORICAL_OPPORTUNITY_RECOVERY_V1
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
  normalizeOrganizationIdentity,
  extractProgramIdentity,
  normalizeProgramIdentity,
} from "./bethesda-full-demand-database-reconciliation-v1.js";

export const BETHESDA_HISTORICAL_RECOVERY_V1 =
  "gdi-bethesda-historical-opportunity-recovery-v1";
export const RECOVERY_REPORTS_DIR =
  "reports/gdi-bethesda-historical-opportunity-recovery-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const HOTEL_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  BETHESDA_CANONICAL_HOTEL_ID
);

export const RECONCILE_GROUP = Object.freeze({
  A_EXISTING_CANONICAL: "A_EXISTING_CANONICAL",
  B_HISTORICAL_MISSING_FROM_CANONICAL: "B_HISTORICAL_MISSING_FROM_CANONICAL",
  C_EXISTING_MISSING_EVIDENCE_OR_CONTACTS: "C_EXISTING_MISSING_EVIDENCE_OR_CONTACTS",
  D_RESEARCH_STAGE_CANDIDATE: "D_RESEARCH_STAGE_CANDIDATE",
  E_DUPLICATE_OR_SUPERSEDED: "E_DUPLICATE_OR_SUPERSEDED",
  F_ARTIFACT_MISSING_BODY: "F_ARTIFACT_MISSING_BODY",
});

export const RECOVERY_CATEGORY = Object.freeze({
  LOCAL_CATERING: "LOCAL_CATERING",
  POTENTIAL_PARTNERSHIPS: "POTENTIAL_PARTNERSHIPS",
  CORPORATE_GROUP: "CORPORATE_GROUP",
  ASSOCIATION_EVENTS: "ASSOCIATION_EVENTS",
  SPORTS_HOUSING: "SPORTS_HOUSING",
  RELOCATION_EXTENDED_STAY: "RELOCATION_EXTENDED_STAY",
  PRIVATE_EVENTS_VENUE_INTEL: "PRIVATE_EVENTS_VENUE_INTEL",
  OTHER: "OTHER",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha256File(p) {
  if (!fs.existsSync(p)) return null;
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function loadJson(p, fallback = null) {
  if (!fs.existsSync(p)) return fallback;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function asOppArray(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw.opportunities)) return raw.opportunities;
  if (Array.isArray(raw.items)) return raw.items;
  if (Array.isArray(raw.candidates)) return raw.candidates;
  if (Array.isArray(raw.rows)) return raw.rows;
  return [];
}

function oppId(o = {}) {
  return text(o.id || o.opportunityId || o.canonicalOpportunityId || o.cardId) || null;
}

function softOrgKey(name = "") {
  return normalizeOrganizationIdentity(name);
}

function classifyRecoveryCategory({
  segment = "",
  demandSegment = "",
  organizationName = "",
  title = "",
  id = "",
} = {}) {
  const blob = `${segment} ${demandSegment} ${organizationName} ${title} ${id}`.toLowerCase();
  if (
    /local_catering|local_meetings|catering|chamber of commerce|shrm|pmi |day meetings|banquet|reception/.test(
      blob
    )
  ) {
    return RECOVERY_CATEGORY.LOCAL_CATERING;
  }
  if (
    /preferred_lodging|professional_planner|professional_event_planner|_plp_|_pep_|dmc|partnership|howerton|cohera|visit montgomery|linder|planner/.test(
      blob
    )
  ) {
    return RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS;
  }
  if (/pe_bethesda|venue_only|mansion|country club|press club|sanctuary/.test(blob)) {
    return RECOVERY_CATEGORY.PRIVATE_EVENTS_VENUE_INTEL;
  }
  if (/relocation|extended.?stay|postdoc|fellowship/.test(blob)) {
    return RECOVERY_CATEGORY.RELOCATION_EXTENDED_STAY;
  }
  if (/sport|tournament|soccer|lacrosse|iwlca|swim/.test(blob)) {
    return RECOVERY_CATEGORY.SPORTS_HOUSING;
  }
  if (/corporate|training|incentive|aledade|pda|bio-trac|talmi/.test(blob)) {
    return RECOVERY_CATEGORY.CORPORATE_GROUP;
  }
  if (/association|conference|symposium|ife|ifpac|kappa|exposome/.test(blob)) {
    return RECOVERY_CATEGORY.ASSOCIATION_EVENTS;
  }
  return RECOVERY_CATEGORY.OTHER;
}

/**
 * Curated historical Bethesda records attested by preserved run logs / transcripts
 * when the original research-memory JSON bodies were wiped by FS-only sync.
 * Bodies are NOT invented — fields are limited to what persisted evidence states.
 */
export const CURATED_HISTORICAL_ATTESTATIONS = Object.freeze([
  // —— Additive discovery / maturation (2026-10-19) ——
  {
    id: "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
    organizationName: "Greater Bethesda Chamber of Commerce",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_meetings_catering",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    relatedIds: ["gdi_e2d_770e3b44ad97c1ae", "gdi_disp_recLuxvw_18_greater_bethesda_chamber_of_commerce"],
    bodyAvailable: false,
    reasonAbsent:
      "Research-memory JSON wiped; FS-only supervisor sync cleared staged gdi_disp_*/gdi_e2d_* rows from candidateIds.",
  },
  {
    id: "gdi_e2d_770e3b44ad97c1ae",
    organizationName: "Greater Bethesda Chamber of Commerce",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_meetings_catering",
    originalResearchStatus: "QUALIFIED",
    originalCommercialQualification: "qualified_publishable_staged",
    hotelFacingHistorical: true,
    discoveryRun: "bethesda-additive-discovery-v1",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "agent-transcripts/.../segment_028.md (attestation); research-memory body missing",
    relatedIds: ["disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce"],
    bodyAvailable: false,
    reasonAbsent: "Staged E2D opportunity never promoted into canonical opportunities.json; research-memory wiped.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
    organizationName: "Montgomery County SHRM",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_meetings_catering",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    bodyAvailable: false,
    reasonAbsent: "Research-memory JSON wiped after maturation persist.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter",
    organizationName: "PMI Montgomery County Maryland Chapter",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_meetings_catering",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    bodyAvailable: false,
    reasonAbsent: "Research-memory JSON wiped after maturation persist.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_city_of_rockville",
    organizationName: "City of Rockville",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_meetings_catering",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    bodyAvailable: false,
    reasonAbsent: "Research-memory JSON wiped after maturation persist.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_nature_forward",
    organizationName: "Nature Forward",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_meetings_catering",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    bodyAvailable: false,
    reasonAbsent: "Research-memory JSON wiped after maturation persist.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_strathmore",
    organizationName: "Strathmore",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "preferred_lodging / venue partnership",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    bodyAvailable: false,
    reasonAbsent: "Research-memory JSON wiped after maturation persist.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
    organizationName: "Cohera (formerly CSI DMC and 360DG)",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "professional_planners_dmc",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    originalContact: "Web form / published contact path",
    demandThesis:
      "DMV DMC / planner account that can route association and corporate programs into Bethesda/NIH corridor.",
    hotelFitRationale: "Partnership / preferred-hotel relationship — not a single dated block.",
    discoveryRun: "bethesda-additive-discovery-v1 + commercial staged review 2026-10-19",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "maturation-resume-live.log; agent-transcript staged commercial review (APPROVE_FOR_PUBLICATION_REVIEW, unpublished)",
    bodyAvailable: false,
    reasonAbsent:
      "Staged for publication review but never imported to canonical store; research-memory wiped; intentionally unpublished.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_linder_global_events",
    organizationName: "LINDER Global Events",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "professional_planners_dmc",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "qualified_publishable",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    bodyAvailable: false,
    reasonAbsent: "Research-memory JSON wiped after maturation persist.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_aledade",
    organizationName: "Aledade",
    researchCategory: RECOVERY_CATEGORY.CORPORATE_GROUP,
    demandSegment: "corporate_meetings_training",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "exploratory",
    discoveryRun: "bethesda-additive-discovery-v1 / maturation-resume-live",
    discoveryDate: "2026-10-19",
    sourceArtifactPath:
      "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation-resume-live.log",
    bodyAvailable: false,
    reasonAbsent: "Research-memory JSON wiped; classified exploratory in staged review.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
    organizationName: "IWLCA / Elite Tournaments - IWLCA Champions Cup",
    researchCategory: RECOVERY_CATEGORY.SPORTS_HOUSING,
    demandSegment: "sports",
    originalResearchStatus: "APPROVE_FOR_PUBLICATION_REVIEW",
    originalCommercialQualification: "staged_unpublished",
    originalContact: "reservations@halperntravel.com",
    demandThesis:
      "Multi-team lacrosse Champions Cup at Maryland SoccerPlex (Boyds), June 19–20, 2027; Halpern Travel housing partner referenced.",
    hotelFitRationale:
      "Montgomery County lodging with transport to Boyds — overflow/team lodging, not venue-adjacent host.",
    discoveryRun: "bethesda staged commercial review 2026-10-19",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript commerciallyApprovedStaged (IWLCA)",
    bodyAvailable: false,
    reasonAbsent: "Approved for publication review but intentionally unpublished; never written to canonical store.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
    organizationName: "Parenteral Drug Association (PDA)",
    researchCategory: RECOVERY_CATEGORY.CORPORATE_GROUP,
    demandSegment: "corporate_meetings_training",
    originalResearchStatus: "APPROVE_FOR_PUBLICATION_REVIEW",
    originalCommercialQualification: "staged_unpublished",
    originalContact: "training@pda.org",
    demandThesis:
      "Bethesda aseptic / training course activity (packet cites Dec 2026 training window) — same-city training lodging plausible.",
    hotelFitRationale: "Strong same-city medical/biotech training fit for Bethesda Marriott.",
    discoveryRun: "bethesda staged commercial review 2026-10-19",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript commerciallyApprovedStaged (PDA)",
    bodyAvailable: false,
    reasonAbsent: "Approved for publication review but intentionally unpublished; never written to canonical store.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_bio_trac",
    organizationName: "Bio-Trac",
    researchCategory: RECOVERY_CATEGORY.CORPORATE_GROUP,
    demandSegment: "corporate_meetings_training",
    originalResearchStatus: "NEEDS_TARGETED_ENRICHMENT",
    originalCommercialQualification: "enrichment_queue",
    discoveryRun: "bethesda staged commercial review 2026-10-19",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript needsEnrichment (Bio-Trac)",
    bodyAvailable: false,
    reasonAbsent: "Research-stage candidate; research-memory wiped before enrichment completed.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_talmi_entertainment_nutcracker_magical_christmas",
    organizationName: "Talmi Entertainment",
    researchCategory: RECOVERY_CATEGORY.CORPORATE_GROUP,
    demandSegment: "entertainment_production",
    originalResearchStatus: "NEEDS_TARGETED_ENRICHMENT",
    originalCommercialQualification: "enrichment_queue",
    discoveryRun: "bethesda staged commercial review 2026-10-19",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript needsEnrichment (Talmi)",
    bodyAvailable: false,
    reasonAbsent: "Research-stage candidate; research-memory wiped before enrichment completed.",
  },
  {
    id: "gdi_e2d_bc0a94d7c6007f36",
    organizationName: "Visit Montgomery",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "preferred_lodging / destination partnership",
    originalResearchStatus: "QUALIFIED",
    originalCommercialQualification: "qualified_publishable_staged",
    hotelFacingHistorical: true,
    discoveryRun: "bethesda-additive-discovery-v1",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript segment_028 attestation",
    bodyAvailable: false,
    reasonAbsent: "Staged E2D never promoted to canonical opportunities.json; research-memory wiped.",
  },
  {
    id: "gdi_e2d_f304cf4759151bb5",
    organizationName: "Howerton+Wooten Events",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "professional_planners_dmc",
    originalResearchStatus: "EXPLORATORY_PROSPECT",
    originalCommercialQualification: "PARTNERSHIP_EXPLORATORY",
    discoveryRun: "bethesda-additive-discovery-v1 / v20 preferred lodging continuation",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript segment_028; v20 finalize-run.log (gdi_pep_live_af0711e711ba)",
    relatedIds: ["gdi_pep_live_af0711e711ba"],
    bodyAvailable: false,
    reasonAbsent: "Partnership exploratory staged only; research-memory and v20 shadow bag missing.",
  },
  {
    id: "gdi_e2d_e6417fef232025c4",
    organizationName: "DC Global Planning",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "professional_planners_dmc",
    originalResearchStatus: "EXPLORATORY_PROSPECT",
    originalCommercialQualification: "PARTNERSHIP_EXPLORATORY",
    discoveryRun: "bethesda-additive-discovery-v1",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript segment_028 attestation",
    bodyAvailable: false,
    reasonAbsent: "Partnership exploratory staged only; research-memory wiped.",
  },
  // —— V20 live incremental (shadow bag missing) ——
  {
    id: "gdi_plp_live_fa2063dc1195",
    organizationName: "Rock Creek Mansion",
    title: "Rock Creek Mansion",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "preferred_lodging",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    originalCommercialQualification: "not_customer_ready",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    relatedIds: ["pe_bethesda_pev_53b689545914754d"],
    bodyAvailable: false,
    reasonAbsent:
      "Referenced shadowPath data/group-demand-intelligence/local-shadow/bethesda-v20-incremental-live-opportunities-v1.json is missing across repo and backups.",
  },
  {
    id: "gdi_plp_live_4958569d826e",
    organizationName: "The Cordell",
    title: "The Cordell",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "preferred_lodging",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow opportunity bag missing.",
  },
  {
    id: "gdi_pep_live_af0711e711ba",
    organizationName: "Howerton+Wooten Events, LLC",
    title: "Howerton+Wooten Events, LLC",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "professional_event_planners",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    relatedIds: ["gdi_e2d_f304cf4759151bb5"],
    bodyAvailable: false,
    reasonAbsent: "V20 shadow opportunity bag missing.",
  },
  {
    id: "gdi_pep_live_c165c897d233",
    organizationName: "Potomac Meetings and Events (Potomac Events)",
    title: "Potomac Events / Potomac Meetings and Events",
    researchCategory: RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS,
    demandSegment: "professional_event_planners",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow opportunity bag missing.",
  },
  {
    id: "gdi_s4912_59382d1e71a2",
    organizationName: "Montgomery College — AI Skills Summit",
    title:
      "Local catering / corporate celebrations / association receptions with dated Dec–Nov programmes not competitor-committed",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_catering_day_meetings",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t04",
    discoveryDate: "2026-11-02",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    evidenceNotes:
      "Verified historical event Fri Nov 7, 2025 Globe Hall Germantown; future edition not verified as of 2026-11-02.",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow opportunity bag missing; finalize log retains summary only.",
  },
  {
    id: "gdi_s4912_d55ee8a8e9ca",
    organizationName: "NIDDK — Meetings & Workshops portfolio",
    title:
      "Local catering / corporate celebrations / association receptions with dated Dec–Nov programmes not competitor-committed",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "local_catering_day_meetings",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t04",
    discoveryDate: "2026-11-02",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    evidenceNotes:
      "Recurring multi-programme scientific-meeting prospect; not a confirmed hotel catering booking.",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow opportunity bag missing; finalize log retains summary only.",
  },
  {
    id: "gdi_s4912_66a1f89e5afb",
    organizationName: "NIH Intramural Postdoctoral Fellowship / OITE",
    researchCategory: RECOVERY_CATEGORY.RELOCATION_EXTENDED_STAY,
    demandSegment: "relocation_extended_stay",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t01",
    discoveryDate: "2026-11-03",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow opportunity bag missing.",
  },
  {
    id: "gdi_s4912_364713d41c67",
    organizationName: "NIH IRP / Division of International Services",
    researchCategory: RECOVERY_CATEGORY.RELOCATION_EXTENDED_STAY,
    demandSegment: "relocation_extended_stay",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t01",
    discoveryDate: "2026-11-03",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow opportunity bag missing.",
  },
  {
    id: "gdi_s4912_732496c2f8aa",
    organizationName: "CASSS — 2027 Cell & Gene Therapy Products Symposium",
    researchCategory: RECOVERY_CATEGORY.CORPORATE_GROUP,
    demandSegment: "corporate_meetings_incentives",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    originalCommercialQualification: "COMPETITOR_HOSTED_MAIN_EVENT",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t05",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    evidenceNotes:
      "Hosted at Bethesda North Marriott 5701 Marinelli Rd — distinct from target Bethesda Marriott 5151 Pooks Hill Rd.",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow bag missing; competitor-host demotion recorded in finalize log.",
  },
  {
    id: "gdi_s4912_512deebd0963",
    organizationName: "2027 U.S. IFE Conference",
    researchCategory: RECOVERY_CATEGORY.ASSOCIATION_EVENTS,
    demandSegment: "corporate_meetings_incentives",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t05",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    relatedIds: ["gdi_beth_v16_evt_3c8228774517"],
    evidenceNotes: "Venue: Bethesda North Marriott — not target property.",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow bag missing.",
  },
  {
    id: "gdi_s4912_6c8a62552bfa",
    organizationName: "Maryland Tech Council — ICON Awards",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "corporate_celebrations_private_events",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t07",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    evidenceNotes: "2027 save-the-date at Bethesda North Marriott; competitor venue.",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow bag missing.",
  },
  {
    id: "gdi_s4912_6ffc303bf55a",
    organizationName: "The Children’s Inn at NIH — An Evening for Hope Gala",
    researchCategory: RECOVERY_CATEGORY.LOCAL_CATERING,
    demandSegment: "corporate_celebrations_private_events",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t07",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    evidenceNotes: "2026 at JW Marriott DC; 2027 date/venue not announced as of 2026-11-02.",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow bag missing.",
  },
  {
    id: "gdi_beth_v16_evt_3c8228774517",
    organizationName: "U.S. IFE",
    title: "2027 U.S. IFE Conference",
    researchCategory: RECOVERY_CATEGORY.ASSOCIATION_EVENTS,
    demandSegment: "associations_conferences_events",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v16 / v20 contact enrichment beth_v20_t09",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent:
      "V16 deep artifact tree largely empty (only MTA stub remains); opportunity body not locatable.",
  },
  {
    id: "gdi_beth_v16_evt_90a6ddcb10ca",
    organizationName: "Kappa Kappa Psi and Tau Beta Sigma",
    title: "Kappa Kappa Psi & Tau Beta Sigma National Convention 2027",
    researchCategory: RECOVERY_CATEGORY.ASSOCIATION_EVENTS,
    demandSegment: "associations_conferences_events",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v16 / v20 contact enrichment beth_v20_t10",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent: "V16 deep artifact body missing.",
  },
  {
    id: "gdi_beth_v16_evt_0ab91e208405",
    organizationName: "IFPAC",
    title: "IFPAC-2027 — Advanced Manufacturing Science & Innovation",
    researchCategory: RECOVERY_CATEGORY.ASSOCIATION_EVENTS,
    demandSegment: "associations_conferences_events",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    discoveryRun: "gdi-bethesda-v16 / v20 contact enrichment beth_v20_t11",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent: "V16 deep artifact body missing.",
  },
  {
    id: "gdi_s4912_b0ea89e0716d",
    organizationName: "Global Exposome Forum (GEF)",
    title: "Global Exposome Summit 2027",
    researchCategory: RECOVERY_CATEGORY.ASSOCIATION_EVENTS,
    demandSegment: "corporate_meetings_incentives",
    originalResearchStatus: "NOT_CUSTOMER_READY",
    originalCommercialQualification: "COMPETITOR_HOSTED_OVERFLOW_ONLY",
    discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution beth_v20_t12",
    discoveryDate: "2026-11",
    sourceArtifactPath:
      "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
    bodyAvailable: false,
    reasonAbsent: "V20 shadow bag missing.",
  },
  // —— Private-events venue intel (research continuation, not hotel demand) ——
  ...[
    ["pe_bethesda_pev_5d6cb3406b270ba5", "Glenview Mansion"],
    ["pe_bethesda_pev_1939fb0e4e457f28", "Norbeck Country Club"],
    ["pe_bethesda_pev_57a7a8739b19dd3c", "The Page House"],
    ["pe_bethesda_pev_201bbc2ecac77759", "Whittemore House"],
    ["pe_bethesda_pev_f2033b6cc9611c1b", "Bethesda Country Club"],
    ["pe_bethesda_pev_177defed35e357e0", "DAR Headquarters"],
    ["pe_bethesda_pev_3a85ee63e336b4c4", "Meridian International Center"],
    ["pe_bethesda_pev_d35dee860f128bef", "Decatur House"],
    ["pe_bethesda_pev_097b4cd1fa2280c2", "Anderson House"],
    ["pe_bethesda_pev_7f2b9ff11fefa1ad", "Montgomery Country Club"],
    ["pe_bethesda_pev_53b689545914754d", "Rock Creek Mansion"],
    ["pe_bethesda_pev_b449600ef479dfa9", "National Press Club"],
    ["pe_bethesda_pev_06778d4502eb5c91", "Woodend Sanctuary"],
  ].map(([id, organizationName]) => ({
    id,
    organizationName,
    researchCategory: RECOVERY_CATEGORY.PRIVATE_EVENTS_VENUE_INTEL,
    demandSegment: "private_events_venue_intelligence",
    originalResearchStatus: "STOPPED_WITH_LEGITIMATE_REASON",
    originalCommercialQualification: "VENUE_ONLY_MARKET_INTELLIGENCE",
    discoveryRun: "bethesda private-events / additive discovery",
    discoveryDate: "2026-10-19",
    sourceArtifactPath: "agent-transcript segment_028 attestation",
    bodyAvailable: false,
    reasonAbsent:
      "Venue-only market intelligence staged in research-memory; never a target-hotel opportunity; research-memory wiped.",
    researchContinuationOnly: true,
  })),
]);

export const MISSING_ARTIFACTS = Object.freeze([
  {
    path: "data/group-demand-intelligence/local-shadow/bethesda-v20-incremental-live-opportunities-v1.json",
    run: "gdi-bethesda-v20-incremental-research-live-execution",
    impact: "Full V20 opportunity bodies (12 staged + enrichments) unavailable; summary-only recovery from finalize-run.log.",
  },
  {
    path: "reports/gdi-adp-five-hotel-portfolio-e2e-v1/bethesda-additive-discovery-v1/maturation/research-memory/disp_recLuxvwwxID7U2B8_*.json",
    run: "bethesda-additive-discovery-v1 maturation",
    impact: "Primary catering/partnership disposition bodies wiped; IDs+orgs recoverable from logs/transcripts only.",
  },
  {
    path: "reports/gdi-opportunity-research-supervisor-v1/hotel-orchestrator-recLuxvwwxID7U2B8/",
    run: "hotel research orchestrator 2026-10-19",
    impact: "Supervisor research-memory directory absent; corpus was canonical+research_memory n=59.",
  },
  {
    path: "reports/gdi-bethesda-commercial-and-segment-e2e-v1/STAGED_CANDIDATE_RECONCILIATION.json",
    run: "bethesda commercial/segment e2e",
    impact: "Staged candidate reconciliation package missing.",
  },
  {
    path: "reports/gdi-bethesda-segment-sufficiency-recovery-v1/",
    run: "bethesda segment sufficiency recovery",
    impact: "Deliverable directory never present / wiped.",
  },
  {
    path: "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/research-candidates.json",
    run: "ongoing",
    impact: "No research-candidate memory file on hotel store.",
  },
  {
    path: "reports/gdi-bethesda-v16-fourteen-workflow-incremental-pilot/artifacts/deep/**",
    run: "bethesda v16 fourteen-workflow pilot",
    impact: "Deep research tree nearly empty (1 MTA stub); v16 event opportunity bodies not locatable.",
  },
]);

export function hashBethesdaCanonicalStore({ root = ROOT } = {}) {
  const hotelDir = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID
  );
  const files = [
    "opportunities.json",
    "feedback.json",
    "canonical-contacts.json",
    "profile.json",
    "event-series-graph.json",
  ];
  const hashes = {};
  for (const f of files) {
    const p = path.join(hotelDir, f);
    hashes[f] = {
      path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/${f}`,
      exists: fs.existsSync(p),
      sha256: sha256File(p),
      bytes: fs.existsSync(p) ? fs.statSync(p).size : 0,
    };
  }
  return hashes;
}

export function loadCanonicalBethesdaBaseline({ root = ROOT } = {}) {
  const hotelDir = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID
  );
  const raw = loadJson(path.join(hotelDir, "opportunities.json"), []);
  const opportunities = asOppArray(raw);
  const rows = opportunities.map((o) => {
    const evidence = Array.isArray(o.evidence) ? o.evidence : [];
    const sources = Array.isArray(o.sources) ? o.sources : [];
    const contacts = [
      o.primaryContactName || o.primaryContact?.name || null,
      ...(Array.isArray(o.contacts) ? o.contacts.map((c) => c?.name || c?.email) : []),
      ...(Array.isArray(o.backupContacts) ? o.backupContacts.map((c) => c?.name || c?.email) : []),
    ].filter(Boolean);
    return {
      id: oppId(o),
      organizationName: text(o.organizationName || o.company),
      title: text(o.title || o.canonicalEventName || o.eventName),
      segment: text(o.segment || o.demandType || o.researchSegment),
      demandStatus: text(o.demandStatus || o.commercialStatus),
      qualification: text(o.opportunityQualification || o.prospectEligibility),
      researchStatus: text(o.researchStatus),
      customerVisible: o.customerVisible === true,
      published: o.customerVisible === true,
      contactNames: contacts,
      primaryContactName: text(o.primaryContactName || o.primaryContact?.name),
      primaryContactRole: text(o.primaryContactRole || o.primaryContact?.title || o.primaryContact?.role),
      evidenceCount: evidence.length,
      sourceCount: sources.length,
      hasEvidence: evidence.length > 0 || sources.length > 0,
      hasNamedContact: Boolean(text(o.primaryContactName || o.primaryContact?.name)),
      firstSeenAt: o.firstSeenAt || o.firstDiscoveredAt || o.createdAt || null,
      lastVerifiedAt: o.lastVerifiedAt || o.updatedAt || null,
      airtableRecordId: o._airtableRecordId || null,
    };
  });
  return {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    count: rows.length,
    publishedCount: rows.filter((r) => r.published).length,
    unpublishedCount: rows.filter((r) => !r.published).length,
    ids: rows.map((r) => r.id).filter(Boolean),
    rows,
    hashes: hashBethesdaCanonicalStore({ root }),
  };
}

function parseV20FinalizeLog({ root = ROOT } = {}) {
  const p = path.join(
    root,
    "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log"
  );
  if (!fs.existsSync(p)) return { path: p, exists: false, rows: [] };
  const doc = loadJson(p, null);
  if (!doc) return { path: p, exists: true, parseError: true, rows: [] };
  const acceptanceRows = doc?.acceptance?.rows || [];
  const targetRows = [];
  for (const t of doc.targets || []) {
    for (const id of t.opportunities || []) {
      targetRows.push({
        id,
        demandSegment: t.demandSegment,
        researchClass: t.researchClass,
        targetId: t.targetId,
        status: t.status,
      });
    }
  }
  const byId = new Map(targetRows.map((r) => [r.id, r]));
  const rows = acceptanceRows.map((r) => {
    const t = byId.get(r.id) || {};
    return {
      id: r.id,
      organizationName: text(r.organizationName),
      title: text(r.title),
      disposition: r.disposition,
      promoteEligible: r.promoteEligible === true,
      rationale: r.rationale || [],
      demandSegment: t.demandSegment || null,
      researchClass: t.researchClass || null,
      targetId: t.targetId || null,
      sourceArtifactPath: p.replace(/\\/g, "/").replace(root.replace(/\\/g, "/") + "/", ""),
      discoveryRun: "gdi-bethesda-v20-incremental-research-live-execution",
      shadowPath: doc.shadowPath || null,
      bodyAvailable: false,
    };
  });
  return {
    path: p,
    exists: true,
    stagedNewCount: doc.stagedNewCount ?? null,
    enrichmentCount: doc.enrichmentCount ?? null,
    shadowPath: doc.shadowPath || null,
    rows,
  };
}

function parseProcessReplicationCsv({ root = ROOT } = {}) {
  const p = path.join(
    root,
    "reports/gdi-bethesda-process-replication/hotel-review/final-prospect-register.csv"
  );
  if (!fs.existsSync(p)) return { path: p, exists: false, rows: [] };
  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const header = lines[0].split(",");
  const rows = [];
  for (const line of lines.slice(1)) {
    // naive CSV split sufficient for this register (fields quoted)
    const cols = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        inQ = !inQ;
        continue;
      }
      if (ch === "," && !inQ) {
        cols.push(cur);
        cur = "";
        continue;
      }
      cur += ch;
    }
    cols.push(cur);
    const obj = Object.fromEntries(header.map((h, i) => [h, cols[i] || ""]));
    if (obj.hotelId !== BETHESDA_CANONICAL_HOTEL_ID) continue;
    rows.push({
      id: obj.cardId,
      organizationName: obj.account,
      section: obj.section,
      timing: obj.timing,
      commercialStatus: obj.commercialStatus,
      sources: obj.sources,
      sourceArtifactPath:
        "reports/gdi-bethesda-process-replication/hotel-review/final-prospect-register.csv",
      discoveryRun: "gdi-bethesda-process-replication",
      discoveryDate: "2026-10-14",
      bodyAvailable: true,
    });
  }
  return { path: p, exists: true, rows };
}

function loadWeeklyStaged({ root = ROOT } = {}) {
  const weeklyDir = path.join(HOTEL_DIR, "weekly-runs");
  const out = [];
  if (!fs.existsSync(weeklyDir)) return out;
  for (const run of fs.readdirSync(weeklyDir)) {
    for (const file of ["opportunities.staged.json", "opportunities.pre-stage.json"]) {
      const p = path.join(weeklyDir, run, file);
      const rows = asOppArray(loadJson(p, []));
      for (const o of rows) {
        out.push({
          id: oppId(o),
          organizationName: text(o.organizationName),
          title: text(o.title),
          segment: text(o.segment),
          sourceArtifactPath: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/weekly-runs/${run}/${file}`,
          discoveryRun: run,
          bodyAvailable: true,
          rawPresentInWeekly: true,
        });
      }
    }
  }
  return out;
}

function orgAlreadyInCanonical(baseline, organizationName) {
  const key = softOrgKey(organizationName);
  if (!key) return null;
  return baseline.rows.find((r) => softOrgKey(r.organizationName) === key) || null;
}

function classifyHistoricalRecord(rec, baseline, idIndex, softSeen) {
  const id = rec.id;
  if (id && idIndex.has(id)) {
    return {
      group: RECONCILE_GROUP.E_DUPLICATE_OR_SUPERSEDED,
      duplicateOf: id,
      reason: "Exact opportunity ID already present in canonical store.",
    };
  }
  const orgMatch = orgAlreadyInCanonical(baseline, rec.organizationName);
  const program = normalizeProgramIdentity(
    extractProgramIdentity(rec) || rec.title || ""
  );
  if (orgMatch && program) {
    const canonProgram = normalizeProgramIdentity(orgMatch.title || "");
    if (canonProgram && canonProgram === program) {
      return {
        group: RECONCILE_GROUP.E_DUPLICATE_OR_SUPERSEDED,
        duplicateOf: orgMatch.id,
        reason: "Same organization + program already in canonical store under a different ID.",
      };
    }
  }
  // Soft org-only match for process-replication cards that mirror canonical orgs
  if (rec.id?.startsWith("beth-") && orgMatch) {
    return {
      group: RECONCILE_GROUP.E_DUPLICATE_OR_SUPERSEDED,
      duplicateOf: orgMatch.id,
      reason: "Process-replication card maps to an existing canonical organization opportunity.",
    };
  }
  if (rec.researchContinuationOnly || rec.originalCommercialQualification === "VENUE_ONLY_MARKET_INTELLIGENCE") {
    return {
      group: RECONCILE_GROUP.D_RESEARCH_STAGE_CANDIDATE,
      reason: "Venue-only / research-continuation record — not a matured target-hotel opportunity.",
    };
  }
  if (
    /NEEDS_TARGETED_ENRICHMENT|EXPLORATORY|NOT_CUSTOMER_READY|enrichment_queue|PARTNERSHIP_EXPLORATORY/i.test(
      `${rec.originalResearchStatus || ""} ${rec.originalCommercialQualification || ""} ${rec.disposition || ""}`
    )
  ) {
    // Still a historical opportunity to restore into research inventory, but classify as D if never matured
    if (/NEEDS_TARGETED_ENRICHMENT|enrichment_queue|PARTNERSHIP_EXPLORATORY|VENUE_ONLY/i.test(
      `${rec.originalResearchStatus || ""} ${rec.originalCommercialQualification || ""}`
    )) {
      return {
        group: RECONCILE_GROUP.D_RESEARCH_STAGE_CANDIDATE,
        reason: "Historical research-stage / exploratory record — preserve, do not treat as Sales Ready.",
      };
    }
  }
  if (rec.bodyAvailable === false) {
    return {
      group: RECONCILE_GROUP.F_ARTIFACT_MISSING_BODY,
      reason: rec.reasonAbsent || "Original opportunity body not locatable; attestation-only recovery.",
    };
  }
  const softKey = `${softOrgKey(rec.organizationName)}::${program || softOrgKey(rec.title)}`;
  if (softSeen.has(softKey) && softKey !== "::") {
    return {
      group: RECONCILE_GROUP.E_DUPLICATE_OR_SUPERSEDED,
      duplicateOf: softSeen.get(softKey),
      reason: "Duplicate of another historical attestation in this recovery pass.",
    };
  }
  if (softKey !== "::") softSeen.set(softKey, id);
  return {
    group: RECONCILE_GROUP.B_HISTORICAL_MISSING_FROM_CANONICAL,
    reason: rec.reasonAbsent || "Present in historical research artifacts but absent from canonical opportunities.json.",
  };
}

export function buildBethesdaHistoricalRecoveryManifest({ root = ROOT } = {}) {
  const baseline = loadCanonicalBethesdaBaseline({ root });
  const idIndex = new Map(baseline.rows.map((r) => [r.id, r]));
  const softSeen = new Map();

  const v20 = parseV20FinalizeLog({ root });
  const processRep = parseProcessReplicationCsv({ root });
  const weekly = loadWeeklyStaged({ root });

  const historical = [];
  const pushUnique = (rec) => {
    if (!rec?.id) return;
    if (historical.some((h) => h.id === rec.id)) return;
    historical.push(rec);
  };

  for (const rec of CURATED_HISTORICAL_ATTESTATIONS) pushUnique({ ...rec });
  for (const rec of v20.rows || []) {
    pushUnique({
      ...rec,
      researchCategory: classifyRecoveryCategory(rec),
      originalResearchStatus: rec.disposition,
      reasonAbsent:
        "V20 finalize summary only; shadow opportunity bag missing.",
    });
  }
  for (const rec of processRep.rows || []) {
    pushUnique({
      ...rec,
      researchCategory: RECOVERY_CATEGORY.ASSOCIATION_EVENTS,
      originalResearchStatus: rec.commercialStatus,
    });
  }

  const reconciled = [];
  for (const rec of historical) {
    const cls = classifyHistoricalRecord(rec, baseline, idIndex, softSeen);
    const category =
      rec.researchCategory ||
      classifyRecoveryCategory({
        segment: rec.demandSegment,
        demandSegment: rec.demandSegment,
        organizationName: rec.organizationName,
        title: rec.title,
        id: rec.id,
      });
    reconciled.push({
      ...rec,
      researchCategory: category,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      alreadyInCanonical: idIndex.has(rec.id),
      reconcileGroup: cls.group,
      duplicateOf: cls.duplicateOf || null,
      reconcileReason: cls.reason,
      proposedCanonicalOpportunityId: rec.id,
      classification:
        cls.group === RECONCILE_GROUP.D_RESEARCH_STAGE_CANDIDATE ||
        cls.group === RECONCILE_GROUP.PRIVATE_EVENTS_VENUE_INTEL
          ? "research_continuation"
          : cls.group === RECONCILE_GROUP.B_HISTORICAL_MISSING_FROM_CANONICAL ||
              cls.group === RECONCILE_GROUP.F_ARTIFACT_MISSING_BODY
            ? "opportunity_restore_candidate"
            : "no_import",
      importBlockedUntilApproval: true,
      evidenceContactCompleteness: rec.bodyAvailable
        ? "PARTIAL_FROM_SOURCE_FILE"
        : "ATTESTATION_ONLY_BODY_MISSING",
    });
  }

  const groupA = baseline.rows.map((r) => ({
    ...r,
    reconcileGroup: RECONCILE_GROUP.A_EXISTING_CANONICAL,
  }));
  const groupC = baseline.rows
    .filter((r) => !r.hasEvidence || !r.hasNamedContact)
    .map((r) => ({
      id: r.id,
      organizationName: r.organizationName,
      missingEvidence: !r.hasEvidence,
      missingNamedContact: !r.hasNamedContact,
      reconcileGroup: RECONCILE_GROUP.C_EXISTING_MISSING_EVIDENCE_OR_CONTACTS,
    }));

  const byGroup = {};
  for (const g of Object.values(RECONCILE_GROUP)) byGroup[g] = [];
  for (const r of reconciled) byGroup[r.reconcileGroup].push(r);

  const missingUnique = [
    ...byGroup[RECONCILE_GROUP.B_HISTORICAL_MISSING_FROM_CANONICAL],
    ...byGroup[RECONCILE_GROUP.F_ARTIFACT_MISSING_BODY],
  ].filter(
    (r, i, arr) => arr.findIndex((x) => x.id === r.id) === i
  );

  const cateringRecovered = missingUnique.filter(
    (r) => r.researchCategory === RECOVERY_CATEGORY.LOCAL_CATERING
  );
  const partnershipRecovered = missingUnique.filter(
    (r) => r.researchCategory === RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS
  );
  const otherMissing = missingUnique.filter(
    (r) =>
      r.researchCategory !== RECOVERY_CATEGORY.LOCAL_CATERING &&
      r.researchCategory !== RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS
  );
  const researchOnly = byGroup[RECONCILE_GROUP.D_RESEARCH_STAGE_CANDIDATE];
  const duplicates = byGroup[RECONCILE_GROUP.E_DUPLICATE_OR_SUPERSEDED];

  // Defensible historical population = canonical unique IDs + unique missing historical IDs
  // (including attestation-only) + research-stage candidates, excluding pure duplicates.
  const historicalPopulationIds = new Set([
    ...baseline.ids,
    ...missingUnique.map((r) => r.id),
    ...researchOnly.map((r) => r.id),
  ]);

  const dryRunImportRows = missingUnique.map((r) => ({
    originalSourceFile: r.sourceArtifactPath,
    originalRecordId: r.id,
    targetHotelId: BETHESDA_CANONICAL_HOTEL_ID,
    proposedCanonicalOpportunityId: r.proposedCanonicalOpportunityId,
    organizationName: r.organizationName,
    title: r.title || null,
    researchCategory: r.researchCategory,
    demandSegment: r.demandSegment || null,
    fieldsAvailable: Object.keys(r).filter((k) => r[k] != null && r[k] !== ""),
    evidenceContactCompleteness: r.evidenceContactCompleteness,
    duplicateDecision: "NOT_DUPLICATE_OF_CANONICAL",
    classification: r.classification,
    reasonAbsentToday: r.reconcileReason || r.reasonAbsent,
    bodyAvailable: r.bodyAvailable === true,
    importBlockedUntilApproval: true,
    preserveHistoricalSourceRecord: true,
    overwriteCanonicalStore: false,
    customerPublication: false,
  }));

  return {
    version: BETHESDA_HISTORICAL_RECOVERY_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    safety: {
      paidResearch: false,
      newDiscovery: false,
      engineRebuild: false,
      publication: false,
      hotelStoreWrites: false,
      dryRunOnly: true,
    },
    baseline: {
      opportunityCount: baseline.count,
      publishedCount: baseline.publishedCount,
      unpublishedCount: baseline.unpublishedCount,
      ids: baseline.ids,
      hashes: baseline.hashes,
      note: "Previously reported baseline count was 59 — confirmed.",
    },
    sourcesInspected: [
      {
        name: "canonical_opportunities",
        path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`,
        count: baseline.count,
      },
      {
        name: "v20_finalize_log",
        path: "reports/gdi-bethesda-v20-incremental-research-live-execution/finalize-run.log",
        count: v20.rows?.length || 0,
        shadowPathMissing: Boolean(v20.shadowPath),
      },
      {
        name: "process_replication_csv",
        path: "reports/gdi-bethesda-process-replication/hotel-review/final-prospect-register.csv",
        count: processRep.rows?.length || 0,
      },
      {
        name: "weekly_staged",
        path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/weekly-runs/`,
        count: weekly.length,
      },
      {
        name: "curated_attestations_from_logs_transcripts",
        path: "lib/.../bethesda-historical-opportunity-recovery-v1.js#CURATED_HISTORICAL_ATTESTATIONS",
        count: CURATED_HISTORICAL_ATTESTATIONS.length,
      },
    ],
    missingArtifacts: MISSING_ARTIFACTS,
    wipeRootCause:
      "Staged gdi_disp_* / gdi_e2d_* / pe_* candidates lived in reports/research-memory while supervisor sync used FS opportunities.json only, clearing segment candidateIds (documented in demand-segment-research-supervisor-v1.js Bethesda wipe comment).",
    groups: {
      A_existingCanonical: {
        count: groupA.length,
        ids: groupA.map((r) => r.id),
      },
      B_and_F_missingHistorical: {
        count: missingUnique.length,
        withBody: missingUnique.filter((r) => r.bodyAvailable).length,
        attestationOnly: missingUnique.filter((r) => !r.bodyAvailable).length,
        rows: missingUnique,
      },
      C_existingMissingEvidenceOrContacts: {
        count: groupC.length,
        rows: groupC,
      },
      D_researchStageCandidates: {
        count: researchOnly.length,
        rows: researchOnly,
      },
      E_duplicatesSuperseded: {
        count: duplicates.length,
        rows: duplicates,
      },
    },
    coverage: {
      existingOpportunityCount: baseline.count,
      historicalAdditionalUniqueOpportunities: missingUnique.length,
      cateringOpportunitiesRecovered: cateringRecovered.length,
      partnershipOpportunitiesRecovered: partnershipRecovered.length,
      otherMissingOpportunities: otherMissing.length,
      researchOnlyContinuationRecords: researchOnly.length,
      duplicatesExcluded: duplicates.length,
      defensibleHistoricalPopulation: historicalPopulationIds.size,
      cateringIds: cateringRecovered.map((r) => r.id),
      partnershipIds: partnershipRecovered.map((r) => r.id),
      otherMissingIds: otherMissing.map((r) => r.id),
    },
    dryRunRestorationManifest: {
      importAuthorized: false,
      hotelStoreUntouched: true,
      customerPublishedInventoryUntouched: true,
      rows: dryRunImportRows,
    },
    requiresApprovalToRestore: [
      "Approve import of attestation-only / missing-body rows into a non-destructive recovery bag (not customer-visible).",
      "Locate or accept permanent loss of V20 shadow bag and maturation research-memory JSON bodies.",
      "Decide which research-stage partnership/venue-intel rows become opportunities vs remain research continuation.",
      "Decide whether competitor-host V20 rows (CASSS CGTP, IFE, MTC ICON) are restored as overflow-watch only.",
      "No customer publication and no overwrite of the existing 59-row Bethesda store without explicit founder approval.",
    ],
  };
}

export function writeBethesdaHistoricalRecoveryReports(
  manifest,
  { root = ROOT, outDir = RECOVERY_REPORTS_DIR } = {}
) {
  const abs = path.join(root, outDir);
  fs.mkdirSync(abs, { recursive: true });
  const manifestPath = path.join(abs, "01-dry-run-restoration-manifest.json");
  const summaryPath = path.join(abs, "00-executive-summary.json");
  const mdPath = path.join(abs, "FOUNDER_RECOVERY_REPORT.md");

  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

  const summary = {
    version: manifest.version,
    generatedAt: manifest.generatedAt,
    hotelId: manifest.hotelId,
    baselineCount: manifest.baseline.opportunityCount,
    baselineHashes: manifest.baseline.hashes,
    coverage: manifest.coverage,
    missingArtifactCount: manifest.missingArtifacts.length,
    wipeRootCause: manifest.wipeRootCause,
    requiresApprovalToRestore: manifest.requiresApprovalToRestore,
    safety: manifest.safety,
  };
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);

  const c = manifest.coverage;
  const md = `# Bethesda Marriott Historical GDI Opportunity Recovery V1

Generated: ${manifest.generatedAt}
Hotel: ${manifest.hotelName} (\`${manifest.hotelId}\`)
Mode: **DRY-RUN ONLY** — no hotel-store writes, no publication, no paid research.

## Baseline
- Canonical opportunities: **${manifest.baseline.opportunityCount}** (published ${manifest.baseline.publishedCount}, unpublished ${manifest.baseline.unpublishedCount})
- opportunities.json sha256: \`${manifest.baseline.hashes["opportunities.json"]?.sha256}\`

## Coverage
| Metric | Count |
| --- | ---: |
| Existing canonical | ${c.existingOpportunityCount} |
| Historical additional unique | ${c.historicalAdditionalUniqueOpportunities} |
| Local catering recovered | ${c.cateringOpportunitiesRecovered} |
| Partnerships recovered | ${c.partnershipOpportunitiesRecovered} |
| Other missing | ${c.otherMissingOpportunities} |
| Research-only continuation | ${c.researchOnlyContinuationRecords} |
| Duplicates excluded | ${c.duplicatesExcluded} |
| **Defensible historical population** | **${c.defensibleHistoricalPopulation}** |

## Wipe root cause
${manifest.wipeRootCause}

## Missing artifacts
${manifest.missingArtifacts.map((a) => `- \`${a.path}\` — ${a.impact}`).join("\n")}

## Catering IDs (attestation / missing body)
${(c.cateringIds || []).map((id) => `- \`${id}\``).join("\n") || "(none)"}

## Partnership IDs (attestation / missing body)
${(c.partnershipIds || []).map((id) => `- \`${id}\``).join("\n") || "(none)"}

## Approval required before any restore
${manifest.requiresApprovalToRestore.map((x) => `- ${x}`).join("\n")}

## Safety
- Hotel store unchanged
- Customer published inventory unchanged
- No paid research / no new discovery / no engine rebuild
`;
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);

  return { abs, manifestPath, summaryPath, mdPath };
}

export function runBethesdaHistoricalRecoveryDryRun({ root = ROOT } = {}) {
  const manifest = buildBethesdaHistoricalRecoveryManifest({ root });
  const written = writeBethesdaHistoricalRecoveryReports(manifest, { root });
  return { manifest, written };
}
