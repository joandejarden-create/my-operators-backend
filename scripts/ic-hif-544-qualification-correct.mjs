#!/usr/bin/env node
/**
 * IC PR #544 — Legacy Green Bay commercial qualification correction.
 * Updates report artifacts only (no Airtable). CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb2-commercial-discovery");
const asOf = "2026-11-21";
const icAt = new Date().toISOString();

const candidatesPath = path.join(OUT, "RESEARCH_CANDIDATES.json");
const scorePath = path.join(OUT, "FIRST_100_SCORECARD.json");
const matrixPath = path.join(OUT, "MATRIX_COUNTRY_TIMING_DEALTYPE_FRESHNESS.json");
const founderPath = path.join(OUT, "FOUNDER_TRACK_A_CB2_COMMERCIAL_DISCOVERY.md");

const batch = JSON.parse(fs.readFileSync(candidatesPath, "utf8"));
const score = JSON.parse(fs.readFileSync(scorePath, "utf8"));
const matrix = JSON.parse(fs.readFileSync(matrixPath, "utf8"));

const QUAL_HISTORY = {
  priorClassification: {
    countsTowardFirst100: true,
    qualificationTier: "FIRST_100_BCT",
    commercialAccessibility: "ACCESSIBLE_PROBABLE",
    currentDecisionStatus: "OPEN_SOFT_BRAND_SELECTION",
    opportunityType: "BCT",
    rationale: "Collection unnamed treated as open brand selection",
  },
  icReviewAt: icAt,
  icPr: 544,
  sourcesReviewed: [
    "https://www.hotelmanagement.net/property-management/dreamscape-hospitality-takes-over-legacy-hotel-green-bay",
    "https://www.hospitalitynet.org/news/4135106/dreamscape-hospitality-assumes-management-of-legacy-hotel-in-green-bay-plans-marriott-soft-brand-transition",
    "https://thepassbrief.com/articles/dreamscape-hospitality-takes-over-green-bay-s-legacy-hotel",
    "https://hotelbusiness.com/dreamscape-hospitality-adds-legacy-hotel-in-green-bay-wi/",
  ],
  commitmentSpectrum: {
    openCompetitiveBrandSelection: false,
    preferredBrandFamilyWithoutBindingCommitment: true,
    committedBrandAffiliation: "NOT_PROVEN_CONTRACT_DISCLOSED",
    intraBrandFamilyCollectionSelection: true,
    unknownContractualCommitment: true,
  },
  verdict:
    "Owner publicly plans Marriott soft-brand conversion. Unnamed collection does not equal open competitive BCT. Classify as preferred Marriott family / intra-family collection selection. Competing brand families not evidenced as accessible.",
};

const legacy = batch.candidates.find((c) => c.id === "legacy-hotel-green-bay-bct");
if (!legacy) throw new Error("legacy missing");

legacy.classificationHistory = QUAL_HISTORY;
legacy.brandFamilyCommitmentClass = "PREFERRED_BRAND_FAMILY_WITHOUT_BINDING_COMMITMENT";
legacy.collectionSelectionClass = "INTRA_BRAND_FAMILY_COLLECTION_SELECTION";
legacy.openCompetitiveBrandSelection = false;
legacy.countsTowardFirst100 = false;
legacy.qualificationTier = "EXPANDED_PIPELINE_CONVERSION_INTELLIGENCE";
legacy.commercialAccessibility = "ACCESS_LIMITED_PREFERRED_BRAND_FAMILY";
legacy.currentDecisionStatus = "PREFERRED_MARRIOTT_SOFT_BRAND_COLLECTION_UNRESOLVED";
legacy.priority = "PRIORITY_B_CONVERSION_WATCH";
legacy.commercialCategory = "BRAND_CONVERSION_INTELLIGENCE_NOT_OPEN_COMPETITIVE";
legacy.opportunityType = "BCT_INTELLIGENCE";
legacy.notes =
  "IC #544 qualification: MCT filled by Dreamscape. Ownership publicly plans Marriott soft brand (Hotel Management 2026-10-22; Hospitality Net 2026-10-26; Pass Brief 2026-10-23). Unnamed collection is intra-Marriott collection selection, not open competitive brand selection. Retain conversion intelligence; exclude from first-100 accessible numerator. Prior class FIRST_100_BCT/ACCESSIBLE_PROBABLE preserved in classificationHistory.";
legacy.icQualificationCorrection = {
  at: icAt,
  fromFirst100: true,
  toFirst100: false,
  reason: QUAL_HISTORY.verdict,
};

batch.icQualificationReview = {
  pr: 544,
  at: icAt,
  legacyHotelGreenBay: QUAL_HISTORY,
  lakevilleInn: {
    status: "STILL_OPEN_BCT",
    affirmativeEvidence: true,
    evidence:
      "Star Tribune 2026-04-09: JAMP/Elevate CEO Jaymin Patel affirmed exploring brand options with goal of new brand agreement by end-2026; Chamber 2026-04-13 confirms independent Lakeville Inn under new ownership. No superseding reflag award found as of IC review. Absence of newer announcement alone is insufficient; affirmative owner statement supports prospective BCT.",
  },
  hiltonTrinidad: {
    status: "STILL_OPEN_MCT",
    affirmativeEvidence: true,
    evidence:
      "e TecK Sep 2026: short-term HMA (6+6 months) while evaluating longer-term hotel arrangements; Oct 2026 EOI for HotelTT AMC shares/assets with responses due ~2027-01-29 (Guardian / e TecK). No buyer/operator award observed. Affirmative short-term HMA + active divestment EOI support prospective commercial decision — not merely absence of award.",
  },
  expandedPipeline: {
    jerichoBeachHostel: {
      keep: "EXPANDED_PIPELINE_ADJACENT",
      openForSubmissions: true,
      due: "2026-11-25",
      leasePolicy:
        "OPERATOR_LEASE incidental to public operator RFP — retain under deferred LEASE_INTELLIGENCE policy; no dedicated lease research; do not inflate BCT/MCT lists",
    },
    whiteSandsGrandTurk: {
      keep: "EXPANDED_PIPELINE_AWARD_WATCH",
      openForSubmissions: false,
      closed: "2026-10-09",
      submissions: 6,
      awardPending: true,
      leasePolicy:
        "LEASE_REDEVELOPMENT_MANAGEMENT incidental — award-pending; deferred dedicated lease research; ACCESS_CLOSED correctly",
    },
    hotelDoMel: { keep: "EXPANDED_PIPELINE_ADJACENT", type: "OPERATING_AGREEMENT_NEW_BUILD" },
    torreUriz: { keep: "EXPANDED_PIPELINE_AWARD_WATCH", type: "CONCESSION" },
  },
};

batch.statusChecks.lakevilleInn = {
  status: "STILL_OPEN_BCT",
  verifiedAt: icAt,
  evidence: batch.icQualificationReview.lakevilleInn.evidence,
  affirmativeProspectiveDecisionEvidence: true,
  action: "NOOP_EXISTING_AIRTABLE",
};
batch.statusChecks.hiltonTrinidad = {
  status: "STILL_OPEN_MCT",
  verifiedAt: icAt,
  evidence: batch.icQualificationReview.hiltonTrinidad.evidence,
  affirmativeProspectiveDecisionEvidence: true,
  action: "NOOP_EXISTING_AIRTABLE",
};
batch.statusChecks.legacyHotelGreenBay = {
  status: "CORRECTED_NOT_OPEN_COMPETITIVE_BCT",
  verifiedAt: icAt,
  evidence: QUAL_HISTORY.verdict,
  action: "UPDATE_EXISTING_FINDING_NON_DESTRUCTIVE",
  airtableFindingId: "recAVM0frtNV6tzLJ",
  airtableInventoryId: "rec3XVviNYdBxEBIN",
};

batch.batchMetrics.newPrincipalBct = 2;
batch.batchMetrics.first100AfterCb2 = 2;
batch.batchMetrics.first100AfterCb2b = 5;
batch.batchMetrics.first100AfterIcQualification = 4;
batch.batchMetrics.legacyCorrectedOutOfFirst100 = true;
batch.batchMetrics.conversionIntelligenceRetained = 1;

score.icQualificationReviewAt = icAt;
score.openPr = 544;
score.resumeWave = "CB2B_IC_QUALIFICATION";
score.policy =
  "Public RFP not required. Count principal BCT/MCT with hotel-specific decision evidence + near-term timing AND open competitive (or multi-family) accessibility. Preferred brand-family / intra-family collection selection without open competitive access does NOT count toward first-100 accessible numerator. Adjacent operating/concession/hostel/new-build/lease incidental excluded from numerator.";
score.metrics = {
  COMMERCIALLY_RELEVANT: 4,
  COMMERCIALLY_ACCESSIBLE: 4,
  OUTREACH_READY: 0,
  priorAfterContinuousWaves: 2,
  afterCb2: 3,
  afterCb2b: 5,
  afterIcQualification: 4,
  deltaCb2: 1,
  deltaCb2b: 2,
  delta: 2,
  legacyCorrectedOut: 1,
};
score.qualifiedIds = [
  "lakeville-inn-mn-bct",
  "hilton-trinidad-mct",
  "shorebreak-huntington-bct",
  "courtyard-denver-downtown-bct",
];
score.byDealType = {
  BCT: ["lakeville-inn-mn-bct", "shorebreak-huntington-bct", "courtyard-denver-downtown-bct"],
  MCT: ["hilton-trinidad-mct"],
};
score.statusVerifiedStillOpen = [
  "lakeville-inn-mn-bct",
  "hilton-trinidad-mct",
  "shorebreak-huntington-bct",
  "courtyard-denver-downtown-bct",
];
score.correctedOutOfFirst100 = [
  {
    id: "legacy-hotel-green-bay-bct",
    reason: QUAL_HISTORY.verdict,
    retainedAs: "EXPANDED_PIPELINE_CONVERSION_INTELLIGENCE",
    brandFamilyCommitmentClass: "PREFERRED_BRAND_FAMILY_WITHOUT_BINDING_COMMITMENT",
    collectionSelectionClass: "INTRA_BRAND_FAMILY_COLLECTION_SELECTION",
  },
];
score.excludedConversionIntelligence = ["legacy-hotel-green-bay-bct"];
score.expandedPipelineCounts = {
  adjacentOpenOperatingOrConcession: 4,
  awardWatch: 3,
  conversionIntelligenceNotOpenCompetitive: 1,
  note: "Jericho = open operator RFP under lease (incidental; deferred dedicated lease research). White Sands = award-pending lease/redevelopment (submissions closed). CB2b adjacent concessions retained separately.",
};

function touch(obj) {
  if (!obj || typeof obj !== "object") return;
  if (Array.isArray(obj)) {
    for (const x of obj) {
      if (x && (x.id === "legacy-hotel-green-bay-bct" || x.hotelName === "Legacy Hotel")) {
        x.countsTowardFirst100 = false;
        x.commercialAccessibility = "ACCESS_LIMITED_PREFERRED_BRAND_FAMILY";
        x.qualificationTier = "EXPANDED_PIPELINE_CONVERSION_INTELLIGENCE";
        x.icCorrected = true;
      }
    }
    return;
  }
  for (const v of Object.values(obj)) touch(v);
}
touch(matrix);
matrix.icQualificationNote =
  "Legacy Green Bay corrected out of first-100; preferred Marriott family / collection selection. First-100 = 4 (Lakeville + Shorebreak + Courtyard Denver BCT + Hilton Trinidad MCT).";

const jericho = batch.candidates.find((c) => c.id === "jericho-beach-hostel-operator");
if (jericho) {
  jericho.leaseIntelligencePolicy = "DEFERRED_INCIDENTAL_ONLY";
  jericho.openForSubmissions = true;
  jericho.submissionDeadline = "2026-11-25";
  jericho.notes = `${jericho.notes || ""} IC#544: OPERATOR_LEASE incidental to open public RFP (due 2026-11-25); deferred dedicated lease research; not BCT/MCT.`.trim();
}
const whiteSands = batch.candidates.find((c) => c.id === "white-sands-grand-turk");
if (whiteSands) {
  whiteSands.leaseIntelligencePolicy = "DEFERRED_INCIDENTAL_ONLY";
  whiteSands.openForSubmissions = false;
  whiteSands.awardPending = true;
  whiteSands.notes = `${whiteSands.notes || ""} IC#544: lease/redevelopment/ops tender closed ~2026-10-09 with 6 submissions; award-pending — not still accepting submissions; deferred dedicated lease research.`.trim();
}

fs.writeFileSync(candidatesPath, `${JSON.stringify(batch, null, 2)}\n`);
fs.writeFileSync(scorePath, `${JSON.stringify(score, null, 2)}\n`);
fs.writeFileSync(matrixPath, `${JSON.stringify(matrix, null, 2)}\n`);

const founder = `# HIF Track A — CB2 Commercial Opportunity Discovery (IC Qualification)

**As of:** ${asOf}  
**Publication:** CUSTOMER_BLOCKED  
**Research spend:** **$0**  
**PR:** #544  
**E2E classification:** **PERSISTENCE_VALIDATED** — not FULL_E2E / not CUSTOMER_PUBLISHED  
**IC qualification:** Applied on PR tip before merge (Legacy Green Bay corrected)

---

## Status re-checks (affirmative evidence)

| Hotel | Result | Basis |
|-------|--------|-------|
| **Lakeville Inn** (US) | **STILL OPEN BCT** | Affirmative owner/CEO brand-search statement (Star Tribune 2026-04-09); independent as of Chamber 2026-04-13; no superseding reflag. Absence of newer announcement alone insufficient — affirmative decision evidence retained. |
| **Hilton Trinidad** (TT) | **STILL OPEN MCT** | Affirmative short-term HMA (6+6) while longer-term arrangements evaluated; HotelTT EOI active (~2027-01-29). Not merely absence of award. |

---

## Legacy Hotel, Green Bay — IC commercial qualification

| Spectrum question | Finding |
|-------------------|---------|
| Open competitive brand selection | **No** — sources state Marriott soft brand |
| Preferred brand family without binding commitment | **Yes** — public owner plan; franchise contract not disclosed |
| Committed brand affiliation | **Not proven** from public filings (no named franchise/collection agreement) |
| Intra-brand-family collection selection | **Yes** — collection unnamed within Marriott soft brands |
| Unknown contractual commitment | **Yes** — binding instrument not published |

**Verdict:** Unnamed collection ≠ open competitive accessibility. **Remove from first-100 accessible numerator.** Retain conversion intelligence.

Sources reviewed: Hotel Management 2026-10-22; Hospitality Net 2026-10-26; Pass Brief 2026-10-23; Hotel Business corroboration.

---

## Corrected first-100

| Metric | Pre-IC (reported) | Post-IC |
|--------|------------------:|--------:|
| Open BCT | 4 | **3** (Lakeville + Shorebreak + Courtyard Denver) |
| Open MCT | 1 | **1** (Hilton Trinidad) |
| **First-100** | 5 / 100 | **4 / 100** (Δ +2 vs #541; Legacy corrected out) |
| Commercially relevant / accessible | 5 | **4** |
| Conversion intelligence retained (not first-100) | — | **1** (Legacy) |

---

## Expanded pipeline (preserved separately)

| Opportunity | Class | Submissions |
|-------------|-------|-------------|
| Hotel do Mel (PT) | Operating / new-build adjacent | Open |
| Jericho Beach Hostel (CA) | Operator **lease** incidental — deferred dedicated lease research | **Open** due 2026-11-25 |
| Torre Uriz (ES) | Concession award-watch | Closed / award pending |
| White Sands (TC) | Lease/redevelopment/ops award-watch — deferred dedicated lease research | **Closed** ~2026-10-09; 6 submissions; award pending |
| Dorset portfolio / George Hotel | Award-watch (carried) | Cabinet / watch |

---

## Airtable

| Action | Result |
|--------|--------|
| Creates (CB2+CB2b) | **18** preserved (10+8; no re-create) |
| Idempotent no-ops | **22** evidence retained |
| IC update | Non-destructive PATCH on Legacy finding \`recAVM0frtNV6tzLJ\` + inventory notes; prior classification preserved in Notes history |
| Publication | CUSTOMER_BLOCKED |

---

**CUSTOMER_BLOCKED** — no customer publication · no weekly automation
`;

fs.writeFileSync(founderPath, founder);
console.log("ARTIFACTS_UPDATED", { icAt, first100: score.metrics.afterIcQualification });
