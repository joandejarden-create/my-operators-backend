#!/usr/bin/env node
/**
 * Zero-paid KnowBe4 + Ameresco geography investigation under CHD.
 * Uses cached ORS dossiers + public web evidence (no Exa/paid providers).
 * Does not publish. Does not write Airtable / opportunity store.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyCommercialHypothesisDevelopmentInChain } from "../lib/group-demand-intelligence/gdi-commercial-hypothesis-development-v1.js";
import {
  evaluateProactiveAccountDevelopment,
  evaluateProactivePublicationReviewReadiness,
} from "../lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { BETHESDA_HOTEL_ID } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ORS = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  BETHESDA_HOTEL_ID,
  "opportunity-research-state"
);
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-knowbe4-ameresco-geography-investigation-v1"
);

function loadOrs(id) {
  const p = path.join(ORS, `${id}.json`);
  if (!fs.existsSync(p)) return null;
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return doc.opportunity || doc;
}

function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}

const GEO = {
  KnowBe4: {
    statement:
      "KnowBe4 opened its DMV public-sector hub at 4075 Wilson Blvd, Arlington VA (Ballston). Contemporaneous Montgomery County coverage states the firm chose Virginia over Maryland after an extensive D.C.-area search. Official U.S. locations list Arlington VA — not Bethesda or Rockville.",
    negatedBethesda:
      "KnowBe4 chose Virginia over Maryland for its D.C.-area office; no Bethesda/Montgomery County operating footprint confirmed.",
    sources: [
      {
        url: "https://www.knowbe4.com/press/knowbe4-expands-public-sector-presence-with-new-arlington-virginia-office",
        title: "KnowBe4 Arlington office press release",
      },
      {
        url: "https://www.knowbe4.com/careers/locations/usa",
        title: "KnowBe4 U.S. office locations",
      },
      {
        url: "http://www.rockvillenights.com/2026/03/ai-firm-knowbe4-chooses-virginia-over.html",
        title: "KnowBe4 chooses Virginia over Maryland",
      },
    ],
  },
  Ameresco: {
    statement:
      "Ameresco’s $197M NRL ESPC covers NRL’s Washington, D.C. campus and Chesapeake Bay Detachment. Ameresco offices include Washington DC and Columbia MD. NRL main campus is SW D.C.; Bethesda lodging for this project is not confirmed.",
    negatedBethesda:
      "NRL project sites are D.C. campus and Chesapeake Bay Detachment — not Bethesda. Columbia MD office does not establish Bethesda Marriott project lodging.",
    sources: [
      {
        url: "https://www.ameresco.com/ameresco-secures-197m-energy-infrastructure-project-with-u-s-naval-research-laboratory/",
        title: "Ameresco $197M NRL ESPC",
      },
      {
        url: "https://www.ameresco.com/ameresco-office-locations/",
        title: "Ameresco office locations",
      },
    ],
  },
};

function enrich(opp, geo) {
  return {
    ...opp,
    evidence: [
      ...(Array.isArray(opp.evidence) ? opp.evidence : []),
      {
        claimKind: "FACT",
        statement: geo.statement,
        sources: geo.sources,
      },
      {
        claimKind: "FACT",
        statement: geo.negatedBethesda,
        sources: geo.sources,
      },
    ],
    sources: [...(Array.isArray(opp.sources) ? opp.sources : []), ...geo.sources],
    acceptedClaims: [
      ...(Array.isArray(opp.acceptedClaims) ? opp.acceptedClaims : []),
      { statement: geo.statement },
      { statement: geo.negatedBethesda },
    ],
    // Strip prior metro-promotion language that CHD previously rejected
    whyThisHotel: geo.negatedBethesda,
  };
}

function assess(label, id) {
  const raw = loadOrs(id) || {
    id,
    title: label,
    organizationName: label,
  };
  const geo = GEO[label];
  const enriched = enrich(raw, geo);
  const applied = applyCommercialHypothesisDevelopmentInChain(enriched, {
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: "Bethesda Marriott",
  });
  const opp = applied.opportunity;
  const hyp = applied.hypothesis;
  const elig = evaluateSharedCommercialEligibility(opp, {
    hotel: { hotelId: BETHESDA_HOTEL_ID, displayName: "Bethesda Marriott" },
  });
  const ad = evaluateProactiveAccountDevelopment(opp);
  const pub = evaluateProactivePublicationReviewReadiness(opp, {
    eligibilityResult: elig,
  });
  const geographyFit =
    hyp?.geography?.tier ||
    hyp?.geography?.fit ||
    hyp?.hotelGeographyFit ||
    opp.hotelGeographyFit ||
    null;
  const qualifies =
    pub.publicationReviewReady === true &&
    (ad.accountDevelopment === true || elig.salesReady === true);
  return {
    opportunityId: id,
    organizationName: opp.organizationName || label,
    demandConfidence: hyp?.demandConfidence || null,
    proposedRevenueType: hyp?.proposedRevenueType || null,
    confirmedCustomerRequirement: hyp?.confirmedCustomerRequirement || null,
    geography: hyp?.geography || null,
    hotelGeographyFit: geographyFit,
    primaryHypothesis: hyp?.primaryHypothesis || null,
    secondaryHypothesis: hyp?.secondaryHypothesis || null,
    targetedResearchQuestions: hyp?.targetedResearchQuestions || [],
    accountDevelopment: ad.accountDevelopment === true,
    salesReady: elig.salesReady === true,
    publicationReviewReady: pub.publicationReviewReady === true,
    customerPublicationReady: false,
    blockers: pub.blockers || [],
    geographyEvidence: geo,
    verdict: qualifies
      ? "QUALIFIES_FOR_FOUNDER_PUBLICATION_REVIEW"
      : "DOES_NOT_QUALIFY_BETHESDA_GEOGRAPHY",
  };
}

const knowbe4 = assess("KnowBe4", "disc_5976f7572131");
const ameresco = assess("Ameresco", "disc_2fc664271289");

const report = {
  version: "gdi-bethesda-knowbe4-ameresco-geography-investigation-v1",
  hotelId: BETHESDA_HOTEL_ID,
  paidSpendUsd: 0,
  customerPublication: false,
  airtableWrites: false,
  opportunityStoreWrites: false,
  candidates: [knowbe4, ameresco],
  summary: {
    knowbe4Qualifies: knowbe4.verdict === "QUALIFIES_FOR_FOUNDER_PUBLICATION_REVIEW",
    amerescoQualifies:
      ameresco.verdict === "QUALIFIES_FOR_FOUNDER_PUBLICATION_REVIEW",
    remainingBlockers: {
      KnowBe4: [
        "DMV hub in Arlington VA (Ballston)",
        "Chose Virginia over Maryland/Montgomery County",
        "No Bethesda/Rockville footprint evidenced",
        `hotelGeographyFit=${knowbe4.hotelGeographyFit}`,
      ],
      Ameresco: [
        "NRL sites = D.C. campus + Chesapeake Bay Detachment",
        "Offices = DC + Columbia MD (not Bethesda)",
        "Bethesda lodging for project travel not evidenced",
        `hotelGeographyFit=${ameresco.hotelGeographyFit}`,
      ],
    },
  },
};

writeJson("01-geography-investigation.json", report);
fs.writeFileSync(
  path.join(OUT, "FOUNDER-KNOWBE4-AMERESCO-GEOGRAPHY.md"),
  `# KnowBe4 + Ameresco — Bethesda geography investigation (zero-paid)

## KnowBe4 — **${knowbe4.verdict}**
- Geography: **${knowbe4.hotelGeographyFit}**
- Primary thesis: ${knowbe4.proposedRevenueType || "n/a"} (${knowbe4.demandConfidence || "n/a"})
- AD / Sales Ready / Pub-review: ${knowbe4.accountDevelopment} / ${knowbe4.salesReady} / ${knowbe4.publicationReviewReady}

## Ameresco — **${ameresco.verdict}**
- Geography: **${ameresco.hotelGeographyFit}**
- Primary thesis: ${ameresco.proposedRevenueType || "n/a"} (${ameresco.demandConfidence || "n/a"})
- AD / Sales Ready / Pub-review: ${ameresco.accountDevelopment} / ${ameresco.salesReady} / ${ameresco.publicationReviewReady}

## Decision
Do **not** stage either for Bethesda customer publication review. Retain Guest Rooms research hypotheses as research-continuation only. Do not invent Bethesda demand from metro adjacency.

Paid spend: **$0** · No store overwrite · No customer publication.
`
);

console.log(JSON.stringify(report.summary, null, 2));
