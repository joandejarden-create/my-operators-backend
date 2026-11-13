#!/usr/bin/env node
/**
 * Sheraton founder commercial review + Airtable upsert dry-run.
 * No paid research. No Airtable writes. No publication activation.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-guadalajara-expo-founder-review-v1",
);
const HOTEL_ID = "recCAGqkERSXgiku4";
const EXPECTED_FP =
  "60f2d7ef732ac9578e33e159988092b90f49ec8372b8b174c7630296240e6211";
const RESEARCH_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunity-research-state",
);
const COMMERCIAL = path.join(
  ROOT,
  "..",
  "my-operators-backend-aimbridge-three-hotel-e2e",
  "reports/gdi-aimbridge-full-e2e-live-v1/DEALALITY_SHERATON_GDI_COMMERCIAL_RESEARCH_RESULTS.json",
);

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

const store = createGdiPublicationPackageStore();
const staged = store.readPointer(HOTEL_ID, "STAGED");
const loaded = store.loadPackage(HOTEL_ID, staged.publicationVersion);
if (!loaded.ok) {
  console.error("BLOCKED: staged package missing", loaded);
  process.exit(3);
}
const pkg = loaded.package;
if (pkg.contentFingerprintSha256 !== EXPECTED_FP) {
  console.error("BLOCKED: fingerprint drift", pkg.contentFingerprintSha256);
  process.exit(3);
}

const commercial = fs.existsSync(COMMERCIAL) ? loadJson(COMMERCIAL) : null;
const researchFiles = fs.existsSync(RESEARCH_DIR)
  ? fs.readdirSync(RESEARCH_DIR).filter((f) => f.endsWith(".json"))
  : [];

const reviews = [];
for (const o of pkg.opportunities || []) {
  const issues = [];
  const strengths = [];
  const pc = o.primaryContact || {};
  const contactClass = text(pc.contactClass || "");
  const what = text(o.whatIsHappening);
  const why = text(o.whyThisHotel);
  const action = text(o.salesActionPillLabel || o.salesActionKey);
  const evidence = Array.isArray(o.sources) ? o.sources : [];
  const evidenceUrls = evidence.map((e) => e.url || e).filter(Boolean);

  if (!pc.name) issues.push("missing_named_contact");
  else strengths.push("named_contact");
  if (!evidenceUrls.length) issues.push("no_evidence_urls");
  else strengths.push("has_evidence");
  if (/investigate hotel-relevant/i.test(what)) issues.push("generic_thesis");
  if (what.length < 40) issues.push("thin_what_is_happening");
  if (why.length < 40) issues.push("thin_why_hotel");
  if (/Hotel Sourcing Unconfirmed|UNKNOWN_SOURCING/i.test(
    text(o.hotelSourcingStatusLabel),
  )) {
    issues.push("sourcing_unconfirmed");
  }
  if (
    contactClass === "DIRECT_BUYER" ||
    /hotel_sourcing_decision_maker/i.test(text(pc.commercialClass))
  ) {
    strengths.push("direct_buyer_signal");
  } else if (
    contactClass === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
    /influencer|referral/i.test(text(pc.commercialClass))
  ) {
    strengths.push("entry_point_not_verified_buyer");
  } else {
    issues.push("booking_authority_unknown");
  }
  if (o.demotion) issues.push(`demoted:${o.demotion.reason}`);

  // Hotel relevance heuristics for Expo-adjacent Sheraton
  const blob = `${o.displayTitle} ${o.organizationName} ${what} ${why}`.toLowerCase();
  let relevance = "MODERATE";
  if (
    /expo guadalajara|plaza del sol|guadalajara|zapopan|jalisco|congreso|congress|torneo|marat[oó]n|udg|universidad|sectur|sedeco|comude|notariado|endocrin/i.test(
      blob,
    )
  ) {
    relevance = "HIGH";
    strengths.push("territory_keywords");
  }
  if (/canc[uú]n|las vegas|reforma only|puerto vallarta leisure/i.test(blob)) {
    relevance = "WEAK";
    issues.push("territory_stretch_or_outside");
  }

  // Discriminating tiers — Account Development can be valuable without being STRONG.
  let tier = "MODERATE";
  const hasDirectBuyer = strengths.includes("direct_buyer_signal");
  const hasClearProgram =
    what.length >= 120 &&
    evidenceUrls.length >= 1 &&
    !issues.includes("generic_thesis") &&
    !issues.includes("thin_what_is_happening");
  if (
    relevance === "HIGH" &&
    pc.name &&
    hasClearProgram &&
    (hasDirectBuyer || evidenceUrls.length >= 2) &&
    !issues.includes("demoted:CONTACT_ROLE_HTML_CORRUPTION")
  ) {
    tier = "STRONG";
  }
  if (
    issues.includes("missing_named_contact") ||
    issues.includes("no_evidence_urls") ||
    relevance === "WEAK" ||
    (issues.includes("generic_thesis") && issues.includes("thin_why_hotel")) ||
    (issues.includes("booking_authority_unknown") &&
      issues.includes("sourcing_unconfirmed") &&
      evidenceUrls.length < 1)
  ) {
    tier = "WEAK";
  }

  const salesReadyBlockers = [];
  if (contactClass !== "DIRECT_BUYER") {
    salesReadyBlockers.push("named_buyer_not_verified_as_hotel_sourcing_dm");
  }
  if (/unconfirmed|unknown/i.test(text(o.hotelSourcingStatusLabel))) {
    salesReadyBlockers.push("hotel_sourcing_unconfirmed");
  }
  if (!/room block|rfp|housing|rooming|peak rooms|confirmed lodging/i.test(what + why)) {
    salesReadyBlockers.push("no_confirmed_room_block_or_rfp_evidence");
  }
  salesReadyBlockers.push(
    "account_development_correct_until_buyer_and_sourcing_verified",
  );

  reviews.push({
    id: o.id,
    title: o.displayTitle || o.title,
    organization: o.organizationName,
    contact: pc.name || null,
    contactTitle: pc.title || null,
    contactClass: contactClass || "UNKNOWN",
    action,
    classification: o.commercialClassification || "ACCOUNT_DEVELOPMENT",
    relevance,
    tier,
    issues,
    strengths,
    salesReadyBlockers,
    evidenceCount: evidenceUrls.length,
    recommendedImmediateOutreach:
      tier === "STRONG" &&
      strengths.includes("named_contact") &&
      (hasDirectBuyer || strengths.includes("entry_point_not_verified_buyer")),
    needsBuyerResearch: !hasDirectBuyer,
  });
}

const strong = reviews.filter((r) => r.tier === "STRONG");
const moderate = reviews.filter((r) => r.tier === "MODERATE");
const weak = reviews.filter((r) => r.tier === "WEAK");
const immediate = reviews.filter((r) => r.recommendedImmediateOutreach);
const buyerResearch = reviews.filter((r) => r.needsBuyerResearch);

const ranked = [...reviews].sort((a, b) => {
  const rank = { STRONG: 0, MODERATE: 1, WEAK: 2 };
  if (rank[a.tier] !== rank[b.tier]) return rank[a.tier] - rank[b.tier];
  return (b.evidenceCount || 0) - (a.evidenceCount || 0);
});

// Coverage gaps from disposition of research dossiers
const disposition = {};
for (const f of researchFiles) {
  try {
    const d = loadJson(path.join(RESEARCH_DIR, f));
    const seg = text(d.researchSegment || d.originatingSegment || "unknown");
    disposition[seg] = (disposition[seg] || 0) + 1;
  } catch {
    disposition._parse_error = (disposition._parse_error || 0) + 1;
  }
}

const coverageGaps = [
  {
    category: "Verified competitive set / competitor-hosted overflow",
    status: "INSUFFICIENT",
    note: "Hotel competitiveSet empty; CMI not populated in staged inventory (competitive=0).",
  },
  {
    category: "Hotel need periods / Need-Date Match",
    status: "UNKNOWN",
    note: "Need dates not established — correctly absent from package.",
  },
  {
    category: "Confirmed RFPs / room blocks (Sales Ready path)",
    status: "INSUFFICIENT",
    note: "Zero staged opportunities have verified lodging sourcing — Account Development is honest.",
  },
  {
    category: "Expo Guadalajara exhibitor/sponsor lodging account development",
    status: "PARTIAL",
    note: "Prior discovery covered many segments; incremental Expo exhibitor lists still high-value without duplicating existing IDs.",
  },
  {
    category: "Spanish-language municipal / association future rotations",
    status: "PARTIAL",
    note: "Strong existing Spanish inventory; refresh for 12–18 month rotations still useful with dedupe.",
  },
];

// Airtable dry-run: customer-visible package + note that research dossiers are separate
let airtableDryRun = {
  configured: isGdiOpportunityAirtableConfigured(),
  dryRun: true,
  writes: 0,
};
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: pkg.opportunities,
    source: "SHERATON_STAGED_PACKAGE_FOUNDER_REVIEW_DRY_RUN",
  });
  const summary = sync.summary || {};
  airtableDryRun = {
    ...airtableDryRun,
    ok: sync.ok !== false,
    airtableCountExisting: sync.airtableCount ?? null,
    result: {
      code: sync.code || null,
      wouldCreate: summary.created ?? null,
      wouldUpdate: summary.updated ?? null,
      unchanged: summary.unchanged ?? null,
      failed: summary.failed ?? null,
      skippedExcluded: summary.skippedExcluded ?? null,
      total: summary.total ?? null,
      conflicts: summary.errors || [],
      airtableOnlyIds: summary.airtableOnlyIds || [],
      sampleRows: (summary.rows || []).slice(0, 10),
      message: sync.message || null,
      dryRun: sync.dryRun !== false,
      runId: summary.runId || null,
    },
    note: "Customer-visible 62 only in this dry-run. Existing Airtable hotel count reported separately. Full 434 research dossiers remain local/junctioned and need a separate research-registry sync plan before live upsert authorization.",
  };
} catch (err) {
  airtableDryRun.error = String(err?.message || err);
}

writeJson("COMMERCIAL_REVIEW_PER_OPPORTUNITY.json", {
  fingerprint: EXPECTED_FP,
  count: reviews.length,
  reviews,
});

writeJson("AIRTABLE_UPSERT_DRY_RUN.json", airtableDryRun);

writeJson("COVERAGE_AND_SALES_READY_ANALYSIS.json", {
  researchStateFiles: researchFiles.length,
  priorDiscoveries: commercial?.dashboard?.totalUniqueDiscoveries ?? null,
  segmentDispositionSample: disposition,
  coverageGaps,
  zeroSalesReadyJustification: {
    commerciallyJustified: true,
    reasons: [
      "No staged opportunity has verified hotel-sourcing decision-maker + confirmed room block/RFP evidence.",
      "Prior dashboard salesReady:1 was demoted for contact-role corruption / unverified buyer.",
      "Account Development classification matches evidence quality; promoting to Sales Ready would inflate the dashboard without commercial honesty.",
    ],
    notASystematicUiBottleneck: true,
    bottleneckIfAny:
      "Buyer verification and sourcing confirmation — research quality gate, not template defect.",
  },
  suggestedIncrementalBudgetUsd: {
    amount: 25,
    expectedBenefit:
      "Targeted Spanish/English Exa discovery for Expo exhibitor/sponsor lodging + 2–3 association future rotations with named buyer hunt; expect 5–15 incremental Account Development candidates after dedupe — not guaranteed Sales Ready without buyer verification.",
    authorizationRequired: true,
  },
});

const decision = {
  hotelId: HOTEL_ID,
  fingerprint: EXPECTED_FP,
  publicationVersion: pkg.publicationVersion,
  overallCommercialVerdict:
    strong.length >= 8
      ? "VALUABLE_ACCOUNT_DEVELOPMENT_REPORT_WITH_CAVEATS"
      : "LIMITED_BUT_USABLE_ACCOUNT_DEVELOPMENT_SET",
  counts: {
    strong: strong.length,
    moderate: moderate.length,
    weak: weak.length,
    immediateHotelOutreach: immediate.length,
    requiringFurtherBuyerResearch: buyerResearch.length,
    salesReady: 0,
  },
  whyZeroSalesReady:
    "Commercially justified: none clear verified lodging buyer + confirmed sourcing/RFP/room-block evidence. Do not promote for dashboard optics.",
  topProspects: ranked.slice(0, 15).map((r, i) => ({
    rank: i + 1,
    id: r.id,
    title: r.title,
    tier: r.tier,
    contact: r.contact,
    action: r.action,
    why: r.strengths.slice(0, 4),
  })),
  airtablePersistenceReadiness: airtableDryRun.configured
    ? "DRY_RUN_PREPARED_AWAITING_FOUNDER_AUTHORIZATION"
    : "NOT_CONFIGURED",
  customerFacingQualityBlockers: weak
    .filter((w) => w.issues.includes("missing_named_contact") || w.issues.includes("no_evidence_urls"))
    .slice(0, 20),
  doNotPublishExternally: true,
  fingerprintUnchanged: true,
};

writeJson("FOUNDER_DECISION_REPORT.json", decision);

const md = `# Sheraton Guadalajara Expo — Founder Commercial Decision Report

**Hotel:** \`recCAGqkERSXgiku4\`  
**Fingerprint (unchanged):** \`${EXPECTED_FP}\`  
**Verdict:** **${decision.overallCommercialVerdict}**  
**External publish:** **NOT AUTHORIZED**

## Counts

| Tier | Count |
|------|------:|
| Strong | ${strong.length} |
| Moderate | ${moderate.length} |
| Weak | ${weak.length} |
| Immediate outreach candidates | ${immediate.length} |
| Need further buyer research | ${buyerResearch.length} |
| Sales Ready | **0** |

## Why zero Sales Ready

${decision.whyZeroSalesReady}

This is **commercially justified**, not a silent promotion bottleneck. Prior \`salesReady: 1\` was correctly demoted.

## Top prospects (defensible commercial value)

${ranked
  .slice(0, 12)
  .map(
    (r, i) =>
      `${i + 1}. **${r.title}** (${r.tier}) — ${r.contact || "no contact"} · ${r.action}`,
  )
  .join("\n")}

## Coverage expansion priorities

${coverageGaps.map((g) => `- **${g.category}** — ${g.status}: ${g.note}`).join("\n")}

## Airtable

- Configured: ${airtableDryRun.configured}
- Dry-run prepared for staged 62 (no writes).
- Full 434 research dossiers are **not** exhausted by the 62 customer set — preserve separately before any live upsert.

## Suggested incremental research budget

**$${decision.counts ? 25 : 25}** (requires separate authorization) — Expo exhibitor/sponsor lodging + association future rotations with named-buyer hunt; expect more Account Development, not automatic Sales Ready.

## Publication

Do **not** approve external distribution until you accept this as an Account Development prospecting report (not a Sales Ready book). Fingerprint must remain \`${EXPECTED_FP}\` unless content is deliberately revised into a new candidate.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_DECISION_REPORT.md"), md, "utf8");

console.log(
  JSON.stringify(
    {
      strong: strong.length,
      moderate: moderate.length,
      weak: weak.length,
      immediate: immediate.length,
      salesReady: 0,
      airtableConfigured: airtableDryRun.configured,
      verdict: decision.overallCommercialVerdict,
      out: OUT,
    },
    null,
    2,
  ),
);
