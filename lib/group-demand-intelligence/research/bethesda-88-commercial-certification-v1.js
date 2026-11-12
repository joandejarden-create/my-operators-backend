/**
 * Bethesda 88 — full commercial certification V1 (audit/report labels).
 *
 * Reviews every internal opportunity (59 canonical + 29 historical) for
 * identity, demand evidence, contact intelligence, sales actionability, and
 * commercial maturity. Does NOT write opportunities.json. Does NOT invent
 * Sales Ready. Contact enrichment projection uses ZERO_PAID evidence +
 * anti-clobber merge for before/after coverage accounting.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import {
  loadHotelOpportunityDoc,
  ACCEPTABLE_59_ROW_SHA256S,
  BETHESDA_PROTECTED_SCALING_IDS,
  TITLE_REPAIRED_59_ROW_SHA256,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import { writeJsonAtomic } from "./gdi-research-stage-persistence-v1.js";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  PROPOSED_PAID_CONTACT_BATCH_V1,
  extractContactSnapshot,
  mergeContactAntiClobber,
  COVERAGE_BUCKET,
  auditBethesda88Contacts,
  applyHistoricalContactEnrichment,
  buildCanonicalContactDryRunPatches,
  BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
} from "./bethesda-88-contact-quality-completion-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../account-development-eligibility-v1.js";

export const BETHESDA_88_COMMERCIAL_CERTIFICATION_V1 =
  "gdi-bethesda-88-commercial-certification-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL = "reports/gdi-bethesda-88-commercial-certification-v1";

export const CERTIFICATION_OUTCOME = Object.freeze({
  COMMERCIALLY_CERTIFIED: "COMMERCIALLY_CERTIFIED",
  CONDITIONALLY_READY: "CONDITIONALLY_READY",
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
  DISQUALIFIED_WITH_EVIDENCE: "DISQUALIFIED_WITH_EVIDENCE",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function len(v) {
  return text(v).length;
}

function demandClassOf(row) {
  const raw = text(
    row.demandStatus ||
      row.demandEvidenceClass ||
      row.demandConfidence ||
      row.demandClassification ||
      row.evidenceDemandClass
  ).toUpperCase();
  if (/CONFIRMED/.test(raw)) return "CONFIRMED_DEMAND";
  if (/RELIABLE|INFERRED/.test(raw)) return "RELIABLE_INFERRED_DEMAND";
  if (/SPECULATIVE/.test(raw)) return "SPECULATIVE_DEMAND";
  // Infer from narrative strength when explicit class absent.
  const thesis = text(
    row.whatIsHappening ||
      row.hotelOpportunityThesis ||
      row.summaryWhat ||
      row.tileCustomerSummary ||
      row.description
  );
  const fit = text(row.hotelFitRationale || row.whyThisHotel || row.localConnection);
  if (len(thesis) >= 120 && len(fit) >= 80 && (row.sources || []).length >= 1) {
    return "RELIABLE_INFERRED_DEMAND";
  }
  if (len(thesis) >= 60 || len(fit) >= 40) return "SPECULATIVE_DEMAND";
  return "SPECULATIVE_DEMAND";
}

function maturityOf(row) {
  const m = text(
    row.commercialMaturity || row.priorityPillLabel || row.priority || row.outreachReadiness
  ).toUpperCase().replace(/\s+/g, "_");
  if (/DISQUALIFIED/.test(m)) return "DISQUALIFIED";
  if (/SALES_READY|SALESREADY/.test(m)) return "SALES_READY";
  if (/ACCOUNT_DEVELOPMENT|ACCOUNT\s*DEVELOPMENT/.test(m)) return "ACCOUNT_DEVELOPMENT";
  if (/PARTNERSHIP/.test(m)) return "PARTNERSHIP_WATCH";
  if (/OVERFLOW/.test(m)) return "OVERFLOW_WATCH";
  if (/RESEARCH/.test(m)) return "RESEARCH_CONTINUATION";
  if (text(row.historicalRecoveryDisposition) === "OVERFLOW_WATCH") return "OVERFLOW_WATCH";
  if (text(row.historicalRecoveryDisposition) === "PARTNERSHIP_WATCH") {
    return "PARTNERSHIP_WATCH";
  }
  return "RESEARCH_CONTINUATION";
}

function hotelFitQuality(row) {
  const fit = text(row.hotelFitRationale || row.whyThisHotel || row.localConnection);
  const maturity = maturityOf(row);
  if (maturity === "OVERFLOW_WATCH") {
    return len(fit) >= 40 ? "OVERFLOW_HONEST" : "WEAK_OVERFLOW";
  }
  if (maturity === "DISQUALIFIED") return "DISQUALIFIED_FIT";
  if (len(fit) >= 100) return "STRONG";
  if (len(fit) >= 60) return "ADEQUATE";
  if (len(fit) >= 20) return "THIN";
  return "MISSING";
}

function demandEvidenceQuality(row) {
  const cls = demandClassOf(row);
  const sources = Array.isArray(row.sources) ? row.sources.length : 0;
  const thesis = text(
    row.whatIsHappening || row.hotelOpportunityThesis || row.summaryWhat || row.description
  );
  if (maturityOf(row) === "DISQUALIFIED") return "DISQUALIFIED_EVIDENCE";
  if (cls === "CONFIRMED_DEMAND" && sources >= 1 && len(thesis) >= 80) return "CONFIRMED";
  if (cls === "RELIABLE_INFERRED_DEMAND" && len(thesis) >= 80) return "RELIABLE_INFERRED";
  if (sources >= 1 && len(thesis) >= 60) return "SOURCED_SPECULATIVE";
  if (len(thesis) >= 40) return "NARRATIVE_ONLY";
  return "INSUFFICIENT";
}

function nextActionOf(row, contact) {
  const existing = text(row.recommendedAction || row.nextAction || row.salesNextStep);
  if (existing.length >= 40) return existing;
  const maturity = maturityOf(row);
  if (maturity === "DISQUALIFIED") {
    return "Do not pursue — retain as disqualified with evidence; no outreach.";
  }
  if (maturity === "OVERFLOW_WATCH") {
    return "Retain Overflow Watch — confirm residual guestroom access before any outreach.";
  }
  if (maturity === "PARTNERSHIP_WATCH") {
    return contact?.named
      ? `Open partnership / referral discussion with ${contact.name} (${contact.title || "named entry"}); confirm mechanism before proposing rooms.`
      : "Continue Partnership Watch — identify named commercial entry and referral mechanism.";
  }
  if (contact?.named && (contact.hasDirectEmail || contact.hasOrgRoute)) {
    const route = contact.hasDirectEmail
      ? contact.email
      : contact.orgEmail || contact.orgRoute;
    return `Outreach ${contact.name} via ${route} — propose Bethesda Marriott fit for this demand cycle; verify timing before RFP ask.`;
  }
  if (contact?.hasOrgRoute) {
    return `Use organization route (${contact.orgEmail || contact.orgRoute}) to identify named commercial owner; do not treat inbox as completed primary contact.`;
  }
  return "Research continuation — recover verified named commercial contact and evidence-backed Bethesda Marriott demand thesis before outreach.";
}

function projectRow(row) {
  const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[row.id];
  if (!evidence || evidence.rejected === true || evidence.founderValidation === "FAIL") {
    return {
      row,
      projected: false,
      before: extractContactSnapshot(row),
      after: extractContactSnapshot(row),
    };
  }
  const before = extractContactSnapshot(row);
  const merged = mergeContactAntiClobber(row, evidence);
  const after = extractContactSnapshot(merged);
  return { row: merged, projected: true, before, after, inventory: evidence.inventory };
}

function certifyOne(raw, { historical }) {
  const { row, projected, before, after } = projectRow(raw);
  const contact = after;
  const maturity = maturityOf(row);
  const demandQ = demandEvidenceQuality(row);
  const fitQ = hotelFitQuality(row);
  const named = contact.named === true;
  const route = contact.hasDirectEmail || contact.hasOrgRoute;
  const missing = [];
  if (!named) missing.push("VERIFIED_NAMED_RELEVANT_PRIMARY");
  if (!route) missing.push("USABLE_OUTREACH_ROUTE");
  if (demandQ === "INSUFFICIENT" || demandQ === "NARRATIVE_ONLY") {
    missing.push("INDEPENDENT_DEMAND_EVIDENCE");
  }
  if (fitQ === "MISSING" || fitQ === "THIN") missing.push("HOTEL_FIT_THESIS");
  if (maturity === "OVERFLOW_WATCH") missing.push("INDEPENDENT_OVERFLOW_ACCESS");

  // Align report certification with established SR / AD engines.
  // Do not mutate product maturity fields; labels only.
  const sr = evaluateSharedCommercialEligibility(row);
  const ad = evaluateAccountDevelopmentEligibility(row);
  const engineSalesReady = sr.salesReady === true;
  const engineAccountDevelopment = ad.accountDevelopment === true;
  const structuralReady =
    named &&
    route &&
    (demandQ === "CONFIRMED" || demandQ === "RELIABLE_INFERRED" || demandQ === "SOURCED_SPECULATIVE") &&
    fitQ !== "MISSING" &&
    maturity !== "DISQUALIFIED";

  let outcome = CERTIFICATION_OUTCOME.RESEARCH_INCOMPLETE;
  if (maturity === "DISQUALIFIED") {
    outcome = CERTIFICATION_OUTCOME.DISQUALIFIED_WITH_EVIDENCE;
  } else if (
    (engineSalesReady || engineAccountDevelopment) &&
    named &&
    route &&
    maturity !== "OVERFLOW_WATCH"
  ) {
    // COMMERCIALLY_CERTIFIED now requires SR or AD engine pass.
    outcome = CERTIFICATION_OUTCOME.COMMERCIALLY_CERTIFIED;
  } else if (
    structuralReady ||
    (named &&
      route &&
      demandQ !== "INSUFFICIENT" &&
      fitQ !== "MISSING" &&
      maturity !== "DISQUALIFIED")
  ) {
    outcome = CERTIFICATION_OUTCOME.CONDITIONALLY_READY;
  } else if (maturity === "OVERFLOW_WATCH" || maturity === "PARTNERSHIP_WATCH") {
    outcome =
      named && route
        ? CERTIFICATION_OUTCOME.CONDITIONALLY_READY
        : CERTIFICATION_OUTCOME.RESEARCH_INCOMPLETE;
  }

  // Product maturity bucket for founder rollup (preserve existing; do not promote).
  let productBucket = maturity;
  if (maturity === "SALES_READY" && outcome !== CERTIFICATION_OUTCOME.COMMERCIALLY_CERTIFIED) {
    productBucket = "ACCOUNT_DEVELOPMENT"; // refuse fabricated SR in report taxonomy
  }

  return {
    id: row.id,
    inventory: historical ? "historical" : "canonical",
    title: text(row.title || row.displayTitle).slice(0, 160) || null,
    organizationName: text(row.organizationName) || null,
    category:
      text(row.opportunityTypeLabel || row.segment || row.opportunityType) || null,
    productMaturity: productBucket,
    demandEvidenceQuality: demandQ,
    demandClass: demandClassOf(row),
    hotelFitQuality: fitQ,
    contactBefore: {
      named: before.named,
      name: before.name,
      coverage: before.coverage,
      email: before.email || before.orgEmail || null,
    },
    contactAfter: {
      named: contact.named,
      name: contact.name,
      title: contact.title,
      coverage: contact.coverage,
      directEmail: contact.hasDirectEmail ? contact.email : null,
      orgRoute: contact.hasOrgRoute ? contact.orgEmail || contact.orgRoute : null,
      secondaryName: contact.secondaryName,
      why: contact.whyThisContact,
      sourceUrl: contact.sourceUrl,
      verifiedAt: contact.verifiedAt,
    },
    outreachRoute:
      contact.hasDirectEmail
        ? `DIRECT:${contact.email}`
        : contact.hasOrgRoute
          ? `ORG:${contact.orgEmail || contact.orgRoute}`
          : "NONE",
    secondaryContact: contact.secondaryNamed
      ? { name: contact.secondaryName, email: contact.secondaryEmail }
      : null,
    missingCommercialInformation: missing,
    recommendedNextAction: nextActionOf(row, contact),
    certificationOutcome: outcome,
    eligibilityEngines: {
      salesReadyEligible: engineSalesReady,
      salesReadyStatus: sr.status,
      salesReadyGaps: sr.gaps || [],
      accountDevelopmentEligible: engineAccountDevelopment,
      accountDevelopmentStatus: ad.status,
      accountDevelopmentGaps: ad.gaps || [],
    },
    contactProjectedFromZeroPaid: projected,
    contactImproved:
      (!before.named && contact.named) ||
      (before.coverage !== contact.coverage &&
        [COVERAGE_BUCKET.A, COVERAGE_BUCKET.B].includes(contact.coverage)),
  };
}

export function runBethesda88CommercialCertification({
  root = ROOT,
  applyHistoricalContacts = true,
} = {}) {
  const storeBefore = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const contactBeforeAudit = auditBethesda88Contacts({ root });

  let historicalApply = null;
  if (applyHistoricalContacts) {
    historicalApply = applyHistoricalContactEnrichment({ root, dryRun: false });
  }
  const canonicalDryRun = buildCanonicalContactDryRunPatches({ root });
  const contactAfterAudit = auditBethesda88Contacts({ root });

  const store = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_CANONICAL_HOTEL_ID,
    store.opportunities,
    { root }
  );

  const rows = [];
  for (const o of union.opportunities) {
    const historical = BETHESDA_29_ATTESTED_IDS.includes(o.id);
    rows.push(certifyOne(o, { historical }));
  }

  const outcomes = {};
  const maturityCounts = {
    SALES_READY: 0,
    ACCOUNT_DEVELOPMENT: 0,
    RESEARCH_CONTINUATION: 0,
    PARTNERSHIP_WATCH: 0,
    OVERFLOW_WATCH: 0,
    DISQUALIFIED: 0,
    OTHER: 0,
  };
  for (const r of rows) {
    outcomes[r.certificationOutcome] = (outcomes[r.certificationOutcome] || 0) + 1;
    if (maturityCounts[r.productMaturity] != null) maturityCounts[r.productMaturity] += 1;
    else maturityCounts.OTHER += 1;
  }

  const namedBefore = contactBeforeAudit.namedRelevantPrimary;
  // Projected after includes canonical dry-run merges for founder visibility.
  const namedAfterProjected = rows.filter((r) => r.contactAfter.named).length;
  const namedPlusRoute = rows.filter(
    (r) => r.contactAfter.named && r.outreachRoute !== "NONE"
  ).length;
  const stillMissingNamed = rows.filter((r) => !r.contactAfter.named).length;
  const recoveredNamed = rows.filter(
    (r) => r.contactImproved && r.contactAfter.named && !r.contactBefore.named
  );

  const buckets = {
    commerciallyCertified: rows.filter(
      (r) => r.certificationOutcome === CERTIFICATION_OUTCOME.COMMERCIALLY_CERTIFIED
    ),
    conditionallyReady: rows.filter(
      (r) => r.certificationOutcome === CERTIFICATION_OUTCOME.CONDITIONALLY_READY
    ),
    accountDevelopment: rows.filter((r) => r.productMaturity === "ACCOUNT_DEVELOPMENT"),
    partnershipDevelopment: rows.filter((r) => r.productMaturity === "PARTNERSHIP_WATCH"),
    researchContinuation: rows.filter((r) => r.productMaturity === "RESEARCH_CONTINUATION"),
    overflowWatch: rows.filter((r) => r.productMaturity === "OVERFLOW_WATCH"),
    disqualified: rows.filter(
      (r) =>
        r.certificationOutcome === CERTIFICATION_OUTCOME.DISQUALIFIED_WITH_EVIDENCE ||
        r.productMaturity === "DISQUALIFIED"
    ),
    researchIncomplete: rows.filter(
      (r) => r.certificationOutcome === CERTIFICATION_OUTCOME.RESEARCH_INCOMPLETE
    ),
  };

  const report = {
    version: BETHESDA_88_COMMERCIAL_CERTIFICATION_V1,
    contactModuleVersion: BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    safety: {
      paidResearch: false,
      opportunitiesJsonWrites: false,
      shareRegistryWrites: false,
      customerPublication: false,
      salesReadyFabrication: false,
      historicalResearchStageMerge: historicalApply?.writeApplied === true,
      canonicalContactPatches: "DRY_RUN_ONLY",
    },
    sourceOfTruth: {
      gitHeadExpected: "47b44182de6714cc87b52cf4089da159e4b68845",
      storeCount: store.count,
      storeSha256: store.sha256,
      storeShaUnchanged: store.sha256 === storeBefore.sha256,
      titleRepairedSha: store.sha256 === TITLE_REPAIRED_59_ROW_SHA256,
      acceptableStore: ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256),
      scalingCompaniesIntact: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
        store.ids.includes(id)
      ),
      union: union.counts,
      persistenceNote:
        "Live 88-item list = filesystem canonical 59 (GDI_OPPORTUNITIES_PERSISTENCE=filesystem) ∪ research-stage 29. Do not overwrite richer FS store from Airtable.",
    },
    contactCoverage: {
      before: {
        namedRelevantPrimary: namedBefore,
        distribution: contactBeforeAudit.distribution,
        directOutreachRoutes: contactBeforeAudit.directOutreachRoutes,
        orgPathOnly: contactBeforeAudit.orgPathOnly,
      },
      afterHistoricalApply: {
        namedRelevantPrimary: contactAfterAudit.namedRelevantPrimary,
        distribution: contactAfterAudit.distribution,
      },
      afterIncludingCanonicalDryRunProjection: {
        namedRelevantPrimary: namedAfterProjected,
        namedPlusUsableRoute: namedPlusRoute,
        stillMissingNamed,
        recoveredNamedCount: recoveredNamed.length,
        recoveredNamedIds: recoveredNamed.map((r) => r.id),
      },
      baselineMission: {
        namedRelevantPrimary: 45,
        stillLackingNamed: 43,
        note: "Mission baseline at kickoff; after figures are this run.",
      },
    },
    certificationCounts: outcomes,
    productMaturityCounts: maturityCounts,
    meetingCommercialCertification: (
      outcomes[CERTIFICATION_OUTCOME.COMMERCIALLY_CERTIFIED] || 0
    ),
    conditionallyReady: outcomes[CERTIFICATION_OUTCOME.CONDITIONALLY_READY] || 0,
    researchIncomplete: outcomes[CERTIFICATION_OUTCOME.RESEARCH_INCOMPLETE] || 0,
    disqualifiedWithEvidence:
      outcomes[CERTIFICATION_OUTCOME.DISQUALIFIED_WITH_EVIDENCE] || 0,
    historicalApply: {
      ok: historicalApply?.ok === true,
      writeApplied: historicalApply?.writeApplied === true,
      appliedCount: historicalApply?.applied?.length || 0,
    },
    canonicalDryRun: {
      patchCount: canonicalDryRun.patches.length,
      storeSha256: canonicalDryRun.storeSha256,
      requiresFounderApproval: true,
      opportunityIds: canonicalDryRun.patches.map((p) => p.id),
    },
    proposedPaidBatch: PROPOSED_PAID_CONTACT_BATCH_V1,
    zeroPaidEvidenceMapped: Object.keys(ZERO_PAID_CONTACT_EVIDENCE_BY_ID).length,
    scorecard: rows,
    buckets: {
      commerciallyCertifiedIds: buckets.commerciallyCertified.map((r) => r.id),
      conditionallyReadyIds: buckets.conditionallyReady.map((r) => r.id),
      accountDevelopmentIds: buckets.accountDevelopment.map((r) => r.id),
      partnershipDevelopmentIds: buckets.partnershipDevelopment.map((r) => r.id),
      researchContinuationIds: buckets.researchContinuation.map((r) => r.id),
      overflowWatchIds: buckets.overflowWatch.map((r) => r.id),
      disqualifiedIds: buckets.disqualified.map((r) => r.id),
    },
  };

  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "00-commercial-certification-summary.json"), {
    ...report,
    scorecard: undefined,
  });
  writeJsonAtomic(path.join(outDir, "01-record-level-scorecard.json"), {
    version: BETHESDA_88_COMMERCIAL_CERTIFICATION_V1,
    generatedAt: report.generatedAt,
    count: rows.length,
    rows,
  });
  writeJsonAtomic(path.join(outDir, "02-contact-coverage-before-after.json"), report.contactCoverage);
  writeJsonAtomic(path.join(outDir, "03-canonical-59-contact-dry-run-manifest.json"), {
    authorized: false,
    patchCount: canonicalDryRun.patches.length,
    patches: canonicalDryRun.patches,
  });
  writeJsonAtomic(path.join(outDir, "04-proposed-paid-research-batch.json"), PROPOSED_PAID_CONTACT_BATCH_V1);
  writeJsonAtomic(path.join(outDir, "05-certification-buckets.json"), report.buckets);

  const md = buildFounderMarkdown(report);
  fs.writeFileSync(path.join(outDir, "FOUNDER_COMMERCIAL_CERTIFICATION_REPORT.md"), md, "utf8");

  return report;
}

function buildFounderMarkdown(report) {
  const c = report.contactCoverage;
  const lines = [
    `# Bethesda Marriott — 88 Commercial Certification + Contact Research`,
    ``,
    `**Contract:** \`${report.version}\``,
    `**Generated:** ${report.generatedAt}`,
    `**Hotel:** ${report.hotelName} (\`${report.hotelId}\`)`,
    ``,
    `## Safety`,
    `- Paid research: **NOT AUTHORIZED**`,
    `- Canonical \`opportunities.json\` writes: **NONE** (dry-run patches only)`,
    `- Historical research-stage merge: ${report.historicalApply.writeApplied ? "APPLIED" : "NOT APPLIED"}`,
    `- Customer publication: **NONE**`,
    ``,
    `## Source of truth`,
    `- Store SHA: \`${report.sourceOfTruth.storeSha256}\` (unchanged=${report.sourceOfTruth.storeShaUnchanged})`,
    `- Title-repaired SHA match: ${report.sourceOfTruth.titleRepairedSha}`,
    `- Scaling Companies intact: ${report.sourceOfTruth.scalingCompaniesIntact}`,
    `- Union: ${JSON.stringify(report.sourceOfTruth.union)}`,
    ``,
    `## Contact coverage`,
    `| Metric | Mission baseline | After historical apply | After + canonical dry-run projection |`,
    `|---|---:|---:|---:|`,
    `| Named relevant primary | ${c.baselineMission.namedRelevantPrimary} | ${c.afterHistoricalApply.namedRelevantPrimary} | ${c.afterIncludingCanonicalDryRunProjection.namedRelevantPrimary} |`,
    `| Named + usable outreach route | — | — | ${c.afterIncludingCanonicalDryRunProjection.namedPlusUsableRoute} |`,
    `| Still missing named | ${c.baselineMission.stillLackingNamed} | — | ${c.afterIncludingCanonicalDryRunProjection.stillMissingNamed} |`,
    `| Newly recovered named (this run) | — | — | ${c.afterIncludingCanonicalDryRunProjection.recoveredNamedCount} |`,
    ``,
    `### Recovered named IDs`,
    ...(c.afterIncludingCanonicalDryRunProjection.recoveredNamedIds || []).map(
      (id) => `- \`${id}\``
    ),
    ``,
    `## Certification outcomes (report labels)`,
    `- COMMERCIALLY_CERTIFIED: **${report.meetingCommercialCertification}**`,
    `- CONDITIONALLY_READY: **${report.conditionallyReady}**`,
    `- RESEARCH_INCOMPLETE: **${report.researchIncomplete}**`,
    `- DISQUALIFIED_WITH_EVIDENCE: **${report.disqualifiedWithEvidence}**`,
    ``,
    `## Product maturity rollup (existing fields; no fabricated Sales Ready)`,
    `- Sales Ready: ${report.productMaturityCounts.SALES_READY}`,
    `- Account Development: ${report.productMaturityCounts.ACCOUNT_DEVELOPMENT}`,
    `- Research Continuation: ${report.productMaturityCounts.RESEARCH_CONTINUATION}`,
    `- Partnership Watch: ${report.productMaturityCounts.PARTNERSHIP_WATCH}`,
    `- Overflow Watch: ${report.productMaturityCounts.OVERFLOW_WATCH}`,
    `- Disqualified: ${report.productMaturityCounts.DISQUALIFIED}`,
    ``,
    `## Canonical dry-run`,
    `- Patch count: **${report.canonicalDryRun.patchCount}** (founder approval required before store write)`,
    ``,
    `## Paid research proposal`,
    `- Authorized: **false**`,
    `- Ceiling: $${report.proposedPaidBatch.ceilingUsd}`,
    `- IDs: ${(report.proposedPaidBatch.opportunityIds || []).map((id) => `\`${id}\``).join(", ")}`,
    ``,
    `See \`01-record-level-scorecard.json\` for all 88 rows.`,
    ``,
  ];
  return lines.join("\n");
}
