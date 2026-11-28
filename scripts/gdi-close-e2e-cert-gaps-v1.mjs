#!/usr/bin/env node
/**
 * GDI close E2E certification gaps V1 — dry-run orchestrator.
 *
 *   node scripts/gdi-close-e2e-cert-gaps-v1.mjs [--hotel-id recnPB3Q02NO8Ma1o]
 *       [--dry-run] [--prior-artifacts <json>] [--report-dir <dir>]
 *       [--write-annotated-package --out-package-dir <isolated dir>]
 *
 * Default is dry-run: loads the STAGED package, applies evidence recovery +
 * buying-authority annotation IN MEMORY, scores before/after, and writes
 * reports. It NEVER: writes Airtable, calls Findymail/Exa/Surfe, spends credits,
 * touches the STAGED/CURRENT pointer, or invents contact details.
 *
 * `--write-annotated-package` is honored ONLY together with `--out-package-dir`
 * (an isolated validation directory outside the publication-package roots).
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { fingerprintPublicationContent } from "../lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js";
import {
  buildHotelContactBestEffortLedger,
  evaluateContactBestEffortCertificationGate,
  CONTACT_CHANNEL_RESEARCH_STATUS,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-best-effort-v1.js";
import {
  annotateHotelOpportunitySet,
  ALTERNATIVE_ENTRY_RESEARCH_STATUS,
} from "../lib/group-demand-intelligence/workflow/gdi-buying-authority-annotation-v1.js";
import { applyEvidenceRecoveryToOpportunities } from "../lib/group-demand-intelligence/workflow/gdi-contact-channel-evidence-recovery-v1.js";
import {
  assessFindymailEmailEligibility,
  looksLikeFullPersonName,
  normalizePersonNameForEnrichment,
} from "../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import { readFindymailUsageLedger } from "../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const GDI_CLOSE_E2E_CERT_GAPS_V1 = "gdi-close-e2e-cert-gaps-v1";
const DEFAULT_HOTEL_ID = "recnPB3Q02NO8Ma1o"; // Wyndham San Diego Bayside
const DEFAULT_REPORT_DIR = path.join(ROOT, "reports/gdi-close-e2e-cert-gaps-v1");

/**
 * Planning estimates only (no spend). Exa search list price per
 * exa-agent-provider-adapter-v1 assumption note: search = $0.005.
 */
export const BUDGET_ASSUMPTIONS = Object.freeze({
  exaSearchUsd: 0.005,
  linkedinSearchesPerPersonLow: 1,
  linkedinSearchesPerPersonHigh: 2,
  phoneSearchesPerPersonLow: 1,
  phoneSearchesPerPersonHigh: 3,
  altEntrySearchesPerOpportunityLow: 2,
  altEntrySearchesPerOpportunityHigh: 4,
  findymailFindCreditsWhenFound: 1,
  findymailFindCreditsWhenNotFound: 0,
  note:
    "Planning estimates, not quotes. Nothing is authorized or auto-spent by this script.",
});

function parseArgs(argv) {
  const a = {
    hotelId: DEFAULT_HOTEL_ID,
    dryRun: true,
    writeAnnotatedPackage: false,
    outPackageDir: null,
    priorArtifacts: null,
    reportDir: DEFAULT_REPORT_DIR,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const t = argv[i];
    const next = () => argv[++i];
    if (t === "--hotel-id") a.hotelId = next();
    else if (t === "--dry-run") a.dryRun = true;
    else if (t === "--write-annotated-package") a.writeAnnotatedPackage = true;
    else if (t === "--out-package-dir") a.outPackageDir = next();
    else if (t === "--prior-artifacts") a.priorArtifacts = next();
    else if (t === "--report-dir") a.reportDir = path.resolve(next());
    else if (t === "--help" || t === "-h") a.help = true;
    else throw new Error(`Unknown argument: ${t}`);
  }
  return a;
}

function sha256File(file) {
  return fs.existsSync(file)
    ? crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")
    : null;
}

function writeJson(dir, name, obj) {
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, name);
  fs.writeFileSync(file, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  return file;
}

/** Pre-fix identity rule (kept only to explain the historic eligible=0 result). */
function legacyLooksLikeFullPersonName(name) {
  const n = String(name || "").replace(/\s+/g, " ").trim();
  if (!n || n.length < 3) return false;
  if (/\b(phd|mba|cpa)\b/i.test(n) && n.split(/\s+/).length < 2) return false;
  if (/\b[A-Za-z]\.$/.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length >= 2 && /^[A-Za-z]$/.test(parts[parts.length - 1])) return false;
  if (!/^[A-Za-z][A-Za-z .'\-]{1,100}$/.test(n)) return false;
  return parts.length >= 2;
}

function scoreboard(ledger, gate) {
  const s = ledger.scorecard;
  return {
    opportunityCount: s.opportunityCount,
    uniqueCommercialContacts: s.uniqueCommercialContacts,
    emailVerifiedCoverage: s.emailVerifiedCoverage,
    telephoneVerifiedCoverage: s.telephoneVerifiedCoverage,
    linkedinVerifiedCoverage: s.linkedinVerifiedCoverage,
    emailNotYetResearched: s.emailNotYetResearched,
    telephoneNotYetResearched: s.telephoneNotYetResearched,
    linkedinNotYetResearched: s.linkedinNotYetResearched,
    findymailEligibleRemaining: s.findymailEligibleRemaining,
    opportunitiesMissingAuthorityAnnotation: s.opportunitiesMissingAuthorityAnnotation,
    opportunitiesWithVerifiedDirectBuyer: s.opportunitiesWithVerifiedDirectBuyer,
    opportunitiesRequiringAlternativeResearch: s.opportunitiesRequiringAlternativeResearch,
    alternativeResearchPresent: s.alternativeResearchPresent,
    opportunitiesWithAlternativeContacts: s.opportunitiesWithAlternativeContacts,
    gateOk: gate.ok,
    gateFailures: gate.failures,
  };
}

function notYetPeople(ledger, channel) {
  return ledger.uniquePersons.filter(
    (p) => p.channels[channel].status === CONTACT_CHANNEL_RESEARCH_STATUS.NOT_YET_RESEARCHED,
  );
}

function buildFindymailReconciliation({ before, after, afterOpps, ledgerRows }) {
  const beforeNotYet = notYetPeople(before, "email");
  const afterNotYetKeys = new Set(notYetPeople(after, "email").map((p) => p.personKey));
  const orgFor = (p) => p.organization || null;
  const people = beforeNotYet.map((p) => {
    const evidence = ledgerRows.filter(
      (r) => String(r.personKey || "").toLowerCase() === p.personKey,
    );
    const norm = normalizePersonNameForEnrichment(p.fullName);
    const afterPerson = after.uniquePersons.find((x) => x.personKey === p.personKey);
    return {
      personKey: p.personKey,
      fullName: p.fullName,
      organization: orgFor(p),
      roleSlots: p.roleSlots,
      opportunityIds: p.opportunityIds,
      findymailReasonBeforeCode: p.findymail.reason,
      legacyIdentityRulePassed: legacyLooksLikeFullPersonName(p.fullName),
      currentIdentityRulePassed: looksLikeFullPersonName(p.fullName),
      nameNormalization: norm.changed ? norm : null,
      ledgerRows: evidence.map((r) => ({
        at: r.at,
        operation: r.operation,
        resultDisposition: r.resultDisposition,
        emailFound: r.emailFound === true,
        creditsDebited: r.creditsDebited ?? null,
        piiPersisted: r.piiPersisted === true,
      })),
      emailStatusAfterRecovery: afterPerson?.channels.email.status || null,
      emailStillNotYetAfterRecovery: afterNotYetKeys.has(p.personKey),
      findymailEligibleAfter: afterPerson?.findymail.eligible === true,
      findymailReasonAfter: afterPerson?.findymail.reason || null,
      explanation: evidence.length
        ? "Findymail already queried this person (ALREADY_IN_LEDGER), so it is correctly not re-eligible; the ledger result had never been hydrated onto package contactChannelResearch, leaving the channel NOT_YET_RESEARCHED."
        : !p.findymail.eligible && p.findymail.reason === "IDENTITY_INCOMPLETE"
          ? "Identity check rejected parentheses/comma credentials; no Findymail query was ever made."
          : "No ledger evidence; eligibility determined by current rules.",
    };
  });

  const reasonCounts = (ledger) =>
    ledger.uniquePersons.reduce((acc, p) => {
      const r = p.findymail.reason || "NONE";
      acc[r] = (acc[r] || 0) + 1;
      return acc;
    }, {});

  return {
    version: GDI_CLOSE_E2E_CERT_GAPS_V1,
    question:
      "Why was Findymail eligibleRemaining=0 while 4 unique people had email NOT_YET_RESEARCHED?",
    answer:
      "Eligibility (assessFindymailEmailEligibility) and the certification gate's NOT_YET count measure different things. Eligibility excludes people already present in the Findymail usage ledger (ALREADY_IN_LEDGER) and people whose display name failed the full-name check (IDENTITY_INCOMPLETE). The gate counts a channel NOT_YET until package contactChannelResearch carries a researched flag. Three people were already researched via Findymail (ledger) but never hydrated onto the package; one person ('Alejandra (Alex) Gutierrez, PhD') was blocked by looksLikeFullPersonName rejecting parentheses/comma credentials, so she was never queried.",
    eligibleRemainingBeforeWithLegacyIdentityRule: before.uniquePersons.filter((p) =>
      assessFindymailEmailEligibility(
        {
          name: p.fullName,
          organization: p.organization,
          email: p.channels.email.value,
        },
        { organizationName: p.organization },
        { ledger: ledgerRows },
      ).eligible &&
      legacyLooksLikeFullPersonName(p.fullName),
    ).length,
    eligibleRemainingBeforeWithFixedIdentityRule: before.scorecard.findymailEligibleRemaining,
    eligibleRemainingAfter: after.scorecard.findymailEligibleRemaining,
    emailNotYetBefore: before.scorecard.emailNotYetResearched,
    emailNotYetAfter: after.scorecard.emailNotYetResearched,
    findymailReasonCountsBefore: reasonCounts(before),
    findymailReasonCountsAfter: reasonCounts(after),
    people,
    newlyEligibleAfterIdentityFix: people
      .filter((p) => p.findymailEligibleAfter)
      .map((p) => ({
        personKey: p.personKey,
        fullName: p.fullName,
        providerQueryName: p.nameNormalization?.normalized || p.fullName,
        estimatedFinderCredits: BUDGET_ASSUMPTIONS.findymailFindCreditsWhenFound,
        creditsIfNotFound: BUDGET_ASSUMPTIONS.findymailFindCreditsWhenNotFound,
        autoSpend: false,
      })),
    opportunitiesExamined: afterOpps.length,
  };
}

function buildBudgetRequest({ after, annotatedOpps, annotationReport, findymail }) {
  const A = BUDGET_ASSUMPTIONS;
  const linkedinPeople = notYetPeople(after, "linkedin");
  const phonePeople = notYetPeople(after, "telephone");
  const emailPeople = notYetPeople(after, "email");

  const byPerson = new Map();
  const touch = (p) => {
    const row = byPerson.get(p.personKey) || {
      personKey: p.personKey,
      fullName: p.fullName,
      organization: p.organization,
      identityFullyVerified: p.identityFullyVerified,
      channels: [],
    };
    byPerson.set(p.personKey, row);
    return row;
  };
  for (const p of linkedinPeople) touch(p).channels.push("LINKEDIN_EXA_PUBLIC_SEARCH");
  for (const p of phonePeople) touch(p).channels.push("TELEPHONE_EXA_PUBLIC_SEARCH");
  const findymailKeys = new Set(findymail.newlyEligibleAfterIdentityFix.map((p) => p.personKey));
  for (const p of emailPeople) {
    if (findymailKeys.has(p.personKey)) touch(p).channels.push("EMAIL_FINDYMAIL_FIND_FROM_NAME");
    else touch(p).channels.push("EMAIL_NOT_ELIGIBLE_FOR_FINDYMAIL");
  }

  const altNeeded = annotationReport.rows.filter(
    (r) =>
      r.alternativeEntryResearchStatus ===
      ALTERNATIVE_ENTRY_RESEARCH_STATUS.REQUIRED_NOT_COMPLETED,
  );

  const exaLinkedinLow = linkedinPeople.length * A.linkedinSearchesPerPersonLow;
  const exaLinkedinHigh = linkedinPeople.length * A.linkedinSearchesPerPersonHigh;
  const exaPhoneLow = phonePeople.length * A.phoneSearchesPerPersonLow;
  const exaPhoneHigh = phonePeople.length * A.phoneSearchesPerPersonHigh;
  const exaAltLow = altNeeded.length * A.altEntrySearchesPerOpportunityLow;
  const exaAltHigh = altNeeded.length * A.altEntrySearchesPerOpportunityHigh;
  const usd = (n) => Number((n * A.exaSearchUsd).toFixed(4));

  return {
    version: GDI_CLOSE_E2E_CERT_GAPS_V1,
    status: "REQUEST_ONLY_NOT_AUTHORIZED",
    autoSpend: false,
    founderAuthorizationRequired: true,
    surfeIncluded: false,
    assumptions: A,
    lineItems: [
      {
        id: "LINKEDIN_PUBLIC_SEARCH_EXA",
        provider: "EXA",
        uniquePeople: linkedinPeople.length,
        searchesLow: exaLinkedinLow,
        searchesHigh: exaLinkedinHigh,
        estimatedUsdLow: usd(exaLinkedinLow),
        estimatedUsdHigh: usd(exaLinkedinHigh),
        method: "Individual-profile public search by verified full name + organization; verified-only; no guessing.",
      },
      {
        id: "TELEPHONE_PUBLIC_SEARCH_EXA",
        provider: "EXA",
        uniquePeople: phonePeople.length,
        searchesLow: exaPhoneLow,
        searchesHigh: exaPhoneHigh,
        estimatedUsdLow: usd(exaPhoneLow),
        estimatedUsdHigh: usd(exaPhoneHigh),
        method: "Official-page / directory public search for person-direct telephone; organization main lines are not person phones.",
      },
      {
        id: "ALTERNATIVE_ENTRY_RESEARCH_EXA",
        provider: "EXA",
        opportunities: altNeeded.length,
        opportunityIds: altNeeded.map((r) => r.opportunityId),
        searchesLow: exaAltLow,
        searchesHigh: exaAltHigh,
        estimatedUsdLow: usd(exaAltLow),
        estimatedUsdHigh: usd(exaAltHigh),
        method: "Named alternative / secondary entry points for opportunities whose primary buying authority is not verified. Required by gate once annotation sets requiresAlternativeEntryPoints.",
      },
      {
        id: "EMAIL_FINDYMAIL_NEWLY_ELIGIBLE",
        provider: "FINDYMAIL",
        uniquePeople: findymail.newlyEligibleAfterIdentityFix.length,
        people: findymail.newlyEligibleAfterIdentityFix,
        creditsMax: findymail.newlyEligibleAfterIdentityFix.length * A.findymailFindCreditsWhenFound,
        creditsMin: 0,
        note: "Credit-based; 0 credits when not found. Eligible only after identity normalization fix. Not auto-spent.",
      },
    ],
    totals: {
      exaSearchesLow: exaLinkedinLow + exaPhoneLow + exaAltLow,
      exaSearchesHigh: exaLinkedinHigh + exaPhoneHigh + exaAltHigh,
      exaEstimatedUsdLow: usd(exaLinkedinLow + exaPhoneLow + exaAltLow),
      exaEstimatedUsdHigh: usd(exaLinkedinHigh + exaPhoneHigh + exaAltHigh),
      findymailCreditsMax: findymail.newlyEligibleAfterIdentityFix.length,
    },
    remainingNotYetByChannel: {
      email: emailPeople.length,
      telephone: phonePeople.length,
      linkedin: linkedinPeople.length,
    },
    perPerson: [...byPerson.values()],
    opportunityCountConsidered: annotatedOpps.length,
  };
}

function buildRemainingGaps({ after, gate, annotationReport, budget, recovery }) {
  const s = after.scorecard;
  const guidance = {
    EMAIL_RESEARCH_INCOMPLETE: "Authorize Findymail for newly eligible person(s) (see 03 report), or accept research-completed-not-found.",
    TELEPHONE_RESEARCH_INCOMPLETE: "Authorize Exa public telephone search (03 report line TELEPHONE_PUBLIC_SEARCH_EXA).",
    LINKEDIN_RESEARCH_INCOMPLETE: "Authorize Exa individual-profile LinkedIn public search (03 report line LINKEDIN_PUBLIC_SEARCH_EXA).",
    FINDYMAIL_ELIGIBLE_GAPS_REMAIN: "Newly eligible Findymail people remain; authorize or decline spend explicitly.",
    ALTERNATIVE_ENTRY_RESEARCH_INCOMPLETE: "Primary buying authority is unverified for these opportunities and no named alternative entry point exists; research required (03 report line ALTERNATIVE_ENTRY_RESEARCH_EXA).",
    BUYING_AUTHORITY_ANNOTATION_INCOMPLETE: "Annotation incomplete — should be cleared by this script's annotation pass.",
    ABBREVIATED_IDENTITY_MISCLASSIFIED_AS_VERIFIED: "Investigate identity classification.",
    NO_QUALIFIED_OPPORTUNITIES: "No opportunities in package.",
  };
  return {
    version: GDI_CLOSE_E2E_CERT_GAPS_V1,
    gateOkAfter: gate.ok,
    failuresAfter: gate.failures.map((f) => ({ failure: f, requiredAction: guidance[f] || "Review." })),
    counts: {
      emailNotYet: s.emailNotYetResearched,
      telephoneNotYet: s.telephoneNotYetResearched,
      linkedinNotYet: s.linkedinNotYetResearched,
      findymailEligibleRemaining: s.findymailEligibleRemaining,
      opportunitiesRequiringAlternativeResearch: s.opportunitiesRequiringAlternativeResearch,
      alternativeResearchPresent: s.alternativeResearchPresent,
      opportunitiesMissingAuthorityAnnotation: s.opportunitiesMissingAuthorityAnnotation,
    },
    notes: [
      "Verified direct lodging decision-makers: " +
        annotationReport.verifiedDirectLodgingDecisionMakers +
        " — no package primary carries explicit WHO/HOW VERIFIED_DIRECT_BUYER plus lodging-authority verification; titles alone are never upgraded.",
      "Annotation + recovery are in-memory; the STAGED package was not modified. A package rematerialization (new version) under separate authorization is required for the app to reflect these flags.",
      "Channel research that remains NOT_YET requires authorized provider spend (see 03 report). Nothing was spent.",
      recovery.uniquePeopleChanged
        ? `Recovered researched flags for ${recovery.uniquePeopleChanged} unique people from existing evidence (no values invented).`
        : "No recoverable evidence found.",
    ],
    budgetRequestRef: "03-CONSOLIDATED_INCREMENTAL_RESEARCH_BUDGET_REQUEST.json",
    budgetTotals: budget.totals,
  };
}

function isInside(child, parent) {
  const rel = path.relative(path.resolve(parent), path.resolve(child));
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

function writeAnnotatedPackage({ pkg, annotatedOpps, outPackageDir, storeRoots }) {
  const out = path.resolve(outPackageDir);
  for (const r of storeRoots) {
    if (isInside(out, r)) {
      throw new Error(
        `Refusing to write annotated package inside a publication-package root: ${r}`,
      );
    }
  }
  const publicationVersion = `${pkg.publicationVersion}__annotated_validation`;
  const body = {
    ...pkg,
    publicationVersion,
    publicationId: `${pkg.publicationId}__annotated_validation`,
    opportunities: annotatedOpps,
    lifecycleState: "ANNOTATED_VALIDATION_ONLY",
    approvalStatus: "NOT_FOR_PUBLICATION",
    notes: `Isolated validation copy of ${pkg.publicationVersion}. Never a STAGED/CURRENT pointer target.`,
  };
  const fp = fingerprintPublicationContent({
    hotelId: body.hotelId,
    publicationVersion: body.publicationVersion,
    presentationSchemaVersion: body.presentationSchemaVersion,
    applicationCompatibilityVersion: body.applicationCompatibilityVersion,
    sortOrder: body.sortOrder,
    opportunities: body.opportunities,
  });
  body.contentFingerprintSha256 = fp.contentFingerprintSha256;
  const file = path.join(out, body.hotelId, publicationVersion, "package.json");
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
  return { file, publicationVersion, contentFingerprintSha256: body.contentFingerprintSha256 };
}

export async function run(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log("See header of scripts/gdi-close-e2e-cert-gaps-v1.mjs");
    return { ok: true };
  }
  if (args.writeAnnotatedPackage && !args.outPackageDir) {
    throw new Error("--write-annotated-package requires --out-package-dir (isolated validation dir)");
  }

  const store = createGdiPublicationPackageStore({});
  const pointer = store.readPointer(args.hotelId, "STAGED");
  if (!pointer) throw new Error(`No STAGED pointer for hotel ${args.hotelId}`);
  const loaded = store.loadPackage(args.hotelId, pointer.publicationVersion);
  if (!loaded.ok) throw new Error(`Cannot load STAGED package: ${loaded.error}`);
  const pkg = loaded.package;
  const pointerFile = path.join(path.dirname(path.dirname(loaded.path)), "STAGED.json");
  const pointerShaBefore = sha256File(pointerFile);

  const ledgerRows = readFindymailUsageLedger({ repoRoot: ROOT });
  const priorArtifacts = args.priorArtifacts
    ? JSON.parse(fs.readFileSync(path.resolve(args.priorArtifacts), "utf8"))
    : [];

  const scoreOpts = { hotelId: args.hotelId, findymailLedger: ledgerRows, repoRoot: ROOT };

  // 1-2. Before
  const beforeLedger = buildHotelContactBestEffortLedger(pkg.opportunities, scoreOpts);
  const beforeGate = evaluateContactBestEffortCertificationGate(beforeLedger);

  // 3. Evidence recovery, then annotation (in memory)
  const recovered = applyEvidenceRecoveryToOpportunities(pkg.opportunities, {
    findymailLedger: ledgerRows,
    priorResearchArtifacts: priorArtifacts,
    normalizeIdentity: true,
  });
  const annotated = annotateHotelOpportunitySet(recovered.opportunities);

  // 4. After
  const afterLedger = buildHotelContactBestEffortLedger(annotated.opportunities, scoreOpts);
  const afterGate = evaluateContactBestEffortCertificationGate(afterLedger);

  // 5. Reports
  const findymail = buildFindymailReconciliation({
    before: beforeLedger,
    after: afterLedger,
    afterOpps: annotated.opportunities,
    ledgerRows,
  });
  const budget = buildBudgetRequest({
    after: afterLedger,
    annotatedOpps: annotated.opportunities,
    annotationReport: annotated.report,
    findymail,
  });
  const remaining = buildRemainingGaps({
    after: afterLedger,
    gate: afterGate,
    annotationReport: annotated.report,
    budget,
    recovery: recovered.report,
  });

  let annotatedPackage = null;
  if (args.writeAnnotatedPackage) {
    const storeRoots = [
      path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
      path.join(ROOT, "data/group-demand-intelligence/publication-packages"),
      path.dirname(path.dirname(loaded.path)),
    ];
    annotatedPackage = writeAnnotatedPackage({
      pkg,
      annotatedOpps: annotated.opportunities,
      outPackageDir: args.outPackageDir,
      storeRoots,
    });
  }

  const pointerShaAfter = sha256File(pointerFile);
  const before = scoreboard(beforeLedger, beforeGate);
  const after = scoreboard(afterLedger, afterGate);
  const scorecard = {
    version: GDI_CLOSE_E2E_CERT_GAPS_V1,
    generatedAt: new Date().toISOString(),
    mode: args.writeAnnotatedPackage ? "DRY_RUN_WITH_ISOLATED_PACKAGE_WRITE" : "DRY_RUN",
    hotelId: args.hotelId,
    stagedPackage: {
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      opportunityCount: pkg.opportunityCount,
      stagedPointerSha256Before: pointerShaBefore,
      stagedPointerSha256After: pointerShaAfter,
      stagedPointerUnchanged: pointerShaBefore === pointerShaAfter,
    },
    guarantees: {
      airtableWrites: 0,
      providerCalls: 0,
      creditsSpent: 0,
      surfeUsed: false,
      uiChanges: false,
      contactValuesInvented: false,
    },
    before,
    beforeNote: {
      findymailEligibleRemainingWithLegacyIdentityRule:
        findymail.eligibleRemainingBeforeWithLegacyIdentityRule,
      explanation:
        "`before` is scored with the current (fixed) looksLikeFullPersonName, so 'Alejandra (Alex) Gutierrez, PhD' already counts as Findymail-eligible. With the legacy identity rule eligibleRemaining was 0 (IDENTITY_INCOMPLETE) — see 01-FINDYMAIL_ELIGIBILITY_RECONCILIATION.json.",
    },
    after,
    delta: {
      emailNotYetResearched: after.emailNotYetResearched - before.emailNotYetResearched,
      telephoneNotYetResearched: after.telephoneNotYetResearched - before.telephoneNotYetResearched,
      linkedinNotYetResearched: after.linkedinNotYetResearched - before.linkedinNotYetResearched,
      opportunitiesMissingAuthorityAnnotation:
        after.opportunitiesMissingAuthorityAnnotation - before.opportunitiesMissingAuthorityAnnotation,
      findymailEligibleRemaining: after.findymailEligibleRemaining - before.findymailEligibleRemaining,
    },
    historicGateFailuresReportedByFounder: [
      "EMAIL_RESEARCH_INCOMPLETE",
      "TELEPHONE_RESEARCH_INCOMPLETE",
      "LINKEDIN_RESEARCH_INCOMPLETE",
      "BUYING_AUTHORITY_ANNOTATION_INCOMPLETE",
    ],
    annotatedPackage,
  };

  const files = [
    writeJson(args.reportDir, "00-BEFORE_AFTER_SCORECARD.json", scorecard),
    writeJson(args.reportDir, "01-FINDYMAIL_ELIGIBILITY_RECONCILIATION.json", findymail),
    writeJson(args.reportDir, "02-BUYING_AUTHORITY_ANNOTATION_REPORT.json", annotated.report),
    writeJson(args.reportDir, "03-CONSOLIDATED_INCREMENTAL_RESEARCH_BUDGET_REQUEST.json", budget),
    writeJson(args.reportDir, "04-CHANNEL_EVIDENCE_RECOVERY.json", recovered.report),
    writeJson(args.reportDir, "05-REMAINING_GAPS.json", remaining),
  ];

  console.log(JSON.stringify({ ok: true, mode: scorecard.mode, before, after, files }, null, 2));
  return { ok: true, scorecard, files };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  run().catch((err) => {
    console.error(err?.stack || err);
    process.exit(1);
  });
}
