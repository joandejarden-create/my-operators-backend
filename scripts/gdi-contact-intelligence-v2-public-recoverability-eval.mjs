/**
 * Free public recoverability eval for Findymail pilot cohort (V2).
 * No Findymail / Surfe / PDL / Prospeo / FullEnrich calls.
 * Does not print full email local-parts in summary (redacts).
 *
 *   node scripts/gdi-contact-intelligence-v2-public-recoverability-eval.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import {
  recoverPublicContactChannels,
  mergePublicChannelsOntoOpportunity,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";
import { assessPublicContactVerification } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";
import { resolvePreferredEmployerDomain } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-employer-domain-v1.js";
import { prioritizePublicContactCandidateUrls } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-findymail-public-optimization",
);
const PILOT =
  "C:/Users/joand/AppData/Local/Temp/dealality-gdi-findymail-benchmark-v1-20261121/reports/gdi-findymail-coverage-benchmark-v1/PILOT_RESULTS.json";
const COHORT =
  "C:/Users/joand/AppData/Local/Temp/dealality-gdi-findymail-benchmark-v1-20261121/reports/gdi-findymail-coverage-benchmark-v1/COHORT.json";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function redactEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@")) return null;
  const [local, domain] = e.split("@");
  const mask =
    local.length <= 2 ? "**" : `${local[0]}***${local[local.length - 1]}`;
  return `${mask}@${domain}`;
}

const pilot = JSON.parse(fs.readFileSync(PILOT, "utf8"));
const cohortDoc = JSON.parse(fs.readFileSync(COHORT, "utf8"));
const cohortByKey = new Map(
  cohortDoc.cohort.map((c) => [c.personIdentityKey, c]),
);

const rows = [];
for (const r of pilot.rows || []) {
  const c = cohortByKey.get(r.personIdentityKey) || {};
  const strictPerson = looksLikePersonName(r.name);
  const domainResolve = resolvePreferredEmployerDomain({
    organizationDomain: r.domain,
    organizationName: r.organization,
    sourceUrls: [c.sourceUrl].filter(Boolean),
  });

  const seedUrls = prioritizePublicContactCandidateUrls(
    [c.sourceUrl].filter(Boolean),
  );
  const opportunity = {
    id: r.opportunityId,
    organizationName: r.organization,
    organizationDomain: domainResolve.preferredDomain || r.domain,
    candidateUrls: seedUrls,
    primaryContact: {
      name: r.name,
      role: r.role,
      sourceUrl: c.sourceUrl || null,
      officialContactUrl: c.sourceUrl || null,
    },
  };

  let publicEmail = null;
  let publicPhone = null;
  let pagesFetched = 0;
  let recoveryOk = false;
  let evidenceUrls = [];

  if (strictPerson && seedUrls.length) {
    const recovered = await recoverPublicContactChannels(opportunity, {
      maxUrls: Math.min(3, seedUrls.length),
      timeoutMs: 15000,
      skipIfComplete: false,
    });
    pagesFetched = Math.min(3, seedUrls.length);
    recoveryOk = recovered?.ok === true;
    if (recovered?.found || recovered?.opportunity) {
      const merged = mergePublicChannelsOntoOpportunity(
        recovered.opportunity || opportunity,
        recovered.found || {},
      );
      const after = assessPublicContactVerification(merged.opportunity);
      if (after.hasVerifiedDirectEmail) {
        publicEmail = after.email?.value || merged.opportunity.primaryContact?.email;
      } else if (after.hasVerifiedOrgEmailRoute) {
        publicEmail = after.email?.value;
      }
      if (after.hasVerifiedDirectPhone || after.hasVerifiedOrgPhoneRoute) {
        publicPhone = after.phone?.value || merged.opportunity.primaryContact?.phone;
      }
      evidenceUrls = [
        after.email?.sourceUrl || recovered.found?.sourceUrl || c.sourceUrl,
        after.phone?.sourceUrl || null,
      ].filter(Boolean);
    }
  }

  const findyAccepted =
    r.classification === "ACCEPTED_PROVIDER_VERIFIED" && strictPerson;
  const findyReturned = Boolean(r.emailRedacted);
  const independentPublicEmail = Boolean(publicEmail);
  // Do NOT treat Findymail success as public verification.
  let incrementalClass = "NO_PUBLIC_EMAIL_FOUND";
  if (!strictPerson) incrementalClass = "INELIGIBLE_NON_PERSON";
  else if (r.classification === "EMPLOYER_MISMATCH") {
    incrementalClass = "FINDYMAIL_REJECTED_EMPLOYER_MISMATCH";
  } else if (r.classification === "NO_MATCH" && !independentPublicEmail) {
    incrementalClass = "BOTH_MISS";
  } else if (independentPublicEmail && findyAccepted) {
    incrementalClass = "INDEPENDENTLY_CORROBORATED_PUBLIC";
  } else if (independentPublicEmail && !findyAccepted) {
    incrementalClass = "PUBLIC_ONLY_FINDYMAIL_MISS";
  } else if (!independentPublicEmail && findyAccepted) {
    incrementalClass = "FINDYMAIL_ONLY_PROVIDER_COVERAGE";
  } else if (r.historicalSurfeOrPublicOverlap && findyAccepted) {
    incrementalClass = "HISTORICAL_OVERLAP_NOT_INDEPENDENTLY_RECONFIRMED";
  }

  rows.push({
    personIdentityKey: r.personIdentityKey,
    name: r.name,
    organization: r.organization,
    pilotDomain: r.domain,
    preferredEmployerDomain: domainResolve.preferredDomain,
    domainClass: domainResolve.preferredClass,
    strictPerson,
    namedPersonVerifiedHeuristic: strictPerson,
    employerPresent: Boolean(r.organization),
    findymailClassification: r.classification,
    findymailReturnedEmail: findyReturned,
    findymailPersonStrictAccepted: findyAccepted,
    gdiHadVerifiedEmailBeforePilot: false,
    historicalOverlapFlag: r.historicalSurfeOrPublicOverlap === true,
    publicRecoveryAttempted: strictPerson && Boolean(c.sourceUrl),
    publicPagesFetched: pagesFetched,
    publicRecoveryOk: recoveryOk,
    independentlyVerifiedPublicEmail: independentPublicEmail,
    independentlyVerifiedPublicPhone: Boolean(publicPhone),
    publicEmailRedacted: publicEmail ? redactEmail(publicEmail) : null,
    publicPhonePresent: Boolean(publicPhone),
    evidenceUrls,
    incrementalClass,
    multiHotelEligible: false,
    furtherPublicResearchJustified:
      strictPerson &&
      !independentPublicEmail &&
      domainResolve.preferredDomain &&
      !isWeakHint(domainResolve.preferredClass),
  });
}

function isWeakHint(cls) {
  return (
    cls === "EVENT_MICROSITE" ||
    cls === "EXPO_PLATFORM" ||
    cls === "DIRECTORY_AGGREGATOR"
  );
}

const summary = {
  cohort: rows.length,
  strictPersons: rows.filter((r) => r.strictPerson).length,
  findymailPersonStrictAccepted: rows.filter((r) => r.findymailPersonStrictAccepted)
    .length,
  independentlyVerifiedPublicEmails: rows.filter(
    (r) => r.independentlyVerifiedPublicEmail,
  ).length,
  independentlyVerifiedPublicPhones: rows.filter(
    (r) => r.independentlyVerifiedPublicPhone,
  ).length,
  findymailOnlyProviderCoverage: rows.filter(
    (r) => r.incrementalClass === "FINDYMAIL_ONLY_PROVIDER_COVERAGE",
  ).length,
  independentlyCorroboratedPublic: rows.filter(
    (r) => r.incrementalClass === "INDEPENDENTLY_CORROBORATED_PUBLIC",
  ).length,
  publicOnlyFindymailMiss: rows.filter(
    (r) => r.incrementalClass === "PUBLIC_ONLY_FINDYMAIL_MISS",
  ).length,
  bothMiss: rows.filter((r) => r.incrementalClass === "BOTH_MISS").length,
  ineligibleNonPerson: rows.filter((r) => r.incrementalClass === "INELIGIBLE_NON_PERSON")
    .length,
  employerMismatchRejected: rows.filter(
    (r) => r.incrementalClass === "FINDYMAIL_REJECTED_EMPLOYER_MISMATCH",
  ).length,
  note:
    "Findymail-returned emails are never counted as public-source verified. Independent public recovery uses authoritative source URLs only (free fetch).",
};

fs.mkdirSync(OUT_DIR, { recursive: true });
const payload = {
  generatedAt: new Date().toISOString(),
  mode: "FREE_PUBLIC_RECOVERABILITY_V2",
  paidProviders: {
    findymail: false,
    surfe: false,
    pdl: false,
    prospeo: false,
    fullEnrich: false,
  },
  summary,
  rows,
};
fs.writeFileSync(
  path.join(OUT_DIR, "PUBLIC_RECOVERABILITY_EVAL.json"),
  JSON.stringify(payload, null, 2),
);
console.log(JSON.stringify({ ok: true, summary, out: OUT_DIR }, null, 2));
