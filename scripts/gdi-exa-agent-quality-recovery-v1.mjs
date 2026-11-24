#!/usr/bin/env node
/**
 * $0 quality recovery — reconcile 30-opp wave scorecard under hardened gates.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WAVE =
  "reports/gdi-exa-agent-30-opportunity-live-wave-v1";
const OUT =
  "reports/gdi-exa-agent-quality-recovery-v1";

const {
  isPlausibleBuyerPersonName,
  personQualifiesForWhoFinalization,
  employerAffiliationPlausible,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  classifyVerifiedAgentCandidate,
  AGENT_CANDIDATE_TYPE,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-exa-agent-contact-escalation-v1.js"
);
const { assessWhoHowCompletenessGates } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js"
);
const { evaluateNamedContactCoverageStandard } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);
const {
  assessPublicContactVerification,
  isPersonLinkedInProfileUrl,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js"
);

function ensureOut() {
  fs.mkdirSync(path.join(ROOT, OUT), { recursive: true });
  fs.mkdirSync(path.join(ROOT, OUT, "staged-revalidation"), { recursive: true });
}

function writeJson(rel, data) {
  ensureOut();
  const fp = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  return fp;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadJson(rel) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
}

function reclassifyPerson(
  person,
  organizationName,
  orgAliases = [],
  fallbackSourceUrl = null,
) {
  if (!person?.name) {
    return {
      type: null,
      reason: "NO_PERSON",
      whoEligible: false,
      provenanceWeak: false,
    };
  }
  if (!isPlausibleBuyerPersonName(person.name)) {
    return {
      type: "REJECTED",
      reason: "JUNK_OR_IMPLAUSIBLE_NAME",
      whoEligible: false,
      provenanceWeak: false,
    };
  }
  const sourceUrl = person.sourceUrl || fallbackSourceUrl || null;
  const classified = classifyVerifiedAgentCandidate(
    {
      name: person.name,
      title: person.title || person.role,
      role: person.role || person.title,
      currentOrganization: person.employer || person.organization,
      organization: person.employer || person.organization,
      sourceUrl,
      sourceUrls: sourceUrl ? [sourceUrl] : [],
      relevanceEvidence: [
        person.title,
        person.classification,
        person.relevanceEvidence,
      ]
        .filter(Boolean)
        .join(" | "),
    },
    { organizationName, orgAliases },
  );
  const mapped = {
    ...person,
    sourceUrl,
    buyerClassification:
      classified.type === AGENT_CANDIDATE_TYPE.DIRECT_BUYER
        ? "DIRECT_BUYER"
        : classified.type === AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE
          ? "VERIFIED_VIABLE_ALTERNATIVE"
          : classified.type === AGENT_CANDIDATE_TYPE.SECONDARY_CONTACT
            ? "SECONDARY_CONTACT"
            : classified.type === AGENT_CANDIDATE_TYPE.REJECTED
              ? "REJECTED"
              : "RELEVANT_ORGANIZATIONAL_CONTACT",
    agentCandidateType: classified.type,
    agentClassificationReason: classified.reason,
    provenanceWeak: classified.provenanceWeak === true,
  };
  return {
    type: classified.type,
    reason: classified.reason,
    whoEligible: personQualifiesForWhoFinalization(mapped),
    provenanceWeak: classified.provenanceWeak === true,
    mapped,
  };
}

const allow = loadJson(`${WAVE}/ALLOWLIST.json`);
const live = loadJson(`${WAVE}/LIVE_RESULTS.json`);
const ledger = loadJson(`${WAVE}/ledgers/agent-live-ledger.json`);
const metricsPrior = fs.existsSync(
  path.join(ROOT, WAVE, "METRICS_CORRECTED.json"),
)
  ? loadJson(`${WAVE}/METRICS_CORRECTED.json`)
  : null;

const stagedDir = path.join(ROOT, WAVE, "staged-candidates");
const stagedIds = new Set(
  fs.existsSync(stagedDir)
    ? fs.readdirSync(stagedDir).map((f) => f.replace(/\.json$/, ""))
    : [],
);

const scorecardById = new Map(
  (live.scorecard || []).map((r) => [r.opportunityId, r]),
);
const ledgerById = new Map(
  (ledger.entries || []).map((e) => [e.opportunityId, e]),
);

const rows = [];
for (const a of allow.allowlist || []) {
  const prior = scorecardById.get(a.opportunityId) || {};
  const led = ledgerById.get(a.opportunityId) || {};
  const tracePath = path.join(ROOT, WAVE, "traces", `${a.opportunityId}.json`);
  const trace = fs.existsSync(tracePath) ? JSON.parse(fs.readFileSync(tracePath, "utf8")) : {};

  const primaryBefore = prior.primary || null;
  const secondaryBefore = prior.secondary || null;
  const classBefore = prior.contactType || primaryBefore?.classification || null;

  const orgAliases = String(a.organizationName || "")
    .split(/[/,;]/)
    .map((s) => s.trim())
    .filter((s) => s.length >= 3);

  const fallbackUrl = a.sourceUrl || null;
  const primaryRe = reclassifyPerson(
    primaryBefore,
    a.organizationName,
    orgAliases,
    fallbackUrl,
  );
  const secondaryRe = reclassifyPerson(
    secondaryBefore
      ? { ...secondaryBefore, classification: "SECONDARY_CONTACT" }
      : null,
    a.organizationName,
    orgAliases,
    fallbackUrl,
  );

  // Secondary: if plausible name + employer ok, keep as secondary even if pathway weak
  let secondaryAfter = null;
  if (
    secondaryBefore?.name &&
    isPlausibleBuyerPersonName(secondaryBefore.name) &&
    (employerAffiliationPlausible(
      {
        currentOrganization: secondaryBefore.employer,
        title: secondaryBefore.title,
      },
      a.organizationName,
    ) ||
      orgAliases.some((al) =>
        `${secondaryBefore.employer || ""} ${secondaryBefore.title || ""}`
          .toLowerCase()
          .includes(al.toLowerCase()),
      ))
  ) {
    secondaryAfter = {
      ...secondaryBefore,
      classification: "SECONDARY_CONTACT",
    };
  } else if (secondaryBefore?.name && !isPlausibleBuyerPersonName(secondaryBefore.name)) {
    secondaryAfter = null;
  }

  const primaryAfter =
    primaryRe.whoEligible && primaryRe.type !== "REJECTED"
      ? primaryRe.mapped
      : null;
  const classAfter = primaryAfter
    ? primaryAfter.buyerClassification
    : primaryRe.type === "REJECTED"
      ? "REJECTED"
      : null;

  let holdReason =
    primaryBefore &&
    !primaryAfter &&
    primaryRe.type !== "REJECTED" &&
    primaryRe.type === AGENT_CANDIDATE_TYPE.RELEVANT_ORGANIZATIONAL_CONTACT
      ? "HOLD_RELEVANT_ORG_NO_COMMERCIAL_PATHWAY"
      : primaryBefore && !primaryAfter && primaryRe.reason === "EMPLOYER_NOT_VERIFIED"
        ? "HOLD_EMPLOYER_VERIFICATION"
        : null;

  const oppForGates = {
    id: a.opportunityId,
    opportunityId: a.opportunityId,
    organizationName: a.organizationName,
    primaryContact: primaryAfter
      ? {
          name: primaryAfter.name,
          title: primaryAfter.title,
          role: primaryAfter.title,
          organization: primaryAfter.employer || primaryAfter.organization,
          email: primaryAfter.email,
          linkedinUrl: isPersonLinkedInProfileUrl(primaryAfter.linkedin)
            ? primaryAfter.linkedin
            : null,
          sourceUrl: primaryAfter.sourceUrl,
          buyerClassification: primaryAfter.buyerClassification,
          channelSourceUrl:
            prior.publicHow?.email && primaryAfter.email
              ? a.sourceUrl
              : primaryAfter.sourceUrl,
          contactDataOrigin: primaryAfter.email ? "PUBLIC_SOURCE" : null,
          channelLastVerifiedAt: primaryAfter.email ? "2026-11-24" : null,
        }
      : null,
    secondaryContact: secondaryAfter
      ? {
          name: secondaryAfter.name,
          title: secondaryAfter.title,
          organization: secondaryAfter.employer,
          buyerClassification: "SECONDARY_CONTACT",
        }
      : null,
    howContactResearchAttempted: true,
    howContactResearchStatus: prior.howResearchStatus || null,
  };

  const coverage = evaluateNamedContactCoverageStandard(oppForGates, {});
  const gates = assessWhoHowCompletenessGates(oppForGates, {});
  const verification = assessPublicContactVerification(oppForGates);
  // Corrected WHO: commercial primary only (not secondary-alone, not RELEVANT_ORG).
  const whoCompleteCorrected =
    Boolean(primaryAfter) &&
    (primaryAfter.buyerClassification === "DIRECT_BUYER" ||
      primaryAfter.buyerClassification === "VERIFIED_VIABLE_ALTERNATIVE");
  const whoFinalizationCorrected = whoCompleteCorrected;

  // Weak provenance HOLDs DIRECT_BUYER only; evidenced VIABLE_ALT may PASS on org source URL.
  if (
    !holdReason &&
    primaryAfter?.buyerClassification === "DIRECT_BUYER" &&
    primaryRe.provenanceWeak === true
  ) {
    holdReason = "HOLD_WEAK_PROVENANCE_URL";
  }

  const commercialPrimary =
    primaryAfter &&
    (primaryAfter.buyerClassification === "DIRECT_BUYER" ||
      primaryAfter.buyerClassification === "VERIFIED_VIABLE_ALTERNATIVE");

  let stagedDisposition = "REJECT";
  let stagedReason = "NO_QUALIFYING_WHO_PRIMARY";
  if (holdReason && primaryAfter) {
    stagedDisposition = "HOLD";
    stagedReason = holdReason;
  } else if (commercialPrimary) {
    stagedDisposition = "PASS";
    stagedReason = "QUALIFYING_COMMERCIAL_WHO";
  } else if (holdReason) {
    stagedDisposition = "HOLD";
    stagedReason = holdReason;
  } else if (primaryBefore && primaryRe.reason === "JUNK_OR_IMPLAUSIBLE_NAME") {
    stagedDisposition = "REJECT";
    stagedReason = "JUNK_OR_IMPLAUSIBLE_NAME";
  } else if (!prior.providerCalled && !prior.verifiedPrimary) {
    stagedDisposition = "REJECT";
    stagedReason = "NO_AGENT_RECOVERY";
  } else if (prior.providerCalled && !primaryBefore) {
    stagedDisposition = "REJECT";
    stagedReason = "AGENT_NO_QUALIFYING_PRIMARY";
  } else if (
    primaryBefore &&
    primaryRe.type === AGENT_CANDIDATE_TYPE.RELEVANT_ORGANIZATIONAL_CONTACT
  ) {
    stagedDisposition = "HOLD";
    stagedReason = "HOLD_RELEVANT_ORG_NO_COMMERCIAL_PATHWAY";
  }

  // If previously staged but now fail quality — override
  const wasStaged = stagedIds.has(a.opportunityId);

  const row = {
    opportunityId: a.opportunityId,
    hotelId: a.hotelId,
    hotelName: a.hotelName,
    country: a.country,
    organizationName: a.organizationName,
    title: a.title,
    demandQualified: a.demandValidated === true,
    agentExecution: {
      providerCalled: prior.providerCalled === true,
      runId: prior.runId || led.runId || null,
      costUsd: Number(prior.agentCost || led.costUsd || 0),
      status: led.status || (prior.providerCalled ? "completed" : "not_called"),
      rejectedCount: prior.agentRejectedCount ?? led.rejectedCount ?? 0,
    },
    primaryBefore: primaryBefore
      ? {
          name: primaryBefore.name,
          title: primaryBefore.title,
          employer: primaryBefore.employer,
          classification: classBefore,
        }
      : null,
    primaryAfter: primaryAfter
      ? {
          name: primaryAfter.name,
          title: primaryAfter.title,
          employer: primaryAfter.employer || primaryAfter.organization,
          classification: primaryAfter.buyerClassification,
          reason: primaryRe.reason,
        }
      : null,
    primaryReview: {
      beforeClass: classBefore,
      afterClass: classAfter,
      afterType: primaryRe.type,
      reason: primaryRe.reason,
      whoEligible: primaryRe.whoEligible === true,
      junkName: primaryBefore
        ? !isPlausibleBuyerPersonName(primaryBefore.name)
        : false,
      employerOk: primaryBefore
        ? employerAffiliationPlausible(
            {
              currentOrganization: primaryBefore.employer,
              title: primaryBefore.title,
            },
            a.organizationName,
          )
        : null,
      heldForVerification: Boolean(holdReason),
      holdReason,
    },
    secondaryBefore: secondaryBefore
      ? { name: secondaryBefore.name, title: secondaryBefore.title }
      : null,
    secondaryAfter: secondaryAfter
      ? { name: secondaryAfter.name, title: secondaryAfter.title }
      : null,
    whoComplete: whoCompleteCorrected,
    whoCompleteLegacyGate: gates.whoComplete === true,
    whoFinalizationEligible: whoFinalizationCorrected,
    whoFinalizationLegacy: coverage.finalizeEligible === true,
    how: {
      email: verification.email?.value || prior.publicHow?.email || null,
      emailKind: verification.email?.kind || null,
      emailOwnership: prior.publicHow?.emailOwnership || null,
      linkedinPerson: prior.publicHow?.linkedinPerson || null,
      howResearchStatus: gates.howResearchStatus,
    },
    salesReady: gates.salesReady === true,
    stagedBefore: wasStaged,
    stagedDisposition,
    stagedReason,
    commercialEvidenceNotes: primaryAfter
      ? primaryRe.reason
      : holdReason || primaryRe.reason || null,
  };

  rows.push(row);
  writeJson(`staged-revalidation/${a.opportunityId}.json`, {
    ...row,
    pointerFlip: false,
    currentModified: false,
  });
}

const agentRows = rows.filter((r) => r.agentExecution.providerCalled);
const beforePrimary = agentRows.filter((r) => r.primaryBefore?.name).length;
const afterPrimary = agentRows.filter((r) => r.primaryAfter?.name).length;
const beforeDirect = agentRows.filter((r) =>
  /DIRECT_BUYER/i.test(String(r.primaryBefore?.classification || "")),
).length;
const afterDirect = agentRows.filter((r) =>
  r.primaryAfter?.classification === "DIRECT_BUYER",
).length;
const afterViable = agentRows.filter(
  (r) => r.primaryAfter?.classification === "VERIFIED_VIABLE_ALTERNATIVE",
).length;
const afterSecondary = rows.filter((r) => r.secondaryAfter?.name).length;
const junkRejected = rows.filter((r) => r.primaryReview.junkName).length;
const holdCount = rows.filter((r) => r.stagedDisposition === "HOLD").length;
const passCount = rows.filter((r) => r.stagedDisposition === "PASS").length;
const rejectCount = rows.filter((r) => r.stagedDisposition === "REJECT").length;

const summary = {
  contract: "gdi-exa-agent-quality-recovery-v1",
  at: new Date().toISOString(),
  tip: "0956eeab8aa6d0ebab790b3535afca42e269d889",
  paidCalls: 0,
  priorWaveMetrics: metricsPrior,
  screened: allow.screenedCount,
  paidEligible: allow.allowlistCount,
  agentResearched: agentRows.length,
  before: {
    primaryAmongAgent: beforePrimary,
    primaryRate: Number((beforePrimary / Math.max(1, agentRows.length)).toFixed(4)),
    directBuyer: beforeDirect,
    directRate: Number((beforeDirect / Math.max(1, agentRows.length)).toFixed(4)),
    stagedPackages: stagedIds.size,
  },
  after: {
    primaryAmongAgent: afterPrimary,
    primaryRate: Number((afterPrimary / Math.max(1, agentRows.length)).toFixed(4)),
    directBuyer: afterDirect,
    directRate: Number((afterDirect / Math.max(1, agentRows.length)).toFixed(4)),
    viableAlternative: afterViable,
    distinctSecondary: afterSecondary,
    whoComplete: rows.filter((r) => r.whoComplete).length,
    whoFinalizationEligible: rows.filter((r) => r.whoFinalizationEligible)
      .length,
    salesReady: rows.filter((r) => r.salesReady).length,
    junkNamesRejected: junkRejected,
    stagedPass: passCount,
    stagedHold: holdCount,
    stagedReject: rejectCount,
  },
  target1_70pct_after: afterPrimary / Math.max(1, agentRows.length) >= 0.7,
  target2_20pct_direct_after: afterDirect / Math.max(1, agentRows.length) >= 0.2,
  recommendation: "C_HOLD_BROADER_SCALING_UNTIL_GATES_PROVE_OUT",
};

writeJson("RECONCILED_SCORECARD.json", { summary, rows });
writeJson("SUMMARY.json", summary);

console.log(JSON.stringify(summary, null, 2));
