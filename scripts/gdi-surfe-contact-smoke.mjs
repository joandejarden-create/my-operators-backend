#!/usr/bin/env node
/**
 * GDI Surfe contact smoke / Phase 2 eval — evaluation-only reachability test.
 *
 * Dealality establishes WHO the person is.
 * Surfe may only help with HOW to reach them.
 *
 * NO production writes · NO share updates · NO priority/grade mutation ·
 * NO global paid-enrichment enablement · NO outreach · NO canonical merge.
 *
 * Usage:
 *   npm run gdi:surfe-contact-smoke
 *   npm run gdi:surfe-contact-smoke -- --cohort=v2
 *   node scripts/gdi-surfe-contact-smoke.mjs --cohort=v2 --dry-run
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SURFE_CLIENT_VERSION,
  SURFE_CREDIT_COSTS,
  describeSurfeKeyPresence,
  getSurfeCredits,
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  estimateWorstCaseCredits,
} from "../lib/surfe/client.js";
import { gateProviderCandidates } from "../lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js";
import {
  IDENTITY_DECISION,
  EMAIL_OUTCOME,
  PHONE_OUTCOME,
  MERGE_SIMULATION,
  acceptSurfeIdentity,
  classifySurfeEmailOutcome,
  classifySurfePhoneOutcome,
  isMeaningfulSurfeImprovement,
  simulateProductionMergeDecision,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import { PHONE_TYPE } from "../lib/group-demand-intelligence/claim-types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const out = { cohort: "v1", dryRun: false };
  for (const a of argv) {
    if (a.startsWith("--cohort=")) out.cohort = a.slice("--cohort=".length).trim().toLowerCase();
    if (a === "--dry-run" || a === "--eval-only") out.dryRun = true;
  }
  return out;
}

const args = parseArgs(process.argv.slice(2));
const isV2 = args.cohort === "v2" || args.cohort === "phase2";

const COHORT_PATH = path.join(
  root,
  isV2
    ? "data/group-demand-intelligence/evals/bethesda-surfe-contact-cohort-v2.json"
    : "data/group-demand-intelligence/evals/bethesda-surfe-contact-cohort-v1.json"
);
const OUT_JSON = path.join(
  root,
  isV2
    ? "data/group-demand-intelligence/evals/bethesda-surfe-contact-eval-v2.json"
    : "data/group-demand-intelligence/evals/bethesda-surfe-contact-smoke.json"
);
const OUT_JOBS = path.join(
  root,
  isV2
    ? "data/group-demand-intelligence/evals/bethesda-surfe-contact-eval-v2.jobs.json"
    : "data/group-demand-intelligence/evals/bethesda-surfe-contact-smoke.jobs.json"
);
const OUT_MD = path.join(
  root,
  isV2
    ? "reports/group-demand-intelligence/bethesda-surfe-contact-eval-v2.md"
    : "reports/group-demand-intelligence/bethesda-surfe-contact-smoke.md"
);

const report = {
  version: isV2 ? "gdi_surfe_contact_eval_v2" : "gdi_surfe_contact_smoke_v1",
  phase: isV2 ? 2 : 1,
  evaluation_only: true,
  dry_run: args.dryRun,
  production_writes: "PROHIBITED",
  customer_publication: "BLOCKED",
  outreach: "PROHIBITED",
  paid_enrichment_global: "NOT_ENABLED",
  contact_intelligence_paid_enrichment_enabled: "0",
  identity_principle:
    "Dealality establishes WHO the person is. Surfe may only enrich HOW to reach them. Surfe cannot create or replace canonical person identity.",
  started_at: new Date().toISOString(),
  completed_at: null,
  adapter: { module: "lib/surfe/client.js", version: SURFE_CLIENT_VERSION },
  docs: SURFE_CREDIT_COSTS.docs,
  auth: { key: describeSurfeKeyPresence() },
  cohort_path: COHORT_PATH,
  caps: null,
  balances: { before: null, after_email: null, after_mobile: null },
  billing_preflight: null,
  freeze: null,
  email_job: null,
  mobile_job: null,
  results: [],
  summary: null,
  precision: null,
  stratification: null,
  production_gate_simulation: null,
  eligibility_recommendation: null,
  recommendation: null,
  future_escalation: null,
  economics: null,
  blocked: [],
  errors: [],
};

const jobs = {
  version: isV2 ? "gdi_surfe_contact_eval_v2_jobs" : "gdi_surfe_contact_smoke_jobs_v1",
  jobs: [],
};

function persist() {
  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.mkdirSync(path.dirname(OUT_MD), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(OUT_JOBS, JSON.stringify(jobs, null, 2));
}

function buildInput(subject) {
  return {
    firstName: subject.first_name,
    lastName: subject.last_name,
    companyName: subject.enrichmentOrganization,
    companyDomain: subject.enrichmentDomain,
    externalID: subject.id,
  };
}

function gateSubject(subject) {
  return gateProviderCandidates(
    [
      {
        person: {
          display_name: subject.full_name,
          full_name: subject.full_name,
          identity_supported: true,
          why_relevant: subject.title,
          publication_label: "EVIDENCED",
        },
        organization: {
          name: subject.enrichmentOrganization,
          relationship_supported: true,
        },
        identifiers: {
          domain: {
            value: subject.enrichmentDomain,
            status: "CONFIRMED_FIRST_PARTY",
            independently_supported: true,
          },
          first_name: subject.first_name,
          last_name: subject.last_name,
        },
        subject_id: subject.id,
      },
    ],
    { provider: "surfe" }
  );
}

function returnedName(person) {
  if (!person) return null;
  return (
    person.full_name ||
    [person.first_name, person.last_name].filter(Boolean).join(" ") ||
    null
  );
}

function evaluatePersonIdentity(subject, person) {
  if (!person) {
    return {
      decision: IDENTITY_DECISION.NOT_FOUND,
      positiveSignals: [],
      contradictions: [],
      reasons: ["no_person_payload"],
      wouldAcceptForMerge: false,
      providerRawFalseMatch: false,
    };
  }

  const email = person.emails?.[0]?.email || null;
  const identity = acceptSurfeIdentity({
    expectedFullName: subject.full_name,
    expectedOrganization: subject.enrichmentOrganization,
    expectedDomain: subject.enrichmentDomain,
    expectedTitle: subject.title,
    returnedFullName: returnedName(person),
    returnedOrganization: person.company_name || null,
    returnedDomain: person.company_domain || null,
    returnedTitle: person.job_title || null,
    returnedEmail: email,
    returnedLinkedInUrl: person.linkedin_url || null,
  });

  // Provider raw false match: surname contradiction or explicit Kessler-style local/LI
  const providerRawFalseMatch =
    identity.decision === IDENTITY_DECISION.REJECTED &&
    identity.contradictions.some(
      (c) =>
        /surname|local_part_contradicts|linkedin_slug_contradicts|org_domain_mismatch/i.test(
          c
        )
    );

  return { ...identity, providerRawFalseMatch };
}

function providerEmailValidationLabel(emailRec) {
  if (!emailRec) return null;
  const v = String(emailRec.validation_status || "").toUpperCase();
  if (v === "VALID") return "SURFE_PROVIDER_VALID";
  if (v) return `SURFE_PROVIDER_${v}`;
  return "SURFE_PROVIDER_UNVERIFIED";
}

function finalizeRow(subject, person, mobilePerson) {
  const identity = evaluatePersonIdentity(subject, person);
  const emailRec = person?.emails?.[0] || null;
  const emailOutcome = classifySurfeEmailOutcome({
    identityDecision: identity.decision,
    surfeEmail: emailRec?.email || null,
    baselineEmail: subject.baselineEmail,
    baselineEmailType: subject.baselineEmailType,
    baselineEmailVerification: subject.baselineEmailVerification,
  });

  let phoneOutcome = PHONE_OUTCOME.NOT_FOUND;
  let phone = null;
  let phoneNote = null;
  let phoneConfidence = null;

  // Never salvage phone from rejected identity
  if (
    identity.decision === IDENTITY_DECISION.REJECTED ||
    identity.decision === IDENTITY_DECISION.AMBIGUOUS ||
    identity.decision === IDENTITY_DECISION.NOT_FOUND
  ) {
    phoneOutcome =
      identity.decision === IDENTITY_DECISION.NOT_FOUND
        ? PHONE_OUTCOME.NOT_FOUND
        : PHONE_OUTCOME.IDENTITY_REJECTED;
  } else if (mobilePerson) {
    const idMobile = evaluatePersonIdentity(subject, mobilePerson);
    if (
      idMobile.decision === IDENTITY_DECISION.REJECTED ||
      idMobile.decision === IDENTITY_DECISION.AMBIGUOUS
    ) {
      phoneOutcome = PHONE_OUTCOME.IDENTITY_REJECTED;
      phoneNote = "mobile_blocked_by_identity_gate";
    } else {
      const m = mobilePerson.mobile_phones?.[0];
      phone = m?.number || null;
      phoneConfidence = m?.confidence_score ?? null;
      phoneOutcome = classifySurfePhoneOutcome({
        identityDecision: identity.decision,
        surfePhone: phone,
        baselinePhone: subject.baselinePhone,
        baselinePhoneType: subject.baselinePhoneType,
        providerPhoneField: "mobilePhones",
      });
      phoneNote = phone ? "provider_reported_mobile; business_use_unknown" : "not_found";
    }
  }

  const meaningful = isMeaningfulSurfeImprovement({
    emailOutcome,
    phoneOutcome,
  });

  const mergeSim = simulateProductionMergeDecision({
    identityDecision: identity.decision,
    emailOutcome,
    phoneOutcome,
    baselineEmailVerification: subject.baselineEmailVerification,
  });

  // Official source precedence check
  const officialPrecedence = {
    official_email_stronger:
      subject.baselineEmailVerification === "OFFICIAL_SOURCE_VERIFIED" &&
      Boolean(subject.baselineEmail),
    surfe_would_overwrite_official:
      subject.baselineEmailVerification === "OFFICIAL_SOURCE_VERIFIED" &&
      emailOutcome === EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL &&
      subject.baselineEmailType === "DIRECT_WORK",
    surfe_may_fill_missing:
      !subject.baselineEmail && emailOutcome === EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL,
    surfe_may_upgrade_role_to_direct:
      subject.baselineEmailType === "ROLE_BASED" &&
      emailOutcome === EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL,
    surfe_corroborates_only:
      emailOutcome === EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL,
  };

  return {
    id: subject.id,
    canonicalPersonId: subject.canonicalPersonId || subject.id,
    name: subject.full_name,
    opportunityId: subject.opportunityId,
    opportunityTitle: subject.opportunityTitle,
    priority: subject.priority,
    opportunityQualification: subject.opportunityQualification,
    segment: subject.segment || null,
    enrichmentOrganization: subject.enrichmentOrganization,
    enrichmentDomain: subject.enrichmentDomain,
    controlCase: Boolean(subject.controlCase),
    knownFalsePositiveRisk: subject.knownFalsePositiveRisk || null,
    baseline: {
      grade: subject.baselineContactGrade,
      confidence: subject.baselineContactConfidence,
      email: subject.baselineEmail,
      emailType: subject.baselineEmailType,
      emailVerification: subject.baselineEmailVerification,
      phone: subject.baselinePhone,
      phoneType: subject.baselinePhoneType,
      source: subject.baselineSource,
      sources: subject.baselineSources || [subject.baselineSource].filter(Boolean),
      title: subject.title,
      relationshipToEvent: subject.relationshipToEvent,
      targetRoleMatch: subject.targetRoleMatch,
      lastVerified: subject.lastVerified || null,
    },
    identity: {
      decision: identity.decision,
      positiveSignals: identity.positiveSignals,
      contradictions: identity.contradictions,
      reasons: identity.reasons,
      wouldAcceptForMerge: identity.wouldAcceptForMerge,
      providerRawFalseMatch: Boolean(identity.providerRawFalseMatch),
      checks: identity.checks || null,
      returned: person
        ? {
            name: returnedName(person),
            company: person.company_name || null,
            domain: person.company_domain || null,
            title: person.job_title || null,
            linkedin: person.linkedin_url || null,
            status: person.status || null,
          }
        : null,
    },
    surfe: {
      email: emailRec?.email || null,
      emailOutcome,
      providerValidationLabel: providerEmailValidationLabel(emailRec),
      providerValidation: emailRec?.validation_status || null,
      providerEmailType: emailRec?.type || null,
      phone,
      phoneOutcome,
      phoneType:
        phoneOutcome === PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL
          ? PHONE_TYPE.MOBILE_PUBLIC_PROFESSIONAL
          : phone
            ? PHONE_TYPE.UNKNOWN
            : null,
      phoneNote,
      phoneConfidence,
      researchedAt: new Date().toISOString(),
    },
    meaningfulImprovement: meaningful,
    productionMergeSimulation: mergeSim,
    officialPrecedence,
    credits: {
      emailAttempt: subject.requestEmail ? 1 : 0,
      mobileAttempt: subject.requestMobile ? 1 : 0,
    },
  };
}

function pct(n, d) {
  if (!d) return 0;
  return Math.round((1000 * n) / d) / 10;
}

function buildSummary(results, credits) {
  const accepted = results.filter((r) =>
    [IDENTITY_DECISION.ACCEPTED, IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE].includes(
      r.identity.decision
    )
  );
  const acceptedStrict = results.filter(
    (r) => r.identity.decision === IDENTITY_DECISION.ACCEPTED
  );
  // Precision: among identities accepted for merge consideration, how many were not false?
  // For this eval: ACCEPTED rows that are not providerRawFalseMatch and not known wrong.
  // We treat ACCEPTED as "accepted by evaluation logic". Limited evidence is held, not accepted.
  const acceptedForPrecision = acceptedStrict;
  const falseAccepted = acceptedForPrecision.filter((r) => r.identity.providerRawFalseMatch);
  const acceptedUseful = acceptedForPrecision.filter((r) => !r.identity.providerRawFalseMatch);

  const rawFalse = results.filter((r) => r.identity.providerRawFalseMatch);
  const guardrailCaught = rawFalse.filter(
    (r) => r.identity.decision === IDENTITY_DECISION.REJECTED
  );

  return {
    peopleTested: results.length,
    identity: {
      accepted: results.filter((r) => r.identity.decision === IDENTITY_DECISION.ACCEPTED)
        .length,
      acceptedLimited: results.filter(
        (r) => r.identity.decision === IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE
      ).length,
      corroborationOnly: results.filter(
        (r) => r.identity.decision === IDENTITY_DECISION.CORROBORATION_ONLY
      ).length,
      ambiguous: results.filter((r) => r.identity.decision === IDENTITY_DECISION.AMBIGUOUS)
        .length,
      rejected: results.filter((r) => r.identity.decision === IDENTITY_DECISION.REJECTED)
        .length,
      notFound: results.filter((r) => r.identity.decision === IDENTITY_DECISION.NOT_FOUND)
        .length,
    },
    email: {
      newDirect: results.filter(
        (r) => r.surfe.emailOutcome === EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL
      ).length,
      newRoleBased: results.filter(
        (r) => r.surfe.emailOutcome === EMAIL_OUTCOME.NEW_ROLE_BASED_EMAIL
      ).length,
      corroboratesOfficial: results.filter(
        (r) => r.surfe.emailOutcome === EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL
      ).length,
      corroboratesInferred: results.filter(
        (r) => r.surfe.emailOutcome === EMAIL_OUTCOME.CORROBORATES_INFERRED_EMAIL
      ).length,
      conflicts: results.filter(
        (r) => r.surfe.emailOutcome === EMAIL_OUTCOME.CONFLICTS_WITH_OFFICIAL
      ).length,
      notFound: results.filter((r) => r.surfe.emailOutcome === EMAIL_OUTCOME.NOT_FOUND)
        .length,
      identityRejected: results.filter(
        (r) => r.surfe.emailOutcome === EMAIL_OUTCOME.IDENTITY_REJECTED
      ).length,
    },
    phone: {
      newMobile: results.filter(
        (r) => r.surfe.phoneOutcome === PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL
      ).length,
      newOffice: results.filter(
        (r) => r.surfe.phoneOutcome === PHONE_OUTCOME.NEW_OFFICE_PHONE
      ).length,
      newDirect: results.filter(
        (r) => r.surfe.phoneOutcome === PHONE_OUTCOME.NEW_DIRECT_PHONE
      ).length,
      sameMain: results.filter((r) => r.surfe.phoneOutcome === PHONE_OUTCOME.SAME_MAIN_LINE)
        .length,
      corroboration: results.filter(
        (r) => r.surfe.phoneOutcome === PHONE_OUTCOME.CORROBORATION_ONLY
      ).length,
      notFound: results.filter((r) => r.surfe.phoneOutcome === PHONE_OUTCOME.NOT_FOUND)
        .length,
    },
    meaningfulImproved: results.filter((r) => r.meaningfulImprovement).length,
    meaningfulRatePct: pct(
      results.filter((r) => r.meaningfulImprovement).length,
      results.length
    ),
    directEmailImprovementRatePct: pct(
      results.filter((r) => r.surfe.emailOutcome === EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL)
        .length,
      results.length
    ),
    phoneImprovementRatePct: pct(
      results.filter((r) =>
        [
          PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL,
          PHONE_OUTCOME.NEW_OFFICE_PHONE,
          PHONE_OUTCOME.NEW_DIRECT_PHONE,
        ].includes(r.surfe.phoneOutcome)
      ).length,
      results.length
    ),
    corroborationOnlyRatePct: pct(
      results.filter(
        (r) =>
          r.surfe.emailOutcome === EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL &&
          !r.meaningfulImprovement
      ).length,
      results.length
    ),
    notFoundRatePct: pct(
      results.filter((r) => r.identity.decision === IDENTITY_DECISION.NOT_FOUND).length,
      results.length
    ),
    ambiguousRatePct: pct(
      results.filter((r) => r.identity.decision === IDENTITY_DECISION.AMBIGUOUS).length,
      results.length
    ),
    rejectedFalseMatchRatePct: pct(rawFalse.length, results.length),
    precision: {
      acceptedResultPrecisionPct:
        acceptedForPrecision.length === 0
          ? null
          : pct(acceptedUseful.length, acceptedForPrecision.length),
      acceptedCount: acceptedForPrecision.length,
      falseAcceptedCount: falseAccepted.length,
      providerRawFalseMatchCount: rawFalse.length,
      providerRawFalseMatchRatePct: pct(rawFalse.length, results.length),
      guardrailRejectionSuccessCount: guardrailCaught.length,
      guardrailRejectionSuccessRatePct:
        rawFalse.length === 0 ? 100 : pct(guardrailCaught.length, rawFalse.length),
    },
    credits: {
      emailConsumed: credits.emailConsumed,
      mobileConsumed: credits.mobileConsumed,
      total:
        (Number(credits.emailConsumed) || 0) + (Number(credits.mobileConsumed) || 0),
      perMeaningful: null,
      perAcceptedNewEmail: null,
      perUsefulPhone: null,
    },
  };
}

function finalizeCreditEconomics(summary) {
  const improved = summary.meaningfulImproved || 0;
  const newEmail = summary.email.newDirect || 0;
  const usefulPhone =
    (summary.phone.newMobile || 0) +
    (summary.phone.newOffice || 0) +
    (summary.phone.newDirect || 0);
  const total = summary.credits.total || 0;
  summary.credits.perMeaningful =
    improved > 0 ? Math.round((total / improved) * 10) / 10 : null;
  summary.credits.perAcceptedNewEmail =
    newEmail > 0 ? Math.round((total / newEmail) * 10) / 10 : null;
  summary.credits.perUsefulPhone =
    usefulPhone > 0 ? Math.round((total / usefulPhone) * 10) / 10 : null;
}

function stratify(results) {
  const buckets = {};
  function add(key, row) {
    if (!buckets[key]) {
      buckets[key] = { n: 0, meaningful: 0, accepted: 0, rejected: 0 };
    }
    buckets[key].n += 1;
    if (row.meaningfulImprovement) buckets[key].meaningful += 1;
    if (row.identity.decision === IDENTITY_DECISION.ACCEPTED) buckets[key].accepted += 1;
    if (row.identity.decision === IDENTITY_DECISION.REJECTED) buckets[key].rejected += 1;
  }
  for (const r of results) {
    add(`grade_${r.baseline.grade}`, r);
    if (r.segment) add(`segment_${r.segment}`, r);
    if (r.baseline.targetRoleMatch) add(`role_${r.baseline.targetRoleMatch}`, r);
    if (r.baseline.title) add("title_known", r);
    if (r.enrichmentDomain) add("org_domain_known", r);
  }
  return Object.fromEntries(
    Object.entries(buckets).map(([k, v]) => [
      k,
      { ...v, meaningfulRatePct: pct(v.meaningful, v.n) },
    ])
  );
}

function buildEconomics(summary) {
  const tested = summary.peopleTested || 1;
  const totalCredits = summary.credits.total || 0;
  const creditsPerEligible = tested > 0 ? totalCredits / tested : 1.5;
  const eligiblePerHotelMonth = 6;
  return {
    assumptions:
      `n=${tested}; assumes ~${eligiblePerHotelMonth} Surfe-eligible named weak-reach contacts per hotel per month after production gate; credits/person from this run ≈ ${creditsPerEligible.toFixed(2)}; NOT a production forecast.`,
    narrative: `This run consumed ${totalCredits} credits across ${tested} people (${summary.meaningfulImproved} meaningful). Credits per meaningful ≈ ${summary.credits.perMeaningful ?? "n/a"}.`,
    projections: [
      {
        scale: "1 hotel / month",
        assumption: `${eligiblePerHotelMonth} eligible × ${creditsPerEligible.toFixed(2)} credits`,
        credits: Math.round(eligiblePerHotelMonth * creditsPerEligible),
      },
      {
        scale: "10 hotels",
        assumption: "linear × 10 (no volume discount assumed)",
        credits: Math.round(10 * eligiblePerHotelMonth * creditsPerEligible),
      },
      {
        scale: "100 hotels",
        assumption: "linear × 100; Grade≤C + missing reachability gate lowers actual volume",
        credits: Math.round(100 * eligiblePerHotelMonth * creditsPerEligible),
      },
    ],
  };
}

function buildRecommendation(summary) {
  const precision = summary.precision.acceptedResultPrecisionPct;
  const falseAccepted = summary.precision.falseAcceptedCount || 0;
  const yieldPct = summary.meaningfulRatePct || 0;
  const tested = summary.peopleTested || 0;

  let verdict = "KEEP EVALUATING";
  let rationale = "";
  let successAnswer = "";

  if (falseAccepted > 0 || (precision !== null && precision < 98)) {
    verdict = "DO NOT USE FOR GDI";
    successAnswer =
      "No — accepted-result precision failed. Surfe cannot be trusted as a reachability layer until identity gates never accept a wrong person.";
    rationale = `False accepted identities=${falseAccepted}; accepted-result precision=${precision}%. Production enablement blocked.`;
  } else if (
    precision !== null &&
    precision >= 98 &&
    falseAccepted === 0 &&
    yieldPct >= 40 &&
    tested >= 10 &&
    summary.meaningfulImproved >= 4
  ) {
    verdict = "ADD AS GATED PAID ESCALATION";
    successAnswer =
      "Yes, with gates — Surfe can safely enrich reachability for already-established identities when multi-signal acceptance holds and official sources remain authoritative.";
    rationale =
      "Accepted-result precision ≥98%, no bad identity passed the gate, meaningful enrichment ≥40%, deterministic production-gate simulation available. Keep CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0 until CI merge path ships. Surfe must never create/replace person identity.";
  } else if (falseAccepted === 0 && (precision === null || precision >= 98) && yieldPct >= 25) {
    verdict = "KEEP EVALUATING";
    successAnswer =
      "Promising but not yet production — precision held and yield is interesting; expand sample / economics before gated enablement.";
    rationale = `Precision safe (${precision ?? "n/a — no ACCEPTED rows"}). Meaningful improvement ${yieldPct}% (${summary.meaningfulImproved}/${tested}). Guardrail caught ${summary.precision.guardrailRejectionSuccessCount} raw false matches. Sample/economics still need more runs. Paid enrichment stays OFF.`;
  } else if (yieldPct < 25 && falseAccepted === 0) {
    verdict = "KEEP EVALUATING";
    successAnswer =
      "Safety OK but yield weak — Surfe may not justify credits for this GDI contact mix yet.";
    rationale = `Meaningful improvement only ${yieldPct}%. Precision held. Consider tighter eligibility (Grade B/C role-inbox only) before another paid wave.`;
  } else {
    verdict = "DO NOT USE FOR GDI";
    successAnswer = "Not for GDI on this evidence.";
    rationale = "Yield and/or precision do not support Surfe for GDI contact enrichment.";
  }

  return { verdict, rationale, successAnswer };
}

function writeMdV2() {
  const s = report.summary || {};
  const p = report.precision || s.precision || {};
  const lines = [];
  lines.push("# Bethesda GDI — Surfe Contact Enrichment Evaluation Phase 2");
  lines.push("");
  lines.push(`**Date:** ${report.started_at?.slice(0, 10) || "—"}`);
  lines.push(
    "**Mode:** Evaluation-only · **NO production writes** · **NO share updates** · **Paid enrichment OFF**"
  );
  lines.push(`**Cohort:** \`${path.basename(COHORT_PATH)}\``);
  lines.push(`**Adapter:** \`${report.adapter.module}\` (${report.adapter.version})`);
  lines.push("");
  lines.push("## Executive Verdict");
  lines.push("");
  lines.push(`# ${report.recommendation?.verdict || "KEEP EVALUATING"}`);
  lines.push("");
  lines.push(report.recommendation?.rationale || "");
  lines.push("");
  lines.push("### Success question");
  lines.push("");
  lines.push(
    '> "Can Dealality safely use Surfe as a reachability enrichment layer for an already-established person identity, with near-zero risk of surfacing the wrong person?"'
  );
  lines.push("");
  lines.push(`**Answer this run:** ${report.recommendation?.successAnswer || "—"}`);
  lines.push("");
  lines.push("## Cohort");
  lines.push("");
  lines.push("| Person | Segment | Grade | Opp | Baseline email | Baseline phone | Control |");
  lines.push("|---|---|---|---|---|---|---|");
  for (const r of report.results) {
    lines.push(
      `| ${r.name} | ${r.segment || "—"} | ${r.baseline.grade} | ${r.opportunityId} | ${r.baseline.email || "—"} (${r.baseline.emailType || "—"}) | ${r.baseline.phone || "—"} (${r.baseline.phoneType || "—"}) | ${r.controlCase ? "yes" : "no"} |`
    );
  }
  lines.push("");
  lines.push("## Identity Safety");
  lines.push("");
  lines.push(`- ACCEPTED: **${s.identity?.accepted ?? 0}**`);
  lines.push(`- ACCEPTED_WITH_LIMITED_EVIDENCE: **${s.identity?.acceptedLimited ?? 0}**`);
  lines.push(`- AMBIGUOUS: **${s.identity?.ambiguous ?? 0}**`);
  lines.push(`- REJECTED: **${s.identity?.rejected ?? 0}**`);
  lines.push(`- NOT_FOUND: **${s.identity?.notFound ?? 0}**`);
  lines.push(
    `- Provider raw false matches: **${p.providerRawFalseMatchCount ?? 0}**`
  );
  lines.push(
    `- Guardrail rejection success: **${p.guardrailRejectionSuccessCount ?? 0}** / ${p.providerRawFalseMatchCount ?? 0}`
  );
  lines.push("");
  for (const r of report.results) {
    lines.push(
      `- **${r.name}**: ${r.identity.decision}` +
        (r.identity.contradictions?.length
          ? ` — ${r.identity.contradictions.join("; ")}`
          : r.identity.positiveSignals?.length
            ? ` — signals: ${r.identity.positiveSignals.join(", ")}`
            : "")
    );
  }
  lines.push("");
  lines.push("## Email Results");
  lines.push("");
  lines.push(`- NEW_DIRECT_WORK_EMAIL: ${s.email?.newDirect ?? 0}`);
  lines.push(`- NEW_ROLE_BASED_EMAIL: ${s.email?.newRoleBased ?? 0}`);
  lines.push(`- CORROBORATES_OFFICIAL_EMAIL: ${s.email?.corroboratesOfficial ?? 0}`);
  lines.push(`- CONFLICTS_WITH_OFFICIAL: ${s.email?.conflicts ?? 0}`);
  lines.push(`- NOT_FOUND / IDENTITY_REJECTED: ${(s.email?.notFound ?? 0) + (s.email?.identityRejected ?? 0)}`);
  lines.push("");
  lines.push("| Person | Baseline | Surfe email | Outcome | Provider label |");
  lines.push("|---|---|---|---|---|");
  for (const r of report.results) {
    lines.push(
      `| ${r.name} | ${r.baseline.email || "—"} | ${r.surfe.email || "—"} | ${r.surfe.emailOutcome} | ${r.surfe.providerValidationLabel || "—"} |`
    );
  }
  lines.push("");
  lines.push("## Phone Results");
  lines.push("");
  lines.push(`- NEW_MOBILE_PROFESSIONAL: ${s.phone?.newMobile ?? 0}`);
  lines.push(`- NEW_OFFICE / DIRECT: ${(s.phone?.newOffice ?? 0) + (s.phone?.newDirect ?? 0)}`);
  lines.push(`- SAME_MAIN_LINE: ${s.phone?.sameMain ?? 0}`);
  lines.push(`- CORROBORATION_ONLY: ${s.phone?.corroboration ?? 0}`);
  lines.push(`- NOT_FOUND: ${s.phone?.notFound ?? 0}`);
  lines.push("");
  lines.push("| Person | Baseline | Surfe phone | Outcome |");
  lines.push("|---|---|---|---|");
  for (const r of report.results) {
    lines.push(
      `| ${r.name} | ${r.baseline.phone || "—"} | ${r.surfe.phone || "—"} | ${r.surfe.phoneOutcome} |`
    );
  }
  lines.push("");
  lines.push("## Precision");
  lines.push("");
  lines.push(
    `- **Accepted-result precision:** ${p.acceptedResultPrecisionPct ?? "n/a"}% (${p.acceptedCount ?? 0} accepted; ${p.falseAcceptedCount ?? 0} false)`
  );
  lines.push(
    `- **Provider raw false-match rate:** ${p.providerRawFalseMatchRatePct ?? 0}%`
  );
  lines.push(
    `- **Guardrail rejection success:** ${p.guardrailRejectionSuccessRatePct ?? 0}%`
  );
  lines.push("");
  lines.push("## Yield");
  lines.push("");
  lines.push(`- Meaningful improvement rate: **${s.meaningfulRatePct ?? 0}%** (${s.meaningfulImproved ?? 0}/${s.peopleTested ?? 0})`);
  lines.push(`- Direct email improvement rate: ${s.directEmailImprovementRatePct ?? 0}%`);
  lines.push(`- Phone improvement rate: ${s.phoneImprovementRatePct ?? 0}%`);
  lines.push(`- Corroboration-only rate: ${s.corroborationOnlyRatePct ?? 0}%`);
  lines.push(`- Not-found rate: ${s.notFoundRatePct ?? 0}%`);
  lines.push(`- Ambiguous rate: ${s.ambiguousRatePct ?? 0}%`);
  lines.push(`- Rejected false-match rate: ${s.rejectedFalseMatchRatePct ?? 0}%`);
  lines.push("");
  lines.push("## Economics");
  lines.push("");
  const b = report.balances;
  lines.push("| Bucket | Starting | After email | Ending |");
  lines.push("|---|---:|---:|---:|");
  lines.push(
    `| Email | ${b.before?.totalEmail ?? "—"} | ${b.after_email?.totalEmail ?? "—"} | ${b.after_mobile?.totalEmail ?? "—"} |`
  );
  lines.push(
    `| Mobile | ${b.before?.totalMobile ?? "—"} | ${b.after_email?.totalMobile ?? "—"} | ${b.after_mobile?.totalMobile ?? "—"} |`
  );
  lines.push("");
  lines.push(`- Credits used (email): **${s.credits?.emailConsumed ?? "—"}**`);
  lines.push(`- Credits used (mobile): **${s.credits?.mobileConsumed ?? "—"}**`);
  lines.push(`- Credits used (total): **${s.credits?.total ?? "—"}**`);
  lines.push(`- Credits / meaningful improvement: **${s.credits?.perMeaningful ?? "n/a"}**`);
  lines.push(`- Credits / accepted new email: **${s.credits?.perAcceptedNewEmail ?? "n/a"}**`);
  lines.push(`- Credits / useful phone: **${s.credits?.perUsefulPhone ?? "n/a"}**`);
  if (report.balances.accounting_note) {
    lines.push(`- Accounting note: ${report.balances.accounting_note}`);
  }
  lines.push("");
  if (report.economics) {
    lines.push(report.economics.narrative);
    lines.push("");
    lines.push("| Scale | Assumption | Projected credits / month |");
    lines.push("|---|---|---:|");
    for (const row of report.economics.projections || []) {
      lines.push(`| ${row.scale} | ${row.assumption} | ${row.credits} |`);
    }
    lines.push("");
    lines.push(`*${report.economics.assumptions}*`);
  }
  lines.push("");
  lines.push("## Stratification");
  lines.push("");
  lines.push("| Bucket | n | Meaningful | Accepted | Rejected | Meaningful % |");
  lines.push("|---|---:|---:|---:|---:|---:|");
  for (const [k, v] of Object.entries(report.stratification || {})) {
    lines.push(
      `| ${k} | ${v.n} | ${v.meaningful} | ${v.accepted} | ${v.rejected} | ${v.meaningfulRatePct}% |`
    );
  }
  lines.push("");
  lines.push("## Eligibility Recommendation");
  lines.push("");
  lines.push("```");
  lines.push(report.eligibility_recommendation || "");
  lines.push("```");
  lines.push("");
  lines.push("## Production Gate Simulation");
  lines.push("");
  const gate = report.production_gate_simulation || {};
  lines.push(`- WOULD_ACCEPT: **${gate.WOULD_ACCEPT ?? 0}**`);
  lines.push(
    `- WOULD_ACCEPT_AS_CORROBORATION_ONLY: **${gate.WOULD_ACCEPT_AS_CORROBORATION_ONLY ?? 0}**`
  );
  lines.push(`- WOULD_HOLD_FOR_REVIEW: **${gate.WOULD_HOLD_FOR_REVIEW ?? 0}**`);
  lines.push(`- WOULD_REJECT: **${gate.WOULD_REJECT ?? 0}**`);
  lines.push("");
  lines.push("| Person | Identity | Email | Phone | Meaningful | Merge sim |");
  lines.push("|---|---|---|---|---|---|");
  for (const r of report.results) {
    lines.push(
      `| ${r.name} | ${r.identity.decision} | ${r.surfe.emailOutcome} | ${r.surfe.phoneOutcome} | ${r.meaningfulImprovement ? "YES" : "no"} | ${r.productionMergeSimulation} |`
    );
  }
  lines.push("");
  lines.push("## Official source precedence");
  lines.push("");
  lines.push(
    "Official verified email/phone remains stronger than Surfe. Surfe may corroborate or fill missing fields; it must not overwrite stronger verified fields. Provider labels are never called OFFICIAL_SOURCE_VERIFIED."
  );
  lines.push("");
  lines.push("## Risks");
  lines.push("");
  lines.push("- Identity collision (same first name, different surname / LinkedIn)");
  lines.push("- Stale employment (board employer ≠ current employer)");
  lines.push("- Wrong LinkedIn mapping");
  lines.push("- Guessed / unverified email");
  lines.push("- Wrong phone type (mobile vs office vs main)");
  lines.push("- Provider conflicts with official source");
  lines.push("");
  lines.push("## Recommendation");
  lines.push("");
  lines.push(report.recommendation?.rationale || "");
  lines.push("");
  lines.push(
    "Keep `CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0`. No production writes from this evaluation."
  );
  lines.push("");
  fs.writeFileSync(OUT_MD, lines.join("\n"));
}

function writeMdV1Compat() {
  // Minimal Phase 1-compatible markdown for --cohort=v1
  writeMdV2();
  const existing = fs.readFileSync(OUT_MD, "utf8");
  fs.writeFileSync(
    OUT_MD,
    existing.replace(
      "Surfe Contact Enrichment Evaluation Phase 2",
      "Surfe Contact Smoke (Evaluation Only)"
    )
  );
}

async function startWithOneRetry(body, kind) {
  let start = await startSurfePeopleEnrichment(body);
  if (!start.ok && !start.insufficient_credits && Number(start.http_status) >= 500) {
    jobs.jobs.push({
      at: new Date().toISOString(),
      kind: `${kind}_retry`,
      reason: "transient_http_5xx",
      http_status: start.http_status,
    });
    await new Promise((r) => setTimeout(r, 1500));
    start = await startSurfePeopleEnrichment(body);
  }
  return start;
}

async function main() {
  if (!report.auth.key.present) {
    report.blocked.push({ operation: "all", reason: "SURFE_API_KEY missing" });
    persist();
    writeMdV2();
    process.exit(1);
  }

  const cohort = JSON.parse(fs.readFileSync(COHORT_PATH, "utf8"));
  const subjects = cohort.subjects || [];
  report.caps = cohort.credit_caps;
  report.freeze = {
    frozen_at: cohort.frozenAt,
    cohort_version: cohort.version,
    subjects: subjects.map((s) => ({
      id: s.id,
      canonicalPersonId: s.canonicalPersonId || s.id,
      full_name: s.full_name,
      organization: s.enrichmentOrganization,
      organization_domain: s.enrichmentDomain,
      title: s.title,
      opportunityId: s.opportunityId,
      relationshipToEvent: s.relationshipToEvent,
      targetRoleMatch: s.targetRoleMatch,
      baselineContactGrade: s.baselineContactGrade,
      baselineContactConfidence: s.baselineContactConfidence,
      baselineEmail: s.baselineEmail,
      baselineEmailType: s.baselineEmailType,
      baselineEmailVerification: s.baselineEmailVerification,
      baselinePhone: s.baselinePhone,
      baselinePhoneType: s.baselinePhoneType,
      baselineSources: s.baselineSources || [s.baselineSource].filter(Boolean),
      lastVerified: s.lastVerified || null,
      requestEmail: s.requestEmail,
      requestMobile: s.requestMobile,
      controlCase: Boolean(s.controlCase),
      whyInCohort: s.whyInCohort,
    })),
  };

  const credits0 = await getSurfeCredits();
  report.balances.before = {
    totalEmail: credits0.payload?.totalEmail ?? null,
    totalMobile: credits0.payload?.totalMobile ?? null,
    totalSearch: credits0.payload?.totalSearch ?? null,
    readable: credits0.ok,
    http_status: credits0.http_status,
    message: credits0.payload?.message ?? null,
  };

  if (!credits0.ok) {
    report.blocked.push({
      operation: "credits",
      reason: "Cannot read Surfe balances",
      http_status: credits0.http_status,
    });
    persist();
    writeMdV2();
    console.log(JSON.stringify({ blocked: report.blocked }, null, 2));
    return;
  }

  const emailPeople = subjects.filter((s) => s.requestEmail);
  const mobilePeople = subjects.filter((s) => s.requestMobile);
  const emailCap = cohort.credit_caps.email_people_max;
  const mobileCap = cohort.credit_caps.mobile_people_max;
  const emailWorst = estimateWorstCaseCredits({ emailPeople: emailPeople.length });
  const mobileWorst = estimateWorstCaseCredits({ mobilePeople: mobilePeople.length });

  report.billing_preflight = {
    email: {
      people: emailPeople.length,
      worst_case: emailWorst.email,
      hard_cap: emailCap,
      balance: report.balances.before.totalEmail,
      proceed:
        emailPeople.length <= emailCap &&
        Number(report.balances.before.totalEmail) >= emailPeople.length,
    },
    mobile: {
      people: mobilePeople.length,
      worst_case: mobileWorst.mobile,
      hard_cap: mobileCap,
      balance: report.balances.before.totalMobile,
      proceed:
        mobilePeople.length <= mobileCap &&
        Number(report.balances.before.totalMobile) >= mobilePeople.length,
    },
    search: { people: 0, note: "No Surfe search — person already known" },
  };

  if (args.dryRun) {
    report.blocked.push({
      operation: "dry_run",
      reason: "Dry-run — no Surfe enrichment calls",
      preflight: report.billing_preflight,
    });
    persist();
    writeMdV2();
    console.log(
      JSON.stringify(
        {
          dry_run: true,
          cohort: cohort.version,
          people: subjects.length,
          preflight: report.billing_preflight,
          out_json: OUT_JSON,
        },
        null,
        2
      )
    );
    return;
  }

  if (!report.billing_preflight.email.proceed) {
    report.blocked.push({
      operation: "email_enrich",
      reason: "Preflight failed",
      preflight: report.billing_preflight.email,
    });
    persist();
    writeMdV2();
    console.log(JSON.stringify({ blocked: report.blocked }, null, 2));
    return;
  }

  const allowed = [];
  for (const s of subjects) {
    const g = gateSubject(s);
    if (!g.allowed.length) {
      report.results.push({
        id: s.id,
        name: s.full_name,
        opportunityId: s.opportunityId,
        baseline: {
          grade: s.baselineContactGrade,
          email: s.baselineEmail,
          phone: s.baselinePhone,
        },
        identity: {
          decision: IDENTITY_DECISION.REJECTED,
          reasons: ["provider_gate_rejected"],
          contradictions: ["GATE_REJECTED"],
          positiveSignals: [],
          providerRawFalseMatch: false,
        },
        surfe: {
          email: null,
          emailOutcome: EMAIL_OUTCOME.IDENTITY_REJECTED,
          phone: null,
          phoneOutcome: PHONE_OUTCOME.IDENTITY_REJECTED,
        },
        meaningfulImprovement: false,
        productionMergeSimulation: MERGE_SIMULATION.WOULD_REJECT,
      });
      continue;
    }
    allowed.push(s);
  }

  const emailBody = {
    people: allowed.filter((s) => s.requestEmail).map(buildInput),
    include: { email: true, mobile: false, linkedInUrl: true, jobHistory: true },
    enrichmentOptions: { acceptedEmailType: "professional" },
  };
  const emailStart = await startWithOneRetry(emailBody, "email");
  const emailId =
    emailStart.payload?.enrichmentID || emailStart.payload?.enrichmentId || null;
  jobs.jobs.push({
    at: new Date().toISOString(),
    kind: "email_only",
    enrichment_id: emailId,
    subject_ids: emailBody.people.map((p) => p.externalID),
    http_status: emailStart.http_status,
  });
  report.email_job = {
    enrichment_id: emailId,
    http_status: emailStart.http_status,
    insufficient_credits: emailStart.insufficient_credits,
    people_requested: emailBody.people.length,
  };
  persist();

  if (!emailStart.ok || !emailId || emailStart.insufficient_credits) {
    report.blocked.push({
      operation: "email_enrich",
      reason: emailStart.insufficient_credits ? "insufficient credits" : "start failed",
      http_status: emailStart.http_status,
    });
    persist();
    writeMdV2();
    console.log(JSON.stringify({ blocked: report.blocked }, null, 2));
    return;
  }

  const emailDone = await pollSurfePeopleEnrichment(emailId, { maxWaitMs: 240000 });
  jobs.jobs.push({
    at: new Date().toISOString(),
    kind: "email_poll",
    enrichment_id: emailId,
    status: emailDone.payload?.status,
    timed_out: Boolean(emailDone.timed_out),
  });

  const byExt = new Map();
  for (const raw of emailDone.payload?.people || []) {
    byExt.set(String(raw.externalID || ""), normalizeSurfePerson(raw));
  }

  const credits1 = await getSurfeCredits();
  report.balances.after_email = {
    totalEmail: credits1.payload?.totalEmail ?? null,
    totalMobile: credits1.payload?.totalMobile ?? null,
    totalSearch: credits1.payload?.totalSearch ?? null,
    readable: credits1.ok,
    http_status: credits1.http_status,
  };

  const mobileByExt = new Map();
  const mobileAllowed = allowed.filter(
    (s) => s.requestMobile && report.billing_preflight.mobile.proceed
  );
  if (mobileAllowed.length) {
    const mobileBody = {
      people: mobileAllowed.map(buildInput),
      include: { email: false, mobile: true, linkedInUrl: false, jobHistory: false },
    };
    const mobileStart = await startWithOneRetry(mobileBody, "mobile");
    const mobileId =
      mobileStart.payload?.enrichmentID || mobileStart.payload?.enrichmentId || null;
    jobs.jobs.push({
      at: new Date().toISOString(),
      kind: "mobile_only",
      enrichment_id: mobileId,
      subject_ids: mobileAllowed.map((s) => s.id),
      http_status: mobileStart.http_status,
    });
    report.mobile_job = {
      enrichment_id: mobileId,
      http_status: mobileStart.http_status,
      insufficient_credits: mobileStart.insufficient_credits,
      people_requested: mobileAllowed.length,
    };
    persist();

    if (mobileStart.ok && mobileId && !mobileStart.insufficient_credits) {
      const mobileDone = await pollSurfePeopleEnrichment(mobileId, {
        maxWaitMs: 240000,
      });
      jobs.jobs.push({
        at: new Date().toISOString(),
        kind: "mobile_poll",
        enrichment_id: mobileId,
        status: mobileDone.payload?.status,
        timed_out: Boolean(mobileDone.timed_out),
      });
      for (const raw of mobileDone.payload?.people || []) {
        mobileByExt.set(String(raw.externalID || ""), normalizeSurfePerson(raw));
      }
    } else {
      report.blocked.push({
        operation: "mobile_enrich",
        reason: mobileStart.insufficient_credits
          ? "insufficient mobile credits"
          : "mobile start failed — email results retained",
        http_status: mobileStart.http_status,
      });
    }
  } else if (mobilePeople.length && !report.billing_preflight.mobile.proceed) {
    report.blocked.push({
      operation: "mobile_enrich",
      reason: "Mobile preflight failed — skipped",
      preflight: report.billing_preflight.mobile,
    });
  }

  const credits2 = await getSurfeCredits();
  report.balances.after_mobile = {
    totalEmail: credits2.payload?.totalEmail ?? null,
    totalMobile: credits2.payload?.totalMobile ?? null,
    totalSearch: credits2.payload?.totalSearch ?? null,
    readable: credits2.ok,
    http_status: credits2.http_status,
  };

  report.results = [];
  for (const s of subjects) {
    if (!allowed.find((a) => a.id === s.id)) continue;
    const person = byExt.get(s.id) || null;
    const mobilePerson = mobileByExt.get(s.id) || null;
    report.results.push(finalizeRow(s, person, mobilePerson));
  }

  const emailBefore = Number(report.balances.before.totalEmail);
  const emailAfter = Number(report.balances.after_email?.totalEmail);
  const mobileMid = Number(
    report.balances.after_email?.totalMobile ?? report.balances.before.totalMobile
  );
  const mobileAfter = Number(report.balances.after_mobile?.totalMobile ?? mobileMid);
  const emailTopUpDetected =
    Number.isFinite(emailBefore) && Number.isFinite(emailAfter) && emailAfter > emailBefore;
  const mobileTopUpDetected =
    Number.isFinite(Number(report.balances.before.totalMobile)) &&
    Number.isFinite(mobileMid) &&
    mobileMid > Number(report.balances.before.totalMobile);

  let emailConsumed = null;
  if (emailTopUpDetected) {
    emailConsumed = report.email_job?.people_requested ?? emailPeople.length;
  } else if (Number.isFinite(emailBefore) && Number.isFinite(emailAfter)) {
    emailConsumed = Math.max(0, emailBefore - emailAfter);
  }

  let mobileConsumed = null;
  if (Number.isFinite(mobileMid) && Number.isFinite(mobileAfter) && mobileAfter <= mobileMid) {
    mobileConsumed = Math.max(0, mobileMid - mobileAfter);
  } else if (mobileTopUpDetected) {
    mobileConsumed = report.mobile_job?.people_requested ?? mobilePeople.length;
  }

  report.balances.accounting_note =
    emailTopUpDetected || mobileTopUpDetected
      ? "Surfe credit balances increased mid-run (account top-up / plan refresh). Email spend estimated from people_requested; mobile spend from post-top-up delta where available."
      : "Balances decreased monotonically — deltas are measured.";

  const summary = buildSummary(report.results, {
    emailConsumed: Number.isFinite(emailConsumed) ? emailConsumed : null,
    mobileConsumed: Number.isFinite(mobileConsumed) ? mobileConsumed : null,
  });
  finalizeCreditEconomics(summary);
  report.summary = summary;
  report.precision = summary.precision;
  report.stratification = stratify(report.results);
  report.production_gate_simulation = {
    WOULD_ACCEPT: report.results.filter(
      (r) => r.productionMergeSimulation === MERGE_SIMULATION.WOULD_ACCEPT
    ).length,
    WOULD_ACCEPT_AS_CORROBORATION_ONLY: report.results.filter(
      (r) =>
        r.productionMergeSimulation === MERGE_SIMULATION.WOULD_ACCEPT_AS_CORROBORATION_ONLY
    ).length,
    WOULD_HOLD_FOR_REVIEW: report.results.filter(
      (r) => r.productionMergeSimulation === MERGE_SIMULATION.WOULD_HOLD_FOR_REVIEW
    ).length,
    WOULD_REJECT: report.results.filter(
      (r) => r.productionMergeSimulation === MERGE_SIMULATION.WOULD_REJECT
    ).length,
  };
  report.economics = buildEconomics(summary);
  report.recommendation = buildRecommendation(summary);
  report.eligibility_recommendation = [
    "HYPOTHETICAL future Surfe eligibility (NOT ENABLED):",
    "- opportunityQualification IN (VERIFIED_OPEN, STRONG)",
    "- priority IN (HIGH_PRIORITY, strong MEDIUM_PRIORITY)",
    "- named person already established by Dealality (canonical identity exists)",
    "- role relevance strong (EVENT_MEETINGS_OWNER | DIRECT_DECISION_MAKER | EVENT_OPERATIONS_CONTACT | HOUSING_SOURCING_CONTACT)",
    "- canonical identity confidence high",
    "- Contact Grade <= C (or Grade A only when email is ROLE_BASED and phone weak)",
    "- useful direct email OR useful phone is missing",
    "",
    "HARD RULES:",
    "- Surfe MUST NOT create or replace canonical person identity",
    "- identity acceptance requires >=2 positive signals and zero contradictions",
    "- name + company domain alone is NEVER sufficient",
    "- REJECTED identities salvage zero fields",
    "- official source verified fields always win conflicts",
    "- CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED remains 0 until merge path ships",
  ].join("\n");

  report.future_escalation = {
    architecture: [
      "Qualified GDI Opportunity",
      "→ Named relevant person identified (Dealality WHO)",
      "→ Official-source contact research",
      "→ Canonical Contact Intelligence",
      "→ If reachability still weak AND paid flag ON",
      "→ Surfe reachability enrich (email, then mobile if justified)",
      "→ Multi-signal identity acceptance",
      "→ Field validation + official precedence",
      "→ Review / hypothetical merge (official > multi-source > provider verified > inferred)",
      "",
      "Default remains CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0.",
    ].join("\n"),
  };

  report.completed_at = new Date().toISOString();
  persist();
  if (isV2) writeMdV2();
  else writeMdV1Compat();

  console.log(
    JSON.stringify(
      {
        evaluation_only: true,
        production_writes: false,
        phase: report.phase,
        cohort: cohort.version,
        people: summary.peopleTested,
        meaningfulImproved: summary.meaningfulImproved,
        meaningfulRatePct: summary.meaningfulRatePct,
        acceptedResultPrecisionPct: summary.precision.acceptedResultPrecisionPct,
        providerRawFalseMatchCount: summary.precision.providerRawFalseMatchCount,
        production_gate_simulation: report.production_gate_simulation,
        credits: summary.credits,
        recommendation: report.recommendation.verdict,
        out_md: OUT_MD,
        out_json: OUT_JSON,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  report.errors.push({ message: String(err?.message || err), code: err?.code || null });
  persist();
  try {
    writeMdV2();
  } catch {
    /* ignore */
  }
  console.error(err);
  process.exit(1);
});
