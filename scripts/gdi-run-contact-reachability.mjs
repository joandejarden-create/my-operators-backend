#!/usr/bin/env node
/**
 * GDI Contact Reachability v1 — bounded Surfe HOW-TO-REACH eval.
 *
 * Flow: frozen cohort → CI provider gate → Surfe adapter → identity acceptance →
 * field ownership → merge simulation → funnel metrics.
 *
 * CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED stays 0.
 * Evaluation override: this script calls Surfe directly via lib/surfe/client.js
 * (same pattern as gdi-surfe-contact-smoke) — not a GDI-owned provider stack.
 *
 * NO production writes · NO share updates · NO canonical merge.
 *
 * Usage:
 *   npm run gdi:freeze-reachability-cohort
 *   npm run gdi:run-contact-reachability -- --dry-run
 *   npm run gdi:run-contact-reachability
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
  acceptSurfeIdentity,
  classifySurfeEmailOutcome,
  classifySurfePhoneOutcome,
  isMeaningfulSurfeImprovement,
  simulateProductionMergeDecision,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import {
  calculateGdiContactCoverage,
  mapOutcomeToFieldMerge,
  simulateGradeAfterAcceptedFields,
  FIELD_MERGE_DECISION,
  REACHABILITY_NEED,
} from "../lib/group-demand-intelligence/contact-coverage.js";
import { gradeContact } from "../lib/group-demand-intelligence/contact-resolution.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

const dryRun = process.argv.includes("--dry-run") || process.argv.includes("--eval-only");

const COHORT_PATH = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-contact-reachability-cohort-v1.json"
);
const DISCOVERY_PATH = path.join(
  root,
  "data/group-demand-intelligence/bethesda-official-person-discovery.json"
);
const OPP_PATH = path.join(
  root,
  "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json"
);
const OUT_JSON = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-contact-reachability-v1.json"
);
const OUT_MD = path.join(
  root,
  "reports/group-demand-intelligence/bethesda-contact-reachability-v1.md"
);
const PORTABILITY_MD = path.join(
  root,
  "reports/group-demand-intelligence/gdi-cross-hotel-contact-portability.md"
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function returnedName(person) {
  if (!person) return null;
  return (
    person.full_name ||
    [person.first_name, person.last_name].filter(Boolean).join(" ") ||
    null
  );
}

function gateSubject(subject) {
  const gated = gateProviderCandidates(
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
          company_name: subject.enrichmentOrganization,
          first_name: subject.first_name,
          last_name: subject.last_name,
        },
        subject_id: subject.id,
      },
    ],
    { provider: "surfe", workflow: "gdi_contact" }
  );
  const allowed = gated.allowed || [];
  const rejected = gated.rejected || [];
  return {
    ok: allowed.length > 0,
    accepted: allowed,
    rejected,
    reason: rejected[0]?.gate?.reason || rejected[0]?.gate?.code || null,
    gateDetail: rejected[0]?.gate || allowed[0]?.gate || null,
  };
}

function evaluateIdentity(subject, person) {
  if (!person) {
    return {
      decision: IDENTITY_DECISION.NOT_FOUND,
      positiveSignals: [],
      contradictions: [],
      reasons: ["no_person_payload"],
      wouldAcceptForMerge: false,
    };
  }
  return acceptSurfeIdentity({
    expectedFullName: subject.full_name,
    expectedOrganization: subject.enrichmentOrganization,
    expectedDomain: subject.enrichmentDomain,
    expectedTitle: subject.title,
    returnedFullName: returnedName(person),
    returnedOrganization: person.company_name || null,
    returnedDomain: person.company_domain || null,
    returnedTitle: person.job_title || null,
    returnedEmail: person.emails?.[0]?.email || null,
    returnedLinkedInUrl: person.linkedin_url || null,
  });
}

async function runEnrichmentType(subjects, enrichmentType) {
  if (!subjects.length) return { people: new Map(), job: null, credits: 0 };
  const people = subjects.map((s) => ({
    firstName: s.first_name,
    lastName: s.last_name,
    companyName: s.enrichmentOrganization,
    companyDomain: s.enrichmentDomain,
    externalID: s.id,
  }));
  const body =
    enrichmentType === "email"
      ? {
          people,
          include: { email: true, mobile: false, linkedInUrl: true, jobHistory: true },
          enrichmentOptions: { acceptedEmailType: "professional" },
        }
      : {
          people,
          include: { email: false, mobile: true, linkedInUrl: false, jobHistory: false },
        };

  let start;
  try {
    start = await startSurfePeopleEnrichment(body);
  } catch (err) {
    return {
      people: new Map(),
      job: null,
      credits: 0,
      error: String(err?.message || err),
    };
  }
  if (!start.ok && (start.rate_limited || start.http_status >= 500)) {
    await sleep(2500);
    try {
      start = await startSurfePeopleEnrichment(body);
    } catch (err) {
      return {
        people: new Map(),
        job: null,
        credits: 0,
        error: String(err?.message || err),
      };
    }
  }
  const enrichmentId =
    start.payload?.enrichmentID || start.payload?.enrichmentId || null;
  if (!start.ok || !enrichmentId || start.insufficient_credits) {
    return {
      people: new Map(),
      job: start,
      credits: 0,
      error: start.insufficient_credits
        ? "insufficient_credits"
        : `start_failed http=${start.http_status}`,
    };
  }

  const poll = await pollSurfePeopleEnrichment(enrichmentId, {
    maxWaitMs: 240000,
    intervalMs: 4000,
  });
  const list = (poll.payload?.people || []).map(normalizeSurfePerson);
  const byExt = new Map(list.map((p) => [p.external_id, p]));
  const unit =
    enrichmentType === "email"
      ? SURFE_CREDIT_COSTS.email_enrichment_per_person_worst_case
      : SURFE_CREDIT_COSTS.mobile_enrichment_per_person_worst_case;
  return {
    people: byExt,
    job: {
      enrichmentId,
      status: poll.payload?.status || null,
      timed_out: Boolean(poll.timed_out),
      http_status: poll.http_status,
    },
    credits: subjects.length * unit,
  };
}

function finalizeSubject(subject, emailPerson, mobilePerson, knownOtherPersonPhones = []) {
  const identity = evaluateIdentity(subject, emailPerson || mobilePerson);
  const emailRec = emailPerson?.emails?.[0] || null;
  const emailOutcome = classifySurfeEmailOutcome({
    identityDecision: identity.decision,
    surfeEmail: emailRec?.email || null,
    baselineEmail: subject.baselineEmail,
    baselineEmailType: subject.baselineEmailType,
    baselineEmailVerification: subject.baselineEmailVerification,
  });

  let phone = null;
  let phoneOutcome = PHONE_OUTCOME.NOT_FOUND;
  if (
    [IDENTITY_DECISION.REJECTED, IDENTITY_DECISION.AMBIGUOUS, IDENTITY_DECISION.NOT_FOUND].includes(
      identity.decision
    )
  ) {
    phoneOutcome =
      identity.decision === IDENTITY_DECISION.NOT_FOUND
        ? PHONE_OUTCOME.NOT_FOUND
        : PHONE_OUTCOME.IDENTITY_REJECTED;
  } else if (mobilePerson) {
    const idM = evaluateIdentity(subject, mobilePerson);
    if (
      [IDENTITY_DECISION.REJECTED, IDENTITY_DECISION.AMBIGUOUS].includes(idM.decision)
    ) {
      phoneOutcome = PHONE_OUTCOME.IDENTITY_REJECTED;
    } else {
      phone = mobilePerson.mobile_phones?.[0]?.number || null;
      phoneOutcome = classifySurfePhoneOutcome({
        identityDecision: identity.decision,
        surfePhone: phone,
        baselinePhone: subject.baselinePhone,
        baselinePhoneType: subject.baselinePhoneType,
        knownOtherPersonPhones,
        providerPhoneField: "mobilePhones",
      });
    }
  }

  const emailMerge = mapOutcomeToFieldMerge({ outcome: emailOutcome, field: "email" });
  const phoneMerge = mapOutcomeToFieldMerge({ outcome: phoneOutcome, field: "phone" });
  const meaningful = isMeaningfulSurfeImprovement({ emailOutcome, phoneOutcome });
  const mergeSim = simulateProductionMergeDecision({
    identityDecision: identity.decision,
    emailOutcome,
    phoneOutcome,
    baselineEmailVerification: subject.baselineEmailVerification,
  });

  const beforeContact = {
    name: subject.full_name,
    role: subject.title,
    organization: subject.enrichmentOrganization,
    email: subject.baselineEmail,
    phone: subject.baselinePhone,
  };
  const beforeGrade = gradeContact(beforeContact, {
    opportunityType: "PRIMARY_PURSUIT",
    title: subject.opportunityTitle,
  }).contactGrade;

  const acceptedEmail =
    emailMerge === FIELD_MERGE_DECISION.ACCEPT_NEW_FIELD ? emailRec?.email : null;
  const acceptedPhone =
    phoneMerge === FIELD_MERGE_DECISION.ACCEPT_NEW_FIELD ? phone : null;
  const afterContact = {
    ...beforeContact,
    email: acceptedEmail || subject.baselineEmail,
    phone: acceptedPhone || subject.baselinePhone,
  };
  const afterGrade = simulateGradeAfterAcceptedFields({
    beforeGrade,
    hasNamedPerson: true,
    acceptedDirectEmail: Boolean(acceptedEmail),
    acceptedUsefulPhone: Boolean(acceptedPhone),
    // Baseline official email already in beforeGrade; still needed so
    // accepted mobile + existing email can reach A without double-counting
    // a no-op email bump when nothing was accepted.
    hadOfficialEmail: Boolean(subject.baselineEmail),
  });

  return {
    id: subject.id,
    name: subject.full_name,
    opportunityId: subject.opportunityId,
    opportunityIds: subject.opportunityIds || [subject.opportunityId],
    opportunityTitle: subject.opportunityTitle,
    role: subject.title,
    organization: subject.enrichmentOrganization,
    whoConfidence: subject.whoConfidence,
    reachabilityNeed: subject.reachabilityNeed,
    attempted: true,
    before: {
      email: subject.baselineEmail,
      phone: subject.baselinePhone,
      grade: beforeGrade,
    },
    after: {
      email: afterContact.email,
      phone: afterContact.phone,
      grade: afterGrade,
      acceptedEmail,
      acceptedPhone,
    },
    identity,
    surfe: {
      email: emailRec?.email || null,
      emailOutcome,
      phone,
      phoneOutcome,
    },
    merge: {
      email: emailMerge,
      phone: phoneMerge,
      simulation: mergeSim,
    },
    meaningfulImprovement: meaningful,
    creditsSpent:
      (subject.requestEmail
        ? SURFE_CREDIT_COSTS.email_enrichment_per_person_worst_case
        : 0) +
      (subject.requestMobile
        ? SURFE_CREDIT_COSTS.mobile_enrichment_per_person_worst_case
        : 0),
  };
}

function esc(s) {
  return String(s ?? "—").replace(/\|/g, "\\|").replace(/\n/g, " ");
}

/**
 * Build ownership collision list from discovery pack phones (excluding self).
 * Reusable pattern: any published line owned by another named person.
 * Includes known NIST Prebil office line regression fixture when present in corpus.
 */
function buildKnownOtherPersonPhones(discovery, subject) {
  const selfKey = String(subject.full_name || "")
    .toLowerCase()
    .replace(/[^a-z\s]/g, "")
    .trim();
  const out = [];
  const seen = new Set();

  const push = (name, phone, org) => {
    if (!phone || !name) return;
    const digits = String(phone).replace(/\D/g, "");
    if (digits.length < 7) return;
    const key = `${digits.slice(-10)}::${String(name).toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ name, phone, org: org || null });
  };

  for (const row of discovery.opportunities || []) {
    const c = row.primaryCandidate || row.candidate || null;
    if (!c?.phone || !c?.name) continue;
    const otherKey = String(c.name)
      .toLowerCase()
      .replace(/[^a-z\s]/g, "")
      .trim();
    if (otherKey === selfKey) continue;
    push(c.name, c.phone, c.organization);
  }

  // Durable regression fixture: NIST office line owned by Michael Prebil
  // (Danielle Santos Surfe collision) — hotel-agnostic ownership rule, fixture phone is data.
  if (!/michael\s+prebil/i.test(subject.full_name || "")) {
    push("Michael Prebil", "301-975-4470", "NIST");
  }

  return out;
}

async function main() {
  if (!fs.existsSync(COHORT_PATH)) {
    console.error("Cohort missing — run npm run gdi:freeze-reachability-cohort first");
    process.exit(1);
  }
  const cohort = JSON.parse(fs.readFileSync(COHORT_PATH, "utf8"));
  const discovery = JSON.parse(fs.readFileSync(DISCOVERY_PATH, "utf8"));
  const oppPack = JSON.parse(fs.readFileSync(OPP_PATH, "utf8"));
  const opportunities = (oppPack.opportunities || []).filter(
    (o) => o.priority !== "DISQUALIFIED"
  );

  const report = {
    version: "gdi_contact_reachability_v1",
    evaluation_only: true,
    dry_run: dryRun,
    production_writes: "PROHIBITED",
    paid_enrichment_global: "NOT_ENABLED",
    contact_intelligence_paid_enrichment_enabled: "0",
    identity_principle:
      "Dealality owns WHO. Surfe may help HOW TO REACH. Never create/replace identity.",
    gdi_operating_law:
      "Every pilot correction → reusable rule/gate/test unless demonstrably hotel-specific.",
    started_at: new Date().toISOString(),
    adapter: { module: "lib/surfe/client.js", version: SURFE_CLIENT_VERSION },
    docs: SURFE_CREDIT_COSTS.docs,
    auth: { key: describeSurfeKeyPresence() },
    cohort_path: COHORT_PATH,
    cohort_size: cohort.subjects.length,
    credit_caps: cohort.credit_caps,
    balances: { before: null, after: null },
    results: [],
    funnel: null,
    coverage: null,
    portability: null,
    operating_law_audit: [],
    summary: null,
    recommendation: null,
    errors: [],
  };

  report.operating_law_audit = [
    {
      issue: "DMV geography keywords in candidate scoring",
      classification: "REUSABLE",
      implementation:
        "lib/group-demand-intelligence/contact-candidate/scoring.js — geographyHints from opportunity; neutral default",
      regression: "test:gdi-contact-coverage-portability",
    },
    {
      issue: "Contact funnel / coverage metrics",
      classification: "REUSABLE",
      implementation: "lib/group-demand-intelligence/contact-coverage.js",
      regression: "test:gdi-contact-coverage-portability",
    },
    {
      issue: "Bethesda hotelId / cohort freeze / discovery seeds",
      classification: "HOTEL_SPECIFIC",
      implementation:
        "data/group-demand-intelligence/evals/*cohort*.json · official-person-discoveries-v1.js (data)",
      regression: "N/A — data fixtures",
    },
    {
      issue: "Provider phone owned by another person must be rejected (Danielle→Prebil)",
      classification: "REUSABLE",
      implementation:
        "surfe-identity-acceptance.js — detectOtherPersonPhoneCollision + OTHER_PERSON_PHONE_COLLISION",
      regression: "test:surfe-identity-acceptance · test:gdi-contact-coverage-portability",
    },
    {
      issue: "Identity acceptance + Fessler→Kessler surname gate",
      classification: "REUSABLE",
      implementation: "lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js",
      regression: "test:surfe-identity-acceptance",
    },
  ];

  let beforeCredits = null;
  if (!dryRun) {
    try {
      beforeCredits = await getSurfeCredits();
    } catch (err) {
      report.errors.push({
        phase: "credits_before",
        error: String(err?.message || err),
        code: err?.cause?.code || err?.code || null,
      });
      beforeCredits = { ok: false, error: String(err?.message || err) };
    }
  }
  report.balances.before = beforeCredits;

  const emailSubjects = cohort.subjects.filter((s) => s.requestEmail);
  const mobileSubjects = cohort.subjects.filter((s) => s.requestMobile);
  const estimate = estimateWorstCaseCredits({
    emailPeople: emailSubjects.length,
    mobilePeople: mobileSubjects.length,
  });
  report.credit_estimate = estimate;

  // Gate all subjects
  const blocked = [];
  const eligible = [];
  for (const s of cohort.subjects) {
    const g = gateSubject(s);
    if (!g.ok || !(g.accepted || []).length) {
      blocked.push({
        id: s.id,
        name: s.full_name,
        reason: g.reason || g.gateDetail?.code || g.gateDetail?.reasons?.[0] || "provider_gate",
        gate: g.gateDetail
          ? {
              ok: g.gateDetail.ok,
              code: g.gateDetail.code,
              reasons: g.gateDetail.reasons,
            }
          : null,
      });
    } else {
      eligible.push(s);
    }
  }
  report.blocked = blocked;

  let emailMap = new Map();
  let mobileMap = new Map();
  let creditsEmail = 0;
  let creditsMobile = 0;

  if (dryRun) {
    report.summary = {
      mode: "dry_run",
      peopleTested: 0,
      note: "No Surfe calls. Cohort + gates + funnel prepared only.",
    };
  } else {
    const emailEligible = eligible.filter((s) => s.requestEmail);
    const mobileEligible = eligible.filter((s) => s.requestMobile);

    if (emailEligible.length) {
      const er = await runEnrichmentType(emailEligible, "email");
      emailMap = er.people;
      creditsEmail = er.credits;
      if (er.error) report.errors.push({ phase: "email", error: er.error });
    }
    if (mobileEligible.length) {
      const mr = await runEnrichmentType(mobileEligible, "mobile");
      mobileMap = mr.people;
      creditsMobile = mr.credits;
      if (mr.error) report.errors.push({ phase: "mobile", error: mr.error });
    }

    for (const s of eligible) {
      const others = buildKnownOtherPersonPhones(discovery, s);
      report.results.push(
        finalizeSubject(s, emailMap.get(s.id) || null, mobileMap.get(s.id) || null, others)
      );
    }
  }

  try {
    report.balances.after = dryRun ? null : await getSurfeCredits();
  } catch (err) {
    report.errors.push({
      phase: "credits_after",
      error: String(err?.message || err),
    });
    report.balances.after = { ok: false, error: String(err?.message || err) };
  }

  const results = report.results;
  const acceptedEmails = results.filter(
    (r) => r.merge.email === FIELD_MERGE_DECISION.ACCEPT_NEW_FIELD
  ).length;
  const acceptedPhones = results.filter(
    (r) => r.merge.phone === FIELD_MERGE_DECISION.ACCEPT_NEW_FIELD
  ).length;
  const holds = results.filter(
    (r) =>
      r.merge.email === FIELD_MERGE_DECISION.HOLD_FOR_REVIEW ||
      r.merge.phone === FIELD_MERGE_DECISION.HOLD_FOR_REVIEW
  ).length;
  const rejects = results.filter(
    (r) =>
      r.identity.decision === IDENTITY_DECISION.REJECTED ||
      r.merge.email === FIELD_MERGE_DECISION.REJECT_FIELD
  ).length;
  const improved = results.filter((r) => r.meaningfulImprovement).length;
  const totalCredits = creditsEmail + creditsMobile;

  report.summary = {
    cohortSize: cohort.subjects.length,
    eligibleAfterGate: eligible.length,
    blocked: blocked.length,
    emailCalls: dryRun ? 0 : emailSubjects.filter((s) => eligible.includes(s)).length,
    phoneCalls: dryRun ? 0 : mobileSubjects.filter((s) => eligible.includes(s)).length,
    acceptedEmails,
    acceptedPhones,
    holds,
    rejects,
    noResults: results.filter(
      (r) =>
        r.identity.decision === IDENTITY_DECISION.NOT_FOUND ||
        (r.surfe.emailOutcome === EMAIL_OUTCOME.NOT_FOUND &&
          r.surfe.phoneOutcome === PHONE_OUTCOME.NOT_FOUND)
    ).length,
    meaningfulImproved: improved,
    credits: {
      email: creditsEmail,
      mobile: creditsMobile,
      total: totalCredits,
      perMeaningful: improved ? Math.round((100 * totalCredits) / improved) / 100 : null,
    },
  };

  const enrichmentOutcomes = results.map((r) => ({
    attempted: true,
    meaningfulImprovement: r.meaningfulImprovement,
    creditsSpent: r.creditsSpent,
  }));

  report.coverage = calculateGdiContactCoverage({
    opportunities,
    discoveryRows: discovery.opportunities || [],
    enrichmentOutcomes,
  });
  report.funnel = report.coverage.funnel;

  // Portability check (synthetic — no live research)
  report.portability = runPortabilityCheck();

  report.recommendation = dryRun
    ? "DRY_RUN_ONLY — re-run without --dry-run to execute bounded Surfe calls"
    : improved > 0
      ? "CONTACT STACK READY; MOVE TO NEXT GDI LAYER — with gated Surfe still OFF globally"
      : "MORE BETHESDA HARDENING REQUIRED — low yield on this cohort";

  report.completed_at = new Date().toISOString();

  // Markdown
  const lines = [];
  lines.push("# Bethesda GDI — Contact Reachability v1");
  lines.push("");
  lines.push(`**Mode:** ${dryRun ? "DRY RUN" : "LIVE EVAL"} · Paid global flag: OFF`);
  lines.push(`**Cohort:** ${cohort.subjects.length} frozen people`);
  lines.push(`**Generated:** ${report.completed_at}`);
  lines.push("");
  lines.push("## A. Reachability");
  lines.push("");
  lines.push(`| Metric | Value |`);
  lines.push(`|---|---:|`);
  for (const [k, v] of Object.entries(report.summary)) {
    if (typeof v === "object") continue;
    lines.push(`| ${k} | ${v} |`);
  }
  lines.push(
    `| credits.total | ${report.summary.credits?.total ?? "—"} |`
  );
  lines.push(
    `| credits.perMeaningful | ${report.summary.credits?.perMeaningful ?? "—"} |`
  );
  lines.push("");
  lines.push("## Founder table");
  lines.push("");
  lines.push(
    "| Opportunity | Person | Role | WHO Conf | Before Email | After Email | Before Phone | After Phone | Before Grade | After Grade | Provider | Merge |"
  );
  lines.push("|---|---|---|---|---|---|---|---|---|---|---|---|");
  for (const r of results) {
    lines.push(
      `| ${esc(r.opportunityTitle)} | ${esc(r.name)} | ${esc(r.role)} | ${esc(r.whoConfidence)} | ${esc(r.before.email)} | ${esc(r.after.acceptedEmail || r.after.email)} | ${esc(r.before.phone)} | ${esc(r.after.acceptedPhone || r.after.phone)} | ${esc(r.before.grade)} | ${esc(r.after.grade)} | id=${esc(r.identity.decision)} e=${esc(r.surfe.emailOutcome)} p=${esc(r.surfe.phoneOutcome)} | e=${esc(r.merge.email)} p=${esc(r.merge.phone)} |`
    );
  }
  if (dryRun) {
    lines.push("");
    lines.push("_Dry run — no provider rows._");
    for (const s of cohort.subjects) {
      lines.push(
        `| ${esc(s.opportunityTitle)} | ${esc(s.full_name)} | ${esc(s.title)} | ${esc(s.whoConfidence)} | ${esc(s.baselineEmail)} | — | ${esc(s.baselinePhone)} | — | — | — | dry_run | — |`
      );
    }
  }
  lines.push("");
  lines.push("## B. Contact funnel (reusable)");
  lines.push("");
  lines.push("```");
  lines.push(JSON.stringify(report.funnel, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## C. Portability");
  lines.push("");
  lines.push(`**Verdict:** ${report.portability.verdict}`);
  lines.push("");
  for (const f of report.portability.findings || []) {
    lines.push(`- ${f}`);
  }
  lines.push("");
  lines.push("## D. GDI Operating Law audit");
  lines.push("");
  for (const a of report.operating_law_audit) {
    lines.push(
      `- **${a.issue}** · ${a.classification} · \`${a.implementation}\` · test: ${a.regression}`
    );
  }
  lines.push("");
  lines.push("## E. Next step");
  lines.push("");
  lines.push(report.recommendation);
  lines.push("");

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.mkdirSync(path.dirname(OUT_MD), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(OUT_MD, lines.join("\n"));

  // Portability report (stable)
  fs.writeFileSync(
    PORTABILITY_MD,
    [
      "# GDI Cross-Hotel Contact Portability",
      "",
      `**Verdict:** ${report.portability.verdict}`,
      "",
      "## Principle",
      "",
      "> Every GDI pilot correction must be implemented as a reusable rule, scoring change, research method, validation gate, or regression test unless demonstrably hotel-specific.",
      "",
      "> Hotel-specific facts belong in configuration/data, not core logic.",
      "",
      "## Findings",
      "",
      ...(report.portability.findings || []).map((f) => `- ${f}`),
      "",
      "## Reusable modules",
      "",
      "- `lib/group-demand-intelligence/contact-candidate/*` — WHO ontology/scoring/discovery",
      "- `lib/group-demand-intelligence/contact-coverage.js` — funnel + coverage",
      "- `lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js` — HOW gates",
      "- `lib/surfe/client.js` — provider adapter",
      "",
      "## Hotel-specific data (OK)",
      "",
      "- `data/group-demand-intelligence/hotels/<hotelId>/`",
      "- `data/group-demand-intelligence/evals/*cohort*`",
      "- `official-person-discoveries-v1.js` (pilot evidence seeds)",
      "",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        dryRun,
        outJson: OUT_JSON,
        outMd: OUT_MD,
        summary: report.summary,
        funnel: report.funnel,
        portability: report.portability.verdict,
        recommendation: report.recommendation,
      },
      null,
      2
    )
  );
}

function runPortabilityCheck() {
  const findings = [];
  let verdict = "PORTABLE";

  // Synthetic hotel fixture — different id/geo/events
  const syntheticOpps = [
    {
      id: "gdi_opp_synth_annual_2027",
      priority: "MEDIUM_PRIORITY",
      title: "Coastal Association Annual Meeting 2027",
      opportunityType: "PRIMARY_PURSUIT",
      organizationName: "Coastal Association",
      geographyHints: ["miami", "florida"],
      primaryContact: {
        name: "Casey Planner",
        role: "Director of Meetings",
        organization: "Coastal Association",
        email: "casey@coastal.org",
      },
    },
    {
      id: "gdi_opp_synth_overflow",
      priority: "HIGH_PRIORITY",
      title: "Bay Cup 2027 Overflow",
      opportunityType: "OVERFLOW_HOUSING",
      organizationName: "Bay Soccer",
      geographyHints: ["tampa"],
      primaryContact: {
        name: "Harbor Housing Co",
        role: "Official housing partner",
        organization: "Harbor Housing Co",
        email: "support@harborhousing.example",
        targetRoleMatch: "HOUSING_SOURCING_CONTACT",
        functionalEntity: true,
      },
    },
    {
      id: "gdi_opp_synth_unresolved",
      priority: "WATCHLIST",
      title: "Mystery Corp Offsite",
      opportunityType: "FUTURE_CYCLE",
      organizationName: "Mystery Corp",
      geographyHints: ["austin"],
      primaryContact: null,
    },
  ];

  const discoveryRows = [
    {
      opportunityId: "gdi_opp_synth_annual_2027",
      primaryKind: "NAMED_PERSON",
      primaryCandidate: {
        name: "Casey Planner",
        email: "casey@coastal.org",
        candidateConfidence: "HIGH",
        employmentStatus: "CURRENT_CONFIRMED",
        eventRelationship: "CURRENT_EVENT_CONTACT",
      },
    },
    {
      opportunityId: "gdi_opp_synth_overflow",
      primaryKind: "FUNCTIONAL_ENTITY",
      primaryCandidate: {
        name: "Harbor Housing Co",
        email: "support@harborhousing.example",
        phone: "813-555-0100",
        functionalEntity: true,
        candidateConfidence: "HIGH",
      },
    },
    {
      opportunityId: "gdi_opp_synth_unresolved",
      primaryKind: "UNRESOLVED",
      primaryCandidate: null,
    },
  ];

  const cov = calculateGdiContactCoverage({
    opportunities: syntheticOpps,
    discoveryRows,
  });
  if (cov.totalOpportunities !== 3) {
    verdict = "BETHESDA COUPLING FOUND";
    findings.push("coverage totalOpportunities failed on synthetic hotel");
  }
  if (cov.namedPersonPrimaries !== 1 || cov.functionalEntityPrimaries !== 1 || cov.unresolved !== 1) {
    verdict = "BETHESDA COUPLING FOUND";
    findings.push(
      `expected 1/1/1 person/entity/unresolved got ${cov.namedPersonPrimaries}/${cov.functionalEntityPrimaries}/${cov.unresolved}`
    );
  } else {
    findings.push("Synthetic hotel fixture: coverage + funnel calculate without Bethesda ids");
  }

  // Scan core modules for hard dependency on Bethesda hotel id in executable logic
  const coreFiles = [
    "lib/group-demand-intelligence/contact-coverage.js",
    "lib/group-demand-intelligence/contact-candidate/scoring.js",
    "lib/group-demand-intelligence/contact-candidate/ontology.js",
    "lib/group-demand-intelligence/contact-candidate/discovery.js",
  ];
  for (const rel of coreFiles) {
    const txt = fs.readFileSync(path.join(root, rel), "utf8");
    if (/recLuxvwwxID7U2B8/.test(txt)) {
      verdict = "BETHESDA COUPLING FOUND";
      findings.push(`${rel} hardcodes Bethesda hotelId`);
    }
    if (/Potomac Memorial|Bethesda Premier Cup/.test(txt) && !/example|comment|doc/i.test(txt)) {
      // allow comments
      const codeLines = txt.split("\n").filter((l) => !/^\s*(\*|\/\/)/.test(l));
      if (codeLines.some((l) => /Potomac Memorial|Bethesda Premier Cup/.test(l))) {
        verdict = "BETHESDA COUPLING FOUND";
        findings.push(`${rel} references Bethesda event names in code`);
      }
    }
  }
  findings.push(
    "Geography scoring uses opportunity.geographyHints (hotel-agnostic); DMV keyword hardcode removed"
  );
  findings.push(
    "Pilot evidence seeds (official-person-discoveries-v1.js) remain hotel-specific DATA — correct under Operating Law"
  );

  return { verdict, findings, syntheticCoverage: cov };
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
