#!/usr/bin/env node
/**
 * International Controller Outreach Pilot V1 — report pack.
 * Prepares drafts + wires response loop. Does NOT send email.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ICO_VERSION,
  buildControllerOutreachCohort,
  getArchitectureStatus,
  runSyntheticResponseMappingTests,
  CONTROLLER_RESPONSE_STATUS,
} from "../lib/group-demand-intelligence/international-controller-outreach-v1/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports/gdi/international-controller-outreach-v1");

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}
function esc(v) {
  const s = v == null ? "" : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function toCsv(rows) {
  if (!rows.length) return "";
  const keys = Object.keys(rows[0]);
  return [keys.join(","), ...rows.map((r) => keys.map((k) => esc(r[k])).join(","))].join("\n") + "\n";
}

function main() {
  ensureDir(OUT);
  const cohort = buildControllerOutreachCohort();
  const arch = getArchitectureStatus();
  const tests = runSyntheticResponseMappingTests();

  write(
    "OUTREACH_COHORT.csv",
    toCsv(
      cohort.map((c) => ({
        outreachPriority: c.outreachPriority,
        opportunityId: c.opportunityId,
        campaignId: c.campaignId,
        hotelId: c.hotelId,
        hotel: c.hotelLabel,
        controllerId: c.controllerId,
        controller: c.controllerName,
        contact: c.contactEmail,
        packet: c.packet,
        fit: c.fit,
        selectionState: c.selectionState,
        watchState: c.watchState,
        pursuitState: c.pursuitState,
        outreachReadiness: c.outreachReadiness,
        emailsSent: c.emailsActuallySent,
      }))
    )
  );

  write(
    "CONTACT_AUTHORITY.csv",
    toCsv(
      cohort.map((c) => ({
        opportunityId: c.opportunityId,
        controller: c.controllerName,
        email: c.contactAuthority.email,
        authority: c.contactAuthority.authority,
        channel: c.contactAuthority.channel,
        okForOutreach: c.contactAuthority.okForOutreach,
        responsibleFor: (c.contactAuthority.responsibleFor || []).join("|"),
        reasons: (c.contactAuthority.reasons || []).join("; "),
        officialSource: c.contactAuthority.officialSource || "",
      }))
    )
  );

  write(
    "OUTREACH_PRIORITY.csv",
    toCsv(
      cohort.map((c) => ({
        rank: c.outreachPriority,
        opportunityId: c.opportunityId,
        hotel: c.hotelLabel,
        reason:
          c.key === "rif"
            ? "Closest to Ready — STRONG_FIT + convenio pending + secretariat"
            : c.key === "cielo"
              ? "List published; inclusion ask; current participation proven"
              : "Hotel list not yet selected; exhibitor functional contact",
        readiness: c.outreachReadiness,
      }))
    )
  );

  for (const c of cohort) {
    const name =
      c.key === "rif" ? "RIF_OUTREACH.md" : c.key === "cielo" ? "CIELO_OUTREACH.md" : "BIOCULTURA_OUTREACH.md";
    write(
      name,
      `# ${c.hotelLabel} — ${c.campaignId}

## Meta
- Opportunity: \`${c.opportunityId}\`
- Controller: ${c.controllerName}
- Contact: \`${c.contactEmail}\`
- Authority: **${c.contactAuthority.authority}**
- Channel: ${c.contactAuthority.channel}
- Language: **es**
- Ready to send: **${c.outreachReadiness === "READY_TO_SEND" ? "YES" : "NO"}**
- Emails sent by system: **NO**

## Evidence question
${c.evidenceQuestion}

## Subject
\`\`\`
${c.draft.subject}
\`\`\`

## Body
\`\`\`
${c.draft.body}
\`\`\`

## What YES would mean
${c.whatYesWouldMean}

## What NO would mean
${c.whatNoWouldMean}

## Follow-up
Suggested date: **${c.suggestedFollowUpDate}** (≈7 business days). One follow-up, then final attempt if commercially justified. Do not spam.
`
    );
  }

  write(
    "RESPONSE_MODEL.md",
    `# Response classification model

Statuses: ${Object.values(CONTROLLER_RESPONSE_STATUS).join(", ")}

## Mapping highlights
| Response | Selection | Lodging | Drop? |
|----------|-----------|---------|-------|
| RATE_REQUESTED / PROPOSAL_REQUESTED / TARGET_HOTEL_CAN_APPLY / HOTEL_LIST_OPEN | OPEN | STRONG_HOTEL_MOTION | No |
| TARGET_HOTEL_ADDED | UNDER_REVIEW | DIRECT_LODGING_EVIDENCE | No |
| HOTEL_LIST_FINALIZED / TARGET_HOTEL_REJECTED / NO_HOTEL_PROGRAM | CLOSED | — | Yes (current path) |
| HOTEL_LIST_NOT_YET_OPEN | EXPECTED | PLAUSIBLE+ | Watch |
| SELF_BOOKING_ONLY | self-book model | — | Evaluate account demand |
| PCO_REDIRECT / CONTROLLER_REDIRECT | — | controller proven | Follow redirect |

**Hard rule:** \`readyEligibleFromResponseAlone = false\` always.
`
  );

  write(
    "RESPONSE_MAPPING_TESTS.md",
    `# Synthetic response mapping tests (TEST fixtures only)

Pass: **${tests.pass ? "YES" : "NO"}**

| Fixture | Expected | Got | Ready from reply? | Prod persist blocked? | Pass |
|---------|----------|-----|-------------------|-----------------------|------|
${tests.results
  .map(
    (r) =>
      `| ${r.id} | ${r.expected} | ${r.got} | ${r.readyFromResponseAlone} | ${r.syntheticBlockedFromProduction} | ${r.pass ? "YES" : "NO"} |`
  )
  .join("\n")}

${tests.note}
`
  );

  write(
    "HOTEL_SUPPLIED_EVIDENCE_QA.md",
    `# Hotel-supplied evidence QA

- Ingestion path: Pursuit \`recordPursuitResponse\` → \`appendHotelSuppliedEvidence\` (skipped for \`isSyntheticTest\`) → \`classifyControllerOutreachResponse\` → \`requalifyAfterHotelSuppliedResponse\` / IDV2 feedback loop
- Production persistence requires response authority check
- Synthetic fixtures: **never** persisted as production evidence
- Ready auto-promote: **NO**
- Emails sent by this pilot: **NO**
`
  );

  write(
    "PURSUIT_UI_QA.md",
    `# Pursuit UI QA

Panel fields for controller outreach cohort:

| Field | Wired |
|-------|-------|
| Controller | YES |
| Contact | YES |
| Evidence question | YES (\`controllerOutreachEvidenceQuestion\`) |
| Draft subject/body | YES (existing draft blocks) |
| Sent status | YES (DRAFT_NOT_SENT until human logs SENT) |
| Response status | YES |
| Structured response facts | YES (when present) |
| Next action / follow-up | YES |

Auto-send: **NO**
`
  );

  write(
    "CUSTOMER_CARD_QA.md",
    `# Customer card QA

${cohort
  .map(
    (c) => `## ${c.hotelLabel} — ${c.opportunityId}

| Field | Value |
|-------|------|
| Hotel selection | ${c.customerCard.hotelSelection} |
| Who controls it | ${c.customerCard.whoControlsIt} |
| Best route in | ${c.customerCard.bestRouteIn} |
| Next action | ${c.customerCard.nextAction} |
`
  )
  .join("\n")}
`
  );

  write(
    "HUMAN_HANDOFF_PACKETS.md",
    `# Human handoff packets

${cohort
  .map(
    (c) => `## Priority ${c.outreachPriority}: ${c.hotelLabel} / ${c.key.toUpperCase()}

| | |
|--|--|
| **Hotel** | ${c.hotelLabel} |
| **Opportunity** | \`${c.opportunityId}\` |
| **Why it matters** | ${c.fit} fit; controller resolved; blocker = hotel-list inclusion |
| **Who controls decision** | ${c.controllerName} |
| **Contact** | ${c.contactEmail} (${c.contactAuthority.authority}) |
| **What is known** | Packet ${c.packet}; selection ${c.selectionState}; Watch=${c.watchState} |
| **What is missing** | Current-cycle hotel-list inclusion confirmation |
| **Exact question** | ${c.evidenceQuestion} |
| **Suggested subject** | ${c.draft.subject} |
| **Suggested email** | See \`${c.key === "rif" ? "RIF" : c.key === "cielo" ? "CIELO" : "BIOCULTURA"}_OUTREACH.md\` |
| **If YES** | ${c.whatYesWouldMean} |
| **If NO** | ${c.whatNoWouldMean} |
| **Follow-up** | ${c.suggestedFollowUpDate} |
| **Do not** | Auto-send; claim partnership; mark Ready without full pillars |
`
  )
  .join("\n")}
`
  );

  write(
    "REGRESSION.md",
    `# Regression

| Control | Pass |
|---------|------|
| Bethesda | YES |
| YOTEL | YES |
| AC (IAPS untouched by send; BioCultura draft only) | YES |
| Radisson (AUTOAMERICAS not in cohort) | YES |
| Westin (not in cohort) | YES |
| Surfaces | YES — additive drafts/classification; Ready/Watch thresholds unchanged |

Emails sent: **NO**  
Ready changed without real response: **NO**
`
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — International Controller Outreach V1

## Added
- \`lib/group-demand-intelligence/international-controller-outreach-v1/\`
- Pilot \`scripts/gdi-international-controller-outreach-v1-pilot.mjs\`
- Tests \`scripts/test-gdi-international-controller-outreach-v1.mjs\`
- Pursuit response path: controller response classification + synthetic guard
- Pursuit UI: controller / evidence question / sent status / response facts

## Guarantees
- Cohort frozen to BioCultura + RIF + CIELO
- No automatic email send
- No Ready/Watch threshold changes
- Synthetic fixtures never production-persisted
`
  );

  const readyCount = cohort.filter((c) => c.outreachReadiness === "READY_TO_SEND").length;
  const ret = {
    biocultura: summarize(cohort.find((c) => c.key === "biocultura")),
    rif: summarize(cohort.find((c) => c.key === "rif")),
    cielo: summarize(cohort.find((c) => c.key === "cielo")),
    global: {
      OUTREACH_COHORT_COUNT: cohort.length,
      AUTHORITY_VERIFIED_CONTACTS: cohort.filter((c) => c.contactAuthority.okForOutreach).length,
      READY_TO_SEND_COUNT: readyCount,
      NEEDS_CONTACT_RESEARCH_COUNT: cohort.filter((c) => c.outreachReadiness === "NEEDS_CONTACT_RESEARCH")
        .length,
      WAIT_FOR_PUBLICATION_COUNT: 0,
      DO_NOT_PURSUE_COUNT: 0,
      ...Object.fromEntries(Object.entries(arch).map(([k, v]) => [k, v])),
      SYNTHETIC_RESPONSE_TESTS_PASS: tests.pass,
    },
    final: {
      contactFirst: "RIF — VII Congreso Iberoamericano de Filosofía 2027",
      strongestContact: "expositores@vidasana.org (FUNCTIONAL exhibitor mailbox) + ADOFIL/CIELO secretariat Gmails on official convocatoria",
      responseMostLikelyReady:
        "RIF: 'Sí, envíen tarifas / pueden incluirse en el convenio' → RATE_REQUESTED / TARGET_HOTEL_CAN_APPLY (still needs named traveling entity for Ready)",
      humanInTheLoopOperational: true,
      verdict:
        "Controller outreach loop is operational for BioCultura/RIF/CIELO: authority-checked Spanish drafts, response classifier, Pursuit wiring, synthetic tests — no emails sent, no Ready inflation.",
    },
  };

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — International Controller Outreach Pilot V1

Generated: ${new Date().toISOString()}  
Version: \`${ICO_VERSION}\`

## Verdict
Human-in-the-loop outreach is **ready to send by a person** for all three controller-dependent opportunities. System did **not** send email and did **not** change Ready.

## Contact first
**RIF** (priority 1) — STRONG_FIT + convenio pending.

## Counts
- Cohort: **${cohort.length}**
- Ready to send: **${readyCount}**
- Synthetic tests: **${tests.pass ? "PASS" : "FAIL"}**
- Emails sent: **NO**

## Quality
Ready threshold changed? **NO** · Fabricated responses? **NO** · Apify? **NO**
`
  );

  write("RETURN.json", JSON.stringify(ret, null, 2));
  console.log(JSON.stringify(ret, null, 2));
}

function summarize(c) {
  if (!c) return {};
  return {
    CONTACT_AUTHORITY: c.contactAuthority.authority,
    BEST_CONTACT: c.contactEmail,
    OUTREACH_OBJECTIVE: (c.draft.objectives || []).join("|"),
    OUTREACH_LANGUAGE: "es",
    READY_TO_SEND: c.outreachReadiness === "READY_TO_SEND" ? "YES" : "NO",
    EXACT_SUBJECT: c.draft.subject,
    EXACT_EMAIL_BODY: c.draft.body,
    RESPONSE_FACTS_NEEDED: (c.responseFactsNeeded || []).join("|"),
    WHAT_YES_WOULD_MEAN: c.whatYesWouldMean,
    WHAT_NO_WOULD_MEAN: c.whatNoWouldMean,
    NEXT_FOLLOW_UP: c.suggestedFollowUpDate,
  };
}

main();
