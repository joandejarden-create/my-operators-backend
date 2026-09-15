#!/usr/bin/env node
/**
 * Official-source person discovery for unresolved Bethesda GDI opportunities.
 * Merges OFFICIAL_PERSON_DISCOVERIES_V1 into candidate discovery. Surfe/paid OFF.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  discoverContactCandidatesForOpportunities,
  OFFICIAL_PERSON_DISCOVERY_PASS_ID,
  PRIMARY_KIND,
  ENRICHMENT_GAP,
  NO_PROBABLE,
} from "../lib/group-demand-intelligence/contact-candidate/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const HOTEL_ID = "recLuxvwwxID7U2B8";

const priorPath = path.join(root, "data/group-demand-intelligence/bethesda-contact-candidates.json");
const oppPath = path.join(
  root,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json"
);
const outJson = path.join(
  root,
  "data/group-demand-intelligence/bethesda-official-person-discovery.json"
);
const outMd = path.join(
  root,
  "reports/group-demand-intelligence/bethesda-official-person-discovery.md"
);

const prior = JSON.parse(fs.readFileSync(priorPath, "utf8"));
const priorUnresolvedIds = new Set(
  (prior.opportunities || [])
    .filter((o) => o.resolution === NO_PROBABLE || !o.primaryCandidate)
    .map((o) => o.opportunityId)
);

const raw = JSON.parse(fs.readFileSync(oppPath, "utf8"));
const qualified = (raw.opportunities || []).filter((o) => o.priority !== "DISQUALIFIED");

const beforePerson = (prior.opportunities || []).filter(
  (o) => o.primaryCandidate && o.primaryKind !== PRIMARY_KIND.FUNCTIONAL_ENTITY
).length;
// Prior file may lack primaryKind — infer from name heuristics
function inferPriorKind(row) {
  if (row.primaryKind) return row.primaryKind;
  const n = row.primaryCandidate?.name || "";
  if (!row.primaryCandidate) return PRIMARY_KIND.UNRESOLVED;
  if (/hbc|expovision|maritz|traveling teams|event services/i.test(n)) {
    return PRIMARY_KIND.FUNCTIONAL_ENTITY;
  }
  if (/^[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3}$/.test(n.trim())) {
    return PRIMARY_KIND.NAMED_PERSON;
  }
  return PRIMARY_KIND.FUNCTIONAL_ENTITY;
}

let beforeNamed = 0;
let beforeEntity = 0;
let beforeUnresolved = 0;
for (const o of prior.opportunities || []) {
  const k = inferPriorKind(o);
  if (k === PRIMARY_KIND.NAMED_PERSON) beforeNamed += 1;
  else if (k === PRIMARY_KIND.FUNCTIONAL_ENTITY) beforeEntity += 1;
  else beforeUnresolved += 1;
}

const result = discoverContactCandidatesForOpportunities(qualified);
const after = result.summary;

const newlyDiscovered = [];
const historicalValidated = [];
const historicalRejected = [];
const successorRoles = [];
const stillHard = [];

for (const row of result.opportunities) {
  const wasUnresolved = priorUnresolvedIds.has(row.opportunityId);
  if (wasUnresolved && row.primaryCandidate) {
    newlyDiscovered.push({
      id: row.opportunityId,
      name: row.primaryCandidate.name,
      kind: row.primaryKind,
      score: row.primaryCandidate.candidateScore,
      confidence: row.primaryCandidate.candidateConfidence,
    });
  }
  if (!row.primaryCandidate) {
    stillHard.push(row.title);
  }
  for (const c of row.audit?.allCandidates || []) {
    if (c.eventRelationship === "HISTORICAL_EVENT_CONTACT" && c.employmentStatus?.startsWith("CURRENT")) {
      historicalValidated.push(c.name);
    }
  }
  for (const r of row.audit?.rejectedCandidates || []) {
    if (/former|superseded|linkedin/i.test(r.reason || "")) {
      historicalRejected.push({ name: r.name, reason: r.reason });
    }
  }
  if (row.primaryCandidate?.eventRelationship === "CURRENT_SUCCESSOR_ROLE") {
    successorRoles.push(row.primaryCandidate.name);
  }
}

const enrichmentQueue = [];
const seen = new Set();
for (const row of result.opportunities) {
  for (const c of [row.primaryCandidate, ...(row.backupCandidates || [])].filter(Boolean)) {
    if (!c.surfeEligible) continue;
    if (c.primaryKind === PRIMARY_KIND.FUNCTIONAL_ENTITY || c.functionalEntity) continue;
    const key = `${row.opportunityId}::${String(c.name).toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    enrichmentQueue.push({
      opportunityId: row.opportunityId,
      title: row.title,
      name: c.name,
      role: c.role,
      organization: c.organization,
      confidence: c.candidateConfidence,
      score: c.candidateScore,
      gap: c.enrichmentGap,
      email: c.email || null,
      phone: c.phone || null,
      nextAction:
        c.enrichmentGap === ENRICHMENT_GAP.EMAIL_GAP
          ? "SURFE_EMAIL"
          : c.enrichmentGap === ENRICHMENT_GAP.PHONE_GAP
            ? "SURFE_PHONE"
            : c.enrichmentGap === ENRICHMENT_GAP.BOTH_MISSING
              ? "SURFE_BOTH"
              : "NONE",
    });
  }
}

const emailCalls = enrichmentQueue.filter((q) =>
  ["SURFE_EMAIL", "SURFE_BOTH"].includes(q.nextAction)
).length;
const phoneCalls = enrichmentQueue.filter((q) =>
  ["SURFE_PHONE", "SURFE_BOTH"].includes(q.nextAction)
).length;

function esc(s) {
  return String(s ?? "—").replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function nextAction(row) {
  if (!row.primaryCandidate) return "DEEPEN_OFFICIAL_RESEARCH";
  if (row.primaryCandidate.surfeEligible) {
    return row.primaryCandidate.enrichmentGap || "REACHABILITY_PASS";
  }
  if (row.primaryKind === PRIMARY_KIND.FUNCTIONAL_ENTITY) {
    return "ENTITY_OUTREACH / optional person backup";
  }
  return "NO_PAID_ENRICHMENT";
}

const lines = [];
lines.push("# Bethesda GDI — Official-Source Person Discovery");
lines.push("");
lines.push(`**Pass:** \`${OFFICIAL_PERSON_DISCOVERY_PASS_ID}\``);
lines.push(`**Hotel:** Bethesda Marriott (\`${HOTEL_ID}\`)`);
lines.push(`**Generated:** ${result.generatedAt}`);
lines.push(`**Prior unresolved in scope:** ${priorUnresolvedIds.size}`);
lines.push(`**Paid enrichment / Surfe:** OFF`);
lines.push("");
lines.push("## Success question");
lines.push("");
lines.push(
  "> Can Dealality reliably discover WHO the likely hotel/venue/housing decision-maker is from official/public evidence before spending money on contact enrichment?"
);
lines.push("");
lines.push("## Before vs After (person vs entity split)");
lines.push("");
lines.push("| Metric | Before | After |");
lines.push("|---|---:|---:|");
lines.push(`| Total opportunities | 29 | 29 |`);
lines.push(`| Named person primaries | ${beforeNamed} | ${after.namedPersonPrimaries} |`);
lines.push(`| Functional entity primaries | ${beforeEntity} | ${after.functionalEntityPrimaries} |`);
lines.push(`| Unresolved | ${beforeUnresolved} | ${after.unresolved} |`);
lines.push(`| High-confidence primary | — | ${after.highConfidencePrimary} |`);
lines.push(`| Medium-confidence primary | — | ${after.mediumConfidencePrimary} |`);
lines.push(`| ≥2 credible candidates | — | ${after.withAtLeastTwoCandidates} |`);
lines.push("");
lines.push(
  `**Converted from prior unresolved → resolved:** ${newlyDiscovered.length} of ${priorUnresolvedIds.size}`
);
lines.push("");
lines.push("## Founder table (all 29)");
lines.push("");
lines.push(
  "| Opportunity | Primary Person/Entity | Kind | Role | Score | Confidence | Event Evidence | Email | Phone | Next Action |"
);
lines.push("|---|---|---|---|---:|---|---|---|---|---|");

for (const row of result.opportunities) {
  const p = row.primaryCandidate;
  const kind = row.primaryKind || PRIMARY_KIND.UNRESOLVED;
  lines.push(
    `| ${esc(row.title)} | ${esc(p?.name || "UNRESOLVED")} | ${kind} | ${esc(p?.gdiContactRole || "—")} | ${p?.candidateScore ?? "—"} | ${esc(p?.candidateConfidence || "UNRESOLVED")} | ${esc(p?.eventRelationship || "—")} | ${esc(p?.email)} | ${esc(p?.phone)} | ${esc(nextAction(row))} |`
  );
}

lines.push("");
lines.push("## Newly discovered (from prior unresolved)");
lines.push("");
for (const n of newlyDiscovered) {
  lines.push(
    `- **${n.name}** (${n.kind}) — \`${n.id}\` · score ${n.score} · ${n.confidence}`
  );
}
lines.push("");
lines.push("## Historical contacts");
lines.push("");
lines.push(`- Validated current/probable: ${[...new Set(historicalValidated)].join(", ") || "—"}`);
lines.push(
  `- Rejected / superseded: ${historicalRejected.map((r) => `${r.name} (${r.reason})`).join("; ") || "—"}`
);
lines.push(`- Successor roles found: ${successorRoles.join(", ") || "—"}`);
lines.push("");
lines.push("## Remaining hardest (still unresolved)");
lines.push("");
for (const t of stillHard) lines.push(`- ${t}`);
lines.push("");
lines.push("## Contact enrichment queue (NOT executed)");
lines.push("");
lines.push(`| # | Name | Opportunity | Gap | Next |`);
lines.push(`|---:|---|---|---|---|`);
enrichmentQueue.forEach((q, i) => {
  lines.push(`| ${i + 1} | ${esc(q.name)} | ${esc(q.title)} | ${q.gap} | ${q.nextAction} |`);
});
lines.push("");
lines.push(`- Surfe email calls justified: **${emailCalls}**`);
lines.push(`- Surfe phone calls justified: **${phoneCalls}**`);
lines.push(`- Queue size: **${enrichmentQueue.length}**`);
lines.push("");
lines.push("## Do NOT pay-enrich (identity too weak / unresolved)");
lines.push("");
for (const row of result.opportunities) {
  if (row.primaryKind === PRIMARY_KIND.UNRESOLVED) {
    lines.push(`- ${row.title}`);
  } else if (
    row.primaryCandidate &&
    ["LOW", "UNRESOLVED"].includes(row.primaryCandidate.candidateConfidence)
  ) {
    lines.push(`- ${row.title} (${row.primaryCandidate.name} — low confidence)`);
  }
}
lines.push("");
lines.push("## Final verdict");
lines.push("");
lines.push(
  `1. **Converted:** ${newlyDiscovered.length} of ${priorUnresolvedIds.size} prior-unresolved now have a credible person or functional entity.`
);
lines.push(`2. **Still unresolved:** ${after.unresolved}`);
lines.push(
  `3. **Easiest families:** medical/scientific staff directories (ACTS, BEBPA, SFB), association housing (NADO Jamie, ASAE Expovision), sports stay-to-play entities (Traveling Teams). **Hardest:** corporate watches, ECS (inbox only), Georgetown (inbox only), Arlington (no named housing), NIH SBPO.`
);
lines.push(
  `4. **Reachability-eligible people:** ${enrichmentQueue.length}`
);
lines.push(`5. **Surfe email calls:** ${emailCalls}`);
lines.push(`6. **Surfe phone calls:** ${phoneCalls}`);
lines.push(
  `7. **No paid enrichment:** all ${after.unresolved} unresolved + any LOW-confidence rows above.`
);
lines.push("");
lines.push(
  "**Answer:** Yes for opportunities with official staff/event pages — Dealality can establish WHO before Surfe. Remaining gaps need deeper prospectus/PDF research or simply wait for organizers to publish contacts; do not invent names."
);
lines.push("");

const payload = {
  passId: OFFICIAL_PERSON_DISCOVERY_PASS_ID,
  generatedAt: result.generatedAt,
  hotelId: HOTEL_ID,
  paidEnrichmentEnabled: false,
  surfeEnabled: false,
  priorUnresolvedCount: priorUnresolvedIds.size,
  before: {
    namedPersonPrimaries: beforeNamed,
    functionalEntityPrimaries: beforeEntity,
    unresolved: beforeUnresolved,
  },
  after,
  newlyDiscovered,
  historicalValidated: [...new Set(historicalValidated)],
  historicalRejected,
  successorRoles,
  stillHard,
  enrichmentQueue,
  surfeEmailCallsJustified: emailCalls,
  surfePhoneCallsJustified: phoneCalls,
  opportunities: result.opportunities,
};

fs.mkdirSync(path.dirname(outJson), { recursive: true });
fs.mkdirSync(path.dirname(outMd), { recursive: true });
fs.writeFileSync(outJson, JSON.stringify(payload, null, 2));
fs.writeFileSync(outMd, lines.join("\n"));

console.log(
  JSON.stringify(
    {
      passId: OFFICIAL_PERSON_DISCOVERY_PASS_ID,
      outJson,
      outMd,
      before: payload.before,
      after,
      converted: newlyDiscovered.length,
      enrichmentQueue: enrichmentQueue.length,
      emailCalls,
      phoneCalls,
    },
    null,
    2
  )
);
