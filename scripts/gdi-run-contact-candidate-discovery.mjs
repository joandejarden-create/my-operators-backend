#!/usr/bin/env node
/**
 * Run GDI Contact Candidate Discovery across Bethesda qualified opportunities.
 * WHO discovery only — CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED stays OFF.
 * Does not mutate opportunities.json unless --apply (default: report-only).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  discoverContactCandidatesForOpportunities,
  buildBeforeSnapshot,
  CONTACT_CANDIDATE_DISCOVERY_PASS_ID,
  NO_PROBABLE,
} from "../lib/group-demand-intelligence/contact-candidate/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const HOTEL_ID = "recLuxvwwxID7U2B8";

const apply = process.argv.includes("--apply");
const oppPath = path.join(
  root,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json"
);
const outJson = path.join(root, "data/group-demand-intelligence/bethesda-contact-candidates.json");
const outMd = path.join(
  root,
  "reports/group-demand-intelligence/bethesda-contact-candidate-discovery.md"
);

const raw = JSON.parse(fs.readFileSync(oppPath, "utf8"));
const all = raw.opportunities || [];
const qualified = all.filter((o) => o.priority !== "DISQUALIFIED");

const before = buildBeforeSnapshot(qualified);
const result = discoverContactCandidatesForOpportunities(qualified);
const after = result.summary;

function esc(s) {
  return String(s ?? "—").replace(/\|/g, "\\|").replace(/\n/g, " ");
}

const lines = [];
lines.push("# Bethesda GDI — Contact Candidate Discovery");
lines.push("");
lines.push(`**Pass:** \`${CONTACT_CANDIDATE_DISCOVERY_PASS_ID}\``);
lines.push(`**Hotel:** Bethesda Marriott (\`${HOTEL_ID}\`)`);
lines.push(`**Generated:** ${result.generatedAt}`);
lines.push(`**Paid enrichment:** OFF · **Surfe:** OFF (discovery does not establish WHO via providers)`);
lines.push("");
lines.push("## Success question");
lines.push("");
lines.push(
  "> For each GDI opportunity, can Dealality identify the most probable person or people who influence the hotel/venue/housing decision, **before** we spend money trying to find their email or phone?"
);
lines.push("");
lines.push("## Before vs After");
lines.push("");
lines.push("| Metric | Before (pack primary) | After (candidate discovery) |");
lines.push("|---|---:|---:|");
lines.push(`| Total opportunities | ${before.totalOpportunities} | ${after.totalOpportunities} |`);
lines.push(
  `| ≥1 probable named/entity contact | ${before.withAtLeastOneProbableNamedContact}* | ${after.withAtLeastOneProbableNamedContact} |`
);
lines.push(`| ≥2 candidates | — | ${after.withAtLeastTwoCandidates} |`);
lines.push(`| High-confidence primary | — | ${after.highConfidencePrimary} |`);
lines.push(`| Unresolved (no probable named) | — | ${after.unresolved} |`);
lines.push(`| Primary with email | ${before.primaryWithEmail} | ${after.primaryWithEmail} |`);
lines.push(`| Primary with phone | ${before.primaryWithPhone} | ${after.primaryWithPhone} |`);
lines.push(`| Primary with both | ${before.primaryWithBoth} | ${after.primaryWithBoth} |`);
lines.push(`| Backup coverage | ${before.backupCoverage} | ${after.backupCoverage} |`);
lines.push("");
lines.push(
  "\\*Before counted pack labels that were not true people (org desks, UNKNOWN, combined names). After applies identity gates — lower count is **higher honesty**, not loss of known people."
);
lines.push("## Founder table (all 29)");
lines.push("");
lines.push(
  "| Opportunity | Primary Candidate | Role | Score | Confidence | Email | Phone | Backup | Why This Person |"
);
lines.push("|---|---|---|---:|---|---|---|---|---|");

for (const row of result.opportunities) {
  const p = row.primaryCandidate;
  const b = row.backupCandidates?.[0];
  const backupCell = b
    ? `${b.name || "—"} (${b.candidateScore ?? "—"})`
    : row.resolution === NO_PROBABLE
      ? "—"
      : "—";
  lines.push(
    `| ${esc(row.title)} | ${esc(p?.name || NO_PROBABLE)} | ${esc(p?.gdiContactRole || p?.role || "—")} | ${p?.candidateScore ?? "—"} | ${esc(p?.candidateConfidence || "UNRESOLVED")} | ${esc(p?.email)} | ${esc(p?.phone)} | ${esc(backupCell)} | ${esc((p?.whyThisPerson || "").slice(0, 160))} |`
  );
}

lines.push("");
lines.push("## Hardest opportunity types (unresolved or weak)");
lines.push("");
const hard = result.opportunities.filter(
  (r) =>
    r.resolution === NO_PROBABLE ||
    (r.primaryCandidate && ["LOW", "UNRESOLVED"].includes(r.primaryCandidate.candidateConfidence))
);
for (const r of hard) {
  lines.push(
    `- **${r.title}** (${r.segment || r.opportunityType}) — ${r.resolution}${r.primaryCandidate ? ` · primary=${r.primaryCandidate.name} score=${r.primaryCandidate.candidateScore}` : ""}`
  );
}
lines.push("");
lines.push("## Surfe next-pass eligibility (NOT run)");
lines.push("");
lines.push(
  "Paid enrichment remains OFF. Candidates below are **named people** with HIGH/MEDIUM confidence and a reachability gap (missing direct email and/or phone). Housing entities with org inboxes are excluded from Surfe WHO (Dealality already owns identity)."
);
lines.push("");

const surfeQueue = [];
const surfeSeen = new Set();
for (const r of result.opportunities) {
  const cands = [r.primaryCandidate, ...(r.backupCandidates || [])].filter(Boolean);
  for (const c of cands) {
    if (!c.name) continue;
    // Surfe only for true people — never org desks or housing entities
    if (!/^[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3}$/.test(String(c.name).trim())) continue;
    if (/\b(services|desk|staff|office|meetings|housing|association|tournaments)\b/i.test(c.name)) {
      continue;
    }
    if (!["HIGH", "MEDIUM"].includes(c.candidateConfidence)) continue;
    const gap = c.reachabilityGap || (!c.email ? "MISSING_EMAIL" : !c.phone ? "MISSING_PHONE" : null);
    if (!gap || gap === "REACHABLE") {
      // still include if email is role/generic only for phone hunt
      if (c.email && c.phone) continue;
    }
    const dedupe = `${r.opportunityId}::${String(c.name).toLowerCase()}`;
    if (surfeSeen.has(dedupe)) continue;
    surfeSeen.add(dedupe);
    surfeQueue.push({
      opportunityId: r.opportunityId,
      title: r.title,
      name: c.name,
      role: c.role,
      organization: c.organization,
      confidence: c.candidateConfidence,
      score: c.candidateScore,
      email: c.email,
      phone: c.phone,
      gap: gap || (c.email && !c.phone ? "MISSING_PHONE" : "MISSING_EMAIL"),
    });
  }
}

lines.push(`**Estimated paid enrichment calls (bounded next pass):** ${surfeQueue.length}`);
lines.push("");
for (const s of surfeQueue.slice(0, 40)) {
  lines.push(
    `- ${s.name} · ${s.title} · ${s.confidence} · gap=${s.gap} · existing email=${s.email || "—"} phone=${s.phone || "—"}`
  );
}
lines.push("");
lines.push("## Architecture notes");
lines.push("");
lines.push("- Candidate discovery (WHO) is separate from Contact Grade / Surfe reachability (HOW).");
lines.push("- Sources: pack contacts + official L1 enrichments + evidence-backed seeds (empty seeds ⇒ unresolved, never fabricated).");
lines.push("- Role priority ladders prefer housing/meetings owners over executives for overflow and association events.");
lines.push("- Regression tests: `npm run test:gdi-contact-candidate-discovery`");
lines.push("");
lines.push("## Final verdict");
lines.push("");
const improved =
  after.withAtLeastOneProbableNamedContact - before.withAtLeastOneProbableNamedContact;
lines.push(
  `- Named/entity primary coverage: **${before.withAtLeastOneProbableNamedContact} → ${after.withAtLeastOneProbableNamedContact}** (${improved >= 0 ? "+" : ""}${improved}).`
);
lines.push(
  `- Unresolved remain hardest: corporate watches, advocacy summits without published planners, future-cycle medical meetings with generic inboxes only.`
);
lines.push(
  `- Candidate scoring is reliable enough for **ranking among evidenced candidates**; it does not invent people — unresolved stay unresolved.`
);
lines.push(
  `- Next step: bounded Surfe pass on the ${surfeQueue.length} high/medium named candidates with reachability gaps (paid still gated).`
);
lines.push("");

fs.mkdirSync(path.dirname(outJson), { recursive: true });
fs.mkdirSync(path.dirname(outMd), { recursive: true });

const payload = {
  ...result,
  before,
  after,
  surfeNextPassQueue: surfeQueue,
  hotelId: HOTEL_ID,
  applyMode: apply,
};

fs.writeFileSync(outJson, JSON.stringify(payload, null, 2));
fs.writeFileSync(outMd, lines.join("\n"));

console.log(
  JSON.stringify(
    {
      passId: CONTACT_CANDIDATE_DISCOVERY_PASS_ID,
      outJson,
      outMd,
      before,
      after,
      surfeQueue: surfeQueue.length,
      apply,
    },
    null,
    2
  )
);

if (apply) {
  console.warn(
    "NOTE: --apply does not rewrite opportunities.json in v1 (discovery is report-only until founder merges candidates into contact-resolution-pass)."
  );
}
