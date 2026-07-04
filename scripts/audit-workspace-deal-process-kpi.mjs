/**
 * End-to-end KPI audit: one deal progressing owner → brand through each BDR step.
 * Run: node scripts/audit-workspace-deal-process-kpi.mjs
 */
import { writeFileSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  auditWorkspaceKpiMirror,
  computeWorkspaceKpiSnapshot,
  enrichWorkspaceRow,
} from "../lib/deal-workspace-pipeline.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const now = Date.now();
const recent = new Date(now - 3 * 86400000).toISOString();
const stale = new Date(now - 20 * 86400000).toISOString();

function row(overrides) {
  return {
    id: "recAudit",
    dealId: "recDeal",
    brandName: "Audit Brand",
    requestSentAt: recent,
    lastUpdated: recent,
    lastActivity: recent,
    ndaStatus: "",
    dealRoomAccess: "",
    proposalStatus: "",
    nextFollowupDate: null,
    ...overrides,
  };
}

/** Process steps in typical owner-led outreach order */
const JOURNEY = [
  {
    step: "1 — Owner sends outreach",
    ownerView: "Owner submits deal / sends brand request",
    brandView: "New inbound appears",
    make: () => row({ status: "New" }),
    expect: {
      bucket: "new",
      ownerAwaitingBrand: 1,
      ownerAction: 0,
      brandNeedsAction: 1,
      brandAwaitingOwner: 0,
      pipelineNew: 1,
      outreach7d: 1,
    },
  },
  {
    step: "2 — Brand opens (viewed)",
    ownerView: "Awaiting brand",
    brandView: "Brand action — mark decision",
    make: () => row({ status: "Brand Viewed" }),
    expect: {
      bucket: "active-review",
      ownerAwaitingBrand: 1,
      ownerAction: 0,
      brandNeedsAction: 1,
      brandAwaitingOwner: 0,
      pipelineReview: 1,
    },
  },
  {
    step: "3 — Brand requests more info",
    ownerView: "Owner action — provide info",
    brandView: "Awaiting owner",
    make: () => row({ status: "More Info Requested" }),
    expect: {
      bucket: "awaiting-info",
      ownerAwaitingBrand: 0,
      ownerAction: 1,
      brandNeedsAction: 0,
      brandAwaitingOwner: 1,
      note: "Flow only — not in pipeline columns",
    },
  },
  {
    step: "4 — Brand accepts",
    ownerView: "Owner action — send term link",
    brandView: "Awaiting owner",
    make: () => row({ status: "Accepted" }),
    expect: {
      bucket: "awaiting-info",
      ownerAwaitingBrand: 0,
      ownerAction: 1,
      brandNeedsAction: 0,
      brandAwaitingOwner: 1,
    },
  },
  {
    step: "5 — Brand sends NDA",
    ownerView: "Awaiting brand (sign NDA)",
    brandView: "Brand action — NDA out",
    make: () =>
      row({
        status: "Accepted",
        ndaStatus: "Sent",
      }),
    expect: {
      bucket: "nda-room",
      ownerAwaitingBrand: 1,
      ownerAction: 0,
      brandNeedsAction: 1,
      brandAwaitingOwner: 0,
      pipelineNegotiation: 1,
    },
  },
  {
    step: "6 — NDA signed, grant deal room",
    ownerView: "Awaiting brand (open room)",
    brandView: "Brand action — grant access",
    make: () =>
      row({
        status: "Accepted",
        ndaStatus: "Signed - Owner Confirmed",
        dealRoomAccess: "",
      }),
    expect: {
      bucket: "nda-room",
      ownerAwaitingBrand: 1,
      ownerAction: 0,
      brandNeedsAction: 1,
      brandAwaitingOwner: 0,
    },
  },
  {
    step: "7 — Deal room active",
    ownerView: "Awaiting brand (deal room)",
    brandView: "Brand action — deal room",
    make: () =>
      row({
        status: "Deal Room Active",
        ndaStatus: "Signed - Owner Confirmed",
        dealRoomAccess: "Granted",
      }),
    expect: {
      bucket: "nda-room",
      ownerAwaitingBrand: 1,
      ownerAction: 0,
      brandNeedsAction: 1,
      brandAwaitingOwner: 0,
      pipelineNegotiation: 1,
    },
  },
  {
    step: "8 — Pre-LOI / terms draft",
    ownerView: "Owner action — compare terms",
    brandView: "Brand action — prepare terms",
    make: () =>
      row({
        status: "Pre-LOI",
        ndaStatus: "Signed - Owner Confirmed",
        dealRoomAccess: "Granted",
        proposalStatus: "Draft",
      }),
    expect: {
      bucket: "terms-proposal",
      ownerAwaitingBrand: 1,
      ownerAction: 1,
      brandNeedsAction: 1,
      brandAwaitingOwner: 1,
      pipelineTerms: 1,
      note: "Mirror holds: owner action = brand awaiting owner; owner awaiting brand = brand action",
    },
  },
  {
    step: "9 — Proposal submitted",
    ownerView: "Owner action — compare terms",
    brandView: "Brand action — follow up owner",
    make: () =>
      row({
        status: "Pre-LOI",
        proposalStatus: "Submitted",
        ndaStatus: "Signed - Owner Confirmed",
        dealRoomAccess: "Granted",
      }),
    expect: {
      bucket: "terms-proposal",
      ownerAwaitingBrand: 1,
      ownerAction: 1,
      brandNeedsAction: 1,
      brandAwaitingOwner: 1,
      pipelineTerms: 1,
    },
  },
  {
    step: "10 — Finalist",
    ownerView: "Owner action — open deal room",
    brandView: "Brand internal review",
    make: () => row({ status: "Finalist" }),
    expect: {
      bucket: "advanced",
      ownerAwaitingBrand: 1,
      ownerAction: 1,
      brandNeedsAction: 1,
      brandAwaitingOwner: 1,
      pipelineNegotiation: 1,
    },
  },
  {
    step: "11 — Stalled (overdue follow-up)",
    ownerView: "Stuck / at risk",
    brandView: "Stuck / at risk",
    make: () =>
      row({
        status: "Pre-LOI",
        proposalStatus: "Draft",
        nextFollowupDate: "2020-06-01",
        lastUpdated: stale,
      }),
    expect: {
      atRisk: 1,
      stalledInTermsMeta: true,
    },
  },
  {
    step: "12 — Declined / passed",
    ownerView: "Passed",
    brandView: "Passed",
    make: () => row({ status: "Declined" }),
    expect: {
      bucket: "archived",
      ownerAwaitingBrand: 0,
      ownerAction: 0,
      brandNeedsAction: 0,
      brandAwaitingOwner: 0,
      pipelinePassed: 1,
    },
  },
];

function snapCounts(rows, persona) {
  const s = computeWorkspaceKpiSnapshot(rows, persona);
  const e = enrichWorkspaceRow(rows[0] || {});
  return {
    persona,
    needsAction: s.needsAction,
    awaitingCounterparty: s.awaitingCounterparty,
    atRisk: s.atRisk,
    newRolling7d: s.newRolling7d,
    inReview: s.inReview,
    pipeline: s.pipeline,
    bucket: e.workspaceBucket,
    brandNextAction: e.brandNextAction,
    ownerNextAction: e.ownerNextAction,
    mirrorOk: s.mirror?.ties?.ownerAwaitingBrandEqualsBrandNeedsAction &&
      s.mirror?.ties?.ownerNeedsActionEqualsBrandAwaitingOwner,
  };
}

function checkStep(def) {
  const rows = [def.make()];
  const owner = snapCounts(rows, "owner");
  const brand = snapCounts(rows, "brand");
  const audit = auditWorkspaceKpiMirror(rows);
  const exp = def.expect;
  const issues = [];

  if (exp.bucket && owner.bucket !== exp.bucket) {
    issues.push(`bucket: got ${owner.bucket}, expected ${exp.bucket}`);
  }
  if (exp.ownerAwaitingBrand != null && owner.awaitingCounterparty !== exp.ownerAwaitingBrand) {
    issues.push(`owner Awaiting brand: ${owner.awaitingCounterparty} ≠ ${exp.ownerAwaitingBrand}`);
  }
  if (exp.ownerAction != null && owner.needsAction !== exp.ownerAction) {
    issues.push(`owner Owner action: ${owner.needsAction} ≠ ${exp.ownerAction}`);
  }
  if (exp.brandNeedsAction != null && brand.needsAction !== exp.brandNeedsAction) {
    issues.push(`brand Brand action: ${brand.needsAction} ≠ ${exp.brandNeedsAction}`);
  }
  if (exp.brandAwaitingOwner != null && brand.awaitingCounterparty !== exp.brandAwaitingOwner) {
    issues.push(`brand Awaiting owner: ${brand.awaitingCounterparty} ≠ ${exp.brandAwaitingOwner}`);
  }
  if (exp.atRisk != null && owner.atRisk !== exp.atRisk) {
    issues.push(`stuck/at risk: owner ${owner.atRisk} brand ${brand.atRisk}, expected ${exp.atRisk}`);
  }
  if (exp.outreach7d != null && owner.newRolling7d !== exp.outreach7d) {
    issues.push(`outreach 7d: ${owner.newRolling7d} ≠ ${exp.outreach7d}`);
  }
  if (exp.pipelineNew != null && owner.pipeline.newInbound !== exp.pipelineNew) {
    issues.push(`pipeline new: ${owner.pipeline.newInbound}`);
  }
  if (exp.pipelineReview != null && owner.pipeline.underReview !== exp.pipelineReview) {
    issues.push(`pipeline review: ${owner.pipeline.underReview}`);
  }
  if (exp.pipelineTerms != null && owner.pipeline.bidSubmitted !== exp.pipelineTerms) {
    issues.push(`pipeline terms: ${owner.pipeline.bidSubmitted}`);
  }
  if (exp.pipelineNegotiation != null && owner.pipeline.negotiation !== exp.pipelineNegotiation) {
    issues.push(`pipeline negotiation: ${owner.pipeline.negotiation}`);
  }
  if (exp.pipelinePassed != null && owner.pipeline.passed !== exp.pipelinePassed) {
    issues.push(`pipeline passed: ${owner.pipeline.passed}`);
  }
  if (!audit.ok) {
    issues.push(`mirror/partition: ${audit.violations.map((v) => v.code).join(", ")}`);
  }
  if (exp.ownerAwaitingBrand != null && exp.brandNeedsAction != null) {
    if (owner.awaitingCounterparty !== brand.needsAction) {
      issues.push(`MIRROR FAIL: owner awaiting brand (${owner.awaitingCounterparty}) ≠ brand action (${brand.needsAction})`);
    }
  }
  if (exp.ownerAction != null && exp.brandAwaitingOwner != null) {
    if (owner.needsAction !== brand.awaitingCounterparty) {
      issues.push(`MIRROR FAIL: owner action (${owner.needsAction}) ≠ brand awaiting owner (${brand.awaitingCounterparty})`);
    }
  }

  return {
    step: def.step,
    ownerView: def.ownerView,
    brandView: def.brandView,
    owner,
    brand,
    note: def.expect.note || def.expect.warnDualBall || "",
    ok: issues.length === 0,
    issues,
  };
}

const results = JOURNEY.map(checkStep);
const failed = results.filter((r) => !r.ok);

let md = `# Workspace deal-process KPI audit\n\nGenerated: ${new Date().toISOString()}\n\n`;
md += `Single-deal journey: owner outreach → brand response → NDA → terms → close.\n\n`;
md += `## Summary\n\n`;
md += `- Steps tested: ${results.length}\n`;
md += `- Passed: ${results.length - failed.length}\n`;
md += `- Failed: ${failed.length}\n\n`;

md += `## Mirror rules (must hold at every step)\n\n`;
md += `| Owner KPI | Brand KPI |\n|-----------|----------|\n`;
md += `| Awaiting brand | Brand action |\n`;
md += `| Owner action | Awaiting owner |\n`;
md += `| Stuck / at risk | Stuck / at risk |\n`;
md += `| Outreach/Requests sent (7d) | Same (request-sent date) |\n\n`;

md += `## Step-by-step\n\n`;

for (const r of results) {
  const icon = r.ok ? "PASS" : "FAIL";
  md += `### ${r.step} — ${icon}\n\n`;
  md += `- **Owner sees:** ${r.ownerView}\n`;
  md += `- **Brand sees:** ${r.brandView}\n`;
  md += `- Bucket: \`${r.owner.bucket}\` · Brand next: ${r.brand.brandNextAction} · Owner next: ${r.owner.ownerNextAction}\n\n`;
  md += `| KPI | Owner | Brand |\n|-----|-------|-------|\n`;
  md += `| Needs action | ${r.owner.needsAction} | ${r.brand.needsAction} |\n`;
  md += `| Awaiting counterparty | ${r.owner.awaitingCounterparty} | ${r.brand.awaitingCounterparty} |\n`;
  md += `| Stuck / at risk | ${r.owner.atRisk} | ${r.brand.atRisk} |\n`;
  md += `| Pipeline (new / review / terms / neg / closed / passed) | ${r.owner.pipeline.newInbound} / ${r.owner.pipeline.underReview} / ${r.owner.pipeline.bidSubmitted} / ${r.owner.pipeline.negotiation} / ${r.owner.pipeline.closed} / ${r.owner.pipeline.passed} | same |\n\n`;
  if (r.note) md += `Note: ${r.note}\n\n`;
  if (r.issues.length) {
    md += `Issues:\n`;
    for (const i of r.issues) md += `- ${i}\n`;
    md += `\n`;
  }
}

md += `## Findings for product / scale\n\n`;
md += `1. **Scope:** Owner totals include every brand on the owner’s deals; brand totals are per signed-in brand. Compare the same BDR \`id\` when validating.\n`;
md += `2. **Pipeline vs flow:** \`awaiting-info\` rows count in **Owner action / Awaiting owner** only, not in the six pipeline stage tiles.\n`;
md += `3. **Dual flow counts:** At some steps (e.g. Pre-LOI, Finalist) both **Owner action** and **Awaiting brand** can be 1 for the same row because both sides have a defined next step. That is accurate for parallel tasks, not a mirror bug.\n`;
md += `4. **Stuck:** Requires \`nextFollowupDate\` and/or \`lastUpdated\` / \`lastActivity\` on the row (full BDR API shape).\n`;
md += `5. **Regression test:** \`node scripts/test-workspace-kpi-mirror.mjs\` + this audit.\n`;

const outPath = path.join(__dirname, "..", "reports", "workspace-deal-process-kpi-audit.md");
writeFileSync(outPath, md, "utf8");

console.log(`Wrote ${outPath}`);
console.log(`Passed ${results.length - failed.length}/${results.length}`);
if (failed.length) {
  console.error("Failures:");
  for (const f of failed) {
    console.error(f.step, f.issues);
  }
  process.exit(1);
}
