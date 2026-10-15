#!/usr/bin/env node
/**
 * Focused local GDI acceptance for Mayflower parity cases.
 * Requires a running local server with Airtable persistence + share DEV secret.
 *
 * Usage:
 *   GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 node scripts/gdi-local-acceptance-parity-v1.mjs
 *   node scripts/gdi-local-acceptance-parity-v1.mjs --base=http://127.0.0.1:8080
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { sanitizeOpportunityForShare } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const baseArg = process.argv.find((a) => a.startsWith("--base="));
const BASE = (baseArg ? baseArg.slice("--base=".length) : "http://127.0.0.1:8080").replace(
  /\/$/,
  ""
);
const MAY = "rec0eTEnCqlsGJJt9";
const SD = "recnPB3Q02NO8Ma1o";

let failed = 0;
function pass(name, detail) {
  console.log("PASS", name, detail || "");
}
function fail(name, detail) {
  failed += 1;
  console.error("FAIL", name, detail || "");
}

function issueShare(hotelId) {
  const r = spawnSync(
    process.execPath,
    ["scripts/gdi-issue-share.mjs", `--hotelId=${hotelId}`, `--label=acceptance-${hotelId.slice(0, 8)}`],
    {
      cwd: ROOT,
      encoding: "utf8",
      env: {
        ...process.env,
        GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET: "1",
      },
    }
  );
  const out = `${r.stdout || ""}\n${r.stderr || ""}`;
  const m = out.match(/share=(gdishare\.v1\.[A-Za-z0-9._-]+)/);
  if (!m) {
    throw new Error(`share issue failed for ${hotelId}: ${out.slice(0, 400)}`);
  }
  return m[1];
}

async function getJson(url) {
  const res = await fetch(url);
  const body = await res.json();
  return { status: res.status, body };
}

// Unit: company-contact fallbacks survive share sanitize → list DTO
{
  const cases = [
    {
      name: "ACEC",
      contactEmail: "info@acecmw.org",
      contactPhone: "202-644-8260",
      contactRole: "chapter staff / events",
      commercialContactPath: "NAMED_DIRECT",
      expectOrg: "info@acecmw.org",
    },
    {
      name: "ASID",
      contactEmail: "administrator@dcmetro.asid.org",
      contactRole: "chapter administrator / events",
      commercialContactPath: "NAMED_DIRECT",
      expectOrg: "administrator@dcmetro.asid.org",
    },
    {
      name: "Aero",
      contactEmail: "stephanie@aeroclub.org",
      contactPhone: "703-386-4330",
      contactRole: "Executive Director",
      commercialContactPath: "NAMED_DIRECT",
      expectPerson: "Stephanie",
    },
  ];
  for (const c of cases) {
    const dto = toOpportunityListDto(sanitizeOpportunityForShare(c));
    const lines = JSON.stringify(dto.contactDisplayLines || []);
    if (c.expectOrg && dto.organizationContactEmail === c.expectOrg && lines.includes(c.expectOrg)) {
      pass(`unit_fallback_${c.name}`, c.expectOrg);
    } else if (c.expectPerson && dto.primaryContact?.name === c.expectPerson) {
      pass(`unit_fallback_${c.name}`, c.expectPerson);
    } else {
      fail(`unit_fallback_${c.name}`, lines);
    }
  }
}

// Live share acceptance
process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET =
  process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET || "1";

const html = await fetch(`${BASE}/group-demand-intelligence-share.html`);
const htmlText = await html.text();
const gdiBust = (htmlText.match(/group-demand-intelligence\.(?:css|js)\?v=([A-Za-z0-9._-]+)/) || [])[1];
if (
  html.status === 200 &&
  gdiBust &&
  /gdi-tile-contact-toolbar-v1|gdi-toolbar-sort-height-v1|gdi-parity-v1|gdi-closeout-v2/.test(
    gdiBust
  )
) {
  pass("local_asset_bust", gdiBust);
} else {
  fail("local_asset_bust", `status=${html.status} bust=${gdiBust || "missing"}`);
}

const mayShare = issueShare(MAY);
const sdShare = issueShare(SD);

const mayOpps = await getJson(
  `${BASE}/api/group-demand-intelligence/share/hotels/${MAY}/opportunities?share=${encodeURIComponent(mayShare)}`
);
const mayResolve = await getJson(
  `${BASE}/api/group-demand-intelligence/share/resolve?share=${encodeURIComponent(mayShare)}`
);
const sdOpps = await getJson(
  `${BASE}/api/group-demand-intelligence/share/hotels/${SD}/opportunities?share=${encodeURIComponent(sdShare)}`
);

if (mayOpps.body?.ok && Number(mayOpps.body.count) > 0) pass("may_opps", mayOpps.body.count);
else fail("may_opps", JSON.stringify(mayOpps.body?.error || mayOpps.status));

if (sdOpps.body?.ok && Number(sdOpps.body.count) > 0) pass("sd_opps", sdOpps.body.count);
else fail("sd_opps", JSON.stringify(sdOpps.body?.error || sdOpps.status));

const ops = mayOpps.body?.opportunities || [];
for (const needle of ["ACEC", "ASID", "Aero Club"]) {
  const o = ops.find((x) => String(x.organizationName || x.title || "").includes(needle));
  if (!o) {
    fail(`live_${needle}`, "missing");
    continue;
  }
  const usable =
    (o.contactDisplayLines || []).some((l) =>
      ["org_email", "company_phone", "person_email", "name", "url"].includes(l.kind)
    ) ||
    o.organizationContactEmail ||
    o.companyPhone ||
    o.primaryContact?.email;
  if (usable) pass(`live_${needle}`, o.organizationContactEmail || o.primaryContact?.name || "route");
  else fail(`live_${needle}`, JSON.stringify(o.contactDisplayLines));
}

const summary = mayResolve.body?.summary || {};
const last = String(summary.lastResearchAt || "");
const runLabel = String(summary.runStatusLabel || summary.runStatus || "");
if (last.startsWith("2026-10-29")) {
  fail("history_not_oct29_false_completed", last);
} else if (last && runLabel) {
  pass("history_date_status", `${last} / ${runLabel}`);
} else if (
  /unavailable|partial|failed/i.test(runLabel) ||
  summary.runStatus === "STATUS_UNAVAILABLE" ||
  summary.lastPublishedUpdateAt
) {
  // Presentation task preserves unavailable/partial/failed history states.
  pass(
    "history_date_status",
    `state=${runLabel || summary.runStatus || "n/a"}; published=${summary.lastPublishedUpdateAt || "n/a"}`
  );
} else {
  fail("history_date_status", JSON.stringify(summary));
}

const cats = new Set(ops.map((o) => o.opportunityTypeFilterCategory).filter(Boolean));
if (cats.size >= 3) pass("opportunity_type_filter_categories", [...cats].join(","));
else fail("opportunity_type_filter_categories", [...cats].join(","));

// Surface smoke: AHIMA drawer fields present on list DTO
const ahima = ops.find((x) => String(x.organizationName || x.title || "").includes("AHIMA"));
if (ahima?.primaryContact?.email) pass("ahima_named_contact", ahima.primaryContact.name);
else fail("ahima_named_contact", ahima?.id || "missing");

const outDir = path.join(ROOT, "reports", "gdi-local-production-parity");
fs.mkdirSync(outDir, { recursive: true });
const result = {
  ok: failed === 0,
  failed,
  checkedAt: new Date().toISOString(),
  base: BASE,
  mayCount: mayOpps.body?.count ?? null,
  sdCount: sdOpps.body?.count ?? null,
  lastResearchAt: summary.lastResearchAt || null,
  runStatusLabel: summary.runStatusLabel || null,
  typeFilterCategories: [...cats],
};
fs.writeFileSync(
  path.join(outDir, "local-acceptance-results.json"),
  JSON.stringify(result, null, 2) + "\n"
);

console.log(failed === 0 ? "\nACCEPTANCE PASS" : `\nACCEPTANCE FAIL (${failed})`);
process.exit(failed === 0 ? 0 : 1);
