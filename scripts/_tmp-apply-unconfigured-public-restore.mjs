#!/usr/bin/env node
/**
 * Explicit public restore for the 3 unconfigured Active/Live brands.
 * Writes Basics release fields only (no CV / Source / Registry / Brand Status / content).
 */
import "dotenv/config";
import fs from "fs";
import {
  writeIntentionalPublicRestoreSlugs,
  readIntentionalPublicRestoreSlugs,
} from "../lib/partner-intelligence/brand-explorer-public-restore-registry.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { resolveBrandExplorerDisplayState } from "../lib/partner-intelligence/brand-explorer-display-state.js";
import { evaluateExternalOwnerReadinessRule } from "../lib/partner-intelligence/brand-explorer-external-owner-readiness-rules.js";
import { evaluateBrandImageRoleMatch } from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { evaluateImageUniqueness } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const basicsTable = "Brand Setup - Brand Basics";

const targets = [
  { slug: "bw-premier-collection", recordId: "recwXZ5gVZ8ZH8ekA", name: "BW Premier Collection" },
  { slug: "bw-signature-collection", recordId: "recdeh1NsP4gjrv80", name: "BW Signature Collection" },
  {
    slug: "preferred-hotels-and-resorts",
    recordId: "recwl5JOYxlChuCAr",
    name: "Preferred Hotels & Resorts",
  },
];

function mockRes() {
  return {
    headers: {},
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

async function patchBasics(recordId, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(basicsTable)}/${recordId}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || res.status);
  return json;
}

const gateResults = [];
for (const t of targets) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: t.recordId }, headers: {} }, res);
  const brand = { ...res.payload.brand, slug: t.slug };
  const blocks = brand.brandExplorer?.blocks || [];
  const ext = evaluateExternalOwnerReadinessRule(blocks);
  const role = evaluateBrandImageRoleMatch({ presentationRows: blocks, brandSlug: t.slug });
  const uniq = evaluateImageUniqueness({
    brand,
    presentationRows: blocks,
    brandSlug: t.slug,
  });
  const galleryCount = blocks.filter(
    (b) => /^materials\.gallery\.\d+$/.test(b.slotKey || "") && b.imageUrl
  ).length;
  const openingsCount = blocks.filter(
    (b) => b.slotKey === "footprint.openings" && b.imageUrl
  ).length;
  const scenariosOk = [1, 2, 3].every((i) =>
    blocks.some((b) => b.slotKey === `overview.scenario.${i}`)
  );
  const contentGatesPass =
    blocks.length >= 70 &&
    galleryCount >= 6 &&
    openingsCount >= 3 &&
    scenariosOk &&
    ext.pass === true &&
    role.pass === true &&
    uniq.pass === true;

  gateResults.push({
    ...t,
    contentGatesPass,
    galleryCount,
    openingsCount,
    rows: blocks.length,
    ext: ext.pass,
    role: role.pass,
    uniq: uniq.pass,
  });
  console.log(
    `${t.slug}: contentGates=${contentGatesPass} rows=${blocks.length} gallery=${galleryCount} openings=${openingsCount} ext=${ext.pass} role=${role.pass} uniq=${uniq.pass}`
  );
}

const ready = gateResults.filter((g) => g.contentGatesPass);
if (ready.length !== 3) {
  console.error("Refuse restore: content gates incomplete", gateResults);
  process.exit(1);
}

const today = new Date().toISOString().slice(0, 10);
const fields = {
  "Active Profile Approved": true,
  "Ready for Active Profile": true,
  "Active Profile Approved Date": today,
  "Founder Visual Review Pass": true,
};

for (const t of ready) {
  await patchBasics(t.recordId, fields);
  console.log("release fields written", t.slug, Object.keys(fields));
  await new Promise((r) => setTimeout(r, 250));
}

const intentional = readIntentionalPublicRestoreSlugs();
const next = [...new Set([...intentional, ...ready.map((r) => r.slug)])];
writeIntentionalPublicRestoreSlugs(next);
console.log("intentional restore registry updated", next.filter((s) => ready.some((r) => r.slug === s)));

for (const t of ready) {
  const packet = [
    `# Founder Review — ${t.name}`,
    ``,
    `Recommendation: **approve_for_active_release**`,
    ``,
    `| Gate | Result |`,
    `| --- | --- |`,
    `| Presentation rows | ${t.rows} |`,
    `| Gallery images | ${t.galleryCount}/6 |`,
    `| Property openings with images | ${t.openingsCount}/3+ |`,
    `| External owner readiness | ${t.ext} |`,
    `| Image role-match | ${t.role} |`,
    `| Image uniqueness | ${t.uniq} |`,
    `| Content gates | ${t.contentGatesPass} |`,
    ``,
    `Public restore applied: Basics release fields + intentional restore registry.`,
    `Company Validated / Source Library / Registry / Brand Status untouched.`,
    ``,
  ].join("\n");
  fs.writeFileSync(`reports/brand-explorer-founder-review-${t.slug}.md`, `${packet}\n`);
}

// Post-check
for (const t of ready) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: t.recordId }, headers: {} }, res);
  const brand = { ...res.payload.brand, slug: t.slug };
  const meta = resolveBrandExplorerDisplayState(brand);
  console.log(
    JSON.stringify({
      slug: t.slug,
      publicFull: brand.shouldRenderFullProfile,
      display: brand.brandExplorerDisplayState || meta.brandExplorerDisplayState,
      resolvedPublic: meta.shouldRenderFullProfile,
      blockers: meta.blockers,
    })
  );
}
