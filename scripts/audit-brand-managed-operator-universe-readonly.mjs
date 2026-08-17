#!/usr/bin/env node
/**
 * READ-ONLY — Brand Explorer → Brand-Managed Operator universe discovery.
 * No Airtable writes.
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const headers = { Authorization: `Bearer ${apiKey}` };

async function fetchJson(url) {
  const res = await fetch(url, { headers });
  const data = await res.json();
  if (!res.ok) throw new Error(`${res.status} ${JSON.stringify(data)}`);
  return data;
}

async function fetchAll(table, filterByFormula) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    if (filterByFormula) params.set("filterByFormula", filterByFormula);
    const data = await fetchJson(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`
    );
    out.push(...(data.records || []));
    offset = data.offset;
  } while (offset);
  return out;
}

const meta = await fetchJson(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`);
const bb = meta.tables.find((t) => t.name === "Brand Setup - Brand Basics");
const parentish = (bb?.fields || [])
  .filter((f) =>
    /parent|company|family|status|slug|scale|management|franchise|agreement|operator|affiliation/i.test(
      f.name
    )
  )
  .map((f) => ({
    name: f.name,
    type: f.type,
    choices: f.options?.choices?.map((c) => c.name) || null,
  }));

const brands = await fetchAll(
  "Brand Setup - Brand Basics",
  "OR({Brand Status}='Active', {Brand Status}='Live')"
);

const masters = await fetchAll("Operator Setup - Master");
const managedMasters = masters
  .filter((m) => /\(Managed\)/i.test(m.fields?.company_name || ""))
  .map((m) => ({
    id: m.id,
    name: m.fields.company_name,
    status: m.fields.submission_status,
  }));

// Detect parent field from first records
const sampleKeys = new Set();
for (const b of brands.slice(0, 20)) {
  for (const k of Object.keys(b.fields || {})) sampleKeys.add(k);
}

function pickParent(f) {
  for (const k of [
    "Parent Company",
    "parent_company",
    "Parent",
    "Brand Family",
    "Company",
    "Parent Brand Company",
  ]) {
    if (f[k] != null && f[k] !== "") {
      const v = f[k];
      if (Array.isArray(v)) {
        if (typeof v[0] === "string") return v.join(" | ");
        return `[link:${v.length}]`;
      }
      return String(v);
    }
  }
  return "(no parent field populated)";
}

function pickName(f) {
  return f["Brand Name"] || f.Name || f.brand_name || f["Brand"] || "(unnamed)";
}

const parents = new Map();
for (const b of brands) {
  const f = b.fields || {};
  const parent = pickParent(f);
  if (!parents.has(parent)) parents.set(parent, []);
  parents.get(parent).push({
    id: b.id,
    name: pickName(f),
    status: f["Brand Status"],
    scale: f["Chain Scale"] || f.chainScale || f["Hotel Chain Scale"] || null,
    fieldsPresent: Object.keys(f),
  });
}

const out = {
  generatedAt: new Date().toISOString(),
  mode: "read-only",
  baseId,
  brandBasicsParentishFields: parentish,
  sampleFieldKeys: [...sampleKeys].sort(),
  activeLiveBrandCount: brands.length,
  parentCompanyCount: parents.size,
  parents: [...parents.entries()]
    .map(([parent, list]) => ({
      parent,
      brandCount: list.length,
      brands: list.map((x) => x.name),
      scales: [...new Set(list.map((x) => x.scale).filter(Boolean))],
    }))
    .sort((a, b) => b.brandCount - a.brandCount || a.parent.localeCompare(b.parent)),
  existingManagedOperatorMasters: managedMasters,
  shortlistCandidateTypeChoices:
    meta.tables
      .find((t) => t.name === "Operator Fit - Shortlist")
      ?.fields?.find((f) => f.name === "Candidate Type")
      ?.options?.choices?.map((c) => c.name) || null,
};

mkdirSync(join(root, "reports"), { recursive: true });
writeFileSync(
  join(root, "reports/operator-explorer-brand-managed-universe-discovery.json"),
  JSON.stringify(out, null, 2)
);
console.log(
  JSON.stringify(
    {
      activeLive: out.activeLiveBrandCount,
      parents: out.parentCompanyCount,
      managedMasters: managedMasters.length,
      parentNames: out.parents.map((p) => `${p.parent} (${p.brandCount})`),
      candidateTypeChoices: out.shortlistCandidateTypeChoices,
      parentishFieldNames: parentish.map((p) => p.name),
    },
    null,
    2
  )
);
