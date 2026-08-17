#!/usr/bin/env node
/**
 * Normalize Strategic Intent Preferred Brands names to exact Brand Basics "Brand Name".
 * Also sync Deal Brand Cache "Preferred Brands" text when present.
 *
 * Usage:
 *   node scripts/normalize-preferred-brands-to-brand-basics.mjs --dry-run
 *   node scripts/normalize-preferred-brands-to-brand-basics.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { STRATEGIC_INTENT_TABLE } from "../api/schemas/deal-setup-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const OUT = path.join(ROOT, "reports", `preferred-brands-normalize-${APPLY ? "apply" : "dry-run"}.json`);

const SI_TABLE = STRATEGIC_INTENT_TABLE || "Strategic Intent - Operational - Key Challenges";
const BB_TABLE = "Brand Setup - Brand Basics";
const CACHE_TABLE = "Deal Brand Cache";
const PREFERRED_KEYS = ["Preferred Brands", "Preferred Brands (up to 4)"];

/** Explicit aliases when short/preferred names differ from Brand Basics Brand Name. */
const ALIASES = Object.freeze({
  moxy: "Moxy Hotels",
  "kimpton hotels & restaurants": "Kimpton Hotels",
  "kimpton hotels and restaurants": "Kimpton Hotels",
  "mgallery hotel collection": "MGallery Collection",
  "hyatt unbound collection": "Unbound Collection by Hyatt",
  "unbound collection": "Unbound Collection by Hyatt",
});

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function atFetch(baseId, apiKey, table, { offset, fields, filterByFormula } = {}) {
  const params = new URLSearchParams({ pageSize: "100" });
  if (offset) params.set("offset", offset);
  if (filterByFormula) params.set("filterByFormula", filterByFormula);
  if (fields) fields.forEach((f) => params.append("fields[]", f));
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const data = await res.json();
  if (!res.ok) throw new Error(`${table}: ${data?.error?.message || res.status}`);
  return data;
}

async function listAll(baseId, apiKey, table, opts = {}) {
  const out = [];
  let offset = null;
  do {
    const data = await atFetch(baseId, apiKey, table, { ...opts, offset });
    out.push(...(data.records || []));
    offset = data.offset || null;
    await new Promise((r) => setTimeout(r, 220));
  } while (offset);
  return out;
}

async function patchRecord(baseId, apiKey, table, recordId, fields, typecast = true) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${encodeURIComponent(recordId)}?typecast=${typecast ? "true" : "false"}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data?.error?.message || JSON.stringify(data.error || data));
  return data;
}

function buildBasicsIndex(records) {
  const byExact = new Map();
  const byNorm = new Map();
  for (const rec of records) {
    const name = String(rec.fields?.["Brand Name"] || "").trim();
    if (!name) continue;
    byExact.set(name, rec);
    const n = norm(name);
    if (!byNorm.has(n)) byNorm.set(n, []);
    byNorm.get(n).push({ id: rec.id, name, status: rec.fields?.["Brand Status"] });
  }
  return { byExact, byNorm, all: [...byExact.keys()] };
}

function resolveCanonical(preferredName, index, knownOptions) {
  const raw = String(preferredName || "").trim();
  if (!raw) return { status: "empty", from: raw, to: null };
  if (index.byExact.has(raw)) return { status: "exact", from: raw, to: raw };

  const aliasTo = ALIASES[norm(raw)];
  if (aliasTo && index.byExact.has(aliasTo)) {
    // Only use alias if Preferred Brands multi-select already has that option (or we will typecast later).
    return { status: "alias", from: raw, to: aliasTo, needsOption: !knownOptions.has(norm(aliasTo)) };
  }

  const n = norm(raw);
  const exactNorm = index.byNorm.get(n);
  if (exactNorm?.length === 1) {
    const to = exactNorm[0].name;
    return { status: "norm", from: raw, to, needsOption: !knownOptions.has(norm(to)) };
  }

  const contains = index.all.filter((bn) => {
    const bnN = norm(bn);
    return bnN !== n && (bnN.includes(n) || n.includes(bnN));
  });
  if (contains.length === 1) {
    const to = contains[0];
    return { status: "contains", from: raw, to, needsOption: !knownOptions.has(norm(to)) };
  }

  const active = contains
    .map((name) => {
      const hits = index.byNorm.get(norm(name)) || [];
      return { name, status: hits[0]?.status };
    })
    .filter((x) => x.status === "Active" || x.status === "Live");
  if (active.length === 1) {
    const to = active[0].name;
    return { status: "contains_active", from: raw, to, needsOption: !knownOptions.has(norm(to)) };
  }

  return {
    status: "unresolved",
    from: raw,
    to: null,
    candidates: contains.slice(0, 8),
  };
}

function extractPreferredNames(fields) {
  for (const key of PREFERRED_KEYS) {
    const val = fields[key];
    if (val == null) continue;
    if (Array.isArray(val)) {
      return {
        fieldKey: key,
        names: val
          .map((v) => (typeof v === "string" ? v.trim() : String(v?.name || v?.id || "").trim()))
          .filter(Boolean),
      };
    }
    if (typeof val === "string" && val.trim()) {
      return {
        fieldKey: key,
        names: val.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean),
      };
    }
  }
  return null;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  console.log(`mode=${APPLY ? "APPLY" : "dry-run"} SI=${SI_TABLE}`);

  const basics = await listAll(baseId, apiKey, BB_TABLE, {
    fields: ["Brand Name", "Brand Status"],
  });
  const index = buildBasicsIndex(basics);

  const siRecords = await listAll(baseId, apiKey, SI_TABLE, {
    fields: ["Preferred Brands"],
  });

  /** Multi-select options already in use (Airtable often blocks creating new ones via API). */
  const knownOptions = new Set();
  for (const rec of siRecords) {
    const extracted = extractPreferredNames(rec.fields || {});
    if (!extracted) continue;
    for (const n of extracted.names) knownOptions.add(norm(n));
  }
  // Seed with Brand Basics names that match known aliases targets so we try typecast for important renames
  for (const to of Object.values(ALIASES)) knownOptions.add(norm(to));

  const changes = [];
  const unresolved = [];
  const skippedNeedsOption = [];

  for (const rec of siRecords) {
    const extracted = extractPreferredNames(rec.fields || {});
    if (!extracted || !extracted.names.length) continue;

    const mapped = extracted.names.map((n) => resolveCanonical(n, index, knownOptions));
    const bad = mapped.filter((m) => m.status === "unresolved");
    if (bad.length) unresolved.push({ siId: rec.id, items: bad });

    const effective = mapped.map((m) => {
      if (!m.to || m.to === m.from) return m;
      // Prefer rename when target option already used somewhere, or explicit alias (typecast attempt).
      if (m.status === "alias") return m;
      if (knownOptions.has(norm(m.to))) return m;
      skippedNeedsOption.push({ siId: rec.id, from: m.from, to: m.to, reason: "target_not_in_preferred_options" });
      return { ...m, status: "skipped_option", to: m.from };
    });

    const needsChange = effective.some((m) => m.to && m.to !== m.from);
    if (!needsChange) continue;

    const nextNames = [];
    const seen = new Set();
    for (const m of effective) {
      const name = m.to || m.from;
      const key = norm(name);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      nextNames.push(name);
    }

    changes.push({
      siId: rec.id,
      fieldKey: extracted.fieldKey,
      from: extracted.names,
      to: nextNames,
      mapping: effective,
    });
  }

  // Deal Brand Cache preferred brands text (display/cache)
  let cacheChanges = [];
  try {
    const cacheRows = await listAll(baseId, apiKey, CACHE_TABLE, {
      fields: ["Preferred Brands", "Deal"],
    });
    for (const rec of cacheRows) {
      const raw = String(rec.fields?.["Preferred Brands"] || "").trim();
      if (!raw) continue;
      const names = raw.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean);
      const mapped = names.map((n) => resolveCanonical(n, index, knownOptions));
      const effective = mapped.map((m) => {
        if (!m.to || m.to === m.from) return m;
        if (m.status === "alias") return m;
        if (knownOptions.has(norm(m.to))) return m;
        return { ...m, status: "skipped_option", to: m.from };
      });
      if (!effective.some((m) => m.to && m.to !== m.from)) continue;
      const nextNames = [];
      const seen = new Set();
      for (const m of effective) {
        const name = m.to || m.from;
        const key = norm(name);
        if (!key || seen.has(key)) continue;
        seen.add(key);
        nextNames.push(name);
      }
      cacheChanges.push({
        cacheId: rec.id,
        dealId: Array.isArray(rec.fields?.Deal) ? rec.fields.Deal[0] : rec.fields?.Deal,
        from: names,
        to: nextNames,
        mapping: effective,
      });
    }
  } catch (err) {
    console.warn("[cache] skip:", err.message || err);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: !APPLY,
    brandBasicsCount: basics.length,
    siRecordsScanned: siRecords.length,
    siChanges: changes.length,
    cacheChanges: cacheChanges.length,
    unresolvedCount: unresolved.length,
    skippedNeedsOptionCount: skippedNeedsOption.length,
    changes,
    cacheChanges,
    unresolved,
    skippedNeedsOption,
  };

  if (APPLY) {
    const applyErrors = [];
    for (const ch of changes) {
      try {
        await patchRecord(baseId, apiKey, SI_TABLE, ch.siId, { [ch.fieldKey]: ch.to }, true);
        console.log("SI patched", ch.siId, ch.from.join(" | "), "→", ch.to.join(" | "));
      } catch (err) {
        const msg = err.message || String(err);
        console.warn("SI patch failed", ch.siId, msg);
        applyErrors.push({ siId: ch.siId, from: ch.from, to: ch.to, error: msg });
      }
      await new Promise((r) => setTimeout(r, 250));
    }
    for (const ch of cacheChanges) {
      try {
        await patchRecord(baseId, apiKey, CACHE_TABLE, ch.cacheId, { "Preferred Brands": ch.to.join(", ") }, true);
        console.log("Cache patched", ch.cacheId, ch.from.join(" | "), "→", ch.to.join(" | "));
      } catch (err) {
        const msg = err.message || String(err);
        console.warn("Cache patch failed", ch.cacheId, msg);
        applyErrors.push({ cacheId: ch.cacheId, from: ch.from, to: ch.to, error: msg });
      }
      await new Promise((r) => setTimeout(r, 250));
    }
    report.applied = true;
    report.applyErrors = applyErrors;
  }

  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        out: OUT,
        siChanges: changes.length,
        cacheChanges: cacheChanges.length,
        unresolved: unresolved.length,
        sample: changes.slice(0, 5).map((c) => ({ from: c.from, to: c.to })),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
