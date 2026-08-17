/**
 * Programmatic Choice Brand Explorer gap audit (Airtable read-only).
 */
import Airtable from "airtable";
import { resolveProfileForAirtableName, AIRTABLE_TO_PROFILE_NAME } from "../../scripts/lib/choice-chi-brand-resolve.mjs";
import {
  buildCompletePresentationRows,
  slotKeyCounts,
} from "../../scripts/lib/choice-explorer-complete-rows.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";

/** @param {string} airtableName @param {string} [profileName] */
export function airtableNameAliases(airtableName, profileName = "") {
  const aliases = new Set([airtableName, profileName].filter(Boolean));
  for (const [k, v] of Object.entries(AIRTABLE_TO_PROFILE_NAME)) {
    if (k === airtableName || v === profileName || v === airtableName) {
      aliases.add(k);
      aliases.add(v);
    }
  }
  return [...aliases].filter(Boolean);
}

/**
 * @param {import('airtable').Base} base
 * @param {string} brandName — Airtable Brand Basics name
 */
export async function fetchBrandPresentationRecords(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  /** @type {import('airtable').Record<any>[]} */
  const existing = [];
  const seen = new Set();
  const push = (records) => {
    for (const r of records) {
      if (seen.has(r.id)) continue;
      seen.add(r.id);
      existing.push(r);
    }
  };
  try {
    push(
      await base(TABLE)
        .select({ filterByFormula: `{Brand} = "${esc}"`, maxRecords: 500 })
        .all()
    );
  } catch {
    /* schema */
  }
  try {
    push(
      await base(TABLE)
        .select({
          filterByFormula: `AND({Brand Name} = "${esc}", {Active} != FALSE())`,
          maxRecords: 500,
        })
        .all()
    );
  } catch {
    /* optional column */
  }
  return existing;
}

function countMapFromRecords(records) {
  /** @type {Map<string, number>} */
  const counts = new Map();
  for (const rec of records) {
    const sk = String(rec.get("Slot Key") || "").trim();
    if (!sk) continue;
    counts.set(sk, (counts.get(sk) || 0) + 1);
  }
  return counts;
}

/**
 * @param {string} brandName — Airtable Brand Basics name
 * @param {{ apiKey?: string, baseId?: string, aliases?: string[] }} [opts]
 */
export async function auditChoiceBrandPresentationGaps(brandName, opts = {}) {
  const apiKey = opts.apiKey || process.env.AIRTABLE_API_KEY;
  const baseId = opts.baseId || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required for gap audit");
  }

  const profile = resolveProfileForAirtableName(brandName);
  const names = opts.aliases?.length
    ? opts.aliases
    : airtableNameAliases(brandName, profile.name);

  const base = new Airtable({ apiKey }).base(baseId);
  /** @type {import('airtable').Record<any>[]} */
  const existing = [];
  const seen = new Set();
  for (const name of names) {
    for (const rec of await fetchBrandPresentationRecords(base, name)) {
      if (seen.has(rec.id)) continue;
      seen.add(rec.id);
      existing.push(rec);
    }
  }

  const have = countMapFromRecords(existing);
  const canonicalName = names.find((n) => AIRTABLE_TO_PROFILE_NAME[n]) || brandName;
  const expected = slotKeyCounts(buildCompletePresentationRows(canonicalName));

  const missing = [];
  const short = [];
  for (const [sk, need] of expected) {
    const got = have.get(sk) || 0;
    if (got === 0) missing.push(sk);
    else if (got < need) short.push({ slotKey: sk, got, need });
  }

  const gapCount = missing.length + short.length;

  return {
    brandName,
    profileName: profile.name,
    slug: profile.slug,
    aliasesChecked: names,
    existingRowCount: existing.length,
    expectedSlotKeys: expected.size,
    missingSlotKeys: missing.sort(),
    shortCounts: short,
    gapCount,
    l1Complete: gapCount === 0,
  };
}
