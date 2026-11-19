/**
 * Read-only audit: secondary contact coverage for Sheraton Guadalajara Expo
 * drawer-comparison inventory. Does not write Airtable or publications.
 */
import { buildSheratonDrawerComparisonInventory } from "../../lib/group-demand-intelligence/presentation/gdi-sheraton-drawer-comparison-v1.js";
import { evaluateSecondaryContactOpportunity } from "../../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import { applyUnifiedWhoToContactStandard } from "../../lib/group-demand-intelligence/unified-who-to-contact-v1.js";

function looksPersonish(name) {
  return /^[\p{Lu}][\p{L}'.-]+(?:\s+[\p{L}'.-]+){1,4}$/u.test(String(name || "").trim());
}

function collectStringNames(value, out = new Set(), depth = 0) {
  if (value == null || depth > 8) return out;
  if (typeof value === "string") {
    if (looksPersonish(value) && value.length < 80) out.add(value.trim());
    return out;
  }
  if (Array.isArray(value)) {
    for (const row of value.slice(0, 80)) collectStringNames(row, out, depth + 1);
    return out;
  }
  if (typeof value === "object") {
    for (const [k, v] of Object.entries(value)) {
      if (/name|person|contact/i.test(k) && typeof v === "string") {
        if (looksPersonish(v)) out.add(v.trim());
      } else {
        collectStringNames(v, out, depth + 1);
      }
    }
  }
  return out;
}

const inv = buildSheratonDrawerComparisonInventory();
const stats = {
  total: inv.items.length,
  primaryNamed: 0,
  secondaryNamedRaw: 0,
  secondaryAfterUnifiedPresentation: 0,
  dispositions: {},
  opportunitiesWithExtraPersonStrings: 0,
  sampleExtraPeople: [],
  sampleMissing: [],
};

for (const item of inv.items) {
  const raw = item.raw || {};
  const primary = raw.primaryContact?.name || raw.contactName || null;
  if (primary) stats.primaryNamed += 1;
  if (raw.secondaryContact?.name) stats.secondaryNamedRaw += 1;

  const evaled = evaluateSecondaryContactOpportunity(raw, {
    force: true,
    allowSavedEvidence: true,
  });
  stats.dispositions[evaled.disposition] =
    (stats.dispositions[evaled.disposition] || 0) + 1;

  const unified = applyUnifiedWhoToContactStandard(raw, { force: true });
  const secNamed =
    unified.scalingTwoContactPresentation?.secondary?.customerFacingNamed === true;
  if (secNamed) stats.secondaryAfterUnifiedPresentation += 1;

  const names = [...collectStringNames(raw)];
  const others = names.filter(
    (n) => n.toLowerCase() !== String(primary || "").toLowerCase(),
  );
  if (others.length) {
    stats.opportunitiesWithExtraPersonStrings += 1;
    if (stats.sampleExtraPeople.length < 12) {
      stats.sampleExtraPeople.push({
        id: item.id,
        primary,
        others: others.slice(0, 6),
      });
    }
  } else if (primary && stats.sampleMissing.length < 8) {
    stats.sampleMissing.push({
      id: item.id,
      primary,
      org: raw.organizationName || raw.displayCompanyName || null,
      segment: raw.segmentDisplayLabel || raw.researchSegment || null,
    });
  }
}

console.log(JSON.stringify(stats, null, 2));
