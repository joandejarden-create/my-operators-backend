/**
 * Offline: prioritize remaining Brand Setup P1 gaps for brands that appear on live deals.
 * Uses reports/match-score-brand-setup-gap-audit.json + reports/match-score-deal-brand-cache-refresh-apply.json.
 * No Airtable writes. No invented fills.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const AUDIT_PATH = path.join(ROOT, "reports", "match-score-brand-setup-gap-audit.json");
const REFRESH_PATH = path.join(ROOT, "reports", "match-score-deal-brand-cache-refresh-apply.json");
const OUT_JSON = path.join(ROOT, "reports", "match-score-brand-setup-p1-deal-priority.json");
const OUT_MD = path.join(ROOT, "reports", "match-score-brand-setup-p1-deal-priority.md");

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function brandKeyMatch(a, b) {
  const na = norm(a);
  const nb = norm(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  const strip = (x) => x.replace(/\bby\s+\w+\b/g, "").replace(/\s+/g, " ").trim();
  return strip(na) === strip(nb);
}

function main() {
  const audit = JSON.parse(fs.readFileSync(AUDIT_PATH, "utf8"));
  const refresh = JSON.parse(fs.readFileSync(REFRESH_PATH, "utf8"));
  const checkMetaByKey = Object.fromEntries((audit.scoreCriticalChecks || []).map((c) => [c.key, c]));

  const preferredOnDeals = new Map();
  for (const row of refresh.sample || []) {
    const dealId = row.dealId;
    for (const brand of row.preferredBrands || []) {
      const key = norm(brand);
      if (!preferredOnDeals.has(key)) preferredOnDeals.set(key, { display: brand, dealIds: new Set() });
      preferredOnDeals.get(key).dealIds.add(dealId);
    }
  }

  const auditByName = audit.brands || [];
  const matched = [];
  const unmatchedPreferred = [];

  for (const [, meta] of preferredOnDeals) {
    const hit = auditByName.find((b) => brandKeyMatch(b.name, meta.display));
    if (!hit) {
      unmatchedPreferred.push({
        preferredAsSeenOnDeals: meta.display,
        dealCount: meta.dealIds.size,
        dealIds: [...meta.dealIds],
        note: "Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete",
      });
      continue;
    }
    const blankKeys = hit.blankRequiredKeys || [];
    matched.push({
      brandName: hit.name,
      preferredAsSeenOnDeals: meta.display,
      p1Complete: Boolean(hit.p1Complete),
      requiredFillPct: hit.scoreCriticalRequiredPct ?? null,
      dealCount: meta.dealIds.size,
      dealIds: [...meta.dealIds],
      remainingRequiredGaps: blankKeys.map((key) => ({
        key,
        label: checkMetaByKey[key]?.label || key,
        table: checkMetaByKey[key]?.table || null,
      })),
      remainingRequiredCount: blankKeys.length,
    });
  }

  matched.sort((a, b) => {
    if (a.p1Complete !== b.p1Complete) return a.p1Complete ? 1 : -1;
    if (b.dealCount !== a.dealCount) return b.dealCount - a.dealCount;
    return b.remainingRequiredCount - a.remainingRequiredCount;
  });
  unmatchedPreferred.sort((a, b) => b.dealCount - a.dealCount);

  const incomplete = matched.filter((x) => !x.p1Complete);
  const payload = {
    generatedAt: new Date().toISOString(),
    sourceAudit: path.relative(ROOT, AUDIT_PATH).replace(/\\/g, "/"),
    sourceRefresh: path.relative(ROOT, REFRESH_PATH).replace(/\\/g, "/"),
    note:
      "P1 fill priority = brands on live deal preferred lists from last cache refresh sample. Fill only from founder (A) or existing docs (B). Engine now excludes null soft factors from denominator (cache model v3).",
    dealPreferredDistinct: preferredOnDeals.size,
    matchedActiveLiveCount: matched.length,
    incompleteOnDeals: incomplete.length,
    notInActiveLiveCount: unmatchedPreferred.length,
    brands: matched,
    preferredNotInActiveLive: unmatchedPreferred,
  };

  fs.writeFileSync(OUT_JSON, JSON.stringify(payload, null, 2));

  const lines = [
    "# Match Score — P1 Brand Setup priority (brands on deals)",
    "",
    `Generated: ${payload.generatedAt}`,
    "",
    payload.note,
    "",
    `Distinct preferred on deals: **${preferredOnDeals.size}** · Matched Active/Live: **${matched.length}** · Incomplete P1: **${incomplete.length}** · Not in Active/Live: **${unmatchedPreferred.length}**`,
    "",
    "## Active/Live brands on deals",
    "",
    "| Brand | Deals | P1 complete | Fill % | Remaining required | Gaps |",
    "| --- | ---: | --- | ---: | ---: | --- |",
  ];
  for (const row of matched) {
    const gapLabels = row.remainingRequiredGaps.map((g) => g.label || g.key).join("; ") || "—";
    lines.push(
      `| ${row.brandName} | ${row.dealCount} | ${row.p1Complete ? "yes" : "no"} | ${row.requiredFillPct ?? "—"} | ${row.remainingRequiredCount} | ${gapLabels} |`
    );
  }
  lines.push("", "## Preferred on deals but not Active/Live (or name mismatch)", "");
  if (!unmatchedPreferred.length) {
    lines.push("_None._");
  } else {
    lines.push("| Preferred name on deal | Deals | Note |", "| --- | ---: | --- |");
    for (const row of unmatchedPreferred) {
      lines.push(`| ${row.preferredAsSeenOnDeals} | ${row.dealCount} | ${row.note} |`);
    }
  }
  lines.push("", "## Next actions", "");
  lines.push("1. Fill remaining required gaps for incomplete Active/Live brands above (A/B only).");
  lines.push("2. For preferred brands not Active/Live: either activate + complete Brand Setup, or leave scores as insufficient/thin.");
  lines.push("3. `npm run refresh-deal-brand-cache-active-brands` after deploy (cache model v3).");
  lines.push("4. Spot-check View Details: Gate → Mismatch → Missing → Fit; insufficient-data when scored weight < 40%.");
  fs.writeFileSync(OUT_MD, lines.join("\n") + "\n");

  console.log(
    JSON.stringify(
      {
        outJson: OUT_JSON,
        outMd: OUT_MD,
        incompleteOnDeals: incomplete.length,
        matchedActiveLiveCount: matched.length,
        notInActiveLiveCount: unmatchedPreferred.length,
      },
      null,
      2
    )
  );
}

main();
