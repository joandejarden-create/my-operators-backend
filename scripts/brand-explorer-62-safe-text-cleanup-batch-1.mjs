#!/usr/bin/env node
/**
 * Brand Explorer 62 — Safe Text Cleanup Batch 1 (founder review only).
 * Exact patch proposals. No production Airtable writes.
 *
 * Usage:
 *   node scripts/brand-explorer-62-safe-text-cleanup-batch-1.mjs --dry-run
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const VERSION = "brand-explorer-62-safe-text-cleanup-batch-1-v1";

const STATUS = Object.freeze({
  READY: "brand_explorer_62_safe_text_cleanup_batch_1_ready_for_founder_review",
  NEEDS_DECISIONS: "brand_explorer_62_safe_text_cleanup_batch_1_needs_founder_decisions",
  HOLD: "brand_explorer_62_safe_text_cleanup_batch_1_hold_before_apply",
});

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const REWRITE_RULES = [
  {
    id: "listed_on_choice",
    re: /\blisted on choicehotels\.com\b/gi,
    replace: "listed on the brand website",
    risk: "Low",
  },
  {
    id: "choicehotels_domain_prose",
    re: /\bon choicehotels\.com\b/gi,
    replace: "on the brand website",
    risk: "Low",
  },
  {
    id: "consumer_site",
    re: /\bconsumer site\b/gi,
    replace: "brand website",
    risk: "Low",
  },
  {
    id: "source_pack",
    re: /\bsource pack\b/gi,
    replace: "brand materials",
    risk: "Low",
  },
  {
    id: "chd_brand_page",
    re: /\b(?:the\s+)?CHD brand page\b/gi,
    replace: "the brand directory page",
    risk: "Low",
  },
  {
    id: "chd_everhome",
    re: /\bon CHD\b/gi,
    replace: "on the brand directory",
    risk: "Low",
  },
  {
    id: "chd",
    re: /\bCHD\b/g,
    replace: "brand directory",
    risk: "Low",
  },
  {
    id: "item_19_tables",
    re: /\bItem\s*19 tables\b/gi,
    replace: "brand performance tables",
    risk: "Low",
  },
  {
    id: "item_19",
    re: /\bItem\s*19\b/gi,
    replace: "brand performance materials",
    risk: "Low",
  },
  {
    id: "census_property_url",
    re: /\bcensus property URL\b/gi,
    replace: "property listing URL",
    risk: "Low",
  },
  {
    id: "census_url",
    re: /\bcensus URL\b/gi,
    replace: "property listing link",
    risk: "Low",
  },
  {
    id: "active_property_page",
    re: /\bActive Choice Hotels property page\b/gi,
    replace: "Active Choice Hotels property listing",
    risk: "Low",
  },
  {
    id: "census",
    re: /\bcensus\b/gi,
    replace: "property inventory",
    risk: "Low",
  },
  {
    id: "loi",
    re: /\bLOI\b/g,
    replace: "preliminary commercial discussion",
    risk: "Low",
  },
  {
    id: "metadata",
    re: /\bmetadata\b/gi,
    replace: "listing details",
    risk: "Low",
  },
  {
    id: "source_data",
    re: /\bsource data\b/gi,
    replace: "brand materials",
    risk: "Low",
  },
];

const TEXT_FIELDS = [
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Owner Objective",
  "Case Summary Interpretation",
  "Case Summary Tags",
];

/** Map rewrite id → batch lane priority */
const BATCH_1A_TERM_IDS = new Set([
  "item_19",
  "item_19_tables",
  "fdd",
  "chd",
  "chd_brand_page",
  "chd_everhome",
  "source_pack",
  "consumer_site",
  "listed_on_choice",
  "choicehotels_domain_prose",
  "loi",
  "adr",
  "revpar",
  "active_property_page",
]);

const BATCH_1B_TERM_IDS = new Set([
  "census",
  "census_url",
  "census_property_url",
  "metadata",
  "source_data",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(abs, data) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, `${JSON.stringify(data, null, 2)}\n`);
}

function writeMd(abs, text) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, text.endsWith("\n") ? text : `${text}\n`);
}

function esc(s) {
  return String(s || "").replace(/\|/g, "\\|").replace(/\n/g, " ⏎ ");
}

async function fetchRecord(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `fetch ${recordId} ${res.status}`);
  const f = json.fields || {};
  const texts = {};
  for (const name of TEXT_FIELDS) texts[name] = nz(f[name]);
  return {
    recordId: json.id,
    brandName: nz(f["Brand Name"]),
    slotKey: nz(f["Slot Key"]),
    externalDisplayStatus: nz(f["External Display Status"]),
    active: f.Active !== false,
    texts,
  };
}

function applyRewrites(text, allowedIds = null) {
  let out = text;
  const applied = [];
  for (const rule of REWRITE_RULES) {
    if (allowedIds && !allowedIds.has(rule.id)) continue;
    if (!rule.re.test(out)) continue;
    rule.re.lastIndex = 0;
    const before = out;
    out = out.replace(rule.re, rule.replace);
    if (out !== before) applied.push(rule.id);
  }
  // Collapse accidental double spaces / awkward articles
  out = out
    .replace(/\bthe\s+the\b/gi, "the")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\s+\./g, ".")
    .trim();
  return { text: out, applied };
}

function samePropertyMatch(beTitle, censusName) {
  const a = nz(beTitle).toLowerCase();
  const b = nz(censusName).toLowerCase();
  if (!a || !b) return false;
  // Require shared distinctive tokens beyond brand/airport/hotel
  const stop = new Set([
    "by",
    "the",
    "hotel",
    "hotels",
    "inn",
    "suites",
    "collection",
    "hilton",
    "marriott",
    "airport",
    "resort",
    "spa",
    "and",
    "curio",
    "kimpton",
    "motto",
    "holiday",
    "express",
    "courtyard",
    "garden",
  ]);
  const tokens = (s) =>
    s
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .split(" ")
      .filter((t) => t.length > 2 && !stop.has(t));
  const ta = new Set(tokens(a));
  const tb = new Set(tokens(b));
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  // Same property if strong place/name overlap (e.g. mas olas + todos santos)
  return inter >= 2 || (inter >= 1 && (a.includes("mas olas") && b.includes("mas olas")));
}

function loadGateSnapshot() {
  const semantic =
    readJson("reports/brand-explorer-global-active-semantic-audit-refresh.json") ||
    readJson("reports/brand-explorer-global-active-semantic-audit.json");
  const pvql = readJson("reports/brand-explorer-public-visibility-quality-lock-quiet.json");
  const footnote = readJson("reports/brand-explorer-ai-assisted-footnote-audit-enriched.json");
  const quality = readJson("reports/brand-explorer-24-tab-section-quality-audit-quiet.json");
  const universe = readJson("reports/brand-explorer-active-universe-source-of-truth.json");
  const momentum = readJson("reports/brand-explorer-recent-momentum-evidence-quality.json");
  const mandatory = readJson("reports/brand-explorer-mandatory-release-gates.json");
  return {
    activeCount: universe?.activeSourceOfTruth?.totalCount ?? null,
    semanticCHM: semantic?.severityTotals || null,
    pvqlPass: pvql?.summary?.overallPass === true,
    pvqlCount: pvql?.summary?.publicFullProfileCount ?? null,
    footnotePass: footnote?.summary?.fail === 0,
    momentumPass: momentum?.pass === true || momentum?.cliPass === true,
    mandatoryPass: mandatory?.pass === true || mandatory?.cliPass === true,
    quality: quality
      ? {
          freezeDecision: quality.baselineFreezeDecision,
          recommendationCounts: quality.recommendationCounts,
          minor: (quality.brandResults || [])
            .filter((b) => b.overallRecommendation === "approve_after_minor_cleanup")
            .map((b) => b.slug || b.brand),
        }
      : null,
  };
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("Refusing --apply. Batch 1 is founder-review proposals only.");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run");
    process.exit(2);
  }

  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE_API_KEY / AIRTABLE_BASE_ID");

  const plan = readJson("reports/brand-explorer/brand-explorer-62-background-validation-plan.json");
  if (!plan) throw new Error("Missing background validation plan JSON");

  const universe = readJson("reports/brand-explorer-active-universe-source-of-truth.json");
  const slugToBrand = new Map((universe?.inventory || []).map((b) => [b.slug, b]));

  // Collect unique hit records from text review
  /** @type {Map<string, { brand: string, recordId: string, termIds: Set<string>, slotKey: string }>} */
  const byRecord = new Map();
  for (const t of plan.ownerFacingTextReview || []) {
    for (const h of [...(t.pvqlHits || []), ...(t.extraHits || [])]) {
      if (!h.recordId) continue;
      const key = h.recordId;
      if (!byRecord.has(key)) {
        byRecord.set(key, {
          brand: t.brand,
          recordId: h.recordId,
          termIds: new Set(),
          slotKey: h.slotKey || "",
        });
      }
      byRecord.get(key).termIds.add(h.id);
    }
  }

  console.log(`[${VERSION}] fetching ${byRecord.size} presentation records...`);
  const patches = [];
  const held = [];
  const founderDecisions = [];

  for (const row of byRecord.values()) {
    process.stdout.write(`  ${row.brand} ${row.recordId}... `);
    let rec;
    try {
      rec = await fetchRecord(baseId, token, row.recordId);
      await sleep(180);
    } catch (e) {
      console.log(`ERR ${e.message}`);
      held.push({
        brand: row.brand,
        recordId: row.recordId,
        reason: `fetch_failed:${e.message}`,
        applyRecommendation: "hold_for_later",
      });
      continue;
    }

    const termIds = [...row.termIds];
    // Always allow full rewrite rule set for safe cleanup — hit ids may under-specify case-summary fields
    const allowed = new Set([
      ...REWRITE_RULES.map((r) => r.id),
    ]);

    const fieldPatches = [];
    const appliedAll = new Set();
    for (const fieldName of TEXT_FIELDS) {
      const current = rec.texts[fieldName] || "";
      if (!current) continue;
      // Do not rewrite trailing announcement URLs out of openings/momentum bodies — only prose terms
      const rw = applyRewrites(current, allowed);
      // Preserve https lines in openings/momentum when only prose changed
      if (rw.text === current) continue;
      for (const a of rw.applied) appliedAll.add(a);
      fieldPatches.push({ fieldName, current, proposed: rw.text, applied: rw.applied });
    }

    if (!fieldPatches.length) {
      held.push({
        brand: row.brand,
        recordId: row.recordId,
        slotKey: rec.slotKey,
        termIds,
        reason: "rewrite_noop_manual_review",
        currentTitle: rec.texts.Title,
        currentBody: (rec.texts.Body || "").slice(0, 240),
        applyRecommendation: "needs_founder_decision",
      });
      founderDecisions.push({
        brand: row.brand,
        topic: "safe_text_cleanup_noop",
        recordId: row.recordId,
        reason: `Terms ${termIds.join(",")} flagged but no Title/Body/Case Summary rewrite delta — inspect manually`,
      });
      console.log("noop");
      continue;
    }

    const applied = [...appliedAll];
    const batchLane = applied.some((id) => BATCH_1A_TERM_IDS.has(id)) ? "1A" : "1B";
    const brandMeta = slugToBrand.get(row.brand);

    for (const fp of fieldPatches) {
      patches.push({
        brand: brandMeta?.brandName || row.brand,
        brandSlug: row.brand,
        airtableRecordId: row.recordId,
        fieldName: fp.fieldName,
        slotKey: rec.slotKey || row.slotKey,
        patchCategory: "safe_text_cleanup",
        batchLane,
        currentText: fp.current,
        proposedText: fp.proposed,
        reason: `Remove/replace owner-facing leakage: ${fp.applied.join(", ") || applied.join(", ")}`,
        sourceSupport: "validation_forbidden_language_scan",
        censusSupport: null,
        riskLevel: "Low",
        founderApprovalRequired: true,
        applyRecommendation: "include_in_batch_1",
        termsRemoved: fp.applied.length ? fp.applied : applied,
      });
    }
    console.log(`${batchLane} ${applied.join(",")}`);
  }

  // Property example refreshes — only true same-property Census matches
  const propExamples = (plan.propertyExamples || []).filter(
    (e) => e.classification === "confirmed_but_needs_text_update"
  );
  const propertyPatches = [];
  for (const ex of propExamples) {
    const match = ex.match;
    const same = samePropertyMatch(ex.title, match?.propertyName);
    if (!same) {
      held.push({
        brand: ex.brand,
        category: "property_example_update",
        currentTitle: ex.title,
        censusCandidate: match?.propertyName,
        reason: "fuzzy_census_match_is_different_property — do not rewrite BE example to a different hotel",
        applyRecommendation: "hold_for_later",
        riskLevel: "High",
      });
      founderDecisions.push({
        brand: ex.brand,
        topic: "property_example_false_match",
        reason: `"${ex.title}" was loosely matched to Census "${match?.propertyName}" — keep International/CALA example; do not swap`,
      });
      continue;
    }
    if (match?.humanReviewRequired || match?.affiliationStatus === "Brand-Unconfirmed") {
      held.push({
        brand: ex.brand,
        category: "property_example_update",
        currentTitle: ex.title,
        reason: "census_record_not_public_safe",
        applyRecommendation: "hold_for_later",
      });
      continue;
    }

    // Find openings record by title via brand presentation fetch is heavy; propose title-only refresh from validation title
    const city = match.city || "";
    const country = match.country || "";
    const cleanName = match.propertyName.replace(/\s*,\s*Curio Collection by Hilton$/i, "");
    let proposedTitle = ex.title;
    // Soft location framing only
    if (/kimpton mas olas/i.test(ex.title)) {
      proposedTitle = `Kimpton Mas Olas Resort & Spa — Todos Santos, Mexico`;
    } else if (city && country) {
      proposedTitle = `${cleanName} — ${city}, ${country}`;
    }

    if (proposedTitle === ex.title) {
      held.push({
        brand: ex.brand,
        category: "property_example_update",
        currentTitle: ex.title,
        reason: "same_property_but_no_safe_title_delta",
        applyRecommendation: "hold_for_later",
      });
      continue;
    }

    propertyPatches.push({
      brand: slugToBrand.get(ex.brand)?.brandName || ex.brand,
      brandSlug: ex.brand,
      airtableRecordId: null, // resolve at apply time by Brand+Title match
      fieldName: "Title",
      slotKey: "footprint.openings",
      patchCategory: "property_example_update",
      batchLane: "1B",
      currentText: ex.title,
      proposedText: proposedTitle,
      reason: "Same-property Census confirmation — refresh location framing only",
      sourceSupport: "census_source_url_present",
      censusSupport: {
        censusRecordId: match.recordId,
        propertyName: match.propertyName,
        city: match.city,
        country: match.country,
        affiliationStatus: match.affiliationStatus,
        humanReviewRequired: match.humanReviewRequired,
      },
      riskLevel: "Low",
      founderApprovalRequired: true,
      applyRecommendation: "include_in_batch_1",
      note: "Resolve Airtable record ID at apply by Brand Name + current Title on footprint.openings",
    });
  }

  // MGallery — not low-risk (major missing slots + image caption work). Hold with proposal note.
  const mgalleryProposal = {
    brand: "MGallery Collection",
    brandSlug: "mgallery-collection",
    airtableRecordId: null,
    fieldName: "quality_audit_minor_cleanup",
    slotKey: null,
    patchCategory: "Webflow_render_fix",
    batchLane: null,
    currentText: "approve_after_minor_cleanup (majors: missing Guest Psychographics, valueOwners.overview, valueOwners.watchouts; minors: thin portfolio_mix + caption roles)",
    proposedText:
      "Hold for a dedicated MGallery cleanup packet — missing major slots are not a Low-risk Batch 1 text rewrite. Optional later: thicken footprint.portfolio_mix (rec8JMqWvldhwE1xn) without factual invention.",
    reason: "Quality quiet minor is not Low-risk factual-safe for Batch 1; majors require content additions",
    sourceSupport: "quality_audit_quiet",
    censusSupport: null,
    riskLevel: "Medium",
    founderApprovalRequired: true,
    applyRecommendation: "hold_for_later",
  };
  held.push({
    ...mgalleryProposal,
    reason: mgalleryProposal.reason,
  });
  founderDecisions.push({
    brand: "mgallery-collection",
    topic: "webflow_render_minor",
    recommendation: "hold_for_later",
    reason: "MGallery needs dedicated cleanup for missing major slots; not include_in_batch_1",
  });

  const allBatch1 = [...patches, ...propertyPatches].filter((p) => p.applyRecommendation === "include_in_batch_1");
  const batch1A = allBatch1.filter((p) => p.batchLane === "1A");
  const batch1B = allBatch1.filter((p) => p.batchLane === "1B");

  const termsRemoved = {};
  for (const p of allBatch1) {
    for (const t of p.termsRemoved || []) termsRemoved[t] = (termsRemoved[t] || 0) + 1;
  }

  const brandsIncluded = [...new Set(allBatch1.map((p) => p.brandSlug))].sort();
  const gates = loadGateSnapshot();

  let status = STATUS.READY;
  if (allBatch1.length === 0) status = STATUS.HOLD;
  else if (founderDecisions.length > 0) status = STATUS.NEEDS_DECISIONS;

  // If we have a ready batch AND founder decisions are only for held items, still READY_FOR_REVIEW
  // but surface decisions. Prefer READY when Batch 1 itself is complete for apply-after-approval.
  if (allBatch1.length > 0 && batch1A.length + batch1B.length > 0) {
    status = founderDecisions.some((d) => /batch_1|include/i.test(d.topic))
      ? STATUS.NEEDS_DECISIONS
      : STATUS.READY;
  }

  const report = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    status,
    executiveSummary: {
      brandsInBatch1: brandsIncluded.length,
      patchCount: allBatch1.length,
      batch1A: batch1A.length,
      batch1B: batch1B.length,
      propertyExampleRefreshes: propertyPatches.length,
      mgalleryIncluded: false,
      mgalleryHeld: true,
      heldCount: held.length,
      founderDecisionCount: founderDecisions.length,
      termsRemoved,
      gates,
    },
    scope: {
      includedCategories: ["safe_text_cleanup", "property_example_update"],
      excluded: [
        "Recent Momentum",
        "hotel count / global footprint from Mexico Census",
        "Company Validated / Brand Verified / Brand Status / release fields",
        "Census writes",
        "owner/operator/rooms/date claims",
        "wrong-property Census fuzzy swaps",
        "MGallery major slot fills",
      ],
    },
    brandsIncluded,
    fieldsIncluded: [...new Set(allBatch1.map((p) => `${p.slotKey}.${p.fieldName}`))].sort(),
    forbiddenLanguageRemoved: termsRemoved,
    propertyExamplesRefreshed: propertyPatches,
    mgalleryRenderFixProposal: mgalleryProposal,
    batch1A,
    batch1B,
    patches: allBatch1,
    heldForLater: held,
    founderDecisionsNeeded: founderDecisions,
    validationGateResults: gates,
    applyCommandForLater: {
      note: "Do not run until founder approval",
      suggested:
        "node scripts/brand-explorer-62-safe-text-cleanup-batch-1-apply.mjs --dry-run  # (not created until approved) then --apply with confirm flags",
      forbiddenNow: true,
    },
    hardRulesHonored: [
      "No production Brand Explorer patches applied",
      "No Census writes",
      "No Recent Momentum patches",
      "No Company Validated / Brand Verified / Brand Status writes",
      "Wrong Census fuzzy matches held — not swapped",
      "MGallery majors held out of Batch 1",
    ],
  };

  const outDir = path.join(ROOT, "reports", "brand-explorer");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  writeJson(path.join(outDir, "brand-explorer-62-safe-text-cleanup-batch-1.json"), report);
  writeMd(path.join(outDir, "brand-explorer-62-safe-text-cleanup-batch-1.md"), renderMainMd(report));
  writeMd(path.join(outDir, "brand-explorer-62-safe-text-cleanup-batch-1-diff.md"), renderDiffMd(report));
  writeMd(path.join(docsDir, "brand-explorer-62-safe-text-cleanup-batch-1.md"), renderDocsMd(report));

  console.log(`Status: ${status}`);
  console.log(`Batch 1 patches: ${allBatch1.length} (1A=${batch1A.length}, 1B=${batch1B.length})`);
  console.log(`Property refreshes: ${propertyPatches.length}`);
  console.log(`Held: ${held.length} · Founder decisions: ${founderDecisions.length}`);
  console.log(`Wrote reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.*`);
}

function renderMainMd(r) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Safe Text Cleanup Batch 1 (Founder Review)");
  lines.push("");
  lines.push(`**Status:** \`${r.status}\``);
  lines.push(`**Generated:** ${r.generatedAt}`);
  lines.push(`**Mode:** dry-run / exact patch proposals only — **do not apply**`);
  lines.push("");
  lines.push("## 1. Executive summary");
  lines.push("");
  const e = r.executiveSummary;
  lines.push(`- Brands in Batch 1: **${e.brandsInBatch1}**`);
  lines.push(`- Patches: **${e.patchCount}** (1A high-leakage **${e.batch1A}** · 1B lighter **${e.batch1B}**)`);
  lines.push(`- Property example refreshes included: **${e.propertyExampleRefreshes}**`);
  lines.push(`- MGallery render fix: **held** (not Low-risk for Batch 1)`);
  lines.push(`- Held for later: **${e.heldCount}** · Founder decisions: **${e.founderDecisionCount}**`);
  lines.push(`- Gates (last known): PVQL=${e.gates?.pvqlPass} · footnote=${e.gates?.footnotePass} · semantic=${JSON.stringify(e.gates?.semanticCHM)} · quality=${e.gates?.quality?.freezeDecision}`);
  lines.push("");
  lines.push("## 2. Brands included in Batch 1");
  lines.push("");
  lines.push(r.brandsIncluded.map((s) => `\`${s}\``).join(", ") || "_none_");
  lines.push("");
  lines.push("## 3. Fields included");
  lines.push("");
  for (const f of r.fieldsIncluded) lines.push(`- \`${f}\``);
  lines.push("");
  lines.push("## 4. Forbidden language removed");
  lines.push("");
  lines.push("| Term id | Patch count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(r.forbiddenLanguageRemoved || {})) {
    lines.push(`| \`${k}\` | ${v} |`);
  }
  lines.push("");
  lines.push("## 5. Property examples refreshed");
  lines.push("");
  if (!r.propertyExamplesRefreshed?.length) {
    lines.push("_None included in Batch 1._ Wrong fuzzy Census matches are held (see §8).");
  } else {
    for (const p of r.propertyExamplesRefreshed) {
      lines.push(`- **${p.brandSlug}**: \`${esc(p.currentText)}\` → \`${esc(p.proposedText)}\``);
    }
  }
  lines.push("");
  lines.push("## 6. MGallery render fix proposal");
  lines.push("");
  lines.push(`- Recommendation: **\`${r.mgalleryRenderFixProposal.applyRecommendation}\`**`);
  lines.push(`- Risk: **${r.mgalleryRenderFixProposal.riskLevel}**`);
  lines.push(`- ${r.mgalleryRenderFixProposal.reason}`);
  lines.push(`- Proposed posture: ${r.mgalleryRenderFixProposal.proposedText}`);
  lines.push("");
  lines.push("## 7. Exact before/after diffs");
  lines.push("");
  lines.push("See `brand-explorer-62-safe-text-cleanup-batch-1-diff.md`.");
  lines.push("");
  lines.push("## 8. Patches held for later");
  lines.push("");
  lines.push(`Count: **${r.heldForLater.length}** (includes wrong-property Census matches, MGallery, noop rewrites).`);
  lines.push("");
  for (const h of r.heldForLater.slice(0, 40)) {
    lines.push(
      `- **${h.brand || h.brandSlug}** · ${h.category || h.slotKey || "—"} · ${esc(h.reason || h.currentTitle || "")}`
    );
  }
  if (r.heldForLater.length > 40) lines.push(`- … +${r.heldForLater.length - 40} more in JSON`);
  lines.push("");
  lines.push("## 9. Founder decisions needed");
  lines.push("");
  for (const d of r.founderDecisionsNeeded) {
    lines.push(`- **${d.brand}** · ${d.topic} — ${esc(d.reason)}`);
  }
  lines.push("");
  lines.push("## 10. Validation gate results");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.validationGateResults, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 11. Apply command (later — do not run now)");
  lines.push("");
  lines.push("```bash");
  lines.push(r.applyCommandForLater.suggested);
  lines.push("```");
  lines.push("");
  lines.push(`**Final status:** \`${r.status}\``);
  lines.push("");
  return lines.join("\n");
}

function renderDiffMd(r) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Safe Text Cleanup Batch 1 Diffs");
  lines.push("");
  lines.push(`Status: \`${r.status}\` · Generated: ${r.generatedAt}`);
  lines.push("");
  lines.push("## Batch 1A — highest-risk public leakage");
  lines.push("");
  for (const p of r.batch1A) {
    lines.push(`### ${p.brandSlug} · \`${p.slotKey}\` · ${p.fieldName} · \`${p.airtableRecordId}\``);
    lines.push("");
    lines.push("**Before:**");
    lines.push("");
    lines.push("```");
    lines.push(p.currentText);
    lines.push("```");
    lines.push("");
    lines.push("**After:**");
    lines.push("");
    lines.push("```");
    lines.push(p.proposedText);
    lines.push("```");
    lines.push("");
    lines.push(`Terms: ${(p.termsRemoved || []).join(", ")} · Risk: ${p.riskLevel} · \`${p.applyRecommendation}\``);
    lines.push("");
  }
  lines.push("## Batch 1B — lighter wording + property example refresh");
  lines.push("");
  for (const p of r.batch1B) {
    lines.push(`### ${p.brandSlug} · \`${p.slotKey}\` · ${p.fieldName} · \`${p.airtableRecordId || "resolve-at-apply"}\``);
    lines.push("");
    lines.push("**Before:**");
    lines.push("");
    lines.push("```");
    lines.push(p.currentText);
    lines.push("```");
    lines.push("");
    lines.push("**After:**");
    lines.push("");
    lines.push("```");
    lines.push(p.proposedText);
    lines.push("```");
    lines.push("");
    lines.push(`Risk: ${p.riskLevel} · \`${p.applyRecommendation}\` · ${esc(p.reason)}`);
    lines.push("");
  }
  return lines.join("\n");
}

function renderDocsMd(r) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Safe Text Cleanup Batch 1");
  lines.push("");
  lines.push(`> **Status:** \`${r.status}\`  `);
  lines.push(`> **Generated:** ${r.generatedAt}  `);
  lines.push(`> **Writes:** none — founder review only`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(
    `Batch 1 prepares **${r.executiveSummary.patchCount}** Low-risk owner-facing text cleanups across **${r.executiveSummary.brandsInBatch1}** brands (1A=${r.executiveSummary.batch1A}, 1B=${r.executiveSummary.batch1B}). MGallery quality minor and wrong Census fuzzy property swaps are **held**. Recent Momentum is excluded.`
  );
  lines.push("");
  lines.push("## Artifacts");
  lines.push("");
  lines.push("- `reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.md`");
  lines.push("- `reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.json`");
  lines.push("- `reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1-diff.md`");
  lines.push("");
  lines.push("## Apply (later)");
  lines.push("");
  lines.push("Do not apply until founder approval. Suggested later command is recorded in the report JSON under `applyCommandForLater`.");
  lines.push("");
  return lines.join("\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
