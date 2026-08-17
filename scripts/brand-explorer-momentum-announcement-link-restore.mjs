#!/usr/bin/env node
/**
 * Restore Recent Momentum announcement URLs + blank-line body structure
 * for brands whose momentum Bodies were scrubbed (URLs stripped, blank lines collapsed).
 *
 * Dry-run by default. Apply:
 *   node scripts/brand-explorer-momentum-announcement-link-restore.mjs --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply --approve-momentum-announcement-link-restore --confirm-no-company-validation-claim
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import {
  getSectionPatternParityContent,
  normalizeMomentumCards,
} from "../lib/partner-intelligence/brand-explorer-section-pattern-parity-content.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const LOOKUPS = {
  "hotel-indigo": "Hotel Indigo",
  "mgallery-collection": "MGallery Collection",
  "small-luxury-hotels-of-the-world": "Small Luxury Hotels of the World",
  "woodspring-suites": "WoodSpring Suites",
  "suburban-studios": "Suburban Studios",
  "quality-inn": "Quality Inn",
  "comfort-inn-suites": "Comfort Inn & Suites",
  "country-inn-suites": "Country Inn & Suites by Choice",
  radisson: "Radisson by Choice",
  "radisson-blu": "Radisson Blu by Choice",
  "radisson-red": "Radisson RED by Choice",
  ascend: "Ascend Hotel Collection",
  "curio-collection": "Curio Collection by Hilton",
  "design-hotels": "Design Hotels",
  "everhome-suites": "Everhome Suites",
  kimpton: "Kimpton Hotels",
  "radisson-individuals-by-choice": "Radisson Individuals by Choice",
  "tribute-portfolio": "Tribute Portfolio",
};

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : Object.keys(LOOKUPS);
  return {
    brands,
    apply: argv.includes("--apply"),
    approve: argv.includes("--approve-momentum-announcement-link-restore"),
    confirmNoCv: argv.includes("--confirm-no-company-validation-claim"),
  };
}

async function fetchBrand(recordId) {
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${recordId}`);
  return res.payload.brand;
}

async function airtablePatch({ apiKey, baseId, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `PATCH failed ${recordId}: ${res.status}`);
  }
  return json;
}

function visibleMomentum(blocks) {
  return (blocks || []).filter(
    (b) =>
      b.slotKey === "footprint.momentum" &&
      b.active !== false &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  );
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }
  if (opts.apply && (!opts.approve || !opts.confirmNoCv)) {
    console.error(
      "Apply requires --approve-momentum-announcement-link-restore --confirm-no-company-validation-claim"
    );
    process.exit(1);
  }

  const report = {
    version: "momentum-announcement-link-restore-v1",
    generatedAt: new Date().toISOString(),
    mode: opts.apply ? "apply" : "dry-run",
    brands: [],
    patches: [],
  };

  for (const slug of opts.brands) {
    const recordId = LOOKUPS[slug];
    const pack = getSectionPatternParityContent(slug);
    if (!recordId || !pack) {
      report.brands.push({ brandSlug: slug, skippedReason: "missing_lookup_or_pack" });
      continue;
    }
    const brand = await fetchBrand(recordId);
    const cards = normalizeMomentumCards(pack);
    const live = visibleMomentum(brand.brandExplorer?.blocks || []).sort(
      (a, b) => Number(a.sort || 0) - Number(b.sort || 0)
    );
    const brandPlan = {
      brandSlug: slug,
      brandName: brand.name,
      updates: [],
      unmatchedLive: [],
      unmatchedPack: [],
    };
    const usedLiveIds = new Set();

    for (let i = 0; i < cards.length; i++) {
      const card = cards[i];
      let match = live.find(
        (r) =>
          !usedLiveIds.has(r.recordId) &&
          nz(r.title).toLowerCase() === card.title.toLowerCase()
      );
      // Title changed (e.g. diligence → openings rewrite): map by sort order.
      if (!match && live[i] && !usedLiveIds.has(live[i].recordId)) {
        match = live[i];
      }
      if (!match?.recordId) {
        brandPlan.unmatchedPack.push(card.title);
        continue;
      }
      usedLiveIds.add(match.recordId);
      const beforeBody = nz(match.body);
      const beforeTitle = nz(match.title);
      const afterBody = card.body;
      const afterTitle = card.title;
      const sortChanged = Number(match.sort || 0) !== Number(card.sort || 0);
      if (beforeBody === afterBody && beforeTitle === afterTitle && !sortChanged) continue;
      const fields = {};
      if (beforeBody !== afterBody) fields.Body = afterBody;
      if (beforeTitle !== afterTitle) fields.Title = afterTitle;
      if (sortChanged) fields["Sort Order"] = card.sort;
      const patch = {
        brandSlug: slug,
        recordId: match.recordId,
        slotKey: "footprint.momentum",
        title: afterTitle,
        priorTitle: beforeTitle,
        beforePreview: beforeBody.slice(0, 160),
        afterPreview: afterBody.slice(0, 220),
        fields,
        fieldMapping: {
          ...(fields.Body ? { Body: "presentation Body (date + summary + announcement URL)" } : {}),
          ...(fields.Title ? { Title: "presentation Title (headline)" } : {}),
          ...(fields["Sort Order"] != null ? { "Sort Order": "presentation Sort Order (newest first)" } : {}),
        },
      };
      brandPlan.updates.push(patch);
      report.patches.push(patch);
    }

    for (const row of live) {
      if (!usedLiveIds.has(row.recordId)) {
        brandPlan.unmatchedLive.push(row.title);
      }
    }

    // Restore momentum label when pack provides one.
    if (pack.momentumLabel) {
      const labelRow = (brand.brandExplorer?.blocks || []).find(
        (b) =>
          b.slotKey === "footprint.momentum_label" &&
          b.active !== false &&
          !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
      );
      if (labelRow?.recordId && nz(labelRow.body) !== nz(pack.momentumLabel)) {
        const patch = {
          brandSlug: slug,
          recordId: labelRow.recordId,
          slotKey: "footprint.momentum_label",
          title: "(label)",
          fields: { Body: pack.momentumLabel },
          fieldMapping: { Body: "presentation Body (momentum section label)" },
        };
        brandPlan.updates.push(patch);
        report.patches.push(patch);
      }
    }

    if (opts.apply) {
      for (const patch of brandPlan.updates) {
        await airtablePatch({ apiKey, baseId, recordId: patch.recordId, fields: patch.fields });
        patch.applied = true;
      }
    }

    report.brands.push(brandPlan);
    console.log(
      `${slug}: ${brandPlan.updates.length} body restore(s)` +
        (brandPlan.unmatchedPack.length ? ` · unmatched pack ${brandPlan.unmatchedPack.length}` : "")
    );
  }

  const outJson = path.join(ROOT, "reports", "brand-explorer-momentum-announcement-link-restore.json");
  const outMd = path.join(ROOT, "reports", "brand-explorer-momentum-announcement-link-restore.md");
  fs.mkdirSync(path.dirname(outJson), { recursive: true });
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2), "utf8");
  const md = [
    "# Momentum announcement link restore",
    "",
    `Mode: **${report.mode}**`,
    `Patches: **${report.patches.length}**`,
    "",
    ...report.brands.flatMap((b) => [
      `## ${b.brandSlug}`,
      "",
      ...(b.updates || []).map(
        (u) => `- \`${u.recordId}\` **${u.title}** → restore date / summary / https URL body`
      ),
      "",
    ]),
  ].join("\n");
  fs.writeFileSync(outMd, md, "utf8");
  console.log(`Wrote ${outJson}`);
  console.log(`Wrote ${outMd}`);
  console.log(`Mode: ${report.mode} · patches: ${report.patches.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
