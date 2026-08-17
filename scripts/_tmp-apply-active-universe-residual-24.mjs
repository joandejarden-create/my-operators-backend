#!/usr/bin/env node
/**
 * Active-universe residual remediation toward 24/24 public-full PVQL.
 * Presentation Body / Case Summary fields only.
 * No CV / Source / Registry / Brand Status / release / image writes.
 */
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { scrubResidualOwnerFacingCopy } from "../lib/partner-intelligence/brand-explorer-residual-owner-copy-remediation.js";
import { evaluateExternalOwnerReadinessRule } from "../lib/partner-intelligence/brand-explorer-external-owner-readiness-rules.js";
import {
  isOwnerFacingPresentationRow,
  scanOwnerFacingForbiddenLanguage,
  evaluateBrandPublicVisibility,
} from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

const APPLY = process.argv.includes("--apply");
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FDD_TARGETS = Object.freeze([
  { slug: "autograph-collection", brandId: "recEJCTDj1zrsjPM6" },
  { slug: "design-hotels", brandId: "rec02zPClpWUTCyXM" },
  { slug: "handwritten-collection", brandId: "rec7hTXwMRC81EPqz" },
  { slug: "hotel-indigo", brandId: "recegXrqaPiSLGCIe" },
  { slug: "mgallery-collection", brandId: "recrWCD1LMqu864oU" },
]);

const MODAL_FILLS = Object.freeze([
  {
    slug: "autograph-collection",
    recordId: "recgRXQXNcFsMi88y",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate resort or destination assets that need Marriott distribution and Bonvoy while keeping a distinctive Autograph story rather than a rigid full-service prototype.",
      "Case Summary Owner Objective":
        "Reference for leisure-forward soft-brand underwriting where property identity and design narrative remain commercially central.",
      "Case Summary Interpretation":
        "Treat published listing details as directional context; validate capital intensity, operating complexity, and systems cutover against the actual asset before modeling affiliation outcomes.",
    },
  },
  {
    slug: "autograph-collection",
    recordId: "recrpSL7aCpQrUOET",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate urban independents seeking Autograph's soft-brand posture with Marriott commercial reach without a standardized midscale conversion template.",
      "Case Summary Owner Objective":
        "Reference for established urban product with a clear guest story—not a ground-up brand-led redesign.",
      "Case Summary Interpretation":
        "Confirm local operating standards, capital gaps, and loyalty participation with brand development before using this example as an underwriting proxy.",
    },
  },
  {
    slug: "autograph-collection",
    recordId: "recxDGhforwwQ7gFg",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate design-led urban assets that want Autograph identity with Marriott systems while preserving ownership character in the guest proposition.",
      "Case Summary Owner Objective":
        "Reference for lifestyle urban soft-brand positioning where individuality is part of the commercial offer.",
      "Case Summary Interpretation":
        "Validate conversion scope, service standards, and commercial ramp for the specific market before treating this listing as performance guidance.",
    },
  },
  {
    slug: "handwritten-collection",
    recordId: "reccdJHKZ1Y2BB66U",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate leisure or residential-adjacent assets seeking Accor's Handwritten soft-brand posture with distribution support while keeping a property-specific identity.",
      "Case Summary Owner Objective":
        "Reference for distinctive leisure product where ownership character stays central—not a prototype midscale conversion play.",
      "Case Summary Interpretation":
        "Confirm membership expectations, capital gaps, and commercial activation with brand development before modeling from this listing.",
    },
  },
  {
    slug: "handwritten-collection",
    recordId: "recNV2ya5WD1bfhXa",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate urban independents seeking Handwritten Collection identity with Accor reach without assuming a full lifestyle prototype rebuild.",
      "Case Summary Owner Objective":
        "Reference for established urban product with a clear guest story and independent operating character.",
      "Case Summary Interpretation":
        "Validate service standards, capital intensity, and systems participation for the specific asset before treating this as an underwriting proxy.",
    },
  },
  {
    slug: "handwritten-collection",
    recordId: "recOcjGx7MvBrmMxA",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate destination or spa-led independents that want Handwritten soft affiliation while preserving a property-specific guest proposition.",
      "Case Summary Owner Objective":
        "Reference for design-led destination assets where individuality is commercially important.",
      "Case Summary Interpretation":
        "Confirm conversion scope, operating readiness, and commercial ramp with brand representatives before relying on this example for deal modeling.",
    },
  },
]);

const COMFORT_EMPTY_BODIES = Object.freeze([
  {
    recordId: "reccJUPHYLweHrFqv",
    body: "Comfort Inn Puebla Centro Histórico is a published property reference for owners comparing midscale conversion fit, guest-experience intensity, and Choice systems under Comfort Inn & Suites—use as positioning context, not a performance proxy.",
  },
  {
    recordId: "recIBb27MXxefRGag",
    body: "Comfort Inn & Suites in Levittown, Puerto Rico is a published property reference for owners comparing midscale conversion fit, guest-experience intensity, and Choice systems—use as positioning context, not a performance proxy.",
  },
  {
    recordId: "recMFFcS2IYz5SCa0",
    body: "Comfort Inn Real San Miguel is a published property reference for owners comparing midscale conversion fit, guest-experience intensity, and Choice systems under Comfort Inn & Suites—use as positioning context, not a performance proxy.",
  },
  {
    recordId: "recLAVmhVsx1ECoX6",
    body: "Comfort Inn & Suites in La Union, El Salvador is a published property reference for owners comparing midscale conversion fit, guest-experience intensity, and Choice systems—use as positioning context, not a performance proxy.",
  },
]);

const CONTENT_THICKEN = Object.freeze({
  "bw-premier-collection": {
    brandId: "recwXZ5gVZ8ZH8ekA",
    bySlot: {
      "overview.why_value":
        "• Strongest where the hotel already has a differentiated upscale story and ownership wants independent positioning with BWH distribution reach.\n• Works when public spaces, service delivery, and design narrative can credibly sit above core Best Western expectations without forcing a rigid prototype rebuild.\n• Weaker for generic assets seeking only a familiar sign, or for properties unable to fund the product and service expectations of an upscale collection.\n• Owners should test local comps, operator capability, and systems readiness before treating Premier as a default soft-brand path.",
      "overview.scenario.2":
        "A well-located hotel requiring a sharper upscale identity, public-space refresh, or service repositioning. Premier can provide a BWH platform path when the asset's design and experience can credibly move beyond core Best Western expectations. Confirm required capital, review milestones, and operator readiness first so the repositioning is funded and executable—not cosmetic branding alone.",
      "overview.scenario.3":
        "A boutique urban, resort, or destination asset where local character is commercially important and owner control over positioning matters. Premier may fit when the property needs distribution support without losing its independent narrative. Test local competitive positioning, operator capability, and systems readiness before proceeding so affiliation amplifies—not replaces—the hotel's guest proposition.",
    },
    galleryBodyTemplate: (title) =>
      `${String(title || "").replace(/^[^—]+—\s*/, "") || "Property gallery view"} illustrates the guest-facing product owners should underwrite for BW Premier Collection—use as visual context for conversion fit and experience intensity, not as a performance claim.`,
  },
  "bw-signature-collection": {
    brandId: "recdeh1NsP4gjrv80",
    bySlot: {
      "overview.why_value":
        "• Strongest where an independent hotel needs flexible soft-brand support and BWH platform access without Premier-level upscale intensity.\n• Works when owners want clearer identity and distribution while keeping practical conversion economics and local character.\n• Weaker when the asset cannot support consistent guest delivery, or when owners expect a full upscale collection repositioning without capital.\n• Confirm how Signature differs from Premier and core Best Western for the specific asset before selecting a collection path.",
    },
    galleryBodyTemplate: (title) =>
      `${String(title || "").replace(/^[^—]+—\s*/, "") || "Property gallery view"} illustrates the guest-facing product owners should underwrite for BW Signature Collection—use as visual context for conversion fit and experience intensity, not as a performance claim.`,
  },
  "preferred-hotels-and-resorts": {
    brandId: "recwl5JOYxlChuCAr",
    bySlot: {
      "overview.why_value":
        "• Strongest where the hotel already has a compelling independent luxury or upscale proposition and needs commercial representation rather than a franchise prototype.\n• Works when ownership values control over identity while seeking distribution, sales support, and collection-fit positioning.\n• Weaker when affiliation is treated as a substitute for product, service, or market positioning quality.\n• Owners should confirm program category, regional resources, and operator accountabilities before modeling outcomes from platform messaging alone.",
      "overview.scenario.1":
        "An established independent luxury or upscale hotel seeking broader commercial representation without converting into a traditional franchise prototype. Preferred can amplify distribution and sales reach when the property already has a credible guest proposition. Confirm membership category, regional support, and operator readiness so representation strengthens—not replaces—local execution.",
      "overview.scenario.2":
        "A repositioning or soft opening where owners want collection fit and commercial support while protecting independent identity. Preferred may help when capital is directed at product and service quality rather than brand-mandated prototype rebuilds. Validate program expectations, content standards, and launch milestones before treating affiliation as the primary ramp driver.",
      "overview.scenario.3":
        "A destination, resort, or urban independent competing against soft brands and lifestyle collections. Preferred can be relevant when representation and loyalty-oriented programs matter, but the owner still wants control of the guest story. Compare against SLH, Design Hotels, Autograph, Tribute, Curio, and Vignette on fit, control, and commercial support—not on unsupported fee or disclosure assumptions.",
      "overview.proof.2":
        "Official materials emphasize sales, marketing, distribution, and guest-loyalty-oriented support for member hotels. Confirm which programs, channels, systems, and regional resources apply to the specific property and agreement before modeling commercial lift from platform messaging alone.",
      "valueOwners.lifecycle.3":
        "Coordinate commercial launch planning, property content, channel setup, training, and operator responsibilities with the applicable Preferred program. Confirm timing, technical requirements, and owner versus operator accountabilities before opening so representation and operations launch together.",
      "valueOwners.lifecycle.4":
        "Launch with a consistent independent identity and service delivery that match the promised guest proposition. Use commercial representation as an amplifier, not as a replacement for operating readiness, staffing, or local market activation in the opening window.",
      "valueOwners.lifecycle.5":
        "Review guest feedback, channel mix, and commercial activity against the hotel's own target segment and competitive set. Adjust programming, sales focus, and service execution without assuming that collection participation alone determines ramp-up performance.",
      "valueOwners.lifecycle.6":
        "Refresh the property's positioning and commercial plan as the market, operator, and guest proposition evolve. Reconfirm membership expectations and program fit before major repositioning, operator change, or renewal decisions so affiliation stays aligned with the asset.",
      "economics.opening.step.2":
        "Review property presentation, membership fit, commercial programs, distribution, guest engagement, and any applicable quality expectations with Preferred representatives. Keep agreement-level obligations separate from general platform positioning when underwriting conversion scope.",
      "economics.opening.step.3":
        "Build the launch plan around operator readiness, content, commercial activation, distribution setup, training, and the hotel's independent guest proposition. Confirm milestones and responsibilities with ownership, operator, and Preferred before capital and opening dates lock.",
      "economics.opening.step.4":
        "Coordinate commercial activation with an operating launch that delivers the promised independent experience. Resolve content, channel, guest-program, and service-readiness issues before relying on broader representation to carry opening demand.",
    },
    galleryBodyTemplate: (title) =>
      `${String(title || "").replace(/^[^—]+—\s*/, "") || "Property gallery view"} illustrates the guest-facing product owners should underwrite for Preferred Hotels & Resorts—use as visual context for affiliation fit and experience intensity, not as a performance claim.`,
    chipScrubs: [
      {
        match: /conversion-friendly/i,
        replaceWith: "independent-identity flexible",
      },
    ],
  },
  "quality-inn": {
    brandId: "recd8o4k1JddhkRWW",
    byRecordId: {
      recqLW7iLzcQLyCK3:
        "Quality Inn in Santo Domingo is a midscale Choice flag in a Caribbean capital corridor—useful when owners compare meetings-capable urban midscale product, Choice systems participation, and conversion intensity. Validate capital envelope, competitive set, and operating model against the actual asset before treating this listing as performance guidance.\n\nAnnouncement: Choice Hotels consumer listing (Santo Domingo Quality Inn)\nhttps://www.choicehotels.com/dominican-republic/santo-domingo/quality-inn-hotels/do002",
      reczDvJLogWDAykwx:
        "Quality Inn on the San José corridor is a midscale Choice example for owners evaluating airport or city-access midscale product in Central America. Use it to compare conversion fit, guest-experience intensity, and systems participation—not as a rate or occupancy proxy—before modeling affiliation outcomes.\n\nAnnouncement: Choice Hotels consumer listing (San José Quality Inn)\nhttps://www.choicehotels.com/costa-rica/san-jose/quality-inn-hotels/cr010",
    },
  },
});

async function fetchBrand(brandId) {
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
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed: ${brandId}`);
  return res.payload.brand;
}

async function airtablePatch({ baseId, apiKey, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`PATCH ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

function findRow(brand, pred) {
  return (brand.brandExplorer?.blocks || []).find(pred);
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const patches = [];

  // 1) FDD standards scrub
  for (const t of FDD_TARGETS) {
    const brand = await fetchBrand(t.brandId);
    const row = findRow(
      brand,
      (r) =>
        r.slotKey === "operations.standards_philosophy" &&
        typeof r.body === "string" &&
        /\bFDD\b/i.test(r.body)
    );
    if (!row) continue;
    const scrub = scrubResidualOwnerFacingCopy(row.body, {
      slotKey: row.slotKey,
      brandSlug: t.slug,
    });
    if (scrub.changed && scrub.clean) {
      patches.push({ slug: t.slug, recordId: row.recordId, fields: { Body: scrub.after }, kind: "fdd" });
    }
  }

  // 2) Modal fills
  for (const m of MODAL_FILLS) {
    patches.push({ slug: m.slug, recordId: m.recordId, fields: m.fields, kind: "modal" });
  }

  // 3) Comfort empty bodies
  for (const c of COMFORT_EMPTY_BODIES) {
    patches.push({
      slug: "comfort-inn-suites",
      recordId: c.recordId,
      fields: { Body: c.body },
      kind: "empty_body",
    });
  }

  // 4) Lane4 / quality content thicken + gallery bodies
  for (const [slug, cfg] of Object.entries(CONTENT_THICKEN)) {
    const brand = await fetchBrand(cfg.brandId);
    const owner = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);

    if (cfg.bySlot) {
      for (const [slotKey, body] of Object.entries(cfg.bySlot)) {
        const row = owner.find((r) => r.slotKey === slotKey);
        if (!row) {
          console.warn(`missing slot ${slug} ${slotKey}`);
          continue;
        }
        patches.push({ slug, recordId: row.recordId, fields: { Body: body }, kind: "thicken" });
      }
    }

    if (cfg.byRecordId) {
      for (const [recordId, body] of Object.entries(cfg.byRecordId)) {
        patches.push({ slug, recordId, fields: { Body: body }, kind: "thicken" });
      }
    }

    if (cfg.galleryBodyTemplate) {
      for (const row of owner.filter((r) => /^materials\.gallery\.\d+$/.test(r.slotKey || ""))) {
        if (String(row.body || "").trim()) continue;
        patches.push({
          slug,
          recordId: row.recordId,
          fields: { Body: cfg.galleryBodyTemplate(row.title || "Gallery image") },
          kind: "gallery_body",
        });
      }
    }

    if (cfg.chipScrubs) {
      for (const row of owner) {
        for (const field of ["body", "title", "caseSummaryTags"]) {
          const val = row[field];
          if (typeof val !== "string" || !val) continue;
          let next = val;
          for (const scrub of cfg.chipScrubs) {
            if (scrub.match.test(next)) next = next.replace(scrub.match, scrub.replaceWith);
          }
          if (next !== val) {
            const airtableField =
              field === "body" ? "Body" : field === "title" ? "Title" : "Case Summary Tags";
            patches.push({
              slug,
              recordId: row.recordId,
              fields: { [airtableField]: next },
              kind: "chip_scrub",
            });
          }
        }
      }
    }
  }

  // Audience placeholder for BW Premier if still blank in Brand Basics-driven card:
  // suppress via presentation row if one exists for positioning.audience
  {
    const brand = await fetchBrand("recwXZ5gVZ8ZH8ekA");
    const row = findRow(brand, (r) => r.slotKey === "positioning.audience");
    if (row && !String(row.body || "").trim()) {
      patches.push({
        slug: "bw-premier-collection",
        recordId: row.recordId,
        fields: {
          Body: "Not publicly disclosed — confirm guest psychographics and positioning language with BWH development for the specific asset.",
        },
        kind: "audience_fill",
      });
    }
  }
  {
    const brand = await fetchBrand("recdeh1NsP4gjrv80");
    const row = findRow(brand, (r) => r.slotKey === "positioning.audience");
    if (row && !String(row.body || "").trim()) {
      patches.push({
        slug: "bw-signature-collection",
        recordId: row.recordId,
        fields: {
          Body: "Not publicly disclosed — confirm guest psychographics and positioning language with BWH development for the specific asset.",
        },
        kind: "audience_fill",
      });
    }
  }

  console.log(`[residual-24] dryRun=${!APPLY} patches=${patches.length}`);
  const byKind = {};
  for (const p of patches) byKind[p.kind] = (byKind[p.kind] || 0) + 1;
  console.log(byKind);
  for (const p of patches) {
    console.log(`  ${p.kind} ${p.slug} ${p.recordId} ${Object.keys(p.fields).join(",")}`);
  }

  if (!APPLY) {
    console.log("Re-run with --apply to write.");
    return;
  }

  for (const p of patches) {
    await airtablePatch({ baseId, apiKey, recordId: p.recordId, fields: p.fields });
    console.log(`  PATCHED ${p.kind} ${p.slug} ${p.recordId}`);
  }

  const checkSlugs = [
    ...FDD_TARGETS.map((t) => t.slug),
    "comfort-inn-suites",
    "bw-premier-collection",
    "bw-signature-collection",
    "preferred-hotels-and-resorts",
    "quality-inn",
    "autograph-collection",
    "handwritten-collection",
  ];
  console.log("\nPost-check (display / forbid / PVQL copy gates):");
  for (const slug of [...new Set(checkSlugs)]) {
    const row = await evaluateBrandPublicVisibility(slug);
    const copyFails = (row.failures || []).filter((f) =>
      /raw_url|forbidden_owner_facing|generic_copy/i.test(f)
    );
    console.log(
      `  ${slug}: pf=${row.publicFullProfile} display=${row.publicDisplayState} fails=${(row.failures || []).join(",") || "-"} copyFails=${copyFails.join(",") || "-"}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
