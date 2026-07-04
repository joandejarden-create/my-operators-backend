/**
 * Generate Radisson Blu (Choice) tab fixtures (economics merge, footprint geo, etc.).
 * Run: node scripts/build-radisson-blu-tab-fixtures.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BRAND = "Radisson Blu (Choice)";

function readJson(rel) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
}

function writeJson(rel, data) {
  const p = path.join(ROOT, rel);
  fs.writeFileSync(p, JSON.stringify(data, null, 2) + "\n");
  console.log("Wrote", rel, `(${data.rows.length} rows)`);
}

function bluText(s) {
  if (!s) return s;
  return (
    s
      .replace(/Radisson Blu Blu/g, "Radisson Blu")
      .replace(/kit-of-parts/gi, "design-forward prototype")
      .replace(/Kit-of-parts/g, "Design-forward prototype")
      .replace(/upscale conversion/gi, "upper-upscale conversion")
      .replace(/upscale full-service/gi, "upper-upscale full-service")
      .replace(/upscale box/gi, "upper-upscale box")
      .replace(/upscale consumers/gi, "upper-upscale consumers")
      .replace(/upscale positioning/gi, "upper-upscale positioning")
      .replace(/upscale or/gi, "upper-upscale or")
      .replace(/upscale retail/gi, "upper-upscale retail")
      .replace(/upscale flag/gi, "upper-upscale flag")
      .replace(/upscale standards/gi, "upper-upscale standards")
      .replace(/upscale ADR/gi, "upper-upscale ADR")
      .replace(/mainstream upscale/gi, "upper-upscale")
      .replace(/core Radisson/gi, "core Radisson (sibling upscale flag)")
      .replace(/this brand/gi, "Radisson Blu")
      .replace(/Brands in this tier/g, "Radisson Blu under Choice")
      .replace(/affiliation with this brand/gi, "affiliation with Radisson Blu")
      .replace(/for this brand/gi, "for Radisson Blu")
      .replace(/A Century Young/g, "Think in Black & White Blu")
      .replace(/Charming simplicity/g, "Enticing Moments")
      .replace(/Contemporary classic/g, "Nordic Nouveau")
      .replace(/Gracious hospitality/g, "Curatorial Warmth")
  );
}

function mapRows(rows, extra = (r) => r) {
  return rows.map((r) => {
    const out = {
      ...r,
      title: r.title ? bluText(r.title) : r.title,
      body: r.body ? bluText(r.body) : r.body,
    };
    if (r.caseSummaryOverview) out.caseSummaryOverview = bluText(r.caseSummaryOverview);
    if (r.caseSummaryOwnerObjective) out.caseSummaryOwnerObjective = bluText(r.caseSummaryOwnerObjective);
    if (r.caseSummaryBrandRelevance) out.caseSummaryBrandRelevance = bluText(r.caseSummaryBrandRelevance);
    if (r.caseSummaryInterpretation) out.caseSummaryInterpretation = bluText(r.caseSummaryInterpretation);
    if (r.caseSummaryTags) r.caseSummaryTags;
    return extra(out);
  });
}

function mergeEconomicsRows() {
  const v2 = readJson("fixtures/brand-explorer-presentation-economics-v2.json").rows;
  const ob = readJson("fixtures/brand-explorer-presentation-economics-obligations.json").rows;
  const seen = new Set();
  const merged = [];
  const key = (r) => `${r.slotKey}\0${r.title || ""}`;
  for (const r of [...v2, ...ob]) {
    const k = key(r);
    if (seen.has(k)) continue;
    seen.add(k);
    merged.push(r);
  }
  return merged;
}

function economicsOverrides(row) {
  const sk = row.slotKey;
  if (sk === "economics.model") {
    row.body =
      "Radisson Blu affiliation trades a recurring upper-upscale fee stack and program participation for Choice distribution, Choice Privileges retail, design-forward standards, and revenue support. Owners fund Nordic Nouveau–level FF&E, public-space investment, conversion or new-build scope, and working capital through ramp—negotiating which line items are fixed versus deal-dependent in CALA and gateway markets.";
  }
  if (sk === "economics.opening.step.2") {
    row.body =
      "Design-forward prototype and upper-upscale standards review—guestroom casegoods, baths, signature F&B, and public-space narrative before major spend.";
  }
  if (sk === "economics.opening.process") {
    row.body =
      "Typical Radisson Blu path: feasibility for upper-upscale fit (urban gateway, resort, or CALA icon), design and prototype approval with realistic FF&E scope, pre-opening systems and gallery-curator service training, opening QA, then stabilization. Third-party operators with upper-upscale depth often lead opening while Choice / Alpha Brand Studios approves milestones.";
  }
  if (sk === "economics.cash.preopening") {
    row.body =
      "Owner typically funds: Upper-upscale FF&E and public-space alignment, technology cutover, working capital through opening, and application or training cash outlays—often heavier than core Radisson conversion.\n\nBrand typically provides: Design and standards review, opening playbooks, pre-opening support, and milestone QA—not day-to-day operating spend.";
  }
  if (sk === "economics.lifecycle.preopening") row.body = "Heavy";
  if (sk === "economics.lifecycle.renewal") row.body = "Heavy when upper-upscale PIP or re-licensing is required";
  if (sk === "economics.kpi.performance") {
    row.body = "Performance and QA themes common in upper-upscale franchise agreements";
  }
  if (sk === "economics.fee.join") {
    row.body =
      "Application and entry fees; training and opening support; initial / franchise license fee; technology implementation; plan review and design inspection for upper-upscale scope. Basis varies by keys, CALA market tier, and conversion versus new-build—confirm in the FDD and LOI.";
  }
  return row;
}

// --- Economics ---
const econRows = mapRows(mergeEconomicsRows(), economicsOverrides);
writeJson("fixtures/brand-explorer-presentation-economics-radisson-blu.json", {
  targetBrandBasicsName: BRAND,
  brandNameFallback: BRAND,
  instructions:
    'Economics tab (v2 + legacy KPI slots). Apply: npm run apply-brand-explorer-presentation -- --brand-name "Radisson Blu (Choice)" --fixture fixtures/brand-explorer-presentation-economics-radisson-blu.json --replace-slot-prefix economics.',
  rows: econRows,
});

// --- Portfolio / compliance / similar ---
writeJson("fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json", {
  targetBrandBasicsName: BRAND,
  brandNameFallback: BRAND,
  instructions:
    'Footprint portfolio mix + compliance + insight.similar. Apply with --replace-slot-prefix footprint.portfolio_mix (partial) — use apply-radisson-blu-choice-all-fixtures.mjs',
  rows: [
    { slotKey: "footprint.portfolio_mix", title: "Urban Gateway", body: "High", sort: 1 },
    { slotKey: "footprint.portfolio_mix", title: "Resort / Leisure", body: "High", sort: 2 },
    { slotKey: "footprint.portfolio_mix", title: "CALA Metro", body: "High", sort: 3 },
    { slotKey: "footprint.portfolio_mix", title: "Conversion / Adaptive Reuse", body: "Moderate", sort: 4 },
    { slotKey: "footprint.portfolio_mix", title: "New-Build Prototype", body: "Moderate", sort: 5 },
    {
      slotKey: "operations.compliance.qa_cadence",
      title: "",
      body: "Recurring upper-upscale property assessments and brand QA—Nordic Nouveau and service rituals, not ad hoc reviews",
      sort: 0,
    },
    {
      slotKey: "operations.compliance.training_rigor",
      title: "",
      body: "High—gallery-curator service, guest experience, and safety programs through Choice University and brand opening discipline",
      sort: 0,
    },
    {
      slotKey: "operations.compliance.reporting",
      title: "",
      body: "Consistent financial, quality, and franchise reporting through mandated management and CRS tools",
      sort: 0,
    },
    {
      slotKey: "operations.compliance.brand_interaction",
      title: "",
      body: "Active Alpha Brand Studios / CALA development support for upper-upscale openings—structured milestones, not daily micromanagement on stabilized assets",
      sort: 0,
    },
    { slotKey: "insight.similar", title: "W Hotels", body: "(Marriott · design-led upper-upscale)", sort: 1 },
    { slotKey: "insight.similar", title: "Kimpton", body: "(IHG · boutique upper-upscale)", sort: 2 },
    { slotKey: "insight.similar", title: "Le Méridien", body: "(Marriott · European design heritage)", sort: 3 },
    { slotKey: "insight.similar", title: "Andaz", body: "(Hyatt · lifestyle upper-upscale)", sort: 4 },
  ],
});

// --- Footprint geo & growth (full tab narrative) ---
const geoSrc = readJson("fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json").rows;
writeJson("fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", {
  targetBrandBasicsName: BRAND,
  brandNameFallback: BRAND,
  instructions: "Footprint geo/growth/editorial slots. Apply via apply-radisson-blu-choice-all-fixtures.mjs",
  rows: mapRows(geoSrc, (row) => {
    if (row.slotKey === "footprint.geo_intro") {
      row.body =
        "Radisson Blu under Choice Hotels is an upper-upscale flag with a CALA-forward Americas story—Argentina debut (Bariloche), Brazil gateways (Belo Horizonte Savassi, São Paulo), Chile (Santiago), and Caribbean leisure (Aruba)—alongside selective U.S. and Canada icons. Owner conversations should center on Inspired Professional guests, Nordic Nouveau investment, and Choice Privileges retail—not core Radisson conversion economics.";
    }
    if (row.slotKey === "footprint.region.cala") {
      row.body =
        "Strong CALA relevance\n\nChoice-affiliated Radisson Blu hotels in Brazil, Chile, Argentina, and Aruba illustrate where Alpha Brand Studios leans: upper-upscale urban boxes, resort leisure, and design-forward openings—not mainstream Radisson CALA conversion plays.";
    }
    if (row.slotKey === "footprint.region.am") {
      row.body =
        "Selective Americas scale\n\nU.S. and Canada properties anchor brand recognition for some owners; CALA development narratives should lead with São Paulo, Santiago, Belo Horizonte, Bariloche, and Aruba proof points unless the deal is explicitly a domestic gateway.";
    }
    if (row.slotKey === "footprint.growth_themes") {
      row.body =
        "CALA urban gateway upper-upscale\nResort and leisure destinations (Aruba, Bariloche)\nDesign-forward conversion and adaptive reuse\nMeetings-capable metro boxes (São Paulo, Santiago)\nSecondary-city upper-upscale (Belo Horizonte Savassi)\nInspired Professional compression markets";
    }
    if (row.slotKey === "footprint.growth_editorial") {
      row.body =
        "Radisson Blu compounds when owners need recognizable upper-upscale design heritage, memorable public spaces, and Choice distribution—not a soft brand with ambiguous QA. Use CALA hotel examples as directional proof of network positioning; individual hotel economics remain deal-specific.";
    }
    if (row.slotKey === "footprint.growth_fit") {
      row.body =
        "top urban and resort CALA destinations\nowners who can fund upper-upscale FF&E and F&B theater\nmeetings and group where the prototype supports it\noperators with gallery-curator service depth\nmarkets where core Radisson economics are too light for the asset";
    }
    if (row.slotKey === "footprint.editorial") {
      row.body =
        "Radisson Blu’s footprint under Choice reads “global upper-upscale with CALA momentum”: design lineage (Arne Jacobsen heritage), Nordic Nouveau discipline, and selective Americas hotels—not a mass conversion flag. It shows best where guests reject boring big-box experiences and owners can deliver upper-upscale casegoods and public-space investment.";
    }
    if (row.slotKey === "footprint.editorial_bullets") {
      row.body =
        "strongest CALA examples: Bariloche, São Paulo, Belo Horizonte Savassi, Plaza El Bosque Santiago, Aruba\nupper-upscale urban and resort—not core Radisson beach or airport conversion stories\nglobal RHG scale supports recognition; Choice-affiliated Americas count is selective\nmore compelling when Inspired Professional demand and FF&E budget align";
    }
    return row;
  }),
});

// --- Momentum ---
writeJson("fixtures/brand-explorer-presentation-radisson-blu-footprint-momentum.json", {
  targetBrandBasicsName: BRAND,
  brandNameFallback: BRAND,
  instructions: "Recent momentum timeline — CALA Blu hotels",
  rows: [
    {
      slotKey: "footprint.momentum_label",
      title: "",
      body: "Radisson Blu CALA · Choice-affiliated openings and portfolio highlights",
      sort: 0,
    },
    {
      slotKey: "footprint.momentum",
      title: "Argentina debut on Lake Nahuel Huapi",
      body: "Sep 2025\n\nChoice Hotels highlighted the opening of Radisson Blu Bariloche—upper-upscale lakeside positioning in San Carlos de Bariloche and the brand’s Argentina entry under Choice.\n\nhttps://media.choicehotels.com/2025-09-04-Choice-Hotels-International-Debuts-in-Argentina-with-the-Opening-of-Radisson-Blu-Bariloche",
      sort: 1,
    },
    {
      slotKey: "footprint.momentum",
      title: "São Paulo upper-upscale gateway",
      body: "Portfolio\n\nRadisson Blu São Paulo serves Brazil’s primary business capital—meetings, dining, and Inspired Professional positioning in the Cidade Jardim corridor.\n\nhttps://www.choicehotels.com/sao-paulo/sao-paulo/radisson-blu-hotels",
      sort: 2,
    },
    {
      slotKey: "footprint.momentum",
      title: "Savassi district presence in Belo Horizonte",
      body: "Portfolio\n\nRadisson Blu Belo Horizonte, Savassi anchors upper-upscale corporate and leisure demand in Minas Gerais’ largest metro.\n\nhttps://www.choicehotels.com/minas-gerais/belo-horizonte/radisson-blu-hotels/br154",
      sort: 3,
    },
    {
      slotKey: "footprint.momentum",
      title: "Santiago financial-corridor upper-upscale",
      body: "Portfolio\n\nRadisson Blu Plaza El Bosque Santiago supports group and transient demand in Las Condes—Chile capital proof point for urban Blu.\n\nhttps://www.choicehotels.com/chile/santiago/radisson-blu-hotels",
      sort: 4,
    },
    {
      slotKey: "footprint.momentum",
      title: "Caribbean resort compression in Aruba",
      body: "Portfolio\n\nRadisson Blu Aruba on Palm Beach illustrates resort-format upper-upscale leisure in the Caribbean CALA corridor.\n\nhttps://www.choicehotels.com/aruba/palm-beach/radisson-blu-hotels/aw007",
      sort: 5,
    },
  ],
});

// --- Gallery ---
writeJson("fixtures/brand-explorer-presentation-radisson-blu-gallery.json", {
  targetBrandBasicsName: BRAND,
  brandNameFallback: BRAND,
  instructions: "Attach hero images in Airtable Image field per row",
  rows: [
    { slotKey: "materials.gallery.1", title: "Radisson Blu Bariloche — Public space", body: "Nordic Nouveau lobby and lake-context arrival", sort: 1 },
    { slotKey: "materials.gallery.2", title: "Radisson Blu Aruba — Rooftop & pool", body: "Palm Beach resort upper-upscale leisure", sort: 2 },
    { slotKey: "materials.gallery.3", title: "Radisson Blu São Paulo — Guest room", body: "Inspired Professional room experience", sort: 3 },
    { slotKey: "materials.gallery.4", title: "Radisson Blu Belo Horizonte — F&B", body: "Savassi dining and social space", sort: 4 },
    { slotKey: "materials.gallery.5", title: "Radisson Blu Santiago — Meetings", body: "Plaza El Bosque group and event product", sort: 5 },
    { slotKey: "materials.gallery.6", title: "Radisson Blu — Design detail", body: "Think in Black & White Blu · signature materials", sort: 6 },
  ],
});

console.log("Done. Case studies + openings are maintained in separate fixture files.");
