/**
 * Fairfield by Marriott — pilot content remediation (fixture-only).
 * Fixes grammar, boilerplate, psychographics, peer prose, and softens
 * over-strong operating claims per ChatGPT QA feedback.
 */

import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";
import { FAIRFIELD_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-fairfield.js";
import { buildFairfieldOpeningsFixtureRows } from "./brand-explorer-fairfield-openings-fixture-rows.js";

const BOILERPLATE_SENTENCE =
  "Keep Fairfield by Marriott product and service responsibilities clear among owner, operator, and brand teams so the efficient upper-midscale Marriott select-service rooms brand stays deliverable after affiliation and through ongoing operations.";

const PEER_COMPARISON_LINE =
  "Underwrite Fairfield as a lean Bonvoy select-service rooms product—never as Courtyard F&B/meetings intensity or an all-suite SpringHill / Residence Inn stay.";

export const FAIRFIELD_CONTENT_REMEDIATION_VERSION = "fairfield-content-remediation-v1";

const POSITIONING_PHRASE = "an efficient upper-midscale Marriott select-service rooms brand";

const SLOT_BODY_OVERRIDES = Object.freeze({
  "Brand Positioning":
    "Fairfield by Marriott is positioned as an efficient upper-midscale Marriott select-service rooms brand focused on dependable transient lodging and Bonvoy consistency. Owner underwriting should prioritize brand-specific fit, demand alignment, and operating-model readiness over Marriott International corporate generalizations or sibling-brand assumptions.",

  "Guest Psychographics Description":
    "Fairfield by Marriott serves practical transient guests—business travelers, families, and leisure visitors who want reliable rooms, straightforward value, and Bonvoy consistency rather than lifestyle-hotel personality. Marriott's brand positioning emphasizes simplicity and select-service delivery for both business and leisure trips. Owners should validate local demand concentration across Corporate / Business, Leisure, and Family segments before assuming portfolio guest averages apply to the asset under review.",

  "footprint.geo_intro":
    "Use named CALA operating examples first for Fairfield by Marriott; label other geographies International Reference. Region cards below summarize owner-relevant geography with explicit CALA or International Reference labeling so sponsors can separate property-backed proof from directional international context.",

  "footprint.growth_fit":
    "Best growth fit: assets ready for an efficient upper-midscale Marriott select-service rooms brand on suburban, airport, highway, or secondary-urban corridors. Weaker fit: hotels whose demand, product scope, or capital program align more credibly to Courtyard by Marriott or cannot sustain Fairfield's rooms-first guest promise.",

  "footprint.growth_editorial":
    "Fairfield by Marriott growth should be evaluated through brand-specific positioning, not Marriott International expansion targets alone. Treat Fairfield as a lean Bonvoy select-service rooms lane—distinct from Courtyard's fuller public-space program and from SpringHill or Residence Inn suite products. CALA markets (Cancún, Santo Domingo, Mexico City, Bogotá) provide property-level proof points for this posture.",

  "footprint.growth_themes":
    "- Anchor underwriting on Fairfield's rooms-first select-service prototype—not Courtyard meeting depth or suite-living products.\n- Favor suburban, airport, highway, and secondary-urban sites where operating cost discipline and consistent rooms product carry the case.\n- Maintain brand-specific positioning discipline across new markets and conversions.\n- Preserve peer separation from Courtyard by Marriott, Four Points by Sheraton, SpringHill Suites by Marriott, and Residence Inn by Marriott in each market entry.",

  "operations.model.typical_ownership":
    "Owners typically pursue Fairfield when they want Bonvoy distribution with a leaner select-service operating profile than Courtyard by Marriott on the same corridor—provided the asset can deliver Fairfield's rooms-first guest promise and meet Marriott platform obligations.",

  "operations.operator_compat.summary":
    "The operator should demonstrate select-service execution and Marriott platform discipline—either on Fairfield by Marriott or a comparable upper-midscale rooms brand—while maintaining Bonvoy, systems, and quality obligations through opening and stabilization.",

  "operations.standards_philosophy":
    "Standards should protect Fairfield by Marriott brand identity while remaining executable for the specific asset, market, and operator. In Dealality's peer comparison, Courtyard typically carries heavier F&B and meeting scope than Fairfield; align PIP and lifecycle capital to the asset's actual product lane.",

  "overview.differentiators.identity":
    "- Fairfield by Marriott guest promise is tied to a clear, efficient upper-midscale Marriott select-service rooms brand rather than a generic Marriott corporate story\n- Keep Fairfield by Marriott distinct from Courtyard by Marriott during underwriting\n- Property expression must support Fairfield by Marriott positioning in rooms and public space\n- Local market demand must match the brand's audience logic (Corporate / Business, Leisure, Family) before affiliation",

  "overview.bestAt.1":
    "Fairfield by Marriott is best at delivering an efficient upper-midscale Marriott select-service rooms brand when the asset and operator can sustain that promise consistently after opening or conversion, with clear separation from Courtyard by Marriott.",

  "overview.development_model":
    "Fairfield by Marriott owner thesis: prioritize rooms-first select-service economics on prototype-ready suburban, airport, highway, and secondary-urban sites. Keep public-space and F&B scope aligned to Fairfield's lane rather than importing Courtyard bistro or meeting-room assumptions.",

  "overview.featured_application":
    "- No dated momentum locked — research before any momentum write.\n- Verify property URLs before image harvest.\n- Do not import Courtyard F&B/meeting intensity into Fairfield by Marriott owner copy.\n- Do not import Residence Inn extended-stay or SpringHill all-suite language into Fairfield copy.\n- Do not import Four Points Sheraton-family framing into Fairfield copy.\n- Keep claims market-specific and tied to the asset under review.\n- Compare Fairfield by Marriott against Courtyard by Marriott, Four Points by Sheraton, SpringHill Suites by Marriott, and Residence Inn by Marriott on operating complexity and asset fit before affiliation.",

  "overview.portfolio_context":
    "Parent/platform context (Marriott International / Marriott Bonvoy) should remain clearly labeled and subordinate to brand-specific positioning proof. Evaluate Fairfield by Marriott on its own operating thesis rather than assuming portfolio averages apply. Dealality peer note: Courtyard typically carries heavier F&B and meeting programming than Fairfield.",

  "overview.proof.3":
    "Fairfield by Marriott is distinguished from Courtyard by Marriott, Four Points by Sheraton, SpringHill Suites by Marriott, and Residence Inn by Marriott by product scope, guest promise, and asset requirements. Courtyard generally underwrites with more F&B and meeting capacity; SpringHill and Residence Inn are suite or extended-stay lanes—not Fairfield substitutes.",

  "overview.scenario.2":
    "Owner value rises on airport, suburban, and highway sites where Fairfield's lean select-service model matches transient demand. Size public space and F&B to Fairfield's prototype rather than Four Points Sheraton-family framing or SpringHill suite expectations. Capital returns hold when the asset stays a practical rooms product rather than a fuller-service Courtyard box.",

  "overview.typical_use_case":
    "Best fit: rooms-focused select-service assets on suburban, airport, highway, or secondary-urban corridors where Bonvoy distribution and operating cost discipline matter. Evaluate demand concentration, product condition, operator capability, and competitive set together before affiliation.",

  "standards.intro":
    "Fairfield by Marriott standards should support an efficient upper-midscale Marriott select-service rooms brand alongside Marriott International platform participation. Current acceptance, product, technology, training, and quality details must be aligned for the specific asset and market.",

  "standards.requirement":
    "The property should present a credible Fairfield by Marriott experience through rooms, public spaces, arrival, and overall design aligned to an efficient upper-midscale Marriott select-service rooms brand.",

  "valueOwners.overview":
    "Owners evaluating Fairfield by Marriott are buying an efficient upper-midscale Marriott select-service rooms brand backed by Marriott International distribution and Marriott Bonvoy. The practical case is rooms-first select-service economics with prototype discipline—not a Courtyard-style bistro/meeting program or an all-suite SpringHill / Residence Inn stay model.",

  "valueOwners.scenario.2":
    "Airport and highway nodes create Fairfield value when transient demand needs a consistent rooms product and owners size F&B to select-service scope rather than fuller-service or suite-living programs.",
});

const MULTI_ROW_OVERRIDES = Object.freeze({
  "insight.similar": Object.freeze({
    700: {
      title: "Similar Brands",
      body: "Courtyard by Marriott sits adjacent in the Marriott portfolio but typically carries heavier F&B and meeting programming than Fairfield's rooms-first select-service lane. Compare operating complexity, capital scope, and guest promise—not parent-company affiliation alone.",
    },
    701: {
      title: "Similar Brands",
      body: "Four Points by Sheraton competes in midscale select-service under a Sheraton-family identity. SpringHill Suites and Residence Inn are suite or extended-stay products; they should not substitute for Fairfield's king-room prototype during diligence.",
    },
  }),

  "economics.opening.step.2": {
    title: "Design & Standards",
    body: "Sequence design, systems integration, staff training, and go-live readiness with clear owner and operator responsibilities defined for each phase. Fairfield pre-opening should follow brand-specific milestones rather than generic platform timelines from Marriott International. Lock responsibility matrices for product, service, and capital decisions before construction or conversion spend accelerates.",
  },
  "economics.opening.step.3": {
    title: "Pre-Opening Planning",
    body: "Build the plan around systems, Marriott Bonvoy readiness, training, staffing, sales, and operating procedures with clear owner/operator/brand responsibilities. Align timing against product completion so Fairfield can open with a credible guest promise. Validate that public-space scope, staffing plans, and F&B programming match Fairfield's select-service prototype—not a sibling brand's operating model.",
  },
  "economics.opening.step.4": {
    title: "Opening Support",
    body: "Coordinate launch communications, systems go-live, quality readiness, and service recovery with operator and brand contacts while keeping the Fairfield story prominent. Establish escalation paths for the first operating weeks and confirm day-one service standards against the approved prototype checklist.",
  },
  "economics.opening.step.5": {
    title: "Stabilization",
    body: "Use the stabilized period to refine service and channel strategy against actual guest feedback for Fairfield. Reassess capital and staffing through early performance and adjust operator execution before assuming affiliation value is fully realized.",
  },

  "valueOwners.lifecycle.2": {
    title: "Conversion Design",
    body: "Translate Fairfield positioning into rooms, public space, service, and technology workstreams that match the intended select-service prototype. Sequence design, systems, and capital milestones with financing and operator decisions so conversion scope stays underwritable and Fairfield-specific.",
  },
  "valueOwners.lifecycle.3": {
    title: "Pre-Opening",
    body: "Coordinate Marriott Bonvoy readiness, training, staffing, and commercial launch with product completion for Fairfield. Clarify owner, operator, and brand responsibilities before opening so the guest promise is deliverable from day one and public-space scope stays aligned to the approved prototype.",
  },
  "valueOwners.lifecycle.4": {
    title: "Opening",
    body: "Launch with the Fairfield guest promise consistently expressed across service and channels while platform systems stabilize. Keep escalation paths clear for the first operating weeks and verify quality readiness against the approved prototype and service standards.",
  },
  "valueOwners.lifecycle.6": {
    title: "Ongoing",
    body: "Maintain Fairfield product discipline while meeting applicable platform quality and commercial obligations. Revisit capital and operator alignment as the hotel stabilizes so Fairfield remains credible versus peer alternatives such as Courtyard by Marriott on product scope and operating complexity.",
  },
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function applyGlobalTextFixes(text) {
  let out = nz(text);
  if (!out) return out;
  out = out.replace(/\ba efficient\b/gi, "an efficient");
  out = out.replace(/International Reference\.\./g, "International Reference.");
  out = out.replace(/(\w)\.\.(?=\s|$)/g, "$1.");
  out = out.replace(/[ \t]{2,}/g, " ");
  if (out.includes(BOILERPLATE_SENTENCE)) {
    out = out.replace(BOILERPLATE_SENTENCE, "").replace(/[ \t]{2,}/g, " ").trim();
  }
  if (out.includes(PEER_COMPARISON_LINE)) {
    out = out.replace(PEER_COMPARISON_LINE, "").replace(/[ \t]{2,}/g, " ").trim();
  }
  return out;
}

function rowKey(row) {
  return `${row.slotKey}::${row.sort ?? 0}`;
}

/**
 * @param {Array<object>} rows
 * @returns {{ rows: Array<object>, changed: Array<{slotKey:string,sort:number,field:string}> }}
 */
export function remediateFairfieldFixtureRows(rows) {
  const changed = [];
  const next = rows.map((row) => {
    const sort = row.sort ?? 0;
    let copy = { ...row };

    const singleOverride = SLOT_BODY_OVERRIDES[copy.slotKey];
    if (singleOverride && copy.slotKey !== "standards.requirement") {
      if (copy.body !== singleOverride) {
        changed.push({ slotKey: copy.slotKey, sort, field: "body" });
        copy.body = singleOverride;
      }
    }

    const multiKey = copy.slotKey;
    const multiSortOverrides = MULTI_ROW_OVERRIDES[multiKey];
    if (multiSortOverrides) {
      const bySort = multiSortOverrides[sort] || multiSortOverrides;
      if (bySort && typeof bySort === "object" && (bySort.body || bySort.title)) {
        if (bySort.body && copy.body !== bySort.body) {
          changed.push({ slotKey: copy.slotKey, sort, field: "body" });
          copy.body = bySort.body;
        }
        if (bySort.title && copy.title !== bySort.title) {
          changed.push({ slotKey: copy.slotKey, sort, field: "title" });
          copy.title = bySort.title;
        }
      }
    }

    // standards.requirement is multi-row — only override sort 601 body
    if (copy.slotKey === "standards.requirement" && sort === 601 && singleOverride) {
      if (copy.body !== singleOverride) {
        changed.push({ slotKey: copy.slotKey, sort, field: "body" });
        copy.body = singleOverride;
      }
    }

    // overview.scenario.3 — remove boilerplate ending
    if (copy.slotKey === "overview.scenario.3") {
      const cleaned =
        "Fairfield conversions and new builds create confidence when owners keep prototype and brand standards Fairfield-specific. Value erodes if diligence borrows Residence Inn extended-stay proof, SpringHill all-suite language, or Courtyard meetings assumptions. Compare against those siblings on product scope, capital intensity, and operating complexity before locking affiliation—not after conversion spend is committed.";
      if (copy.body !== cleaned) {
        changed.push({ slotKey: copy.slotKey, sort, field: "body" });
        copy.body = cleaned;
      }
    }

    // caseSummaryOverview on featured_application
    if (copy.slotKey === "overview.featured_application" && copy.caseSummaryOverview) {
      const cs =
        "Featured path for hotels evaluating Fairfield by Marriott as an efficient upper-midscale Marriott select-service rooms brand.";
      if (copy.caseSummaryOverview !== cs) {
        changed.push({ slotKey: copy.slotKey, sort, field: "caseSummaryOverview" });
        copy.caseSummaryOverview = cs;
      }
    }

    // Openings — rebuild from canonical builder (title, body paragraphs, case summary)
    if (copy.slotKey === "footprint.openings") {
      const fresh = buildFairfieldOpeningsFixtureRows().find((r) => r.sort === sort);
      if (fresh) {
        for (const field of [
          "title",
          "body",
          "imageUrl",
          "caseSummaryOverview",
          "caseSummaryBrandRelevance",
          "caseSummaryOwnerObjective",
          "caseSummaryInterpretation",
          "caseSummaryTags",
        ]) {
          if (fresh[field] && copy[field] !== fresh[field]) {
            changed.push({ slotKey: copy.slotKey, sort, field });
            copy[field] = fresh[field];
          }
        }
      }
    }

    // Momentum — restore structured date line + URL paragraph breaks
    if (copy.slotKey === "footprint.momentum") {
      const card = FAIRFIELD_SECTION_PATTERN_PARITY_CONTENT.momentumCards.find(
        (c) => c.sort === sort
      );
      if (card) {
        const rebuilt = buildMomentumBody({
          dateLine: card.dateLine,
          summary: card.summary,
          sourceUrl: card.url,
        });
        if (rebuilt !== copy.body) {
          changed.push({ slotKey: copy.slotKey, sort, field: "body" });
          copy.body = rebuilt;
        }
      }
    }

    // Skip global inline whitespace fixes on structured multi-paragraph openings bodies.
    const skipGlobalFields =
      copy.slotKey === "footprint.openings"
        ? new Set(["body"])
        : copy.slotKey === "footprint.momentum"
          ? new Set(["body"])
          : new Set();

    for (const field of [
      "body",
      "title",
      "caseSummaryOverview",
      "caseSummaryBrandRelevance",
      "caseSummaryOwnerObjective",
      "caseSummaryInterpretation",
    ]) {
      if (skipGlobalFields.has(field)) continue;
      if (typeof copy[field] === "string") {
        const fixed = applyGlobalTextFixes(copy[field]);
        if (fixed !== copy[field]) {
          if (!changed.some((c) => c.slotKey === copy.slotKey && c.sort === sort && c.field === field)) {
            changed.push({ slotKey: copy.slotKey, sort, field });
          }
          copy[field] = fixed;
        }
      }
    }

    return copy;
  });

  return { rows: next, changed };
}

export { POSITIONING_PHRASE, BOILERPLATE_SENTENCE };
