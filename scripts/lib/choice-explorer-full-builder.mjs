/**
 * Build full Brand Explorer presentation rows for a Choice Tier 1 brand profile.
 */
import { item19ForBrand } from "./choice-tier1-explorer-profiles.mjs";
import { overviewForBrand } from "./choice-tier1-overview-content.mjs";
import { buildProofCards } from "./choice-proof-cards.mjs";
import { sanitizeExternalCopy } from "./external-owner-voice.mjs";
import {
  flexEditorialSupplement,
  flexLevelForSlot,
} from "../../lib/brand-explorer-flexibility-levels.mjs";
import {
  commercialLeversForProfileWithImpact,
  commercialDemandScenariosForProfile,
  commercialThemesForProfile,
  formatCommercialLeverBody,
} from "../../lib/brand-explorer-commercial-levers.mjs";
import { projectImpactForBrandLever } from "./choice-tier1-commercial-impacts.mjs";
import { defaultFootprintOpeningsForBrand } from "./choice-default-footprint-openings.mjs";
import { externalSimilarPeersForBrand } from "./choice-chi-insight-similar-external.mjs";
import { portfolioContextForAirtableBrand } from "./choice-chi-portfolio-context.mjs";

/** @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p */
function row(slotKey, body, { title = "", sort = 0 } = {}) {
  return {
    slotKey,
    title: title ? sanitizeExternalCopy(title) : "",
    body: sanitizeExternalCopy(body),
    sort,
  };
}

/** @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p */
function segmentPositioningPhrase(p) {
  if (p.segment === "upscale" || p.segment === "softCollection") {
    return "design-led guest positioning and stronger rate posture";
  }
  if (p.segment === "extendedStay") {
    return "longer-stay demand resilience and efficient operating economics";
  }
  if (p.segment === "economy") {
    return "high-visibility value positioning and broad drive-to demand";
  }
  return "balanced mainstream positioning and reliable franchise demand";
}

/**
 * @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p
 * @param {"am"|"cala"|"eu"|"mea"|"apac"} region
 */
function footprintRegionBody(p, region) {
  const scale = p.scaleLabel.toLowerCase();
  const segmentEdge = segmentPositioningPhrase(p);
  if (region === "am") {
    return `Americas\n\nIn the Americas, ${p.name} is positioned as a ${scale} option with ${segmentEdge}. Regional scale, distribution visibility, and brand familiarity support a strong growth narrative for owners in competitive franchise corridors.`;
  }
  if (region === "cala") {
    return `CALA\n\nIn CALA, ${p.name} is positioned around gateway-city and resort momentum, giving owners a brand story that travels across business and leisure demand. The regional profile supports credible international positioning while staying commercially relevant to local market dynamics.`;
  }
  if (region === "eu") {
    return `Europe\n\nIn Europe, ${p.name} benefits from a globally visible brand context that reinforces quality perception and investor confidence. This regional positioning helps owners present a stronger international brand narrative for higher-consideration demand segments.`;
  }
  if (region === "mea") {
    return `MEA\n\nIn MEA, ${p.name} contributes to a broader global footprint in high-visibility destinations, supporting international brand recognition. For owners, this strengthens long-term brand credibility and portfolio depth in cross-border conversations.`;
  }
  return `APAC\n\nIn APAC, ${p.name} gains strategic exposure in fast-moving travel markets, reinforcing a forward-growth brand narrative. This regional positioning supports broader global relevance and helps owners market the asset with stronger international context.`;
}

/** @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p */
function buildCommercialRows(p) {
  const rows = [
    row(
      "commercial.intro",
      `${p.name} — ${p.tagline} ${p.positioning} Commercial strengths below show how this flag’s systems can affect demand, rate, and channel mix on your asset (illustrative; not a performance guarantee).`,
      { sort: 0 }
    ),
    row("commercial.differentiator", (p.bestAt && p.bestAt[0]) || p.developmentModel, {
      title: "Commercial edge on this brand",
      sort: 0,
    }),
    row("commercial.kpi.channels", "Brand.com · major OTAs · GDS · metasearch", {
      title: "Channels in Franchise Materials",
      sort: 0,
    }),
    row("commercial.kpi.campaigns", "Always-on + seasonal / market bursts", {
      title: "Campaign Rhythm",
      sort: 0,
    }),
    row("commercial.kpi.b2b", "RFP & account programs (where active)", {
      title: "B2B Programs",
      sort: 0,
    }),
    row("commercial.kpi.lens", "Net contribution after fees and channel costs", {
      title: "Owner Underwriting Lens",
      sort: 0,
    }),
  ];
  let sort = 0;
  for (const lever of commercialLeversForProfileWithImpact(p, projectImpactForBrandLever)) {
    rows.push(
      row(`commercial.lever.${lever.key}`, formatCommercialLeverBody(lever), {
        title: lever.title,
        sort: sort++,
      })
    );
  }
  commercialThemesForProfile(p).forEach((theme, i) => {
    rows.push(row("commercial.theme", theme, { sort: i }));
  });
  commercialDemandScenariosForProfile(p).forEach(([title, status], i) => {
    rows.push(row("commercial.demand", status, { title, sort: i }));
  });
  return rows;
}

function loyaltyProofRows(p, item19) {
  const loyaltyLine = item19.loyaltyPct
    ? `FDD Item 19 (${item19.performanceYear || "FY 2025"} sample): ~${item19.loyaltyPct}% of rooms from Choice Privileges contribution.`
    : "Confirm Choice Privileges room-mix contribution in your FDD Item 19—no published % in current disclosure for this brand.";
  const crsLine = item19.enterprisePct
    ? `~${item19.enterprisePct}% enterprise/CRS booking mix in same sample.`
    : item19.proprietaryPct
      ? `~${item19.proprietaryPct}% proprietary (non-OTA) booking mix in same sample.`
      : "Confirm CRS/enterprise or proprietary mix in FDD Item 19.";

  return [
    row("loyalty.proof", "Repeat Guest Capture", loyaltyLine, { sort: 0 }),
    row(
      "loyalty.proof",
      "Direct Channel & Member Pricing",
      `Member rates and choicehotels.com retail defend contribution versus OTA—${crsLine}`,
      { sort: 1 }
    ),
    row(
      "loyalty.proof",
      "Elite Member Value",
      "Gold (5 nights), Platinum, Diamond, and Titanium tiers with published earn and recognition—fulfillment cost hits reviews if understaffed.",
      { sort: 2 }
    ),
    row(
      "loyalty.proof",
      "Cross-Brand Traveler Flow",
      "7,100+ hotels worldwide; members earn across 20+ brands—relevant when your market sees blended corporate and leisure portfolios.",
      { sort: 3 }
    ),
    row(
      "loyalty.proof",
      "Corporate & SME Pull",
      "Choice Privileges business travel paths and partner ecosystem (hotels, resorts, airlines, cobrand)—model only what your agreement authorizes.",
      { sort: 4 }
    ),
    row(
      "loyalty.proof",
      "Campaign Scale",
      "U.S. News #1 hotel rewards program claim (consumer site); Return & Earn and seasonal campaigns—pair with property execution.",
      { sort: 5 }
    ),
  ];
}

function standardsRows(p) {
  const intro = row(
    "standards.intro",
    `${p.name} standards vary by prototype, conversion path, and agreement vintage. Use the table as a planning checklist—confirm every row in the FDD, design manual, and LOI before capital commitments.`,
    { sort: 1 }
  );
  const last = row(
    "standards.last_reviewed",
    "Confirm standards manual vintage and agreement year with Choice development",
    { sort: 0 }
  );

  /** @type {{ title: string; body: string; sort: number }[]} */
  const reqs = [];

  if (p.slug === "radisson-red-choice") {
    reqs.push(
      {
        title: "OUIBar + KTCHN (Flex F&B)",
        body: "Typical consideration: All-day communal bar-and-food hub combining soul of a bar with great food—social hangout for meet-ups, remote work, and local personality.\nOwner planning consideration: Confirm flex kitchen, grab-and-go, liquor, staffing model, and hours versus full restaurant capex.\nTypical status: Typically Expected\nNotes to confirm: Select-service flex F&B—not core Radisson full-service dining.",
        sort: 8,
      },
      {
        title: "24/7 Fitness & Social Lobby",
        body: "Typical consideration: Round-the-clock fitness and buzzing lobby with digiwall and communal furniture for work and socializing.\nOwner planning consideration: Budget 24/7 access, equipment replacement, and lobby activation labor.\nTypical status: Typically Expected\nNotes to confirm: Urban social lobby is core RED identity touchpoint.",
        sort: 9,
      },
      {
        title: "Guestroom — RED Amenities",
        body: "Typical consideration: Rain showers, RED-branded bath amenities, professional hairdryers, magnifying mirrors, and bold design accents.\nOwner planning consideration: Confirm PIP scope for wet rooms and FF&E versus conversion constraints.\nTypical status: Typically Expected\nNotes to confirm: Design-forward guestrooms support Instagrammable positioning.",
        sort: 10,
      }
    );
  } else if (p.slug === "radisson-individual-choice") {
    reqs.push(
      {
        title: "Hand-Selected Design & Local Character",
        body: "Typical consideration: Vivid Settings—bold design embracing each destination; property uniqueness preserved within collection rules.\nOwner planning consideration: Confirm which design elements are protected vs. required upgrades in conversion PIP.\nTypical status: Typically Expected\nNotes to confirm: Soft brand rewards authenticity—still requires collection compliance.",
        sort: 8,
      },
      {
        title: "Characterful Encounters & Local F&B",
        body: "Typical consideration: Curated local culture experiences and F&B narrative rooted in destination—not standardized chain menu.\nOwner planning consideration: Budget experience programming, local partnerships, and F&B labor for upper-upscale expectations.\nTypical status: Typically Expected\nNotes to confirm: Explorer's Compass service implies knowledgeable, exploration-minded staff.",
        sort: 9,
      },
      {
        title: "Meetings & Events (As Applicable)",
        body: "Typical consideration: Flexible meetings where asset supports corporate and group demand in cities, airports, or resorts.\nOwner planning consideration: Confirm room count, AV, and catering paths for your submarket.\nTypical status: May Apply\nNotes to confirm: Scale with keys and positioning—not mandatory for every boutique member.",
        sort: 10,
      }
    );
  } else if (p.segment === "upscale" || p.segment === "softCollection") {
    reqs.push(
      {
        title: "F&B / Restaurant & Bar",
        body: "Typical consideration: Upscale or collection F&B with local menu narrative, bar, and often marketplace.\nOwner planning consideration: Confirm kitchen, hood, liquor, staffing, and operating model.\nTypical status: Typically Expected\nNotes to confirm: Collection brands may retain more local F&B flexibility—verify contract.",
        sort: 10,
      },
      {
        title: "Meetings & Events",
        body: "Typical consideration: Flexible meeting and event space with AV and catering paths.\nOwner planning consideration: Confirm room count, breakout, and sales support expectations.\nTypical status: Typically Expected\nNotes to confirm: Scale with keys and market tier.",
        sort: 11,
      }
    );
  } else if (p.segment === "extendedStay") {
    reqs.push({
      title: "In-Suite Kitchen",
      body: "Typical consideration: Kitchenette or full kitchen with appliances, ware, and ventilation per prototype.\nOwner planning consideration: Confirm FF&E, utilities, fire/life safety, and wear reserves.\nTypical status: Typically Expected\nNotes to confirm: Weekly housekeeping cadence affects wear.",
      sort: 10,
    });
  } else if (p.segment !== "economy") {
    reqs.push({
      title: "Breakfast / Morning Meal",
      body: "Typical consideration: Complimentary or brand-standard breakfast where required by prototype.\nOwner planning consideration: Confirm kitchen, layout, staffing, and food cost model.\nTypical status: Typically Expected\nNotes to confirm: May differ for economy-tier brands without breakfast.",
      sort: 10,
    });
  }

  reqs.push(
    {
      title: "Lobby / Public Space",
      body: "Typical consideration: Arrival, seating, and brand-appropriate public space.\nOwner planning consideration: Confirm layout, FF&E, and conversion flexibility.\nTypical status: Typically Expected\nNotes to confirm: Upscale/collection may require higher design investment.",
      sort: 12,
    },
    {
      title: "Guestroom Standards",
      body: "Typical consideration: Bedding, bath, work area, finishes, and FF&E per prototype.\nOwner planning consideration: Confirm PIP scope, procurement, and room mix.\nTypical status: Typically Expected\nNotes to confirm: Conversions often need project-specific review.",
      sort: 13,
    },
    {
      title: "Fitness / Pool",
      body: "Typical consideration: Fitness and/or pool where prototype requires.\nOwner planning consideration: Confirm minimum size, equipment, and operating cost.\nTypical status: May Apply\nNotes to confirm: Economy brands may have reduced requirements.",
      sort: 14,
    },
    {
      title: "Signage / Exterior",
      body: "Typical consideration: Brand signage and exterior identity.\nOwner planning consideration: Confirm monument, façade, and permitting.\nTypical status: Typically Expected\nNotes to confirm: Radisson-family flags have distinct identity packages.",
      sort: 15,
    },
    {
      title: "Technology / Systems",
      body: "Typical consideration: PMS, CRS, loyalty, Wi-Fi, and reporting integrations.\nOwner planning consideration: Confirm cutover timeline, training, and recurring tech fees.\nTypical status: Typically Expected\nNotes to confirm: Mandatory Choice stack beyond headline royalty.",
      sort: 16,
    },
    {
      title: "Training / QA",
      body: "Typical consideration: Opening checklist, Choice University training, and QA inspections.\nOwner planning consideration: Confirm inspection fees, remediation, and ongoing QA cadence.\nTypical status: Typically Expected\nNotes to confirm: Upscale QA burden exceeds economy.",
      sort: 17,
    }
  );

  const requirementRows = reqs.map((r) =>
    row("standards.requirement", r.body, { title: r.title, sort: r.sort })
  );

  return [
    last,
    intro,
    ...requirementRows,
    row(
      "standards.conversion",
      `Conversion PIP scope for ${p.name} should align ${p.developmentModel.toLowerCase()}—sequence FF&E, signage, systems, and breakfast/F&B (if required) with financing and brand approval gates.`,
      { sort: 20 }
    ),
    row(
      "standards.questions",
      "Which prototype applies to this address?\nWhat is mandatory at opening vs. phased 12–24 months?\nHow do meetings, F&B, or kitchen requirements change PIP?\nWhat technology and loyalty cutover dates are contractual?\nWhat QA fees apply at opening and annually?",
      { sort: 30 }
    ),
    row(
      "standards.deal_inputs",
      "Room count and mix · New build vs. conversion · Prior flag · PIP timing · F&B / breakfast scope · Meeting space · Market tier · Incentive package",
      { sort: 40 }
    ),
  ];
}

function lifecycleWeight(p) {
  if (p.segment === "upscale" || p.segment === "softCollection") {
    return { pre: "Heavy", ramp: "Moderate", steady: "Moderate", renewal: "Heavy when PIP or re-licensing required" };
  }
  if (p.segment === "economy") {
    return { pre: "Moderate", ramp: "Light", steady: "Light", renewal: "Moderate" };
  }
  if (p.segment === "extendedStay") {
    return { pre: "Moderate", ramp: "Moderate", steady: "Light", renewal: "Moderate" };
  }
  return { pre: "Moderate", ramp: "Moderate", steady: "Moderate", renewal: "Moderate" };
}

/** @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p */
function buildOverviewRows(p) {
  const ov = overviewForBrand(p.name);
  if (!ov) {
    const scenarioBodies = p.scenarios.slice(0, 3);
    while (scenarioBodies.length < 3) scenarioBodies.push(p.typicalUseCase);
    const rows = [row("overview.scenarios", scenarioBodies.join("\n\n"), { sort: 0 })];
    for (let i = 0; i < 3; i++) {
      rows.push(
        row(`overview.scenario.${i + 1}`, scenarioBodies[i] || "", {
          title: ["Conversion & repositioning", "CALA / gateway growth", "Portfolio standardization"][i],
          sort: 0,
        })
      );
    }
    rows.push(
      row("overview.why_value", p.positioning, { sort: 0 }),
      row("overview.owner_experience", p.heroPurpose, { sort: 0 }),
      row("overview.proof_operator", p.bestAt[0] || p.tagline, { sort: 0 }),
      row("overview.differentiators.identity", p.tagline, { sort: 0 }),
      row("overview.differentiators.commercial", p.developmentModel, { sort: 0 })
    );
    for (let i = 0; i < 3; i++) {
      rows.push(
        row(`overview.bestAt.${i + 1}`, p.bestAt[i] || p.growthThemes[i] || "", {
          title: p.bestAt[i]?.split("—")[0]?.trim() || `Best at ${i + 1}`,
          sort: 0,
        })
      );
    }
    let proofCards = [];
    try {
      proofCards = buildProofCards(p.name);
    } catch {
      proofCards = [
        { title: "Choice distribution", body: `${p.name} uses Choice enterprise channels and Choice Privileges—confirm mix in FDD Item 19.` },
        { title: "Prototype economics", body: `${p.scaleLabel} capex and amenity stack must match ${p.name} standards—not a generic midscale box.` },
        { title: "CALA context", body: "Select CHI brands expand in CALA—confirm authorized geography and opening pipeline in LOI." },
        { title: "Operator fit", body: p.heroPurpose },
        { title: "Loyalty participation", body: "Choice Privileges member rates and direct paths support contribution when fulfillment is staffed." },
        { title: "Tier discipline", body: `Compare fee stack to ${p.similarBrands[0]} before signing—same parent, different tier requirements.` },
      ];
    }
    proofCards.forEach((pr, i) => {
      rows.push(row(`overview.proof.${i + 1}`, pr.body, { title: pr.title, sort: i }));
    });
    return rows;
  }

  const rows = [row("overview.scenarios", ov.scenarioBodies.join("\n\n"), { sort: 0 })];
  for (let i = 0; i < 3; i++) {
    rows.push(
      row(`overview.scenario.${i + 1}`, ov.scenarioBodies[i] || "", {
        title: ov.scenarioTitles[i] || "",
        sort: 0,
      })
    );
  }
  rows.push(
    row("overview.why_value", ov.whyValue, { sort: 0 }),
    row("overview.owner_experience", ov.ownerExperience, { sort: 0 }),
    row("overview.proof_operator", ov.proofOperator, { sort: 0 }),
    row("overview.differentiators.identity", ov.differentiatorsIdentity, { sort: 0 }),
    row("overview.differentiators.commercial", ov.differentiatorsCommercial, { sort: 0 })
  );
  for (let i = 0; i < 3; i++) {
    rows.push(
      row(`overview.bestAt.${i + 1}`, ov.bestAtBodies[i] || "", {
        title: ov.bestAtTitles[i] || "",
        sort: 0,
      })
    );
  }
  const proofCards = buildProofCards(p.name);
  proofCards.forEach((pr, i) => {
    rows.push(
      row(`overview.proof.${i + 1}`, pr.body, {
        title: pr.title,
        sort: i,
      })
    );
  });
  return rows;
}

/** @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p */
export function buildFullPresentationRows(p) {
  const item19 = item19ForBrand(p.name);
  const lc = lifecycleWeight(p);
  const rows = [];

  // —— Economics ——
  rows.push(
    row(
      "economics.intro",
      `${p.name} (${p.scaleLabel}) economics are illustrative only—not a quote or substitute for the FDD, LOI, or advisors. ${item19.notes || ""}`.trim(),
      { sort: 0 }
    ),
    row(
      "economics.checklist",
      `Fee bases for ${p.royaltyLabel.split("(")[0].trim()} and all marketing, technology, loyalty, and reservation charges\nInitial term, renewals, and notice\nPIP at opening, conversion, and renewal\nPerformance test and termination rights\nTransfer and change-of-control\nIncentives actually offered for this asset\nOpening milestone funding split\nMandatory Choice systems beyond fee lines`,
      { sort: 0 }
    ),
    row(
      "economics.cash.preopening",
      "Owner typically funds: Standards and FF&E alignment, technology cutover, working capital through opening, and application or training cash outlays.\n\nBrand typically provides: Design review, opening playbooks, pre-opening support, and milestone QA—not operating payroll.",
      { sort: 1 }
    ),
    row(
      "economics.cash.ramp",
      "Owner typically funds: Ramp marketing and loyalty enrollment while occupancy builds; recurring fees scale with revenue.\n\nBrand typically provides: Negotiated ramp relief or co-op when offered, plus channel guidance.",
      { sort: 2 }
    ),
    row(
      "economics.cash.steadystate",
      `Owner funds: recurring fee stack (${p.scaleLabel}) plus program participation once stabilized.\n\nBrand provides: Choice distribution, QA cadence, and benchmarks—not property payroll or routine FF&E.`,
      { sort: 3 }
    ),
    row(
      "economics.cash.renewal",
      "Owner typically funds: Renewal or conversion PIP, reserves, and re-licensing when triggered.\n\nBrand typically provides: Renewal standards; phased PIP timing may be negotiable in competitive renewals.",
      { sort: 4 }
    ),
    row("economics.opening.step.1", "Align on asset fit, market, and scope with brand development.", {
      title: "Application & Feasibility",
      sort: 1,
    }),
    row("economics.opening.step.2", "Prototype and standards review—PIP scope before major spend.", {
      title: "Design & standards",
      sort: 2,
    }),
    row("economics.opening.step.3", "OS&E, systems implementation, and franchise readiness checklist.", {
      title: "Pre-Opening Planning",
      sort: 3,
    }),
    row("economics.opening.step.4", "Training, QA readiness, and brand-led opening touchpoints.", {
      title: "Opening Support",
      sort: 4,
    }),
    row("economics.opening.step.5", "Heightened reporting and QA during early months; operator-led day-to-day.", {
      title: "Stabilization",
      sort: 5,
    }),
    row(
      "economics.opening.process",
      `Typical path for ${p.name}: feasibility, design approval, pre-opening systems cutover, opening QA, stabilization. Third-party operators often run opening while the brand approves milestones.`,
      { sort: 0 }
    ),
    row(
      "economics.opening.financials",
      "Financial planning themes (no deal-specific amounts):\n\nFront-loaded standards, FF&E, and technology\n\nWorking capital through ramp\n\nFee stack stepping from opening-weighted to stabilized\n\nPotential incentive credits or ramps\n\nRenewal PIP reserves even if opening PIP is lighter\n\nSeparate owner capex, brand fees, and operator pass-throughs",
      { sort: 0 }
    ),
    row(
      "economics.fee.join",
      "Application and entry fees; training and opening support; initial franchise fee; technology implementation; plan review and inspection. Basis varies by keys, market, and new build vs. conversion—confirm in FDD and LOI.",
      { sort: 1, title: "To Join" }
    ),
    row(
      "economics.fee.operate",
      `Steady-state: ${p.royaltyLabel}; plus marketing, technology, loyalty, and distribution per disclosure—model net contribution, not headline RevPAR.`,
      { sort: 2, title: "To Operate" }
    ),
    row(
      "economics.fee.change",
      "Renewal PIP, conversion PIP, termination obligations, and reserves—triggered at renewal or flag change.",
      { sort: 3, title: "When Things Change" }
    ),
    row(
      "economics.fee_variability",
      "Room count, market tier, new build vs. conversion, incentive package, and operator-led opening change how fees and capital land on your deal.",
      { sort: 0 }
    ),
    row(
      "economics.risk",
      "Expect a long initial franchise term with renewal options subject to notice, conditions, and standards compliance.",
      { sort: 1, title: "Term & renewal" }
    ),
    row(
      "economics.risk",
      "Performance tests and QA programs are common—understand cure periods and termination rights.",
      { sort: 2, title: "Performance & exit" }
    ),
    row(
      "economics.risk",
      "Assignment, change-of-control, and buyout provisions affect liquidity—confirm brand approval paths.",
      { sort: 3, title: "Transfer & sale" }
    ),
    row(
      "economics.risk",
      "Area-of-protection radius limits same-brand competition—confirm geography in legal documents.",
      { sort: 4, title: "Area of protection" }
    ),
    row(
      "economics.negotiability",
      "Often negotiated in competitive markets—key money, marketing support, fee ramps, or conversion assistance may be available. Confirm what was offered for your asset.",
      { sort: 0 }
    ),
    row(
      "economics.negotiable_items",
      "Key Money or Ramp Relief\nMarketing Co-Op or Opening Support\nPIP Scope or Timing\nApplication or Training Fee Structure\nFee Ramp in Early Operating Years",
      { sort: 0 }
    ),
    row(
      "economics.rarely_negotiable",
      "Core Royalty and Program Participation\nMandatory Technology Stack\nBrand Standards Compliance Framework\nFundamental QA and Reporting Obligations",
      { sort: 0 }
    ),
    row(
      "economics.model",
      `${p.name} trades recurring fees and program participation for Choice distribution, Choice Privileges, standards, and revenue support. Owners fund prototype FF&E, conversion scope, and working capital through ramp.`,
      { sort: 0 }
    ),
    row(
      "economics.kpi.fee_stack",
      `Confirm in FDD: Application · ${p.royaltyLabel.includes("membership") ? "Membership" : "Royalty"} · Marketing · Technology · Loyalty · Reservation · Training`,
      { sort: 0 }
    ),
    row("economics.kpi.agreement", "Multi-year term · renewal with PIP risk · notice-driven renewal", { sort: 0 }),
    row("economics.kpi.performance", "Performance and QA themes per franchise agreement", { sort: 0 }),
    row(
      "economics.kpi.capital",
      p.segment === "upscale"
        ? "Upscale PIP (guestrooms, public space, F&B) · owner reserves · prototype refresh"
        : "Conversion/new-build PIP · owner reserves · prototype compliance",
      { sort: 0 }
    ),
    row("economics.kpi.incentives", "Key money, ramp relief, marketing co-op—deal-specific", { sort: 0 }),
    row("economics.kpi.negotiability", "Often negotiated", { sort: 0 }),
    row(
      "economics.fee",
      `Ongoing compensation for ${p.scaleLabel} standards, QA, and portfolio access—${p.royaltyLabel}`,
      { sort: 1, title: "Royalty / brand fee" }
    ),
    row(
      "economics.fee",
      "Funds brand marketing, campaigns, and portfolio retail—model net after fund charges.",
      { sort: 2, title: "Marketing / brand fund" }
    ),
    row(
      "economics.fee",
      "Prescribed PMS/CRS/mobile stack—budget cutover, interfaces, and training.",
      { sort: 3, title: "Technology / systems" }
    ),
    row(
      "economics.fee",
      "Choice Privileges enrollment—model chargebacks, elite fulfillment, and direct mix.",
      { sort: 4, title: "Loyalty / program participation" }
    ),
    row(
      "economics.fee",
      "Choice-mediated reservation paths—stress-test OTA vs member/direct contribution.",
      { sort: 5, title: "Reservation / distribution" }
    ),
    row(
      "economics.fee",
      "Opening and ongoing training per prototype—often front-loaded at opening.",
      { sort: 6, title: "Training / opening support" }
    ),
    row("economics.lifecycle.preopening", lc.pre, { sort: 0 }),
    row("economics.lifecycle.ramp", lc.ramp, { sort: 0 }),
    row("economics.lifecycle.steadystate", lc.steady, { sort: 0 }),
    row("economics.lifecycle.renewal", lc.renewal, { sort: 0 }),
    row(
      "economics.incentives",
      `Competitive markets may offer key money, ramps, or co-op for ${p.name}—confirm actual package, not marketing typicals.`,
      { sort: 0 }
    ),
    row(
      "economics.term_renewal",
      "Renewals may trigger PIP and standards refresh—confirm term, notice, and triggers in agreement.",
      { sort: 0 }
    ),
    row(
      "economics.performance_exit",
      "QA failures can trigger cure paths—model termination exposure with counsel.\n\nEarly exit without cause is often limited.\n\nConfirm audit rights and re-inspection costs.",
      { sort: 0 }
    ),
    row(
      "economics.legal",
      "Confirm area-of-protection geography in legal documents.",
      { sort: 1, title: "Area of protection" }
    ),
    row(
      "economics.legal",
      "Transfers require brand approval—plan before marketing the asset.",
      { sort: 2, title: "Transfer & sale" }
    ),
    row(
      "economics.legal",
      "Clarify binding vs exploratory LOI terms and design approval gates early.",
      { sort: 3, title: "LOI & process" }
    ),
    row(
      "economics.support_burden",
      `Owners carry design review, training, and QA for ${p.scaleLabel}—budget management time for brand milestones before signing.`,
      { sort: 0 }
    ),
    row(
      "economics.diligence",
      `Which fee types apply to this asset?\nRenewal PIP scope for ${p.name}?\nPerformance/QA cures?\nTransfer rules with operator track record?\nActual key money/ramp offered?\nWho funds prototype review and opening training?`,
      { sort: 0 }
    )
  );

  rows.push(...standardsRows(p));

  // —— Materials ——
  const matFiles = [
    {
      title: "Franchise Disclosure Document",
      body: `Request the current ${p.name} franchise disclosure document from Choice Hotels. Item 19: ${item19.loyaltyPct ? `~${item19.loyaltyPct}% loyalty contribution in the published sample` : "review performance representations in your copy"}.`,
    },
    {
      title: p.pressKitFile ? "Brand overview & media" : "Development & architecture",
      body: p.pressKitFile
        ? "Marketing facts and imagery from the Choice Hotels media center."
        : "Product standards and prototype guidance on choicehotelsdevelopment.com and the Choice Hotels brand architecture portfolio.",
    },
    {
      title: "Choice Privileges®",
      body: "Program overview, earn, and redeem at choicehotels.com/choice-privileges.",
    },
  ];
  matFiles.forEach((f, i) => {
    rows.push(row("materials.file", f.body, { title: f.title, sort: i }));
  });
  for (let g = 1; g <= 6; g++) {
    rows.push(
      row(
        `materials.gallery.${g}`,
        p.pressKitFile
          ? "Representative brand photography from Choice Hotels media resources. Add your property renderings or site photos when available."
          : `Use Choice development resources and brand guidelines for imagery. Gallery slot ${g} reserved for property marketing photos.`,
        { sort: g }
      )
    );
  }
  const caseStudy = p.materialsCaseStudy || {
    title: `${p.name} — pipeline & system scale`,
    body: `Growth · North America · Choice portfolio\n\nConfirm opening commitments in Item 20 of your franchise disclosure document\n\n${p.scaleLabel} · Choice Hotels International\n\nPipeline and system scale—for context only, not a specific hotel opening\n\n${p.pipelineStats}\n\nCombine franchise disclosure Items 19 and 20 with local market study and your pro forma before signing.\n\n${p.footprintEditorial}`,
    caseSummaryOverview: p.pipelineStats,
    caseSummaryOwnerObjective: `Evaluate whether ${p.name} fits your asset tier, capex envelope, and operator capability.`,
    caseSummaryBrandRelevance: p.positioning,
    caseSummaryInterpretation:
      "System averages and published brand statistics do not replace your market comp set, fee model, and ramp plan.",
    caseSummaryTags: `${p.scaleLabel}, Choice Hotels, Pipeline context`,
  };
  rows.push({
    slotKey: "materials.caseStudy",
    title: caseStudy.title,
    body: caseStudy.body,
    sort: 0,
    caseSummaryOverview: caseStudy.caseSummaryOverview,
    caseSummaryOwnerObjective: caseStudy.caseSummaryOwnerObjective,
    caseSummaryBrandRelevance: caseStudy.caseSummaryBrandRelevance,
    caseSummaryInterpretation: caseStudy.caseSummaryInterpretation,
    caseSummaryTags: caseStudy.caseSummaryTags,
  });

  // —— Footprint (no fabricated property openings) ——
  rows.push(
    row(
      "footprint.geo_intro",
      `${p.name} is a ${p.scaleLabel} brand within the Choice Hotels International portfolio. ${p.pipelineStats} For owners, the key question is fit: confirm local demand drivers, required investment level, prototype/PIP scope, and net return versus your true competitive set.`,
      { sort: 10 }
    ),
    row(
      "footprint.region.am",
      footprintRegionBody(p, "am"),
      { sort: 11, title: "Americas" }
    ),
    row(
      "footprint.region.cala",
      footprintRegionBody(p, "cala"),
      { sort: 12, title: "CALA" }
    ),
    row(
      "footprint.region.eu",
      footprintRegionBody(p, "eu"),
      { sort: 13, title: "Europe" }
    ),
    row(
      "footprint.region.mea",
      footprintRegionBody(p, "mea"),
      { sort: 14, title: "MEA" }
    ),
    row(
      "footprint.region.apac",
      footprintRegionBody(p, "apac"),
      { sort: 15, title: "APAC" }
    ),
    row("footprint.growth_themes", p.growthThemes.join("\n"), { sort: 20 }),
    row("footprint.growth_editorial", p.footprintEditorial, { sort: 21 }),
    row("footprint.growth_fit", p.growthThemes.slice(0, 5).join("\n"), { sort: 22 }),
    row("footprint.editorial", p.footprintEditorial, { sort: 30 }),
    row(
      "footprint.editorial_bullets",
      p.growthThemes.map((t) => `• ${t}`).join("\n"),
      { sort: 31 }
    ),
    row("footprint.momentum_label", "Pipeline & system scale", { sort: 40 }),
    row("footprint.momentum", p.pipelineStats, { sort: 41 }),
    row(
      "footprint.momentum",
      item19.loyaltyPct
        ? `Choice Privileges contribution ~${item19.loyaltyPct}% of rooms (Item 19 sample).`
        : "Confirm loyalty contribution in FDD Item 19.",
      { sort: 42 }
    ),
    row(
      "footprint.momentum",
      item19.enterprisePct
        ? `Enterprise/CRS booking mix ~${item19.enterprisePct}% (Item 19 sample).`
        : item19.proprietaryPct
          ? `Proprietary (non-OTA) mix ~${item19.proprietaryPct}% (Item 19 sample).`
          : "Confirm distribution mix in FDD.",
      { sort: 43 }
    ),
    row(
      "footprint.geo.summary",
      `${p.name}: ${p.scaleLabel} · Choice Americas focus · confirm international authorization in LOI`,
      { sort: 0 }
    ),
    row(
      "footprint.growth.narrative",
      p.footprintEditorial,
      { sort: 0 }
    ),
    row(
      "footprint.portfolio_mix",
      `${p.scaleLabel} within CHI portfolio · compare to ${p.similarBrands.slice(0, 2).join(" and ")} in same tier`,
      { sort: 0 }
    )
  );

  const footprintOpenings = defaultFootprintOpeningsForBrand(p);
  for (const o of footprintOpenings) {
    rows.push({
      slotKey: "footprint.openings",
      title: sanitizeExternalCopy(o.title),
      body: sanitizeExternalCopy(o.body),
      sort: o.sort ?? 0,
      caseSummaryOverview: sanitizeExternalCopy(o.caseSummaryOverview),
      caseSummaryOwnerObjective: sanitizeExternalCopy(o.caseSummaryOwnerObjective),
      caseSummaryBrandRelevance: sanitizeExternalCopy(o.caseSummaryBrandRelevance),
      caseSummaryInterpretation: sanitizeExternalCopy(o.caseSummaryInterpretation),
      caseSummaryTags: sanitizeExternalCopy(o.caseSummaryTags),
    });
  }

  // —— Operations ——
  const flexSegment =
    p.segment === "economy" ||
    p.segment === "extendedStay" ||
    p.segment === "upscale" ||
    p.segment === "softCollection"
      ? p.segment
      : "midscale";
  const fb =
    p.segment === "upscale" || p.segment === "softCollection"
      ? "Moderate to High"
      : p.segment === "extendedStay"
        ? "Low"
        : p.segment === "economy"
          ? "Low"
          : "Low to Moderate";

  rows.push(
    row("operations.flexibility.design", flexLevelForSlot(flexSegment, "operations.flexibility.design"), {
      sort: 0,
    }),
    row("operations.flexibility.conversion", flexLevelForSlot(flexSegment, "operations.flexibility.conversion"), {
      sort: 0,
    }),
    row(
      "operations.flexibility.localization",
      flexLevelForSlot(flexSegment, "operations.flexibility.localization"),
      { sort: 0 }
    ),
    row(
      "operations.flexibility.operational_rigidity",
      flexLevelForSlot(flexSegment, "operations.flexibility.operational_rigidity"),
      { sort: 0 }
    ),
    row("operations.flexibility.pip", flexLevelForSlot(flexSegment, "operations.flexibility.pip"), { sort: 0 }),
    row(
      "operations.flexibility.prototype",
      flexLevelForSlot(flexSegment, "operations.flexibility.prototype"),
      { sort: 0 }
    ),
    row("operations.model.primary_model", "Franchise", { sort: 0 }),
    row("operations.model.management_option", "Third-party management common; brand approves operator fit.", { sort: 0 }),
    row("operations.model.typical_ownership", "Institutional and entrepreneurial owners; select franchisees.", { sort: 0 }),
    row("operations.model.brand_involvement", "Standards, QA, pre-opening, and commercial systems—not day-to-day operations post-stabilization.", { sort: 0 }),
    row("operations.model.systems_integration", "Mandatory Choice PMS/CRS/loyalty stack per FDD.", { sort: 0 }),
    row("operations.model.pre_opening", "Brand-led milestones; operator executes staffing and opening.", { sort: 0 }),
    row(
      "operations.model.staffing_intensity",
      p.segment === "economy" ? "Lean" : p.segment === "upscale" ? "Moderate to High" : "Moderate",
      { sort: 0 }
    ),
    row("operations.model.fb_complexity", fb, { sort: 0 }),
    row("operations.model.training", "Choice University and brand opening programs.", { sort: 0 }),
    row("operations.model.reporting_discipline", "Financial and quality reporting through mandated systems.", { sort: 0 }),
    row("operations.model.qa_rhythm", "Recurring property assessments per brand tier.", { sort: 0 }),
    row("operations.model.technology", "Prescribed tech stack; confirm interfaces and mobile guest journeys.", { sort: 0 }),
    row(
      "operations.standards_philosophy",
      `${p.name} balances ${p.scaleLabel} consistency with ${p.segment === "softCollection" ? "collection-level uniqueness" : "repeatable prototype execution"}—underwrite to standards manual, not marketing renderings alone.\n\n${flexEditorialSupplement(p)}`,
      { sort: 0 }
    ),
    row(
      "operations.operator_compat.summary",
      p.heroPurpose,
      { sort: 0 }
    ),
    row(
      "operations.operator_compat.tags",
      `${p.scaleLabel} · ${p.segment} · Choice Privileges · ${p.developmentModel.split(";")[0]}`,
      { sort: 0 }
    ),
    row(
      "operations.operator_compat.fit",
      `Strong fit when operator has ${p.scaleLabel} depth, can meet prototype/PIP, and models net contribution after ${p.royaltyLabel.split(";")[0]}. Weak fit when market cannot support required amenity stack or operator lacks tier experience.`,
      { sort: 0 }
    ),
    row("operations.compliance.qa_cadence", `Recurring ${p.scaleLabel} QA and brand inspections—not ad hoc only.`, { sort: 0 }),
    row("operations.compliance.training_rigor", p.segment === "economy" ? "Moderate—opening and safety focused" : "High—guest experience and brand programs", { sort: 0 }),
    row("operations.compliance.reporting", "Financial, quality, and franchise reporting through mandated tools.", { sort: 0 }),
    row(
      "operations.compliance.brand_interaction",
      "Structured pre-opening support; day-to-day operations with owner/operator once stabilized.",
      { sort: 0 }
    )
  );

  // —— Hero, overview, value owners, loyalty, insight ——
  rows.push(
    row(
      "hero.benefit_zones",
      `${p.tagline} · ${p.scaleLabel} · ${p.pipelineStats} · Choice Privileges participation.`,
      { sort: 0 }
    ),
    row("hero.operator_compat", p.heroPurpose, { sort: 0 }),
    row("overview.typical_use_case", p.typicalUseCase, { sort: 0 }),
    ...(portfolioContextForAirtableBrand(p.name)
      ? [
          row(
            "overview.portfolio_context",
            portfolioContextForAirtableBrand(p.name).relativePositioning,
            {
              title: String(portfolioContextForAirtableBrand(p.name).ladderTier),
              sort: 0,
            }
          ),
        ]
      : [
          row("overview.relative_positioning", p.positioning, { sort: 0 }),
        ]),
    row("overview.development_model", p.developmentModel, { sort: 0 }),
    ...buildOverviewRows(p)
  );
  rows.push(
    row(
      "valueOwners.overview",
      `Guests: ${p.tagline}\n\nOwners: ${p.scaleLabel} Choice flag with portfolio distribution and standards clarity.\n\nUnderwrite contribution after fees, loyalty costs, and channel mix versus comp set.`,
      { sort: 0 }
    ),
    row("valueOwners.scenarios", p.scenarios.join("\n\n"), { sort: 0 }),
    row(
      "valueOwners.watchouts",
      `Markets that cannot support ${p.scaleLabel} ADR or required amenity stack.\nHeavy conversion PIP misaligned with building constraints.\nOTA-heavy mix without modeling net after retail and member discounts.\nOperator tier mismatch shows in QA and reviews first.\nSponsor KPIs should track contribution and direct mix—not vanity metrics.`,
      { sort: 0 }
    ),
    row("valueOwners.lifecycle.1", `Screen: market tier, capex, and whether ${p.name} matches physical asset.`, { sort: 0 }),
    row("valueOwners.lifecycle.2", "Conversion design: PIP, prototype, and systems sequencing.", { sort: 1 }),
    row("valueOwners.lifecycle.3", "Pre-opening: hiring, training, systems cutover.", { sort: 2 }),
    row("valueOwners.lifecycle.4", "Opening: rate integrity, channel mix, QA first 90–120 days.", { sort: 3 }),
    row("valueOwners.lifecycle.5", "Ramp: loyalty contribution and seasonal retail tuning.", { sort: 4 }),
    row("valueOwners.lifecycle.6", "Ongoing: capex plan, brand initiatives, portfolio benchmarks.", { sort: 5 }),
    ...buildCommercialRows(p),
    row(
      "loyalty.hero_title",
      `${p.name} · Choice Privileges® — loyalty at a glance`,
      { sort: 0 }
    ),
    row(
      "loyalty.ecosystem",
      "7,100+ properties worldwide; earn up to 10 points per $1 on eligible direct stays; reward nights from 8,000 points; Titanium elite tier; partners across travel, cobrand cards, and experiences—confirm chargeback terms in your FDD.",
      { sort: 0 }
    ),
    row(
      "loyalty.owner_lens",
      `Model loyalty as net contribution—member discounts, fulfillment, and ${item19.loyaltyPct ? `~${item19.loyaltyPct}%` : "FDD-reported"} room mix from Choice Privileges in Item 19 sample.`,
      { sort: 0 }
    ),
    ...loyaltyProofRows(p, item19),
    row(
      "loyalty.earn",
      "Base earn on eligible spend with member rates on direct paths.\nPromotional accelerators during campaigns—confirm current rules.\nCobrand accelerators up to 16x–22x on stays with Choice Privileges Mastercard per consumer site.",
      { sort: 0 }
    ),
    row(
      "loyalty.redeem",
      "Award nights from 8,000 points where enabled—pricing demand-responsive.\nCash + points where offered.\nPartner redemptions for storytelling—not audited economics without disclosure.",
      { sort: 0 }
    ),
    row("loyalty.elite", "Member — baseline rates, milestone rewards path, points never expire.", { sort: 0, title: "Member" }),
    row("loyalty.elite", "Gold — 5 nights or 10,000 EQCs; bonus points on stays and enhanced recognition.", { sort: 1, title: "Gold" }),
    row("loyalty.elite", "Platinum — 15 nights or 30,000 EQCs; stronger earn bonus and elite benefits where available.", { sort: 2, title: "Platinum" }),
    row("loyalty.elite", "Diamond — 35 nights or 70,000 EQCs; top mainstream tier with upgrades and milestone rewards where published.", { sort: 3, title: "Diamond" }),
    row("loyalty.elite", "Titanium — 55 nights or 110,000 EQCs; highest published tier including Titanium Travel Award where offered.", { sort: 4, title: "Titanium" }),
    row(
      "loyalty.implications.pnl",
      `Model net after distribution, member discounts, and redemption—strongest when direct/member mix supports ${p.scaleLabel} ADR.`,
      { sort: 0 }
    ),
    row(
      "loyalty.implications.ops",
      "Elite recognition and breakfast/F&B promises become labor and COGS—staff to brand standards before campaigns.",
      { sort: 0 }
    ),
    row(
      "loyalty.implications.systems",
      "CRS/PMS loyalty integration and campaign tooling are mandatory—budget cutover and training.",
      { sort: 0 }
    ),
    row(
      "insight.summary",
      `${p.name} fits when you want ${p.scaleLabel} Choice distribution with clear prototype economics and Choice Privileges participation—not a mismatched tier (e.g., ${p.similarBrands[0]} vs ${p.name} without comparing fee stack and amenity requirements). Best with operator tier experience, honest ramp plan, and FDD-backed channel mix. Weaker where market ADR cannot support required product or PIP exceeds conversion ROI.`,
      { sort: 0 }
    ),
  );

  const externalSimilar = externalSimilarPeersForBrand(p.name);
  if (externalSimilar?.length) {
    for (const peer of externalSimilar) {
      rows.push(
        row("insight.similar", peer.body, {
          title: peer.title,
          sort: peer.sort,
        })
      );
    }
  } else {
    rows.push(
      row(
        "insight.similar",
        `Compare diligence to: ${p.similarBrands.join(", ")} — same parent company, different tier economics.`,
        { sort: 0 }
      )
    );
  }

  return rows;
}

/** @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p */
export function buildFixture(p) {
  return {
    targetBrandBasicsName: p.name,
    brandNameFallback: p.name,
    instructions: `Tier 1 full presentation for ${p.name}. Apply: node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name "${p.name}" --fixture fixtures/brand-explorer-presentation-${p.slug}-full.json --replace`,
    rows: buildFullPresentationRows(p),
  };
}
