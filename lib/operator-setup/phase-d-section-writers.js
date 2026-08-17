/**
 * Phase D — evidence-aware Setup section writers (Production only).
 * Downstream of OE Assignments / Presence / Brand Relationships / packs.
 * Never invents KPI scores, portfolio %, Fit bf_*, or named executives without sources.
 * Never clones Test Fixture / synthetic golden marketing prose.
 */
import { isAggregateAssignmentName } from "../operator-explorer/readiness.js";
import { TEST_FIXTURE_MASTER_IDS } from "../operator-explorer/phase-1-universe.js";

export const PHASE_D_VERSION = "operator-setup-phase-d-v1";

const HELD = new Set(["recJ6NPSYveCTo3At"]); // Tafer

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}
function isPopulated(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === "boolean") return true;
  if (typeof v === "number") return !Number.isNaN(v);
  if (typeof v === "string") return v.trim().length > 0;
  if (Array.isArray(v)) return v.length > 0;
  return Boolean(v);
}

export function namedCurrent(assignments, masterId) {
  return (assignments || []).filter(
    (r) =>
      (r.fields?.Operator || []).includes(masterId) &&
      !isAggregateAssignmentName(r.fields?.["Property Name"]) &&
      String(r.fields?.["Assignment Status"] || "") === "Current" &&
      !/Various/i.test(String(r.fields?.["Property Name"] || ""))
  );
}

export function isTestFixtureId(id) {
  return TEST_FIXTURE_MASTER_IDS.includes(id);
}

export function buildOeContext({ masterId, canonicalName, assignments, marketPresence, brandRelationships, master }) {
  const named = namedCurrent(assignments, masterId);
  const countries = [...new Set(named.map((r) => nz(r.fields?.Country)).filter(Boolean))].sort();
  const brands = [
    ...new Set(
      [
        ...(brandRelationships || [])
          .filter((r) => (r.fields?.Operator || []).includes(masterId))
          .map((r) => nz(r.fields?.Brand)),
        ...named.map((r) => nz(r.fields?.Brand)),
      ].filter(Boolean)
    ),
  ].sort();
  const structures = [
    ...new Set(named.map((r) => nz(r.fields?.["Operating / Management Structure"])).filter(Boolean)),
  ];
  const hotelTypes = named.map((r) => nz(r.fields?.["Hotel Type"])).filter(Boolean);
  const dev = [...new Set(named.map((r) => nz(r.fields?.["Development Context"])).filter(Boolean))];
  const mpCurrent = (marketPresence || []).filter(
    (r) =>
      (r.fields?.Operator || []).includes(masterId) &&
      ["Current Managed Property", "Current Operating Portfolio"].includes(String(r.fields?.["Market Presence Type"] || ""))
  );
  const om = nz(master?.fields?.["Operating Model"]);
  const ma = nz(master?.fields?.["Management Availability"]);
  const website = nz(master?.fields?.["Operator Website"]);
  return {
    masterId,
    canonicalName,
    named,
    countries,
    brands,
    structures,
    hotelTypes,
    developmentContexts: dev,
    mpCurrentCount: mpCurrent.length,
    om,
    ma,
    website,
    held: HELD.has(masterId),
    namedCount: named.length,
  };
}

/** Platform & Markets narrative fields only — no geo counts, no % experience, no KPI selects */
export function buildPlatformNarratives(ctx) {
  if (ctx.namedCount < 1 && !ctx.brands.length && !ctx.om) return {};
  const fields = {};
  if (ctx.namedCount >= 1 || ctx.om) {
    fields.cap_profile_operational = [
      `${ctx.canonicalName}:`,
      ctx.namedCount >= 1
        ? `current named operating evidence covers ${ctx.namedCount} assignment(s)`
        : "named assignment sample is thin in the CALA evidence set",
      ctx.countries.length ? `across ${ctx.countries.join(", ")}` : null,
      ctx.structures.length ? `with documented structure(s): ${ctx.structures.join("; ")}` : null,
      ctx.om ? `Master Operating Model: ${ctx.om}.` : null,
    ]
      .filter(Boolean)
      .join(" ")
      .trim();
  }

  if (ctx.brands.length || ctx.hotelTypes.length) {
    fields.cap_profile_commercial = [
      ctx.brands.length ? `Brand evidence includes: ${ctx.brands.slice(0, 8).join(", ")}.` : null,
      ctx.hotelTypes.length
        ? `Assignment hotel-type examples include: ${[...new Set(ctx.hotelTypes)].slice(0, 6).join("; ")}.`
        : null,
      "Counts and portfolio mix percentages are not inferred from the CALA evidence sample.",
    ]
      .filter(Boolean)
      .join(" ");
  }

  if (ctx.developmentContexts.length) {
    fields.cap_profile_transition = `Documented assignment development contexts: ${ctx.developmentContexts.join("; ")}. Transition/pre-opening capability should be underwritten asset-by-asset.`;
  }

  if (ctx.countries.length && !ctx.held) {
    fields.specificMarkets = `Current evidence markets: ${ctx.countries.join("; ")} (from Assignments / current Market Presence; Strategic Interest excluded).`;
  }
  return fields;
}

/** Thin Profile narrative when pack missing */
export function buildProfileNarratives(ctx) {
  if (ctx.namedCount < 1 && !ctx.brands.length && !ctx.website && !ctx.om && !ctx.ma) return {};
  const fields = {};
  fields.companyDescription = [
    `${ctx.canonicalName} —`,
    ctx.namedCount >= 1
      ? `Operator Explorer evidence includes ${ctx.namedCount} current named assignment(s)`
      : `Operator Explorer named-assignment sample is limited in the current CALA evidence set`,
    ctx.countries.length ? ` in ${ctx.countries.join(", ")}` : null,
    ctx.brands.length ? `; brands evidenced: ${ctx.brands.slice(0, 6).join(", ")}` : null,
    ctx.om ? `. Master Operating Model: ${ctx.om}` : null,
    `.`,
    ctx.website ? ` Website: ${ctx.website}.` : null,
    ` Underwrite scale and capabilities from primary sources and management agreements.`,
  ]
    .filter(Boolean)
    .join("")
    .replace(/\s+/g, " ")
    .trim();
  if (ctx.brands.length) {
    fields.differentiators = `Evidence-backed brand relationships in the intelligence set include: ${ctx.brands.slice(0, 8).join(", ")}. Differentiation claims beyond this evidence are not asserted.`;
  }
  if (ctx.website && !fields.website) fields.website = ctx.website;
  return fields;
}

/** Commercial — non-Fit narrative / structure only */
export function buildCommercialNarratives(ctx, packCommercial = {}) {
  const fields = {};
  if (ctx.structures.length) {
    // Map to free-text / multi if field accepts strings — Management Structures Supported is often multiSelect
    fields._managementStructuresList = ctx.structures;
  }
  if (nz(packCommercial.ownerEngagementNarrative)) {
    fields.ownerEngagementNarrative = packCommercial.ownerEngagementNarrative;
  } else if (ctx.namedCount >= 1 || ctx.ma || ctx.structures.length) {
    fields.ownerEngagementNarrative = [
      `Owner engagement should be underwritten from the management agreement and asset context.`,
      ctx.ma ? `Master Management Availability: ${ctx.ma}.` : null,
      ctx.structures.length ? `Observed assignment structures: ${ctx.structures.join("; ")}.` : null,
      ctx.namedCount >= 1 ? `Current named assignments in evidence set: ${ctx.namedCount}.` : null,
    ]
      .filter(Boolean)
      .join(" ");
  }
  if (ctx.hotelTypes.length) {
    fields.specializations = `Evidence-backed asset/hotel-type examples from current assignments: ${[...new Set(ctx.hotelTypes)].slice(0, 8).join("; ")}.`;
  }
  // Thin ov cards only when evidence exists — not golden marketing clone
  if (ctx.brands.length >= 2) {
    fields.ov_card_commercial = `Multi-brand operating evidence present (${ctx.brands.slice(0, 6).join(", ")}). Commercial engine details remain brand/asset-specific.`;
  }
  if (ctx.developmentContexts.some((d) => /conversion|reflag|pre-opening|new build|transition/i.test(d))) {
    fields.ov_card_flexibility = `Development-context evidence includes: ${ctx.developmentContexts.join("; ")}. Flexibility/tradeoffs depend on agreement scope.`;
  }
  return fields;
}

/** Governance narrative — no infra_kpi scores */
export function buildGovernanceNarratives(ctx, packGov = {}) {
  const fields = {};
  if (nz(packGov.risk_programs_narrative)) {
    fields.risk_programs_narrative = packGov.risk_programs_narrative;
  } else if (ctx.namedCount >= 1 || ctx.om) {
    fields.risk_programs_narrative =
      "Risk, insurance, and control programs should be confirmed in diligence by asset and brand agreement. No unverified scorecards applied.";
  }
  if (/Brand \/ Operator|Integrated Brand|Hybrid/i.test(ctx.om) || /Brand-managed/i.test(ctx.structures.join(" "))) {
    fields.infra_systems_technology =
      "Systems are typically brand-dependent and/or property-specific under brand-managed or hybrid paths. Confirm PMS/CRS/RMS, reporting channels, and owner portal access per asset—do not assume a single operator-owned stack.";
  } else if (ctx.namedCount >= 1 || ctx.om) {
    fields.infra_systems_technology =
      "Operating systems and reporting channels vary by asset, owner preference, and brand agreement. Confirm technology stack and owner reporting tools in diligence; no vendor names invented.";
  }
  if (ctx.ma || ctx.structures.length) {
    fields.infra_asset_management_reporting = [
      "Owner/asset-management reporting cadence should follow the management agreement.",
      ctx.ma ? `Management Availability classification: ${ctx.ma}.` : null,
      "Monthly operating reviews and variance reporting are common market practice but not asserted as verified for this operator without a source.",
    ]
      .filter(Boolean)
      .join(" ");
  }
  return fields;
}

/** Engagement section rows — thin, evidence-backed (not 40+ golden marketing rows) */
export function buildEngagementRows(ctx) {
  if (ctx.namedCount < 1 || ctx.held) return [];
  const rows = [];
  let order = 10;
  rows.push({
    section: "Strategic Owner Value",
    row_key: "phase_d_strategic_value",
    display_order: order++,
    title: "Evidence-backed operating footprint",
    body: `${ctx.canonicalName} has ${ctx.namedCount} current named assignment(s)${
      ctx.countries.length ? ` in ${ctx.countries.join(", ")}` : ""
    }${ctx.brands.length ? ` with brand evidence including ${ctx.brands.slice(0, 5).join(", ")}` : ""}. Owner value propositions beyond this footprint require agreement-level diligence.`,
  });
  rows.push({
    section: "Engagement Cadence",
    row_key: "phase_d_engagement_cadence",
    display_order: order++,
    title: "Reporting cadence (to confirm)",
    body: "Standard third-party/brand-managed practice is monthly operating reporting plus periodic owner reviews. Exact cadence, portal access, and escalation paths must be confirmed from the management agreement—not assumed.",
    extra: "Evidence class: market practice + OE footprint; agreement-specific unverified",
  });
  if (ctx.structures.length) {
    rows.push({
      section: "Controls & Governance",
      row_key: "phase_d_controls",
      display_order: order++,
      title: "Operating structure evidence",
      body: `Documented assignment operating structures: ${ctx.structures.join("; ")}. Brand QA and owner controls follow the applicable brand/management agreement.`,
    });
  }
  if (ctx.developmentContexts.length) {
    rows.push({
      section: "Lifecycle Support",
      row_key: "phase_d_lifecycle",
      display_order: order++,
      title: "Development / transition contexts observed",
      body: `Assignment development contexts include: ${ctx.developmentContexts.join("; ")}. Pre-opening/transition scope is asset-specific.`,
    });
  }
  return rows;
}

/** Infrastructure Platform thin rows */
export function buildInfrastructureRows(ctx) {
  if (ctx.namedCount < 1 || ctx.held) return [];
  const rows = [];
  let order = 10;
  rows.push({
    section: "Technology Stack",
    row_key: "phase_d_tech_stack",
    display_order: order++,
    title: "Systems posture",
    body:
      /Brand|Hybrid|Integrated/i.test(ctx.om) || /Brand-managed/i.test(ctx.structures.join(" "))
        ? "Brand-dependent and/or property-specific systems are expected. Summarize owner reporting cadence and secure channels without inventing vendor names."
        : "Operator and brand systems vary by asset. Confirm PMS/CRS/RMS, BI, and owner reporting tools in diligence—no vendor names invented from brand affiliation alone.",
  });
  rows.push({
    section: "Portfolio Metric",
    row_key: "phase_d_reporting_systems",
    display_order: order++,
    title: "Reporting Systems",
    body: `Current named operating evidence: ${ctx.namedCount} assignment(s); Market Presence current rows: ${ctx.mpCurrentCount}. Owner reporting tools are not asserted beyond available sources.`,
  });
  return rows;
}

/** Leadership Platform — markets/languages/governance capability, NOT invented people */
export function buildLeadershipPlatformRows(ctx) {
  if (ctx.namedCount < 1 || ctx.held) return [];
  const rows = [];
  let order = 10;
  for (const c of ctx.countries.slice(0, 8)) {
    rows.push({
      section: "Team Market",
      display_order: order++,
      title: c,
      body: `Current named assignment evidence includes ${c}. This indicates market exposure, not a verified permanent regional office headcount.`,
    });
  }
  // Language heuristic — only Spanish/English for CALA-heavy footprints, marked provisional
  const calaish = ctx.countries.some((c) =>
    /Mexico|Dominican|Colombia|Costa Rica|Panama|Brazil|Argentina|Chile|Peru|Puerto Rico|Jamaica|Guatemala|Ecuador/i.test(c)
  );
  if (calaish) {
    rows.push({
      section: "Language",
      display_order: order++,
      title: "Spanish",
      body: "CALA operating evidence present; Spanish is typically required for owner/staff communication. Confirm bilingual corporate coverage in diligence.",
      depth: "Provisional",
    });
    rows.push({
      section: "Language",
      display_order: order++,
      title: "English",
      body: "English commonly used for brand/owner reporting on international platforms. Confirm coverage by market.",
      depth: "Provisional",
    });
  }
  if (ctx.ma || ctx.structures.length) {
    rows.push({
      section: "Governance Cadence",
      display_order: order++,
      title: "Owner-Performance Alignment",
      body: [
        "Owner-performance alignment should follow the management agreement and brand standards where applicable.",
        ctx.ma ? `Management Availability: ${ctx.ma}.` : null,
      ]
        .filter(Boolean)
        .join(" "),
    });
  }
  return rows;
}

export const PHASE_D_BLOCKED_FIELDS = new Set([
  "bf_operating_situations",
  "bf_not_ideal_for",
  "bf_selected_deal_structures",
  "bf_selected_asset_types",
  "bf_selected_situation_types",
  "readyForInvestorPublication",
  "Active Countries",
  "locationTypeUrban",
  "locationTypeSuburban",
  "locationTypeResort",
  "locationTypeAirport",
  "locationTypeTotal",
  "conversionExperience",
  "newBuildExperience",
  "infra_kpi_exec",
  "infra_kpi_tools",
  "infra_kpi_revenue",
  "cap_kpi_operating_model",
  "cap_kpi_execution_strength",
  "cap_kpi_transition",
  "cap_kpi_reporting",
]);

export function isPhaseDBlocked(field) {
  if (PHASE_D_BLOCKED_FIELDS.has(field)) return true;
  if (/^bf_/i.test(field)) return true;
  if (/^geo_/i.test(field)) return true;
  if (/^cap_kpi_/i.test(field)) return true;
  if (/^cap_signal_/i.test(field)) return true;
  if (/^infra_kpi_/i.test(field)) return true;
  if (/^infra_signal_/i.test(field)) return true;
  if (/^risk_signal_/i.test(field)) return true;
  if (/^tr_signal_/i.test(field)) return true;
  if (/^lead_signal_/i.test(field)) return true;
  if (/^lead_kpi_/i.test(field)) return true;
  if (/^locationType/i.test(field)) return true;
  if (/Experience$/i.test(field)) return true;
  return false;
}

export { isPopulated, HELD };
