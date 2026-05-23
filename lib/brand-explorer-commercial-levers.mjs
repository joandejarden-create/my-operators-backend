/**
 * Commercial Engine presentation slots — benefit/impact copy differentiated per brand.
 * Body format: "{mechanism}\n\nProject impact: {benefit on your asset}"
 */

/** @typedef {{ key: string; title: string; mechanism: string; projectImpact: string }} CommercialLever */

const IMPACT_SPLIT = /\n\n(?:Project impact|Owner lens):\s*/i;

/**
 * @param {string} body
 * @returns {{ mechanism: string; projectImpact: string }}
 */
export function parseCommercialLeverBody(body) {
  const raw = String(body || "").trim();
  if (!raw) return { mechanism: "", projectImpact: "" };
  const parts = raw.split(IMPACT_SPLIT);
  if (parts.length >= 2) {
    return {
      mechanism: parts[0].trim(),
      projectImpact: parts.slice(1).join("\n\nProject impact: ").trim(),
    };
  }
  return { mechanism: raw, projectImpact: "" };
}

/** @param {CommercialLever} lever */
export function formatCommercialLeverBody(lever) {
  return `${lever.mechanism}\n\nProject impact: ${lever.projectImpact}`;
}

/**
 * @param {import('../scripts/lib/choice-tier1-explorer-profiles.mjs').Tier1Profile} p
 * @param {(profile: typeof p, leverKey: string) => string} impactFn
 * @returns {CommercialLever[]}
 */
export function commercialLeversForProfileWithImpact(p, impactFn) {
  const scale = p.scaleLabel || "franchise";
  const name = p.name || "This brand";
  const segment = p.segment || "midscale";

  const leisureMechanism =
    segment === "softCollection"
      ? "Inspiration content, packages, and destination narratives for guests seeking character—when rate premium depends on design and local story."
      : "Inspiration content, packages, partnerships, and destination narratives for high-intent leisure shoppers—when rate premium depends on aspiration and uniqueness.";

  const corporateMechanism =
    segment === "economy"
      ? "Highway and suburban negotiated programs where the flag adds trust on price-sensitive corporate and crew demand."
      : "Contracted travelers, small meetings, and negotiated programs where the flag acts as a trusted filter—especially in urban and gateway mixed-use assets.";

  const keys = [
    ["distribution", "Distribution & Retail Reach", `Branded retail paths guests already use—CRS connectivity, brand.com and app, retail OTA relationships, and packages—so ${name} shows up in consideration sets where independents often under-index.`],
    ["revenue_management", "Revenue Management & Pricing Discipline", `Forecasting tools, competitive sets, restriction strategies, and brand-level playbooks tuned to ${scale} economics—not only discounting.`],
    ["digital_marketing", "Digital Marketing & Performance Media", "Paid and owned media, search, social, and retargeting at portfolio scale, with creative templates that can still carry property-level story."],
    ["corporate_group", "Corporate, SME & Group Pull", corporateMechanism],
    ["leisure_destination", "Leisure & Destination Visibility", leisureMechanism],
    ["international", "International & Feeder Markets", "Inbound and cross-border feeders where global recognition reduces perceived risk—gateways, hubs, and resorts with international mix."],
    ["sales_catering", "Sales & Catering Brand Pull", "Brand credibility, central inquiry flow, and proposal tools for weddings, SMERF, and small corporate meetings."],
    ["reputation_qa", "Reputation, Reviews & QA Lift", "Recognizable flags improve post-click conversion; QA and service standards reduce variance that hurts reviews and repeat visits."],
    ["data_analytics", "Data, Analytics & Experimentation", "Portfolio benchmarks, test-and-learn, and guest insights to refine offers, room types, and channel mix."],
  ];

  return keys.map(([key, title, mechanism]) => ({
    key,
    title,
    mechanism,
    projectImpact: impactFn(p, key),
  }));
}

/**
 * @param {{ name: string; scaleLabel: string; segment?: string; growthThemes?: string[]; bestAt?: string[] }} p
 */
export function commercialDemandScenariosForProfile(p) {
  const segment = p.segment || "midscale";
  const base = [
    ["Gateway Urban", "Strong"],
    ["Regional & Secondary Upscale", "Moderate–strong"],
    ["Corporate-Led Urban", "Strong"],
    ["Resort / Coastal Leisure", segment === "economy" ? "Moderate" : "Strong"],
    ["Conversion / Repositioning", "Strong"],
  ];
  if (segment === "economy") {
    base.push(["Pure Economy / Highway", "Strong"]);
    base.push(["Luxury Urban Flagship", "Not a fit"]);
  } else if (segment === "extendedStay") {
    base.push(["Extended-Stay / Weekly Rate", "Strong"]);
    base.push(["Pure Economy / Highway", "Selective"]);
  } else {
    base.push(["Pure Economy / Highway", "Not a fit"]);
  }
  return base;
}

/**
 * @param {{ name: string; scaleLabel: string; bestAt?: string[]; growthThemes?: string[] }} p
 */
export function commercialThemesForProfile(p) {
  const fromBestAt = (p.bestAt || []).slice(0, 4).filter(Boolean);
  if (fromBestAt.length >= 3) {
    return fromBestAt;
  }
  const themes = (p.growthThemes || []).slice(0, 4);
  if (themes.length) return themes;
  return [
    `Retail and loyalty reach aligned to ${p.scaleLabel || "franchise"} positioning.`,
    "Conversion and trust on branded booking paths.",
    "Repeat and higher-quality guest mix versus standalone independent retail.",
    "Commercial systems that should justify fees in your project returns.",
  ];
}
