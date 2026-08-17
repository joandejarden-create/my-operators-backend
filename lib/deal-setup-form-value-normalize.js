/**
 * Coerce sample-deal / fixture field values to exact Deal Setup form select options.
 * Source of truth: lib/deal-setup-form-options.json (from new-deal-setup.html).
 */
import FORM_OPTIONS from "./deal-setup-form-options.json" with { type: "json" };

const MULTI_SELECT_FIELDS = new Set([
  "Ownership Type",
  "Additional Amenities",
  "Primary Demand Drivers",
  "Preferred Brands (up to 4)",
  "F&B Program Type",
  "Preferred Third-Party Operator Profile",
  "Services Required From Operator",
  "Top 3 Success Metrics",
  "Top Priorities for Project",
  "Top Concerns for this Project",
  "Top 3 Deal Breakers",
  "Must-haves From Brand or Operator",
  "Incentive Types Interested In",
  "Operator Capability Priorities",
  "Owner Control Priorities",
  "Contract Flexibility Priorities",
  "Is the property encumbered",
  "Capital Structure",
  "Demand Mix Targets",
  "Operational Complexity Profile",
  "Sustainability Features",
  "Target Guest Segment",
]);

/** Fields where invalid custom values move to the paired Other text column. */
const OTHER_TEXT_FIELD = {
  "Primary Demand Drivers": "Primary Demand Drivers Other",
  "Top 3 Deal Breakers": "Top 3 Deal Breakers Other",
  "Must-haves From Brand or Operator": "Must-haves From Brand or Operator Other",
  "Top Priorities for Project": "Top Priorities for Project Other",
  "Top Concerns for this Project": "Top Concerns for this Project Other",
  "Top 3 Success Metrics": "Top 3 Success Metrics Other",
  "Access to Transit or Highway": "Access to Transit or Highway Other Text",
};

/** Per-field lowercase alias → canonical option value. */
const FIELD_ALIASES = {
  "Hotel Type": {
    urban: "Urban / City Center",
    "historic / urban": "Historic / Heritage",
    "historic / adaptive reuse": "Historic / Heritage",
    historic: "Historic / Heritage",
    airport: "Airport",
    resort: "Resort",
    "all-inclusive resort": "All-Inclusive Resort",
    "boutique / lifestyle": "Boutique / Lifestyle",
    "convention / conference": "Convention / Conference",
    "extended stay": "Extended Stay",
  },
  "Hotel Service Model": {
    "lifestyle / boutique": "Lifestyle / Boutique",
    "all-inclusive resort": "All-Inclusive",
    "lifestyle / select-service": "Select-Service",
    "lifestyle / full-service": "Full-Service",
    "lifestyle / wellness resort": "Lifestyle / Boutique",
    "full service": "Full-Service",
    "select service": "Select-Service",
    resort: "Lifestyle / Boutique",
    "conference / convention hotel": "Full-Service",
    "mixed-use / hotel + real estate": "Full-Service",
  },
  "Project Type": {
    "other / to be confirmed": "Adaptive Reuse",
    conversion: "Conversion / Reflag",
    "repositioning / rebrand": "Renovation / Repositioning",
    "expansion / add-on": "Expansion / Add-on",
  },
  "Additional Amenities": {
    "other amenities (specify)": "Other Amenities",
    other: "Other Amenities",
    spa: "Spa or Wellness Center",
    pools: "Pool",
    "outdoor pool": "Pool",
    "rooftop pool": "Rooftop Pool",
    "rooftop bar": "Rooftop Bar",
    "rooftop bar / terrace": "Rooftop Terrace / Deck",
    "beach club": "Beach Club",
    "beach frontage": "Beach Access / Frontage",
    "fitness center": "Fitness Center",
    "business center": "Business Center",
  },
  "Building Type": {
    historic: "Historic / Renovated",
    "historic / mixed-use": "Mixed-Use",
    "historic / renovated": "Historic / Renovated",
    "historic / adaptive reuse": "Historic / Adaptive Reuse",
    "high-rise / mixed-use": "Mixed-Use",
    "high-rise": "High-Rise",
    "low-rise resort": "Resort-Style Compound",
    "mid-rise": "Mid-Rise",
    "low-rise": "Low-Rise",
    "podium / tower": "Podium / Tower",
    "mixed-use": "Mixed-Use",
    "resort-style compound": "Resort-Style Compound",
  },
  "Ownership Type": {
    private: "Fee Simple",
    "fee simple": "Fee Simple",
    "jv ownership": "JV Ownership",
  },
  "Ownership Structure": {
    "private partnership (sample)": "Private Partnership",
    "private partnership": "Private Partnership",
    "private / family office + local partner (sample)": "Family Office",
    "joint venture (jv)": "Joint Venture (JV)",
    "family office": "Family Office",
    "institutional / fund": "Institutional / Fund",
    "jv — developer + institutional partner (sample, terms tbd)": "Joint Venture (JV)",
    "jv - developer + institutional partner (sample, terms tbd)": "Joint Venture (JV)",
  },
  "Ownership/Brand History or Track Record": {
    "regional owner/developer (fictional sample)": "Experienced - Multi-Property Owner",
    "regional developer… (fictional)": "Experienced - Multi-Property Owner",
    "regional owner/developer (fictional)": "Experienced - Multi-Property Owner",
    "regional developer with prior select-service and extended-stay experience in mexico (fictional)":
      "Experienced - Multi-Property Owner",
  },
  "Current Form of Site Control": {
    "owned (sample)": "Fee Simple Ownership",
    "owned - fee simple (sample)": "Fee Simple Ownership",
    "owned — fee simple (sample)": "Fee Simple Ownership",
    "purchase agreement executed (sample)": "Under Contract",
    "leasehold — term sheet executed (sample)": "Letter of intent (LOI) / Term Sheet",
    "option to purchase (sample)": "Option to Purchase",
    "under contract (sample)": "Under Contract",
  },
  "Zoning Status": {
    "permitted / in process (sample)": "Conditional / In Progress",
    "hotel / commercial permitted (sample assumption)": "Approved for Hotel Use",
    "approved for hotel (sample)": "Approved for Hotel Use",
    "conditional — entitlements in process (sample)": "Conditional / In Progress",
  },
  "Site/Development Restrictions?": {
    "see sample notes": "Yes",
    "unesco heritage district constraints — approvals in process": "Yes",
    "height limit near flight path; shuttle circulation requires coordination (sample)": "Yes",
    "retail podium integration (sample)": "Yes",
    "mixed-use phasing; retail/residential integration (sample)": "Yes",
  },
  "Total Site Size Unit": {
    acres: "Sq. Ft.",
    hectares: "Sq. M.",
  },
  "Parking Amenities?": {
    "on-site parking (sample)": "Yes",
    "limited urban parking (inferred)": "Yes",
    "resort parking (inferred)": "Yes",
    "complimentary self-parking (public listing)": "Yes",
    "surface parking for airport/crew demand (sample)": "Yes",
  },
  "Access to Transit or Highway": {
    "puj airport highway access (sample)": "Highway or Major Road Frontage",
    "metro access assumed; loading dock constraints tbd": "Walking Distance to Transit",
    "metro and urban transit access (inferred)": "Walking Distance to Transit",
    "historic district pedestrian access; port/airport via transfer": "Limited (No Direct Transit or Highway)",
    "airport corridor — highway frontage (sample)": "Highway or Major Road Frontage",
  },
  "Primary Demand Drivers": {
    meetings: "Meetings, Incentives, Conferences, Exhibitions",
    airport: "Transportation Hub",
    weddings: "Leisure / Tourism",
    groups: "Meetings, Incentives, Conferences, Exhibitions",
    "crew / extended stay": "Corporate / Business Travel",
    "leisure / tourism": "Leisure / Tourism",
    "corporate / business travel": "Corporate / Business Travel",
    "cultural / historic district": "Cultural / Historic District",
    "beach / resort destination": "Beach / Resort Destination",
  },
  "Plan to Self-Manage or Hire Third Party?": {
    "hire third-party operator (sample intent)": "Third-Party Managed",
    "hire third-party operator (sample)": "Third-Party Managed",
    "third-party managed (sample)": "Third-Party Managed",
    "third-party operator (sample)": "Third-Party Managed",
    "third-party operator required (sample)": "Third-Party Managed",
    "third-party luxury operator required (sample)": "Third-Party Managed",
    "retain operator or re-bid (sample)": "Undecided",
    "owner-operated (sample)": "Owner-Operated",
  },
  "Soft vs Hard Brand Preference": {
    "operator-led all-inclusive model required": "Hard Brand",
    "prefer soft/collection; limited standardization flexibility": "Soft Brand",
    "open to soft/collection first; hard brand if economics justify": "Unsure / Open to Both",
    "soft luxury preferred; preservation-first": "Soft Brand",
    "hard brand preferred (sample)": "Hard Brand",
    "soft brand / collection preferred (sample)": "Soft Brand",
    "hard brand with manageable pip": "Hard Brand",
    "hard brand with fee discipline and manageable standards": "Hard Brand",
    "hard brand acceptable; prioritize efficient prototype and fee discipline": "Hard Brand",
    "preserve identity; soft brand strongly preferred": "Soft Brand",
    "lifestyle brand supporting mixed-use visibility": "Soft Brand",
    "brand must support mixed-use financing narrative": "Unsure / Open to Both",
    "lifestyle path preferred; soft vs hard still open": "Unsure / Open to Both",
    "open — operator capability priority": "Unsure / Open to Both",
    "open - operator capability priority": "Unsure / Open to Both",
  },
  "Open to Soft Brand First Then Reflag?": {
    "open to discussion (sample)": "Maybe",
    "no — prefer definitive hard brand path (sample)": "No",
    "no — prefer definitive hard-brand path for financing story": "No",
    "yes — collection first pathway (sample)": "Yes",
  },
  "Open to Outside Capital or Partnerships?": {
    "yes — minority capital partner possible (sample)": "Yes",
  },
  "Preferred Deal Structure": {
    "franchise + 3rd party mgmt.": "Brand + Third-Party Mgmt. (Combined)",
    "franchise + 3rd party management": "Brand + Third-Party Mgmt. (Combined)",
    "brand + third-party management (combined)": "Brand + Third-Party Mgmt. (Combined)",
    "brand + third-party management (separate)": "Brand + Third-Party Mgmt. (Separate)",
    "brand + third-party mgmt. (separate)": "Brand + Third-Party Mgmt. (Separate)",
    "brand + third-party mgmt. (combined)": "Brand + Third-Party Mgmt. (Combined)",
    "franchise + 3rd party mgmt. (separate)": "Brand + Third-Party Mgmt. (Separate)",
    "franchise only (sample)": "Franchise Only",
    "brand-managed": "Brand-Managed Only",
    "brand managed": "Brand-Managed Only",
    "brand-managed only": "Brand-Managed Only",
  },
  "PIP / CapEx Status": {
    "preliminary budget range only — details tbd": "Budgeted",
    "range under review": "Budgeted",
    "scope under development": "Planned",
    "planned — scope tbd (sample)": "Planned",
    "partially defined": "Planned",
    "partially budgeted": "Budgeted",
    "budgeted — construction phase (sample)": "Budgeted",
    "budgeted - construction phase (sample)": "Budgeted",
    "new build — capex in construction budget": "None",
    "not applicable — new build": "None",
  },
  "Who should receive bids for this project?": {
    "both brands and third-party operators": "Both Brands and Third-Party Operators",
    "hotel brands only (franchise/license)": "Hotel Brands Only (Franchise/License)",
  },
  "Financial Model Available?": {
    "yes — draft (sample)": "Yes",
    "yes (draft)": "Yes",
  },
  "Legal Support Needed?": {
    "yes (sample)": "Yes — Connect me With a Legal Advisor",
    "yes — connect me with a legal advisor": "Yes — Connect me With a Legal Advisor",
    "yes — franchise and site control (sample)": "Yes — Connect me With a Legal Advisor",
  },
  "Working with Broker/Advisor?": {
    "yes — cala hospitality advisors (fictional)": "Yes",
    "yes — cala urban advisors (fictional)": "Yes",
    "yes — caribbean resort advisors (fictional)": "Yes",
    "yes - cala hospitality advisors (fictional)": "Yes",
    "yes - cala urban advisors (fictional)": "Yes",
    "yes - caribbean resort advisors (fictional)": "Yes",
  },
  "Level of Involvement in Day-to-Day Ops": {
    "active asset management (sample)": "Low - Strategic Oversight",
    "governance board — monthly (sample)": "Medium - Key Decisions Only",
    "high — weekly involvement (sample)": "High - Regular Input on Decisions",
    "oversight — weekly kpi review (sample)": "Low - Strategic Oversight",
  },
  "Decision Timeline for Brand/Operator": {
    "q1 2027": "3–6 Months",
    "q2 2027": "3–6 Months",
    "q3 2027": "6–12 Months",
    "q4 2026": "3–6 Months",
    "3-6 months": "3–6 Months",
    "6-12 months": "6–12 Months",
    "0-3 months": "0–3 Months",
    "12+ months": "12+ Months",
  },
  "Minimum Operator Experience (years)": {
    "10": "10+ Years",
    "5": "5+ Years",
    "15": "15+ Years",
  },
  "Planned Hold Period": {
    "7–10 years": "7-10 Years",
    "7-10 years": "7-10 Years",
    "5–7 years": "5-7 Years",
    "10+ years": "15+ Years",
    "long-term hold": "Long-term / Indefinite",
  },
  "IRR/Yield Goals": {
    "13–16% (sample)": "10% – 13%",
    "13-16% (sample)": "10% – 13%",
    "14–17% (sample)": "14% – 17%",
    "14–17% (sample target)": "14% – 17%",
    "10–13% (sample)": "10% – 13%",
    "not yet determined (sample)": "Not Yet Determined",
  },
  "Top 3 Deal Breakers": {
    "prototype requirements that break budget": "High Fees or Unfavorable Economics",
    "prototype that conflicts with heritage fabric": "Inflexible Contract Terms",
    "rigid prototype that eliminates local character": "Inflexible Contract Terms",
    "uncapped pip requirements": "High Fees or Unfavorable Economics",
    "long lock-in without performance tests": "Exit Restrictions or Long Lock-In",
    "operator without ai resort track record":
      "Insufficient Operator Experience (e.g. &lt; 10 Years)",
    "pip without phasing plan": "Inflexible Contract Terms",
    "f&b minimums that break resort economics": "High Fees or Unfavorable Economics",
  },
  "Must-haves From Brand or Operator": {
    "heritage-sensitive design process": "Design Approval Rights",
    "collection brand flexibility": "Flexible Contract Terms",
    "experienced boutique operator": "Experienced Operator (e.g. 10+ Years)",
    "design approval collaboration": "Design Approval Rights",
    "flexible f&b standards": "Flexible Contract Terms",
    "experienced urban operator (e.g. 10+ years)": "Experienced Operator (e.g. 10+ Years)",
    "proven all-inclusive operator": "Experienced Operator (e.g. 10+ Years)",
    "phased pip plan": "Flexible Contract Terms",
    "group/wedding sales support": "Strong Distribution and Marketing Support",
    "airport-corridor distribution strength": "Strong Distribution and Marketing Support",
  },
  "Services Required From Operator": {
    "revenue management": "Revenue Management",
    "sales": "Sales & Marketing",
    "accounting": "Accounting & Reporting",
    "hr": "HR & Training",
    "marketing": "Sales & Marketing",
    "f&b management": "Full Management",
    "revenue management, sales, accounting, hr, marketing": [
      "Revenue Management",
      "Sales & Marketing",
      "Accounting & Reporting",
      "HR & Training",
    ],
  },
  "Top Priorities for Project": {
    "brand credibility, distribution, operator capability, standards flexibility": [
      "Brand Recognition",
      "Operational Expertise",
      "Design Flexibility",
      "Flexible Deal Terms",
    ],
    "design story, soft brand fit, heritage compliance, distribution": [
      "Design Flexibility",
      "Brand Recognition",
      "Owner Support",
    ],
    "operator capability, resort programming, brand distribution, pip feasibility": [
      "Operational Expertise",
      "Brand Recognition",
      "Strong Financial Performance",
    ],
    "airport demand capture, brand distribution, manageable standards and fees": [
      "Strong Financial Performance",
      "Brand Recognition",
      "Cost Efficiency",
    ],
  },
  "Top Concerns for this Project": {
    "pip/capex scope, soft vs hard brand path, competitive set completeness, commercial incentive assumptions":
      ["High Costs", "Inflexibility", "Underperformance"],
    "brand standards waivers, local design story capture, pip/capex range validation": [
      "Inflexibility",
      "High Costs",
    ],
    "all-inclusive operating model definition, pip scope, operator role clarity, brand standards tolerance":
      ["Inflexibility", "High Costs", "Loss of Control"],
  },
  "F&B Program Type": {
    "bistro + bar (public class)": "Full-Service Restaurant + Bar",
    "fine dining + bar (public class)": "Full-Service Restaurant + Bar",
    "signature restaurant + bar (public listing class)": "Full-Service Restaurant + Bar",
    "restaurant + bar + lounge (public class)": "Full-Service Restaurant + Bar",
    "restaurant + bar (public class)": "Full-Service Restaurant + Bar",
    "breakfast-focused + bar (public class)": "Coffee Shop / Cafe",
    "multiple restaurants + bars; all-inclusive programming (public class)":
      "Full-Service Restaurant + Bar",
    "all-day dining + grab-and-go market (sample program)": "Fast Casual Restaurant",
    "destination f&b + wellness programming (public class)": "Full-Service Restaurant + Bar",
    "signature restaurant, bar, lounge (public class)": "Full-Service Restaurant + Bar",
    "restaurant + limited bar; breakfast buffet (fee)": "Full-Service Restaurant + Bar",
  },
  "Would you like to receive regular updates?": {
    yes: "Weekly Summary",
  },
  "Preferred Chain Scales": {
    "upper midscale, upscale": "Upper Midscale",
    "luxury, upper upscale": "Luxury",
    "upscale, upper upscale": "Upscale",
    "upper upscale, luxury": "Upper Upscale",
  },
  "Target Guest Segment": {
    "affluent leisure, destination weddings, cultural travelers": "Leisure",
    "adult leisure, groups, destination weddings": "Leisure",
    "urban creative/leisure, corporate weekday demand": "Bleisure",
    "bleisure (business + leisure)": "Bleisure",
    "family leisure": "Family",
    "convention / meetings": "Group / MICE",
    "tour groups": "Group / MICE",
    "business": "Corporate / Business",
    "corporate transient, airport crew, meeting groups (sample)": [
      "Corporate / Business",
      "Group / MICE",
      "Contract / Extended Stay",
    ],
  },
  "Current Operating Model": {
    "owner-operated (unbranded)": "Owner-Operated (Unbranded)",
    "owner-operated (branded/franchised)": "Owner-Operated (Branded/Franchised)",
    "third-party managed (branded)": "Third-Party Managed (Branded)",
    "third-party managed (independent/collection)": "Third-Party Managed (Independent/Collection)",
    "brand-managed": "Brand-Managed",
    "lease/operator lease structure": "Lease/Operator Lease Structure",
    "mixed/transitioning": "Mixed/Transitioning",
  },
  "Preferred Future Operating Model": {
    "third-party management only": "Third-Party Management Only",
    "owner-operated": "Owner-Operated",
    "franchise/license only (owner or third-party operator)":
      "Franchise/License Only (Owner or Third-Party Operator)",
    "brand + third-party management": "Brand + Third-Party Management",
  },
  "Opening / Transition Phase": {
    "n/a (stabilized operating)": "N/A (Stabilized Operating)",
    "planning / entitlement": "Planning / Entitlement",
    "pre-opening ramp": "Pre-Opening Ramp",
    "soft opening": "Soft Opening",
    "reopening after renovation": "Reopening After Renovation",
    "rebranding in place": "Rebranding In Place",
  },
  "Operator Scope": {
    "full management": "Full Management",
    "commercial support": "Commercial Support",
    "pre-opening support": "Pre-Opening Support",
    "brand compliance support": "Brand Compliance Support",
    "owner reporting": "Owner Reporting",
    "asset management support": "Asset Management Support",
    "technical services coordination": "Technical Services Coordination",
  },
  "Operating Model": {
    "owner-operated": "Owner-Operated",
    "third-party managed": "Third-Party Managed",
    "brand-managed": "Brand-Managed",
  },
  "Brand Agreement Structure": {
    "soft brand / collection affiliation": "Soft Brand / Collection Affiliation",
  },
  "Branded Residence Program Model": {
    "hotel-branded residences (integrated with hotel)": "Hotel-Branded Residences (Integrated With Hotel)",
    "branded residences + independent condo units (same building)":
      "Branded Residences + Independent Condo Units (Same Building)",
    "condo-hotel / rental pool integrated": "Condo-Hotel / Rental Pool Integrated",
    "residences only — hotel component separate": "Residences Only — Hotel Component Separate",
    "to be defined with operator": "To Be Defined With Operator",
  },
  "Condo Rental Program Model": {
    "operator-managed rental pool (optional owner participation)":
      "Operator-Managed Rental Pool (Optional Owner Participation)",
    "operator-managed rental pool (mandatory participation)":
      "Operator-Managed Rental Pool (Mandatory Participation)",
    "owner-managed rental pool": "Owner-Managed Rental Pool",
    "no rental program (owner use only)": "No Rental Program (Owner Use Only)",
    "to be defined with operator": "To Be Defined With Operator",
  },
  "F&B Operating Model": {
    "hotel-operated": "Hotel-Operated",
    "third-party lease (independent restaurateur)": "Third-Party Lease (Independent Restaurateur)",
    "third-party management agreement (hotel-branded)": "Third-Party Management Agreement (Hotel-Branded)",
    "hybrid (hotel core + leased flagship restaurant)": "Hybrid (Hotel Core + Leased Flagship Restaurant)",
    "not applicable": "Not Applicable",
  },
  "Development Proforma Available?": {
    "in progress": "In Progress",
  },
};

function normKey(s) {
  return String(s)
    .trim()
    .toLowerCase()
    .replace(/\u2013|\u2014/g, "-")
    .replace(/\s+/g, " ");
}

function getOptions(fieldName) {
  return FORM_OPTIONS[fieldName] || null;
}

function exactOption(fieldName, value) {
  const opts = getOptions(fieldName);
  if (!opts || value === undefined || value === null) return null;
  const s = String(value).trim();
  if (!s) return null;
  if (opts.includes(s)) return s;
  const nk = normKey(s);
  for (const o of opts) {
    if (normKey(o) === nk) return o;
  }
  return null;
}

function aliasOption(fieldName, value) {
  const map = FIELD_ALIASES[fieldName];
  if (!map) return null;
  const nk = normKey(value);
  if (map[nk] !== undefined) return map[nk];
  for (const [k, v] of Object.entries(map)) {
    if (normKey(k) === nk) return v;
  }
  return null;
}

/** @param {number} midMillions */
function projectCostBand(midMillions) {
  if (midMillions < 5) return "Under $5M";
  if (midMillions < 10) return "$5M – $10M";
  if (midMillions < 15) return "$10M – $15M";
  if (midMillions < 25) return "$15M – $25M";
  if (midMillions < 50) return "$25M – $50M";
  if (midMillions < 100) return "$50M – $100M";
  if (midMillions < 200) return "$100M – $200M";
  return "$200M+";
}

function parseUsdMillionsRange(value) {
  const s = String(value);
  let m = s.match(/(\d+(?:\.\d+)?)\s*[–\-—to]+\s*(\d+(?:\.\d+)?)\s*m/i);
  if (m) {
    const a = parseFloat(m[1]);
    const b = parseFloat(m[2]);
    return (a + b) / 2;
  }
  m = s.match(/[$€]\s*(\d+(?:\.\d+)?)\s*[–\-—to]+\s*(\d+(?:\.\d+)?)\s*m/i);
  if (m) {
    return (parseFloat(m[1]) + parseFloat(m[2])) / 2;
  }
  return null;
}

function parseRevparRange(value) {
  const s = String(value);
  const m = s.match(/(\d+)\s*[–\-—to]+\s*(\d+)/i);
  if (!m) return null;
  return (parseInt(m[1], 10) + parseInt(m[2], 10)) / 2;
}

function revparBand(mid) {
  if (mid < 50) return "Under $50";
  if (mid < 100) return "$50 – $99";
  if (mid < 150) return "$100 – $149";
  if (mid < 200) return "$150 – $199";
  if (mid < 300) return "$200 – $299";
  if (mid < 400) return "$300 – $399";
  if (mid < 500) return "$400 – $499";
  if (mid < 600) return "$500 – $599";
  return "$600+";
}

function pipBudgetBand(midMillions) {
  if (midMillions < 0.5) return "Under $500K";
  if (midMillions < 1) return "$500K – $1M";
  if (midMillions < 2) return "$1M – $2M";
  if (midMillions < 3) return "$2M – $3M";
  if (midMillions < 5) return "$3M – $5M";
  if (midMillions < 10) return "$5M – $10M";
  if (midMillions < 20) return "$10M – $20M";
  return "$20M+";
}

function titleCaseAmenityToken(token) {
  const map = {
    "business center": "Business Center",
    "fitness center": "Fitness Center",
    pool: "Pool",
    pools: "Pool",
    spa: "Spa or Wellness Center",
    "spa or wellness center": "Spa or Wellness Center",
    "other amenities (specify)": "Other Amenities",
    "other amenities": "Other Amenities",
    lobby: "Lobby",
    "rooftop pool": "Rooftop Pool",
    "rooftop bar": "Rooftop Bar",
    concierge: "Concierge",
    "ev charging": "EV Charging",
  };
  const nk = normKey(token);
  return map[nk] || null;
}

function inferAmenitiesFromText(text) {
  const t = normKey(text);
  const out = [];
  if (/pool/.test(t)) out.push("Pool");
  if (/fitness|gym/.test(t)) out.push("Fitness Center");
  if (/spa|wellness/.test(t)) out.push("Spa or Wellness Center");
  if (/business center/.test(t)) out.push("Business Center");
  if (/meeting|event/.test(t)) out.push("Meeting/Event Space");
  if (/pet/.test(t)) out.push("Pet Amenities");
  if (/rooftop|outdoor|courtyard|beach/.test(t)) out.push("Outdoor Area / Courtyard");
  if (/bar|lounge/.test(t)) out.push("Bar or Beverage Concept");
  if (!out.length) return ["Other Amenities"];
  return out;
}

function inferFbProgramFromText(text) {
  const t = normKey(text);
  if (/grab|go|market|coffee|breakfast/.test(t)) return "Coffee Shop / Cafe";
  if (/minimal|limited f&b/.test(t)) return "Minimal / Grab & Go";
  if (/pool bar|rooftop bar/.test(t)) return "Pool Bar / Rooftop Bar / Feature Bar";
  if (/restaurant|dining|bistro|bar|f&b|all-inclusive/.test(t)) {
    return "Full-Service Restaurant + Bar";
  }
  return "Full-Service Restaurant + Bar";
}

function coerceScalar(fieldName, value) {
  if (value === undefined || value === null) return { value, changed: false };
  if (typeof value === "number") return { value, changed: false };
  const s = String(value).trim();
  if (!s) return { value: s, changed: false };

  const opts = getOptions(fieldName);
  if (!opts) return { value: s, changed: false };

  if (fieldName === "Preferred Chain Scales" && s.includes(",")) {
    for (const part of s.split(/\s*,\s*/)) {
      const hit = exactOption(fieldName, part) || aliasOption(fieldName, part);
      if (hit) return { value: hit, changed: true };
    }
  }

  if (fieldName === "Additional Amenities" && s.length > 40) {
    const inferred = inferAmenitiesFromText(s);
    return { value: inferred, changed: true, spillOther: s };
  }

  const exact = exactOption(fieldName, s);
  if (exact) return { value: exact, changed: exact !== s };

  const alias = aliasOption(fieldName, s);
  if (alias !== null) {
    if (Array.isArray(alias)) {
      const pick = alias[0];
      return { value: pick, changed: pick !== s };
    }
    return { value: alias, changed: alias !== s };
  }

  if (fieldName === "Total Project Cost Range") {
    const mid = parseUsdMillionsRange(s);
    if (mid != null) {
      const band = projectCostBand(mid);
      return { value: band, changed: true };
    }
  }
  if (fieldName === "PIP Budget Range (if conversion)") {
    const mid = parseUsdMillionsRange(s);
    if (mid != null) {
      const band = pipBudgetBand(mid);
      return { value: band, changed: true };
    }
  }
  if (fieldName === "Estimated or Actual RevPAR") {
    const mid = parseRevparRange(s);
    if (mid != null) {
      const band = revparBand(mid);
      return { value: band, changed: true };
    }
    if (/not yet|tbd|validation/i.test(s)) {
      return { value: "Not Yet Determined", changed: true };
    }
  }

  if (opts.includes("Other")) {
    return { value: "Other", changed: true, spillOther: s };
  }

  return { value: s, changed: false, invalid: true };
}

function splitMultiValue(value) {
  if (Array.isArray(value)) return value.map((v) => String(v).trim()).filter(Boolean);
  return String(value)
    .split(/\s*,\s*/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function coerceMulti(fieldName, value) {
  if (!Array.isArray(value)) {
    const rawStr = String(value).trim();
    if (rawStr) {
      const fullAlias = aliasOption(fieldName, rawStr);
      if (Array.isArray(fullAlias)) {
        return { value: fullAlias, changed: true };
      }
      if (typeof fullAlias === "string") {
        return { value: [fullAlias], changed: true };
      }
    }
  }

  if (fieldName === "Additional Amenities") {
    const tokens = splitMultiValue(value);
    const out = [];
    let changed = false;
    for (const token of tokens) {
      const exact = exactOption(fieldName, token);
      if (exact) {
        if (!out.includes(exact)) out.push(exact);
        if (exact !== token) changed = true;
        continue;
      }
      const titled = titleCaseAmenityToken(token);
      if (titled) {
        if (!out.includes(titled)) out.push(titled);
        changed = true;
        continue;
      }
    }
    if (!out.length || tokens.join(", ").length > 40) {
      const inferred = inferAmenitiesFromText(Array.isArray(value) ? value.join(", ") : String(value));
      for (const a of inferred) if (!out.includes(a)) out.push(a);
      changed = true;
    }
    if (out.length) return { value: out, changed };
  }

  const tokens = splitMultiValue(value);
  const out = [];
  const spill = [];
  let changed = false;

  for (const token of tokens) {
    const alias = aliasOption(fieldName, token);
    if (Array.isArray(alias)) {
      for (const a of alias) {
        if (!out.includes(a)) out.push(a);
      }
      changed = true;
      continue;
    }
    if (typeof alias === "string") {
      if (!out.includes(alias)) out.push(alias);
      changed = true;
      continue;
    }
    const exact = exactOption(fieldName, token);
    if (exact) {
      if (!out.includes(exact)) out.push(exact);
      if (exact !== token) changed = true;
      continue;
    }
    const scalar = coerceScalar(fieldName, token);
    if (scalar.invalid) {
      spill.push(token);
      if (!out.includes("Other")) out.push("Other");
      changed = true;
    } else {
      const v = scalar.value;
      if (Array.isArray(v)) {
        for (const x of v) if (!out.includes(x)) out.push(x);
      } else if (!out.includes(v)) out.push(v);
      changed = true;
    }
  }

  return { value: out, changed, spillOther: spill.length ? spill.join("; ") : undefined };
}

/**
 * @param {string} fieldName
 * @param {unknown} value
 */
export function normalizeDealSetupFieldValue(fieldName, value) {
  if (value === undefined || value === null) return { value, changed: false };
  if (typeof value === "number") return { value, changed: false };
  if (typeof value === "string" && value.trim() === "") return { value: "", changed: false };

  if (MULTI_SELECT_FIELDS.has(fieldName) || Array.isArray(value)) {
    return coerceMulti(fieldName, value);
  }
  return coerceScalar(fieldName, value);
}

/**
 * @param {Record<string, unknown>} fields
 * @returns {{ fields: Record<string, unknown>, coercions: { field: string, from: unknown, to: unknown }[] }}
 */
/** Form selects that accept only one option (not multi-select in Deal Setup). */
const SINGLE_SELECT_FIELDS = new Set([
  "Preferred Chain Scales",
  "Hotel Type",
  "Hotel Service Model",
  "Hotel Chain Scale",
  "Building Type",
  "Ownership Structure",
  "Soft vs Hard Brand Preference",
  "Preferred Deal Structure",
  "PIP / CapEx Status",
  "Total Project Cost Range",
  "PIP Budget Range (if conversion)",
  "Estimated or Actual RevPAR",
  "IRR/Yield Goals",
  "Planned Hold Period",
  "Primary Goal for the Hotel",
]);

const LEGACY_OTHER_FIELD_RENAMES = {
  "Access to Transit or Highway Other": "Access to Transit or Highway Other Text",
};

export function normalizeDealSetupFields(fields) {
  const out = { ...fields };
  const coercions = [];

  for (const [legacy, canonical] of Object.entries(LEGACY_OTHER_FIELD_RENAMES)) {
    if (out[legacy] !== undefined && out[canonical] === undefined) {
      out[canonical] = out[legacy];
      delete out[legacy];
    }
  }

  for (const [fieldName, raw] of Object.entries(fields)) {
    if (raw === undefined) continue;
    const result = normalizeDealSetupFieldValue(fieldName, raw);
    const { value, changed, spillOther, invalid } = result;
    if (!changed && !spillOther && !invalid) continue;

    if (changed || spillOther) {
      coercions.push({ field: fieldName, from: raw, to: value });
      out[fieldName] = value;
    }

    const otherField = OTHER_TEXT_FIELD[fieldName];
    if (spillOther && otherField) {
      const prev = out[otherField] ? `${out[otherField]}; ` : "";
      out[otherField] = `${prev}${spillOther}`.trim();
      coercions.push({ field: otherField, from: fields[otherField], to: out[otherField] });
    }
  }

  for (const fieldName of SINGLE_SELECT_FIELDS) {
    const value = out[fieldName];
    if (!Array.isArray(value)) continue;
    const pick = value.find((v) => v && v !== "Other") || value[0];
    if (pick !== undefined) {
      out[fieldName] = pick;
      coercions.push({ field: fieldName, from: value, to: pick });
    }
  }

  return { fields: out, coercions };
}

export { FORM_OPTIONS, MULTI_SELECT_FIELDS };
