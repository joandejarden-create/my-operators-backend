/**
 * Owner-facing presentation fields for Capital Provider Explorer profiles.
 * Merged onto seed records until Airtable read path is live.
 */

const LEADER_IMG = {
  a: "https://images.unsplash.com/photo-1560250097-0b93528c311a?auto=format&fit=crop&w=900&q=80",
  b: "https://images.unsplash.com/photo-1573496359142-b8d87734a5a2?auto=format&fit=crop&w=900&q=80",
  c: "https://images.unsplash.com/photo-1556157382-97eda2d62296?auto=format&fit=crop&w=900&q=80",
  d: "https://images.unsplash.com/photo-1542206395-9feb3edaa68d?auto=format&fit=crop&w=900&q=80",
};

const HOTEL_IMG = {
  a: "https://images.unsplash.com/photo-1566073771259-6a8506099945?auto=format&fit=crop&w=1200&q=80",
  b: "https://images.unsplash.com/photo-1582719508461-905c673771fd?auto=format&fit=crop&w=1200&q=80",
  c: "https://images.unsplash.com/photo-1551882547-ff40c63fe5fa?auto=format&fit=crop&w=1200&q=80",
};

/** @type {Record<string, object>} */
export const CAPITAL_PROVIDER_PRESENTATION = {
  "cp-1": {
    headquarters: "Charlotte, NC",
    shortDescription: "Dedicated hospitality lending desk for branded select-service and upscale hotels.",
    portfolioStats: {
      dealsFinanced: 124,
      totalVolumeLabel: "$4.8B+",
      activeMarkets: 8,
      yearsLending: 22,
    },
    currentLendingAppetiteOwner: "Active — prioritizing select-service acquisitions and refinances",
    sponsorPreference: "Institutional and experienced hotel owners preferred",
    keyDifferentiators: [
      "Dedicated hotel credit team with regional RM coverage",
      "Fast term sheet turnaround for stabilized branded assets",
      "Deep Marriott / Hilton / IHG franchise familiarity",
      "Construction and bridge capability within bank platform",
    ],
    ownerValueProps: [
      {
        title: "Relationship-Led Origination",
        body: "Owners gain a single hospitality credit contact who understands flag requirements, PIP timing, and sponsor underwriting.",
      },
      {
        title: "Stabilized + Light Transitional",
        body: "The desk balances core recapitalizations with limited conversion and repositioning when operator quality is strong.",
      },
      {
        title: "Institutional Process, Local Markets",
        body: "Bank-grade diligence with market teams that know Southeast and Texas demand drivers.",
      },
    ],
    leadership: [
      {
        name: "Sarah Mitchell",
        title: "SVP, Hospitality Lending",
        role: "Originations · Southeast",
        bio: "Leads hospitality lending for the Southeast and Texas. 18+ years in branded hotel credit and relationship origination.",
        imageUrl: LEADER_IMG.a,
      },
      {
        name: "James Ortiz",
        title: "Managing Director, Credit",
        role: "Credit · National",
        bio: "Oversees hotel credit policy, committee presentations, and portfolio risk for the lodging vertical.",
        imageUrl: LEADER_IMG.d,
      },
      {
        name: "Lisa Tran",
        title: "VP, Portfolio Monitoring",
        role: "Portfolio · Hospitality",
        bio: "Manages ongoing asset surveillance, covenant compliance, and workout strategy for hotel borrowers.",
        imageUrl: LEADER_IMG.b,
      },
    ],
    trackRecord: [
      {
        name: "Marriott Select-Service Portfolio Refinance",
        location: "Atlanta, GA",
        dealType: "Refinance",
        loanAmount: "$42M",
        year: "2025",
        summary: "Refinanced a 4-hotel select-service portfolio with 65% LTV and 10-year fixed structure.",
        imageUrl: HOTEL_IMG.a,
      },
      {
        name: "Hilton Garden Inn Acquisition",
        location: "Austin, TX",
        dealType: "Acquisition",
        loanAmount: "$28M",
        year: "2024",
        summary: "Acquisition financing for a stabilized upscale select-service asset near major corporate demand.",
        imageUrl: HOTEL_IMG.b,
      },
      {
        name: "Hyatt Place Conversion",
        location: "Nashville, TN",
        dealType: "Conversion",
        loanAmount: "$19M",
        year: "2024",
        summary: "Supported independent-to-brand conversion with experienced third-party operator.",
        imageUrl: HOTEL_IMG.c,
      },
    ],
  },
  "cp-2": {
    headquarters: "Los Angeles, CA",
    shortDescription: "Private credit focused on transitional hotel value creation across the West Coast.",
    portfolioStats: {
      dealsFinanced: 36,
      totalVolumeLabel: "$1.2B+",
      activeMarkets: 6,
      yearsLending: 11,
    },
    currentLendingAppetiteOwner: "Selective — seeking transitional assets with experienced sponsors",
    sponsorPreference: "Experienced hotel owner or developer with clear stabilization plan",
    keyDifferentiators: [
      "Speed to term sheet on bridge and recapitalization opportunities",
      "Comfort with lifestyle, resort, and full-service transitional assets",
      "Mezzanine and stretch senior capability within one platform",
      "Operator-led turnaround and repositioning expertise",
    ],
    ownerValueProps: [
      {
        title: "Transitional Credit Specialist",
        body: "Built for owners pursuing repositioning, conversion, or lease-up where operational upside supports the capital structure.",
      },
      {
        title: "Flexible Capital Stack",
        body: "Senior bridge plus mezzanine options help sponsors preserve equity for capex, PIP, and working capital.",
      },
      {
        title: "Fast Screening Discipline",
        body: "Indicative terms within 48 hours when business plan, sources & uses, and operator track record are complete.",
      },
    ],
    leadership: [
      {
        name: "Mark Delaney",
        title: "Managing Partner",
        role: "Originations · West Coast",
        bio: "Founded Pacific Lodging Capital after two decades in hospitality debt and special situations investing.",
        imageUrl: LEADER_IMG.a,
      },
      {
        name: "Rachel Kim",
        title: "Partner, Portfolio",
        role: "Asset Management",
        bio: "Leads portfolio surveillance, workout strategy, and lender–operator coordination on transitional assets.",
        imageUrl: LEADER_IMG.b,
      },
      {
        name: "Tomás Herrera",
        title: "Director, Underwriting",
        role: "Credit · Hospitality",
        bio: "Underwrites bridge and mezzanine opportunities with focus on sponsor liquidity and business plan credibility.",
        imageUrl: LEADER_IMG.c,
      },
    ],
    trackRecord: [
      {
        name: "Lifestyle Hotel Repositioning",
        location: "Scottsdale, AZ",
        dealType: "Bridge",
        loanAmount: "$32M",
        year: "2025",
        summary: "Bridge senior plus mezz for full-service lifestyle conversion with 24-month stabilization plan.",
        imageUrl: HOTEL_IMG.b,
      },
      {
        name: "Resort Recapitalization",
        location: "Lake Tahoe, NV",
        dealType: "Recapitalization",
        loanAmount: "$45M",
        year: "2024",
        summary: "Recapitalized seasonal resort after operator transition and revenue management reset.",
        imageUrl: HOTEL_IMG.c,
      },
      {
        name: "Urban Full-Service Bridge",
        location: "San Diego, CA",
        dealType: "Acquisition",
        loanAmount: "$38M",
        year: "2023",
        summary: "Acquisition bridge for underperforming full-service asset with new operator and capex program.",
        imageUrl: HOTEL_IMG.a,
      },
    ],
  },
  "cp-3": {
    headquarters: "Miami, FL",
    shortDescription: "Cross-border hotel lending for Caribbean and Latin America tourism markets.",
    portfolioStats: {
      dealsFinanced: 58,
      totalVolumeLabel: "$1.6B+",
      activeMarkets: 9,
      yearsLending: 15,
    },
    currentLendingAppetiteOwner: "Active in DR, Mexico Riviera, and Cartagena resort markets",
    sponsorPreference: "U.S. sponsor equity with experienced local operating partner",
    keyDifferentiators: [
      "Cross-border structuring for U.S. and offshore lending",
      "Resort and all-inclusive specialization",
      "Local counsel and FX risk coordination",
      "Regional CALA hospitality desk coverage",
    ],
    ownerValueProps: [
      {
        title: "CALA Tourism Expertise",
        body: "Teams understand airport access, seasonality, and tourism demand drivers critical to resort underwriting.",
      },
      {
        title: "Cross-Border Execution",
        body: "Supports entity structuring, local insurance, and political risk review as part of standard process.",
      },
      {
        title: "Operator + Brand Alignment",
        body: "Comfortable with international flags and regional operators with proven CALA track records.",
      },
    ],
    leadership: [
      {
        name: "Elena Vargas",
        title: "Director, CALA Hospitality",
        role: "Originations · CALA",
        bio: "Leads Caribbean and Latin America hospitality origination. Bilingual deal execution across U.S. and offshore structures.",
        imageUrl: LEADER_IMG.b,
      },
      {
        name: "Roberto Fuentes",
        title: "Credit Committee Chair",
        role: "Credit · CALA",
        bio: "Chairs regional hotel credit decisions with focus on tourism market depth and sponsor-local partner alignment.",
        imageUrl: LEADER_IMG.a,
      },
    ],
    trackRecord: [
      {
        name: "Riviera Resort Acquisition",
        location: "Playa del Carmen, Mexico",
        dealType: "Acquisition",
        loanAmount: "$24M",
        year: "2025",
        summary: "Acquisition financing for branded resort with U.S. sponsor and regional operator.",
        imageUrl: HOTEL_IMG.c,
      },
      {
        name: "All-Inclusive Conversion",
        location: "Punta Cana, DR",
        dealType: "Conversion",
        loanAmount: "$31M",
        year: "2024",
        summary: "Supported conversion to all-inclusive model with tourism demand study and insurance review.",
        imageUrl: HOTEL_IMG.a,
      },
    ],
  },
  "cp-4": {
    headquarters: "New York, NY",
    shortDescription: "Conduit lender for stabilized, securitization-ready branded hotels.",
    portfolioStats: {
      dealsFinanced: 210,
      totalVolumeLabel: "$12B+",
      activeMarkets: 45,
      yearsLending: 28,
    },
    currentLendingAppetiteOwner: "Active for select-service and extended-stay; cautious on urban full-service",
    sponsorPreference: "Institutional sponsors with stabilized operating history",
    keyDifferentiators: [
      "National CMBS distribution and rate lock capability",
      "Stabilized asset focus with predictable cash flow",
      "Top-50 MSA market preference",
      "Broker network with hospitality specialization",
    ],
    ownerValueProps: [
      {
        title: "Fixed-Rate Conduit Execution",
        body: "Owners seeking long-term fixed-rate senior debt on stabilized flagged assets benefit from securitization market access.",
      },
      {
        title: "Process Transparency",
        body: "Formal application timeline with clear third-party report requirements and closing milestones.",
      },
      {
        title: "Scale for Larger Deals",
        body: "Comfortable with loans from $25M to $150M on investment-grade select-service and extended-stay.",
      },
    ],
    leadership: [
      {
        name: "Diane Foster",
        title: "Head of Lodging Originations",
        role: "Conduit · National",
        bio: "Leads hospitality conduit originations with focus on select-service and extended-stay securitization.",
        imageUrl: LEADER_IMG.b,
      },
      {
        name: "Michael Chen",
        title: "SVP, Underwriting",
        role: "Credit · CMBS",
        bio: "Oversees hotel underwriting standards, DSCR sizing, and franchise term review for conduit loans.",
        imageUrl: LEADER_IMG.d,
      },
    ],
    trackRecord: [
      {
        name: "Extended-Stay Portfolio Loan",
        location: "National USA",
        dealType: "Refinance",
        loanAmount: "$86M",
        year: "2025",
        summary: "Fixed-rate conduit refinance for 6-property extended-stay portfolio.",
        imageUrl: HOTEL_IMG.a,
      },
      {
        name: "Marriott Select-Service Acquisition",
        location: "Denver, CO",
        dealType: "Acquisition",
        loanAmount: "$34M",
        year: "2024",
        summary: "Stabilized acquisition with 72% LTV and 10-year fixed conduit structure.",
        imageUrl: HOTEL_IMG.b,
      },
    ],
  },
  "cp-5": {
    headquarters: "Chicago, IL",
    shortDescription: "Long-duration fixed-rate senior debt for institutional-quality hotel assets.",
    portfolioStats: {
      dealsFinanced: 72,
      totalVolumeLabel: "$8.5B+",
      activeMarkets: 22,
      yearsLending: 35,
    },
    currentLendingAppetiteOwner: "Selective — core and core-plus gateway markets only",
    sponsorPreference: "Institutional sponsors with multi-asset track record",
    keyDifferentiators: [
      "10-year fixed-rate life company balance sheet",
      "Institutional full-service and luxury focus",
      "Relationship-led, invite-only pipeline",
      "Conservative leverage and DSCR standards",
    ],
    ownerValueProps: [
      {
        title: "Long-Duration Fixed Rate",
        body: "Ideal for owners seeking stable, long-tenor senior debt on cash-flowing institutional assets.",
      },
      {
        title: "Institutional Credit Standards",
        body: "Aligns with owners pursuing conservative leverage and predictable debt service profiles.",
      },
      {
        title: "Relationship Continuity",
        body: "Dedicated relationship manager through underwriting, closing, and ongoing asset surveillance.",
      },
    ],
    leadership: [
      {
        name: "William Hayes",
        title: "Managing Director, Real Estate",
        role: "Originations · Life Co",
        bio: "Leads hospitality origination for institutional hotel assets across gateway markets.",
        imageUrl: LEADER_IMG.a,
      },
    ],
    trackRecord: [
      {
        name: "Luxury Full-Service Refinance",
        location: "Boston, MA",
        dealType: "Refinance",
        loanAmount: "$95M",
        year: "2024",
        summary: "10-year fixed refinance for upper-upscale full-service asset with national operator.",
        imageUrl: HOTEL_IMG.c,
      },
    ],
  },
  "cp-6": {
    headquarters: "Washington, DC",
    shortDescription: "Internal reference desk for HUD/FHA-insured hotel financing programs.",
    portfolioStats: {
      dealsFinanced: 14,
      totalVolumeLabel: "$480M+",
      activeMarkets: 1,
      yearsLending: 8,
    },
    currentLendingAppetiteOwner: "Program-dependent — verify eligibility before outreach",
    sponsorPreference: "Experienced HUD program operators",
    keyDifferentiators: [
      "HUD program eligibility screening",
      "Coordination with approved HUD lenders",
      "Long-duration insured debt expertise",
    ],
    ownerValueProps: [
      {
        title: "Program Navigation",
        body: "Helps owners understand HUD hospitality program eligibility before engaging formal application.",
      },
    ],
    leadership: [],
    trackRecord: [],
  },
};

export function mergeCapitalProviderPresentation(record) {
  if (!record) return null;
  const extra = CAPITAL_PROVIDER_PRESENTATION[record.id] || {};
  return { ...record, ...extra };
}
