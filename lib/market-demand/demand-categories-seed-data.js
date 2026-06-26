/**
 * Demand Categories reference seed data (Market Demand Intelligence).
 */

export const DEMAND_CATEGORIES_SEED_ROWS = [
  {
    category: "Leisure",
    description:
      "Visitor, recreation, and experience-driven demand tied to attractions, waterfronts, entertainment districts, and destination appeal.",
    typicalDemandPattern: ["Weekend", "Seasonal", "Leisure"],
    mostRelevantHotelTypes: ["Lifestyle", "Boutique", "Upper Upscale", "Resort", "Full-Service"],
    brandFitImplications:
      "May support resort, lifestyle, soft brand, or upper-upscale positioning when the asset can deliver experience-led product.",
    operatorFitImplications:
      "Operator should understand leisure segmentation, OTA/channel strategy, and experience-driven positioning.",
    scoringWeight: 10,
  },
  {
    category: "Corporate",
    description:
      "Weekday and account-driven demand from offices, business parks, and regional employment centers.",
    typicalDemandPattern: ["Weekday", "Transient"],
    mostRelevantHotelTypes: ["Select-Service", "Full-Service", "Upper Upscale"],
    brandFitImplications:
      "May support weekday transient, negotiated account, and select-service or full-service positioning.",
    operatorFitImplications:
      "Operator should have local sales and account management capability for corporate demand capture.",
    scoringWeight: 10,
  },
  {
    category: "Group / Event",
    description:
      "Meeting, convention, wedding, sports, and event-driven demand from venues and event calendars.",
    typicalDemandPattern: ["Group", "Seasonal", "Weekday"],
    mostRelevantHotelTypes: ["Full-Service", "Upper Upscale", "Convention"],
    brandFitImplications:
      "May support brands and operators with meaningful group sales and event catering capability.",
    operatorFitImplications:
      "Operator group sales capability and event operations experience may matter.",
    scoringWeight: 10,
  },
  {
    category: "Medical",
    description:
      "Demand associated with hospitals, medical campuses, and recurring patient and visitor stays.",
    typicalDemandPattern: ["Year-Round", "Weekday"],
    mostRelevantHotelTypes: ["Select-Service", "Extended-Stay", "Midscale"],
    brandFitImplications:
      "May support extended-stay, select-service, and family or visitor-oriented product.",
    operatorFitImplications:
      "Operator should understand extended-stay patterns and recurring local medical demand.",
    scoringWeight: 8,
  },
  {
    category: "Education",
    description:
      "University, college, and school-related demand including visiting families, faculty, sports, and events.",
    typicalDemandPattern: ["Seasonal", "Weekend", "Event"],
    mostRelevantHotelTypes: ["Select-Service", "Midscale", "Extended-Stay"],
    brandFitImplications:
      "May support event-based, visiting family, sports, faculty, and seasonal demand strategies.",
    operatorFitImplications:
      "Operator should understand academic calendars, event spikes, and recurring visitor demand.",
    scoringWeight: 8,
  },
  {
    category: "Transportation",
    description:
      "Airport, cruise port, highway corridor, and transit-hub demand with short booking windows.",
    typicalDemandPattern: ["Transient", "Year-Round"],
    mostRelevantHotelTypes: ["Select-Service", "Airport", "Midscale", "Full-Service"],
    brandFitImplications:
      "May support select-service, transient, airport, or short-stay demand strategies.",
    operatorFitImplications:
      "Operator should understand transient, crew, airport, and short-booking-window demand.",
    scoringWeight: 10,
  },
  {
    category: "Industrial",
    description:
      "Project, crew, logistics, and manufacturing-adjacent demand from industrial and trade zones.",
    typicalDemandPattern: ["Weekday", "Year-Round"],
    mostRelevantHotelTypes: ["Extended-Stay", "Midscale", "Select-Service"],
    brandFitImplications:
      "May support extended-stay, midscale, and select-service demand near industrial corridors.",
    operatorFitImplications:
      "Operator should understand crew, project, and extended-stay business.",
    scoringWeight: 7,
  },
  {
    category: "Retail / Mixed-Use",
    description:
      "Urban, lifestyle, and mixed-use district demand tied to shopping, dining, and walkable environments.",
    typicalDemandPattern: ["Weekend", "Leisure", "Weekday"],
    mostRelevantHotelTypes: ["Lifestyle", "Boutique", "Upper Upscale", "Full-Service"],
    brandFitImplications:
      "May support lifestyle, urban, leisure, and mixed-use positioning.",
    operatorFitImplications:
      "Operator should understand urban leisure mix and retail-adjacent demand patterns.",
    scoringWeight: 8,
  },
  {
    category: "Government",
    description:
      "Civic, government campus, embassy, and public-sector related demand.",
    typicalDemandPattern: ["Weekday", "Year-Round"],
    mostRelevantHotelTypes: ["Select-Service", "Full-Service", "Extended-Stay"],
    brandFitImplications:
      "May support government-contract, extended-stay, and select-service demand near civic centers.",
    operatorFitImplications:
      "Operator should understand contract, extended-stay, and weekday public-sector demand.",
    scoringWeight: 6,
  },
  {
    category: "Other",
    description:
      "Demand drivers that do not map cleanly to a primary category; use when context is useful but classification is uncertain.",
    typicalDemandPattern: ["Year-Round"],
    mostRelevantHotelTypes: ["Select-Service", "Full-Service"],
    brandFitImplications:
      "Use as a holding category until demand drivers are validated and reclassified.",
    operatorFitImplications:
      "Clarify demand source before relying on this category for operator or brand positioning.",
    scoringWeight: 3,
  },
];
