/**
 * Sample payload for manual Market Demand MVP testing.
 * Use with POST /api/deals/:dealId/import-demand-centers
 */

export const SAMPLE_DEMAND_CENTER_CATEGORIES = [
  "Airport",
  "Cruise Port",
  "Historic District",
  "Convention Center",
  "Hospital",
  "University",
  "Beach District",
  "Shopping / Mixed-Use Area",
  "Government District",
  "Stadium / Arena",
  "Business District",
  "Industrial / Logistics Area",
];

export const SAMPLE_IMPORT_PAYLOAD = {
  demandCenters: [
    {
      name: "Sample International Airport",
      category: "Transportation",
      subcategory: "Airport",
      distanceFromDeal: 12,
      estimatedDriveTime: 18,
      demandStrength: "High",
      relevanceToHotelDemand: "High",
      demandPattern: ["Transient", "Year-Round"],
      relevantHotelTypes: ["Select-Service", "Full-Service", "Airport", "Midscale"],
      source: ["Manual Research"],
      dataConfidence: "Medium",
      relevanceScore: 90,
      notes: "Sample demand center for MVP testing.",
    },
    {
      name: "Downtown Convention Center",
      category: "Group / Event",
      subcategory: "Convention Center",
      distanceFromDeal: 4,
      estimatedDriveTime: 12,
      demandStrength: "Medium",
      relevanceToHotelDemand: "High",
      demandPattern: ["Group", "Weekday", "Seasonal"],
      relevantHotelTypes: ["Full-Service", "Upper Upscale"],
      source: ["Manual Research"],
      dataConfidence: "Medium",
      relevanceScore: 78,
      notes: "Sample group demand anchor.",
    },
    {
      name: "Historic Waterfront District",
      category: "Leisure",
      subcategory: "Historic District",
      distanceFromDeal: 2,
      estimatedDriveTime: 8,
      demandStrength: "High",
      relevanceToHotelDemand: "Medium",
      demandPattern: ["Leisure", "Weekend"],
      relevantHotelTypes: ["Lifestyle", "Boutique", "Upper Upscale"],
      source: ["Manual Research"],
      dataConfidence: "Low",
      relevanceScore: 65,
      notes: "Sample leisure demand for scoring coverage.",
    },
  ],
};
