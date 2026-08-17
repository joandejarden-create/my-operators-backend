/**
 * Operator Explorer taxonomy — organized by operator type (not chain scale).
 */

/** @typedef {{ parent?: string, operators: string[] }} OperatorGroup */
/** @typedef {{
 *   id: string,
 *   seedId: string,
 *   recordId: string,
 *   stepNumber: number,
 *   taskName: string,
 *   deliverables: string,
 *   priorityNote: string,
 *   operators: string[],
 * }} OperatorExplorerSegment
 */

/** @type {OperatorExplorerSegment[]} */
export const OPERATOR_EXPLORER_SEGMENTS = [
  {
    id: "third-party-institutional",
    seedId: "explorer-operator-01-third-party",
    recordId: "recsyq5eyLYQq3nwW",
    stepNumber: 14,
    taskName: "Complete Operator Explorer profiles — Third-party / institutional operators",
    deliverables: "Completed Third-party / institutional Operator Explorer checklist and profile updates",
    priorityNote:
      "Answer: would this third-party / institutional operator be a fit for this owner/opportunity?",
    operators: [
      "Aimbridge Hospitality",
      "Hotel Equities",
      "Remington Hospitality",
      "Highgate",
      "Pyramid Global Hospitality",
      "HHM Hotels",
      "Davidson Hospitality",
      "HEI Hotels & Resorts",
      "Crescent Hotels & Resorts",
      "Benchmark / Pyramid Luxury & Lifestyle",
      "Sage Hospitality",
      "Valor Hospitality",
      "PM Hotel Group",
      "GF Hotels & Resorts",
    ],
  },
  {
    id: "regional-cala-owner-operator",
    seedId: "explorer-operator-02-regional-cala",
    recordId: "recGA1BQsbLFBVuEA",
    stepNumber: 15,
    taskName: "Complete Operator Explorer profiles — Regional / CALA / owner-operators",
    deliverables: "Completed Regional / CALA / owner-operator Explorer checklist and profile updates",
    priorityNote: "CALA, regional, and owner-operator fit for pilot markets.",
    operators: [
      "Grupo Marta Hospitality",
      "Enchanting Hotels Collection",
      "Faranda Hotels & Resorts",
      "Grupo Posadas",
      "Atlantica Hotels & Resorts",
      "PortoBay Hotels & Resorts",
      "Princess Hotels & Resorts",
      "Iberostar",
      "Minor Hotels / NH",
      "Belmond",
      "TUI Hotels & Resorts",
    ],
  },
  {
    id: "lifestyle-boutique-operator",
    seedId: "explorer-operator-03-lifestyle",
    recordId: "rec85eaHTdGcgQrEe",
    stepNumber: 17,
    taskName: "Complete Operator Explorer profiles — Lifestyle / boutique operators",
    deliverables: "Completed Lifestyle / boutique Operator Explorer checklist and profile updates",
    priorityNote: "Lifestyle and boutique operating models for conversion/repositioning conversations.",
    operators: [
      "Ennismore",
      "Kasa",
      "Life House",
      "Sonder-style operating model",
      "Bunkhouse / Hyatt lifestyle",
      "Standard International / Hyatt",
      "Starwood Hotels / SH Hotels-style lifestyle operator",
    ],
  },
  {
    id: "resort-all-inclusive-operator",
    seedId: "explorer-operator-04-resort",
    recordId: "recdkY3nfe3VDDqY5",
    stepNumber: 16,
    taskName: "Complete Operator Explorer profiles — Resort / all-inclusive operators",
    deliverables: "Completed Resort / all-inclusive Operator Explorer checklist and profile updates",
    priorityNote: "Resort and all-inclusive operator fit for Caribbean, Mexico, and leisure-heavy assets.",
    operators: [
      "Iberostar",
      "TUI / ROBINSON / TUI MAGIC LIFE",
      "Hyatt Inclusive Collection",
      "Playa Hotels & Resorts",
      "Apple Leisure / ALG legacy context",
      "Blue Diamond Resorts",
      "Palladium Hotel Group",
      "Meliá",
      "Barceló",
      "RIU",
    ],
  },
];

export function buildOperatorScopeDescription(segment) {
  return [
    "Purpose:",
    `- Complete Operator Explorer profiles for ${segment.taskName.replace("Complete Operator Explorer profiles — ", "")}.`,
    "",
    segment.priorityNote,
    "",
    "Scope:",
    segment.operators.map((o) => `- ${o}`).join("\n"),
    "",
    "For each operator profile capture: operating model, geography, asset fit, owner economics, engagement style, pilot relevance.",
    "Flag unknown fields rather than guessing.",
    "",
    "Completion standard:",
    "- Every listed operator has its own Explorer coverage row with status and next action.",
    "- Profiles usable in pilot demos and owner/advisor fit conversations.",
  ].join("\n");
}
