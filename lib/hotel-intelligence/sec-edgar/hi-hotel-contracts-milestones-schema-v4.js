/**
 * Founder-approved HI Hotel Contracts / HI Contract Milestones schema blueprint (V4).
 * Exact Meta API field definitions + link graph. Apply remains founder-gated.
 */

export const HI_CONTRACTS_MILESTONES_SCHEMA_V4 = "hi-hotel-contracts-milestones-schema-v4";

export const CANONICAL_BASE_ID = "appCCUsuGsE1ifoLk";
export const TABLE_IDS = Object.freeze({
  hpc: "tbl9aY5ijiuIzzWam",
  inventory: "tblx77Nmh56h4xa9z",
  findings: "tblGjTWNm8G4UOF1e",
  organizations: "tbliXEfUOWk32AN9y",
  relationships: "tblGiwN0foXXDESvZ",
  graphEvidence: "tblc9JhKT1edr4grE",
});

export const PUBLICATION_STATE_CHOICES = ["CUSTOMER_BLOCKED", "INTERNAL_ONLY", "DO_NOT_PUBLISH"];
export const PUBLICATION_STATE_DEFAULT = "CUSTOMER_BLOCKED";

export const CONTRACT_STATUS_CHOICES = [
  "CURRENTLY_CORROBORATED",
  "HISTORICALLY_VERIFIED",
  "FUTURE_DATE_REQUIRES_REVALIDATION",
  "EXPIRED_STATUS_UNCONFIRMED",
  "SUPERSEDED",
  "TERMINATED_VERIFIED",
  "CONFLICTED",
];

export const AGREEMENT_TYPE_CHOICES = ["FRANCHISE", "MANAGEMENT", "LEASE", "LICENSE", "OTHER"];
export const AGREEMENT_ROLE_CHOICES = [
  "ORIGINAL",
  "AMENDMENT",
  "RENEWAL_EXTENSION",
  "TERMINATION_INSTRUMENT",
];
export const GEO_CLASS_CHOICES = [
  "PRIORITY_HPC_ENRICHMENT",
  "GLOBAL_RESEARCH_STANDARD",
  "EXISTING_HPC_OPPORTUNISTIC",
];
export const CONTINENT_CHOICES = [
  "NORTH_AMERICA",
  "CENTRAL_AMERICA",
  "SOUTH_AMERICA",
  "CARIBBEAN",
  "EUROPE",
  "MIDDLE_EAST",
  "AFRICA",
  "ASIA",
  "OCEANIA",
  "OTHER_UNKNOWN",
];
export const MILESTONE_TYPE_CHOICES = [
  "FRANCHISE_EXPIRATION",
  "MANAGEMENT_EXPIRATION",
  "COMMENCEMENT",
  "RENEWAL",
  "EXTENSION",
  "AMENDMENT_EFFECTIVE",
  "TERMINATION",
  "REFLAG",
  "OPTION_EXERCISE",
  "OTHER",
];
export const DATE_PRECISION_CHOICES = ["EXACT_DAY", "MONTH_YEAR", "YEAR_ONLY", "NARRATIVE"];
export const EVIDENCE_CONFIDENCE_CHOICES = ["HIGH", "MEDIUM", "LOW", "UNVERIFIED"];

function select(choices) {
  return { choices: choices.map((name) => ({ name })) };
}

function dateIso() {
  return { dateFormat: { name: "iso" } };
}

/**
 * Contract fields that can be created with the table (no self-link yet).
 * Placeholders TABLE_IDS used for outbound links.
 */
export function buildContractCreateFields({ inventoryId, hpcId, orgsId }) {
  return [
    { name: "Contract ID", type: "singleLineText" },
    {
      name: "Discovery Property",
      type: "multipleRecordLinks",
      options: { linkedTableId: inventoryId },
    },
    {
      name: "HPC Hotel",
      type: "multipleRecordLinks",
      options: { linkedTableId: hpcId },
    },
    {
      name: "Agreement Type",
      type: "singleSelect",
      options: select(AGREEMENT_TYPE_CHOICES),
    },
    {
      name: "Agreement Role",
      type: "singleSelect",
      options: select(AGREEMENT_ROLE_CHOICES),
    },
    {
      name: "Counterparty Org",
      type: "multipleRecordLinks",
      options: { linkedTableId: orgsId },
    },
    { name: "Counterparty Name Raw", type: "singleLineText" },
    { name: "Brand Family", type: "singleLineText" },
    { name: "Brand Label", type: "singleLineText" },
    { name: "Governing Law / Jurisdiction", type: "singleLineText" },
    { name: "Hotel Country", type: "singleLineText" },
    { name: "Hotel State / Region", type: "singleLineText" },
    { name: "Hotel City", type: "singleLineText" },
    {
      name: "Geographic Processing Class",
      type: "singleSelect",
      options: select(GEO_CLASS_CHOICES),
    },
    {
      name: "Contract Status",
      type: "singleSelect",
      options: select(CONTRACT_STATUS_CHOICES),
    },
    { name: "Commencement Date", type: "date", options: dateIso() },
    { name: "Scheduled Expiration Date", type: "date", options: dateIso() },
    { name: "Term Years", type: "number", options: { precision: 0 } },
    { name: "Renewal Options Summary", type: "multilineText" },
    { name: "Termination Rights Summary", type: "multilineText" },
    { name: "Primary Source URL", type: "url" },
    { name: "Primary SEC Accession", type: "singleLineText" },
    {
      name: "Evidence Confidence",
      type: "singleSelect",
      options: select(EVIDENCE_CONFIDENCE_CHOICES),
    },
    {
      name: "Publication State",
      type: "singleSelect",
      options: select(PUBLICATION_STATE_CHOICES),
    },
    { name: "Schema Version", type: "singleLineText" },
    { name: "Notes / Quarantine", type: "multilineText" },
  ];
}

/** Self-links added after Contracts table id is known. */
export function buildContractSelfLinkFields(contractsTableId) {
  return [
    {
      name: "Supersedes Contract",
      type: "multipleRecordLinks",
      options: { linkedTableId: contractsTableId },
    },
    {
      name: "Superseded By Contract",
      type: "multipleRecordLinks",
      options: { linkedTableId: contractsTableId },
    },
  ];
}

/**
 * Milestone create fields. Contract link REQUIRES contractsTableId.
 */
export function buildMilestoneCreateFields({
  contractsTableId,
  inventoryId,
  hpcId,
  findingsId,
}) {
  if (!contractsTableId) {
    throw new Error("contractsTableId required for Milestone Contract link");
  }
  return [
    { name: "Milestone ID", type: "singleLineText" },
    {
      name: "Contract",
      type: "multipleRecordLinks",
      options: { linkedTableId: contractsTableId },
    },
    {
      name: "Discovery Property",
      type: "multipleRecordLinks",
      options: { linkedTableId: inventoryId },
    },
    {
      name: "HPC Hotel",
      type: "multipleRecordLinks",
      options: { linkedTableId: hpcId },
    },
    {
      name: "Milestone Type",
      type: "singleSelect",
      options: select(MILESTONE_TYPE_CHOICES),
    },
    { name: "Milestone Date", type: "date", options: dateIso() },
    {
      name: "Date Meaning Precision",
      type: "singleSelect",
      options: select(DATE_PRECISION_CHOICES),
    },
    { name: "Reporting Date", type: "date", options: dateIso() },
    {
      name: "Current Status Class",
      type: "singleSelect",
      options: select(CONTRACT_STATUS_CHOICES),
    },
    {
      name: "Status Corroborated",
      type: "checkbox",
      options: { color: "greenBright", icon: "check" },
    },
    { name: "Commercial Relevance Score", type: "number", options: { precision: 0 } },
    { name: "Source URL", type: "url" },
    { name: "SEC Accession", type: "singleLineText" },
    { name: "Evidence Snippet", type: "multilineText" },
    {
      name: "Related Finding",
      type: "multipleRecordLinks",
      options: { linkedTableId: findingsId },
    },
    {
      name: "Publication State",
      type: "singleSelect",
      options: select(PUBLICATION_STATE_CHOICES),
    },
    { name: "Schema Version", type: "singleLineText" },
    { name: "Notes", type: "multilineText" },
  ];
}

export function buildMilestoneSelfLinkFields(milestonesTableId) {
  return [
    {
      name: "Superseded By Milestone",
      type: "multipleRecordLinks",
      options: { linkedTableId: milestonesTableId },
    },
  ];
}

export function buildAdditiveInventoryFields() {
  return [
    {
      name: "Geographic Processing Class",
      type: "singleSelect",
      options: select(GEO_CLASS_CHOICES),
    },
    {
      name: "Continent / Macro Region",
      type: "singleSelect",
      options: select(CONTINENT_CHOICES),
    },
  ];
}

export function buildFindingsLinkFields({ contractsTableId, milestonesTableId }) {
  return [
    {
      name: "Linked Contract",
      type: "multipleRecordLinks",
      options: { linkedTableId: contractsTableId },
    },
    {
      name: "Linked Milestone",
      type: "multipleRecordLinks",
      options: { linkedTableId: milestonesTableId },
    },
  ];
}

/** Ordered apply steps for Meta API (never execute without founder auth). */
export function buildApplySequence({ inventoryId, hpcId, orgsId, findingsId }) {
  const contractCreate = buildContractCreateFields({ inventoryId, hpcId, orgsId });
  return {
    steps: [
      {
        step: 1,
        action: "CREATE_TABLE",
        name: "HI Hotel Contracts",
        fields: contractCreate,
        fieldCount: contractCreate.length,
        note: "Self-links Supersedes/Superseded By added in step 2 after table id known",
      },
      {
        step: 2,
        action: "ADD_FIELDS",
        table: "HI Hotel Contracts",
        fields: ["Supersedes Contract", "Superseded By Contract"],
        fieldBuilders: "buildContractSelfLinkFields(contractsTableId)",
        fieldCount: 2,
      },
      {
        step: 3,
        action: "CREATE_TABLE",
        name: "HI Contract Milestones",
        requiredPrior: "HI Hotel Contracts table id",
        fieldsInclude: [
          "Contract (multipleRecordLinks → HI Hotel Contracts) — REQUIRED",
          ...buildMilestoneCreateFields({
            contractsTableId: "PLACEHOLDER_CONTRACTS_TABLE_ID",
            inventoryId,
            hpcId,
            findingsId,
          }).map((f) => f.name),
        ],
        fieldCount: buildMilestoneCreateFields({
          contractsTableId: "PLACEHOLDER_CONTRACTS_TABLE_ID",
          inventoryId,
          hpcId,
          findingsId,
        }).length,
      },
      {
        step: 4,
        action: "ADD_FIELDS",
        table: "HI Contract Milestones",
        fields: ["Superseded By Milestone"],
        fieldCount: 1,
      },
      {
        step: 5,
        action: "ADD_FIELDS",
        table: "HI Hotel Discovery Inventory",
        tableId: inventoryId,
        fields: buildAdditiveInventoryFields().map((f) => f.name),
        fieldCount: 2,
      },
      {
        step: 6,
        action: "ADD_FIELDS",
        table: "HI Discovery Findings",
        tableId: findingsId,
        fields: ["Linked Contract", "Linked Milestone"],
        fieldCount: 2,
        note: "Requires Contracts + Milestones table ids",
      },
    ],
    totals: {
      contractFieldsOnCreate: contractCreate.length,
      contractSelfLinks: 2,
      contractFieldsComplete: contractCreate.length + 2,
      milestoneFieldsOnCreate: buildMilestoneCreateFields({
        contractsTableId: "x",
        inventoryId,
        hpcId,
        findingsId,
      }).length,
      milestoneSelfLinks: 1,
      milestoneFieldsComplete:
        buildMilestoneCreateFields({
          contractsTableId: "x",
          inventoryId,
          hpcId,
          findingsId,
        }).length + 1,
      additiveInventory: 2,
      additiveFindings: 2,
    },
    publicationState: {
      contracts: PUBLICATION_STATE_DEFAULT,
      milestones: PUBLICATION_STATE_DEFAULT,
      enforcement:
        "Writers must set Publication State=CUSTOMER_BLOCKED; fail-closed if unset. Airtable Meta singleSelect has no reliable server default — application-enforced.",
    },
  };
}

export function expectedLinkGraph() {
  return [
    {
      from: "HI Hotel Contracts",
      field: "Discovery Property",
      to: "HI Hotel Discovery Inventory",
      required: true,
    },
    { from: "HI Hotel Contracts", field: "HPC Hotel", to: "Hotel Property Census", required: false },
    {
      from: "HI Hotel Contracts",
      field: "Counterparty Org",
      to: "HI Organizations",
      required: false,
    },
    {
      from: "HI Hotel Contracts",
      field: "Supersedes Contract",
      to: "HI Hotel Contracts",
      required: false,
    },
    {
      from: "HI Hotel Contracts",
      field: "Superseded By Contract",
      to: "HI Hotel Contracts",
      required: false,
    },
    {
      from: "HI Contract Milestones",
      field: "Contract",
      to: "HI Hotel Contracts",
      required: true,
      type: "multipleRecordLinks",
    },
    {
      from: "HI Contract Milestones",
      field: "Discovery Property",
      to: "HI Hotel Discovery Inventory",
      required: false,
    },
    {
      from: "HI Contract Milestones",
      field: "HPC Hotel",
      to: "Hotel Property Census",
      required: false,
    },
    {
      from: "HI Contract Milestones",
      field: "Related Finding",
      to: "HI Discovery Findings",
      required: false,
    },
    {
      from: "HI Contract Milestones",
      field: "Superseded By Milestone",
      to: "HI Contract Milestones",
      required: false,
    },
    {
      from: "HI Discovery Findings",
      field: "Linked Contract",
      to: "HI Hotel Contracts",
      required: false,
    },
    {
      from: "HI Discovery Findings",
      field: "Linked Milestone",
      to: "HI Contract Milestones",
      required: false,
    },
  ];
}

export function validateBlueprintConsistency() {
  const ids = TABLE_IDS;
  const contracts = buildContractCreateFields({
    inventoryId: ids.inventory,
    hpcId: ids.hpc,
    orgsId: ids.organizations,
  });
  const self = buildContractSelfLinkFields("tblContractsPlaceholder");
  const milestones = buildMilestoneCreateFields({
    contractsTableId: "tblContractsPlaceholder",
    inventoryId: ids.inventory,
    hpcId: ids.hpc,
    findingsId: ids.findings,
  });
  const msSelf = buildMilestoneSelfLinkFields("tblMilestonesPlaceholder");
  const contractNames = [...contracts, ...self].map((f) => f.name);
  const milestoneNames = [...milestones, ...msSelf].map((f) => f.name);
  const errors = [];
  for (const req of [
    "Contract ID",
    "Discovery Property",
    "HPC Hotel",
    "Counterparty Org",
    "Supersedes Contract",
    "Superseded By Contract",
    "Publication State",
  ]) {
    if (!contractNames.includes(req)) errors.push(`contracts missing ${req}`);
  }
  for (const req of [
    "Milestone ID",
    "Contract",
    "Discovery Property",
    "HPC Hotel",
    "Superseded By Milestone",
    "Publication State",
  ]) {
    if (!milestoneNames.includes(req)) errors.push(`milestones missing ${req}`);
  }
  const contractLink = milestones.find((f) => f.name === "Contract");
  if (!contractLink || contractLink.type !== "multipleRecordLinks") {
    errors.push("Milestone Contract must be multipleRecordLinks");
  }
  if (contractLink?.options?.linkedTableId !== "tblContractsPlaceholder") {
    errors.push("Milestone Contract must link to Contracts table");
  }
  const pubC = contracts.find((f) => f.name === "Publication State");
  const pubM = milestones.find((f) => f.name === "Publication State");
  if (!pubC?.options?.choices?.some((c) => c.name === PUBLICATION_STATE_DEFAULT)) {
    errors.push("Contracts Publication State missing CUSTOMER_BLOCKED");
  }
  if (!pubM?.options?.choices?.some((c) => c.name === PUBLICATION_STATE_DEFAULT)) {
    errors.push("Milestones Publication State missing CUSTOMER_BLOCKED");
  }
  return {
    ok: errors.length === 0,
    errors,
    counts: {
      contractsComplete: contractNames.length,
      milestonesComplete: milestoneNames.length,
      linkGraphEdges: expectedLinkGraph().length,
    },
    contractFieldNames: contractNames,
    milestoneFieldNames: milestoneNames,
  };
}

export default {
  buildContractCreateFields,
  buildContractSelfLinkFields,
  buildMilestoneCreateFields,
  buildMilestoneSelfLinkFields,
  buildAdditiveInventoryFields,
  buildFindingsLinkFields,
  buildApplySequence,
  expectedLinkGraph,
  validateBlueprintConsistency,
  TABLE_IDS,
  CANONICAL_BASE_ID,
  PUBLICATION_STATE_DEFAULT,
};
