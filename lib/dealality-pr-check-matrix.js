/**
 * Machine-readable PR validation matrix.
 * Human docs: docs/dealality-pr-validation-matrix.md
 */

/** @typedef {{ id: string, label: string, risk: "Low"|"Medium"|"High", commands: string[], match: (filePath: string) => boolean }} PrCheckRule */

/** @type {PrCheckRule[]} */
export const PR_CHECK_RULES = [
  {
    id: "auth",
    label: "Auth / Memberstack",
    risk: "High",
    commands: ["npm run test:batch1-route-auth"],
    match: (p) =>
      /^api\/(me|auth-)/.test(p) ||
      p.includes("auth-memberstack") ||
      p.includes("memberstack"),
  },
  {
    id: "operator-api",
    label: "Operator Setup API / writer",
    risk: "High",
    commands: [
      "npm run test:batch2a-route-auth",
      "npm run test:operator-setup-new-base-save-coverage",
    ],
    match: (p) =>
      /^api\/third-party-operator/.test(p) ||
      /^api\/lib\/operator-setup/.test(p) ||
      p.includes("third-party-operator-new-two-field-bindings.json"),
  },
  {
    id: "operator-ui",
    label: "Operator Setup / Explorer UI",
    risk: "Medium",
    commands: ["npm run test:batch2a-final-validation"],
    match: (p) =>
      /^public\/third-party-operator/.test(p) ||
      /^public\/js\/operator-(setup|explorer)/.test(p),
  },
  {
    id: "brand-fixtures",
    label: "Brand Explorer presentation fixtures",
    risk: "Medium",
    commands: [
      "npm run audit-choice-explorer-presentation-gaps",
      "npm run audit-brand-explorer-presentation-formats",
    ],
    match: (p) =>
      /^fixtures\/brand-explorer-presentation-/.test(p) ||
      /^scripts\/apply-.*explorer/.test(p) ||
      /^scripts\/apply-choice-/.test(p),
  },
  {
    id: "brand-api-ui",
    label: "Brand Explorer API / UI",
    risk: "Medium",
    commands: [],
    match: (p) =>
      p === "api/brand-library.js" ||
      /^public\/js\/brand-explorer/.test(p),
  },
  {
    id: "deals",
    label: "Deals / deal room / next action",
    risk: "High",
    commands: [
      "npm run test:batch1-cross-owner-access",
      "npm run test:deal-next-action",
    ],
    match: (p) =>
      /^api\/brand-deal-requests/.test(p) ||
      /^public\/js\/deal-/.test(p) ||
      /^public\/deal-/.test(p) ||
      /deal-room/.test(p) ||
      /\/nda[-.]/.test(p) ||
      p.includes("patch-nda-deal-room"),
  },
  {
    id: "deal-attachments",
    label: "Deal Setup CU attachments",
    risk: "High",
    commands: [
      "npm run test:deal-setup-cu-attachment-fields",
      "npm run test:deal-setup-cu-attachment-persistence",
      "npm run test:deal-setup-cu-attachment-browser-path",
    ],
    match: (p) => p.includes("deal-setup-cu-attachment") || p.includes("Contact & Uploads"),
  },
  {
    id: "gtm",
    label: "GTM / pilot / outreach",
    risk: "High",
    commands: [
      "npm run test:outreach-setup-field-map",
      "npm run test:owner-targets-outreach-export",
      "npm run audit-gtm-owner-target-base",
    ],
    match: (p) =>
      /^api\/(target-list|outreach-setup)/.test(p) ||
      /gtm|pilot-target|outreach-setup|owner-targets/.test(p),
  },
  {
    id: "pilot-target-tests",
    label: "Pilot Target List schema/views",
    risk: "High",
    commands: [
      "npm run test:pilot-target-list-field-descriptions",
      "npm run test:pilot-target-list-dropdown-options",
      "npm run test:pilot-target-list-views",
    ],
    match: (p) => /^scripts\/(setup|test)-pilot-target-list/.test(p),
  },
  {
    id: "master-todo",
    label: "Dealality Master To-Do",
    risk: "Medium",
    commands: [
      "npm run test:dealality-master-todo",
      "node scripts/audit-dealality-master-todo-structure.mjs --dry-run",
    ],
    match: (p) =>
      /^lib\/dealality-master-todo\//.test(p) ||
      /master-todo|founder-project-plan-phase/.test(p),
  },
  {
    id: "market-demand",
    label: "Market Demand",
    risk: "Medium",
    commands: ["npm run test:market-demand", "npm run validate:market-demand"],
    match: (p) => /market-demand/.test(p),
  },
  {
    id: "scout",
    label: "Scout modules",
    risk: "Medium",
    commands: [],
    match: (p) => /^public\/js\/scout-/.test(p) || /scout-/.test(p),
  },
  {
    id: "travel-infra",
    label: "Travel Infrastructure / Radar",
    risk: "Medium",
    commands: [
      "npm run test:travel-infrastructure-radar",
      "npm run test:market-ti-audit-config",
    ],
    match: (p) =>
      /^lib\/(travel-infrastructure|radar-buildout)\//.test(p) ||
      /^scripts\/audit-market-travel-infrastructure/.test(p) ||
      /^scripts\/backfill-.*-ti/.test(p),
  },
  {
    id: "partner-intelligence",
    label: "Partner Intelligence",
    risk: "High",
    commands: ["npm run ensure-partner-intelligence-tables"],
    match: (p) => /partner-intelligence/.test(p),
  },
  {
    id: "airtable-automations",
    label: "Airtable automations",
    risk: "Medium",
    commands: [],
    match: (p) => /^airtable\/automations\//.test(p),
  },
  {
    id: "package-json",
    label: "Dependencies / npm scripts",
    risk: "Medium",
    commands: [],
    match: (p) => p === "package.json" || p === "package-lock.json",
  },
];

const SCOUT_TEST_BY_PATH = [
  { re: /scout-demand-overlays/, cmd: "npm run test:scout-demand-overlays" },
  { re: /scout-market-map/, cmd: "npm run test:scout-market-map" },
  { re: /scout-market-insights/, cmd: "npm run test:scout-insight-review" },
  { re: /scout-insight-review/, cmd: "npm run test:scout-insight-review" },
  { re: /scout-market-coverage/, cmd: "npm run test:scout-market-coverage" },
  { re: /scout-signal-watchlist/, cmd: "npm run test:scout-signal-watchlist" },
  { re: /scout-opportunity/, cmd: "npm run test:scout-opportunity-signals" },
];

/**
 * @param {string[]} changedFiles — repo-relative posix paths
 */
export function suggestPrChecks(changedFiles) {
  const normalized = changedFiles.map((f) => f.replace(/\\/g, "/")).filter(Boolean);
  /** @type {Map<string, { id: string, label: string, risk: string, commands: Set<string>, files: Set<string> }>} */
  const matched = new Map();

  for (const file of normalized) {
    for (const rule of PR_CHECK_RULES) {
      if (!rule.match(file)) continue;
      let entry = matched.get(rule.id);
      if (!entry) {
        entry = {
          id: rule.id,
          label: rule.label,
          risk: rule.risk,
          commands: new Set(rule.commands),
          files: new Set(),
        };
        matched.set(rule.id, entry);
      }
      entry.files.add(file);
      for (const cmd of rule.commands) entry.commands.add(cmd);
    }

    for (const scout of SCOUT_TEST_BY_PATH) {
      if (!scout.re.test(file)) continue;
      let entry = matched.get("scout");
      if (!entry) {
        entry = {
          id: "scout",
          label: "Scout modules",
          risk: "Medium",
          commands: new Set(),
          files: new Set(),
        };
        matched.set("scout", entry);
      }
      entry.files.add(file);
      entry.commands.add(scout.cmd);
    }
  }

  const rules = [...matched.values()].sort((a, b) => {
    const order = { High: 0, Medium: 1, Low: 2 };
    return (order[a.risk] ?? 9) - (order[b.risk] ?? 9);
  });

  const allCommands = [...new Set(rules.flatMap((r) => [...r.commands]))];
  const maxRisk =
    rules.find((r) => r.risk === "High")?.risk ||
    rules.find((r) => r.risk === "Medium")?.risk ||
    rules.find((r) => r.risk === "Low")?.risk ||
    "Low";

  return { rules, allCommands, maxRisk, changedFiles: normalized };
}
