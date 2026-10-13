/**
 * Stakeholder role targeting for Market Alerts contact enrichment (Phase A).
 * Deterministic — no provider calls.
 */

/** @typedef {{ id: string, label: string, rank: number, surfeTitles?: string[] }} StakeholderRole */

const ROLE = {
  OWNER_PRINCIPAL: {
    id: "owner_principal",
    label: "Owner principal",
    rank: 1,
    surfeTitles: ["Owner", "Founder", "Co-Founder", "Managing Partner", "Principal"],
  },
  DEVELOPER_PRINCIPAL: {
    id: "developer_principal",
    label: "Developer principal",
    rank: 1,
    surfeTitles: ["Founder", "CEO", "President", "Managing Partner", "Principal"],
  },
  HEAD_DEVELOPMENT: {
    id: "head_development",
    label: "Head / VP / SVP Development",
    rank: 2,
    surfeTitles: [
      "SVP Development",
      "VP Development",
      "Head of Development",
      "Chief Development Officer",
      "EVP Development",
      "Development Director",
    ],
  },
  PROJECT_LEAD: {
    id: "project_lead",
    label: "Project lead",
    rank: 3,
    surfeTitles: ["Project Director", "Project Executive", "Development Manager"],
  },
  CFO: {
    id: "cfo",
    label: "CFO",
    rank: 2,
    surfeTitles: ["CFO", "Chief Financial Officer"],
  },
  HEAD_INVESTMENTS: {
    id: "head_investments",
    label: "Head of Investments / CIO",
    rank: 2,
    surfeTitles: ["CIO", "Chief Investment Officer", "Head of Investments", "Managing Director Investments"],
  },
  ASSET_MANAGER: {
    id: "asset_manager",
    label: "Asset manager",
    rank: 2,
    surfeTitles: ["Asset Manager", "VP Asset Management", "Head of Asset Management"],
  },
  PREOPEN_GM: {
    id: "preopen_gm",
    label: "Pre-opening General Manager",
    rank: 1,
    surfeTitles: ["General Manager", "Opening General Manager", "Pre-Opening General Manager"],
  },
  PREOPEN_DOSM: {
    id: "preopen_dosm",
    label: "Pre-opening Director of Sales",
    rank: 2,
    surfeTitles: ["Director of Sales", "Director of Sales and Marketing", "DOSM"],
  },
  PREOPEN_REVENUE: {
    id: "preopen_revenue",
    label: "Pre-opening Director of Revenue",
    rank: 3,
    surfeTitles: ["Director of Revenue", "Revenue Director"],
  },
};

const BLOCKED_GENERIC_ROLES = [
  "receptionist",
  "reservations",
  "front desk",
  "chef",
  "spa director",
  "marketing coordinator",
  "housekeeping",
];

/**
 * @param {{ eventType?: string|null, signalType?: string|null, actionable?: boolean, brandClosed?: boolean, operatorClosed?: boolean }} input
 * @returns {StakeholderRole[]}
 */
export function inferStakeholderRoles(input = {}) {
  const eventType = input.eventType || "";
  const signalType = String(input.signalType || "");
  const brandClosed =
    input.brandClosed === true ||
    /Competitive Brand Move|Likely Decided/i.test(signalType) ||
    ["Brand Signing", "Reflag", "Conversion"].includes(eventType);
  const operatorClosed =
    input.operatorClosed === true ||
    /Competitive Operator Move|Management Agreement Announced/i.test(signalType) ||
    ["Operator Appointment", "Management Agreement", "Operator Change"].includes(eventType);

  /** @type {StakeholderRole[]} */
  let roles = [];

  switch (eventType) {
    case "Site Acquisition":
      roles = [ROLE.OWNER_PRINCIPAL, ROLE.DEVELOPER_PRINCIPAL, ROLE.HEAD_DEVELOPMENT];
      break;
    case "Planning Application":
    case "Planning Approval":
      roles = [ROLE.DEVELOPER_PRINCIPAL, ROLE.HEAD_DEVELOPMENT, ROLE.PROJECT_LEAD];
      break;
    case "Financing":
    case "JV":
    case "Recapitalization":
      roles = [ROLE.OWNER_PRINCIPAL, ROLE.DEVELOPER_PRINCIPAL, ROLE.CFO, ROLE.HEAD_INVESTMENTS, ROLE.HEAD_DEVELOPMENT];
      break;
    case "Development Proposal":
    case "New Development":
    case "Adaptive Reuse Proposal":
      roles = [ROLE.DEVELOPER_PRINCIPAL, ROLE.HEAD_DEVELOPMENT, ROLE.OWNER_PRINCIPAL];
      break;
    case "Construction Start":
    case "Construction Progress":
      roles = [ROLE.DEVELOPER_PRINCIPAL, ROLE.OWNER_PRINCIPAL, ROLE.HEAD_DEVELOPMENT, ROLE.PROJECT_LEAD];
      break;
    case "Pre-Opening Leadership":
      roles = [ROLE.PREOPEN_GM, ROLE.PREOPEN_DOSM, ROLE.PREOPEN_REVENUE, ROLE.OWNER_PRINCIPAL, ROLE.DEVELOPER_PRINCIPAL];
      break;
    case "Conversion":
    case "Reflag":
      roles = [ROLE.OWNER_PRINCIPAL, ROLE.ASSET_MANAGER, ROLE.HEAD_DEVELOPMENT];
      break;
    case "Brand Signing":
      // Brand decision closed — owner/developer only if still actionable for another audience.
      roles = input.actionable ? [ROLE.OWNER_PRINCIPAL, ROLE.DEVELOPER_PRINCIPAL, ROLE.ASSET_MANAGER] : [];
      break;
    case "Operator Appointment":
    case "Management Agreement":
    case "Operator Change":
      roles = input.actionable ? [ROLE.OWNER_PRINCIPAL, ROLE.ASSET_MANAGER] : [];
      break;
    case "Commercialization":
      roles = [ROLE.PREOPEN_DOSM, ROLE.PREOPEN_GM, ROLE.OWNER_PRINCIPAL];
      break;
    default:
      if (input.actionable) {
        roles = [ROLE.DEVELOPER_PRINCIPAL, ROLE.HEAD_DEVELOPMENT, ROLE.OWNER_PRINCIPAL];
      }
      break;
  }

  // Open brand/operator decision windows (unflagged development).
  if (input.actionable && /Potential Development|Potential Management|Reflag Opportunity|Brand White-Space/i.test(signalType)) {
    if (!brandClosed) {
      roles = mergeRoles(roles, [ROLE.OWNER_PRINCIPAL, ROLE.DEVELOPER_PRINCIPAL, ROLE.HEAD_DEVELOPMENT, ROLE.ASSET_MANAGER]);
    }
    if (!operatorClosed && /Potential Management|New Development Opportunity/i.test(signalType)) {
      roles = mergeRoles(roles, [ROLE.OWNER_PRINCIPAL, ROLE.DEVELOPER_PRINCIPAL, ROLE.ASSET_MANAGER, ROLE.HEAD_DEVELOPMENT]);
    }
  }

  if (brandClosed && eventType === "Brand Signing") {
    // Explicit: do not search Marriott brand-development people for closed franchise.
    roles = roles.filter((r) => r.id !== "head_development" || input.actionable);
  }

  return dedupeRank(roles);
}

function mergeRoles(a, b) {
  return dedupeRank([...(a || []), ...(b || [])]);
}

function dedupeRank(roles) {
  const seen = new Set();
  return [...roles]
    .filter((r) => {
      if (!r || seen.has(r.id)) return false;
      seen.add(r.id);
      return true;
    })
    .sort((x, y) => x.rank - y.rank);
}

export function isBlockedGenericRole(title = "") {
  const t = String(title || "").toLowerCase();
  return BLOCKED_GENERIC_ROLES.some((b) => t.includes(b));
}

export function stakeholderRoleLabels(roles = []) {
  return roles.map((r) => r.label);
}

export { ROLE as STAKEHOLDER_ROLES, BLOCKED_GENERIC_ROLES };
