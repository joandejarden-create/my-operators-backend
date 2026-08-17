#!/usr/bin/env node
/**
 * Apply Proper Case to Deal Setup select options (display + value stay aligned).
 * Updates HTML, lib/deal-setup-form-options.json, and operator-alignment fixture.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

/** Exact old → new option text (form value = display). */
const OPTION_RENAMES = Object.freeze({
  // Opening / Transition Phase
  "N/A (stabilized operating)": "N/A (Stabilized Operating)",
  "Planning / entitlement": "Planning / Entitlement",
  "Pre-construction": "Pre-Construction",
  "Pre-opening ramp": "Pre-Opening Ramp",
  "Soft opening": "Soft Opening",
  "Reopening after renovation": "Reopening After Renovation",
  "Rebranding in place": "Rebranding In Place",

  // Operating models
  "Owner-operated (unbranded)": "Owner-Operated (Unbranded)",
  "Owner-operated (branded/franchised)": "Owner-Operated (Branded/Franchised)",
  "Third-party managed (branded)": "Third-Party Managed (Branded)",
  "Third-party managed (independent/collection)": "Third-Party Managed (Independent/Collection)",
  "Brand-managed": "Brand-Managed",
  "Lease/operator lease structure": "Lease/Operator Lease Structure",
  "Mixed/transitioning": "Mixed/Transitioning",
  "Owner-operated": "Owner-Operated",
  "Third-party managed": "Third-Party Managed",
  "Third-party management only": "Third-Party Management Only",
  "Franchise/license only (owner or third-party operator)":
    "Franchise/License Only (Owner or Third-Party Operator)",
  "Brand + third-party management": "Brand + Third-Party Management",
  "Lease structure": "Lease Structure",
  "Undecided / exploring": "Undecided / Exploring",

  // Site control / MEP
  "Letter of intent (LOI) / Term Sheet": "Letter of Intent (LOI) / Term Sheet",
  "Upgrade needed": "Upgrade Needed",
  "Unknown / to be assessed": "Unknown / To Be Assessed",

  // Capital / project
  "Not Applicable / New build": "Not Applicable / New Build",
  "Not yet determined": "Not Yet Determined",
  "Under 1 year": "Under 1 Year",
  "0-3 months": "0–3 Months",
  "3-6 months": "3–6 Months",
  "6-12 months": "6–12 Months",
  "12+ months": "12+ Months",

  // Operator strategy
  "Not seeking operator input": "Not Seeking Operator Input",
  "Exploring capabilities only": "Exploring Capabilities Only",
  "Building shortlist for advisor review": "Building Shortlist for Advisor Review",
  "Ready for structured operator review": "Ready for Structured Operator Review",
  "Already in discussions": "Already In Discussions",

  // Operator capability priorities
  "Full hotel management": "Full Hotel Management",
  "Pre-opening / opening support": "Pre-Opening / Opening Support",
  "Conversion & PIP execution": "Conversion & PIP Execution",
  "Revenue management & distribution": "Revenue Management & Distribution",
  "Accounting & owner reporting": "Accounting & Owner Reporting",
  "Procurement & cost control": "Procurement & Cost Control",
  "F&B / culinary operations": "F&B / Culinary Operations",
  "Sales & marketing": "Sales & Marketing",
  "HR & training": "HR & Training",
  "Technology & systems": "Technology & Systems",
  "Design / renovation PM": "Design / Renovation PM",
  "Asset management / capex planning": "Asset Management / CapEx Planning",
  "Local market / CALA execution": "Local Market / CALA Execution",
  "Lifestyle / experience programming": "Lifestyle / Experience Programming",
  "Crisis / business continuity": "Crisis / Business Continuity",

  // Owner reporting
  "Weekly financial": "Weekly Financial",
  "Monthly operating metrics": "Monthly Operating Metrics",
  "Quarterly board pack": "Quarterly Board Pack",
  "Annual budget / forecast": "Annual Budget / Forecast",
  "Owner portal access": "Owner Portal Access",
  "On-demand / ad hoc": "On-Demand / Ad Hoc",
  "Third-party audit support": "Third-Party Audit Support",
  "Ad hoc": "Ad Hoc",

  // Timelines / consultants
  "Within 30 days": "Within 30 Days",
  "30-90 days": "30–90 Days",
  "Yes - Connect me with Consultant": "Yes — Connect Me With a Consultant",
  "Yes — Connect me With a Legal Advisor": "Yes — Connect Me With a Legal Advisor",

  // Amenities
  "Other Amenities (specify)": "Other Amenities (Specify)",

  // OAS / operator alignment fixture
  "Full management": "Full Management",
  "Commercial support": "Commercial Support",
  "Pre-opening support": "Pre-Opening Support",
  "Brand compliance support": "Brand Compliance Support",
  "Owner reporting": "Owner Reporting",
  "Asset management support": "Asset Management Support",
  "Technical services coordination": "Technical Services Coordination",
  "Owner-operated with commercial support": "Owner-Operated With Commercial Support",
  "Owner-operated with brand support": "Owner-Operated With Brand Support",
  "Full third-party management": "Full Third-Party Management",
  "Franchise with third-party operator": "Franchise With Third-Party Operator",
  "Commercial-only support": "Commercial-Only Support",
  "Pre-opening / transition support": "Pre-Opening / Transition Support",
  "Hybrid / project-specific": "Hybrid / Project-Specific",
  "Not started": "Not Started",
  "Exploring operator options": "Exploring Operator Options",
  "Operator review in scope": "Operator Review In Scope",
  "Ready for operator shortlist": "Ready for Operator Shortlist",
  "Operator already selected": "Operator Already Selected",
  "Not applicable": "Not Applicable",
  "Active local market operations required": "Active Local Market Operations Required",
  "Active country operations required": "Active Country Operations Required",
  "Regional experience acceptable": "Regional Experience Acceptable",
  "Prior similar-market experience acceptable": "Prior Similar-Market Experience Acceptable",
  "Open to new-market operator": "Open to New-Market Operator",
  "Basic owner reporting": "Basic Owner Reporting",
  "Monthly operating review": "Monthly Operating Review",
  "Institutional reporting": "Institutional Reporting",
  "Lender / investor-grade reporting": "Lender / Investor-Grade Reporting",
  "Custom / project-specific": "Custom / Project-Specific",
  "Brand standards with third-party operator": "Brand Standards With Third-Party Operator",
  "Franchise with owner/operator execution": "Franchise With Owner/Operator Execution",
  "Operator-led with brand compliance support": "Operator-Led With Brand Compliance Support",
  "Owner wants high control": "Owner Wants High Control",
  "Shared control": "Shared Control",
  "Operator-led operations": "Operator-Led Operations",
  "Institutional governance preferred": "Institutional Governance Preferred",
  "Revenue management": "Revenue Management",
  "Digital marketing": "Digital Marketing",
  "Loyalty / brand channels": "Loyalty / Brand Channels",
  "Group sales": "Group Sales",
  "Corporate accounts": "Corporate Accounts",
  "None specified": "None Specified",
  "Strong internal hotel operations team": "Strong Internal Hotel Operations Team",
  "Partial internal capability": "Partial Internal Capability",
  "Limited internal capability": "Limited Internal Capability",
  "No internal hotel operations team": "No Internal Hotel Operations Team",
  "Pre-development": "Pre-Development",
  "Under construction": "Under Construction",
  "Soft brand / collection affiliation": "Soft Brand / Collection Affiliation",
  "Limited-service": "Limited-Service",
  "Select-service": "Select-Service",
  "Focused-service": "Focused-Service",
  "Full-service": "Full-Service",
  "All-inclusive": "All-Inclusive",
  "Extended stay": "Extended Stay",
  "Branded residential / mixed-use": "Branded Residential / Mixed-Use",
  "Active operations": "Active Operations",
  "Prior experience": "Prior Experience",
  "Pipeline / signed project": "Pipeline / Signed Project",
  "Target market": "Target Market",
  "No known presence": "No Known Presence",
  "None documented": "None Documented",
  "Property-level only": "Property-Level Only",
  "Centralized support": "Centralized Support",
  "Advanced centralized platform": "Advanced Centralized Platform",
  "Third-party partner": "Third-Party Partner",
  "Local sales": "Local Sales",
  "Regional sales": "Regional Sales",
  "Global sales support": "Global Sales Support",
  "Digital / e-commerce": "Digital / E-Commerce",
  "Asset-management style": "Asset-Management Style",
  "Board / investor reporting": "Board / Investor Reporting",
  "Operator-provided": "Operator-Provided",
  "Public-source": "Public-Source",
  "Internal research": "Internal Research",
  "Public website": "Public Website",
  "Brand / owner conversation": "Brand / Owner Conversation",
  "Prior project knowledge": "Prior Project Knowledge",
  "Imported sample data": "Imported Sample Data",
  "Soft brands / collections": "Soft Brands / Collections",
  "None / rooms-only": "None / Rooms-Only",
  "Limited F&B": "Limited F&B",
  "Moderate F&B": "Moderate F&B",
  "Significant F&B": "Significant F&B",
  "Lifestyle / experiential F&B": "Lifestyle / Experiential F&B",
  "Opening / transition support": "Opening / Transition Support",
  "Distribution / channel management": "Distribution / Channel Management",
  "Accounting / finance": "Accounting / Finance",
  "HR / staffing": "HR / Staffing",
  "F&B operations": "F&B Operations",
  "Pre-opening planning": "Pre-Opening Planning",
  "Select one...": "Select One…",
  "Select one…": "Select One…",
  "Marketing Allowance (one-time or recurring)": "Marketing Allowance (One-Time or Recurring)",
});

function replaceInText(text) {
  let out = text;
  const entries = Object.entries(OPTION_RENAMES).sort((a, b) => b[0].length - a[0].length);
  for (const [oldVal, newVal] of entries) {
    if (oldVal === newVal) continue;
    out = out.split(oldVal).join(newVal);
  }
  return out;
}

function renameOptionsInJson(obj) {
  if (Array.isArray(obj)) {
    return obj.map((v) => (typeof v === "string" ? OPTION_RENAMES[v] ?? v : v));
  }
  if (obj && typeof obj === "object") {
    const next = {};
    for (const [k, v] of Object.entries(obj)) {
      next[k] = renameOptionsInJson(v);
    }
    return next;
  }
  return obj;
}

const targets = [
  path.join(ROOT, "public", "new-deal-setup.html"),
  path.join(ROOT, "public", "deal-setup.html"),
  path.join(ROOT, "public", "fixtures", "operator-alignment-field-options.json"),
  path.join(ROOT, "lib", "deal-setup-form-options.json"),
];

for (const file of targets) {
  const raw = fs.readFileSync(file, "utf8");
  const next = file.endsWith(".json")
    ? JSON.stringify(renameOptionsInJson(JSON.parse(raw)), null, 2) + "\n"
    : replaceInText(raw);
  if (next !== raw) {
    fs.writeFileSync(file, next, "utf8");
    console.log("Updated", path.relative(ROOT, file));
  } else {
    console.log("No changes", path.relative(ROOT, file));
  }
}

// Re-extract JSON from HTML (HTML is source for extract script)
import { spawnSync } from "node:child_process";
spawnSync(process.execPath, [path.join(ROOT, "scripts", "extract-deal-setup-form-options.mjs")], {
  stdio: "inherit",
  cwd: ROOT,
});

console.log("Done. Review deal-setup-form-value-normalize.js aliases for Airtable legacy values.");
