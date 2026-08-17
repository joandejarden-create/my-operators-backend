/**
 * Founder Project Plan — phase status updates aligned to shipped Dealality work.
 * Match rows by Phase + Step Number (excludes [Phase rollup] and immutable statuses).
 */
import { MAP_MASTER_TODO } from "./master-todo-field-map.js";
import { PLATFORM_DESIGN_STATUS_BY_STEP } from "./founder-project-plan-platform-design-status.js";

const F = MAP_MASTER_TODO;
const COMPLETED_DATE = "2026-07-03";

const DONE = {
  status: "Completed",
  progress: "100%",
  completedDate: COMPLETED_DATE,
  nextAction: "No action required — task completed.",
};

/** @param {string} progress @param {string} nextAction */
function ip(progress, nextAction) {
  return { status: "In Progress", progress, nextAction };
}

/** @param {string} progress @param {string} nextAction */
function needsReview(progress, nextAction) {
  return { status: "Needs Review", progress, nextAction };
}

/** @type {Record<string, Record<number, { status: string, progress: string, completedDate?: string, nextAction: string }>>} */
export const PHASE_STATUS_UPDATES = {
  "Strategy & Foundations": {
    1: ip(
      "55%",
      "Close remaining foundation gaps: financial model, legal docs, compliance review, and risk register before pilot scale-up."
    ),
    21: ip(
      "15%",
      "Document current bootstrapped runway and pilot capital needs; identify optional funding sources without blocking Wave 1."
    ),
    22: ip(
      "15%",
      "Build lightweight operating model spreadsheet (pilot costs, capacity, revenue hypotheses) — not full 5-year forecast yet."
    ),
  },

  "Product Definition": {
    1: DONE,
    2: ip(
      "75%",
      "Publish scoring-weight source of truth from operator-alignment engine + field matrix; sign off pilot weighting."
    ),
    3: ip(
      "70%",
      "Finalize field-to-field mapping doc from operator-alignment-field-matrix.md; close remaining deal↔operator gaps."
    ),
  },

  "Strategy & Design": {
    1: ip(
      "20%",
      "Referral program design pending — legal sponsorship model complete; eligibility, rewards, and routing still open."
    ),
  },

  "Resources / Collateral": {
    4: ip(
      "45%",
      "Package sanitized sample-deals fixtures + explorer screenshots into pilot conversation deck; redact sensitive fields."
    ),
  },

  "GTM / Outreach": {
    2: DONE,
    3: DONE,
  },

  "Pilot Conversion": {
    1: DONE,
    2: DONE,
  },

  "Pilot Delivery": {
    1: ip(
      "40%",
      "Extract pilot-specific intake question subset from deal-setup flows; tailor for advisor/owner discovery calls."
    ),
  },

  "Platform Design": PLATFORM_DESIGN_STATUS_BY_STEP,

  "Platform Build": {
    1: ip(
      "45%",
      "Core MVP, auth, intake, dashboards, and scoring shipped in-house; referral-program build and formal security QA remain."
    ),
    2: ip(
      "30%",
      "Airtable + Memberstack integrated; prioritize signature, payment, and CRM connectors for pilot."
    ),
    3: ip(
      "55%",
      "Admin/user management and internal tooling live; expand approvals analytics and messaging controls."
    ),
    4: DONE,
    5: DONE,
    6: ip(
      "25%",
      "Comparison UI supports print layout; build dedicated side-by-side comparison PDF export."
    ),
    7: ip(
      "45%",
      "My Deals insights and outreach-analytics exist; unify deal-flow and engagement dashboard."
    ),
    8: ip(
      "75%",
      "Operator/brand scoring engines and alignment snapshots live; complete match-accuracy validation pass."
    ),
    9: ip(
      "50%",
      "Deal-room file attach and upload paths exist; harden encryption and comparison-support tooling."
    ),
    10: DONE,
    11: DONE,
    12: ip(
      "45%",
      "Pilot acceptance criteria drafted; document MVP launch structure, timeline, and success metrics."
    ),
    14: DONE,
    16: ip(
      "65%",
      "Local dev (npm run dev), repo CI patterns, and Airtable staging workflows operational; document staging checklist."
    ),
    18: DONE,
    19: DONE,
    21: {
      ...DONE,
      nextAction:
        "No action required — built in-house via deal-capture-proxy (Webflow + Node + Airtable).",
    },
    23: ip(
      "45%",
      "Run scoring fairness review using operator-alignment audits and sample-deal regression checks.",
    ),
  },

  "Content & GTM": {
    1: ip(
      "40%",
      "Pitch decks nearly final; marketing site and CRM/outreach tooling partially live — finish onboarding collateral."
    ),
    2: DONE,
    5: ip(
      "70%",
      "Webflow marketing pages and landing flows live; polish positioning copy and pilot CTA paths."
    ),
    6: ip(
      "55%",
      "Pilot Target List + outreach module operational; formalize CRM templates and lead-tracking SOP."
    ),
    7: ip(
      "30%",
      "how-it-works and role-specific pages exist; produce user onboarding guides and FAQs for pilot cohort."
    ),
  },

  "Testing & Pilot": {
    1: ip(
      "15%",
      "Internal alpha via sample deals and fixture workflows underway; real pilot-user beta not started."
    ),
    3: ip(
      "45%",
      "Continue internal alpha with sample-deals fixtures across intake, match, and comparison flows."
    ),
    4: ip(
      "50%",
      "Ongoing UX fixes and scoring refinements from internal testing — log issues before pilot beta."
    ),
  },

  "Launch & Operations": {
    1: ip(
      "8%",
      "Founder-operated soft launch only; full PR launch, support desk, and referral program activation pending."
    ),
    8: ip(
      "20%",
      "Platform accessible to founder and test users; public launch to approved users blocked on pilot validation."
    ),
    9: ip(
      "30%",
      "Basic engagement tracking via outreach analytics and My Deals; formal post-launch monitoring SOP not set."
    ),
  },

  "Scale & Optimize": {
    1: ip(
      "5%",
      "Backlog phase — capture Phase 2 ideas from pilot learning; no active scale work yet."
    ),
    2: ip(
      "25%",
      "Phase 2 roadmap notes exist in docs (AI scoring, white-label); formalize after pilot signal."
    ),
  },
};

const IMMUTABLE_STATUSES = new Set(["Not Needed", "Deferred"]);

export function isPhaseStatusTaskRow(fields) {
  const task = String(fields?.[F.task] || "");
  if (task.includes("[Phase rollup]")) return false;
  if (!fields?.[F.phase]) return false;
  if (IMMUTABLE_STATUSES.has(fields?.[F.status])) return false;
  return true;
}

export function buildPhaseStatusPatch(fields) {
  const phase = fields?.[F.phase];
  const step = fields?.[F.stepNumber];
  if (!phase || step == null || step === "") return null;

  const phaseUpdates = PHASE_STATUS_UPDATES[phase];
  if (!phaseUpdates) return null;

  const target = phaseUpdates[Number(step)];
  if (!target) return null;

  const patch = {};
  const existing = fields || {};

  if (existing[F.status] !== target.status) patch[F.status] = target.status;
  if (existing[F.progress] !== target.progress) patch[F.progress] = target.progress;
  if (existing[F.nextAction] !== target.nextAction) patch[F.nextAction] = target.nextAction;

  if (target.status === "Completed" && target.completedDate) {
    if (!existing[F.completedDate]) patch[F.completedDate] = target.completedDate;
  } else if (target.status !== "Completed" && existing[F.completedDate]) {
    patch[F.completedDate] = null;
  }

  if (Object.keys(patch).length === 0) return null;
  return { patch, target, phase, step: Number(step) };
}
