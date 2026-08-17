/**
 * Founder Project Plan — multi-worker agent workflow (Cursor, ChatGPT, Helena AI).
 * Joan approves before any task is marked Completed.
 */
import { MAP_MASTER_TODO, MASTER_TODO_SOURCE_VALUE } from "./master-todo-field-map.js";
import { parseSeedId } from "./dealality-airtable-field-fill.js";

const F = MAP_MASTER_TODO;

/** @typedef {'cursor' | 'chatgpt' | 'helena' | 'human-only'} FppWorkerId */

/** @typedef {{
 *   id: string,
 *   label: string,
 *   description: string,
 *   primaryUse: string,
 *   stopStatus: string,
 *   tools: string[],
 * }} FppWorkerProfile */

export const FPP_WORKERS = {
  cursor: {
    id: "cursor",
    label: "Cursor",
    description: "Repo code, scripts, Airtable sync, platform UI, dry-run → execute after Joan approves.",
    primaryUse: "Platform build, data scripts, PTL field hygiene, audits, technical deliverables.",
    stopStatus: "Needs Review",
    tools: ["deal-capture-proxy scripts", "npm run dev", "Airtable via master-todo scripts"],
  },
  chatgpt: {
    id: "chatgpt",
    label: "ChatGPT",
    description: "GTM copy, playbooks, call scripts, strategy drafts; read/write GTM via Custom GPT + API.",
    primaryUse: "Warm intro, reply playbook, pilot scripts, collateral copy, planning memos.",
    stopStatus: "Needs Review",
    tools: ["Custom GPT + dealality-airtable-chatgpt API", "GTM notes", "docs/"],
  },
  helena: {
    id: "helena",
    label: "Helena AI",
    description: "Outbound material requests to brands/operators; log intake — never marks outreach sent without Joan.",
    primaryUse: "Brand/operator data outreach, reference material collection, follow-up logging.",
    stopStatus: "Needs Review",
    tools: [
      "Partner Intelligence - Helena Outreach Intake",
      "docs/partner-reference-material-collection-guide.md",
      "PARTNER_REFERENCE_ROOT",
    ],
  },
  "human-only": {
    id: "human-only",
    label: "Joan (human only)",
    description: "Agents may advise or draft; Joan executes and approves completion.",
    primaryUse: "Legal, financial, compliance, contract negotiation, sending live outreach, production access.",
    stopStatus: "Needs Review",
    tools: [],
  },
};

/** Status values agents may write without Joan's completion approval. */
export const FPP_AGENT_ALLOWED_STATUSES = new Set([
  "Not Started",
  "In Progress",
  "Drafted",
  "Needs Review",
  "Waiting",
  "Blocked",
]);

/** Only Joan (explicit CLI flag) may set Completed. */
export const FPP_COMPLETION_STATUS = "Completed";

const HUMAN_ONLY_PATTERNS = [
  /legal doc/i,
  /financial model/i,
  /funding requirement/i,
  /compliance/i,
  /gdpr|ccpa/i,
  /negotiate/i,
  /business entity/i,
  /risk assessment/i,
  /send outreach|announce launch|go live/i,
  /access hygiene sprint/i,
  /pilot invite standard/i,
];

/**
 * @typedef {{
 *   playbookId: string,
 *   worker: FppWorkerId,
 *   title: string,
 *   steps: string[],
 *   deliverables: string[],
 *   approvalChecklist: string[],
 *   repoPaths?: string[],
 *   airtableNotes?: string,
 *   agentEligible: boolean,
 * }} FppTaskPlaybook
 */

/** @type {Record<string, Partial<FppTaskPlaybook> & Pick<FppTaskPlaybook, 'playbookId' | 'worker' | 'title' | 'steps' | 'approvalChecklist'>>} */
export const FPP_PLAYBOOK_BY_SEED_ID = {
  "mt-04": {
    playbookId: "gtm-warm-intro",
    worker: "chatgpt",
    title: "Finalize warm intro blurb",
    steps: [
      "Read Task, Deliverables, and Pilot Target List segment context (read-only).",
      "Draft or refine forwardable warm intro for lawyers, advisors, referral sources.",
      "Save draft to GTM notes or linked doc; do not send messages.",
      "Set Status = Needs Review, Progress = 75%, update Next Action with file location.",
    ],
    approvalChecklist: [
      "Tone is confidential, low-pressure, pilot-appropriate.",
      "Copy/paste ready for Mail Merge / LinkedIn.",
      "No promises the platform cannot keep yet.",
    ],
    repoPaths: ["docs/gtm-resources/warm-intro-blurb.md"],
  },
  "mt-05": {
    playbookId: "gtm-reply-playbook",
    worker: "chatgpt",
    title: "Finalize reply playbook",
    steps: [
      "Cover reply types: happy to chat, more info, referral, confidentiality, not relevant, no response.",
      "Produce one template per type with placeholders.",
      "Set Needs Review with link to master GTM notes.",
    ],
    approvalChecklist: ["Each template is accurate and on-brand.", "Confidentiality language is correct."],
    repoPaths: ["docs/gtm-resources/reply-playbook.md"],
  },
  "mt-06": {
    playbookId: "pilot-call-script",
    worker: "chatgpt",
    title: "Finalize pilot call script",
    steps: [
      "Draft script branches for lawyer/advisor vs owner/developer.",
      "Include discovery questions and pilot framing (feedback vs real opportunity).",
      "Set Needs Review; do not book calls on Joan's behalf.",
    ],
    approvalChecklist: ["Script matches pilot acceptance criteria.", "Clear opt-in and confidentiality."],
    repoPaths: ["docs/gtm-resources/pilot-call-script.md"],
  },
  "mt-07": {
    playbookId: "ptl-wave-cleanup",
    worker: "cursor",
    title: "Update Pilot Target List — Wave 1",
    steps: [
      "Run outreach readiness report (read-only): npm run report or scripts/report-owner-targets-outreach-readiness.mjs.",
      "Propose field patches for missing Mail Merge Batch, Send Channel, Next Follow-Up Date.",
      "Show sanitized payload preview; execute only after Joan approves.",
    ],
    approvalChecklist: [
      "June 16 sends cleaned up.",
      "Wave 1 (Jul 2) fully tracked.",
      "No duplicate or wrong-status rows.",
    ],
    repoPaths: ["scripts/report-owner-targets-outreach-readiness.mjs", "lib/gtm-owner-target/"],
    airtableNotes: "Pilot Target List — do not bulk-write without explicit --execute approval.",
  },
  "mt-08": {
    playbookId: "monitor-replies",
    worker: "cursor",
    title: "Monitor first-wave replies",
    steps: [
      "Check inbox/PTL for new replies (Joan may paste summaries).",
      "Classify: chat, referral, feedback, not relevant, follow-up later.",
      "Propose Reply Notes + Outreach Status updates; Joan approves writes.",
    ],
    approvalChecklist: ["Every reply logged.", "Conversion signals classified."],
  },
  "mt-09": {
    playbookId: "no-reply-follow-up",
    worker: "chatgpt",
    title: "No-reply follow-up copy",
    steps: [
      "Draft light follow-up for Wave 1 (4–6 business days after first touch).",
      "Align send window with Next Follow-Up Date in PTL.",
      "Set Needs Review; Joan sends manually.",
    ],
    approvalChecklist: ["Short, respectful, one clear CTA.", "Matches reply playbook tone."],
  },
  "mt-10": {
    playbookId: "pilot-intake-questions",
    worker: "chatgpt",
    title: "Pilot intake questions",
    steps: [
      "Extract subset from deal-setup / new-deal-setup intake fields.",
      "Tailor for advisor/owner discovery calls (not full platform intake).",
      "Set Needs Review with question list doc.",
    ],
    approvalChecklist: ["Covers location, asset, objective, timing, brand/operator prefs.", "Pilot-appropriate length."],
    repoPaths: ["public/new-deal-setup.html", "public/deal-setup.html"],
  },
  "mt-11": {
    playbookId: "pilot-participant-checklist",
    worker: "chatgpt",
    title: "What we need from you checklist",
    steps: ["One-page participant checklist or email template.", "Set Needs Review."],
    approvalChecklist: ["Clear materials list.", "Confidentiality called out."],
  },
  "mt-14": {
    playbookId: "access-hygiene",
    worker: "human-only",
    title: "Access hygiene sprint",
    steps: [
      "Cursor may produce smoke-test checklist only.",
      "Joan runs Webflow/Memberstack/Airtable access tests.",
      "Agent must not mark Completed without Joan sign-off.",
    ],
    approvalChecklist: [
      "Owner/advisor lands in correct view.",
      "Admin pages protected.",
      "Demo users scoped correctly.",
    ],
    agentEligible: false,
  },
  "mt-16": {
    playbookId: "sample-output-pack",
    worker: "cursor",
    title: "Sanitized sample output pack",
    steps: [
      "Package fixtures/sample-deals + explorer screenshots.",
      "Redact sensitive fields; export PDF or deck outline.",
      "Set Needs Review with artifact paths.",
    ],
    approvalChecklist: ["Safe to show external pilot contacts.", "Demonstrates comparison/match value."],
    repoPaths: ["fixtures/sample-deals/", "public/brand-explorer-combined.html", "public/deal-compare.html"],
  },
  "mt-24": {
    playbookId: "pilot-acceptance-criteria",
    worker: "chatgpt",
    title: "Pilot acceptance criteria",
    steps: [
      "Define real opportunity vs feedback-only conversation.",
      "Document in GTM notes; link from task Next Action.",
      "Set Needs Review.",
    ],
    approvalChecklist: ["Criteria are objective and usable on calls.", "Aligned with pilot intake."],
    repoPaths: ["docs/gtm-resources/pilot-acceptance-criteria.md"],
  },
};

/** @type {Record<string, Partial<FppTaskPlaybook> & Pick<FppTaskPlaybook, 'playbookId' | 'worker' | 'title' | 'steps' | 'approvalChecklist'>>} */
export const FPP_PLAYBOOK_BY_WORKSTREAM = {
  "Reply Handling": FPP_PLAYBOOK_BY_SEED_ID["mt-05"],
  "Pilot Target List": FPP_PLAYBOOK_BY_SEED_ID["mt-07"],
  "Data Collection & Integration": {
    playbookId: "brand-data-helena",
    worker: "helena",
    title: "Brand / operator data outreach",
    steps: [
      "Identify target brand/operator from task context.",
      "Log Helena Outreach Intake row (when table exists) or draft outreach in Notes.",
      "Request specific materials per partner-reference guide.",
      "On receipt: link to Partner Source Library — set Needs Review.",
    ],
    approvalChecklist: [
      "Outreach logged before send.",
      "Received materials linked to source library.",
      "Joan approved send text.",
    ],
  },
  "Platform & Feature Design": {
    playbookId: "platform-design-cursor",
    worker: "cursor",
    title: "Platform design / UI task",
    steps: [
      "Confirm live page or module exists in public/ or api/.",
      "If done: document URL + screenshot path; set Needs Review with evidence.",
      "If not done: implement minimal slice; set Needs Review — never auto-Completed.",
    ],
    approvalChecklist: ["Deliverable matches Task Objective.", "No schema writes without approval."],
  },
  "Tech Build": {
    playbookId: "platform-build-cursor",
    worker: "cursor",
    title: "Platform build task",
    steps: [
      "Map task to repo module; run relevant script or implement change.",
      "Dry-run Airtable writes first.",
      "Set Needs Review with test checklist.",
    ],
    approvalChecklist: ["Manual QA steps listed.", "Regression risks noted."],
  },
};

export const FPP_DEFAULT_PLAYBOOK = {
  playbookId: "generic-review",
  worker: "cursor",
  title: "Generic FPP task",
  steps: [
    "Read Task, Deliverables, Task Objective/Description, Next Action.",
    "Produce draft deliverable or execution plan.",
    "Stop at Needs Review with artifact link and proposed Next Action.",
    "Joan approves → run apply-fpp-agent-task-update with --approved-by.",
  ],
  deliverables: ["Draft artifact or report path"],
  approvalChecklist: ["Deliverable matches task.", "No outreach or production writes without Joan."],
  agentEligible: true,
};

export function isPhaseRollupTask(fields) {
  return String(fields?.[F.task] || "").trim().startsWith("[Phase rollup]");
}

export function isP0OrP1(priority) {
  const p = String(priority ?? "");
  return /^P0\b/.test(p) || /^P1\b/.test(p) || p.includes("P0 =") || p.includes("P1 =");
}

function startOfWeek(d) {
  const x = new Date(d);
  const day = x.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  x.setDate(x.getDate() + diff);
  x.setHours(0, 0, 0, 0);
  return x;
}

function endOfWeek(d) {
  const s = startOfWeek(d);
  const e = new Date(s);
  e.setDate(e.getDate() + 6);
  e.setHours(23, 59, 59, 999);
  return e;
}

function parseIsoDate(iso) {
  if (!iso) return null;
  const d = new Date(`${iso}T12:00:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function isDueTodayOrThisWeek(endIso, refDate = new Date()) {
  const end = parseIsoDate(endIso);
  if (!end) return false;
  const ref = new Date(refDate);
  ref.setHours(12, 0, 0, 0);
  const sameDay =
    end.getFullYear() === ref.getFullYear() &&
    end.getMonth() === ref.getMonth() &&
    end.getDate() === ref.getDate();
  if (sameDay) return true;
  return end >= startOfWeek(ref) && end <= endOfWeek(ref);
}

/** Mirror Today's Focus view logic in JS (for agent queue). */
export function isTodaysFocusTask(fields, refDate = new Date()) {
  const status = String(fields?.[F.status] || "");
  if (["Completed", "Deferred", "Not Needed"].includes(status)) return false;
  if (isPhaseRollupTask(fields)) return false;

  const source = fields?.[F.source];
  const priority = fields?.[F.priority];
  const end = fields?.[F.dueDate];

  if (source === MASTER_TODO_SOURCE_VALUE && isP0OrP1(priority)) return true;
  if (status === "In Progress" && isP0OrP1(priority)) return true;
  if (isP0OrP1(priority) && isDueTodayOrThisWeek(end, refDate)) return true;
  return false;
}

export function isHumanOnlyTask(fields) {
  const task = String(fields?.[F.task] || "");
  const workstream = String(fields?.[F.workstream] || "");
  if (HUMAN_ONLY_PATTERNS.some((re) => re.test(task) || re.test(workstream))) return true;
  const seedId = parseSeedId(fields);
  const seedPlaybook = seedId ? FPP_PLAYBOOK_BY_SEED_ID[seedId] : null;
  if (seedPlaybook?.agentEligible === false) return true;
  return false;
}

/**
 * @param {object} fields
 * @returns {FppTaskPlaybook}
 */
export function resolveTaskPlaybook(fields) {
  const seedId = parseSeedId(fields);
  if (seedId && FPP_PLAYBOOK_BY_SEED_ID[seedId]) {
    const partial = FPP_PLAYBOOK_BY_SEED_ID[seedId];
    return {
      ...FPP_DEFAULT_PLAYBOOK,
      ...partial,
      agentEligible: partial.agentEligible !== undefined ? partial.agentEligible : true,
    };
  }

  const ws = String(fields?.[F.workstream] || "").trim();
  if (ws && FPP_PLAYBOOK_BY_WORKSTREAM[ws]) {
    const partial = FPP_PLAYBOOK_BY_WORKSTREAM[ws];
    return {
      ...FPP_DEFAULT_PLAYBOOK,
      ...partial,
      agentEligible: partial.agentEligible !== undefined ? partial.agentEligible : true,
    };
  }

  if (isHumanOnlyTask(fields)) {
    return {
      ...FPP_DEFAULT_PLAYBOOK,
      playbookId: "human-only",
      worker: "human-only",
      title: "Human execution required",
      agentEligible: false,
      steps: [
        "Agent may draft or advise only.",
        "Joan executes and marks Needs Review or Completed after real work is done.",
      ],
    };
  }

  return { ...FPP_DEFAULT_PLAYBOOK };
}

function priorityRank(priority) {
  const p = String(priority ?? "");
  if (/^P0\b/.test(p) || p.includes("P0 =")) return 0;
  if (/^P1\b/.test(p) || p.includes("P1 =")) return 1;
  if (/^P2\b/.test(p) || p.includes("P2 =")) return 2;
  if (/^P3\b/.test(p) || p.includes("P3 =")) return 3;
  return 4;
}

function statusRank(status) {
  const s = String(status || "");
  if (s === "Needs Review") return 0;
  if (s === "In Progress") return 1;
  if (s === "Blocked") return 2;
  if (s === "Not Started") return 3;
  return 4;
}

/** Sort queue: Needs Review first (Joan backlog), then In Progress, then by priority / roadmap / due. */
export function compareFppTaskQueue(a, b) {
  const fa = a.fields || {};
  const fb = b.fields || {};
  const sr = statusRank(fa[F.status]) - statusRank(fb[F.status]);
  if (sr !== 0) return sr;
  const pr = priorityRank(fa[F.priority]) - priorityRank(fb[F.priority]);
  if (pr !== 0) return pr;
  const rs =
    (Number(fa["Roadmap Sort"]) || 999) - (Number(fb["Roadmap Sort"]) || 999);
  if (rs !== 0) return rs;
  const ea = String(fa[F.dueDate] || "");
  const eb = String(fb[F.dueDate] || "");
  return ea.localeCompare(eb);
}

export function selectNextFppTask(records, { recordId = null, includeNeedsReview = true } = {}) {
  if (recordId) {
    const hit = records.find((r) => r.id === recordId);
    return hit || null;
  }

  const eligible = records.filter((r) => {
    if (isPhaseRollupTask(r.fields)) return false;
    if (!isTodaysFocusTask(r.fields)) return false;
    if (!includeNeedsReview && r.fields?.[F.status] === "Needs Review") return false;
    return true;
  });

  eligible.sort(compareFppTaskQueue);
  return eligible[0] || null;
}
