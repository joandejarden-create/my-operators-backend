import {
  VAL_MASTER_TODO_PHASE,
  VAL_MASTER_TODO_PRIORITY,
  VAL_MASTER_TODO_STATUS,
  VAL_MASTER_TODO_WORKSTREAM,
} from "./master-todo-field-map.js";

/**
 * Current Dealality master to-do seed list (pilot / GTM conversion readiness).
 * @typedef {{ id: string, taskName: string, workstream: string, phase: string, priority: string, status: string, description: string, nextAction: string, owner?: string, relatedArea?: string, progress?: string }} MasterTodoSeed
 */

/** @type {MasterTodoSeed[]} */
export const MASTER_TODO_SEED = [
  {
    id: "mt-01",
    taskName: "Finalize Owner / Developer Pilot Overview",
    workstream: "GTM Resources",
    phase: "Resources / Collateral",
    priority: "P1",
    status: "Completed",
    relatedArea: "Owner Pilot",
    description:
      "Owner/developer one-pager created with framing around structure deals, compare options, stay in control.",
    nextAction: "Use in owner/developer outreach and replies.",
    progress: "100%",
  },
  {
    id: "mt-02",
    taskName: "Finalize Lawyer / Advisor Referral Overview",
    workstream: "GTM Resources",
    phase: "Resources / Collateral",
    priority: "P1",
    status: "Completed",
    relatedArea: "Lawyer Referral",
    description:
      "Lawyer/referral one-pager created with emphasis on confidentiality, owner control, advisor relationships, and owner opt-in.",
    nextAction: "Use for lawyer follow-ups and referral conversations.",
    progress: "100%",
  },
  {
    id: "mt-03",
    taskName: "Finalize Advisor / Consultant Pilot Overview",
    workstream: "GTM Resources",
    phase: "Resources / Collateral",
    priority: "P1",
    status: "Completed",
    relatedArea: "Advisor / Consultant",
    description:
      "Advisor/consultant one-pager created with framing around structure deals, compare options, guide clients.",
    nextAction: "Use with consultants, brokers, and owner advisors.",
    progress: "100%",
  },
  {
    id: "mt-04",
    taskName: "Draft Warm Intro Blurb",
    workstream: "Reply Handling",
    phase: "GTM / Outreach",
    priority: "P1",
    status: "Completed",
    relatedArea: "GTM",
    description: "Forwardable warm intro copy drafted for lawyers, advisors, and referral sources.",
    nextAction: "Use EN+ES copy in docs/gtm-resources/ and Google Drive Word docs.",
    progress: "100%",
  },
  {
    id: "mt-05",
    taskName: "Draft Reply Playbook",
    workstream: "Reply Handling",
    phase: "GTM / Outreach",
    priority: "P1",
    status: "Completed",
    relatedArea: "GTM",
    description:
      "Reply templates drafted for happy to chat, send more info, may know someone, confidentiality, advisory/brokerage clarification, no deal, not relevant, and no response.",
    nextAction: "Use EN+ES copy in docs/gtm-resources/ and Google Drive Word docs.",
    progress: "100%",
  },
  {
    id: "mt-06",
    taskName: "Draft Pilot Call Script",
    workstream: "Pilot Conversion",
    phase: "Pilot Conversion",
    priority: "P1",
    status: "Completed",
    relatedArea: "GTM",
    description: "Call script drafted for lawyers, advisors/consultants, and owners/developers.",
    nextAction: "Use EN/ES Word docs on Google Drive and docs/gtm-resources/pilot-call-script.md.",
    progress: "100%",
  },
  {
    id: "mt-07",
    taskName: "Update 10 Sent Contacts In Pilot Target List",
    workstream: "Pilot Target List",
    phase: "GTM / Outreach",
    priority: "P1",
    status: "In Progress",
    relatedArea: "Pilot Target List",
    description:
      "10 contacts have been emailed/messaged, primarily lawyers. Airtable should reflect sent status and next follow-up.",
    nextAction:
      "Wave 1 (11 contacts, Jul 2) is fully tracked. Clean up ~15 June 16 sends: add Mail Merge Batch, Send Channel, and Next Follow-Up Date where missing.",
    progress: "55%",
  },
  {
    id: "mt-08",
    taskName: "Monitor Replies From First 10 Contacts",
    workstream: "Outreach Execution",
    phase: "Pilot Conversion",
    priority: "P1",
    status: "In Progress",
    relatedArea: "Pilot Target List",
    description:
      "First lawyer/advisor outreach wave is live. Track replies and classify into chat, referral, feedback, not relevant, or follow-up later.",
    nextAction:
      "Diego Fernández (Altiplano) call held Jul 15 on live San José del Cabo / Mijares 32 project; Brand/Operator/F&B proposal drafted. Send proposal, track signature/deposit, and continue monitoring Wave 1 replies (Abelardo Spain referrals, Miguel pending).",
    progress: "90%",
  },
  {
    id: "mt-09",
    taskName: "Prepare No-Reply Follow-Up For First Wave",
    workstream: "Outreach Execution",
    phase: "GTM / Outreach",
    priority: "P1",
    status: "In Progress",
    relatedArea: "Pilot Target List",
    description: "Prepare and schedule/send light follow-up 4–6 business days after first message.",
    nextAction:
      "Draft no-reply follow-up copy now; send on/after Jul 9 for Wave 1 contacts with Next Follow-Up Date = 2026-07-09.",
    progress: "25%",
  },
  {
    id: "mt-10",
    taskName: "Create Pilot Intake Questions",
    workstream: "Pilot Delivery",
    phase: "Pilot Delivery",
    priority: "P1",
    status: "In Progress",
    relatedArea: "Owner Pilot",
    description:
      "Create intake questions for owner/advisor pilot opportunities covering location, asset, deal objective, timing, brand/operator preferences, missing info, and documents.",
    nextAction:
      "Tailor intake questions to Altiplano Mijares 32 (brand purpose, operator role, residential association, F&B structure, design/permitting timing).",
    progress: "25%",
  },
  {
    id: "mt-11",
    taskName: "Create “What We Need From You” Checklist",
    workstream: "Pilot Delivery",
    phase: "Pilot Delivery",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Owner Pilot",
    description: "Participant-facing checklist explaining what information/materials are needed for the pilot.",
    nextAction: "Draft simple one-page checklist or email template.",
    progress: "0%",
  },
  {
    id: "mt-12",
    taskName: "Create Pilot Review Call Agenda",
    workstream: "Pilot Delivery",
    phase: "Pilot Delivery",
    priority: "P2",
    status: "Not Started",
    relatedArea: "Owner Pilot",
    description: "Create agenda for reviewing Dealality outputs after a pilot opportunity is processed.",
    nextAction: "Draft call agenda.",
    progress: "0%",
  },
  {
    id: "mt-13",
    taskName: "Create Pilot Feedback Form",
    workstream: "Pilot Delivery",
    phase: "Pilot Delivery",
    priority: "P2",
    status: "Not Started",
    relatedArea: "Owner Pilot",
    description:
      "Feedback form to capture usefulness, clarity, actionability, willingness to use, willingness to refer, and concerns.",
    nextAction: "Draft questions.",
    progress: "0%",
  },
  {
    id: "mt-14",
    taskName: "Run Access Hygiene Sprint",
    workstream: "Access Hygiene",
    phase: "Product / Access",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Webflow / Memberstack",
    description:
      "Smoke test Webflow/Memberstack/Airtable access for owner/advisor pilot users.",
    nextAction:
      "Confirm owner/advisor users land in correct view, admin pages are protected, demo users are separate, and linked deals are scoped correctly.",
    progress: "0%",
  },
  {
    id: "mt-15",
    taskName: "Confirm Pilot Invite Standard",
    workstream: "Access Hygiene",
    phase: "Product / Access",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Webflow / Memberstack",
    description: "Confirm repeatable checklist for provisioning a real pilot user.",
    nextAction:
      "Verify Users record, Company Profile, Workspace Access, User Type, Account Status, Memberstack ID, and linked deals.",
    progress: "0%",
  },
  {
    id: "mt-16",
    taskName: "Create Sample Output Pack",
    workstream: "GTM Resources",
    phase: "Resources / Collateral",
    priority: "P2",
    status: "Not Started",
    relatedArea: "Dealality Platform",
    description:
      "Create sanitized or fictional sample showing Deal Readiness Snapshot, Brand / Operator Alignment Snapshot, Missing Information Checklist, and comparison support.",
    nextAction: "Select sample deal scenario and generate outputs.",
    progress: "0%",
  },
  {
    id: "mt-17",
    taskName: "Create Founder Video Script",
    workstream: "Content / LinkedIn",
    phase: "Resources / Collateral",
    priority: "P3",
    status: "Not Started",
    relatedArea: "GTM",
    description: "Draft 60–90 second founder video explaining Dealality and the pilot.",
    nextAction: "Draft script after first reply/call learnings.",
    progress: "0%",
  },
  {
    id: "mt-18",
    taskName: "Create LinkedIn GTM Content Series",
    workstream: "Content / LinkedIn",
    phase: "GTM / Outreach",
    priority: "P3",
    status: "Not Started",
    relatedArea: "GTM",
    description:
      "Draft 3 founder-led posts about fragmented hotel decision processes, relationship capital, and Dealality pilot learnings.",
    nextAction: "Draft after one-pagers are fully finalized.",
    progress: "0%",
  },
  {
    id: "mt-19",
    taskName: "Add Next 10–20 Pilot Targets",
    workstream: "Pilot Target List",
    phase: "GTM / Outreach",
    priority: "P2",
    status: "Not Started",
    relatedArea: "Pilot Target List",
    description:
      "Add next batch of targets beyond initial lawyer wave, including hospitality advisors/consultants, owners, operators, and brand/referral contacts.",
    nextAction: "Add but do not send until first wave signal is reviewed.",
    progress: "0%",
  },
  {
    id: "mt-20",
    taskName: "Review First Wave Performance",
    workstream: "Outreach Execution",
    phase: "Pilot Conversion",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Pilot Target List",
    description:
      "Review replies, call bookings, referral offers, and message resonance from first 10 contacts.",
    nextAction:
      "Summarize Wave 1 replies, follow-ups, and message resonance after Jul 9 follow-up window.",
    progress: "0%",
  },
  {
    id: "mt-21",
    taskName: "Select Wave 2 Send List",
    workstream: "Outreach Execution",
    phase: "GTM / Outreach",
    priority: "P2",
    status: "Not Started",
    relatedArea: "Pilot Target List",
    description: "Select next five targets after reviewing first-wave signal.",
    nextAction: "Prioritize advisor/consultant, owner/developer, operator, and brand/referral mix.",
    progress: "0%",
  },
  {
    id: "mt-22",
    taskName: "Prepare First Pilot Opportunity QA",
    workstream: "Product QA",
    phase: "Pilot Delivery",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Dealality Platform",
    description:
      "Before inviting a real pilot user, test deal creation, visibility, DRS, BAS, OAS, uploads, and print/export.",
    nextAction:
      "Run first pilot opportunity QA against Altiplano Mijares 32 once engagement is accepted and Diego is provisioned.",
    progress: "0%",
  },
  {
    id: "mt-23",
    taskName: "Capture Pilot Learning Log",
    workstream: "Data / Reporting",
    phase: "Pilot Conversion",
    priority: "P2",
    status: "Not Started",
    relatedArea: "GTM",
    description:
      "Create a place to capture what each outreach/call teaches about value proposition, objections, trust, willingness to refer, and pilot fit.",
    nextAction: "Add field or task notes structure.",
    progress: "0%",
  },
  {
    id: "mt-24",
    taskName: "Define Pilot Acceptance Criteria",
    workstream: "Pilot Offer",
    phase: "Pilot Conversion",
    priority: "P1",
    status: "Completed",
    relatedArea: "Owner Pilot",
    description:
      "Criteria drafted for accepting real pilot opportunities versus feedback/referral only.",
    nextAction: "Use EN+ES copy in docs/gtm-resources/ and Google Drive Word docs.",
    progress: "100%",
  },
  {
    id: "mt-25",
    taskName: "Defer Full Role-Based Portal Rebuild",
    workstream: "Access Hygiene",
    phase: "Later",
    priority: "P3",
    status: "Deferred",
    relatedArea: "Dealality Platform",
    description:
      "Full brand/operator/advisor portals, advanced region/deal access, paid plans, and self-serve onboarding should not block first pilot.",
    nextAction: "Revisit after first accepted pilot and user feedback.",
    progress: "0%",
  },
  {
    id: "mt-26",
    taskName: "Send Altiplano Brand, Operator & F&B Market Approach Proposal",
    workstream: "Pilot Offer",
    phase: "Pilot Conversion",
    priority: "P0",
    status: "Drafted",
    relatedArea: "Owner Pilot",
    description:
      "AO Hospitality Advisors six-week Brand, Operator & F&B Market Approach proposal for Altiplano Desarrolladora (San José del Cabo / Mijares 32), supported by Dealality workflow. Fee US$45,000; optional continuation US$7,500/mo. Team: Joan Dejarden + Michael Jones.",
    nextAction:
      "Final read-through of Word proposal on Google Drive; send to Diego Fernández Briseño; confirm intro path via German/Herman; log send date and reply in Pilot Target List.",
    progress: "75%",
  },
  {
    id: "mt-27",
    taskName: "Close Altiplano Engagement (SOW Signed + Deposit)",
    workstream: "Pilot Offer",
    phase: "Pilot Conversion",
    priority: "P0",
    status: "Waiting",
    relatedArea: "Owner Pilot",
    description:
      "Convert Altiplano proposal into a signed engagement: deposit 30% (US$13,500) upon signing, then commence six-week Brand/Operator/F&B market approach.",
    nextAction:
      "Await Diego response; answer questions; collect signed acceptance and deposit; set engagement kickoff date.",
    progress: "0%",
  },
  {
    id: "mt-28",
    taskName: "Deliver Altiplano M1 — Project Criteria & Dealality Readiness",
    workstream: "Pilot Delivery",
    phase: "Pilot Delivery",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Owner Pilot",
    description:
      "Week 1 engagement milestone: organize project facts, open gaps, brand/operator/F&B decision criteria, and Dealality readiness snapshot for Mijares 32.",
    nextAction: "Start after signed engagement + deposit; load Altiplano materials into Dealality workflow.",
    progress: "0%",
  },
  {
    id: "mt-29",
    taskName: "Deliver Altiplano M2–M3 — Strategy, Target List & Outreach Package",
    workstream: "Pilot Delivery",
    phase: "Pilot Delivery",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Owner Pilot",
    description:
      "Weeks 2–3: Brand/Operator/F&B strategy report plus curated target list (6–8 brands, 6–8 operators, F&B candidates) and outreach-ready project brief/question set. Milestone payment due on outreach package delivery.",
    nextAction: "Begin after M1 readiness snapshot is accepted by Altiplano.",
    progress: "0%",
  },
  {
    id: "mt-30",
    taskName: "Deliver Altiplano M4 — Response Comparison & Recommended Path",
    workstream: "Pilot Delivery",
    phase: "Pilot Delivery",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Owner Pilot",
    description:
      "Weeks 4–6: selected outreach support, response tracking/comparison matrix, active pipeline status, and recommended path memo. Final payment due on this deliverable.",
    nextAction: "Launch curated outreach after target list approval; track responses in Dealality.",
    progress: "0%",
  },
  {
    id: "mt-31",
    taskName: "Provision Altiplano / Diego Dealality Pilot Workspace",
    workstream: "Access Hygiene",
    phase: "Product / Access",
    priority: "P1",
    status: "Not Started",
    relatedArea: "Owner Pilot",
    description:
      "Provision Diego Fernández Briseño / Altiplano as a real pilot user with Company Profile, Mijares 32 deal linkage, Workspace Access, and Memberstack access for Dealality workflow support during the AO engagement.",
    nextAction:
      "After engagement acceptance, run pilot invite checklist using existing Altiplano company/deal seeds; complete access hygiene first if still open.",
    progress: "0%",
  },
];

export function validateMasterTodoSeed() {
  const errors = [];
  const names = new Set();
  for (const task of MASTER_TODO_SEED) {
    if (!task.taskName?.trim()) errors.push(`${task.id}: missing taskName`);
    if (!task.workstream?.trim()) errors.push(`${task.id}: missing workstream`);
    if (!task.phase?.trim()) errors.push(`${task.id}: missing phase`);
    if (!task.status?.trim()) errors.push(`${task.id}: missing status`);
    if (!task.priority?.trim()) errors.push(`${task.id}: missing priority`);
    const norm = task.taskName.trim().toLowerCase();
    if (names.has(norm)) errors.push(`duplicate taskName: ${task.taskName}`);
    names.add(norm);
    if (!VAL_MASTER_TODO_STATUS.includes(task.status)) {
      errors.push(`${task.id}: invalid status "${task.status}"`);
    }
    if (!VAL_MASTER_TODO_PRIORITY.includes(task.priority)) {
      errors.push(`${task.id}: invalid priority "${task.priority}"`);
    }
    if (!VAL_MASTER_TODO_PHASE.includes(task.phase)) {
      errors.push(`${task.id}: invalid phase "${task.phase}"`);
    }
    if (!VAL_MASTER_TODO_WORKSTREAM.includes(task.workstream)) {
      errors.push(`${task.id}: invalid workstream "${task.workstream}"`);
    }
  }
  return errors;
}
