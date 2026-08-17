/**
 * Field-fill inference for Founder Project Plan and Pilot Target List.
 */
import { MAP_MASTER_TODO as F, MASTER_TODO_SOURCE_VALUE, mapPriorityForWrite } from "./master-todo-field-map.js";
import { MASTER_TODO_SEED } from "./master-todo-seed.js";

const FPP = {
  phase: "Phase",
  workstream: "Workstream",
  task: "Task",
  description: "Task Objective/Description",
  deliverables: "Deliverables",
  owner: "Assigned To",
  source: "Source",
  relatedArea: "Related Area",
  progress: "Progress",
  status: "Status",
  start: "Start",
  end: "End",
  completedDate: "Completed Date",
  priority: "Priority",
  sprintWave: "Sprint / Wave",
  dependency: "Dependency",
  blocker: "Blocker",
  nextAction: "Next Action",
  milestone: "Milestone?",
  relatedTable: "Related Table",
  successMetric: "Success Metric",
};

export const PILOT_TARGET_LIST_TABLE_ID = "tblgsKWuI25MWohAP";
export const FILL_AS_OF_DATE = "2026-07-03";

/** @type {Record<string, Partial<Record<string, unknown>>>} */
export const MASTER_FIELD_ENRICHMENT = {
  "mt-01": {
    deliverables: "Owner/Developer Pilot Overview one-pager (final PDF/doc)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-06-01",
    end: "2026-06-28",
    completedDate: "2026-06-28",
    milestone: true,
    dependency: "Pilot offer framing and GTM positioning",
    blocker: "None",
  },
  "mt-02": {
    deliverables: "Lawyer / Advisor Referral Overview one-pager (final PDF/doc)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-06-01",
    end: "2026-06-28",
    completedDate: "2026-06-28",
    milestone: true,
    dependency: "Owner opt-in referral model defined",
    blocker: "None",
  },
  "mt-03": {
    deliverables: "Advisor / Consultant Pilot Overview one-pager (final PDF/doc)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-06-01",
    end: "2026-06-28",
    completedDate: "2026-06-28",
    milestone: true,
    dependency: "Advisor pilot positioning approved",
    blocker: "None",
  },
  "mt-04": {
    deliverables: "Warm intro blurb (copy/paste ready)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-07",
    end: "2026-07-09",
    milestone: false,
    dependency: "Pilot overview one-pagers finalized",
    blocker: "None",
  },
  "mt-05": {
    deliverables: "Reply playbook with templates by reply type",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-07",
    end: "2026-07-10",
    milestone: false,
    dependency: "Warm intro blurb and pilot overviews",
    blocker: "None",
  },
  "mt-06": {
    deliverables: "Pilot call script (lawyer, advisor, owner variants)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-07",
    end: "2026-07-10",
    milestone: false,
    dependency: "Pilot acceptance criteria draft",
    blocker: "None",
  },
  "mt-07": {
    deliverables: "Pilot Target List rows updated for all first-wave sends",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-02",
    end: "2026-07-11",
    milestone: false,
    dependency: "Wave 1 outreach sent",
    blocker: "June 16 send rows still missing follow-up metadata",
  },
  "mt-08": {
    deliverables: "Reply log entries in Pilot Target List",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-02",
    end: "2026-07-18",
    milestone: false,
    dependency: "First-wave outreach live",
    blocker: "No replies logged in Reply Notes yet",
  },
  "mt-09": {
    deliverables: "No-reply follow-up copy + send schedule",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-03",
    end: "2026-07-11",
    milestone: false,
    dependency: "Wave 1 Next Follow-Up Date set (Jul 9)",
    blocker: "None",
  },
  "mt-10": {
    deliverables: "Pilot intake question set",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-07-10",
    end: "2026-07-18",
    milestone: true,
    dependency: "Pilot acceptance criteria finalized",
    blocker: "None",
  },
  "mt-11": {
    deliverables: "Participant-facing 'What We Need From You' checklist",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-07-10",
    end: "2026-07-18",
    milestone: false,
    dependency: "Pilot intake questions drafted",
    blocker: "None",
  },
  "mt-12": {
    deliverables: "Pilot review call agenda",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-07-21",
    end: "2026-08-01",
    milestone: false,
    dependency: "First pilot opportunity accepted",
    blocker: "None",
  },
  "mt-13": {
    deliverables: "Pilot feedback form / question set",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-08-01",
    end: "2026-08-14",
    milestone: false,
    dependency: "First pilot delivery completed",
    blocker: "None",
  },
  "mt-14": {
    deliverables: "Access hygiene test results + issue log",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Properties",
    start: "2026-07-07",
    end: "2026-07-14",
    milestone: true,
    dependency: "Webflow/Memberstack/Airtable environments stable",
    blocker: "None",
  },
  "mt-15": {
    deliverables: "Pilot invite provisioning checklist (documented SOP)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Contacts",
    start: "2026-07-14",
    end: "2026-07-18",
    milestone: false,
    dependency: "Access hygiene sprint complete",
    blocker: "None",
  },
  "mt-16": {
    deliverables: "Sanitized sample output pack (DRS, BAS, OAS, MIC)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Properties",
    start: "2026-07-21",
    end: "2026-08-07",
    milestone: false,
    dependency: "Sample deal scenario selected",
    blocker: "None",
  },
  "mt-17": {
    deliverables: "60–90 second founder video script",
    sprintWave: "Pilot Wave 2",
    relatedTable: "Pilot Target List",
    start: "2026-08-04",
    end: "2026-08-15",
    milestone: false,
    dependency: "First reply/call learnings captured",
    blocker: "None",
  },
  "mt-18": {
    deliverables: "3-post LinkedIn GTM content series (drafts)",
    sprintWave: "Pilot Wave 2",
    relatedTable: "Pilot Target List",
    start: "2026-08-04",
    end: "2026-08-22",
    milestone: false,
    dependency: "One-pagers and first-wave learnings",
    blocker: "None",
  },
  "mt-19": {
    deliverables: "10–20 new Pilot Target List records (not sent)",
    sprintWave: "Pilot Wave 2",
    relatedTable: "Pilot Target List",
    start: "2026-07-15",
    end: "2026-07-22",
    milestone: false,
    dependency: "First wave performance review",
    blocker: "Do not send until Wave 1 signal reviewed",
  },
  "mt-20": {
    deliverables: "First wave performance summary",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-10",
    end: "2026-07-14",
    milestone: true,
    dependency: "Jul 9 follow-up window complete",
    blocker: "None",
  },
  "mt-21": {
    deliverables: "Wave 2 send list (5 prioritized targets)",
    sprintWave: "Pilot Wave 2",
    relatedTable: "Pilot Target List",
    start: "2026-07-15",
    end: "2026-07-22",
    milestone: false,
    dependency: "First wave performance review",
    blocker: "None",
  },
  "mt-22": {
    deliverables: "Pilot opportunity QA checklist (passed/failed)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Properties",
    start: "2026-07-21",
    end: "2026-08-07",
    milestone: true,
    dependency: "Pilot opportunity accepted",
    blocker: "Waiting for first accepted pilot",
  },
  "mt-23": {
    deliverables: "Pilot learning log structure + initial entries",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Pilot Target List",
    start: "2026-07-07",
    end: "2026-07-18",
    milestone: false,
    dependency: "Outreach and call activity underway",
    blocker: "None",
  },
  "mt-24": {
    deliverables: "Pilot acceptance criteria document (final)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-07-07",
    end: "2026-07-09",
    milestone: true,
    dependency: "Pilot offer framing",
    blocker: "None",
  },
  "mt-25": {
    deliverables: "Deferred — full role-based portal rebuild scope doc",
    sprintWave: "Backlog",
    relatedTable: "Properties",
    start: "2026-10-01",
    end: "2026-12-31",
    milestone: false,
    dependency: "First accepted pilot + user feedback",
    blocker: "Intentionally deferred",
  },
  "mt-26": {
    deliverables: "Altiplano Brand/Operator/F&B market approach proposal (Word)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-07-15",
    end: "2026-07-18",
    milestone: true,
    dependency: "Diego Fernández Jul 15 discovery call completed",
    blocker: "None",
  },
  "mt-27": {
    deliverables: "Signed Altiplano SOW + 30% deposit (US$13,500)",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-07-16",
    end: "2026-07-31",
    milestone: true,
    dependency: "Proposal sent to Diego",
    blocker: "Waiting for Altiplano signature/deposit",
  },
  "mt-28": {
    deliverables: "Project Criteria & Dealality Readiness Snapshot",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-08-03",
    end: "2026-08-08",
    milestone: true,
    dependency: "Altiplano engagement signed + deposit received",
    blocker: "Waiting for signed engagement",
  },
  "mt-29": {
    deliverables: "Brand/Operator/F&B Strategy Report + Target List & Outreach Package",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-08-10",
    end: "2026-08-22",
    milestone: true,
    dependency: "Altiplano M1 readiness snapshot accepted",
    blocker: "Waiting for signed engagement",
  },
  "mt-30": {
    deliverables: "Response Comparison Matrix, Active Pipeline Status & Recommended Path Memo",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Owner Targets",
    start: "2026-08-24",
    end: "2026-09-12",
    milestone: true,
    dependency: "Altiplano M3 outreach package delivered",
    blocker: "Waiting for signed engagement",
  },
  "mt-31": {
    deliverables: "Diego / Altiplano Users + Company Profile + Mijares 32 deal access live",
    sprintWave: "Pilot Wave 1",
    relatedTable: "Contacts",
    start: "2026-07-28",
    end: "2026-08-08",
    milestone: true,
    dependency: "Altiplano engagement accepted + access hygiene complete",
    blocker: "Waiting for signed engagement",
  },
};

const seedById = new Map(MASTER_TODO_SEED.map((s) => [s.id, s]));

export function parseSeedId(fields) {
  const sm = String(fields?.[F.successMetric] || "");
  const m = sm.match(/Seed ID: (mt-\d+)/);
  return m?.[1] || null;
}

export function isEmpty(value) {
  if (value === undefined || value === null || value === "") return true;
  if (Array.isArray(value) && value.length === 0) return true;
  return false;
}

function setIfEmpty(target, key, value, existing = {}) {
  if (isEmpty(value)) return;
  if (!isEmpty(existing[key])) return;
  if (isEmpty(target[key])) target[key] = value;
}

function addBusinessDays(isoDate, days) {
  const d = new Date(`${isoDate}T12:00:00Z`);
  let added = 0;
  while (added < days) {
    d.setUTCDate(d.getUTCDate() + 1);
    const dow = d.getUTCDay();
    if (dow !== 0 && dow !== 6) added += 1;
  }
  return d.toISOString().slice(0, 10);
}

export function inferSprintWave(phase) {
  const p = String(phase || "");
  if (/GTM|Pilot Conversion|Pilot Delivery|Product \/ Access|Resources|Pilot Readiness|Testing & Pilot/i.test(p)) {
    return "Pilot Wave 1";
  }
  if (/Later|Scale/i.test(p)) return "Backlog";
  if (/Launch/i.test(p)) return "Post-Pilot";
  if (/Platform Build|Content & GTM/i.test(p)) return "Founder Sprint 2";
  if (/Strategy|Design/i.test(p)) return "Founder Sprint 1";
  return "Backlog";
}

export function inferFounderRelatedArea(phase, workstream) {
  const ws = String(workstream || "").toLowerCase();
  const ph = String(phase || "").toLowerCase();
  if (/outreach|gtm|content|referral|sales/i.test(ws) || /content & gtm/i.test(ph)) return "GTM";
  if (/pilot|owner/i.test(ws) || /testing & pilot/i.test(ph)) return "Owner Pilot";
  if (/platform|product|ux|match|data model|wireframe|mvp/i.test(ws) || /platform/i.test(ph)) {
    return "Dealality Platform";
  }
  return "Dealality Platform";
}

export function inferRelatedTable(workstream, phase) {
  const ws = String(workstream || "").toLowerCase();
  if (/pilot target|outreach|crm|owner outreach|brand.*outreach/i.test(ws)) return "Pilot Target List";
  if (/owner/i.test(ws)) return "Owner Targets";
  if (/contact|referral/i.test(ws)) return "Contacts";
  if (/compan/i.test(ws)) return "Companies";
  if (/property|deal/i.test(ws)) return "Properties";
  if (/testing & pilot|pilot readiness/i.test(String(phase || ""))) return "Pilot Target List";
  return "Founder Project Plan";
}

export function inferFounderPriority(status) {
  const s = String(status || "");
  if (s === "In Progress") return mapPriorityForWrite("P1");
  if (s === "Blocked") return mapPriorityForWrite("P1");
  if (s === "Completed" || s === "Not Needed") return mapPriorityForWrite("P3");
  if (s === "Not Started") return mapPriorityForWrite("P2");
  return mapPriorityForWrite("P2");
}

export function inferNextAction(status) {
  const s = String(status || "");
  if (s === "Completed") return "No action required — task completed.";
  if (s === "Not Needed") return "No action required — not in current scope.";
  if (s === "Deferred") return "Revisit when dependencies are met.";
  if (s === "Blocked") return "Resolve blocker before resuming work.";
  if (s === "In Progress") return "Continue active work; update Progress at next milestone.";
  if (s === "Needs Review") return "Review deliverable and approve or request changes.";
  if (s === "Drafted") return "Polish draft and move to Needs Review or Completed.";
  if (s === "Waiting") return "Follow up on external dependency.";
  return "Schedule and begin work on this task.";
}

export function inferDeliverables(fields) {
  const existing = fields[FPP.deliverables];
  if (!isEmpty(existing)) return existing;
  const task = fields[FPP.task];
  if (!isEmpty(task)) return `Deliverable supporting: ${String(task).slice(0, 120)}`;
  return "Phase-level deliverables tracked in child tasks";
}

export function inferSuccessMetric(fields, isMaster) {
  if (isMaster) return fields[FPP.successMetric];
  const ws = fields[FPP.workstream] || fields[FPP.phase] || "Founder roadmap";
  return `Founder roadmap — ${ws}`;
}

export function isPhaseRollup(fields) {
  return isEmpty(fields[FPP.task]) && !isEmpty(fields[FPP.phase]);
}

export function buildMasterFillPatch(record, schemaFieldNames) {
  const seedId = parseSeedId(record.fields);
  if (!seedId) return null;
  const seed = seedById.get(seedId);
  const enrich = MASTER_FIELD_ENRICHMENT[seedId] || {};
  const existing = record.fields || {};
  const patch = {};
  const has = (n) => schemaFieldNames.has(n.toLowerCase());

  if (has(FPP.deliverables)) setIfEmpty(patch, FPP.deliverables, enrich.deliverables, existing);
  if (has(FPP.sprintWave)) setIfEmpty(patch, FPP.sprintWave, enrich.sprintWave, existing);
  if (has(FPP.relatedTable)) setIfEmpty(patch, FPP.relatedTable, enrich.relatedTable, existing);
  if (has(FPP.start)) setIfEmpty(patch, FPP.start, enrich.start, existing);
  if (has(FPP.end)) setIfEmpty(patch, FPP.end, enrich.end, existing);
  if (has(FPP.completedDate) && seed?.status === "Completed") {
    setIfEmpty(patch, FPP.completedDate, enrich.completedDate, existing);
  }
  if (has(FPP.dependency)) setIfEmpty(patch, FPP.dependency, enrich.dependency, existing);
  if (has(FPP.blocker)) setIfEmpty(patch, FPP.blocker, enrich.blocker, existing);
  if (has(FPP.milestone) && patch[FPP.milestone] === undefined && enrich.milestone !== undefined) {
    if (isEmpty(existing[FPP.milestone])) patch[FPP.milestone] = enrich.milestone;
  }
  if (has(FPP.owner)) setIfEmpty(patch, FPP.owner, "Joan D.", existing);
  if (has(FPP.source)) setIfEmpty(patch, FPP.source, MASTER_TODO_SOURCE_VALUE, existing);
  if (has(FPP.relatedArea) && seed?.relatedArea) setIfEmpty(patch, FPP.relatedArea, seed.relatedArea, existing);
  if (has(FPP.description) && seed?.description) setIfEmpty(patch, FPP.description, seed.description, existing);
  if (has(FPP.nextAction) && seed?.nextAction) setIfEmpty(patch, FPP.nextAction, seed.nextAction, existing);

  return Object.keys(patch).length ? patch : null;
}

export function buildFounderFillPatch(record, schemaFieldNames) {
  const fields = record.fields || {};
  if (fields[FPP.source] === MASTER_TODO_SOURCE_VALUE || parseSeedId(fields)) return null;

  const patch = {};
  const has = (n) => schemaFieldNames.has(n.toLowerCase());

  if (isPhaseRollup(fields)) {
    const phase = fields[FPP.phase];
    setIfEmpty(patch, FPP.task, `[Phase rollup] ${phase}`, fields);
    setIfEmpty(patch, FPP.workstream, "Founder / PMO", fields);
    setIfEmpty(
      patch,
      FPP.description,
      `Aggregate progress tracker for all tasks in the ${phase} phase.`,
      fields
    );
    setIfEmpty(patch, FPP.deliverables, `Phase completion for ${phase}`, fields);
    if (has(FPP.milestone) && isEmpty(fields[FPP.milestone])) patch[FPP.milestone] = true;
  }

  if (has(FPP.source)) setIfEmpty(patch, FPP.source, "Manual", fields);
  if (has(FPP.relatedArea)) {
    setIfEmpty(
      patch,
      FPP.relatedArea,
      inferFounderRelatedArea(fields[FPP.phase], fields[FPP.workstream] || patch[FPP.workstream]),
      fields
    );
  }
  if (has(FPP.priority)) setIfEmpty(patch, FPP.priority, inferFounderPriority(fields[FPP.status]), fields);
  if (has(FPP.sprintWave)) setIfEmpty(patch, FPP.sprintWave, inferSprintWave(fields[FPP.phase]), fields);
  if (has(FPP.relatedTable)) {
    setIfEmpty(
      patch,
      FPP.relatedTable,
      inferRelatedTable(fields[FPP.workstream] || patch[FPP.workstream], fields[FPP.phase]),
      fields
    );
  }
  if (has(FPP.nextAction)) setIfEmpty(patch, FPP.nextAction, inferNextAction(fields[FPP.status]), fields);
  if (has(FPP.deliverables)) setIfEmpty(patch, FPP.deliverables, inferDeliverables({ ...fields, ...patch }), fields);
  if (has(FPP.successMetric)) setIfEmpty(patch, FPP.successMetric, inferSuccessMetric({ ...fields, ...patch }, false), fields);
  if (has(FPP.description)) {
    setIfEmpty(
      patch,
      FPP.description,
      fields[FPP.task] ? `Objective: ${fields[FPP.task]}` : patch[FPP.description],
      fields
    );
  }
  if (has(FPP.dependency)) setIfEmpty(patch, FPP.dependency, "See dependent tasks in same Workstream / Phase", fields);
  if (has(FPP.blocker)) {
    if (fields[FPP.status] === "Blocked") setIfEmpty(patch, FPP.blocker, "Blocker not documented — update manually", fields);
    else setIfEmpty(patch, FPP.blocker, "None", fields);
  }
  if (has(FPP.owner)) setIfEmpty(patch, FPP.owner, "Joan D.", fields);
  if (has(FPP.milestone) && isEmpty(fields[FPP.milestone]) && !patch[FPP.milestone]) {
    patch[FPP.milestone] = fields[FPP.status] === "Completed";
  }
  if (has(FPP.completedDate) && fields[FPP.status] === "Completed" && fields[FPP.end]) {
    setIfEmpty(patch, FPP.completedDate, fields[FPP.end], fields);
  }
  if (has(FPP.workstream) && isEmpty(fields[FPP.workstream]) && !isPhaseRollup(fields)) {
    setIfEmpty(patch, FPP.workstream, fields[FPP.phase] || "Founder / PMO", fields);
  }

  return Object.keys(patch).length ? patch : null;
}

const PTL = {
  outreachStatus: "Outreach Status",
  lastContact: "Last Contact Date",
  nextFollowUp: "Next Follow-Up Date",
  sendChannel: "Send Channel",
  mailMergeBatch: "Mail Merge Batch",
  replyNotes: "Reply Notes",
  nextAction: "Next Action",
  language: "Language",
  priority: "Priority",
  readyMerge: "Ready for Mail Merge",
  doNotContact: "Do Not Contact",
  pilotRegion: "Pilot Region",
  status: "Status",
  category: "Category",
  outreachSegment: "Outreach Segment",
};

const REGION_TO_PILOT = {
  "Caribbean & Latin America": "CALA",
  "Latin America": "Latin America",
  Mexico: "Mexico",
  Caribbean: "Caribbean",
  "Central America": "Central America",
  "South America": "South America",
  "United States": "United States / Canada",
  Canada: "United States / Canada",
  Europe: "Europe / Spain",
  "Global / Multi-Region": "Global / Multi-Region",
};

export function inferPilotRegion(regionField) {
  if (!regionField) return "CALA";
  const regions = Array.isArray(regionField) ? regionField : [regionField];
  for (const r of regions) {
    if (REGION_TO_PILOT[r]) return REGION_TO_PILOT[r];
  }
  return "CALA";
}

export function inferOutreachSegment(category) {
  const c = String(category || "").toLowerCase();
  if (c.includes("lawyer") || c.includes("advisor") || c.includes("consultant") || c.includes("broker")) {
    return "Advisor / Consultant / Broker";
  }
  if (c.includes("owner")) return "Owner / Investor";
  if (c.includes("operator")) return "Operator";
  if (c.includes("brand")) return "Brand / Referral Source";
  return "Other";
}

export function buildPilotTargetFillPatch(record, schemaFieldNames) {
  const fields = record.fields || {};
  const patch = {};
  const has = (n) => schemaFieldNames.has(n.toLowerCase());

  const outreach = fields[PTL.outreachStatus];
  const lastContact = fields[PTL.lastContact];
  const effectiveOutreach = outreach || (lastContact ? "Sent" : outreach);

  if (has(PTL.language)) setIfEmpty(patch, PTL.language, "English", fields);
  if (has(PTL.priority)) setIfEmpty(patch, PTL.priority, "P2", fields);
  if (has(PTL.readyMerge) && isEmpty(fields[PTL.readyMerge])) patch[PTL.readyMerge] = false;
  if (has(PTL.doNotContact) && isEmpty(fields[PTL.doNotContact])) patch[PTL.doNotContact] = false;
  if (has(PTL.pilotRegion)) setIfEmpty(patch, PTL.pilotRegion, inferPilotRegion(fields.Region), fields);
  if (has(PTL.outreachSegment) && fields[PTL.category]) {
    setIfEmpty(patch, PTL.outreachSegment, inferOutreachSegment(fields[PTL.category]), fields);
  }
  if (has(PTL.status)) setIfEmpty(patch, PTL.status, "Not Contacted", fields);

  if ((effectiveOutreach === "Sent" || outreach === "Sent") && lastContact) {
    if (isEmpty(outreach) && has(PTL.outreachStatus)) patch[PTL.outreachStatus] = "Sent";
    if (has(PTL.mailMergeBatch) && isEmpty(fields[PTL.mailMergeBatch])) {
      if (lastContact === "2026-07-02") patch[PTL.mailMergeBatch] = "Wave 1 - Lawyers / Advisors";
      else if (lastContact === "2026-06-16") patch[PTL.mailMergeBatch] = "Pre-Wave 1 — Jun 16 Outreach";
      else patch[PTL.mailMergeBatch] = `Outreach — ${lastContact}`;
    }
    if (has(PTL.sendChannel) && isEmpty(fields[PTL.sendChannel])) {
      patch[PTL.sendChannel] = "LinkedIn";
    }
    if (has(PTL.nextFollowUp) && isEmpty(fields[PTL.nextFollowUp])) {
      patch[PTL.nextFollowUp] = addBusinessDays(lastContact, 5);
    }
    if (has(PTL.replyNotes) && isEmpty(fields[PTL.replyNotes])) {
      patch[PTL.replyNotes] = `No reply logged as of ${FILL_AS_OF_DATE}.`;
    }
    if (has(PTL.nextAction) && isEmpty(fields[PTL.nextAction])) {
      patch[PTL.nextAction] = "Monitor for reply; follow up if no response after follow-up date.";
    }
  }

  if (isEmpty(outreach) && !lastContact && has(PTL.outreachStatus)) {
    if (fields["Email Draft"] || fields["Final Approved Email"]) patch[PTL.outreachStatus] = "Drafted";
    else if (fields[PTL.category] === "Lawyer") patch[PTL.outreachStatus] = "Draft Needed";
    else patch[PTL.outreachStatus] = "Not Started";
  }

  if (outreach === "Draft Needed" && has(PTL.nextAction) && isEmpty(fields[PTL.nextAction])) {
    patch[PTL.nextAction] = "Draft and review outreach message before sending.";
  }

  if (has(PTL.nextAction) && isEmpty(fields[PTL.nextAction]) && !patch[PTL.nextAction]) {
    patch[PTL.nextAction] = "Research contact and prepare outreach when prioritized.";
  }

  return Object.keys(patch).length ? patch : null;
}

export { FPP };
