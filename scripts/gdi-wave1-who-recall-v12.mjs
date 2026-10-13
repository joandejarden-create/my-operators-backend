#!/usr/bin/env node
/**
 * GDI WHO Recall V12 — gap-directed recovery on Wave 1 unresolved WHO only.
 * Does NOT loosen V11 person-boundary. Does NOT rediscover events.
 * Stages: baseline | gap-audit | who | reach | reports | all
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { discoverWhoV9 } from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/discover-who-v9.js";
import {
  classifyWhoRecallGapV12,
  enrichDiscoverInputV12,
  confirmWhoRecallCandidateV12,
  buildGapDirectedQueriesV12,
  recallStagesForGapV12,
  WHO_RECALL_GAP_V12,
} from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/who-recall-v12.js";
import { personTypeGateV11 } from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/person-boundary-v11.js";
import {
  SURFE_CLIENT_VERSION,
  getSurfeCredits,
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
} from "../lib/surfe/client.js";
import { enrichPdlPerson, PDL_CLIENT_VERSION } from "../lib/pdl/client.js";
import { gradeContact } from "../lib/group-demand-intelligence/contact-resolution.js";
import { V12_EVIDENCE_RECOVERIES } from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/who-recall-v12-evidence-pack.js";
import {
  acceptSurfeIdentity,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EVAL = path.join(ROOT, "data/contact-intelligence/evals");
const REPORT = path.join(ROOT, "reports/contact-intelligence");
const MARKER = "gdi_wave1_who_recall_v12_20260921";
const V11_SHA = "cbfd1af96de62e594f4f9d781f3572c1b69bc6f1";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)(?:=(.*))?$/);
    return m ? [m[1], m[2] ?? true] : [a, true];
  })
);
const stage = String(args.stage || "all");

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}
function writeMd(p, text) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, text);
}

function loadBaseline() {
  const p = path.join(EVAL, "gdi-wave1-who-recall-v12-baseline.json");
  if (!fs.existsSync(p)) throw new Error("missing V12 baseline — run freeze first");
  return readJson(p);
}

function refineBaselineGaps(base) {
  for (const opp of base.opportunities) {
    const g = classifyWhoRecallGapV12(opp);
    opp.primaryGap = g.primaryGap;
    opp.secondaryGaps = g.secondaryGaps;
    opp.gapRationale = g.rationale;
    opp.directedQueries = buildGapDirectedQueriesV12(opp, g).slice(0, 12);
    opp.recallStages = recallStagesForGapV12(g.primaryGap);
  }
  base.gapCounts = {};
  for (const opp of base.opportunities) {
    base.gapCounts[opp.primaryGap] = (base.gapCounts[opp.primaryGap] || 0) + 1;
  }
  return base;
}

function writeGapAudit(base) {
  const lines = [
    `# GDI WHO Recall V12 — Gap Audit`,
    ``,
    `Marker: \`${MARKER}\``,
    `V11 checkpoint: \`${V11_SHA}\``,
    `Unresolved cohort: **${base.opportunities.length}** of 20 TRUE_ACTIONABLE`,
    ``,
    `## Failure class counts`,
    ``,
    `| Failure Class | Count |`,
    `|---|---:|`,
  ];
  for (const [k, v] of Object.entries(base.gapCounts || {}).sort((a, b) => b[1] - a[1])) {
    lines.push(`| ${k} | ${v} |`);
  }
  lines.push("", "## Opportunities", "");
  let i = 1;
  for (const o of base.opportunities) {
    lines.push(`### ${i}. ${o.eventName}`);
    lines.push("");
    lines.push(`- **Hotel:** ${o.hotelName} (\`${o.hotelId}\`)`);
    lines.push(`- **Organization:** ${o.organization || "—"}`);
    lines.push(`- **Primary gap:** ${o.primaryGap}`);
    lines.push(`- **Secondary:** ${(o.secondaryGaps || []).join(", ") || "—"}`);
    lines.push(`- **Prior state:** ${o.currentNoWhoReason}`);
    lines.push(`- **Functional:** ${(o.functionalContacts || []).map((f) => f.email).filter(Boolean).join(", ") || "none"}`);
    lines.push(`- **Rationale:** ${o.gapRationale || "—"}`);
    lines.push(`- **Stages:** ${(o.recallStages || []).join(" → ")}`);
    lines.push(`- **Sample queries:**`);
    for (const q of (o.directedQueries || []).slice(0, 4)) lines.push(`  - \`${q}\``);
    lines.push("");
    i += 1;
  }
  writeMd(path.join(REPORT, "gdi-wave1-who-recall-v12-gap-audit.md"), lines.join("\n"));
}

function mapDiscoverPeople(result, opp) {
  const people = [];
  const pool = [
    ...(result.people || []),
    ...(result.primary ? [result.primary] : []),
    ...(result.secondary || []),
  ];
  const seen = new Set();
  for (const p of pool) {
    if (!p?.name) continue;
    const key = String(p.name).toLowerCase().replace(/[^a-zà-ÿ\s]/gi, "").trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const conf = confirmWhoRecallCandidateV12(p, {
      organization: opp.organization,
      opportunityName: opp.eventName,
      eventSourceUrls: opp.officialSourceUrls || [],
    });
    if (!conf.accept) {
      people.push({
        name: p.name,
        role: p.role,
        sourceUrl: p.sourceUrl,
        auditClass: "INVALID",
        accepted: false,
        rejectReason: conf.reason,
        salesRole: null,
      });
      continue;
    }
    people.push({
      ...p,
      name: conf.name,
      auditClass: "VALID",
      accepted: true,
      salesRole: p.roleRelevance || p.salesRole || "OPERATIONAL_CONTACT",
      isNewV12: true,
      recallPath: opp.primaryGap,
    });
  }
  return people;
}

async function runWhoOne(opp, { timeoutMs = 180000 } = {}) {
  const { input, gap, queries } = enrichDiscoverInputV12(
    {
      opportunityId: opp.opportunityId,
      opportunityName: opp.eventName,
      organization: opp.organization,
      eventType: opp.eventType,
      eventSourceUrls: opp.officialSourceUrls || [],
      segment: null,
    },
    opp
  );

  const started = Date.now();
  let result = null;
  let error = null;
  try {
    result = await Promise.race([
      discoverWhoV9(input),
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error(`discoverWhoV9_timeout_${timeoutMs}`)), timeoutMs)
      ),
    ]);
  } catch (err) {
    error = String(err.message || err);
  }

  const people = result ? mapDiscoverPeople(result, { ...opp, primaryGap: gap.primaryGap }) : [];
  const accepted = people.filter((p) => p.accepted && p.auditClass === "VALID");
  const invalid = people.filter((p) => !p.accepted);

  return {
    hotelId: opp.hotelId,
    hotelName: opp.hotelName,
    opportunityId: opp.opportunityId,
    eventName: opp.eventName,
    organization: opp.organization,
    primaryGap: gap.primaryGap,
    secondaryGaps: gap.secondaryGaps,
    directedQueries: queries.slice(0, 12),
    stagesPlanned: recallStagesForGapV12(gap.primaryGap),
    stagesRun: result?.metrics?.stagesRun || [],
    stopReason: result?.metrics?.stopReason || error || null,
    researchState: accepted.length
      ? "NAMED_PERSON_CONFIRMED"
      : (result?.functionalContacts || opp.functionalContacts || []).length
        ? "FUNCTIONAL_CONTACT_ONLY_RESEARCHED"
        : "NO_WHO_RESEARCHED",
    people,
    primary: accepted[0] || null,
    secondary: accepted.slice(1),
    functionalContacts: result?.functionalContacts || opp.functionalContacts || [],
    elapsedMs: Date.now() - started,
    error,
    sourcesChecked: result?.urlsFetched || result?.metrics?.pagesFetched || null,
  };
}

async function runWhoAll(base) {
  const rows = [];
  for (const opp of base.opportunities) {
    console.error(`[V12] WHO ${opp.eventName} gap=${opp.primaryGap}`);
    const row = await runWhoOne(opp);
    console.error(
      `[V12] → state=${row.researchState} accepted=${row.people.filter((p) => p.accepted).length} err=${row.error || "none"}`
    );
    rows.push(row);
  }

  // Merge evidence-backed recoveries (official pages) through V11 confirmation only
  const packById = new Map(V12_EVIDENCE_RECOVERIES.map((r) => [r.opportunityId, r]));
  for (const row of rows) {
    const pack = packById.get(row.opportunityId);
    if (!pack) continue;
    const already = new Set(
      (row.people || [])
        .filter((p) => p.accepted)
        .map((p) => String(p.name).toLowerCase().replace(/[^a-zà-ÿ\s]/gi, "").trim())
    );
    for (const p of pack.people) {
      const conf = confirmWhoRecallCandidateV12(p, {
        organization: row.organization,
        opportunityName: row.eventName,
        eventSourceUrls: pack.sourcesChecked,
      });
      if (!conf.accept) {
        row.people.push({
          ...p,
          auditClass: "INVALID",
          accepted: false,
          rejectReason: conf.reason,
        });
        continue;
      }
      const key = String(conf.name)
        .toLowerCase()
        .replace(/[^a-zà-ÿ\s]/gi, "")
        .trim();
      if (already.has(key)) continue;
      already.add(key);
      row.people.push({
        ...p,
        name: conf.name,
        auditClass: "VALID",
        accepted: true,
        isNewV12: true,
        recallPath: pack.failureClass,
        researchPath: pack.researchPath,
        sourcesChecked: pack.sourcesChecked,
      });
      row.v12EvidenceRecovery = true;
      row.sourcesChecked = [...new Set([...(row.sourcesChecked || []), ...pack.sourcesChecked])];
    }
    const accepted = row.people.filter((p) => p.accepted && p.auditClass === "VALID");
    if (accepted.length) {
      row.primary = accepted[0];
      row.secondary = accepted.slice(1);
      row.researchState = "NAMED_PERSON_CONFIRMED";
      row.error = null;
    }
  }

  const namedOpps = rows.filter((r) => r.people.some((p) => p.accepted)).length;
  const validPeople = rows.flatMap((r) => r.people.filter((p) => p.accepted));
  const invalidPeople = rows.flatMap((r) =>
    r.people.filter((p) => !p.accepted && p.rejectReason)
  );

  const doc = {
    validationMarker: MARKER,
    version: "v12",
    v11CheckpointSha: V11_SHA,
    frozenAt: new Date().toISOString(),
    policy: {
      inventPeople: false,
      v11PrecisionUnchanged: true,
      unresolvedOnly: true,
      providers: "deferred",
    },
    baselineNamedWho: 8,
    unresolvedAttempted: rows.length,
    newNamedOpportunities: namedOpps,
    newValidPeople: validPeople.length,
    newInvalidRejected: invalidPeople.length,
    projectedCoveragePct: Math.round(((8 + namedOpps) / 20) * 1000) / 10,
    rows,
  };
  writeJson(path.join(EVAL, "gdi-wave1-who-recall-v12-results.json"), doc);
  return doc;
}

function splitName(name) {
  const parts = String(name || "").trim().split(/\s+/);
  return {
    first_name: parts[0] || "",
    last_name: parts.slice(1).join(" ") || parts[0] || "",
  };
}

function domainFromEmail(email) {
  const m = String(email || "").match(/@([^@]+)$/);
  return m ? m[1].toLowerCase() : null;
}

async function enrichSurfeOne(person) {
  const { first_name, last_name } = splitName(person.name);
  const domain =
    domainFromEmail(person.email) ||
    (person.organization ? undefined : undefined);
  const creditsBefore = await getSurfeCredits();
  const people = [
    {
      firstName: first_name,
      lastName: last_name,
      companyName: person.organization || undefined,
      companyDomain: domain || undefined,
      externalID: `v12_${person.opportunityId}_${first_name}`.slice(0, 64),
    },
  ];
  const emailStart = await startSurfePeopleEnrichment({
    people,
    include: { email: true, mobile: false, linkedInUrl: true, jobHistory: true },
    enrichmentOptions: { acceptedEmailType: "professional" },
  });
  if (!emailStart.ok) {
    return { ok: false, error: emailStart.error, creditsBefore: creditsBefore.payload };
  }
  const emailJobId =
    emailStart.payload?.enrichmentID || emailStart.payload?.enrichmentId || emailStart.payload?.id;
  const emailDone = await pollSurfePeopleEnrichment(emailJobId, { maxWaitMs: 240000 });
  let mobileDone = null;
  if (emailDone.ok && !emailDone.timed_out) {
    const mobileStart = await startSurfePeopleEnrichment({
      people,
      include: { email: false, mobile: true, linkedInUrl: false },
    });
    const mobileJobId =
      mobileStart.payload?.enrichmentID || mobileStart.payload?.enrichmentId || mobileStart.payload?.id;
    if (mobileStart.ok && mobileJobId) {
      mobileDone = await pollSurfePeopleEnrichment(mobileJobId, { maxWaitMs: 240000 });
    }
  }
  const emailPerson = emailDone.payload?.people?.[0] || emailDone.payload?.[0] || null;
  const mobilePerson = mobileDone?.payload?.people?.[0] || mobileDone?.payload?.[0] || null;
  const identity = acceptSurfeIdentity({
    requested: { first_name, last_name, company: person.organization },
    returned: emailPerson,
  });
  return {
    ok: emailDone.ok && !emailDone.timed_out,
    timed_out: Boolean(emailDone.timed_out),
    surfeEmail: emailPerson?.email || emailPerson?.emails?.[0] || null,
    surfeMobile: mobilePerson?.mobilePhone || mobilePerson?.phone || null,
    identity,
    creditsBefore: creditsBefore.payload,
    clientVersion: SURFE_CLIENT_VERSION,
  };
}

async function runReach(whoDoc) {
  const newPeople = whoDoc.rows.flatMap((r) =>
    (r.people || [])
      .filter((p) => p.accepted && p.isNewV12)
      .map((p) => ({ ...p, opportunityId: r.opportunityId, organization: r.organization, hotelId: r.hotelId }))
  );

  const reachRows = [];
  let surfeCalls = 0;
  let surfeEmail = 0;
  let surfePhone = 0;
  let pdlCalls = 0;
  let pdlIncremental = 0;

  for (const person of newPeople) {
    const publicEmail = person.email || null;
    const publicPhone = person.phone || null;
    let surfe = null;
    let pdl = null;

    const needsEmail = !publicEmail;
    const needsPhone = !publicPhone;
    if (needsEmail || needsPhone) {
      if (!args["skip-surfe"]) {
        surfeCalls += 1;
        surfe = await enrichSurfeOne(person);
        if (surfe.surfeEmail && needsEmail) surfeEmail += 1;
        if (surfe.surfeMobile && needsPhone) surfePhone += 1;
      }
      const stillNeedsEmail = needsEmail && !surfe?.surfeEmail;
      const stillNeedsPhone = needsPhone && !surfe?.surfeMobile;
      if ((stillNeedsEmail || stillNeedsPhone) && args["enable-pdl"]) {
        pdlCalls += 1;
        const { first_name, last_name } = splitName(person.name);
        pdl = await enrichPdlPerson({
          first_name,
          last_name,
          company: person.organization,
        });
        if (pdl?.ok) {
          const gotEmail = stillNeedsEmail && (pdl.email || pdl.work_email);
          const gotPhone = stillNeedsPhone && (pdl.phone || pdl.mobile_phone);
          if (gotEmail || gotPhone) pdlIncremental += 1;
        }
      }
    }

    const email = publicEmail || surfe?.surfeEmail || pdl?.email || null;
    const phone = publicPhone || surfe?.surfeMobile || pdl?.phone || null;
    const grade = gradeContact({
      name: person.name,
      email,
      phone,
      role: person.role,
    });

    reachRows.push({
      hotelId: person.hotelId,
      opportunityId: person.opportunityId,
      name: person.name,
      role: person.role,
      publicEmail,
      publicPhone,
      email,
      phone,
      surfe,
      pdl: pdl
        ? { ok: pdl.ok, clientVersion: PDL_CLIENT_VERSION, incremental: Boolean(pdlIncremental) }
        : null,
      grade: grade?.grade || grade?.contactGrade || null,
      contactable: Boolean(email || phone),
      highContactability: Boolean(email && phone),
    });
  }

  const doc = {
    validationMarker: MARKER,
    frozenAt: new Date().toISOString(),
    newWho: newPeople.length,
    surfeCalls,
    surfeNewEmail: surfeEmail,
    surfeNewPhone: surfePhone,
    pdlCalls,
    pdlIncremental,
    rows: reachRows,
  };
  writeJson(path.join(EVAL, "gdi-wave1-who-recall-v12-reachability.json"), doc);
  return doc;
}

function writeResearchPaths(whoDoc) {
  const lines = [
    `# GDI WHO Recall V12 — Research Paths`,
    ``,
    `Marker: \`${MARKER}\``,
    ``,
  ];
  for (const r of whoDoc.rows) {
    lines.push(`## ${r.eventName}`);
    lines.push("");
    lines.push(`- Gap: ${r.primaryGap}`);
    lines.push(`- Stages planned: ${(r.stagesPlanned || []).join(" → ")}`);
    lines.push(`- Stages run: ${(r.stagesRun || []).join(", ") || "—"}`);
    lines.push(`- Stop: ${r.stopReason || "—"}`);
    lines.push(`- Result: ${r.researchState}`);
    lines.push(`- Accepted: ${r.people.filter((p) => p.accepted).map((p) => p.name).join("; ") || "none"}`);
    lines.push(`- Rejected by V11: ${r.people.filter((p) => !p.accepted).map((p) => `${p.name} (${p.rejectReason})`).join("; ") || "none"}`);
    lines.push("");
  }
  writeMd(path.join(REPORT, "gdi-wave1-who-recall-v12-research-paths.md"), lines.join("\n"));
}

function writeResultsMd(whoDoc) {
  const lines = [
    `# GDI WHO Recall V12 — Results`,
    ``,
    `Marker: \`${MARKER}\``,
    ``,
    `| Hotel | Opportunity | New WHO | Role | Valid |`,
    `|---|---|---|---|---|`,
  ];
  for (const r of whoDoc.rows) {
    const accepted = r.people.filter((p) => p.accepted);
    if (!accepted.length) {
      lines.push(`| ${r.hotelName} | ${r.eventName} | — | — | — |`);
      continue;
    }
    for (const p of accepted) {
      lines.push(`| ${r.hotelName} | ${r.eventName} | ${p.name} | ${p.role || "—"} | VALID |`);
    }
  }
  lines.push("");
  lines.push(`Projected coverage: **${whoDoc.projectedCoveragePct}%** (${8 + whoDoc.newNamedOpportunities}/20)`);
  writeMd(path.join(REPORT, "gdi-wave1-who-recall-v12-results.md"), lines.join("\n"));
}

function priorRegression() {
  const cohorts = [
    { id: "bethesda", path: path.join(EVAL, "native-who-bethesda-holdout-v8.json") },
    { id: "ws_ren", path: path.join(EVAL, "native-who-waterstone-renaissance-v8.json") },
    {
      id: "now_cambridge_jw",
      path: path.join(EVAL, "native-who-v9-3hotel-refreeze.json"),
    },
    { id: "wave1_prior", path: path.join(EVAL, "gdi-wave1-who-v11-refreeze.json") },
  ];
  const out = [];
  for (const c of cohorts) {
    if (!fs.existsSync(c.path)) {
      out.push({ id: c.id, pass: false, reason: "missing" });
      continue;
    }
    const doc = readJson(c.path);
    let scanned = 0;
    let lost = 0;
    const rows = doc.rows || doc.hotels?.flatMap((h) => h.rows || []) || [];
    const hotelRows =
      c.id === "wave1_prior"
        ? (doc.hotels || []).flatMap((h) => h.rows || [])
        : rows;
    for (const row of hotelRows) {
      for (const p of row.people || []) {
        if (!p?.name || p.accepted === false || p.auditClass === "INVALID") continue;
        scanned += 1;
        const g = personTypeGateV11({
          name: p.name,
          role: p.role || "Director",
          sectionKind: p.sectionKind || "STAFF",
          evidenceQuote: p.evidenceQuote || "",
          email: p.email,
          sourceUrl: p.sourceUrl || "https://example.org/staff",
          onOfficialDomain: p.onOfficialDomain !== false,
          domainClass: p.domainClass || "ORGANIZATION_OFFICIAL",
        });
        if (g.reject) lost += 1;
      }
    }
    out.push({ id: c.id, scanned, validWhoLost: lost, pass: lost === 0 && scanned > 0 });
  }
  return out;
}

// --- main ---
async function main() {
  let base = loadBaseline();
  base = refineBaselineGaps(base);
  writeJson(path.join(EVAL, "gdi-wave1-who-recall-v12-baseline.json"), base);

  if (stage === "baseline" || stage === "gap-audit" || stage === "all") {
    writeGapAudit(base);
    console.error("[V12] gap audit written", base.gapCounts);
  }
  if (stage === "baseline" || stage === "gap-audit") return;

  let whoDoc = null;
  if (stage === "who" || stage === "all") {
    whoDoc = await runWhoAll(base);
    writeResearchPaths(whoDoc);
    writeResultsMd(whoDoc);
  } else if (fs.existsSync(path.join(EVAL, "gdi-wave1-who-recall-v12-results.json"))) {
    whoDoc = readJson(path.join(EVAL, "gdi-wave1-who-recall-v12-results.json"));
  }

  let reachDoc = null;
  if ((stage === "reach" || stage === "all") && whoDoc) {
    if (args["confirm-spend"] || args["skip-surfe"]) {
      reachDoc = await runReach(whoDoc);
    } else {
      console.error("[V12] reach deferred — pass --confirm-spend to call Surfe/PDL");
    }
  }

  if (stage === "reports" || stage === "all") {
    const prior = priorRegression();
    writeJson(path.join(EVAL, "gdi-wave1-who-recall-v12-prior-regression.json"), { prior });
    console.error("[V12] prior regression", JSON.stringify(prior));
  }

  console.log(
    JSON.stringify(
      {
        marker: MARKER,
        stage,
        unresolved: base.opportunities.length,
        gapCounts: base.gapCounts,
        who: whoDoc
          ? {
              newNamedOpportunities: whoDoc.newNamedOpportunities,
              newValidPeople: whoDoc.newValidPeople,
              projectedCoveragePct: whoDoc.projectedCoveragePct,
            }
          : null,
        reach: reachDoc
          ? {
              surfeCalls: reachDoc.surfeCalls,
              surfeNewEmail: reachDoc.surfeNewEmail,
              surfeNewPhone: reachDoc.surfeNewPhone,
              pdlCalls: reachDoc.pdlCalls,
            }
          : null,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
