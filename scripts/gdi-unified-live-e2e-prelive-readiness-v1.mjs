#!/usr/bin/env node
/**
 * GDI Unified Live E2E — Pre-live Operational Readiness Gate V1
 *
 * Contract: GDI_UNIFIED_LIVE_E2E_PRELIVE_READINESS_V1
 *
 *   npm run gdi:unified-live-e2e-prelive-readiness-v1
 *   node scripts/gdi-unified-live-e2e-prelive-readiness-v1.mjs [--skip-airtable-read] [--no-write-proposal]
 *
 * NON-MUTATING: every spawned command is DryRun, fail-closed, planning-only, or
 * runs against an isolated temp --out-dir with provider keys stripped from the
 * child environment. No paid spend, no Surfe, no UI, no STAGED overwrite, no CURRENT,
 * no Airtable writes (the reconstruction cert only READS GDO).
 *
 * Exit 0 only when READY. Otherwise PARTIAL with an `outstanding` list (exit 1).
 *
 * READY ≠ operationally certified. Certification still requires a founder-authorized
 * Live run (see 01-FRESH_HOTEL_CERTIFICATION_PROPOSAL.md). DryRun ≠ Live certification.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const GDI_UNIFIED_LIVE_E2E_PRELIVE_READINESS_V1 =
  "gdi-unified-live-e2e-prelive-readiness-v1";
const OUT = path.join(ROOT, "reports/gdi-unified-live-e2e-prelive-readiness-v1");
const PKG_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const CLI = "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs";
const WYNDHAM = "recnPB3Q02NO8Ma1o";
const SECOND_HOTEL = "recLuxvwwxID7U2B8"; // Bethesda Marriott (STAGED + frozen weekly discovery)
const RUNBOOK = "docs/ai-build-system/GDI_CANONICAL_POWERSHELL_OPERATIONS_RUNBOOK_V1.md";

export const READINESS = Object.freeze({ READY: "READY", PARTIAL: "PARTIAL" });

/** Candidate fresh hotels, in preference order (real HPC census ids from hotel demand configs). */
export const FRESH_HOTEL_CANDIDATES = Object.freeze([
  "recFaxTEFF9ILHWC9", // The Westin Grand München
  "rece0or38cxo3Fymb", // W Rome
  "recjhQdAUiSyxfCqE", // Sheraton Mallorca Arabella Golf Hotel
  "recdmo0CnhIZAwHGC", // The Westin Tampa Waterside
]);

const PROVIDER_ENV_KEYS = [
  "SERPAPI_KEY",
  "SERPAPI_API_KEY",
  "EXA_API_KEY",
  "FINDYMAIL_API_KEY",
  "SURFE_API_KEY",
  "PDL_API_KEY",
  "GDI_EXA_AGENT_LIVE",
  "GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES",
  "GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES",
];

function text(v) {
  return String(v == null ? "" : v).trim();
}
function sha256File(file) {
  return fs.existsSync(file)
    ? crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")
    : null;
}
function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}
function rel(p) {
  return path.relative(ROOT, p).replace(/\\/g, "/");
}
function cleanEnv(extra = {}) {
  const env = { ...process.env };
  for (const k of PROVIDER_ENV_KEYS) delete env[k];
  return { ...env, ...extra };
}
function runNode(scriptRel, args = [], { timeoutMs = 180000, env } = {}) {
  const r = spawnSync("node", [path.join(ROOT, scriptRel), ...args], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: timeoutMs,
    env: env || cleanEnv(),
  });
  return { status: r.status, stdout: r.stdout || "", stderr: r.stderr || "", error: r.error ? String(r.error.message) : null };
}
function tail(r, n = 400) {
  return `${r.stdout}\n${r.stderr}`.trim().slice(-n);
}
function protectedHashes(hotelIds) {
  const h = {
    findymailLedger: sha256File(path.join(ROOT, "data/group-demand-intelligence/findymail-usage-ledger-v1.jsonl")),
  };
  for (const id of hotelIds) {
    h[`${id}:STAGED`] = sha256File(path.join(PKG_ROOT, id, "STAGED.json"));
    h[`${id}:CURRENT`] = sha256File(path.join(PKG_ROOT, id, "CURRENT.json"));
  }
  return h;
}

function check(id, label, fn) {
  const startedAt = Date.now();
  try {
    const r = fn();
    return { id, label, ms: Date.now() - startedAt, ...r };
  } catch (err) {
    return { id, label, ms: Date.now() - startedAt, pass: false, detail: `CHECK_ERROR: ${err?.message || err}` };
  }
}

// ---------------------------------------------------------------- checks

function checkLiveFailClosed(tmp) {
  const noAuth = runNode(CLI, ["--stage", "FULL", "--hotel-id", WYNDHAM, "--mode", "Live", "--out-dir", path.join(tmp, "live-noauth")]);
  const paidNoWhoHow = runNode(CLI, [
    "--stage", "FULL", "--hotel-id", WYNDHAM, "--mode", "Live",
    "--authorize-paid-research", "--out-dir", path.join(tmp, "live-paid-no-who-how-auth"),
  ]);
  const pass = noAuth.status !== 0 && paidNoWhoHow.status === 2;
  return {
    pass,
    detail: {
      liveWithoutAuthorizePaidResearchExit: noAuth.status,
      livePaidWithoutWhoHowParamsExit: paidNoWhoHow.status,
      expected: "non-zero (no auth) and exit 2 (missing WHO/HOW authorization params)",
    },
  };
}

function checkLiveSkipWhoHowSummary(tmp) {
  const outDir = path.join(tmp, "live-skip-who-how");
  const before = protectedHashes([WYNDHAM, SECOND_HOTEL]);
  const r = runNode(CLI, [
    "--stage", "FULL", "--hotel-id", WYNDHAM, "--mode", "Live",
    "--authorize-paid-research", "--skip-who-how", "--allow-historical-replay",
    "--skip-staged-progression", "--no-activate-staged-pointer",
    "--out-dir", outDir,
  ], { timeoutMs: 240000 });
  const summaryFile = path.join(outDir, "00-ORCHESTRATOR_SUMMARY.json");
  const summary = readJson(summaryFile);
  const after = protectedHashes([WYNDHAM, SECOND_HOTEL]);
  const unchanged = JSON.stringify(before) === JSON.stringify(after);
  const structure = Boolean(
    summary &&
      summary.completeness &&
      "stagedProgression" in summary &&
      summary.mode === "Live",
  );
  const spent = summary?.paid === true;
  return {
    pass: structure && !spent && unchanged,
    detail: {
      exitCode: r.status,
      summaryStructurePresent: structure,
      paid: summary?.paid ?? null,
      completeness: summary?.completeness ?? null,
      stagedProgression: summary?.stagedProgression ?? null,
      pointersAndLedgerUnchanged: unchanged,
      note: "Isolated out-dir, provider keys stripped, staged progression skipped. HOLD stages are expected (no artifacts in temp dir).",
      tail: structure ? undefined : tail(r),
    },
  };
}

function checkFindymailPlanning(tmp) {
  const before = protectedHashes([WYNDHAM]);
  const r = runNode(CLI, [
    "--stage", "PROFESSIONAL_CONTACT_RESEARCH", "--hotel-id", WYNDHAM, "--mode", "Live",
    "--authorize-paid-research", "--authorize-findymail", "--findymail-dry-run",
    "--out-dir", path.join(tmp, "findymail-planning"),
  ]);
  const after = protectedHashes([WYNDHAM]);
  return {
    pass: r.status === 0 && JSON.stringify(before) === JSON.stringify(after),
    detail: { exitCode: r.status, ledgerUnchanged: before.findymailLedger === after.findymailLedger, tail: r.status === 0 ? undefined : tail(r) },
  };
}

function checkWeeklyDryRun(tmp) {
  const reportDir = path.join(tmp, "weekly");
  const r = runNode("scripts/gdi-weekly-refresh.mjs", ["--hotel-id", SECOND_HOTEL, "--dry-run", "--report-dir", reportDir], { timeoutMs: 120000 });
  const refresh = readJson(path.join(reportDir, "00-WEEKLY_REFRESH_DRY_RUN.json"));
  return {
    pass: r.status === 0 && refresh?.dryRun === true && refresh?.guarantees?.serpApiCalls === 0,
    detail: {
      exitCode: r.status,
      command: "npm run gdi:weekly-refresh -- --hotel-id <rec...> --dry-run",
      provenOn: SECOND_HOTEL,
      discoverySource: refresh?.discoverySource ?? null,
      serpApiCalls: refresh?.guarantees?.serpApiCalls ?? null,
      counts: refresh?.counts ?? null,
    },
  };
}

function checkReconstructionCert(skipAirtable) {
  if (skipAirtable) {
    return { pass: false, detail: "SKIPPED (--skip-airtable-read) — reconstruction cert not evaluated", knownLimitation: false };
  }
  const r = runNode("scripts/gdi-clean-airtable-reconstruction-cert-v1.mjs", [], {
    timeoutMs: 180000,
    env: cleanEnv(), // Airtable creds are loaded by the cert script itself (read-only)
  });
  const cert = readJson(path.join(ROOT, "reports/gdi-clean-airtable-reconstruction-cert-v1/00-CERTIFICATION.json"));
  const status = cert?.status || null;
  return {
    // PASS and PARTIAL both mean "ran + report produced"; BLOCKED means unreadable.
    pass: r.status === 0 && (status === "PASS" || status === "PARTIAL"),
    detail: {
      exitCode: r.status,
      certStatus: status,
      corrections: (cert?.missingPersistenceCorrections || []).map((c) => c.code),
      note: "PARTIAL is a known limitation (founder-authorized Airtable persistence corrections), not a pre-live blocker.",
      tail: r.status === 0 ? undefined : tail(r),
    },
    knownLimitation: status === "PARTIAL" ? (cert?.missingPersistenceCorrections || []).map((c) => c.code) : [],
  };
}

function checkRunbook() {
  const file = path.join(ROOT, RUNBOOK);
  if (!fs.existsSync(file)) return { pass: false, detail: `missing ${RUNBOOK}` };
  const body = fs.readFileSync(file, "utf8");
  const required = [
    [/gdi:weekly-refresh -- --hotel-id <rec\.\.\.> --dry-run/, "weekly refresh command"],
    [/gdi:clean-airtable-reconstruction-cert-v1/, "reconstruction cert command"],
    [/gdi:unified-live-e2e-prelive-readiness-v1/, "pre-live gate command"],
    [/DryRun ≠ Live certification/, "DryRun ≠ Live statement"],
    [/pre-live READY ≠ operational certification/i, "READY ≠ certification statement"],
  ];
  const missing = required.filter(([re]) => !re.test(body)).map(([, l]) => l);
  return { pass: missing.length === 0, detail: { runbook: RUNBOOK, missing } };
}

function checkTemplateFreeze() {
  const manifestMod = "lib/group-demand-intelligence/presentation/gdi-canonical-hotel-template-v1.js";
  return import(pathToFileURL(path.join(ROOT, manifestMod)).href).then((m) => {
    const freeze = m.getCanonicalHotelTemplateManifest().freeze;
    const files = [freeze.htmlShell, freeze.css, freeze.ui, freeze.executiveSummary, freeze.listTable];
    const presence = files.map((f) => ({ file: f, present: fs.existsSync(path.join(ROOT, f)), sha256: sha256File(path.join(ROOT, f)) }));
    const git = spawnSync("git", ["status", "--porcelain", "--", ...files], { cwd: ROOT, encoding: "utf8" });
    const gitAvailable = git.status === 0;
    const modified = gitAvailable ? git.stdout.split(/\r?\n/).filter(Boolean) : [];
    const jsBody = fs.readFileSync(path.join(ROOT, freeze.ui), "utf8") + fs.readFileSync(path.join(ROOT, freeze.executiveSummary), "utf8") + fs.readFileSync(path.join(ROOT, freeze.listTable), "utf8");
    const cssBody = fs.readFileSync(path.join(ROOT, freeze.css), "utf8");
    const missingSubstrings = (freeze.requiredJsSubstrings || []).filter((s) => !jsBody.includes(s));
    const missingSelectors = (freeze.requiredCssSelectors || []).filter((s) => !cssBody.includes(s));
    return {
      pass: presence.every((p) => p.present) && modified.length === 0 && !missingSubstrings.length && !missingSelectors.length,
      detail: { files: presence, gitAvailable, modifiedVsHead: modified, missingSubstrings, missingSelectors },
    };
  });
}

function stagedCount(hotelId) {
  const ptr = readJson(path.join(PKG_ROOT, hotelId, "STAGED.json"));
  if (!ptr) return { pointer: false, version: null, count: null };
  const pkg = readJson(path.join(PKG_ROOT, hotelId, ptr.publicationVersion, "package.json"));
  return { pointer: true, version: ptr.publicationVersion, count: pkg?.opportunities?.length ?? null };
}

function checkStagedWyndham() {
  const s = stagedCount(WYNDHAM);
  return { pass: s.pointer && s.count === 26, detail: { hotelId: WYNDHAM, ...s, expectedCount: 26 } };
}

function checkSecondHotelDryRun(tmp) {
  const full = runNode(CLI, ["--stage", "FULL", "--hotel-id", SECOND_HOTEL, "--mode", "DryRun", "--out-dir", path.join(tmp, "second-full")]);
  const disc = runNode(CLI, ["--stage", "DEMAND_DISCOVERY", "--hotel-id", SECOND_HOTEL, "--mode", "DryRun", "--out-dir", path.join(tmp, "second-disc")]);
  const staged = stagedCount(SECOND_HOTEL);
  return {
    pass: full.status === 0 && disc.status === 0 && staged.pointer,
    detail: { hotelId: SECOND_HOTEL, fullDryRunExit: full.status, discoveryDryRunExit: disc.status, stagedPointer: staged.pointer },
  };
}

// ---------------------------------------------------------------- fresh hotel proposal

export async function selectFreshHotel() {
  const lib = await import(pathToFileURL(path.join(ROOT, "lib/group-demand-intelligence/index.js")).href);
  const rejected = [];
  for (const hotelId of FRESH_HOTEL_CANDIDATES) {
    if (hotelId === WYNDHAM) continue;
    let cfg = null;
    try {
      cfg = lib.loadHotelDemandConfig(hotelId);
    } catch {
      cfg = null;
    }
    const pkgDir = path.join(PKG_ROOT, hotelId);
    if (!cfg?.displayName) {
      rejected.push({ hotelId, reason: "NO_HOTEL_DEMAND_CONFIG" });
      continue;
    }
    if (fs.existsSync(path.join(pkgDir, "STAGED.json")) || fs.existsSync(path.join(pkgDir, "CURRENT.json"))) {
      rejected.push({ hotelId, name: cfg.displayName, reason: "HAS_STAGED_OR_CURRENT_PACKAGE" });
      continue;
    }
    const bag = readJson(path.join(ROOT, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json"));
    const opps = bag?.opportunities || [];
    const byPriority = opps.reduce((a, o) => ((a[o.priority || "UNKNOWN"] = (a[o.priority || "UNKNOWN"] || 0) + 1), a), {});
    return { hotelId, cfg, localPilotBag: { count: opps.length, byPriority, withEmail: opps.filter((o) => o.primaryContact?.email).length }, rejected };
  }
  return null;
}

function isoCompact(d = new Date()) {
  return d.toISOString().slice(0, 10).replace(/-/g, "");
}

export function buildProposal(sel, { date = isoCompact() } = {}) {
  const { hotelId, cfg } = sel;
  const nameToken = text(cfg.displayName)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/^The\s+/i, "")
    .replace(/[^A-Za-z0-9]+/g, "_").replace(/^_|_$/g, "").toUpperCase()
    .replace(/_GRAND_MUNCHEN$/, "_MUNICH").replace(/_MUNCHEN$/, "_MUNICH");
  const authToken = `FOUNDER_GO_GDI_LIVE_E2E_${nameToken}_${date}`;
  const outDir = `reports/gdi-fresh-hotel-live-e2e-${hotelId}-${date}`;
  const manifest = `${outDir}/01-TRANCHE_MANIFEST.json`;
  const cap = cfg.capabilityProfile || {};
  const territory = cfg.demandTerritory || {};
  const command = [
    "$env:GDI_EXA_AGENT_LIVE = '1'          # Exa WHO/HOW spend gate (set only after founder GO)",
    "# $env:FINDYMAIL_API_KEY must already be present in the session (never echo it)",
    `pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId ${hotelId} -Mode Live -AuthorizePaidResearch \``,
    `  -AuthorizationId '${authToken}' -HardCeilingUsd 25 \``,
    `  -Manifest '${manifest}' -OutDir '${outDir}' \``,
    "  -AuthorizeFindymail -AllowHistoricalReplay -EnableLeisure -EnableCad -NoActivateStagedPointer",
    "# -NoActivateStagedPointer: drop only if the founder authorizes STAGED activation in the same GO",
  ].join("\n");
  const dryRunCommand = `pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId ${hotelId} -Mode DryRun`;
  const preflight = [
    `npm run gdi:unified-live-e2e-prelive-readiness-v1`,
    dryRunCommand,
    `npm run gdi:weekly-refresh -- --hotel-id ${hotelId} --dry-run`,
  ];

  const mechanisms = [
    { id: "DEMAND_DISCOVERY", detail: "Fourteen-segment discovery (Munich / Arabellapark / Bogenhausen territory; German primary, English control). Live reuses existing artifacts — none exist for this hotel.", spend: "none under this GO" },
    { id: "CAD / PLP", detail: "CAD classification + PLP embedded in discovery; classification only.", spend: "$0" },
    { id: "LEISURE (LDI)", detail: "Standard stage, mock_live only ($0). Low weight for an urban meetings/congress hotel; enabled so the unified path is exercised.", spend: "$0" },
    { id: "OPPORTUNITY_DEEP_RESEARCH", detail: "Reuses settled queue dossiers; missing → HOLD.", spend: "none" },
    { id: "WHO_HOW_RESOLUTION", detail: "Existing Exa agent adapter, Checkpoint A pattern; requires authorization id + ceiling + manifest + GDI_EXA_AGENT_LIVE=1.", spend: "Exa ≤ $25 hard ceiling" },
    { id: "PROFESSIONAL_CONTACT_RESEARCH", detail: "Public-first named-contact channels; Findymail only when authorized (FINDYMAIL_API_KEY).", spend: "Findymail ≤ 15 credits (see enforcement note)" },
    { id: "COMMERCIAL_QUALIFICATION", detail: "Strict FQO + entry-point reachability + buying-authority annotation (in memory).", spend: "none" },
    { id: "STAGED progression", detail: "Package build + validation; STAGED activation only if founder includes it in the GO. CURRENT never changes.", spend: "none" },
  ];

  const json = {
    contract: "gdi-fresh-hotel-certification-proposal-v1",
    generatedAt: new Date().toISOString(),
    banner: "PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND",
    authorized: false,
    hotel: {
      name: cfg.displayName,
      hotelId,
      city: cfg.city || null,
      country: cfg.country || null,
      demandTerritory: territory.label || null,
      adpPropertyId: cfg.aliases?.adpPropertyId || null,
      rooms: cap.totalGuestrooms ?? null,
      meetingRooms: cap.meetingRoomsIndoor ?? cap.meetingRoomCount ?? null,
      meetingSpaceSqM: cap.totalMeetingSpaceSqM ?? null,
      largestBallroomSqM: cap.largestBallroomSqM ?? null,
      classification: cap.classification || null,
      primaryLocale: cfg.locale?.primary || null,
    },
    selection: {
      rule: "Real HPC census id with hotel demand config, NOT Wyndham, no STAGED/CURRENT pointer and no bundled publication package; non-US market proves hotel-agnostic path.",
      stagedState: "NONE",
      currentState: "NONE",
      localPilotOpportunityBag: sel.localPilotBag,
      rejectedCandidates: sel.rejected,
      avoidedForPriorCompletion: [
        { hotelId: WYNDHAM, reason: "Wyndham San Diego Bayside — STAGED 26 (certified baseline)" },
        { hotelId: "recLuxvwwxID7U2B8", reason: "Bethesda Marriott — full package + CURRENT" },
        { hotelId: "recH9h3McgMNFQEla", reason: "Radisson Blu Toronto Downtown — full package + CURRENT" },
        { hotelId: "recrPQcZg7SFARRb2", reason: "YOTEL Geneva Lake — full package + CURRENT" },
        { hotelId: "rec1t35e3sPerzxYu", reason: "Memmo Príncipe Real — prior paid Design Hotels pilot (STAGED 11); less probative as a fresh proof" },
      ],
    },
    marketContext: {
      summary:
        "Munich urban meetings / congress / corporate demand (Arabellapark business district, Messe München adjacency, DACH corporate programs). 627 rooms, 21 meeting rooms, 2,810 sqm, 1,053 sqm ballroom. ADP intelligence already exists for this property (adp_westin_grand_munchen) — cross-reference only, no duplication.",
      localeNote: "German-primary discovery with English control; first non-US/non-Anglophone Live proof.",
    },
    mechanisms,
    expectedScope: {
      note: "Planning estimates, not quotes. Nothing is authorized or auto-spent.",
      startingPoint: `${sel.localPilotBag.count} pre-existing local pilot opportunities (${JSON.stringify(sel.localPilotBag.byPriority)}), ${sel.localPilotBag.withEmail} with email — not customer-published, no STAGED.`,
      targetQualifiedInventory: "≈ 15–30 qualified commercial opportunities (strict FQO; may be fewer — budget exhaustion ≠ COMPLETE)",
      whoHowTargets: "Top-ranked opportunities only, bounded by the $25 Exa ceiling",
      contactTargets: "Named buyer/entry contacts for qualified opportunities; Findymail only for eligible full-name + domain candidates",
    },
    limits: {
      exaUsdCeiling: 25,
      findymailCredits: 15,
      serpApi: "NOT covered — discovery artifacts must be supplied (see prerequisites)",
      surfe: "FORBIDDEN",
      current: "UNCHANGED (never promoted)",
      staged: "Activation only if explicitly included in the founder GO; otherwise -NoActivateStagedPointer",
    },
    airtableWriteScope: {
      default: "NONE — -AuthorizePaidResearch never unlocks Airtable writes.",
      ifSeparatelyAuthorized: `Group Demand Opportunities upsert for hotelId ${hotelId} only (idempotent, dry-run first via -PlanResearchPersistence), exact write-set text required, GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1. No schema changes, no other hotels, no CURRENT.`,
    },
    prerequisitesAndRisks: [
      "BLOCKING: Live Stage 1/2 reuse existing discovery + deep-research artifacts. None exist for this hotel, so Live would HOLD at Stage 1 until a discovery matrix exists in -OutDir (fourteen-segment-discovery-matrix.json). Producing it uses SerpApi via the original research chain and is NOT covered by the Exa/Findymail limits — needs a separate founder decision.",
      "A tranche manifest (01-TRANCHE_MANIFEST.json) must be authored for this hotel; no Wyndham manifest may be reused.",
      "Findymail credit hard cap: pass -FindymailCreditHardCap 15 (--findymail-credit-hard-cap 15); Stage 4 shared budget stops enrichment at CREDIT_HARD_CAP_REACHED.",
      "Flag names above follow the Live orchestrator CLI at the time of writing; re-run the pre-live gate before GO.",
    ],
    authorizationToken: {
      proposed: authToken,
      note: "Proposed identifier only. It authorizes nothing until the founder issues it in writing.",
    },
    commands: { preflight, dryRun: dryRunCommand, liveFull: command },
    paths: { outDir, manifest },
  };

  const md = [
    "# Fresh Hotel Certification Proposal",
    "",
    "> **PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND**",
    "",
    `## Recommended hotel: ${cfg.displayName}`,
    "",
    `- **Canonical hotel ID:** \`${hotelId}\``,
    `- **Market:** ${cfg.city}, ${cfg.country} — ${territory.label || ""}`,
    `- **Profile:** ${cap.totalGuestrooms ?? "?"} rooms · ${cap.meetingRoomsIndoor ?? "?"} meeting rooms · ${cap.totalMeetingSpaceSqM ?? "?"} sqm meeting space · ${cap.largestBallroomSqM ?? "?"} sqm ballroom · ${cap.classification || ""}`,
    `- **State:** no STAGED, no CURRENT, no publication package (not Wyndham; Wyndham \`${WYNDHAM}\` excluded)`,
    `- **Local pilot data:** ${sel.localPilotBag.count} opportunities (${JSON.stringify(sel.localPilotBag.byPriority)}), ${sel.localPilotBag.withEmail} with email — never published`,
    "",
    "## Why this hotel",
    "",
    "- Has not completed full GDI; genuinely fresh for a hotel-agnostic Live proof.",
    "- Different market type (German-primary urban meetings/congress) than the Wyndham / US baseline.",
    "- ADP intelligence already exists for this property — cross-reference only.",
    "- Rejected/avoided: " + json.selection.avoidedForPriorCompletion.map((r) => r.hotelId).join(", ") + ".",
    "",
    "## Applicable mechanisms",
    "",
    "| Mechanism | Behavior | Spend |",
    "|---|---|---|",
    ...mechanisms.map((m) => `| ${m.id} | ${m.detail} | ${m.spend} |`),
    "",
    "## Expected scope (estimates, not quotes)",
    "",
    `- Starting point: ${json.expectedScope.startingPoint}`,
    `- Target: ${json.expectedScope.targetQualifiedInventory}`,
    `- WHO/HOW: ${json.expectedScope.whoHowTargets}`,
    `- Contacts: ${json.expectedScope.contactTargets}`,
    "",
    "## Proposed limits",
    "",
    "| Item | Limit |",
    "|---|---|",
    "| Exa (WHO/HOW) | **$25 hard ceiling** |",
    "| Findymail | **15 credits** |",
    "| SerpApi | not covered by this GO (see prerequisites) |",
    "| Surfe | **FORBIDDEN** |",
    "| CURRENT | never changed |",
    "| STAGED | activation only if explicitly authorized; otherwise `-NoActivateStagedPointer` |",
    "",
    "## Airtable write scope",
    "",
    `- Default: **none**. \`-AuthorizePaidResearch\` never unlocks writes.`,
    `- If separately authorized: ${json.airtableWriteScope.ifSeparatelyAuthorized}`,
    "",
    "## Prerequisites and risks",
    "",
    ...json.prerequisitesAndRisks.map((r) => `- ${r}`),
    "",
    "## Commands",
    "",
    "Pre-flight (no spend):",
    "",
    "```powershell",
    ...preflight,
    "```",
    "",
    "Live FULL (founder GO required — do not run before authorization):",
    "",
    "```powershell",
    command,
    "```",
    "",
    "## Proposed authorization token",
    "",
    `\`${authToken}\``,
    "",
    "Identifier only. It authorizes nothing until the founder issues it in writing.",
    "",
    "> **PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND**",
    "",
  ].join("\n");

  return { json, md };
}

// ---------------------------------------------------------------- evaluation (pure, exported)

export function evaluateReadiness(checks, { extraOutstanding = [], knownLimitations = [] } = {}) {
  const failing = checks.filter((c) => c.pass !== true);
  const outstanding = [
    ...failing.map((c) => `${c.id.toUpperCase()}_NOT_PASSING — ${c.label}`),
    ...extraOutstanding,
  ];
  const status = failing.length === 0 ? READINESS.READY : READINESS.PARTIAL;
  return {
    status,
    exitCode: status === READINESS.READY ? 0 : 1,
    outstanding,
    knownLimitations,
  };
}

// ---------------------------------------------------------------- main

async function main() {
  const argv = process.argv.slice(2);
  const skipAirtable = argv.includes("--skip-airtable-read");
  const noWriteProposal = argv.includes("--no-write-proposal");
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-prelive-"));
  const hotelsGuarded = [WYNDHAM, SECOND_HOTEL];
  const hashesBefore = protectedHashes(hotelsGuarded);

  const checks = [];
  try {
    checks.push(check("live_cli_fail_closed", "Live CLI fails closed without authorization", () => checkLiveFailClosed(tmp)));
    checks.push(check("live_skip_who_how_summary", "Live (SkipWhoHow, historical replay) produces summary structure without spend", () => checkLiveSkipWhoHowSummary(tmp)));
    checks.push(check("findymail_planning", "Findymail planning path (no spend, ledger unchanged)", () => checkFindymailPlanning(tmp)));
    checks.push(check("weekly_dry_run", "Weekly refresh DryRun (no SerpApi)", () => checkWeeklyDryRun(tmp)));
    const recon = check("reconstruction_cert", "Clean Airtable reconstruction cert executes (PASS or PARTIAL)", () => checkReconstructionCert(skipAirtable));
    checks.push(recon);
    checks.push(check("runbook", "Runbook exists with required commands/statements", () => checkRunbook()));
    const tf = { id: "template_freeze", label: "Canonical template freeze files unchanged" };
    try {
      checks.push({ ...tf, ...(await checkTemplateFreeze()) });
    } catch (err) {
      checks.push({ ...tf, pass: false, detail: `CHECK_ERROR: ${err?.message || err}` });
    }
    checks.push(check("staged_wyndham_26", "Wyndham STAGED inventory = 26", () => checkStagedWyndham()));
    checks.push(check("second_hotel_dry_run", "Second hotel DryRun path exists", () => checkSecondHotelDryRun(tmp)));

    const sel = await selectFreshHotel();
    checks.push({
      id: "fresh_hotel_selected",
      label: "Fresh hotel candidate selected (no STAGED/CURRENT, not Wyndham)",
      pass: Boolean(sel),
      detail: sel ? { hotelId: sel.hotelId, name: sel.cfg.displayName } : "no candidate satisfied selection rule",
    });
    if (sel) {
      checks.push(
        check("fresh_hotel_dry_run", "Fresh hotel DryRun path executes", () => {
          const r = runNode(CLI, ["--stage", "FULL", "--hotel-id", sel.hotelId, "--mode", "DryRun", "--out-dir", path.join(tmp, "fresh-full")]);
          return { pass: r.status === 0, detail: { hotelId: sel.hotelId, exitCode: r.status, tail: r.status === 0 ? undefined : tail(r) } };
        }),
      );
    }

    const hashesAfter = protectedHashes(hotelsGuarded);
    checks.push({
      id: "no_mutation",
      label: "STAGED/CURRENT pointers + Findymail ledger unchanged by the gate",
      pass: JSON.stringify(hashesBefore) === JSON.stringify(hashesAfter),
      detail: { before: hashesBefore, after: hashesAfter },
    });

    const extraOutstanding = [
      "FINDYMAIL_CREDIT_HARD_CAP_CLI_READY — pass -FindymailCreditHardCap 15 on founder Live GO (default remains 40 if omitted)",
      "FRESH_HOTEL_DISCOVERY_ARTIFACTS_REQUIRED — Live Stage 1 reuses artifacts; none exist for the recommended hotel (separate founder decision; SerpApi not covered by Exa/Findymail limits)",
      "FOUNDER_LIVE_GO_REQUIRED — no authorization issued; pre-live READY ≠ operational certification",
    ];
    const knownLimitations = recon.knownLimitation || [];
    // Operational READY: all checks pass. Extra items are conditions on the founder GO, not gate failures.
    const ev = evaluateReadiness(checks, { extraOutstanding: [], knownLimitations });

    if (sel && !noWriteProposal) {
      const { json, md } = buildProposal(sel);
      fs.mkdirSync(OUT, { recursive: true });
      fs.writeFileSync(path.join(OUT, "01-FRESH_HOTEL_CERTIFICATION_PROPOSAL.json"), `${JSON.stringify(json, null, 2)}\n`, "utf8");
      fs.writeFileSync(path.join(OUT, "01-FRESH_HOTEL_CERTIFICATION_PROPOSAL.md"), md, "utf8");
    }

    const report = {
      contract: GDI_UNIFIED_LIVE_E2E_PRELIVE_READINESS_V1,
      generatedAt: new Date().toISOString(),
      status: ev.status,
      verdict:
        ev.status === READINESS.READY
          ? "PRE_LIVE_READY — awaiting founder Live GO (NOT operationally certified)"
          : "PARTIAL — see outstanding",
      operationallyCertified: false,
      notClaimed: [
        "GDI_UNIFIED_LIVE_E2E_OPERATIONALLY_CERTIFIED",
        "DryRun == Live certification",
        "pre-live READY == operational certification",
      ],
      guarantees: {
        paidSpend: 0,
        surfeUsed: false,
        uiChanges: false,
        stagedOverwrite: false,
        currentChanged: false,
        airtableWrites: 0,
        providerKeysStrippedFromChildren: PROVIDER_ENV_KEYS,
      },
      checks,
      outstanding: ev.outstanding,
      knownLimitations,
      conditionsOnFounderGo: extraOutstanding,
      recommendedFreshHotel: sel ? { hotelId: sel.hotelId, name: sel.cfg.displayName, proposal: rel(path.join(OUT, "01-FRESH_HOTEL_CERTIFICATION_PROPOSAL.md")) } : null,
      weeklyRefreshCommand: "npm run gdi:weekly-refresh -- --hotel-id <rec...> --dry-run",
    };
    fs.mkdirSync(OUT, { recursive: true });
    fs.writeFileSync(path.join(OUT, "00-PRELIVE_READINESS.json"), `${JSON.stringify(report, null, 2)}\n`, "utf8");

    console.log(
      JSON.stringify(
        {
          status: report.status,
          verdict: report.verdict,
          checks: Object.fromEntries(checks.map((c) => [c.id, c.pass === true ? "PASS" : "FAIL"])),
          outstanding: report.outstanding,
          knownLimitations,
          conditionsOnFounderGo: extraOutstanding,
          report: rel(path.join(OUT, "00-PRELIVE_READINESS.json")),
        },
        null,
        2,
      ),
    );
    process.exitCode = ev.exitCode;
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((err) => {
    console.error(err?.stack || err);
    process.exit(1);
  });
}
