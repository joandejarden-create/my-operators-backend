#!/usr/bin/env node
/**
 * GDI Clean Airtable Reconstruction Certification V1
 *
 * Contract: GDI_CLEAN_AIRTABLE_RECONSTRUCTION_CERT_V1
 *
 *   node scripts/gdi-clean-airtable-reconstruction-cert-v1.mjs [--hotel-id <rec…>]
 *        [--report-dir <dir>] [--staged-version <v>] [--airtable-fixture <json>]
 *
 * Source of the reconstruction  = Airtable Group Demand Opportunities
 *                                 (listOpportunitiesForHotel) — NEVER STAGED.
 * Comparison baseline           = the hotel's current STAGED package (read-only).
 * Reconstruction package        = isolated, non-publishing, under
 *                                 <report-dir>/_isolated/ (never inside a
 *                                 publication-package root).
 * In-memory enrichment          = Findymail-ledger / prior-artifact channel
 *                                 evidence recovery + buying-authority annotation.
 *
 * Guarantees: no Airtable writes, no provider calls, no spend, no Surfe, no UI,
 * STAGED/CURRENT pointers + package bytes are hashed before/after and must not change.
 *
 * Status vocabulary: PASS | PARTIAL | BLOCKED
 *   BLOCKED  — Airtable unreadable / STAGED baseline missing / nothing to compare.
 *   PARTIAL  — reconstruction ran; exact missing persistence corrections listed.
 *   PASS     — Airtable alone reproduces STAGED membership + contact/commercial/authority fields.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  buildHotelContactBestEffortLedger,
  evaluateContactBestEffortCertificationGate,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-best-effort-v1.js";
import { annotateHotelOpportunitySet } from "../lib/group-demand-intelligence/workflow/gdi-buying-authority-annotation-v1.js";
import { applyEvidenceRecoveryToOpportunities } from "../lib/group-demand-intelligence/workflow/gdi-contact-channel-evidence-recovery-v1.js";
import { readFindymailUsageLedger } from "../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const GDI_CLEAN_AIRTABLE_RECONSTRUCTION_CERT_V1 =
  "gdi-clean-airtable-reconstruction-cert-v1";
export const DEFAULT_HOTEL_ID = "recnPB3Q02NO8Ma1o"; // Wyndham San Diego Bayside
const DEFAULT_REPORT_DIR = path.join(
  ROOT,
  "reports/gdi-clean-airtable-reconstruction-cert-v1",
);
const PACKAGE_ROOT = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
);

export const CERT_STATUS = Object.freeze({
  PASS: "PASS",
  PARTIAL: "PARTIAL",
  BLOCKED: "BLOCKED",
});

/** Commercial / presentation fields compared per opportunity (value or presence). */
export const COMMERCIAL_FIELDS = Object.freeze([
  "title",
  "displayTitle",
  "organizationName",
  "priority",
  "opportunityType",
  "roomDemandStatus",
  "summaryWhat",
  "whyThisHotel",
  "recommendedAction",
  "recommendedNextStep",
  "eventDateDisplay",
  "eventStartDate",
  "eventEndDate",
  "potentialHotelBusiness",
  "salesActionKey",
  "sources",
]);

export const DURABLE_STORES = Object.freeze([
  {
    store: "Airtable — Group Demand Opportunities (GDO)",
    role: "SYSTEM_OF_RECORD for opportunity identity + embedded commercial / contact payload",
    readBy: "listOpportunitiesForHotel (lib/group-demand-intelligence/airtable-opportunity-store.js)",
    survivesRestart: true,
  },
  {
    store: "Airtable — GDI Hotel Research Opportunities (tblbkDj0vhQLfrssV)",
    role: "Raw research inventory (researchPayloadJson); separate from customer GDO",
    readBy: "gdi-hotel-research-inventory-store-v1",
    survivesRestart: true,
    note: "Not read by this cert; membership/drawer-model recovery depends on it plus the publication builder.",
  },
  {
    store: "Findymail usage ledger (data/group-demand-intelligence/findymail-usage-ledger-v1.jsonl)",
    role: "Provider spend + per-person disposition evidence (NOT_FOUND vs found) used for channel evidence recovery",
    readBy: "readFindymailUsageLedger → applyEvidenceRecoveryToOpportunities",
    survivesRestart: true,
    note: "Local repo file — must be tracked/backed-up; email values are only recoverable when persisted into GDO/package (piiPersisted).",
  },
  {
    store: "Immutable publication packages (bundled-publication-packages/<hotel>/<version>)",
    role: "OUTPUT of the publication builder (STAGED/CURRENT pointers). Never an input to reconstruction.",
    readBy: "createGdiPublicationPackageStore (comparison baseline only)",
    survivesRestart: true,
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function lc(v) {
  return text(v).toLowerCase();
}
function digits(v) {
  return text(v).replace(/\D+/g, "");
}

function sha256File(file) {
  return fs.existsSync(file)
    ? crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")
    : null;
}
function sha256Json(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}
function writeJson(dir, name, obj) {
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, name);
  fs.writeFileSync(file, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  return file;
}
function isInside(child, parent) {
  const rel = path.relative(path.resolve(parent), path.resolve(child));
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

/** Load ONLY read-only Airtable credentials (never provider keys). */
function loadAirtableEnv() {
  for (const envPath of [
    path.join(ROOT, ".env"),
    "C:/Dev/dealality/my-operators-backend/.env",
  ]) {
    if (!fs.existsSync(envPath)) continue;
    for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
      const m = line.match(
        /^(AIRTABLE_API_KEY|AIRTABLE_PAT|AIRTABLE_BASE_ID|AIRTABLE_GDI_BASE_ID)=(.*)$/,
      );
      if (m && !process.env[m[1]]) {
        process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
      }
    }
  }
}

function parseArgs(argv) {
  const a = {
    hotelId: DEFAULT_HOTEL_ID,
    reportDir: DEFAULT_REPORT_DIR,
    stagedVersion: null,
    airtableFixture: null,
    help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const t = argv[i];
    const eq = t.indexOf("=");
    const [k, inline] = t.startsWith("--") && eq > 0 ? [t.slice(0, eq), t.slice(eq + 1)] : [t, null];
    const next = () => (inline != null ? inline : argv[++i]);
    if (k === "--hotel-id") a.hotelId = next();
    else if (k === "--report-dir") a.reportDir = path.resolve(next());
    else if (k === "--staged-version") a.stagedVersion = next();
    else if (k === "--airtable-fixture") a.airtableFixture = path.resolve(next());
    else if (k === "--help" || k === "-h") a.help = true;
    else throw new Error(`Unknown argument: ${t}`);
  }
  return a;
}

// ---------------------------------------------------------------- field extraction

export function personChannelValue(person = {}, channelType) {
  const ch = (person.channels || []).find(
    (c) => text(c.channelType).toUpperCase() === channelType && text(c.value),
  );
  return ch ? text(ch.value) : "";
}

export function contactFacts(opp = {}) {
  const p = opp.primaryContact || {};
  return {
    name: text(p.fullName || p.name),
    title: text(p.professionalTitle || p.title || p.role),
    email: lc(p.email || p.workEmail || personChannelValue(p, "PROFESSIONAL_EMAIL")),
    phone: digits(
      p.phone || personChannelValue(p, "PROFESSIONAL_DIRECT_PHONE") || personChannelValue(p, "PERSON_PHONE"),
    ),
    linkedin: lc(
      p.linkedinUrl || p.publicProfileUrl || personChannelValue(p, "PERSON_LINKEDIN"),
    ),
    authority: text(
      p.buyingAuthorityStatus ||
        p.buyingAuthorityAnnotation?.status ||
        opp.buyingAuthorityAnnotation?.primaryStatus,
    ),
    secondaryCount:
      (opp.secondaryContact && text(opp.secondaryContact.fullName || opp.secondaryContact.name) ? 1 : 0) +
      (Array.isArray(opp.alternativeEntryPoints) ? opp.alternativeEntryPoints.length : 0),
  };
}

function commercialValue(opp = {}, field) {
  switch (field) {
    case "whyThisHotel":
      return text(opp.whyThisHotel || opp.summaryWhyHotel || opp.hotelOpportunityThesis);
    case "potentialHotelBusiness":
      return text(
        typeof opp.potentialHotelBusiness === "object"
          ? JSON.stringify(opp.potentialHotelBusiness)
          : opp.potentialHotelBusiness || (typeof opp.potentialBusiness === "object" ? JSON.stringify(opp.potentialBusiness) : opp.potentialBusiness),
      );
    case "sources":
      return Array.isArray(opp.sources) ? String(opp.sources.length) : "";
    case "customerVisible":
      return opp.customerVisible === undefined ? "" : String(opp.customerVisible);
    case "displayTitle":
      return text(opp.displayTitle || opp.drawerHeaderTitle);
    default: {
      const v = opp[field];
      return v && typeof v === "object" ? JSON.stringify(v) : text(v);
    }
  }
}

function compareValues(stagedV, airtableV, normalize = (x) => x) {
  const s = normalize(stagedV);
  const a = normalize(airtableV);
  if (!s && !a) return "BOTH_EMPTY";
  if (s && !a) return "STAGED_ONLY";
  if (!s && a) return "AIRTABLE_ONLY";
  return s === a ? "MATCH" : "MISMATCH";
}

function tally(rows, key) {
  return rows.reduce((acc, r) => {
    acc[r[key]] = (acc[r[key]] || 0) + 1;
    return acc;
  }, {});
}

// ---------------------------------------------------------------- core (exported for tests)

/**
 * Pure comparison: Airtable-sourced opportunities → in-memory enriched
 * reconstruction → parity vs STAGED opportunities.
 */
export function buildReconstruction({ airtableOpps, stagedOpps, findymailLedger = [], hotelId }) {
  const stagedById = new Map(stagedOpps.map((o) => [o.id || o.opportunityId, o]));
  const rawAirtableById = new Map(airtableOpps.map((o) => [o.id || o.opportunityId, o]));

  // Reconstruction candidate set = customer-visible, non-test Airtable rows.
  // (STAGED is deliberately NOT consulted for membership.)
  const candidates = airtableOpps.filter(
    (o) => o.customerVisible !== false && o.isTestData !== true,
  );
  const recovered = applyEvidenceRecoveryToOpportunities(candidates, {
    findymailLedger,
    normalizeIdentity: true,
  });
  const annotated = annotateHotelOpportunitySet(recovered.opportunities);
  const recById = new Map(annotated.opportunities.map((o) => [o.id || o.opportunityId, o]));

  const stagedIds = [...stagedById.keys()];
  const presentInAirtable = stagedIds.filter((id) => rawAirtableById.has(id));
  const missingInAirtable = stagedIds.filter((id) => !rawAirtableById.has(id));
  const presentInReconstruction = stagedIds.filter((id) => recById.has(id));
  const extraBeyondStaged = [...recById.keys()].filter((id) => !stagedById.has(id));

  const rows = [];
  for (const id of presentInReconstruction) {
    const s = contactFacts(stagedById.get(id));
    const aRaw = contactFacts(rawAirtableById.get(id));
    const aRec = contactFacts(recById.get(id));
    rows.push({
      opportunityId: id,
      name: compareValues(s.name, aRec.name, lc),
      title: compareValues(s.title, aRec.title, lc),
      email: compareValues(s.email, aRec.email),
      phone: compareValues(s.phone, aRec.phone),
      linkedin: compareValues(s.linkedin, aRec.linkedin),
      authorityStagedPresent: Boolean(s.authority),
      authorityAirtableRawPresent: Boolean(aRaw.authority),
      authorityReconstructedInMemoryPresent: Boolean(aRec.authority),
      authorityStatusMatchesStaged: Boolean(s.authority) && s.authority === aRec.authority,
      secondaryStaged: s.secondaryCount,
      secondaryAirtable: aRaw.secondaryCount,
    });
  }

  const commercialRows = [];
  for (const id of presentInReconstruction) {
    const s = stagedById.get(id);
    const a = recById.get(id);
    const rec = { opportunityId: id };
    for (const f of COMMERCIAL_FIELDS) {
      rec[f] = compareValues(commercialValue(s, f), commercialValue(a, f), lc);
    }
    commercialRows.push(rec);
  }
  const commercialSummary = Object.fromEntries(
    COMMERCIAL_FIELDS.map((f) => [f, tally(commercialRows, f)]),
  );

  // Presentation fields present in STAGED but absent from EVERY Airtable row.
  const stagedKeys = new Set(stagedOpps.flatMap((o) => Object.keys(o)));
  const airtableKeys = new Set(airtableOpps.flatMap((o) => Object.keys(o)));
  const stagedKeysAbsentFromAirtable = [...stagedKeys].filter((k) => !airtableKeys.has(k)).sort();

  const contactSummary = {
    idParity: {
      stagedCount: stagedIds.length,
      presentInAirtable: presentInAirtable.length,
      missingInAirtable: missingInAirtable.length,
      presentInReconstruction: presentInReconstruction.length,
    },
    name: tally(rows, "name"),
    title: tally(rows, "title"),
    email: tally(rows, "email"),
    phone: tally(rows, "phone"),
    linkedin: tally(rows, "linkedin"),
    authority: {
      stagedPresent: rows.filter((r) => r.authorityStagedPresent).length,
      airtableRawPresent: rows.filter((r) => r.authorityAirtableRawPresent).length,
      reconstructedInMemoryPresent: rows.filter((r) => r.authorityReconstructedInMemoryPresent).length,
      statusMatchesStaged: rows.filter((r) => r.authorityStatusMatchesStaged).length,
      compared: rows.length,
    },
  };

  const scoreOpts = { hotelId, findymailLedger };
  const reconstructedIntersection = presentInReconstruction.map((id) => recById.get(id));
  const ledgerStaged = buildHotelContactBestEffortLedger(stagedOpps, scoreOpts);
  const ledgerRec = buildHotelContactBestEffortLedger(reconstructedIntersection, scoreOpts);

  return {
    candidates,
    reconstructedAll: annotated.opportunities,
    reconstructedIntersectionIds: presentInReconstruction,
    ids: { presentInAirtable, missingInAirtable, presentInReconstruction, extraBeyondStaged },
    contactRows: rows,
    contactSummary,
    commercialRows,
    commercialSummary,
    stagedKeysAbsentFromAirtable,
    recoveryReport: recovered.report,
    annotationReport: { ...annotated.report, rows: undefined },
    scorecards: {
      staged: ledgerStaged.scorecard,
      reconstructedIntersection: ledgerRec.scorecard,
      stagedGate: evaluateContactBestEffortCertificationGate(ledgerStaged),
      reconstructedGate: evaluateContactBestEffortCertificationGate(ledgerRec),
    },
  };
}

/** Derive status + exact missing persistence corrections from a reconstruction. */
export function evaluateReconstruction(rec, { liveAirtableRead }) {
  const corrections = [];
  const c = rec.contactSummary;
  const n = rec.ids.presentInReconstruction.length;

  if (rec.ids.missingInAirtable.length) {
    corrections.push({
      code: "STAGED_OPPORTUNITIES_MISSING_FROM_AIRTABLE_GDO",
      count: rec.ids.missingInAirtable.length,
      correction: "Upsert the missing STAGED opportunity ids into Group Demand Opportunities (dry-run first; founder-authorized apply).",
      sample: rec.ids.missingInAirtable.slice(0, 10),
    });
  }
  if (rec.ids.extraBeyondStaged.length) {
    corrections.push({
      code: "INVENTORY_MEMBERSHIP_NOT_PERSISTED",
      count: rec.ids.extraBeyondStaged.length,
      correction:
        "Airtable holds more customer-visible rows than the STAGED inventory and no persisted field distinguishes them (no STAGED/CURRENT membership, qualification gate or inventory-population field on GDO). Persist per-opportunity publication membership (e.g. stagedPublicationVersion + inventoryPopulationPrimary) when STAGED is activated.",
    });
  }
  for (const [field, label] of [
    ["email", "EMAIL"],
    ["phone", "PHONE"],
    ["linkedin", "LINKEDIN"],
    ["title", "TITLE"],
    ["name", "NAME"],
  ]) {
    const t = c[field];
    const stagedOnly = t.STAGED_ONLY || 0;
    const mismatch = t.MISMATCH || 0;
    if (stagedOnly || mismatch) {
      corrections.push({
        code: `CONTACT_${label}_NOT_RECOVERABLE_FROM_AIRTABLE`,
        stagedOnly,
        mismatch,
        correction: `Persist primaryContact ${field} (and channels[]) to GDO payload at contact-research/Findymail apply time; ${stagedOnly} STAGED value(s) absent in Airtable, ${mismatch} differ.`,
      });
    }
  }
  if (n > 0 && c.authority.airtableRawPresent < c.authority.compared) {
    corrections.push({
      code: "BUYING_AUTHORITY_ANNOTATION_NOT_PERSISTED_IN_AIRTABLE",
      stagedPresent: c.authority.stagedPresent,
      airtableRawPresent: c.authority.airtableRawPresent,
      reconstructedInMemoryPresent: c.authority.reconstructedInMemoryPresent,
      correction:
        "Authority is recomputed in memory but is persisted in neither Airtable GDO nor the STAGED package. Persist primaryContact.buyingAuthorityStatus + opportunity.buyingAuthorityAnnotation to GDO (founder-authorized write) and rematerialize STAGED through the publication builder.",
    });
  }
  const commercialGaps = Object.entries(rec.commercialSummary)
    .map(([field, t]) => ({ field, stagedOnly: t.STAGED_ONLY || 0, mismatch: t.MISMATCH || 0 }))
    .filter((r) => r.stagedOnly || r.mismatch);
  if (commercialGaps.length) {
    corrections.push({
      code: "COMMERCIAL_FIELDS_NOT_FULLY_RECOVERABLE_FROM_AIRTABLE",
      fields: commercialGaps,
      correction:
        "These commercial/presentation fields exist in STAGED but are absent or different in Airtable. Persist them in the GDO payload (or regenerate via the publication builder from the research inventory).",
    });
  }
  if (rec.stagedKeysAbsentFromAirtable.length) {
    corrections.push({
      code: "DERIVED_PRESENTATION_MODELS_REQUIRE_PUBLICATION_BUILDER",
      count: rec.stagedKeysAbsentFromAirtable.length,
      fields: rec.stagedKeysAbsentFromAirtable,
      correction:
        "Drawer / tile / segment models exist only in the immutable package. Full package regeneration needs research inventory + publication builder; GDO alone cannot reproduce them.",
    });
  }
  if (!liveAirtableRead) {
    corrections.push({
      code: "LIVE_AIRTABLE_READ_NOT_PERFORMED",
      correction: "Fixture source used — rerun without --airtable-fixture against authorized Airtable credentials.",
    });
  }

  const hardGap = corrections.some((x) =>
    [
      "STAGED_OPPORTUNITIES_MISSING_FROM_AIRTABLE_GDO",
      "INVENTORY_MEMBERSHIP_NOT_PERSISTED",
      "BUYING_AUTHORITY_ANNOTATION_NOT_PERSISTED_IN_AIRTABLE",
      "COMMERCIAL_FIELDS_NOT_FULLY_RECOVERABLE_FROM_AIRTABLE",
      "DERIVED_PRESENTATION_MODELS_REQUIRE_PUBLICATION_BUILDER",
      "LIVE_AIRTABLE_READ_NOT_PERFORMED",
    ].includes(x.code) || x.code.startsWith("CONTACT_"),
  );
  return {
    status: hardGap ? CERT_STATUS.PARTIAL : CERT_STATUS.PASS,
    missingPersistenceCorrections: corrections,
  };
}

// ---------------------------------------------------------------- orchestration

async function loadAirtableOpportunities(args) {
  if (args.airtableFixture) {
    const raw = JSON.parse(fs.readFileSync(args.airtableFixture, "utf8"));
    return {
      sourceKind: "FIXTURE",
      liveAirtableRead: false,
      opportunities: Array.isArray(raw) ? raw : raw.opportunities || [],
    };
  }
  loadAirtableEnv();
  const store = await import(
    pathToFileURL(path.join(ROOT, "lib/group-demand-intelligence/airtable-opportunity-store.js")).href
  );
  if (typeof store.isGdiOpportunityAirtableConfigured === "function" && !store.isGdiOpportunityAirtableConfigured()) {
    const err = new Error("AIRTABLE_GDO_NOT_CONFIGURED");
    err.code = "AIRTABLE_GDO_NOT_CONFIGURED";
    throw err;
  }
  const opportunities = await store.listOpportunitiesForHotel(args.hotelId);
  return { sourceKind: "AIRTABLE_GDO_listOpportunitiesForHotel", liveAirtableRead: true, opportunities };
}

function loadStaged(hotelId, stagedVersionOverride) {
  const pointerFile = path.join(PACKAGE_ROOT, hotelId, "STAGED.json");
  const currentFile = path.join(PACKAGE_ROOT, hotelId, "CURRENT.json");
  if (!fs.existsSync(pointerFile)) return { ok: false, error: "STAGED_POINTER_MISSING", pointerFile, currentFile };
  const pointer = JSON.parse(fs.readFileSync(pointerFile, "utf8"));
  const version = stagedVersionOverride || pointer.publicationVersion;
  const pkgFile = path.join(PACKAGE_ROOT, hotelId, version, "package.json");
  if (!fs.existsSync(pkgFile)) return { ok: false, error: "STAGED_PACKAGE_MISSING", pointerFile, currentFile, version };
  const pkg = JSON.parse(fs.readFileSync(pkgFile, "utf8"));
  return { ok: true, pointer, version, pkg, pointerFile, currentFile, pkgFile };
}

function writeIsolatedPackage({ hotelId, reconstructedAll, stagedPkg, reportDir, nowIso }) {
  const isolatedRoot = path.join(reportDir, "_isolated");
  for (const protectedRoot of [
    PACKAGE_ROOT,
    path.join(ROOT, "data/group-demand-intelligence/publication-packages"),
  ]) {
    if (isInside(isolatedRoot, protectedRoot)) {
      throw new Error(`Refusing isolated reconstruction inside a publication-package root: ${protectedRoot}`);
    }
  }
  const publicationVersion = `airtable_reconstruction_${nowIso.slice(0, 10).replace(/-/g, "")}`;
  const body = {
    hotelId,
    publicationVersion,
    publicationId: `${publicationVersion}__${hotelId}`,
    lifecycleState: "RECONSTRUCTION_VALIDATION_ONLY",
    approvalStatus: "NOT_FOR_PUBLICATION",
    notes:
      "Isolated Airtable-only reconstruction. Never a STAGED/CURRENT pointer target. Built from GDO rows + in-memory evidence recovery + buying-authority annotation.",
    source: "AIRTABLE_GDO_listOpportunitiesForHotel",
    usedStagedAsSource: false,
    presentationSchemaVersion: stagedPkg?.presentationSchemaVersion || null,
    generatedAt: nowIso,
    opportunityCount: reconstructedAll.length,
    opportunities: reconstructedAll,
  };
  body.contentFingerprintSha256 = sha256Json(body.opportunities);
  const file = path.join(isolatedRoot, hotelId, publicationVersion, "package.json");
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
  return { file: path.relative(ROOT, file).replace(/\\/g, "/"), publicationVersion, contentFingerprintSha256: body.contentFingerprintSha256 };
}

export async function run(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log("See header of scripts/gdi-clean-airtable-reconstruction-cert-v1.mjs");
    return { ok: true, help: true };
  }
  const nowIso = new Date().toISOString();
  const { hotelId, reportDir } = args;
  const staged = loadStaged(hotelId, args.stagedVersion);

  const base = {
    contract: GDI_CLEAN_AIRTABLE_RECONSTRUCTION_CERT_V1,
    generatedAt: nowIso,
    hotelId,
    reconstructionSource: "AIRTABLE_GDO_listOpportunitiesForHotel",
    usedStagedAsSource: false,
    stagedIsComparisonBaselineOnly: true,
    guarantees: {
      airtableWrites: 0,
      providerCalls: 0,
      creditsSpent: 0,
      surfeUsed: false,
      uiChanges: false,
      stagedOverwrite: false,
      currentChanged: false,
    },
    durableStores: DURABLE_STORES,
  };

  const blocked = (reason, extra = {}) => {
    const report = {
      ...base,
      status: CERT_STATUS.BLOCKED,
      blockedReason: reason,
      missingPersistenceCorrections: [{ code: reason, correction: extra.correction || reason }],
      ...extra,
    };
    writeJson(reportDir, "00-CERTIFICATION.json", report);
    console.log(JSON.stringify({ status: report.status, blockedReason: reason, reportDir }, null, 2));
    return { ok: false, status: CERT_STATUS.BLOCKED, report };
  };

  if (!staged.ok) {
    return blocked(staged.error, {
      correction: `Comparison baseline unavailable for ${hotelId} (${staged.error}).`,
    });
  }

  const hashesBefore = {
    stagedPointer: sha256File(staged.pointerFile),
    currentPointer: sha256File(staged.currentFile),
    stagedPackage: sha256File(staged.pkgFile),
  };

  let source;
  try {
    source = await loadAirtableOpportunities(args);
  } catch (err) {
    return blocked(err.code || "AIRTABLE_READ_FAILED", {
      correction: `Airtable GDO unreadable: ${err.message}. Provide read-only Airtable credentials (AIRTABLE_API_KEY/AIRTABLE_PAT + base id).`,
    });
  }
  if (!source.opportunities.length) {
    return blocked("AIRTABLE_GDO_EMPTY_FOR_HOTEL", {
      correction: `No GDO rows for ${hotelId}; nothing to reconstruct.`,
    });
  }

  const stagedOpps = staged.pkg.opportunities || [];
  const ledger = readFindymailUsageLedger({ repoRoot: ROOT });
  const rec = buildReconstruction({
    airtableOpps: source.opportunities,
    stagedOpps,
    findymailLedger: ledger,
    hotelId,
  });
  const evaluation = evaluateReconstruction(rec, { liveAirtableRead: source.liveAirtableRead });
  const isolated = writeIsolatedPackage({
    hotelId,
    reconstructedAll: rec.reconstructedAll,
    stagedPkg: staged.pkg,
    reportDir,
    nowIso,
  });

  const hashesAfter = {
    stagedPointer: sha256File(staged.pointerFile),
    currentPointer: sha256File(staged.currentFile),
    stagedPackage: sha256File(staged.pkgFile),
  };
  const unchanged =
    hashesBefore.stagedPointer === hashesAfter.stagedPointer &&
    hashesBefore.currentPointer === hashesAfter.currentPointer &&
    hashesBefore.stagedPackage === hashesAfter.stagedPackage;
  if (!unchanged) {
    throw new Error("INVARIANT_VIOLATION: STAGED/CURRENT bytes changed during reconstruction cert");
  }

  const certification = {
    ...base,
    status: evaluation.status,
    sourceKind: source.sourceKind,
    liveAirtableRead: source.liveAirtableRead,
    comparisonBaseline: {
      kind: "CURRENT_STAGED_PACKAGE",
      publicationVersion: staged.version,
      opportunityCount: stagedOpps.length,
      stagedPointerSha256: hashesAfter.stagedPointer,
      currentPointerPresent: Boolean(hashesAfter.currentPointer),
      pointersAndPackageUnchanged: unchanged,
    },
    airtable: {
      gdoRowCount: source.opportunities.length,
      reconstructionCandidateCount: rec.candidates.length,
      candidateRule: "customerVisible !== false && isTestData !== true (STAGED not consulted)",
    },
    isolatedPackage: isolated,
    idParity: {
      stagedIdsPresentInAirtable: rec.ids.presentInAirtable.length,
      stagedIdsMissingInAirtable: rec.ids.missingInAirtable.length,
      reconstructedCandidatesBeyondStaged: rec.ids.extraBeyondStaged.length,
      canRecoverStagedMembershipFromAirtableAlone:
        rec.ids.missingInAirtable.length === 0 && rec.ids.extraBeyondStaged.length === 0,
    },
    contactParity: rec.contactSummary,
    commercialParity: rec.commercialSummary,
    scorecards: rec.scorecards,
    stagedKeysAbsentFromAirtable: rec.stagedKeysAbsentFromAirtable,
    missingPersistenceCorrections: evaluation.missingPersistenceCorrections,
    notClaimed:
      "Full publication package regeneration, drawer models and :8080 parity are not claimed; this certifies Airtable GDO recoverability only.",
  };

  const files = [
    writeJson(reportDir, "00-CERTIFICATION.json", certification),
    writeJson(reportDir, "01-ID_AND_CONTACT_PARITY.json", {
      summary: rec.contactSummary,
      rows: rec.contactRows,
    }),
    writeJson(reportDir, "02-COMMERCIAL_FIELD_PARITY.json", {
      fields: COMMERCIAL_FIELDS,
      summary: rec.commercialSummary,
      rows: rec.commercialRows,
    }),
    writeJson(reportDir, "03-AUTHORITY_AND_CHANNEL_RECOVERY.json", {
      annotation: rec.annotationReport,
      channelEvidenceRecovery: rec.recoveryReport,
      scorecards: rec.scorecards,
    }),
    writeJson(reportDir, "04-DURABLE_STORES.json", { durableStores: DURABLE_STORES }),
    writeJson(reportDir, "05-MISSING_PERSISTENCE_CORRECTIONS.json", {
      status: evaluation.status,
      corrections: evaluation.missingPersistenceCorrections,
    }),
  ];

  console.log(
    JSON.stringify(
      {
        status: certification.status,
        hotelId,
        sourceKind: certification.sourceKind,
        gdoRowCount: certification.airtable.gdoRowCount,
        stagedCount: stagedOpps.length,
        idParity: certification.idParity,
        authority: certification.contactParity.authority,
        corrections: evaluation.missingPersistenceCorrections.map((c) => c.code),
        files: files.map((f) => path.relative(ROOT, f).replace(/\\/g, "/")),
      },
      null,
      2,
    ),
  );
  return { ok: true, status: certification.status, certification, files };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  run().catch((err) => {
    console.error(err?.stack || err);
    process.exit(1);
  });
}
