/**
 * Replace research-note titles with concise account + occasion titles for
 * Bethesda + Mayflower. Moves verification/date notes into sourcing fields.
 * Preserves stable IDs. Dry-run by default; --apply writes via importer.
 *
 * Usage:
 *   node scripts/gdi-title-cleanup-beth-may-v1.mjs
 *   node scripts/gdi-title-cleanup-beth-may-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { importReviewedProspectPackage } from "../lib/group-demand-intelligence/reviewed-prospect-import-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const apply = process.argv.includes("--apply");

const HOTELS = [
  {
    hotelId: "recLuxvwwxID7U2B8",
    key: "bethesda",
    label: "Bethesda Marriott",
  },
  {
    hotelId: "rec0eTEnCqlsGJJt9",
    key: "mayflower",
    label: "Mayflower",
  },
];

const OUT = path.join(
  ROOT,
  "reports/gdi-operational-baseline/title-cleanup"
);

/** Known concise titles keyed by opportunity id substring or org+year pattern. */
const TITLE_OVERRIDES = {
  // Mayflower
  may_ahima: "AHIMA Advocacy Summit 2027",
  may_ndss: "NDSS Advocacy Conference 2027",
  may_acc: "ACC Legislative Conference 2027",
  may_amwa: "AMWA Annual Meeting 2027",
  may_acts: "ACTS Translational Science 2027",
  may_nado: "NADO / DDAA Washington Conference 2028",
  may_nar: "NAR GAD Institute 2027",
  may_aad: "AAD Annual Meeting 2028",
  may_nchv: "NCHV Annual Conference (DC pattern)",
  may_uscm: "U.S. Conference of Mayors — next DC cycle",
  may_ustravel: "U.S. Travel Association — next DC lodging cycle",
  // Bethesda
  beth_nado_2028: "NADO Arlington Washington Conference 2028",
  beth_nado_2027: "NADO Arlington Washington Conference 2027 (overflow watch)",
  beth_ahima: "AHIMA Advocacy Summit 2027 (overflow)",
  beth_nih: "NIH / institute corridor lodging — future cycle",
};

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isResearchNoteTitle(title) {
  const t = text(title);
  if (!t) return true;
  if (/verified via|verified:|hotel not named|hotel block tba|not verified|unconfirmed this pass|recurring program pattern claimed|exact next dates/i.test(t)) {
    return true;
  }
  if (t.length > 90 && /—/.test(t)) return true;
  return false;
}

function extractVerificationNote(title) {
  const t = text(title);
  const m = t.match(/—\s*(.+)$/);
  if (m && /verified|hotel not|dates|unconfirmed|claimed/i.test(m[1])) {
    return m[1].trim();
  }
  return null;
}

function deriveCleanTitle(opp, hotelKey) {
  const id = text(opp.id || opp.opportunityId).toLowerCase();
  const org = text(opp.organizationName || opp.account || "");
  const title = text(opp.title);
  const year =
    (title.match(/\b(202[6-9]|2030)\b/) || [])[0] ||
    (text(opp.eventStartDate).match(/\b(202[6-9]|2030)\b/) || [])[0] ||
    "";

  // Overlay / known id patterns
  if (/ahima/i.test(id + org + title)) {
    return hotelKey === "bethesda"
      ? TITLE_OVERRIDES.beth_ahima
      : TITLE_OVERRIDES.may_ahima;
  }
  if (/ndss|down syndrome/i.test(id + org + title)) return TITLE_OVERRIDES.may_ndss;
  if (/acc|cardiology/i.test(id + org + title) && /legis/i.test(id + title + text(opp.hotelOpportunityThesis))) {
    return TITLE_OVERRIDES.may_acc;
  }
  if (/amwa|medical women/i.test(id + org + title)) return TITLE_OVERRIDES.may_amwa;
  if (/acts|translational/i.test(id + org + title)) return TITLE_OVERRIDES.may_acts;
  if (/nado/i.test(id + org + title)) {
    if (/2028/.test(title + year)) {
      return hotelKey === "bethesda"
        ? TITLE_OVERRIDES.beth_nado_2028
        : TITLE_OVERRIDES.may_nado;
    }
    if (/2027/.test(title + year) && hotelKey === "bethesda") {
      return TITLE_OVERRIDES.beth_nado_2027;
    }
    return hotelKey === "bethesda"
      ? TITLE_OVERRIDES.beth_nado_2028
      : TITLE_OVERRIDES.may_nado;
  }
  if (/realtor|nar\b|gad institute/i.test(id + org + title)) return TITLE_OVERRIDES.may_nar;
  if (/dermatology|\baad\b/i.test(id + org + title)) return TITLE_OVERRIDES.may_aad;
  if (/homeless veterans|nchv/i.test(id + org + title)) return TITLE_OVERRIDES.may_nchv;
  if (/conference of mayors|uscm/i.test(id + org + title)) return TITLE_OVERRIDES.may_uscm;
  if (/u\.?s\.?\s*travel/i.test(id + org + title)) return TITLE_OVERRIDES.may_ustravel;
  if (/nih|institute named/i.test(id + org + title) && hotelKey === "bethesda") {
    return TITLE_OVERRIDES.beth_nih;
  }

  // Generic: org short + occasion fragment before em-dash
  const occasion = title.split("—")[0].trim();
  if (org && year && occasion.length > 8 && occasion.length < 80) {
    // Prefer short org + year when occasion is just the org name again
    if (occasion.toLowerCase().startsWith(org.toLowerCase().slice(0, 12))) {
      const rest = occasion.slice(org.length).replace(/^[\s—:-]+/, "").trim();
      if (rest) return `${shortOrg(org)} ${rest}`.slice(0, 90);
      return `${shortOrg(org)} ${year}`.slice(0, 90);
    }
    return occasion.slice(0, 90);
  }
  if (org && year) return `${shortOrg(org)} ${year}`.slice(0, 90);
  if (occasion && occasion.length < 90) return occasion;
  return title.slice(0, 80);
}

function shortOrg(org) {
  return text(org)
    .replace(/\s*\([^)]*\)\s*$/, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 48);
}

function loadLocalOpportunities(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json"
  );
  if (!fs.existsSync(p)) return { path: p, opportunities: [] };
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const list = Array.isArray(doc)
    ? doc
    : Array.isArray(doc.opportunities)
      ? doc.opportunities
      : [];
  return { path: p, opportunities: list, doc };
}

fs.mkdirSync(OUT, { recursive: true });
const packageRows = [];
const diffs = [];

for (const hotel of HOTELS) {
  const { opportunities } = loadLocalOpportunities(hotel.hotelId);
  for (const opp of opportunities) {
    const title = text(opp.title);
    if (!isResearchNoteTitle(title)) continue;
    const clean = deriveCleanTitle(opp, hotel.key);
    if (!clean || clean === title) continue;
    const verificationNote = extractVerificationNote(title);
    const next = {
      ...opp,
      hotelId: hotel.hotelId,
      title: clean,
      // Preserve original evidence
      titleCleanup: {
        previousTitle: title,
        cleanedAt: new Date().toISOString(),
        verificationNoteMoved: verificationNote || null,
      },
    };
    if (verificationNote) {
      const existingLimitations = text(opp.materialLimitations);
      if (!existingLimitations.includes(verificationNote.slice(0, 40))) {
        next.materialLimitations = [existingLimitations, verificationNote]
          .filter(Boolean)
          .join(" | ")
          .slice(0, 800);
      }
      if (!text(opp.sourcingStatus) || /unknown|unconfirmed/i.test(text(opp.sourcingStatus))) {
        next.sourcingStatus = verificationNote.slice(0, 240);
      }
    }
    packageRows.push(next);
    diffs.push({
      hotelId: hotel.hotelId,
      hotelKey: hotel.key,
      id: opp.id,
      from: title,
      to: clean,
      verificationNote: verificationNote || null,
    });
  }
}

fs.writeFileSync(
  path.join(OUT, "title-cleanup-diff.json"),
  JSON.stringify({ asOf: new Date().toISOString(), apply, diffs }, null, 2) +
    "\n"
);
fs.writeFileSync(
  path.join(OUT, "title-cleanup-package.json"),
  JSON.stringify(
    {
      version: "gdi-title-cleanup-beth-may-v1",
      hotelIds: HOTELS.map((h) => h.hotelId),
      opportunities: packageRows,
    },
    null,
    2
  ) + "\n"
);

let importResult = null;
if (apply && packageRows.length) {
  // Split by hotel for importer
  for (const hotel of HOTELS) {
    const rows = packageRows.filter((r) => r.hotelId === hotel.hotelId);
    if (!rows.length) continue;
    const pkgPath = path.join(OUT, `title-cleanup-package-${hotel.key}.json`);
    fs.writeFileSync(
      pkgPath,
      JSON.stringify(
        {
          version: "gdi-title-cleanup-package-v1",
          hotelId: hotel.hotelId,
          opportunities: rows,
        },
        null,
        2
      ) + "\n"
    );
    importResult = await importReviewedProspectPackage({
      packagePath: pkgPath,
      hotelId: hotel.hotelId,
      adapter: "airtable",
      apply: true,
      outDir: path.join(OUT, `apply-${hotel.key}`),
    });
  }
}

console.log(
  JSON.stringify(
    {
      ok: true,
      apply,
      diffCount: diffs.length,
      mayflower: diffs.filter((d) => d.hotelKey === "mayflower").length,
      bethesda: diffs.filter((d) => d.hotelKey === "bethesda").length,
      sample: diffs.slice(0, 8),
      importResult: importResult
        ? {
            updated: importResult.updated ?? importResult.counts?.updated,
            status: importResult.status,
          }
        : null,
      note: apply
        ? "Applied via reviewed-prospect importer"
        : "Dry-run only — pass --apply to persist",
    },
    null,
    2
  )
);
