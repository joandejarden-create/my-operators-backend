/**
 * Jev advisory shadow — optional, never qualifies / rejects / publishes.
 * Prefer deterministic checks; record incremental Jev findings separately.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const JEV_ADVISORY_SHADOW_VERSION = "gdi-jev-advisory-shadow-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const DEFAULT_LAB_ROOT = path.resolve(ROOT, "../gdi-parallel-lab");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Deterministic card integrity checks (no Jev API).
 */
export function runDeterministicAdvisoryChecks(cards = []) {
  const findings = [];
  for (const card of cards) {
    const id = card.cardId || card.id || card.opportunityId || "unknown";
    const evidence = card.evidenceLinks || card.sources || [];
    const urls = Array.isArray(evidence)
      ? evidence.map((e) => (typeof e === "string" ? e : e?.url)).filter(Boolean)
      : [];
    if (!urls.length) {
      findings.push({
        cardId: id,
        severity: "missing_evidence",
        message: "No source URLs — missing evidence remains missing even if Jev would agree",
        source: "deterministic",
      });
    }
    const thesis =
      card.whyWyndhamFits ||
      card.hotelOpportunityThesis ||
      card.fitExplanation ||
      "";
    if (!text(thesis)) {
      findings.push({
        cardId: id,
        severity: "missing_hotel_rationale",
        message: "Missing hotel-specific rationale",
        source: "deterministic",
      });
    }
    const contact = card.contact || card.primaryContact || {};
    const route =
      contact.email ||
      contact.functionalEmail ||
      contact.phone ||
      card.functionalContactEmail;
    if (!route) {
      findings.push({
        cardId: id,
        severity: "missing_contact_route",
        message: "No usable contact route on card",
        source: "deterministic",
      });
    }
    if (card.noveltyVsBaseline === "UNKNOWN") {
      findings.push({
        cardId: id,
        severity: "novelty_unknown",
        message: "Baseline novelty UNKNOWN — do not claim net-new without corpus compare",
        source: "deterministic",
      });
    }
  }
  return {
    version: JEV_ADVISORY_SHADOW_VERSION,
    layer: "deterministic",
    cardCount: cards.length,
    findingCount: findings.length,
    findings,
    autoQualify: false,
    autoReject: false,
    autoPublish: false,
  };
}

/**
 * Optional Jev shadow — loads lab freeze artifacts if present; does not call Jev
 * unless opts.invokeJev === true (default false).
 */
export function runJevAdvisoryShadow(opts = {}) {
  const labRoot = opts.labRoot || process.env.GDI_PARALLEL_LAB_ROOT || DEFAULT_LAB_ROOT;
  const cards = Array.isArray(opts.cards) ? opts.cards : [];
  const deterministic = runDeterministicAdvisoryChecks(cards);

  let jevIncremental = {
    invoked: false,
    available: false,
    findings: [],
    note: "Jev not invoked — deterministic layer only",
  };

  const detPath = path.join(
    labRoot,
    "data/sandiego-bayside/../jev-shadow"
  );
  // Prefer documented lab jev-shadow outputs if they exist
  const candidates = [
    path.join(labRoot, "data/jev-shadow/deterministic-report.json"),
    path.join(labRoot, "data/sandiego-bayside/jev-shadow/deterministic-report.json"),
    path.join(labRoot, "data/jev-shadow/latest.json"),
  ];

  for (const p of candidates) {
    if (fs.existsSync(p)) {
      try {
        const doc = JSON.parse(fs.readFileSync(p, "utf8"));
        jevIncremental = {
          invoked: false,
          available: true,
          path: p,
          findings: Array.isArray(doc.findings) ? doc.findings : [],
          note: "Loaded prior Jev/deterministic shadow artifact — advisory only",
        };
        break;
      } catch {
        /* ignore */
      }
    }
  }

  if (opts.invokeJev === true) {
    jevIncremental = {
      invoked: false,
      available: false,
      findings: [],
      note:
        "Live Jev invocation refused in workflow integration task — keep advisory offline/shadow only",
    };
  }

  return {
    version: JEV_ADVISORY_SHADOW_VERSION,
    hotelId: opts.hotelId || null,
    asOf: new Date().toISOString(),
    deterministic,
    jevIncremental,
    policy: {
      autoQualify: false,
      autoReject: false,
      autoPublish: false,
      missingEvidenceRemainsMissing: true,
      note: "Jev agreement does not create source evidence",
    },
  };
}

export function writeJevAdvisoryArtifacts(result, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const p = path.join(outDir, "jev-advisory-shadow.json");
  fs.writeFileSync(p, JSON.stringify(result, null, 2) + "\n");
  return p;
}
