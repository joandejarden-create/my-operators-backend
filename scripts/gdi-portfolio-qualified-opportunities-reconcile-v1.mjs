#!/usr/bin/env node
/**
 * Portfolio-wide GDI Qualified Opportunity reconciliation (read-only).
 *
 * Discovers every hotel across research stores, publication packages, configs,
 * and the selectable catalog. Evaluates the six FQO gates. Does NOT activate
 * STAGED/CURRENT pointers, write Airtable, or spend paid research.
 *
 *   node scripts/gdi-portfolio-qualified-opportunities-reconcile-v1.mjs
 *   npm run gdi:portfolio-qualified-opportunities-reconcile-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateFinalizedQualifiedOpportunity,
  evaluatePersonSpecificOutreachChannel,
  FQO_STATUS,
} from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
import {
  evaluatePortfolioQualifiedOpportunityFullE2eGate,
  FQE2E_DISPOSITION,
  FQE2E_REQUIRED_STEPS,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-qualified-opportunity-full-e2e-gate-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-portfolio-qualified-opportunities-v1",
);

const HOTEL_NAMES = {
  rec35fExUxCClpOP6: "Hilton New York Times Square",
  rece0NdV9FCnaFlVS: "Condesa DF Hotel",
  recaokE8psIbuVeuH: "LaLuna Boutique Hotels & Villas",
  rec1t35e3sPerzxYu: "Memmo Príncipe Real",
  recLuxvwwxID7U2B8: "Bethesda Marriott",
  recH9h3McgMNFQEla: "Radisson Blu Toronto Downtown",
  recrPQcZg7SFARRb2: "YOTEL Geneva Lake",
  recXh4NgF8sDbi9cQ: "Wyndham Alltra Punta Cana",
  recCAGqkERSXgiku4: "Sheraton Guadalajara Expo",
  rec2PVBDavppGpenm: "AC Hotel A Coruña",
  recIwaP1etgx2g9nA: "Cambridge Beaches Resort & Spa",
  rec82D9zpB8fede1I: "Castillo Hotel Son Vida",
  recGkME49yYuxQl0u: "NOW NOW NOHO",
  recjhQdAUiSyxfCqE: "Sheraton Mallorca Arabella Golf Hotel",
  recFaxTEFF9ILHWC9: "The Westin Grand München",
  recnPB3Q02NO8Ma1o: "Wyndham San Diego Bayside",
  recG66DQJKP2c0UNh: "Renaissance New York Times Square Hotel",
  recgMYovrrZDJMqzX: "Waterstone Resort & Marina Boca Raton",
  recUOyzOXn2Zdp98I: "Radisson Hotel Santo Domingo",
  rece0or38cxo3Fymb: "W Rome",
  recKRJjcPnb4tVDDS: "Spice Island Beach Resort",
  recC2kbCaS48jFjFF: "Nobu Hotel London Portman Square",
  rec0eTEnCqlsGJJt9: "The Mayflower Hotel, Autograph Collection",
  recdmo0CnhIZAwHGC: "The Westin Tampa Waterside",
  recRkGGNKslJptFIL: "Falls Church Marriott Fairview Park",
};

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(file, body) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadResearchFromDir(hotelId) {
  const file = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(file)) return [];
  try {
    const doc = JSON.parse(fs.readFileSync(file, "utf8"));
    return Array.isArray(doc.opportunities)
      ? doc.opportunities
      : Array.isArray(doc)
        ? doc
        : [];
  } catch {
    return [];
  }
}

/** Load research for canonical ID plus any provisional alias directories. */
function loadResearch(hotelId) {
  const dirs = new Set([hotelId]);
  const configRoot = path.join(ROOT, "config/group-demand-intelligence/hotels");
  if (fs.existsSync(configRoot)) {
    for (const f of fs.readdirSync(configRoot)) {
      if (!f.endsWith(".json")) continue;
      try {
        const cfg = JSON.parse(
          fs.readFileSync(path.join(configRoot, f), "utf8"),
        );
        const provisional = f.replace(/\.json$/i, "");
        const canonical = resolveCanonicalHotelId(provisional);
        if (canonical === hotelId) dirs.add(provisional);
      } catch {
        /* ignore */
      }
    }
  }
  const byOpp = new Map();
  for (const dir of dirs) {
    for (const o of loadResearchFromDir(dir)) {
      const id = text(o.id || o.opportunityId);
      if (!id) continue;
      if (!byOpp.has(id)) byOpp.set(id, o);
    }
  }
  return [...byOpp.values()];
}

function packageOpps(store, hotelId, kind) {
  const ptr = store.readPointer(hotelId, kind);
  if (!ptr?.publicationVersion) return { pointer: null, opportunities: [] };
  const loaded = store.loadPackage(hotelId, ptr.publicationVersion);
  if (!loaded?.ok) {
    return { pointer: ptr, opportunities: [], error: loaded?.error || null };
  }
  return {
    pointer: ptr,
    opportunities:
      loaded.package?.opportunities ||
      loaded.package?.payload?.opportunities ||
      [],
  };
}

function listCandidatePackageVersions(store, hotelId) {
  const root = path.join(store.root, hotelId);
  if (!fs.existsSync(root)) return [];
  const out = [];
  for (const name of fs.readdirSync(root)) {
    const dir = path.join(root, name);
    if (!fs.statSync(dir).isDirectory()) continue;
    const pkgFile = path.join(dir, "package.json");
    if (!fs.existsSync(pkgFile)) continue;
    try {
      const pkg = JSON.parse(fs.readFileSync(pkgFile, "utf8"));
      out.push({
        publicationVersion: name,
        opportunityCount: Array.isArray(pkg.opportunities)
          ? pkg.opportunities.length
          : Number(pkg.opportunityCount) || 0,
        lifecycleState: pkg.lifecycleState || null,
      });
    } catch {
      out.push({ publicationVersion: name, opportunityCount: null });
    }
  }
  return out;
}

function marketScope(o) {
  const s = String(
    o.marketScope || o.gdiMarketScope || o.competitiveMarketScope || "",
  ).toUpperCase();
  if (/COMPETITIVE|CMI/.test(s)) return "COMPETITIVE";
  if (
    o.competitiveContext ||
    o.incumbentHotelName ||
    o.incumbentHotel ||
    o.competitorHotel ||
    o.likelyCompetitor ||
    o.competitiveMarketScopeEligible === true
  ) {
    return "COMPETITIVE";
  }
  const relevance = text(
    o.primaryContact?.relevanceEvidence ||
      o.competitiveStatusLabel ||
      o.whyThisHotel ||
      "",
  );
  if (
    /Westin New York at Times Square|CONFIRMED_COMPETITOR|EVIDENCED_OVERFLOW|OVERFLOW_OPPORTUNITY/i.test(
      relevance,
    )
  ) {
    return "COMPETITIVE";
  }
  // Prefer FQO contract competitive detection when available
  try {
    const evaled = evaluateFinalizedQualifiedOpportunity(o);
    if (evaled.competitive === true) return "COMPETITIVE";
  } catch {
    /* ignore */
  }
  return "HOTEL";
}

/** Collapse provisional gdi_hotel_* aliases onto canonical Airtable rec… IDs. */
function resolveCanonicalHotelId(hotelId) {
  const id = text(hotelId);
  if (!id) return id;
  const cfgPath = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${id}.json`,
  );
  if (!fs.existsSync(cfgPath)) return id;
  try {
    const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
    const redirect = text(cfg.redirectTo || cfg.canonicalHotelId || "");
    const resolved = text(cfg.hotelId || "");
    if (redirect.startsWith("rec")) return redirect;
    if (resolved.startsWith("rec") && id.startsWith("gdi_hotel_")) return resolved;
    if (cfg.deprecatedProvisionalId && resolved.startsWith("rec")) return resolved;
  } catch {
    /* keep original */
  }
  return id;
}

function certifyCandidatePackage(pkg) {
  const opps = Array.isArray(pkg?.opportunities) ? pkg.opportunities : [];
  const rows = [];
  let pass = 0;
  let hotel = 0;
  let competitive = 0;
  let named = 0;
  let channels = 0;
  for (const o of opps) {
    const evaled = evaluateFinalizedQualifiedOpportunity(o);
    const scope = marketScope(o);
    const ch = evaluatePersonSpecificOutreachChannel(o);
    const nameOk = Boolean(
      text(o.primaryContact?.name || o.primaryContactName || o.contactName),
    );
    if (nameOk) named += 1;
    if (ch.ok) channels += 1;
    if (evaled.finalizedQualified) {
      pass += 1;
      if (scope === "COMPETITIVE") competitive += 1;
      else hotel += 1;
    }
    rows.push({
      opportunityId: text(o.id || o.opportunityId),
      disposition: dispositionFromEval(evaled),
      reasons: evaled.reasons || [],
      marketScope: scope,
      namedPerson: nameOk,
      personSpecificChannel: ch.ok,
      channels: (ch.channels || []).map((c) => c.kind),
    });
  }
  return {
    opportunityCount: opps.length,
    fqoPassCount: pass,
    hotelCount: hotel,
    competitiveCount: competitive,
    namedPersonCoverage: named,
    personSpecificChannelCoverage: channels,
    allPass: opps.length > 0 && pass === opps.length,
    rows,
  };
}

function dispositionFromEval(evaled) {
  if (evaled.finalizedQualified) return FQE2E_DISPOSITION.PASS;
  if (evaled.status === FQO_STATUS.FOUNDER_HELD) return FQE2E_DISPOSITION.HOLD;
  if (evaled.status === FQO_STATUS.FOUNDER_REJECTED) {
    return FQE2E_DISPOSITION.REJECT;
  }
  if (
    (evaled.reasons || []).some((r) => /HISTORICAL|PAST_ONLY/i.test(String(r)))
  ) {
    return FQE2E_DISPOSITION.HISTORICAL;
  }
  return FQE2E_DISPOSITION.HOLD;
}

function discoverHotelIds(store) {
  const ids = new Set();
  const researchRoot = path.join(ROOT, "data/group-demand-intelligence/hotels");
  if (fs.existsSync(researchRoot)) {
    for (const d of fs.readdirSync(researchRoot, { withFileTypes: true })) {
      if (d.isDirectory()) ids.add(d.name);
    }
  }
  const configRoot = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
  );
  if (fs.existsSync(configRoot)) {
    for (const f of fs.readdirSync(configRoot)) {
      if (!f.endsWith(".json")) continue;
      ids.add(f.replace(/\.json$/i, ""));
    }
  }
  if (store.root && fs.existsSync(store.root)) {
    for (const d of fs.readdirSync(store.root, { withFileTypes: true })) {
      if (d.isDirectory()) ids.add(d.name);
    }
  }
  try {
    for (const h of listGdiSelectableHotels() || []) {
      if (h.hotelId) ids.add(h.hotelId);
    }
  } catch {
    /* ignore */
  }
  // Collapse provisional aliases onto canonical IDs; keep only one matrix row each.
  const collapsed = new Set();
  for (const id of ids) {
    collapsed.add(resolveCanonicalHotelId(id));
  }
  return [...collapsed].filter(Boolean).sort();
}

function main() {
  const store = createGdiPublicationPackageStore();
  const hotelIds = discoverHotelIds(store);
  const selectable = new Map(
    (listGdiSelectableHotels() || []).map((h) => [h.hotelId, h]),
  );

  const matrix = [];
  let totalResearched = 0;
  let totalQualified = 0;
  let totalHotelScope = 0;
  let totalCompetitive = 0;
  let missingNamed = 0;
  let missingChannel = 0;
  let historicalOrRejected = 0;
  const candidateActivationBatch = [];
  const paidResearchPlan = [];
  const fullE2eStatus = [];

  for (const hotelId of hotelIds) {
    if (hotelId === "recTEST") continue;
    const research = loadResearch(hotelId);
    const staged = packageOpps(store, hotelId, "STAGED");
    const current = packageOpps(store, hotelId, "CURRENT");
    const candidates = listCandidatePackageVersions(store, hotelId);
    const sel = selectable.get(hotelId);
    const hotelName =
      HOTEL_NAMES[hotelId] ||
      sel?.displayName ||
      sel?.hotelName ||
      hotelId;

    const dispositions = [];
    let qualified = 0;
    let verifiedContacts = 0;
    let hotelScope = 0;
    let competitiveScope = 0;
    const gaps = [];

    for (const o of research) {
      const id = String(o.id || o.opportunityId || "").trim();
      if (!id) continue;
      totalResearched += 1;
      const evaled = evaluateFinalizedQualifiedOpportunity(o);
      const disp = dispositionFromEval(evaled);
      dispositions.push({
        opportunityId: id,
        disposition: disp,
        reasons: evaled.reasons || [],
        marketScope: marketScope(o),
      });
      if (disp === FQE2E_DISPOSITION.PASS) {
        qualified += 1;
        totalQualified += 1;
        if (marketScope(o) === "COMPETITIVE") {
          competitiveScope += 1;
          totalCompetitive += 1;
        } else {
          hotelScope += 1;
          totalHotelScope += 1;
        }
      } else if (
        disp === FQE2E_DISPOSITION.HISTORICAL ||
        disp === FQE2E_DISPOSITION.REJECT
      ) {
        historicalOrRejected += 1;
      }
      const ch = evaluatePersonSpecificOutreachChannel(o);
      if (ch.ok) verifiedContacts += 1;
      else {
        if ((evaled.reasons || []).some((r) => /NAMED_COMMERCIAL_PERSON/i.test(r))) {
          missingNamed += 1;
        }
        if (
          (evaled.reasons || []).some((r) => /PERSON_SPECIFIC_CHANNEL/i.test(r))
        ) {
          missingChannel += 1;
        }
      }
    }

    // Candidate STAGED: only real versioned packages. Never invent PROPOSED_*
    // placeholders. Certify each package under hardened FQO before listing as
    // activatable. Funnel PASS counts remain informational only.
    const nonPointerCandidates = candidates.filter(
      (c) =>
        c.publicationVersion &&
        c.publicationVersion !== staged.pointer?.publicationVersion &&
        c.publicationVersion !== current.pointer?.publicationVersion,
    );
    const certifiedCandidates = [];
    for (const c of nonPointerCandidates) {
      const loaded = store.loadPackage(hotelId, c.publicationVersion);
      if (!loaded?.ok || !loaded.package) continue;
      const cert = certifyCandidatePackage(loaded.package);
      certifiedCandidates.push({
        publicationVersion: c.publicationVersion,
        publicationId: loaded.package.publicationId || null,
        opportunityCount: cert.opportunityCount,
        fqoPassCount: cert.fqoPassCount,
        hotelCount: cert.hotelCount,
        competitiveCount: cert.competitiveCount,
        namedPersonCoverage: cert.namedPersonCoverage,
        personSpecificChannelCoverage: cert.personSpecificChannelCoverage,
        certificationResult: cert.allPass
          ? "FQO_PACKAGE_CERTIFIED"
          : "FQO_PACKAGE_INCOMPLETE",
        pointerActivated: false,
        activatable: cert.allPass === true,
        rows: cert.rows,
      });
    }
    const candidateStagedLabel = certifiedCandidates.length
      ? certifiedCandidates
          .map(
            (c) =>
              `${c.publicationVersion} (${c.fqoPassCount}/${c.opportunityCount} FQO ${c.certificationResult})`,
          )
          .join("; ")
      : "—";

    for (const c of certifiedCandidates.filter((x) => x.activatable)) {
      candidateActivationBatch.push({
        hotelId,
        hotelName,
        certifiedCandidatePackageVersion: c.publicationVersion,
        publicationId: c.publicationId,
        verifiedQualifiedCount: c.fqoPassCount,
        hotelCount: c.hotelCount,
        competitiveCount: c.competitiveCount,
        namedPersonCoverage: c.namedPersonCoverage,
        personSpecificChannelCoverage: c.personSpecificChannelCoverage,
        certificationResult: c.certificationResult,
        existingStagedVersion: staged.pointer?.publicationVersion || null,
        existingCurrentVersion: current.pointer?.publicationVersion || null,
        proposedAdditionsVsStaged: staged.pointer
          ? "REVIEW_DIFF_REQUIRED"
          : "NEW_STAGED_CANDIDATE",
        proposedRemovalsVsStaged: staged.pointer
          ? "REVIEW_DIFF_REQUIRED"
          : "NONE",
        rollbackReference: staged.pointer?.publicationVersion || null,
        note: "Requires founder approval before STAGED pointer activation — not activated",
      });
    }

    if (research.length > 0 && qualified < research.length) {
      const hold = dispositions.filter(
        (d) => d.disposition === FQE2E_DISPOSITION.HOLD,
      ).length;
      if (hold > 0) {
        paidResearchPlan.push({
          hotelId,
          hotelName,
          holdCount: hold,
          note: "Hotel-level paid research budget required before execution — not authorized",
          priorHilton16AuthDoesNotApply: true,
        });
      }
    }

    const remainingGaps = [];
    if (!sel) remainingGaps.push("NOT_IN_SELECTABLE_CATALOG");
    if (research.length === 0 && !staged.pointer && !current.pointer) {
      remainingGaps.push("NO_RESEARCH_OR_PUBLICATION");
    }
    if (qualified === 0 && research.length > 0) {
      remainingGaps.push("NO_FQO_PASS_YET");
    }
    if (staged.pointer && staged.opportunities.length > 0) {
      const stagedFail = staged.opportunities.filter(
        (o) => !evaluateFinalizedQualifiedOpportunity(o).finalizedQualified,
      ).length;
      if (stagedFail > 0) {
        remainingGaps.push(
          `EXISTING_STAGED_BELOW_FQO_STANDARD:${stagedFail}`,
        );
      }
    }

    const e2e = evaluatePortfolioQualifiedOpportunityFullE2eGate({
      hotelId,
      opportunityIds: dispositions.map((d) => d.opportunityId),
      dispositions,
      stepsCompleted:
        dispositions.length > 0
          ? [
              "LOAD_HISTORICAL_AND_CURRENT_INVENTORY",
              "RECONCILE_ORIGINAL_EXISTING_AND_NEW",
              "REVALIDATE_EVERY_OPPORTUNITY",
              "APPLY_SIX_QUALIFICATION_GATES",
              "COMPARE_AGAINST_EXISTING_STAGED_AND_CURRENT",
              "RECORD_PASS_HOLD_REJECT_HISTORICAL",
            ]
          : [],
      stagedCandidatePrepared: qualified > 0 || nonPointerCandidates.length > 0,
      stagedPointerActivated: false,
      paidResearchExecutedWithoutAuth: false,
    });
    fullE2eStatus.push({
      hotelId,
      hotelName,
      complete: e2e.complete,
      incompleteReason: e2e.incompleteReason,
      counts: e2e.counts,
      missingSteps: e2e.missingSteps,
    });

    matrix.push({
      hotel: hotelName,
      hotelId,
      inSelectableCatalog: Boolean(sel),
      totalResearched: research.length,
      qualified,
      verifiedContacts,
      hotelScopeQualified: hotelScope,
      competitiveScopeQualified: competitiveScope,
      candidateStaged: candidateStagedLabel,
      activeStaged: staged.pointer
        ? `${staged.pointer.publicationVersion} (${staged.opportunities.length})`
        : "—",
      activeCurrent: current.pointer
        ? `${current.pointer.publicationVersion} (${current.opportunities.length})`
        : "—",
      remainingGaps: remainingGaps.length ? remainingGaps.join("; ") : "—",
      fullE2eComplete: e2e.complete,
    });

    // Per-hotel disposition ledger (auditable)
    writeJson(path.join(OUT, "hotels", `${hotelId}.dispositions.json`), {
      hotelId,
      hotelName,
      asOf: new Date().toISOString(),
      dispositions,
      gate: e2e,
    });
  }

  const report = {
    contract: "gdi-portfolio-qualified-opportunities-reconcile-v1",
    asOf: new Date().toISOString(),
    discovery: {
      totalHotelsDiscoveredAcrossSources: matrix.length,
      selectableCatalogCount: selectable.size,
      hotelsWithResearch: matrix.filter((r) => r.totalResearched > 0).length,
      hotelsWithActiveStaged: matrix.filter((r) => r.activeStaged !== "—")
        .length,
      hotelsWithActiveCurrent: matrix.filter((r) => r.activeCurrent !== "—")
        .length,
    },
    totals: {
      totalResearched,
      totalQualifiedOpportunities: totalQualified,
      hotelVersusCompetitive: {
        hotel: totalHotelScope,
        competitive: totalCompetitive,
      },
      missingNamedContactsApprox: missingNamed,
      missingVerifiedPersonSpecificChannelsApprox: missingChannel,
      historicalAndRejectedApprox: historicalOrRejected,
      hotelsWithCompletedFullE2eReconciliation: fullE2eStatus.filter(
        (h) => h.complete,
      ).length,
      newlyGeneratedCandidatePackagesProposed: candidateActivationBatch.length,
    },
    proposedStagedActivationBatch: candidateActivationBatch,
    proposedPaidResearchRequirements: paidResearchPlan,
    permanentFullE2eRequiredSteps: FQE2E_REQUIRED_STEPS,
    governance: {
      researchViewPrs590_591: "CLOSED_SUPERSEDED",
      integrateOrder: ["#593", "#592"],
      noStagedPointerWrites: true,
      noCurrentPointerWrites: true,
      noAirtable: true,
      noPaidResearchExecuted: true,
      stopBeforeStagedActivation: true,
    },
    matrix,
    fullE2eStatus,
  };

  writeJson(path.join(OUT, "PORTFOLIO_MATRIX.json"), report);
  writeJson(path.join(OUT, "PROPOSED_STAGED_ACTIVATION_BATCH.json"), {
    ok: true,
    activated: false,
    requiresFounderApproval: true,
    batch: candidateActivationBatch,
  });
  writeJson(path.join(OUT, "PROPOSED_PAID_RESEARCH_PLAN.json"), {
    ok: true,
    authorizedSpendUsd: 0,
    note: "Prior Hilton $16 authorization does not apply to other hotels.",
    hotels: paidResearchPlan,
  });

  const md = [];
  md.push("# Portfolio-wide GDI Qualified Opportunities — Reconciliation");
  md.push("");
  md.push(`**As of:** ${report.asOf}`);
  md.push("**STAGED activation:** NOT performed (founder approval required)");
  md.push("**Paid research:** none executed");
  md.push("");
  md.push("## Discovery");
  md.push(`- Hotels discovered across all sources: **${report.discovery.totalHotelsDiscoveredAcrossSources}**`);
  md.push(`- Selectable catalog: **${report.discovery.selectableCatalogCount}**`);
  md.push(`- Hotels with research inventory: **${report.discovery.hotelsWithResearch}**`);
  md.push(`- Hotels with active STAGED: **${report.discovery.hotelsWithActiveStaged}**`);
  md.push(`- Hotels with active CURRENT: **${report.discovery.hotelsWithActiveCurrent}**`);
  md.push("");
  md.push("## Totals");
  md.push(`- Total researched opportunities: **${totalResearched}**`);
  md.push(`- Qualified (FQO PASS funnel): **${totalQualified}**`);
  md.push(`- Hotel / Competitive: **${totalHotelScope}** / **${totalCompetitive}**`);
  md.push(`- Full E2E complete (all steps + dispositions): **${report.totals.hotelsWithCompletedFullE2eReconciliation}**`);
  md.push("");
  md.push("| Hotel | Total researched | Qualified | Verified contacts | Candidate STAGED | Active STAGED | Remaining gaps |");
  md.push("|---|---:|---:|---:|---|---|---|");
  for (const r of matrix) {
    md.push(
      `| ${r.hotel} | ${r.totalResearched} | ${r.qualified} | ${r.verifiedContacts} | ${String(r.candidateStaged).replace(/\|/g, "/")} | ${String(r.activeStaged).replace(/\|/g, "/")} | ${String(r.remainingGaps).replace(/\|/g, "/")} |`,
    );
  }
  md.push("");
  md.push("## Integration");
  md.push("- #590 / #591 Research View: **CLOSED — superseded**");
  md.push("- IC merge order: **#593 → #592**");
  md.push("- localhost:8080 must be refreshed to #593+ tip after merge (was serving inventory-visibility-r1)");
  md.push("");
  md.push("## STOP");
  md.push("Founder approval required before consolidating/activating any STAGED batch.");
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), `${md.join("\n")}\n`, "utf8");

  console.log(
    JSON.stringify(
      {
        hotels: matrix.length,
        selectable: selectable.size,
        totalResearched,
        totalQualified,
        candidateBatch: candidateActivationBatch.length,
        fullE2eComplete: report.totals.hotelsWithCompletedFullE2eReconciliation,
        out: OUT,
      },
      null,
      2,
    ),
  );
}

main();
