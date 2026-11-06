/**
 * E2D buyer-resolution + commercial disposition + LBS publication path.
 * Reusable Full E2E stage — hotelId selectable; Nobu is the live canary.
 *
 * Usage:
 *   node scripts/gdi-e2d-buyer-resolution-disposition-v1.mjs --hotel recC2kbCaS48jFjFF --dry-run
 *   node scripts/gdi-e2d-buyer-resolution-disposition-v1.mjs --hotel recC2kbCaS48jFjFF --apply
 *   node scripts/gdi-e2d-buyer-resolution-disposition-v1.mjs --hotel recC2kbCaS48jFjFF --apply --authorize-publication
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyCommercialDisposition,
  diagnoseConversionBlocker,
  assertInferredDemandCopyIntegrity,
  sanitizeInferredDemandCopy,
  COMMERCIAL_DISPOSITION,
  DEMAND_EVIDENCE_LABEL,
} from "../lib/group-demand-intelligence/research/event-to-demand/event-commercial-disposition-v1.js";
import {
  resolveBuyerForProspect,
  CONTACT_EVIDENCE_TIER,
} from "../lib/group-demand-intelligence/research/event-to-demand/event-buyer-resolution-v1.js";
import {
  evaluateProspectPublicationEligibility,
  authorizeProspectPublicationPromotion,
  gradeTravelNeedEvidence,
  REVIEWED_PUBLICATION_BASIS,
} from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  classifyCustomerSurfaceOpportunity,
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import {
  persistPromotedOpportunityCanonical,
  verifyCanonicalCustomerVisibility,
} from "../lib/group-demand-intelligence/research/maturation-canonical-persist-v1.js";
import { getGdiOpportunityPersistenceMode } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { EVENT_DEMAND_CLASS } from "../lib/group-demand-intelligence/research/event-to-demand/event-demand-class-v1.js";
import { normalizeOrgIdentityKey } from "../lib/group-demand-intelligence/workflow/reuse-prior-intelligence-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const args = process.argv.slice(2);
const hotelId =
  args.find((a, i) => args[i - 1] === "--hotel") || "recC2kbCaS48jFjFF";
const apply = args.includes("--apply");
const authorizePublication = args.includes("--authorize-publication");

const OUT = path.join(
  ROOT,
  "reports/gdi-opportunity-research-supervisor-v1/e2d-buyer-resolution-disposition-v1"
);

function memDir() {
  return path.join(
    ROOT,
    "reports/gdi-opportunity-research-supervisor-v1",
    `hotel-orchestrator-${hotelId}`,
    "research-memory"
  );
}

function loadSummaryBuyers() {
  const p = path.join(
    ROOT,
    "reports/gdi-opportunity-research-supervisor-v1/e2d-demand-ownership-buyer-v1/summary.json"
  );
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return Array.isArray(j.uniqueBuyers) ? j.uniqueBuyers : [];
}

function loadOpp(id) {
  const p = path.join(memDir(), `${id}.json`);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function stripJunkPhone(contact = {}) {
  const next = { ...contact };
  const phone = String(next.businessPhone || "");
  const digits = phone.replace(/\D/g, "");
  if (
    /^(?:0123456789|1234567890|9876543210)/.test(digits) ||
    /^(\d)\1{9,}$/.test(digits) ||
    digits.length < 10
  ) {
    delete next.businessPhone;
  }
  // Venue contact pages must not be attributed as the org sales form
  if (
    /excel\.london\/contact/i.test(String(next.contactFormUrl || "")) &&
    next.businessEmail &&
    !/@excel\.london$/i.test(next.businessEmail)
  ) {
    delete next.contactFormUrl;
  }
  // Venue calendars / what's-on listings are research evidence, not sales desks
  for (const key of ["contactFormUrl", "contactUrl", "sourceUrl"]) {
    if (/\/(whats-on|whats_on|calendar|fairs?\/)/i.test(String(next[key] || ""))) {
      delete next[key];
    }
  }
  return next;
}

function countBy(rows, keyFn) {
  const out = {};
  for (const r of rows) {
    const k = keyFn(r) || "UNKNOWN";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

async function maybePublishLbs(opp, outDir) {
  if (opp?.id !== "gdi_e2d_524c5e284f6afebd") {
    return { attempted: false };
  }
  const elig = evaluateProspectPublicationEligibility(opp);
  const ready = isGdiCustomerOpportunityReady(opp);
  const packageDir = path.join(outDir, "persist-packages");
  fs.mkdirSync(packageDir, { recursive: true });
  const replayPath = path.join(packageDir, `${opp.id}.publication-package.json`);

  if (!elig.hotelFacing) {
    const pkg = {
      at: new Date().toISOString(),
      hotelId,
      opportunityId: opp.id,
      status: "BLOCKED_NOT_HOTEL_FACING",
      eligibility: elig,
      ready,
      persistenceMode: getGdiOpportunityPersistenceMode(),
    };
    fs.writeFileSync(replayPath, JSON.stringify(pkg, null, 2) + "\n");
    return { attempted: true, ok: false, status: pkg.status, replayPath, eligibility: elig, ready };
  }

  if (!ready?.ok) {
    const pkg = {
      at: new Date().toISOString(),
      hotelId,
      opportunityId: opp.id,
      status: "BLOCKED_NOT_READY",
      eligibility: elig,
      ready,
      note:
        "HOTEL_FACING alone is not publication authorization. Ready must pass before canonical write. Replayable package preserved.",
      persistenceMode: getGdiOpportunityPersistenceMode(),
    };
    fs.writeFileSync(replayPath, JSON.stringify(pkg, null, 2) + "\n");
    return { attempted: true, ok: false, status: pkg.status, replayPath, eligibility: elig, ready };
  }

  if (!authorizePublication) {
    const pkg = {
      at: new Date().toISOString(),
      hotelId,
      opportunityId: opp.id,
      status: "AUTHORIZE_PUBLICATION_REQUIRED",
      eligibility: elig,
      ready,
      note:
        "Replayable package preserved. Pass --authorize-publication to run canonical writer. Research-memory ≠ production publication.",
      persistenceMode: getGdiOpportunityPersistenceMode(),
      patchedPreview: {
        id: opp.id,
        organizationName: opp.organizationName,
        prospectHotelFacing: true,
        eventDemandClass: opp.eventDemandClass || EVENT_DEMAND_CLASS.INFERRED,
        commercialDisposition: opp.commercialDisposition,
      },
    };
    fs.writeFileSync(replayPath, JSON.stringify(pkg, null, 2) + "\n");
    return {
      attempted: true,
      ok: false,
      status: pkg.status,
      replayPath,
      eligibility: elig,
      ready,
      persistenceMode: pkg.persistenceMode,
    };
  }

  const promoted = authorizeProspectPublicationPromotion(opp, {
    authorizePublication: true,
    reason: "e2d_buyer_resolution_disposition_v1_lbs",
  });
  if (!promoted.ok) {
    const pkg = {
      at: new Date().toISOString(),
      hotelId,
      opportunityId: opp.id,
      status: "PROMOTION_REFUSED",
      reason: promoted.reason,
      eligibility: promoted.eligibility || elig,
      ready,
    };
    fs.writeFileSync(replayPath, JSON.stringify(pkg, null, 2) + "\n");
    return { attempted: true, ok: false, status: pkg.status, reason: promoted.reason, replayPath };
  }

  const persist = await persistPromotedOpportunityCanonical({
    hotelId,
    patched: {
      ...promoted.patched,
      prospectPublicationBasis:
        promoted.patched.prospectPublicationBasis || REVIEWED_PUBLICATION_BASIS,
    },
    outDir,
    authorizePublication: true,
  });

  const visibility = await verifyCanonicalCustomerVisibility(hotelId, opp.id);
  const pkg = {
    at: new Date().toISOString(),
    hotelId,
    opportunityId: opp.id,
    status: persist.status,
    persist,
    visibility,
    eligibility: elig,
    ready,
    persistenceMode: getGdiOpportunityPersistenceMode(),
    airtableRecordId: persist.airtableRecordId || visibility.airtableRecordId || null,
    note:
      getGdiOpportunityPersistenceMode() === "filesystem"
        ? "Filesystem persistence only — do not claim Airtable production publication"
        : persist.airtableRecordId
          ? "Airtable record id from adapter"
          : "Airtable mode without record id — verify adapter configuration",
  };
  fs.writeFileSync(replayPath, JSON.stringify(pkg, null, 2) + "\n");
  return {
    attempted: true,
    ok: persist.ok === true,
    status: persist.status,
    replayPath,
    persist,
    visibility,
    eligibility: elig,
    ready,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const buyers = loadSummaryBuyers();
  if (!buyers.length) {
    throw new Error("No uniqueBuyers in demand-ownership summary — run buyer discovery first");
  }

  const rows = [];
  let lbsPublication = null;

  for (const buyer of buyers) {
    const oppId = buyer.sourceProspectId;
    let opp = loadOpp(oppId);
    if (!opp) {
      rows.push({
        organizationName: buyer.organizationName,
        sourceProspectId: oppId,
        error: "research_memory_missing",
      });
      continue;
    }

    // Sanitize nested contact junk before disposition / eligibility
    if (opp.contact) {
      opp = { ...opp, contact: stripJunkPhone(opp.contact) };
    }
    // Secondary demand-owners must not inherit another org's contact routes
    // (e.g. Frieze EXHIBITOR row pointing at Voloshyn Gallery prospect id).
    const buyerKey = normalizeOrgIdentityKey(buyer.organizationName || "");
    const originalOppOrgName = opp.organizationName;
    const oppKey = normalizeOrgIdentityKey(originalOppOrgName || "");
    const isSecondaryOwner = Boolean(buyerKey && oppKey && buyerKey !== oppKey);
    if (isSecondaryOwner) {
      const email = String(opp.contact?.businessEmail || "").toLowerCase();
      const domain = email.includes("@") ? email.split("@")[1] : "";
      // Allow shared estate/domain routes when the email clearly belongs to the
      // secondary owner (e.g. resi@hdwe.co.uk → Howard de Walden Estate).
      const domainMatchesBuyer =
        domain &&
        ((/hdwe\.co\.uk/i.test(domain) && /howard de walden/i.test(buyer.organizationName || "")) ||
          (domain.split(".")[0] &&
            buyerKey.includes(domain.split(".")[0].replace(/[^a-z0-9]/g, ""))));
      opp = {
        ...opp,
        organizationName: buyer.organizationName,
        organizationIdentityKey: buyerKey,
        contact: domainMatchesBuyer ? stripJunkPhone(opp.contact || {}) : {},
        contactEmail: domainMatchesBuyer ? email : undefined,
        contactPhone: undefined,
        contactUrl: undefined,
        primaryContactEmail: domainMatchesBuyer ? email : undefined,
        primaryContactPhone: undefined,
        notes: [
          ...(opp.notes || []),
          domainMatchesBuyer
            ? `e2d_buyer_resolution_v1:shared_domain_route_kept_for_${buyerKey}`
            : `e2d_buyer_resolution_v1:contact_not_inherited_from_${oppKey}`,
        ],
      };
    }
    if (!opp.eventDemandClass) {
      opp = {
        ...opp,
        eventDemandClass: EVENT_DEMAND_CLASS.INFERRED,
      };
    }

    const disposition = classifyCommercialDisposition({
      opportunity: opp,
      demandOwner: buyer,
      buyer,
    });
    // Stamp disposition onto the bag before eligibility so partnership ≠ lodging-facing.
    opp = {
      ...opp,
      commercialDisposition: disposition.disposition,
      demandEvidenceLabel: disposition.demandEvidenceLabel,
      eventTerritoryClass:
        opp.eventTerritoryClass || opp.hotelFit?.territory || undefined,
    };
    const resolution = resolveBuyerForProspect({
      opportunity: opp,
      demandOwner: buyer,
      disposition,
      researchExhausted: false,
      paidJustified: false,
    });
    const eligibility = evaluateProspectPublicationEligibility(opp);
    const readyEval = isGdiCustomerOpportunityReady(opp);
    const blocker = diagnoseConversionBlocker({
      opportunity: opp,
      eligibility: {
        ...eligibility,
        usableContactRoute: resolution.contactEvidence.usableRoute,
      },
      disposition,
      contactEvidence: resolution.contactEvidence,
    });

    const travelGrade = eligibility.travelNeedGrade || gradeTravelNeedEvidence(opp);
    const surface = classifyCustomerSurfaceOpportunity(opp, {
      nowDate: new Date().toISOString().slice(0, 10),
    });
    const presentationBlob = [
      opp.summaryWhat,
      opp.summaryWhyMatters,
      opp.summaryWhyHotel,
      opp.whyNow,
      opp.recommendedAction,
      opp.hotelFitRationale,
      opp.localConnection,
    ].join(" ");
    const copyCheck = assertInferredDemandCopyIntegrity({
      textBlob: presentationBlob,
      demandEvidenceLabel: disposition.demandEvidenceLabel,
      confirmedBuying: opp.confirmedBuying === true,
    });

    const patched = {
      ...opp,
      commercialDisposition: disposition.disposition,
      demandEvidenceLabel: disposition.demandEvidenceLabel,
      commercialDispositionMeta: disposition,
      buyerResolution: resolution,
      conversionDiagnosis: blocker,
      travelNeedGrade: travelGrade,
      customerSurfaceDisposition: surface.disposition,
      customerSurfaceReasons: surface.reasons,
      eventDemandClass: opp.eventDemandClass,
      summaryWhat: sanitizeInferredDemandCopy(
        opp.summaryWhat || "",
        disposition.demandEvidenceLabel
      ),
      summaryWhyMatters: sanitizeInferredDemandCopy(
        opp.summaryWhyMatters || "",
        disposition.demandEvidenceLabel
      ),
      whyNow: sanitizeInferredDemandCopy(opp.whyNow || "", disposition.demandEvidenceLabel),
      recommendedAction: sanitizeInferredDemandCopy(
        opp.recommendedAction || "",
        disposition.demandEvidenceLabel
      ),
      inferredCopyIntegrityOk: copyCheck.ok,
      notes: [
        ...(opp.notes || []).filter(
          (n) =>
            !/^e2d_buyer_resolution_disposition_v1/.test(n) &&
            !/^e2d_cross_gate_evidence_reconciliation_v1/.test(n)
        ),
        "e2d_buyer_resolution_disposition_v1",
        "e2d_cross_gate_evidence_reconciliation_v1",
      ],
    };

    // Only persist onto research-memory when the buyer org owns the prospect bag.
    // Never overwrite another organization's canonical research-memory file.
    if (apply && !isSecondaryOwner) {
      fs.writeFileSync(
        path.join(memDir(), `${oppId}.json`),
        JSON.stringify(patched, null, 2) + "\n"
      );
    } else if (apply && isSecondaryOwner) {
      const sideDir = path.join(OUT, "secondary-owners");
      fs.mkdirSync(sideDir, { recursive: true });
      fs.writeFileSync(
        path.join(
          sideDir,
          `${buyer.demandOwnerId || buyerKey.replace(/\s+/g, "_")}.json`
        ),
        JSON.stringify(
          {
            at: new Date().toISOString(),
            hotelId,
            demandOwner: buyer,
            sourceProspectId: oppId,
            sourceProspectOrg: originalOppOrgName,
            diagnosis: {
              disposition,
              resolution,
              blocker,
              eligibility: eligibility.eligibility,
            },
            note: "Secondary demand-owner — not written over source prospect research-memory",
          },
          null,
          2
        ) + "\n"
      );
    }

    if (opp.id === "gdi_e2d_524c5e284f6afebd" && buyer.organizationName === "London Business School") {
      lbsPublication = await maybePublishLbs(patched, OUT);
    }

    rows.push({
      organizationName: buyer.organizationName,
      sourceProspectId: oppId,
      underlyingEvent: buyer.eventName || opp.eventName,
      demandOwnerRole: buyer.role,
      demandMechanism: buyer.mechanism || opp.eventDemandMechanism,
      demandEvidenceLabel: disposition.demandEvidenceLabel,
      commercialClassification: disposition.disposition,
      verifiedDemandMechanism: travelGrade.ok
        ? `${disposition.demandEvidenceLabel}:${travelGrade.grade}`
        : `UNSUPPORTED_OR_SPECULATIVE:${travelGrade.grade}`,
      evidenceStatus:
        disposition.demandEvidenceLabel === DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING
          ? "VERIFIED"
          : travelGrade.ok
            ? "INFERRED"
            : "UNSUPPORTED",
      namedPerson: resolution.namedPerson || "explicitly_unknown",
      buyerFunction: resolution.buyerFunction,
      contactRoute: {
        tier: resolution.contactEvidence.tier,
        quality: resolution.contactRouteQuality,
        usable: resolution.usableSalesRoute,
        reason: resolution.contactEvidence.routeReason,
        email: resolution.contactEvidence.surface?.email || null,
        url: resolution.contactEvidence.surface?.contactUrl || null,
      },
      primaryBlocker: blocker.primaryBlocker,
      blockerCategory: blocker.category,
      researchCanResolve: blocker.researchCanResolve,
      eligibility: eligibility.eligibility,
      hotelFacing: eligibility.hotelFacing === true,
      eligibilityReasons: eligibility.reasons || [],
      travelNeedGrade: travelGrade,
      surfaceDisposition: surface.disposition,
      surfaceReasons: surface.reasons || [],
      ready: readyEval?.ok === true,
      readyDetail:
        typeof readyEval === "object"
          ? { ok: readyEval.ok, failed: readyEval.failed, state: readyEval.state }
          : readyEval,
      publicationDestination:
        disposition.disposition === COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP
          ? "private_events_or_referral_partnership_path"
          : eligibility.hotelFacing
            ? "gdi_customer_opportunity_airtable_after_ready"
            : "enrichment_queue_or_market_intelligence",
      nextAction: resolution.resolutionBlocker?.nextAction || blocker.detail,
      dispositionReasons: disposition.reasons || [],
      inferredCopyIntegrityOk: copyCheck.ok,
    });
  }

  const inflatedFacingBaseline = 9; // pre-audit HOTEL_FACING after mapping corrections
  const afterFacing = rows.filter((r) => r.hotelFacing).length;
  const metrics = {
    hotelId,
    at: new Date().toISOString(),
    apply,
    authorizePublication,
    persistenceMode: getGdiOpportunityPersistenceMode(),
    cohortBuyers: rows.length,
    dispositions: countBy(rows, (r) => r.commercialClassification),
    demandEvidenceLabels: countBy(rows, (r) => r.demandEvidenceLabel),
    contactTiers: countBy(rows, (r) => r.contactRoute?.tier),
    hotelFacingBeforeAudit: inflatedFacingBaseline,
    hotelFacingAfterAudit: afterFacing,
    hotelFacingBefore: inflatedFacingBaseline,
    hotelFacingAfter: afterFacing,
    genuineHotelSalesProspects: rows.filter(
      (r) => r.commercialClassification === COMMERCIAL_DISPOSITION.HOTEL_SALES_PROSPECT
    ).length,
    strategicPartnerships: rows.filter(
      (r) => r.commercialClassification === COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP
    ).length,
    inferredDemand: rows.filter(
      (r) => r.demandEvidenceLabel === DEMAND_EVIDENCE_LABEL.INFERRED_DEMAND
    ).length,
    confirmedDemand: rows.filter(
      (r) => r.demandEvidenceLabel === DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING
    ).length,
    passingReady: rows.filter((r) => r.ready).length,
    airtablePersisted: lbsPublication?.persist?.airtableRecordId ? 1 : 0,
    productionCustomerVisible:
      lbsPublication?.persist?.productionPublished === true &&
      lbsPublication?.persist?.customerVisible === true
        ? 1
        : 0,
    namedRoleRelevantPeople: rows.filter(
      (r) =>
        r.contactRoute?.tier === CONTACT_EVIDENCE_TIER.VERIFIED_NAMED_DECISION_MAKER ||
        r.contactRoute?.tier === CONTACT_EVIDENCE_TIER.VERIFIED_ROLE_RELEVANT_PERSON
    ).length,
    verifiedUsableContactRoutes: rows.filter((r) => r.contactRoute?.usable).length,
    lbsPublication,
    paidUsedUsd: 0,
    fullE2e: "PARTIAL",
    note:
      "Evidence audit demotes partnership + stretch spillover + listing contacts from lodging HOTEL_FACING. Ready ≠ eligibility. Filesystem ≠ Airtable. Full E2E PARTIAL.",
  };

  const summary = { metrics, rows };
  fs.writeFileSync(path.join(OUT, "summary.json"), JSON.stringify(summary, null, 2) + "\n");

  const md = [
    `# E2D Buyer Resolution & Commercial Disposition`,
    ``,
    `**Hotel:** \`${hotelId}\``,
    `**Generated:** ${metrics.at}`,
    `**Mode:** ${apply ? "apply" : "dry-run"}${authorizePublication ? " + authorize-publication" : ""}`,
    `**Persistence:** ${metrics.persistenceMode}`,
    `**Full E2E:** PARTIAL`,
    `**Incremental paid:** $0`,
    ``,
    `## Diagnosis: why 7 routes → 1 eligible (baseline)`,
    ``,
    `1. **Nested contact mapping** — research-memory stores routes under \`contact.*\`; eligibility previously ignored them → false \`no_usable_route\` (Marylebone Village email).`,
    `2. **Travel-need field gap** — dated-lodging suitability omitted \`localConnection\` / \`hotelFitRationale\` where E2D overnight language lives → false \`travel_need_evidence\` (Tech Week, Climate, PopCultr, BodyPower).`,
    `3. **Partnership misread as lodging buyer** — Marylebone Village / Howard de Walden are place-management / estate partnership routes, not accommodation buyers.`,
    `4. **Publication path** — LBS was HOTEL_FACING but never ran authorize → reviewed package → canonical writer → read-back.`,
    ``,
    `## Disposition counts`,
    ``,
    `| Disposition | Count |`,
    `|---|---:|`,
    ...Object.entries(metrics.dispositions).map(([k, v]) => `| ${k} | ${v} |`),
    ``,
    `| Metric | Before audit | After audit |`,
    `|---|---:|---:|`,
    `| Hotel-facing eligible | ${metrics.hotelFacingBeforeAudit} | ${metrics.hotelFacingAfterAudit} |`,
    `| Usable contact routes | — | ${metrics.verifiedUsableContactRoutes} |`,
    `| Named role-relevant people | 0 | ${metrics.namedRoleRelevantPeople} |`,
    `| Passing Ready | — | ${metrics.passingReady} |`,
    `| Airtable persisted | — | ${metrics.airtablePersisted} |`,
    ``,
    `## Per-buyer reconciliation`,
    ``,
    `| Organization | Event | Mechanism | Classification | Named person | Buyer function | Contact tier | Blocker | Eligibility | Ready | Next action |`,
    `|---|---|---|---|---|---|---|---|---|---|---|`,
    ...rows.map((r) =>
      `| ${r.organizationName} | ${r.underlyingEvent || ""} | ${r.demandMechanism || ""} | ${r.commercialClassification} | ${r.namedPerson} | ${r.buyerFunction} | ${r.contactRoute?.tier || ""} | ${r.primaryBlocker} | ${r.eligibility} | ${r.ready} | ${(r.nextAction || "").replace(/\|/g, "/")} |`
    ),
    ``,
    `## London Business School publication`,
    ``,
    lbsPublication
      ? [
          `- Attempted: yes`,
          `- Status: \`${lbsPublication.status}\``,
          `- Eligibility: \`${lbsPublication.eligibility?.eligibility || ""}\``,
          `- Ready: \`${JSON.stringify(lbsPublication.ready)}\``,
          `- Replay package: \`${lbsPublication.replayPath || ""}\``,
          `- Persistence mode: \`${metrics.persistenceMode}\``,
          `- Customer visibility: \`${JSON.stringify(lbsPublication.visibility || lbsPublication.persist?.customerVisible || false)}\``,
          `- Airtable record id: \`${lbsPublication.persist?.airtableRecordId || lbsPublication.visibility?.airtableRecordId || "none — do not invent"}\``,
          `- Production published: \`${lbsPublication.persist?.productionPublished === true}\``,
        ].join("\n")
      : `- Not attempted`,
    ``,
    `## Rule changes (evidence integrity)`,
    ``,
    `### Partnership ≠ lodging HOTEL_FACING`,
    `- Place-management / estate (Marylebone Village, Howard de Walden) → \`partnership_not_lodging_sales\`.`,
    `- Private Events / referral path remains; do not invent room demand.`,
    ``,
    `### Listing URLs / stretch spillover`,
    `- \`/whats-on/\` and fair calendars rejected as functional contacts.`,
    `- \`TERRITORY_STRETCH\` + spillover-only prose → \`SPECULATIVE_SPILLOVER\` (not travel-need).`,
    ``,
    `### Eligibility ↔ Ready surface contract`,
    `- Customer-surface hotel-motion reads the same E2D research fields as travel-need grading.`,
    `- Filesystem fallback fails closed for production Airtable claims.`,
    ``,
  ].join("\n");

  fs.writeFileSync(path.join(OUT, "COMMERCIAL_RECONCILIATION.md"), md);

  const evidenceMd = [
    `# Cross-Gate Evidence Reconciliation`,
    ``,
    `**Hotel:** \`${hotelId}\``,
    `**Generated:** ${metrics.at}`,
    `**Full E2E:** PARTIAL`,
    `**Budget incremental:** $0`,
    ``,
    `| Organization | Commercial classification | Verified demand mechanism | Contact route | Buyer identity | Eligibility | Ready | Canonical publication | Customer visibility | Remaining blocker | Next sales action |`,
    `|---|---|---|---|---|---|---|---|---|---|---|`,
    ...rows.map((r) => {
      const pub =
        r.organizationName === "London Business School" && lbsPublication
          ? lbsPublication.status
          : r.publicationDestination;
      const vis =
        r.organizationName === "London Business School" && lbsPublication
          ? lbsPublication.persist?.customerVisible === true &&
            lbsPublication.persist?.productionPublished === true
            ? "production_visible"
            : "not_production_visible"
          : "not_published";
      return `| ${r.organizationName} | ${r.commercialClassification} | ${r.verifiedDemandMechanism} | ${r.contactRoute?.tier || ""} / ${r.contactRoute?.reason || ""} | ${r.namedPerson} | ${r.eligibility} | ${r.ready} | ${pub} | ${vis} | ${r.primaryBlocker} | ${(r.nextAction || "").replace(/\|/g, "/")} |`;
    }),
    ``,
    `## Aggregate`,
    ``,
    `| Metric | Value |`,
    `|---|---:|`,
    `| HOTEL_FACING before audit | ${metrics.hotelFacingBeforeAudit} |`,
    `| HOTEL_FACING after audit | ${metrics.hotelFacingAfterAudit} |`,
    `| Genuine hotel sales prospects (classification) | ${metrics.genuineHotelSalesProspects} |`,
    `| Strategic partnerships | ${metrics.strategicPartnerships} |`,
    `| Inferred demand labels | ${metrics.inferredDemand} |`,
    `| Confirmed demand labels | ${metrics.confirmedDemand} |`,
    `| Passing Ready | ${metrics.passingReady} |`,
    `| Airtable persisted | ${metrics.airtablePersisted} |`,
    `| Production customer-visible | ${metrics.productionCustomerVisible} |`,
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "EVIDENCE_RECONCILIATION.md"), evidenceMd);

  console.log(JSON.stringify(metrics, null, 2));
  console.log(`Wrote ${path.join(OUT, "COMMERCIAL_RECONCILIATION.md")}`);
  console.log(`Wrote ${path.join(OUT, "EVIDENCE_RECONCILIATION.md")}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
