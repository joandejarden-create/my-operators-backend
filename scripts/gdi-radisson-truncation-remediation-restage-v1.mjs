#!/usr/bin/env node
/**
 * Restore Radisson drawer truncations from existing research ledgers and stage
 * a new immutable package. Preserves CURRENT 533f2642 and prior Tempo staged
 * package 3f342937 (does not overwrite either). No paid research / Airtable.
 *
 *   node scripts/gdi-radisson-truncation-remediation-restage-v1.mjs
 *   node scripts/gdi-radisson-truncation-remediation-restage-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  fingerprintPublicationContent,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  appearsTruncatedCustomerProse,
  softClampCustomerProse,
  assertPackageEssentialProseCompleteness,
} from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  summarizeCertifiedDrawerAudit,
  resolveContactRelevance,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recH9h3McgMNFQEla";
const SOURCE_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1";
const TEMPO_STAGED_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_tempo_drawer_commercial_remediation_v1";
const NEW_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_drawer_truncation_remediation_v1";
const PRIOR_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const TEMPO_STAGED_FP =
  "3f342937994ebaa36769e8c90af56e9cb26effeb940fa8a4de2be6cce4f995a8";
const TEMPO_ID = "rx_gdi_s4912_d46e3e911701";
const OUT = path.join(
  ROOT,
  "reports/gdi-radisson-production-drawer-content-remediation-v1",
);
const apply = process.argv.includes("--apply");

const RAW_DIRS = [
  "reports/gdi-radisson-toronto-six-missing-segments-exa-30usd-v1/raw-runs",
  "reports/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1/raw-runs",
  "reports/gdi-radisson-blu-toronto-paid-full-e2e-15usd-v1/raw-runs",
];

const HOTEL_FIT_LEAD =
  "Radisson Blu Toronto Downtown (157 rooms, Harbourfront / Queens Quay) fits mid-size exhibitor, speaker, visiting-team, or overflow lodging near downtown and Exhibition Place.";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function collectField(node, field, depth = 0, out = []) {
  if (!node || depth > 12) return out;
  if (typeof node !== "object") return out;
  if (typeof node[field] === "string" && node[field].trim()) out.push(node[field]);
  for (const v of Object.values(node)) {
    if (v && typeof v === "object") collectField(v, field, depth + 1, out);
  }
  return out;
}

function longestField(id, field) {
  let best = "";
  let bestPath = null;
  for (const dir of RAW_DIRS) {
    for (const prefix of ["p2_", "p3_", ""]) {
      const p = path.join(ROOT, dir, `${prefix}${id}.json`);
      if (!fs.existsSync(p)) continue;
      let j;
      try {
        j = JSON.parse(fs.readFileSync(p, "utf8"));
      } catch {
        continue;
      }
      const vals = collectField(j, field).sort((a, b) => b.length - a.length);
      if (vals[0] && vals[0].length > best.length) {
        best = vals[0];
        bestPath = p;
      }
    }
  }
  return best ? { text: best, path: bestPath } : null;
}

function preferComplete(current, candidate) {
  const cur = text(current);
  const cand = text(candidate);
  if (!cand) return { value: cur, changed: false, reason: "no_candidate" };
  if (!cur) return { value: cand, changed: true, reason: "fill_empty" };
  if (appearsTruncatedCustomerProse(cur) && !appearsTruncatedCustomerProse(cand)) {
    return { value: cand, changed: true, reason: "replace_truncated_with_complete" };
  }
  if (
    appearsTruncatedCustomerProse(cur) &&
    cand.length > cur.length &&
    cand.startsWith(cur.slice(0, Math.min(80, cur.length)))
  ) {
    return { value: cand, changed: true, reason: "extend_prefix_match" };
  }
  if (
    appearsTruncatedCustomerProse(cur) &&
    cand.length >= cur.length &&
    !appearsTruncatedCustomerProse(cand)
  ) {
    return { value: cand, changed: true, reason: "prefer_complete_peer" };
  }
  if (!appearsTruncatedCustomerProse(cur) && cand.length > cur.length + 40) {
    // Keep current if already complete unless candidate is clearly fuller + complete.
    if (!appearsTruncatedCustomerProse(cand)) {
      return { value: cand, changed: true, reason: "prefer_longer_complete" };
    }
  }
  return { value: cur, changed: false, reason: "keep_current" };
}

function rebuildWhyThisHotel(opp, hotelProductFit) {
  const cur = text(opp.whyThisHotel);
  const fit = text(hotelProductFit);
  if (!fit) return preferComplete(cur, cur);
  if (!appearsTruncatedCustomerProse(cur) && cur) {
    return { value: cur, changed: false, reason: "already_complete" };
  }
  // Existing packages typically lead with HOTEL_FIT_LEAD then hotelProductFit.
  if (fit && !appearsTruncatedCustomerProse(fit)) {
    const composed = cur.startsWith("Radisson Blu Toronto Downtown")
      ? `${HOTEL_FIT_LEAD} ${fit}`.replace(/\s+/g, " ").trim()
      : fit;
    if (!appearsTruncatedCustomerProse(composed)) {
      return { value: composed, changed: composed !== cur, reason: "compose_fit_lead" };
    }
    return { value: fit, changed: fit !== cur, reason: "use_complete_hotel_product_fit" };
  }
  // Last resort: sentence-boundary clamp of current (no invented ending).
  if (appearsTruncatedCustomerProse(cur)) {
    const clamped = softClampCustomerProse(cur, Math.max(220, cur.lastIndexOf(". ") + 1 || 220));
    if (clamped && !appearsTruncatedCustomerProse(clamped)) {
      return { value: clamped, changed: true, reason: "sentence_boundary_clamp" };
    }
  }
  return { value: cur, changed: false, reason: "unrecovered" };
}

function syncDrawerModel(opp) {
  const model = opp.eventsDrawerModel;
  if (!model || !model.sections) return;
  const sec = model.sections;
  if (sec.why_act_now) sec.why_act_now.body = opp.whyActNow || sec.why_act_now.body;
  if (sec.why_this_hotel) sec.why_this_hotel.body = opp.whyThisHotel || sec.why_this_hotel.body;
  if (sec.recommended_next_step) {
    sec.recommended_next_step.body =
      opp.recommendedNextStep || opp.recommendedAction || sec.recommended_next_step.body;
  }
  if (sec.potential_business) {
    sec.potential_business.body =
      opp.potentialHotelBusiness || opp.potentialBusiness || sec.potential_business.body;
  }
  if (sec.what_is_happening) {
    sec.what_is_happening.body =
      opp.whatIsHappening || opp.summaryWhat || sec.what_is_happening.body;
  }
}

function cardLineFromWhyActNow(whyActNow) {
  const t = text(whyActNow);
  if (!t) return null;
  return softClampCustomerProse(t, 180);
}

fs.mkdirSync(OUT, { recursive: true });

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});
const loaded = store.loadPackage(HOTEL, SOURCE_VERSION);
if (!loaded.ok) {
  console.error(JSON.stringify(loaded, null, 2));
  process.exit(1);
}
const tempoStaged = store.loadPackage(HOTEL, TEMPO_STAGED_VERSION);

const src = loaded.package;
const pkg = JSON.parse(JSON.stringify(src));
const beforeRows = [];
const afterRows = [];
const changes = [];

for (const opp of pkg.opportunities || []) {
  const before = {
    id: opp.id,
    whyActNow: opp.whyActNow || null,
    whyThisHotel: opp.whyThisHotel || null,
    recommendedNextStep: opp.recommendedNextStep || opp.recommendedAction || null,
    truncated: {
      whyActNow: appearsTruncatedCustomerProse(opp.whyActNow),
      whyThisHotel: appearsTruncatedCustomerProse(opp.whyThisHotel),
      recommendedNextStep: appearsTruncatedCustomerProse(
        opp.recommendedNextStep || opp.recommendedAction,
      ),
    },
  };
  beforeRows.push(before);

  const rowChanges = [];

  // Tempo: merge already-certified staged rewrite when present.
  if (opp.id === TEMPO_ID && tempoStaged.ok) {
    const t = (tempoStaged.package.opportunities || []).find((o) => o.id === TEMPO_ID);
    if (t) {
      for (const f of [
        "whyThisHotel",
        "whyActNow",
        "potentialHotelBusiness",
        "potentialBusiness",
        "recommendedNextStep",
        "recommendedAction",
        "primaryContact",
        "whoPrimaryReason",
      ]) {
        if (t[f] != null) opp[f] = JSON.parse(JSON.stringify(t[f]));
      }
      rowChanges.push({ field: "tempo_staged_merge", reason: "from_3f342937_package" });
    }
  }

  const fe = longestField(opp.id, "futureEdition");
  if (fe && (appearsTruncatedCustomerProse(opp.whyActNow) || !text(opp.whyActNow))) {
    const pref = preferComplete(opp.whyActNow, fe.text);
    if (pref.changed) {
      opp.whyActNow = pref.value;
      rowChanges.push({
        field: "whyActNow",
        reason: pref.reason,
        source: fe.path,
        fromLen: text(before.whyActNow).length,
        toLen: text(pref.value).length,
      });
    }
  }

  const nextSrc =
    longestField(opp.id, "recommendedNextStep") ||
    longestField(opp.id, "recommendedAction");
  if (
    nextSrc &&
    appearsTruncatedCustomerProse(opp.recommendedNextStep || opp.recommendedAction)
  ) {
    const pref = preferComplete(
      opp.recommendedNextStep || opp.recommendedAction,
      nextSrc.text,
    );
    if (pref.changed) {
      opp.recommendedNextStep = pref.value;
      opp.recommendedAction = pref.value;
      rowChanges.push({
        field: "recommendedNextStep",
        reason: pref.reason,
        source: nextSrc.path,
        fromLen: text(before.recommendedNextStep).length,
        toLen: text(pref.value).length,
      });
    }
  }

  const fit =
    longestField(opp.id, "hotelProductFit") ||
    longestField(opp.id, "whyThisHotel");
  if (appearsTruncatedCustomerProse(opp.whyThisHotel)) {
    const rebuilt = rebuildWhyThisHotel(opp, fit?.text || "");
    if (rebuilt.changed) {
      opp.whyThisHotel = rebuilt.value;
      rowChanges.push({
        field: "whyThisHotel",
        reason: rebuilt.reason,
        source: fit?.path || null,
        fromLen: text(before.whyThisHotel).length,
        toLen: text(rebuilt.value).length,
      });
    }
  }

  // Keep card line coherent with restored whyActNow.
  if (text(opp.whyActNow)) {
    const card = cardLineFromWhyActNow(opp.whyActNow);
    if (card && card !== text(opp.cardWhyNowLine)) {
      opp.cardWhyNowLine = card;
      rowChanges.push({ field: "cardWhyNowLine", reason: "resync_from_whyActNow" });
    }
  }

  // Contact relevance fallback when class is commercial entry point.
  if (opp.primaryContact && !text(opp.primaryContact.whyThisContact || opp.primaryContact.relevance)) {
    const rel = resolveContactRelevance(opp);
    if (rel) {
      opp.primaryContact.whyThisContact = rel;
      opp.primaryContact.relevance = rel;
      opp.whoPrimaryReason = opp.whoPrimaryReason || rel;
      rowChanges.push({ field: "contactRelevance", reason: "resolve_from_contact_class" });
    }
  }

  syncDrawerModel(opp);

  if (rowChanges.length) changes.push({ id: opp.id, changes: rowChanges });

  afterRows.push({
    id: opp.id,
    whyActNow: opp.whyActNow || null,
    whyThisHotel: opp.whyThisHotel || null,
    recommendedNextStep: opp.recommendedNextStep || opp.recommendedAction || null,
    truncated: {
      whyActNow: appearsTruncatedCustomerProse(opp.whyActNow),
      whyThisHotel: appearsTruncatedCustomerProse(opp.whyThisHotel),
      recommendedNextStep: appearsTruncatedCustomerProse(
        opp.recommendedNextStep || opp.recommendedAction,
      ),
    },
  });
}

pkg.publicationVersion = NEW_VERSION;
pkg.publicationId = "gdi_pubpkg_recH9h3McgMNFQEla_drawer_truncation_remediation_v1";
pkg.lifecycleState = GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED;
pkg.approvalStatus = "PENDING_FOUNDER_APPROVAL";
pkg.approvedAt = null;
pkg.priorContentFingerprintSha256 = PRIOR_FP;
pkg.preservedPriorStagedFingerprintSha256 = TEMPO_STAGED_FP;
pkg.createdAt = new Date().toISOString();
pkg.remediationNote =
  "Restore truncated commercial prose from existing Exa research ledgers; merge Tempo staged rewrite; CURRENT 533f2642 preserved; prior Tempo staged package 3f342937 preserved immutable.";
pkg.founderApprovalReceipt = null;

const fp = fingerprintPublicationContent({
  hotelId: pkg.hotelId,
  publicationVersion: pkg.publicationVersion,
  presentationSchemaVersion: pkg.presentationSchemaVersion,
  applicationCompatibilityVersion: pkg.applicationCompatibilityVersion,
  sortOrder: pkg.sortOrder || (pkg.opportunities || []).map((o) => o.id),
  opportunities: pkg.opportunities || [],
});
pkg.contentFingerprintSha256 = fp.contentFingerprintSha256;
pkg.opportunityCount = fp.opportunityCount;

const integrity = verifyPublicationPackageIntegrity(pkg);
const proseGate = assertPackageEssentialProseCompleteness(pkg);
const certified = summarizeCertifiedDrawerAudit(
  (pkg.opportunities || []).map((o) =>
    evaluateCertifiedDrawerCommercialContent(o, { hotelId: HOTEL }),
  ),
);

let written = { ok: true };
if (apply) {
  written = store.writePackage(pkg);
  if (!written.ok) {
    console.error(JSON.stringify(written, null, 2));
    process.exit(1);
  }
  store.writePointer(HOTEL, "STAGED", {
    publicationId: pkg.publicationId,
    publicationVersion: NEW_VERSION,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
  });
}

const remainingTrunc = afterRows.filter(
  (r) =>
    r.truncated.whyActNow ||
    r.truncated.whyThisHotel ||
    r.truncated.recommendedNextStep,
);

const result = {
  ok: integrity.ok !== false && remainingTrunc.length === 0,
  apply,
  priorFingerprint: PRIOR_FP,
  preservedTempoStagedFingerprint: TEMPO_STAGED_FP,
  newFingerprint: pkg.contentFingerprintSha256,
  publicationVersion: NEW_VERSION,
  currentPreserved: true,
  opportunities: (pkg.opportunities || []).length,
  changedOpportunityCount: changes.length,
  truncationsBefore: beforeRows.filter(
    (r) =>
      r.truncated.whyActNow ||
      r.truncated.whyThisHotel ||
      r.truncated.recommendedNextStep,
  ).length,
  truncationsAfter: remainingTrunc.length,
  remainingTruncationIds: remainingTrunc.map((r) => r.id),
  certifiedSummary: certified,
  proseGate: {
    ok: proseGate.ok,
    failingCount: proseGate.failingCount,
  },
  integrity,
  written: written.ok !== false,
  founderApprovalPhraseRequired: `FOUNDER_APPROVED_FOR_PUBLICATION ${pkg.contentFingerprintSha256}`,
};

fs.writeFileSync(
  path.join(OUT, "02-TRUNCATION_BEFORE_AFTER.json"),
  JSON.stringify({ beforeRows, afterRows, changes }, null, 2) + "\n",
);
fs.writeFileSync(
  path.join(OUT, "03-STAGED_PACKAGE_RESULT.json"),
  JSON.stringify(result, null, 2) + "\n",
);

console.log(JSON.stringify(result, null, 2));
process.exit(result.ok || !apply ? (remainingTrunc.length ? 1 : 0) : 1);
