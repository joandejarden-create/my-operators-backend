/**
 * Apply founder-approved Bethesda 12 PASS canonical contact patches.
 *
 * Writes only contact-related fields via mergeContactAntiClobber from
 * ZERO_PAID_CONTACT_EVIDENCE_BY_ID. Requires explicit --apply + founderAuthorization.
 *
 * Does NOT promote Sales Ready / Account Development / customer publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  BETHESDA_EXPECTED_CANONICAL_COUNT,
  TITLE_REPAIRED_59_ROW_SHA256,
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  sha256File,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import { writeJsonAtomic } from "./gdi-research-stage-persistence-v1.js";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  mergeContactAntiClobber,
  extractContactSnapshot,
} from "./bethesda-88-contact-quality-completion-v1.js";

export const BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1 =
  "gdi-bethesda-88-apply-approved-canonical-contact-patches-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL =
  "reports/gdi-bethesda-88-apply-approved-canonical-contact-patches-v1";
const MANIFEST_REL =
  "reports/gdi-bethesda-88-commercial-certification-v1/07-founder-approved-candidate-manifest.json";

const WOMANS_CLUB_ID = "gdi_pe_781f12393f8117e7";

const CONTACT_TOP_KEYS = new Set([
  "primaryContact",
  "secondaryContact",
  "contactRecovery",
  "primaryContactName",
  "primaryContactRole",
  "primaryContactEmail",
  "primaryContactPhone",
  "functionalContactEmail",
  "organizationContactEmail",
  "contactName",
  "contactTitle",
  "contactEmail",
  "contactPhone",
  "contactSourceUrl",
  "contactVerifiedAt",
  "contactOfficialUrl",
  "whyThisContact",
  "commercialContactPath",
  "contactPathClass",
  "contactTier",
  "contactGrade",
  "contactGradeLabel",
  "contactQuality",
  "approvedContactUpdate",
  "commercialEventFamilyId",
  "commercialEventFamilyRole",
  "hotelFitHonesty",
  "hotelFitNote",
  // honesty locks from merge (must stay false / not authorized)
  "salesReady",
  "customerFacing",
  "customerVisible",
  "publicationApproved",
  "publication",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isVerifiedNamedPerson(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/^(unknown|tbd|n\/?a|none|null|staff|contact|info)$/i.test(n)) return false;
  return n.split(/\s+/).filter(Boolean).length >= 2;
}

function loadManifest(root = ROOT) {
  const p = path.join(root, MANIFEST_REL);
  if (!fs.existsSync(p)) return { ok: false, code: "MANIFEST_ABSENT", path: p };
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const passed = Array.isArray(doc.passedPatches) ? doc.passedPatches : [];
  const failed = Array.isArray(doc.failedPatches) ? doc.failedPatches : [];
  const passIds = [...new Set(passed.map((p) => text(p.id)).filter(Boolean))];
  return {
    ok: true,
    path: p,
    doc,
    passed,
    failed,
    passIds,
    failedIds: failed.map((p) => text(p.id)).filter(Boolean),
  };
}

function nonContactEqual(before, after) {
  const keys = new Set([...Object.keys(before || {}), ...Object.keys(after || {})]);
  for (const k of keys) {
    if (CONTACT_TOP_KEYS.has(k)) continue;
    if (JSON.stringify(before?.[k]) !== JSON.stringify(after?.[k])) {
      return { ok: false, key: k };
    }
  }
  return { ok: true };
}

function hasBetterNamedContact(beforeSnap, afterSnap) {
  if (!beforeSnap.named) return false;
  if (!afterSnap.named) return true;
  // Same person — allow email/route upgrades.
  if (
    text(beforeSnap.name).toLowerCase() === text(afterSnap.name).toLowerCase()
  ) {
    return false;
  }
  // Different named person already present — do not clobber.
  return true;
}

function finalizeAppliedRow(before, merged, evidence, patchId) {
  const snap = extractContactSnapshot(merged);
  const personEmail = snap.hasDirectEmail ? snap.email : null;
  const orgRoute = snap.orgEmail || snap.orgRoute || null;
  const next = {
    ...merged,
    primaryContactName: snap.named ? snap.name : before.primaryContactName || null,
    primaryContactRole: snap.title || before.primaryContactRole || null,
    primaryContactEmail: personEmail,
    primaryContactPhone:
      text(merged.primaryContact?.phone) || before.primaryContactPhone || null,
    functionalContactEmail:
      orgRoute || before.functionalContactEmail || null,
    organizationContactEmail:
      orgRoute || before.organizationContactEmail || null,
    contactName: snap.named ? snap.name : before.contactName || null,
    contactTitle: snap.title || before.contactTitle || null,
    contactEmail: personEmail || orgRoute || before.contactEmail || null,
    contactPhone:
      text(merged.primaryContact?.phone) || before.contactPhone || null,
    contactSourceUrl:
      text(merged.primaryContact?.sourceUrl) || before.contactSourceUrl || null,
    contactVerifiedAt:
      text(merged.primaryContact?.verifiedAt) || before.contactVerifiedAt || null,
    whyThisContact:
      text(merged.primaryContact?.whyRelevant) || before.whyThisContact || null,
    commercialContactPath: snap.named ? "NAMED_CONTACT" : before.commercialContactPath,
    contactPathClass: snap.named
      ? snap.hasDirectEmail
        ? "NAMED_DIRECT"
        : "NAMED_PARTIAL"
      : before.contactPathClass,
    salesReady: false,
    customerFacing: false,
    customerVisible: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
    // Preserve maturity / priority / demand evidence unless already set
    commercialMaturity: before.commercialMaturity,
    priority: before.priority,
    demandClass: before.demandClass,
    demandEvidenceQuality: before.demandEvidenceQuality,
    approvedContactUpdate: {
      version: BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1,
      appliedAt: new Date().toISOString(),
      founderApproved: true,
      opportunityId: patchId,
      emailClassification: snap.hasDirectEmail
        ? "DIRECT_PERSON_EMAIL"
        : snap.hasOrgRoute
          ? "ORGANIZATION_ROUTE"
          : "NONE",
      directPersonEmail: snap.hasDirectEmail === true,
      coverage: snap.coverage,
    },
  };
  if (evidence.commercialEventFamilyId) {
    next.commercialEventFamilyId = evidence.commercialEventFamilyId;
  }
  if (evidence.commercialEventFamilyRole) {
    next.commercialEventFamilyRole = evidence.commercialEventFamilyRole;
  }
  if (evidence.hotelFitHonesty) {
    next.hotelFitHonesty = evidence.hotelFitHonesty;
  }
  if (evidence.hotelFitNote) {
    next.hotelFitNote = evidence.hotelFitNote;
  }
  return next;
}

/**
 * @param {{ root?: string, dryRun?: boolean, founderAuthorization?: boolean }} opts
 */
export function applyApprovedCanonicalContactPatchesV1({
  root = ROOT,
  dryRun = true,
  founderAuthorization = false,
} = {}) {
  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });

  const manifest = loadManifest(root);
  if (!manifest.ok) return manifest;

  if (manifest.passIds.length !== 12) {
    return {
      ok: false,
      code: "UNEXPECTED_PASS_COUNT",
      passCount: manifest.passIds.length,
      expected: 12,
      passIds: manifest.passIds,
    };
  }

  // Hard-exclude failed NCI RNA / Carrie Wagerman IDs from any write path.
  const blocked = new Set(
    [
      ...manifest.failedIds,
      ...(manifest.doc.rejectedOrDeferredIds || []),
    ].map(text).filter(Boolean)
  );
  for (const id of manifest.passIds) {
    if (blocked.has(id)) {
      return { ok: false, code: "PASS_ID_IN_FAILED_SET", id };
    }
  }

  const loaded = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (!loaded.exists) return { ok: false, code: "STORE_ABSENT", path: loaded.path };
  if (loaded.count !== BETHESDA_EXPECTED_CANONICAL_COUNT) {
    return {
      ok: false,
      code: "UNEXPECTED_COUNT",
      count: loaded.count,
      expected: BETHESDA_EXPECTED_CANONICAL_COUNT,
    };
  }
  if (loaded.sha256 !== TITLE_REPAIRED_59_ROW_SHA256) {
    // Allow idempotent re-run after this module already applied.
    const already = loaded.opportunities.some(
      (o) =>
        o?.approvedContactUpdate?.version ===
        BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1
    );
    if (!already) {
      return {
        ok: false,
        code: "UNEXPECTED_STORE_SHA",
        sha256: loaded.sha256,
        expected: TITLE_REPAIRED_59_ROW_SHA256,
      };
    }
  }

  const beforeIds = loaded.ids.slice().sort();
  const byId = new Map(
    loaded.opportunities.map((o) => [text(o.id), o])
  );

  const applied = [];
  const skipped = [];
  const nextById = new Map(byId);

  for (const patch of manifest.passed) {
    const id = text(patch.id);
    if (patch.verdict !== "PASS") {
      skipped.push({ id, reason: "NOT_PASS_VERDICT", verdict: patch.verdict });
      continue;
    }
    if (blocked.has(id)) {
      skipped.push({ id, reason: "FAILED_EXCLUDED" });
      continue;
    }
    const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
    if (!evidence || evidence.inventory !== "canonical") {
      skipped.push({ id, reason: "EVIDENCE_MISSING_OR_NOT_CANONICAL" });
      continue;
    }
    if (evidence.rejected === true || evidence.founderValidation === "FAIL") {
      skipped.push({ id, reason: "EVIDENCE_REJECTED" });
      continue;
    }
    const before = byId.get(id);
    if (!before) {
      skipped.push({ id, reason: "ID_NOT_IN_CANONICAL_STORE" });
      continue;
    }

    const beforeSnap = extractContactSnapshot(before);
    let merged = mergeContactAntiClobber(before, evidence);
    // Founder-approved org route wins over leftover store domain variants
    // (e.g. NIHSmallBusiness@od.nih.gov vs @mail.nih.gov).
    const expectedDirect = text(patch.directEmail) || null;
    const expectedOrg = text(patch.orgRoute) || null;
    if (expectedOrg) {
      merged = {
        ...merged,
        primaryContact: {
          ...(merged.primaryContact || {}),
          organizationContactRoute: expectedOrg,
        },
        functionalContactEmail: expectedOrg,
        organizationContactEmail: expectedOrg,
      };
    }
    const afterSnap = extractContactSnapshot(merged);

    if (!isVerifiedNamedPerson(afterSnap.name)) {
      skipped.push({
        id,
        reason: "AFTER_NOT_NAMED",
        after: afterSnap,
      });
      continue;
    }

    if (hasBetterNamedContact(beforeSnap, afterSnap)) {
      skipped.push({
        id,
        reason: "WOULD_OVERWRITE_EXISTING_BETTER_NAMED_CONTACT",
        beforeName: beforeSnap.name,
        afterName: afterSnap.name,
      });
      continue;
    }

    // Manifest vs evidence consistency for direct vs org routes.
    if (expectedDirect) {
      if (text(afterSnap.email).toLowerCase() !== expectedDirect.toLowerCase()) {
        skipped.push({
          id,
          reason: "DIRECT_EMAIL_MISMATCH_VS_MANIFEST",
          expectedDirect,
          afterEmail: afterSnap.email || null,
        });
        continue;
      }
    } else if (afterSnap.hasDirectEmail) {
      skipped.push({
        id,
        reason: "UNEXPECTED_DIRECT_EMAIL_WHEN_MANIFEST_ORG_ONLY",
        afterEmail: afterSnap.email,
      });
      continue;
    }
    if (expectedOrg) {
      const afterOrg = text(afterSnap.orgEmail || afterSnap.orgRoute);
      if (afterOrg.toLowerCase() !== expectedOrg.toLowerCase()) {
        skipped.push({
          id,
          reason: "ORG_ROUTE_MISMATCH_VS_MANIFEST",
          expectedOrg,
          afterOrg: afterOrg || null,
        });
        continue;
      }
    }
    const afterRow = finalizeAppliedRow(before, merged, evidence, id);
    const nc = nonContactEqual(before, afterRow);
    if (!nc.ok) {
      skipped.push({
        id,
        reason: "NON_CONTACT_FIELD_WOULD_CHANGE",
        key: nc.key,
      });
      continue;
    }

    nextById.set(id, afterRow);
    applied.push({
      id,
      before: beforeSnap,
      after: afterSnap,
      proposedName: patch.proposedName,
      emailClassification: patch.emailClassification,
      directEmail: expectedDirect,
      orgRoute: expectedOrg,
      sourceUrl: patch.sourceUrl,
    });
  }

  const nextOps = loaded.opportunities.map((o) => nextById.get(text(o.id)) || o);

  const idGate = assertNoProtectedIdLoss({
    hotelId: BETHESDA_HOTEL_ID,
    nextOpportunities: nextOps,
    protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS, WOMANS_CLUB_ID, ...manifest.passIds],
  });
  if (!idGate.ok) return { ok: false, ...idGate };

  const countGate = assertCountNotDropped({
    hotelId: BETHESDA_HOTEL_ID,
    priorCount: loaded.count,
    nextCount: nextOps.length,
  });
  if (!countGate.ok) return { ok: false, ...countGate };

  // Ensure untouched rows are referentially unchanged.
  for (let i = 0; i < nextOps.length; i++) {
    const id = text(nextOps[i].id);
    if (!applied.some((a) => a.id === id)) {
      if (nextOps[i] !== loaded.opportunities[i]) {
        return { ok: false, code: "UNTOUCHED_ROW_MUTATED", id };
      }
    }
  }

  const preview = {
    version: BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_HOTEL_ID,
    beforeSha256: loaded.sha256,
    expectedPass: 12,
    appliedCount: applied.length,
    skippedCount: skipped.length,
    applied,
    skipped,
    failedExcluded: manifest.failedIds,
  };
  writeJsonAtomic(path.join(outDir, "01-apply-preview.json"), preview);

  if (dryRun || !founderAuthorization) {
    return {
      ok: true,
      dryRun: true,
      writeApplied: false,
      founderAuthorization: Boolean(founderAuthorization),
      ...preview,
      note:
        dryRun || !founderAuthorization
          ? "DRY_RUN — no opportunities.json write (founderAuthorization + --apply required)"
          : null,
    };
  }

  const backup = backupHotelOpportunityStore(BETHESDA_HOTEL_ID, {
    root,
    label: "pre-12-pass-canonical-contact-patches",
  });
  if (!backup.ok) return { ok: false, ...backup };

  const doc =
    loaded.doc && typeof loaded.doc === "object" && !Array.isArray(loaded.doc)
      ? { ...loaded.doc }
      : { hotelId: BETHESDA_HOTEL_ID, opportunities: [] };
  doc.hotelId = BETHESDA_HOTEL_ID;
  doc.opportunities = nextOps;
  doc.updatedAt = new Date().toISOString();
  doc.approvedCanonicalContactPatches = {
    version: BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1,
    appliedAt: doc.updatedAt,
    founderApproved: true,
    appliedIds: applied.map((a) => a.id),
    skipped,
    beforeSha256: loaded.sha256,
  };

  writeJsonAtomic(loaded.path, doc);
  const afterSha = sha256File(loaded.path);
  const afterLoaded = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const afterIds = afterLoaded.ids.slice().sort();
  if (JSON.stringify(afterIds) !== JSON.stringify(beforeIds)) {
    return {
      ok: false,
      code: "ID_SET_CHANGED_AFTER_WRITE",
      backup,
      afterSha,
    };
  }
  if (afterLoaded.count !== 59) {
    return { ok: false, code: "COUNT_NOT_59_AFTER_WRITE", count: afterLoaded.count, backup };
  }

  // Named coverage on canonical only (union audited separately).
  const namedCanonical = afterLoaded.opportunities.filter((o) =>
    isVerifiedNamedPerson(
      o.primaryContact?.name || o.primaryContactName || o.contactName
    )
  ).length;

  const summary = {
    ok: true,
    writeApplied: true,
    backupPath: backup.backupPath,
    beforeSha256: loaded.sha256,
    afterSha256: afterSha,
    count: afterLoaded.count,
    appliedCount: applied.length,
    skippedCount: skipped.length,
    appliedIds: applied.map((a) => a.id),
    skipped,
    namedCanonicalAfter: namedCanonical,
    paidResearch: false,
    customerPublication: false,
  };
  writeJsonAtomic(path.join(outDir, "02-apply-summary.json"), summary);

  return {
    ok: true,
    dryRun: false,
    writeApplied: true,
    backup,
    ...summary,
    applied,
  };
}
