/**
 * Post-qualification contact resolution pass.
 * Only enriches High / strong Medium / Overflow housing / Reactivation.
 * Uses official curated L1 enrichments + re-grades existing contacts.
 * $0 Webhound unless a future caller injects approved spend.
 */

import { buildOpportunity } from "./opportunity-factory.js";
import { PRIORITY } from "./claim-types.js";
import {
  CONTACT_ENRICHMENT_PASS_ID,
  OFFICIAL_CONTACT_ENRICHMENTS_V1,
} from "./contact-official-enrichments-v1.js";
import {
  contactabilityAdjustmentFromGrade,
  dedupeContactCandidates,
  enrichContactRecord,
  gradeContact,
  selectPrimaryAmongCandidates,
  shouldEnrichContact,
} from "./contact-resolution.js";
import {
  applyCommercialQaOverride,
  finalizeCommercialQaOpportunity,
} from "./commercial-qa-overrides-v1.js";

function toRebuildInput(o) {
  return {
    ...o,
    fitComponents: {
      physicalFit: o.physicalFitScore ?? o.fitComponents?.physicalFit,
      geographyFit: o.geographyFitScore ?? o.fitComponents?.geographyFit,
      timing: o.timingScore ?? o.fitComponents?.timing,
      commercialValue: o.commercialValueScore ?? o.fitComponents?.commercialValue,
      historicalFit: o.historicalFitScore ?? o.fitComponents?.historicalFit,
      competitiveAccessibility:
        o.competitiveAccessibilityScore ?? o.fitComponents?.competitiveAccessibility,
      contactability: o.contactabilityScore ?? o.fitComponents?.contactability,
    },
    confidenceInput: o.confidenceInput
      ? o.confidenceInput
      : o.evidenceConfidenceFactors
        ? {
            sourceAuthority: o.evidenceConfidenceFactors.sourceAuthority,
            independentSourceCount: Math.round(
              (o.evidenceConfidenceFactors.independentSources || 0) / 20
            ),
            directness: o.evidenceConfidenceFactors.directness,
            recency: o.evidenceConfidenceFactors.recency,
            verifiedFieldRatio:
              (o.evidenceConfidenceFactors.verifiedFieldRatio || 0) / 100,
            firstPartyShare: o.evidenceConfidenceFactors.firstParty,
            completeness: o.evidenceConfidenceFactors.completeness,
            conflictPenalty: o.evidenceConfidenceFactors.conflictPenalty,
          }
        : undefined,
  };
}

function snapshotContact(o) {
  const c = o.primaryContact || null;
  const g = c ? gradeContact(c, o) : { contactGrade: "E" };
  return {
    id: o.id,
    title: o.title,
    priority: o.priority,
    contactName: c?.name || null,
    contactEmail: c?.email || null,
    contactPhone: c?.phone || null,
    contactQuality: o.contactQuality || c?.contactQuality || null,
    contactGrade: o.contactGrade || g.contactGrade,
    named: Boolean(c?.name && String(c.name).split(/\s+/).length >= 2),
  };
}

function applyEnrichmentToOpportunity(opportunity) {
  const beforeGrade = snapshotContact(opportunity).contactGrade;
  if (!shouldEnrichContact(opportunity)) {
    return {
      opportunity,
      skipped: true,
      reason: "not_in_contact_enrichment_scope",
      beforeGrade,
      afterGrade: beforeGrade,
      costUsd: 0,
    };
  }

  const patch = OFFICIAL_CONTACT_ENRICHMENTS_V1[opportunity.id] || null;
  const candidates = [];

  if (patch?.primary) {
    candidates.push({ ...patch.primary, __officialPatchPrimary: true });
  }
  if (Array.isArray(patch?.backups)) {
    candidates.push(...patch.backups.map((b) => ({ ...b, __officialPatchBackup: true })));
  }
  if (opportunity.primaryContact) candidates.push(opportunity.primaryContact);
  if (Array.isArray(opportunity.contacts)) candidates.push(...opportunity.contacts);
  if (Array.isArray(opportunity.backupContacts)) candidates.push(...opportunity.backupContacts);

  const deduped = dedupeContactCandidates(candidates);
  // Prefer curated official primary when present (do not let a weaker legacy primary win)
  let primary = null;
  if (patch?.primary) {
    primary = enrichContactRecord(patch.primary, opportunity);
  }
  if (!primary) {
    primary = selectPrimaryAmongCandidates(deduped, opportunity);
  }
  const backups = (patch?.backups || [])
    .map((c) => enrichContactRecord(c, opportunity))
    .filter(Boolean)
    .concat(
      deduped
        .map((c) => enrichContactRecord(c, opportunity))
        .filter(Boolean)
        .filter((c) => c.dedupeKey !== primary?.dedupeKey)
    )
    .filter((c, i, arr) => arr.findIndex((x) => x.dedupeKey === c.dedupeKey) === i)
    .filter((c) => c.contactGrade !== "E")
    .slice(0, 3);

  if (!primary) {
    return {
      opportunity: {
        ...opportunity,
        contactGrade: "E",
        contactGradeLabel: "Grade E — no usable contact",
        contactConfidence: 0,
        contactResearchAudit: {
          pass: CONTACT_ENRICHMENT_PASS_ID,
          targetRole: patch?.targetRole || null,
          usefulContactResolved: false,
          costUsd: 0,
        },
      },
      skipped: false,
      beforeGrade,
      afterGrade: "E",
      costUsd: 0,
      usefulContactResolved: false,
    };
  }

  const baseContactability =
    Number(opportunity.contactabilityScore ?? opportunity.fitComponents?.contactability) || 50;
  const adj = contactabilityAdjustmentFromGrade(primary.contactGrade);
  const nextContactability = Math.max(0, Math.min(100, baseContactability + adj));

  const researchMethods = new Set([
    ...(opportunity.researchMethodsAttempted || []),
    "GDI-CONTACT-01",
  ]);

  const next = {
    ...opportunity,
    primaryContact: primary,
    backupContacts: backups,
    contacts: [primary, ...backups],
    contactQuality: primary.contactQuality,
    contactQualityLabel: primary.contactQualityLabel,
    contactGrade: primary.contactGrade,
    contactGradeLabel: primary.contactGradeLabel,
    contactConfidence: primary.contactConfidence,
    contactRole: primary.role,
    contactRoleMatch: primary.targetRoleMatch,
    contactRoleMatchLabel: primary.targetRoleMatchLabel,
    relationshipToEvent: primary.relationshipToEvent,
    relationshipConfidence: primary.roleConfidence,
    whyThisContact: primary.whyThisContact,
    contactabilityScore: nextContactability,
    fitComponents: {
      ...(opportunity.fitComponents || {}),
      contactability: nextContactability,
    },
    researchMethodsAttempted: [...researchMethods],
    contactResearchAudit: {
      pass: CONTACT_ENRICHMENT_PASS_ID,
      targetRole: patch?.targetRole || primary.targetRoleMatch,
      personCandidatesConsidered: deduped.map((c) => ({
        name: c.name || null,
        email: c.email || null,
        role: c.role || null,
        organization: c.organization || null,
      })),
      sources: [
        ...new Set(
          deduped.map((c) => c.sourceUrl || c.source || c.emailSource).filter(Boolean)
        ),
      ],
      providers: ["official_source", "dealality_contact_intelligence_reachability"],
      acceptedPrimary: {
        name: primary.name,
        email: primary.email,
        phone: primary.phone,
        contactGrade: primary.contactGrade,
      },
      rejectedWeaker: patch?.auditNote || null,
      verificationResult: primary.emailVerificationStatus,
      confidence: primary.contactConfidence,
      costUsd: patch?.costUsd ?? 0,
      usefulContactResolved: primary.contactGrade === "A" || primary.contactGrade === "B" || primary.contactGrade === "C",
      gradeBefore: beforeGrade,
      gradeAfter: primary.contactGrade,
      webhoundCalls: 0,
    },
  };

  return {
    opportunity: next,
    skipped: false,
    beforeGrade,
    afterGrade: primary.contactGrade,
    costUsd: patch?.costUsd ?? 0,
    usefulContactResolved: next.contactResearchAudit.usefulContactResolved,
  };
}

function metricsFromSnapshots(rows) {
  const n = rows.length || 1;
  const pct = (pred) => Math.round((1000 * rows.filter(pred).length) / n) / 10;
  return {
    count: rows.length,
    pctNamedContact: pct((r) => r.named),
    pctVerifiedEmailStyle: pct((r) => Boolean(r.contactEmail)),
    pctWithPhone: pct((r) => Boolean(r.contactPhone)),
    pctGradeA: pct((r) => r.contactGrade === "A"),
    pctGradeB: pct((r) => r.contactGrade === "B"),
    pctGradeC: pct((r) => r.contactGrade === "C"),
    pctGradeD: pct((r) => r.contactGrade === "D"),
    pctGradeE: pct((r) => r.contactGrade === "E" || !r.contactGrade),
  };
}

/**
 * @param {object[]} opportunities
 */
export function applyContactResolutionPass(opportunities = []) {
  const beforeSnaps = [];
  const afterSnaps = [];
  const beforeAfter = [];
  const audits = [];
  let costUsd = 0;
  let enrichedCount = 0;
  let skippedCount = 0;

  const out = [];
  const seen = new Set();

  for (const o of opportunities || []) {
    if (!o?.id || seen.has(o.id)) continue;
    seen.add(o.id);

    const beforeSnap = snapshotContact(o);
    beforeSnaps.push(beforeSnap);

    const applied = applyEnrichmentToOpportunity(o);
    if (applied.skipped) skippedCount += 1;
    else enrichedCount += 1;
    costUsd += applied.costUsd || 0;

    const qa = applyCommercialQaOverride(toRebuildInput(applied.opportunity));
    let rebuilt = buildOpportunity(qa.opportunity);
    if (qa.applied) {
      rebuilt = finalizeCommercialQaOpportunity(rebuilt, qa.override, qa.forceMaxPriority);
    }

    // Preserve contact resolution fields that buildOpportunity may not yet map
    rebuilt = {
      ...rebuilt,
      primaryContact: applied.opportunity.primaryContact || rebuilt.primaryContact,
      backupContacts: applied.opportunity.backupContacts || [],
      contactGrade: applied.opportunity.contactGrade || rebuilt.contactGrade,
      contactGradeLabel: applied.opportunity.contactGradeLabel || rebuilt.contactGradeLabel,
      contactConfidence: applied.opportunity.contactConfidence ?? rebuilt.contactConfidence,
      contactRoleMatch: applied.opportunity.contactRoleMatch || rebuilt.contactRoleMatch,
      contactRoleMatchLabel:
        applied.opportunity.contactRoleMatchLabel || rebuilt.contactRoleMatchLabel,
      whyThisContact: applied.opportunity.whyThisContact || rebuilt.whyThisContact,
      contactResearchAudit: applied.opportunity.contactResearchAudit || null,
      contactQuality:
        applied.opportunity.primaryContact?.contactQuality || rebuilt.contactQuality,
      contactQualityLabel:
        applied.opportunity.primaryContact?.contactQualityLabel || rebuilt.contactQualityLabel,
      relationshipToEvent:
        applied.opportunity.relationshipToEvent || rebuilt.relationshipToEvent,
    };

    const afterSnap = snapshotContact(rebuilt);
    afterSnaps.push(afterSnap);
    beforeAfter.push({
      id: o.id,
      title: o.title,
      priorityBefore: o.priority,
      priorityAfter: rebuilt.priority,
      gradeBefore: applied.beforeGrade,
      gradeAfter: applied.afterGrade,
      contactName: rebuilt.primaryContact?.name || null,
      email: rebuilt.primaryContact?.email || null,
      emailVerification: rebuilt.primaryContact?.emailVerificationStatus || null,
      phone: rebuilt.primaryContact?.phone || null,
      phoneType: rebuilt.primaryContact?.phoneType || null,
      contactConfidence: rebuilt.contactConfidence,
      whyThisContact: rebuilt.whyThisContact,
      skipped: applied.skipped,
      costUsd: applied.costUsd,
    });
    if (applied.opportunity.contactResearchAudit) {
      audits.push(applied.opportunity.contactResearchAudit);
    }
    out.push(rebuilt);
  }

  const enrichableBefore = beforeSnaps.filter((s) =>
    shouldEnrichContact(opportunities.find((o) => o.id === s.id) || {})
  );
  const enrichableAfter = afterSnaps.filter((s) =>
    out.find((o) => o.id === s.id && shouldEnrichContact(o))
  );

  const count = (p) => out.filter((x) => x.priority === p).length;

  return {
    opportunities: out,
    enrichment: {
      pass: CONTACT_ENRICHMENT_PASS_ID,
      webhoundSpentUsd: 0,
      webhoundCalls: 0,
      paidEnrichmentUsd: 0,
      contactResearchCostUsd: costUsd,
      enrichedCount,
      skippedCount,
      appliedAt: new Date().toISOString(),
      priorityCounts: {
        HIGH_PRIORITY: count(PRIORITY.HIGH),
        MEDIUM_PRIORITY: count(PRIORITY.MEDIUM),
        WATCHLIST: count(PRIORITY.WATCHLIST),
        DISQUALIFIED: count(PRIORITY.DISQUALIFIED),
      },
      metricsBefore: metricsFromSnapshots(enrichableBefore.length ? enrichableBefore : beforeSnaps),
      metricsAfter: metricsFromSnapshots(enrichableAfter.length ? enrichableAfter : afterSnaps),
      metricsBeforeAll: metricsFromSnapshots(beforeSnaps),
      metricsAfterAll: metricsFromSnapshots(afterSnaps),
      audits,
    },
    beforeAfter,
  };
}
