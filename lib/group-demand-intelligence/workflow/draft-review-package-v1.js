/**
 * Generate canonical draft review packages from candidate / store records.
 * Does not require a hand-authored CSV as a prerequisite.
 * Human review of fit / credibility / sales copy is an explicit checkpoint.
 */

import fs from "node:fs";
import path from "node:path";
import { evaluateDrawerContentCompleteness } from "./drawer-content-contract-v1.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { isGenericThesis } from "./drawer-content-contract-v1.js";

export const DRAFT_REVIEW_PACKAGE_VERSION = "gdi-draft-review-package-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function cardIdFor(opp, hotelKey) {
  if (opp.prospectReviewCardId) return opp.prospectReviewCardId;
  const org = text(opp.organizationName || opp.company)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 40);
  const year = String(opp.eventStartDate || "").slice(0, 4) || "undated";
  return `${hotelKey}-${org || "org"}-${year}`;
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/**
 * Build draft package + human review checkpoint from opportunities + contact candidates.
 */
export function buildDraftReviewPackage({
  hotelId,
  hotelKey = "hotel",
  hotelLabel = "Hotel",
  opportunities = [],
  peDraftOpportunities = [],
  contactResults = [],
  fitAssessments = [],
  outDir,
  maxCards = 40,
} = {}) {
  if (!outDir) throw new Error("outDir_required");
  fs.mkdirSync(outDir, { recursive: true });

  const contactByOpp = new Map(
    (contactResults || []).map((c) => [c.opportunityId, c])
  );
  const fitByOpp = new Map(
    (fitAssessments || []).map((f) => [
      f.opportunityId || f.candidateId || f.id,
      f,
    ])
  );

  const register = [];
  const packages = [];
  const reviewDecisions = [];
  const drawerScores = [];

  const seenIds = new Set();
  const merged = [];
  for (const o of [...(opportunities || []), ...(peDraftOpportunities || [])]) {
    if (!o?.id || seenIds.has(o.id)) continue;
    seenIds.add(o.id);
    merged.push(o);
  }

  const pool = merged
    .filter((o) => {
      if (o.draftSource === "private_event_intelligence") return true;
      const elig = evaluateProspectPublicationEligibility(o);
      // Include facing + enrichment queue + watch with contactable route —
      // do not force a target count by lowering eligibility.
      return (
        elig.hotelFacing === true ||
        elig.enrichmentQueue === true ||
        o.prospectHotelFacing === true ||
        o.prospectReviewCardId
      );
    })
    .slice(0, maxCards);

  for (const opp of pool) {
    const elig = evaluateProspectPublicationEligibility(opp);
    const contact = contactByOpp.get(opp.id);
    const fit = fitByOpp.get(opp.id);
    const cardId = cardIdFor(opp, hotelKey);
    const section =
      String(opp.prospectReviewSection || "").toUpperCase() === "EXPLORATORY"
        ? "EXPLORATORY"
        : elig.hotelFacing
          ? "PRIORITY"
          : "EXPLORATORY";

    const relevance = opp.hotelRelevanceResearch || null;
    const thesis =
      text(relevance?.propertySpecificFit?.text) ||
      text(opp.hotelOpportunityThesis) ||
      text(opp.summaryWhyHotel) ||
      text(opp.fitExplanation);
    const genericThesis = isGenericThesis(thesis);
    // Coordinator hotel-relevance gaps block "ready" sales copy
    if (
      relevance &&
      (relevance.disposition === "INTERNAL_GAP" ||
        relevance.disposition === "INSUFFICIENT")
    ) {
      // still emit for human checkpoint — do not pretend facing-ready
    }
    const sources = [
      ...(Array.isArray(opp.sources) ? opp.sources : []),
      ...(opp.officialSource ? [{ url: opp.officialSource }] : []),
    ]
      .map((s) => s.url || s)
      .filter(Boolean);

    // Staff-only sources must not be treated as event-date evidence
    const eventSources = sources.filter(
      (u) => !/\/(?:staff|team|people|meet-our-staff)\/?$/i.test(u)
    );

    const pkg = {
      cardId,
      hotelId,
      orgKey: text(opp.prospectOrgKey || opp.organizationName)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .slice(0, 40),
      company: opp.organizationName || opp.company || null,
      person: contact?.after?.name || opp.primaryContact?.name || "Not established",
      title: contact?.after?.role || opp.primaryContact?.role || null,
      relevanceEvidence:
        contact?.after?.relevanceEvidence ||
        opp.primaryContact?.relevanceEvidence ||
        "Not established — confirm role vs lodging authority before outreach.",
      contactMethod:
        contact?.after?.email ||
        opp.primaryContact?.email ||
        opp.functionalContactEmail ||
        "Not established",
      contactMethodType: contact?.after?.email
        ? "named_or_functional_email"
        : "not_established",
      businessPhone:
        contact?.after?.phone || opp.primaryContact?.phone || "Not established",
      phoneType: contact?.after?.phoneType || opp.primaryContactPhoneType || null,
      bookingResponsibility:
        contact?.after?.bookingResponsibilityClass ||
        opp.bookingResponsibilityClass ||
        "POTENTIAL_RELEVANT_CONTACT",
      sourceUrl:
        contact?.after?.sourceUrl ||
        opp.primaryContact?.sourceUrl ||
        eventSources[0] ||
        sources[0] ||
        null,
      verificationDate: new Date().toISOString().slice(0, 10),
      completenessGroup:
        contact?.contactCompletenessGroup ??
        opp.contactCompletenessGroup ??
        null,
      hotelFacing: elig.hotelFacing === true,
    };

    packages.push(pkg);

    register.push({
      cardId,
      hotelId,
      hotelLabel,
      section,
      orgKey: pkg.orgKey,
      account: pkg.company,
      timing: text(opp.timingNote) ||
        [opp.eventStartDate, opp.eventEndDate].filter(Boolean).join(" – ") ||
        "Timing not established",
      commercialStatus: elig.hotelFacing
        ? "Prospect to qualify — hotel sourcing unconfirmed."
        : "Exploratory — needs human fit/credibility review.",
      confirmedBuying: "NO",
      horizon: "PRIMARY",
      sources: eventSources.length
        ? eventSources.map((u) => `${u} (retrieved ${pkg.verificationDate})`).join(" ; ")
        : sources[0]
          ? `${sources[0]} (contact/context only; event evidence not established)`
          : "",
      notes: genericThesis
        ? "Draft thesis missing hotel-specific substance — human rewrite required"
        : "",
    });

    const draftOpp = {
      ...opp,
      prospectReviewCardId: cardId,
      prospectReviewSection: section,
      hotelOpportunityThesis: genericThesis ? null : thesis,
      primaryContact: {
        ...(opp.primaryContact || {}),
        ...(contact?.updatedOpportunityPatch?.primaryContact || {}),
      },
      sources: eventSources.map((url) => ({
        url,
        verificationDate: pkg.verificationDate,
        claimKind: "FACT",
      })),
    };
    const drawer = evaluateDrawerContentCompleteness(draftOpp);
    drawerScores.push({
      cardId,
      opportunityId: opp.id,
      drawerOk: drawer.ok,
      failed: drawer.failed,
      warnings: drawer.warnings,
      layers: drawer.layers,
    });

    reviewDecisions.push({
      cardId,
      opportunityId: opp.id,
      checkpoint: "human_review_fit_credibility_sales_copy",
      status: "pending",
      requiredDecisions: [
        "confirm_hotel_specific_fit",
        "confirm_credibility_of_occasion",
        "approve_or_rewrite_sales_copy",
        "confirm_contact_relevance_vs_booking_authority",
        ...(opp.draftSource === "private_event_intelligence"
          ? [
              "confirm_pe_travel_cohort_or_partnership_thesis",
              "confirm_pe_not_auto_promoted",
            ]
          : []),
      ],
      blockingForPublication: true,
      draftDrawerOk: drawer.ok,
      draftFailed: drawer.failed,
      fitOutcome: fit?.outcome || fit?.independentHotelFit?.outcome || null,
      eligibility: elig.eligibility,
      hotelFacing: elig.hotelFacing === true,
      peDisposition: opp.peDisposition || null,
      draftSource: opp.draftSource || "opportunity_store",
      note: "Package is draft — do not publish until checkpoint decisions recorded",
    });
  }

  const packagesDoc = {
    version: DRAFT_REVIEW_PACKAGE_VERSION,
    hotelId,
    generatedAt: new Date().toISOString(),
    packages,
    note: "Draft contact packages from coordinator candidates — human review required",
  };

  const headers = [
    "cardId",
    "hotelId",
    "hotelLabel",
    "section",
    "orgKey",
    "account",
    "timing",
    "commercialStatus",
    "confirmedBuying",
    "horizon",
    "sources",
    "notes",
  ];
  const csv =
    headers.join(",") +
    "\n" +
    register
      .map((r) => headers.map((h) => csvEscape(r[h])).join(","))
      .join("\n") +
    "\n";

  fs.writeFileSync(path.join(outDir, "final-prospect-register.csv"), csv);
  fs.writeFileSync(
    path.join(outDir, "contact-packages.json"),
    JSON.stringify(packagesDoc, null, 2) + "\n"
  );
  fs.writeFileSync(
    path.join(outDir, "human-review-checkpoint.json"),
    JSON.stringify(
      {
        version: DRAFT_REVIEW_PACKAGE_VERSION,
        hotelId,
        generatedAt: new Date().toISOString(),
        pendingCount: reviewDecisions.filter((d) => d.status === "pending").length,
        decisions: reviewDecisions,
        note: "Explicit checkpoint — record approve/rewrite/reject before publication prep treats package as reviewed",
      },
      null,
      2
    ) + "\n"
  );
  fs.writeFileSync(
    path.join(outDir, "draft-drawer-scores.json"),
    JSON.stringify(
      {
        hotelId,
        total: drawerScores.length,
        pass: drawerScores.filter((d) => d.drawerOk).length,
        fail: drawerScores.filter((d) => !d.drawerOk).length,
        results: drawerScores,
      },
      null,
      2
    ) + "\n"
  );

  return {
    version: DRAFT_REVIEW_PACKAGE_VERSION,
    hotelId,
    packageDir: outDir,
    registerCount: register.length,
    packageCount: packages.length,
    peDraftCount: (peDraftOpportunities || []).length,
    pendingHumanReviews: reviewDecisions.length,
    drawerPass: drawerScores.filter((d) => d.drawerOk).length,
    drawerFail: drawerScores.filter((d) => !d.drawerOk).length,
    humanReviewCheckpointPath: path.join(outDir, "human-review-checkpoint.json"),
  };
}
