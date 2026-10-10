/**
 * Trigger → shared second-generation campaign decomposition.
 * Uses exact YOTEL-era pipeline (runHotelDemandCampaignDecompositions).
 * Only new/changed entities are seeded for research; organizers stay SIGNAL_ONLY.
 * Never auto-promotes Ready solely because a list published.
 */

import crypto from "node:crypto";
import {
  loadDemandCampaigns,
  upsertDemandCampaigns,
  runHotelDemandCampaignDecompositions,
} from "../demand-campaigns/index.js";
import { loadOpportunities } from "../repository.js";
import {
  saveOpportunitiesCanonical,
} from "../opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../read-cache.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import { listPursuits, updatePursuitRecord } from "../pursuit/pursuit-store-v1.js";
import { REDECOMPOSITION_STATUS, PUBLICATION_TRIGGER_TYPE } from "./constants.js";
import { applyMonitorStatusToOpportunities } from "./customer-status.js";
import { detectPublicationTriggersInText } from "./multilingual-terms.js";

function entityKey(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 80);
}

/**
 * Lightweight named-entity extraction from newly published page text.
 * Conservative — only clear org/university patterns; never invent lists.
 */
export function extractNamedEntitiesFromPublishedText(text = "", opts = {}) {
  const out = [];
  const seen = new Set();
  const push = (name, type, role) => {
    const n = String(name || "").replace(/\s+/g, " ").trim();
    if (n.length < 4 || n.length > 90) return;
    const k = entityKey(n);
    if (!k || seen.has(k)) return;
    seen.add(k);
    out.push({
      entityId: `ent_${k}`,
      organizationName: n,
      entityType: type,
      participationRole: role,
      evidenceText: `Extracted from published ${opts.triggerType || "artifact"}`,
    });
  };

  const blob = String(text || "");
  // Universities
  const uniRe =
    /Universidad(?:e)?\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚáéíóúñÑ\s]{2,50}/g;
  for (const m of blob.match(uniRe) || []) {
    push(m.replace(/[.;:].*$/, "").trim(), "UNIVERSITY", "SPEAKER_ORG");
    if (out.length >= 40) break;
  }
  // Exhibitor-ish lines: "Stand XX — Company" or bullet company names near expositor
  if (
    opts.triggerType === PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED ||
    /expositores|exhibitors/i.test(blob)
  ) {
    const lines = blob.split(/[\n|;]/).map((l) => l.trim()).filter(Boolean);
    for (const line of lines) {
      if (!/expositor|stand|booth|empresa/i.test(line) && line.length > 60) continue;
      const company = line
        .replace(/^(stand|booth|expositor)\s*[:#]?\s*\w+\s*[-–—:]?\s*/i, "")
        .replace(/\s+/g, " ")
        .trim();
      if (company.length >= 4 && company.length <= 80 && !/cookie|privacy|menu/i.test(company)) {
        push(company, "COMPANY", "EXHIBITOR");
      }
      if (out.length >= 60) break;
    }
  }

  return out;
}

function toEvidenceSeed(entity, monitor, pageUrl, isNew) {
  const isOrganizer =
    /vida sana|latinpress|red iberoamericana|asociación dominicana|cielo laboral$|iaps —/i.test(
      entity.organizationName
    );
  return {
    organizationName: entity.organizationName,
    role: entity.participationRole || "PARTICIPATING_ORG",
    participantType: entity.entityType || "ORG",
    travelingGroup:
      entity.entityType === "UNIVERSITY"
        ? "Faculty / research delegation"
        : entity.participationRole === "EXHIBITOR"
          ? "Exhibitor staffing team"
          : "Participating organization traveling team (if attendance confirmed)",
    travelingEntityType:
      entity.entityType === "UNIVERSITY" ? "UNIVERSITY_TEAM" : "CORPORATE_TEAM",
    travelingEntityEvidence: entity.evidenceText,
    travelingEntityConfidence: "UNRESOLVED",
    travelingEntityProven: false,
    buyerEntity: `${entity.organizationName} — events / conference services`,
    buyerRole:
      entity.entityType === "UNIVERSITY"
        ? "Program / Academic Events"
        : entity.participationRole === "EXHIBITOR"
          ? "Exhibitor Management"
          : "Events / Conference Services",
    publicContactPath: pageUrl,
    lodgingState: "UNKNOWN",
    lodgingNote: `Publication trigger ${monitor.triggerType} — lodging not auto-inferred`,
    evidenceUrl: pageUrl,
    sourceLanguage: monitor.sourceLanguage || "es",
    sourceType: "PUBLICATION_MONITOR_TRIGGER",
    currentCycleStatus: "CURRENT_CYCLE",
    entityId: entity.entityId,
    isNewFromMonitor: isNew === true,
    forceClass: isOrganizer ? "SIGNAL_ONLY" : undefined,
    // AUTOAMERICAS: never treat official hotel as Radisson opportunity
    ...(monitor.autoAmericasSpecial?.treatOfficialHotelAsRadissonOpportunity === false &&
    /dominican fiesta|palladium/i.test(entity.organizationName)
      ? { forceClass: "SIGNAL_ONLY", lodgingNote: "Official hotel partner — not Radisson opportunity" }
      : {}),
  };
}

/**
 * Invoke shared decomposition for one triggered monitor.
 */
export async function invokeRedecompositionOnTrigger(monitor, classification, opts = {}) {
  const hotelId = monitor.hotelId;
  const nowDate =
    (opts.now && new Date(opts.now).toISOString().slice(0, 10)) ||
    new Date().toISOString().slice(0, 10);

  const beforeOpps = loadOpportunities(hotelId)?.opportunities || [];
  const beforeReady = beforeOpps.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate })?.ok)
    .length;
  const beforeWatch = beforeOpps.filter((o) => isValidFutureWatch(o, { nowDate })?.ok).length;

  const extracted = extractNamedEntitiesFromPublishedText(opts.pageText || "", {
    triggerType: classification.primaryTriggerType || monitor.triggerType,
  });

  const seen = new Set(monitor.previouslySeenEntityIds || []);
  const newEntities = extracted.filter((e) => !seen.has(e.entityId));
  const changedEntities = extracted.filter((e) => seen.has(e.entityId)); // re-evidence only if needed

  // Only process new entities for fresh research seeds
  const processEntities = newEntities.length
    ? newEntities
    : changedEntities.length === 0
      ? []
      : [];

  if (processEntities.length === 0 && !(classification.newArtifacts || []).length) {
    // Still sync customer monitor status + pursuit intelligence notes
    await syncCustomerAndPursuit(monitor, classification, {
      nowDate,
      newAccounts: 0,
    });
    return {
      invoked: false,
      status: REDECOMPOSITION_STATUS.SKIPPED_NO_NEW_EVIDENCE,
      newEntityIds: [],
      newAccounts: 0,
      newReady: 0,
      newWatch: 0,
      reason: "no_new_named_entities",
    };
  }

  const campDoc = loadDemandCampaigns(hotelId);
  const campaign = campDoc.campaigns.find((c) => c.campaignId === monitor.campaignId);
  if (!campaign) {
    return {
      invoked: false,
      status: REDECOMPOSITION_STATUS.FAILED,
      newEntityIds: [],
      newAccounts: 0,
      newReady: 0,
      newWatch: 0,
      reason: "campaign_missing",
    };
  }

  const pageUrl = opts.pageUrl || monitor.triggerSourceUrl;
  const newSeeds = processEntities.map((e) => toEvidenceSeed(e, monitor, pageUrl, true));

  // Merge: keep prior seeds; append only new org names
  const priorSeeds = Array.isArray(campaign.evidenceSeeds) ? campaign.evidenceSeeds : [];
  const priorNames = new Set(priorSeeds.map((s) => entityKey(s.organizationName)));
  const mergedSeeds = [
    ...priorSeeds,
    ...newSeeds.filter((s) => !priorNames.has(entityKey(s.organizationName))),
  ];

  upsertDemandCampaigns(
    hotelId,
    [
      {
        campaignId: monitor.campaignId,
        evidenceSeeds: mergedSeeds,
        officialListStatus: "LIST_PARTIAL",
        officialListNextTrigger: monitor.notes || classification.reason,
        lodgingEvidenceClass: campaign.lodgingEvidenceClass,
        publicationMonitorTriggeredAt: new Date().toISOString(),
        publicationMonitorTriggerType: classification.primaryTriggerType || monitor.triggerType,
        publicationMonitorArtifactUrls: classification.newArtifacts || [],
        researchStatus: "MONITOR_TRIGGERED",
        nextAction: `Re-decompose after ${classification.primaryTriggerType || monitor.triggerType}`,
      },
    ],
    { note: "Publication monitor trigger — evidenceSeeds merged (new only)" }
  );

  invalidateGdiHotelReadCache(hotelId);
  const decomp = await runHotelDemandCampaignDecompositions(hotelId, {
    nowDate,
    persist: true,
    enableJev: false,
    maxCompletionSteps: 0,
    campaignIds: [monitor.campaignId],
  });

  invalidateGdiHotelReadCache(hotelId);
  const afterOpps = loadOpportunities(hotelId)?.opportunities || [];
  const afterReady = afterOpps.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate })?.ok)
    .length;
  const afterWatch = afterOpps.filter((o) => isValidFutureWatch(o, { nowDate })?.ok).length;

  await syncCustomerAndPursuit(monitor, classification, {
    nowDate,
    newAccounts: newSeeds.length,
    decomp,
  });

  return {
    invoked: true,
    status: REDECOMPOSITION_STATUS.COMPLETED,
    newEntityIds: processEntities.map((e) => e.entityId),
    changedEntityEvidence: changedEntities.map((e) => e.entityId),
    newAccounts: newSeeds.length,
    newReady: Math.max(0, afterReady - beforeReady),
    newWatch: Math.max(0, afterWatch - beforeWatch),
    decompStatus: decomp?.results?.[0]?.status || null,
    runId: `pm_redecomp_${crypto.randomBytes(3).toString("hex")}`,
  };
}

async function syncCustomerAndPursuit(monitor, classification, opts = {}) {
  const hotelId = monitor.hotelId;
  const doc = loadOpportunities(hotelId);
  const opps = doc?.opportunities || [];
  const patched = applyMonitorStatusToOpportunities(opps, {
    ...monitor,
    customerNextTriggerNote: classification.primaryTriggerType
      ? `Published signal: ${classification.primaryTriggerType.replace(/_/g, " ").toLowerCase()}`
      : undefined,
  });

  // Persist only if watch-card monitor fields changed
  const changed = patched.some((o, i) => {
    const prev = opps[i];
    return (
      o.watchCardMonitoringStatus !== prev.watchCardMonitoringStatus ||
      o.watchCardMonitoringLastChecked !== prev.watchCardMonitoringLastChecked
    );
  });
  if (changed) {
    await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      opportunities: patched,
      updatedAt: new Date().toISOString(),
    });
    invalidateGdiHotelReadCache(hotelId);
  }

  // Pursuit intelligence update — never auto CONTACTED / ENGAGED / HOTEL_INCLUDED
  const pursuits = listPursuits(hotelId);
  for (const p of pursuits) {
    const blob = `${p.opportunityTitle || ""} ${p.organizationName || ""}`.toLowerCase();
    const key = String(monitor.campaignKey || "").toLowerCase();
    const match =
      (key === "iaps" && /iaps|spaces in transition/i.test(blob)) ||
      (key === "biocultura" && /biocultura/i.test(blob)) ||
      (key === "rif" && /filosof|rif/i.test(blob)) ||
      (key === "cielo" && /cielo/i.test(blob)) ||
      (key === "autoamericas" && /autoamericas|autoam/i.test(blob));
    if (!match) continue;

    const triggerNote = classification.meaningful
      ? `Monitor detected: ${classification.primaryTriggerType || monitor.triggerType}`
      : `Monitoring: ${(monitor.customerMonitoringFor || []).join(", ")}`;

    updatePursuitRecord(
      hotelId,
      p.pursuitId,
      {
        nextTrigger: triggerNote,
        triggerType: classification.primaryTriggerType || monitor.triggerType,
        triggerSource: monitor.triggerSourceUrl,
        triggerStatus: classification.meaningful ? "TRIGGERED" : p.triggerStatus || "OPEN",
        // Do NOT set pursuitStatus to CONTACTED/ENGAGED
        // Do NOT set hotelInclusionStatus to HOTEL_INCLUDED
        notes: [p.notes || "", `[publication-monitor ${opts.nowDate}] ${triggerNote}`]
          .filter(Boolean)
          .join("\n")
          .slice(0, 4000),
      },
      "publication_monitor",
      "publication_monitor_intelligence"
    );
  }
}

export { detectPublicationTriggersInText };
