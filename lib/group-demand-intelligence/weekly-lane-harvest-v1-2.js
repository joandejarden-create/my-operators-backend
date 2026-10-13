/**
 * Weekly Discovery Lane Harvest V1.2 — invoke validated engines per due target.
 * Bounded: no broad market rediscovery. Reuses qualify + promote services.
 */

import { qualifyOpportunityV3 } from "./discovery-hygiene-v3.js";
import {
  buildLodgingEvidenceQueries,
  extractLodgingEvidenceFromText,
  evidenceToQualificationHints,
  classifyAgainstExistingGdi,
  HARVEST_CLASSIFICATION,
} from "./demand-generators/lodging-evidence.js";
import {
  qualifyGeneratorSignal,
  buildGeneratorDrivenOpportunityDraft,
  buildDemandGeneratorSignal,
} from "./demand-generators/qualify-promote.js";
import { researchVenuePartnershipSecondPass } from "./private-events/partnership-second-pass.js";
import { buildGdiOpportunityFromPrivateEvent } from "./private-events/promote-to-gdi.js";
import { PLAYBOOK, routeTargetToPlaybook } from "./weekly-discovery-orchestrator.js";

async function fetchPage(url) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return {
      ok: false,
      error: "no_url",
      queriesUsed: 0,
      fetchesUsed: 0,
      text: "",
      html: "",
      finalUrl: null,
      contactIntelligence: null,
    };
  }
  try {
    const r = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(20000),
      headers: { "User-Agent": "DealalityGDI-WeeklyLaneHarvest/1.2" },
    });
    const text = await r.text();
    const hasLodging = /hotel|lodging|accommodat|housing|room.?block|travel/i.test(text);
    const hasFuture = /2026|2027|2028|2029/i.test(text);
    let contactIntelligence = null;
    try {
      const { extractContactIntelligenceFromSource } = await import(
        "./extract-contact-intelligence-from-source.js"
      );
      contactIntelligence = extractContactIntelligenceFromSource({
        source: { url: r.url || url, sourceType: "official_web" },
        pageContent: text,
      });
    } catch {
      contactIntelligence = null;
    }
    return {
      ok: r.status >= 200 && r.status < 400,
      status: r.status,
      finalUrl: r.url,
      text,
      html: text,
      queriesUsed: 0,
      fetchesUsed: 1,
      hasLodging,
      hasFuture,
      contactIntelligence,
    };
  } catch (err) {
    return {
      ok: false,
      error: err.message || String(err),
      queriesUsed: 0,
      fetchesUsed: 1,
      text: "",
      html: "",
      finalUrl: null,
      contactIntelligence: null,
    };
  }
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function extractIsoDates(text) {
  const out = [];
  const re = /\b(20(?:2[6-9]|3[0-5]))-(\d{2})-(\d{2})\b/g;
  let m;
  while ((m = re.exec(text))) out.push(m[0]);
  // Month DD, YYYY
  const re2 =
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20(?:2[6-9]|3[0-5]))\b/gi;
  while ((m = re2.exec(text))) {
    const months = {
      january: "01",
      february: "02",
      march: "03",
      april: "04",
      may: "05",
      june: "06",
      july: "07",
      august: "08",
      september: "09",
      october: "10",
      november: "11",
      december: "12",
    };
    const mo = months[m[1].toLowerCase()];
    if (mo) out.push(`${m[3]}-${mo}-${String(m[2]).padStart(2, "0")}`);
  }
  return [...new Set(out)];
}

/**
 * Build a candidate opportunity from a research target + official page evidence.
 */
export function buildCandidateFromTargetEvidence(target, page, hotelId) {
  const title = target.canonicalName || target.entityKey || "Monitored demand target";
  const dates = extractIsoDates(page.text || "");
  const start = dates[0] || null;
  const id = `gdi_opp_${slugify(title)}_${(start || "cycle").replace(/-/g, "").slice(0, 8)}`;
  const lodgingEvidence = page.hasLodging
    ? `Official page lodging/travel evidence: ${page.finalUrl || target.primarySourceUrl}`
    : null;
  return {
    id,
    opportunityId: id,
    hotelId,
    title,
    opportunityName: title,
    organizationName: target.canonicalName || target.organizationName || title,
    organizationId: slugify(target.entityId || target.canonicalName || title),
    opportunityType: /sport|cup|tournament/i.test(title)
      ? "OVERFLOW_HOUSING"
      : "PRIMARY_PURSUIT",
    demandType: target.entityType || target.targetType || "EVENT_SERIES",
    segment: target.entityType || target.targetType,
    priority: target.priority === "HIGH" ? "HIGH_PRIORITY" : "MEDIUM_PRIORITY",
    eventStartDate: start,
    eventLocationSummary: null,
    officialSource: page.finalUrl || target.primarySourceUrl,
    discoverySource: page.finalUrl || target.primarySourceUrl,
    lodgingEvidence,
    roomDemandStatus: page.hasLodging ? "VERIFIED_HOUSING_PROGRAM" : "UNKNOWN",
    housingStatus: page.hasLodging ? "HOUSING_OPEN" : null,
    venueSourcingStatus: "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
    sourcingStatus: "OPEN_UNRESOLVED",
    opportunityQualification: "PENDING",
    hotelFitScore: 60,
    evidenceConfidence: page.hasLodging && page.hasFuture ? 0.75 : 0.45,
    sources: [
      {
        name: title,
        url: page.finalUrl || target.primarySourceUrl,
        sourceType: "official_web",
        supportsFact: "source",
        claimKind: "FACT",
      },
    ],
    whyNow: lodgingEvidence || "Monitored registry target refreshed from official source.",
    summaryWhyMatters: lodgingEvidence || title,
    summaryWhat: `${title} — registry-driven weekly harvest.`,
    recommendedAction:
      "Monitor official housing/venue announcements; pursue when sourcing is open.",
    isTestData: false,
    customerVisible: true,
    demandStatus: "ACTIVE",
    notes: ["weekly_lane_harvest_v1_2", `target:${target.targetId}`],
  };
}

/**
 * New Opportunities / event / housing / training / government lane — URL evidence + V3 qualify.
 */
export async function harvestNewOpportunitiesLane({
  hotelId,
  target,
  existingOpps = [],
  subjectHotel = null,
} = {}) {
  const playbook = routeTargetToPlaybook(target);
  const page = await fetchPage(target.primarySourceUrl);
  const metrics = {
    queriesUsed: page.queriesUsed,
    fetchesUsed: page.fetchesUsed,
    candidates: 0,
    watch: 0,
    true: 0,
    lane: "New Opportunities",
    playbook,
    invoked: true,
  };

  if (!page.ok) {
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [],
      summary: `NEW_OPPS lane: fetch failed (${page.error || page.status})`,
    };
  }

  const candidate = buildCandidateFromTargetEvidence(target, page, hotelId);
  metrics.candidates = 1;

  const q = qualifyOpportunityV3(hotelId, candidate, {
    nowDate: new Date(),
    subjectHotel: subjectHotel || { hotelId },
    seenKeys: new Set(
      (existingOpps || []).map((o) => String(o.id || o.opportunityId || "").toLowerCase())
    ),
  });

  const actionability = q.actionabilityV3 || q.actionability || null;
  const qualified = {
    ...candidate,
    ...(q.opportunity || {}),
    opportunityQualification:
      actionability === "TRUE_ACTIONABLE"
        ? "STRONG"
        : actionability === "VALID_WATCH" || actionability === "VALID_FUTURE"
          ? "WATCH"
          : candidate.opportunityQualification,
    trueActionable: actionability === "TRUE_ACTIONABLE",
    actionabilityV3: actionability,
    hygiene: q,
  };

  // Registry URL harvest: require lodging + future page evidence AND hygiene TRUE
  const pageBar = page.hasLodging && (page.hasFuture || Boolean(qualified.eventStartDate));
  const trueCandidates =
    qualified.trueActionable && pageBar ? [qualified] : [];
  const watchCandidates =
    !trueCandidates.length &&
    (page.hasLodging || page.hasFuture || actionability === "VALID_WATCH")
      ? [qualified]
      : [];

  metrics.true = trueCandidates.length;
  metrics.watch = watchCandidates.length;

  return {
    metrics,
    candidates: [qualified],
    trueCandidates,
    watchCandidates,
    page,
    contactIntelligence: page.contactIntelligence || null,
    summary: `NEW_OPPS ${playbook}: true=${metrics.true} watch=${metrics.watch} lodging=${page.hasLodging} future=${page.hasFuture} contactClues=${Boolean(page.contactIntelligence?.hasContactClues)}`,
  };
}

/**
 * Demand Generator lane — lodging evidence from official URL + qualify.
 */
export async function harvestDemandGeneratorLane({
  hotelId,
  target,
  existingOpps = [],
} = {}) {
  const playbook = PLAYBOOK.DEMAND_GENERATOR;
  const page = await fetchPage(target.primarySourceUrl);
  const metrics = {
    queriesUsed: 0,
    fetchesUsed: page.fetchesUsed,
    candidates: 0,
    watch: 0,
    true: 0,
    lane: "Demand Generators",
    playbook,
    invoked: true,
  };

  if (!page.ok) {
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [],
      summary: `DG lane: fetch failed`,
    };
  }

  const evidence = extractLodgingEvidenceFromText({
    text: page.text || "",
    url: page.finalUrl || target.primarySourceUrl,
  });
  const hints = evidenceToQualificationHints(evidence, {
    demandGeneratorId: target.demandGeneratorId || target.entityId,
    programId: target.programId,
  });

  const generator = {
    demandGeneratorId: target.demandGeneratorId || target.entityId,
    organizationName: target.canonicalName,
    organizationType: target.entityType,
  };
  const program = target.programId
    ? { programId: target.programId, programName: target.canonicalName }
    : null;

  const signalInput = {
    demandGeneratorId: generator.demandGeneratorId,
    programId: program?.programId,
    triggerType: "FUTURE_CYCLE_SIGNAL",
    sourceUrl: page.finalUrl || target.primarySourceUrl,
    sourceUrls: [page.finalUrl || target.primarySourceUrl],
    eventStartDate: (extractIsoDates(page.text || "") || [])[0] || null,
    lodgingDemandThesis: Boolean(evidence?.lodging || page.hasLodging),
    hasCredibleLodgingDemand: Boolean(evidence?.lodging || page.hasLodging),
    hotelFitOk: true,
    marketRelevance: "MEDIUM",
    commercialGeographyOk: true,
    hotelDemandThesis: evidence?.notes?.join("; ") || hints?.hotelDemandThesis || null,
    timingStatus: page.hasFuture || evidence?.futureCycle ? "FUTURE_CONFIRMED" : null,
    confirmedFutureCycle: Boolean(page.hasFuture || evidence?.futureCycle),
    ...hints,
  };

  const qualify = qualifyGeneratorSignal(signalInput);
  metrics.candidates = 1;

  const overlap = classifyAgainstExistingGdi({
    generator,
    program,
    evidence,
    trueActionable: qualify.trueActionable,
    existingOpps,
  });
  const overlapKind =
    typeof overlap === "string" ? overlap : overlap?.kind || overlap?.classification;

  if (overlapKind === HARVEST_CLASSIFICATION.EXISTING_UPDATE) {
    metrics.watch = 1;
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [{ overlap, generator, qualify }],
      summary: `DG lane: overlap existing GDI — UPDATE not NEW`,
      overlap,
    };
  }

  if (!qualify.trueActionable) {
    metrics.watch = 1;
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [{ qualify, generator }],
      summary: `DG lane: WATCH (${(qualify.failReasons || []).join(",")})`,
    };
  }

  const signal = buildDemandGeneratorSignal({
    ...signalInput,
    ...qualify,
    trueActionable: true,
  });
  const draft = buildGeneratorDrivenOpportunityDraft(signal, generator, program, {
    hotelId,
    generatorPriority: target.priority,
  });
  if (!draft) {
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [],
      summary: "DG lane: draft blocked",
    };
  }

  metrics.true = 1;
  return {
    metrics,
    candidates: [draft],
    trueCandidates: [draft],
    watchCandidates: [],
    summary: `DG lane: TRUE draft ${draft.id || draft.title}`,
  };
}

/**
 * Private Events / Venue Partnership lane — bounded partnership second-pass when URL present.
 */
export async function harvestPrivateEventsLane({
  hotelId,
  hotelName,
  target,
  maxQueries = 3,
} = {}) {
  const playbook = PLAYBOOK.VENUE_PARTNERSHIP;
  const metrics = {
    queriesUsed: 0,
    fetchesUsed: 0,
    candidates: 0,
    watch: 0,
    true: 0,
    lane: "Private Events / Venue",
    playbook,
    invoked: true,
  };

  const venue = {
    venueId: target.venueId || target.entityId,
    venueName: target.canonicalName,
    website: target.primarySourceUrl,
    officialDomain: target.officialDomain,
  };

  // Prefer lightweight URL check; escalate to second-pass only when budget allows
  const page = await fetchPage(target.primarySourceUrl);
  metrics.fetchesUsed += page.fetchesUsed;

  if (!page.ok) {
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [],
      summary: "PE lane: venue URL fetch failed",
    };
  }

  const hasEvents = /wedding|private event|banquet|catering|event space|host your/i.test(
    page.text || ""
  );
  const hasLodging = page.hasLodging;

  if (!hasEvents && !hasLodging) {
    metrics.watch = 1;
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [{ venue, reason: "no_event_or_lodging_signal" }],
      summary: "PE lane: monitored — no material event/lodging signal",
    };
  }

  // Do not auto-create venue partnership from URL alone without full PE qualify —
  // treat as signal update / watch unless second-pass explicitly enabled.
  if (maxQueries <= 0) {
    metrics.watch = 1;
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [{ venue, hasEvents, hasLodging }],
      summary: `PE lane: signal watch events=${hasEvents} lodging=${hasLodging}`,
    };
  }

  try {
    const second = await researchVenuePartnershipSecondPass({
      venue,
      hotelCtx: { hotelId, hotelName },
      maxQueries,
      maxFetches: Math.max(2, maxQueries),
    });
    metrics.queriesUsed += Number(second?.queriesUsed) || maxQueries;
    metrics.fetchesUsed += Number(second?.fetchesUsed) || 0;
    const qual = second?.qualification || second?.qualify || null;
    if (qual?.promote || qual?.trueActionable || qual?.status === "TRUE_ACTIONABLE") {
      const draft = buildGdiOpportunityFromPrivateEvent({
        hotel: { hotelId, name: hotelName },
        venue,
        fit: second.fit || {},
        signal: second.signal || {},
        qualification: qual,
        isTestData: false,
      });
      if (draft) {
        metrics.true = 1;
        metrics.candidates = 1;
        return {
          metrics,
          candidates: [draft],
          trueCandidates: [draft],
          watchCandidates: [],
          summary: `PE lane: TRUE partnership ${draft.id || draft.title}`,
        };
      }
    }
    metrics.watch = 1;
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [{ venue, second }],
      summary: "PE lane: second-pass watch",
    };
  } catch (err) {
    metrics.watch = 1;
    return {
      metrics,
      candidates: [],
      trueCandidates: [],
      watchCandidates: [],
      summary: `PE lane: second-pass error ${err.message || err}`,
    };
  }
}

/**
 * Route one due target to the correct harvest lane.
 */
export async function harvestDueTarget(target, ctx = {}) {
  const playbook = routeTargetToPlaybook(target);
  if (playbook === PLAYBOOK.DEMAND_GENERATOR) {
    return harvestDemandGeneratorLane({
      hotelId: ctx.hotelId,
      target,
      existingOpps: ctx.existingOpps,
    });
  }
  if (
    playbook === PLAYBOOK.VENUE_PARTNERSHIP ||
    playbook === PLAYBOOK.PRIVATE_EVENT_SIGNAL
  ) {
    return harvestPrivateEventsLane({
      hotelId: ctx.hotelId,
      hotelName: ctx.hotelName,
      target,
      maxQueries: ctx.peMaxQueries ?? 0,
    });
  }
  // New Opps family: EVENT_FUTURE_CYCLE, LODGING, SPORTS, TRAINING, GOVERNMENT, GENERAL
  return harvestNewOpportunitiesLane({
    hotelId: ctx.hotelId,
    target,
    existingOpps: ctx.existingOpps,
    subjectHotel: ctx.subjectHotel,
  });
}

export { PLAYBOOK, routeTargetToPlaybook, buildLodgingEvidenceQueries };
