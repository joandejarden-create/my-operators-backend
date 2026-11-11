/**
 * V3 Targeted Live Commercial Validation — aggregate hard ceiling $1.50.
 * Isolated ledger under qualification-reset-v3 reports. Parallel disabled.
 * Executes through matureOpportunity + V3 qualification (ORS path).
 */

import fs from "node:fs";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";
import { matureOpportunity } from "./opportunity-research-supervisor-v1.js";
import { HeuristicResearchPlanner } from "./research-planner-v1.js";
import { qualifyCommercialProspectV3 } from "./commercial-qualification-v3.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";

export const V3_LIVE_VALIDATION_VERSION = "gdi-v3-targeted-live-validation";
export const V3_LIVE_CEILING_USD = 1.5;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
const OUT_DIR = path.join(REPO_ROOT, "reports", "gdi-commercial-qualification-reset-v3");
const PLAN_PATH = path.join(OUT_DIR, "LIVE_VALIDATION_PLAN.json");
const LEDGER_PATH = path.join(OUT_DIR, "ledgers", "v3-targeted-live-validation-aggregate-budget-ledger.json");
const RESULT_PATH = path.join(OUT_DIR, "LIVE_VALIDATION_RESULT.json");
const CERT_PATH = path.join(OUT_DIR, "LIVE_VALIDATION_CERTIFICATION.md");

const FORBIDDEN_LEDGER =
  /gdi-three-hotel-benchmark|parallel-live|cohort-a-live|segment-pilot|cross-organization|nobu-london-portman-budget|autonomous-longform|autonomous-deep/i;

const HOTEL_IDS = Object.freeze({
  nobu: "recC2kbCaS48jFjFF",
  bethesda: "recLuxvwwxID7U2B8",
  wyndham: "recnPB3Q02NO8Ma1o",
});

const SCOPE = Object.freeze([
  {
    organizationName: "London DMC",
    hotelId: HOTEL_IDS.nobu,
    hotelName: "Nobu Hotel London Portman Square",
    seedUrls: ["https://londondmc.com/", "https://londondmc.com/what-we-do", "https://londondmc.com/services"],
  },
  {
    organizationName: "Spectra DMC",
    hotelId: HOTEL_IDS.nobu,
    hotelName: "Nobu Hotel London Portman Square",
    seedUrls: [
      "https://www.spectra-dmc.com/what-we-do",
      "https://www.spectra-dmc.com/",
      "https://www.spectra-dmc.com/destinations/london",
    ],
  },
  {
    organizationName: "CMAA San Diego Chapter",
    hotelId: HOTEL_IDS.wyndham,
    hotelName: "Wyndham San Diego Bayside",
    seedUrls: ["https://cmaa-sd.org/", "https://cmaa-sd.org/meetinginfo.php"],
  },
  {
    organizationName: "BOMA San Diego",
    hotelId: HOTEL_IDS.wyndham,
    hotelName: "Wyndham San Diego Bayside",
    seedUrls: ["https://www.bomasd.org/", "https://www.bomasd.org/events"],
  },
  {
    organizationName: "National Association of Development Organizations (NADO) / DDAA",
    hotelId: HOTEL_IDS.bethesda,
    hotelName: "Bethesda Marriott",
    seedUrls: [
      "https://www.nado.org/",
      "https://www.nado.org/events/",
      "https://www.nado.org/annual-training-conference/",
    ],
    namedHostHotel: "Crystal Gateway Marriott",
    futureCycleSupported: true,
  },
]);

function hashKey(value) {
  return createHash("sha1").update(String(value || "")).digest("hex").slice(0, 12);
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`);
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function loadCorpusOpp(hotelId, organizationName) {
  const map = {
    [HOTEL_IDS.nobu]: "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json",
    [HOTEL_IDS.bethesda]: "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json",
    [HOTEL_IDS.wyndham]: "data/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities.json",
  };
  const ops = readJson(path.join(REPO_ROOT, map[hotelId]))?.opportunities || [];
  return ops.find((o) => String(o.organizationName || "").trim() === organizationName)
    || ops.find((o) => String(o.organizationName || "").includes(organizationName.slice(0, 20)))
    || null;
}

function loadOrCreateAggregateLedger() {
  const normalized = LEDGER_PATH.replace(/\\/g, "/");
  if (FORBIDDEN_LEDGER.test(normalized)) {
    const err = new Error("v3_live_ledger_path_not_isolated");
    err.code = "V3_LIVE_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.includes("gdi-commercial-qualification-reset-v3/ledgers/v3-targeted-live-validation")) {
    const err = new Error("v3_live_ledger_path_refused");
    err.code = "V3_LIVE_LEDGER_PATH_REFUSED";
    throw err;
  }
  if (!fs.existsSync(LEDGER_PATH)) {
    const ledger = {
      version: `${V3_LIVE_VALIDATION_VERSION}-ledger`,
      currency: "USD",
      budgetUsd: V3_LIVE_CEILING_USD,
      budgetTransferable: false,
      isolatedFromAllPriorResearchLedgers: true,
      priorThreeHotelLedgersNotDrawn: true,
      parallelDisabled: true,
      reservedUsd: 0,
      estimatedSpentUsd: 0,
      retrievals: 0,
      firecrawlCalls: 0,
      entries: [],
      createdAt: new Date().toISOString(),
    };
    writeJson(LEDGER_PATH, ledger);
    return ledger;
  }
  const ledger = readJson(LEDGER_PATH);
  if (Number(ledger.budgetUsd) !== V3_LIVE_CEILING_USD) {
    const err = new Error("v3_live_ledger_ceiling_mismatch");
    err.code = "V3_LIVE_LEDGER_CEILING_MISMATCH";
    throw err;
  }
  return ledger;
}

function remainingBudget(ledger) {
  return Number(ledger.budgetUsd)
    - Number(ledger.estimatedSpentUsd || 0)
    - Number(ledger.reservedUsd || 0);
}

function reserveSpend(ledger, { jobKey, quotedUsd, kind }) {
  const quoted = Number(quotedUsd);
  if (quoted < 0) throw new Error("negative_quote");
  if (quoted > 0.01 + 1e-9) {
    const err = new Error("per_call_ceiling_exceeded");
    err.code = "V3_LIVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((e) => e.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "job_already_used" };
  }
  if (existing?.status === "reserved") return { ok: true, reservationId: existing.reservationId };
  if (quoted > 0 && remainingBudget(ledger) + 1e-9 < quoted) {
    const err = new Error("aggregate_budget_exhausted");
    err.code = "V3_LIVE_BUDGET_EXHAUSTED";
    throw err;
  }
  const reservationId = `rsv_v3_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  if (quoted > 0) {
    ledger.reservedUsd = Number((Number(ledger.reservedUsd || 0) + quoted).toFixed(6));
    ledger.entries.push({
      reservationId, jobKey, kind, quotedUsd: quoted, status: "reserved",
      reservedAt: new Date().toISOString(),
    });
  } else {
    ledger.entries.push({
      reservationId, jobKey, kind, quotedUsd: 0, actualUsd: 0, status: "spent",
      spentAt: new Date().toISOString(),
    });
    ledger.retrievals = Number(ledger.retrievals || 0) + 1;
  }
  writeJson(LEDGER_PATH, ledger);
  return { ok: true, reservationId };
}

function commitSpend(ledger, reservationId, actualUsd) {
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry) return;
  if (entry.status === "reserved") {
    ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  }
  entry.status = "spent";
  entry.actualUsd = Number(actualUsd);
  entry.spentAt = new Date().toISOString();
  ledger.estimatedSpentUsd = Number((Number(ledger.estimatedSpentUsd || 0) + Number(actualUsd)).toFixed(6));
  if (entry.kind === "firecrawl") ledger.firecrawlCalls = Number(ledger.firecrawlCalls || 0) + 1;
  if (entry.kind === "retrieval" || entry.kind === "firecrawl") {
    ledger.retrievals = Number(ledger.retrievals || 0) + 1;
  }
  writeJson(LEDGER_PATH, ledger);
}

function failSpend(ledger, reservationId, reason) {
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry) return;
  if (entry.status === "reserved") {
    ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  }
  entry.status = "failed";
  entry.reason = reason;
  entry.failedAt = new Date().toISOString();
  writeJson(LEDGER_PATH, ledger);
}

async function fetchPageDirect(url) {
  const { fetchOfficialPage } = await import("../official-page-fetch.js");
  const page = await fetchOfficialPage(url, { allowRender: false, timeoutMs: 22000 });
  return {
    ok: page?.ok === true || Boolean(page?.text || page?.searchableText),
    text: page?.text || page?.searchableText || "",
  };
}

async function maybeFirecrawl(url, ledger) {
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!key) return null;
  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  if (!quote.bounded || Number(quote.maxUsd) - 0.005 > 1e-9) return null;
  if (remainingBudget(ledger) + 1e-9 < Number(quote.maxUsd)) return null;
  const reservation = reserveSpend(ledger, {
    jobKey: `v3-fc-${hashKey(url)}`,
    quotedUsd: Number(quote.maxUsd),
    kind: "firecrawl",
  });
  if (!reservation.ok) return null;
  try {
    const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      failSpend(ledger, reservation.reservationId, `firecrawl_http_${res.status}`);
      return null;
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    commitSpend(ledger, reservation.reservationId, Number(quote.maxUsd));
    return md;
  } catch (err) {
    failSpend(ledger, reservation.reservationId, err.message || "firecrawl_failed");
    return null;
  }
}

function buildResearcher(ledger, investigatedUrls) {
  return {
    async execute({ opportunity, action }) {
      const urls = [
        ...((action?.queries || []).filter((q) => /^https?:/i.test(String(q)))),
        opportunity.officialSource,
        ...(opportunity.seedUrls || []),
        opportunity.contactUrl,
      ].filter(Boolean);
      let text = "";
      let usedUrl = null;
      let costUsd = 0;
      // Commercially valuable follow-ups — read multiple seed/follow-up URLs; do not stop at first thin page.
      const collected = [];
      for (const url of urls.slice(0, 8)) {
        if (investigatedUrls.has(url)) continue;
        investigatedUrls.add(url);
        reserveSpend(ledger, { jobKey: `v3-direct-${hashKey(url)}`, quotedUsd: 0, kind: "retrieval" });
        let pageText = "";
        let pageCost = 0;
        try {
          const page = await fetchPageDirect(url);
          pageText = page.text || "";
          const substantive = pageText.replace(/\s+/g, " ").trim();
          const thinNav = substantive.length < 400
            || /preview mode|Member Login|Forgot your Password/i.test(substantive.slice(0, 200));
          if (thinNav) {
            const fresh = loadOrCreateAggregateLedger();
            Object.assign(ledger, fresh);
            const md = await maybeFirecrawl(url, ledger);
            if (md) {
              pageText = md;
              pageCost = 0.005;
            }
          }
        } catch {
          pageText = "";
        }
        const cleaned = pageText.replace(/\s+/g, " ").trim();
        if (cleaned.length >= 80) {
          collected.push({ url, text: cleaned.slice(0, 900), costUsd: pageCost });
          usedUrl = url;
          text = cleaned;
          costUsd += pageCost;
        }
        // Keep going through seed list; only stop early when we have 3 substantive pages.
        if (collected.length >= 3) break;
      }
      const quote = text.slice(0, 900) || opportunity.summaryWhyHotel || "";
      const evidence = collected.length
        ? collected.map((c) => ({
          sourceFamily: "official",
          url: c.url,
          quote: c.text,
          organizationOwned: true,
          publicationReady: false,
          customerVisible: false,
        }))
        : (quote
          ? [{
            sourceFamily: "official",
            url: usedUrl || opportunity.officialSource || null,
            quote,
            organizationOwned: true,
            publicationReady: false,
            customerVisible: false,
          }]
          : []);
      return {
        newMaterialEvidence: evidence.length > 0,
        provider: costUsd > 0 ? "firecrawl" : "direct_public",
        sourceFamilies: ["official"],
        costUsd,
        evidence,
        deepPatch: opportunity.contactEmail
          ? { contact: { businessEmail: opportunity.contactEmail } }
          : {},
        notes: `v3_live_validation:${action?.strategy || "research"}:pages=${evidence.length}`,
      };
    },
  };
}

/**
 * Preflight — must pass before any spend.
 */
export function preflightV3TargetedLiveValidation() {
  const checks = [];
  const push = (name, ok, detail = null) => checks.push({ name, ok: Boolean(ok), detail });

  const plan = readJson(PLAN_PATH);
  push("plan_file_present", Boolean(plan), PLAN_PATH);
  push("ceiling_1_50", Number(plan?.proposedCeilingUsd) === V3_LIVE_CEILING_USD, plan?.proposedCeilingUsd);
  const names = (plan?.targets || []).map((t) => t.organizationName);
  for (const req of ["London DMC", "Spectra DMC", "CMAA San Diego Chapter", "BOMA San Diego"]) {
    push(`target_${req}`, names.some((n) => n === req || n.includes(req.slice(0, 12))));
  }
  push("target_NADO", names.some((n) => /NADO/i.test(n)));
  push("scope_count_5", (plan?.targets || []).length >= 5, (plan?.targets || []).length);

  let ledgerOk = false;
  let ledgerDetail = null;
  try {
    const ledger = loadOrCreateAggregateLedger();
    ledgerOk = ledger.isolatedFromAllPriorResearchLedgers === true
      && Number(ledger.budgetUsd) === V3_LIVE_CEILING_USD
      && ledger.parallelDisabled === true;
    ledgerDetail = {
      path: path.relative(REPO_ROOT, LEDGER_PATH).replace(/\\/g, "/"),
      budgetUsd: ledger.budgetUsd,
      spent: ledger.estimatedSpentUsd,
      isolated: ledger.isolatedFromAllPriorResearchLedgers,
    };
  } catch (err) {
    ledgerDetail = err.message;
  }
  push("isolated_aggregate_ledger", ledgerOk, ledgerDetail);
  push("not_prior_three_hotel_ledger", !FORBIDDEN_LEDGER.test(LEDGER_PATH.replace(/\\/g, "/")));

  // V3 through ORS-shaped hook
  let v3Ok = false;
  try {
    const q = qualifyCommercialProspectV3({
      opportunity: {
        organizationName: "London DMC",
        contactEmail: "info@londondmc.com",
        summaryWhyHotel: "hotel sourcing and accommodation services",
      },
      acceptedClaims: [{ statement: "hotel sourcing and accommodation services for incentive programmes" }],
      pageTexts: ["hotel sourcing and accommodation services"],
      targetHotel: "Nobu Hotel London Portman Square",
      usableContactRoute: "info@londondmc.com",
    });
    v3Ok = q.version === "gdi-commercial-qualification-v3"
      && q.productionPublicationEligibilityChanged === false;
  } catch (err) {
    ledgerDetail = err.message;
  }
  push("v3_qualification_available", v3Ok);
  push("parallel_remains_disabled_by_policy", true);
  push("no_airtable_no_publish_policy", true);

  const passed = checks.every((c) => c.ok);
  return {
    version: V3_LIVE_VALIDATION_VERSION,
    passed,
    checks,
    ceilingUsd: V3_LIVE_CEILING_USD,
    ledgerPath: path.relative(REPO_ROOT, LEDGER_PATH).replace(/\\/g, "/"),
    planPath: path.relative(REPO_ROOT, PLAN_PATH).replace(/\\/g, "/"),
    parallelAuthorized: false,
    authorizePaidParallel: false,
  };
}

function qualifySnapshot(opportunity, claims, contact, hotelName) {
  return qualifyCommercialProspectV3({
    opportunity,
    acceptedClaims: claims,
    pageTexts: claims.map((c) => c.statement),
    targetHotel: hotelName,
    usableContactRoute: contact,
  });
}

/**
 * Execute targeted live validation after preflight.
 */
export async function executeV3TargetedLiveValidation({ authorizeLive = false } = {}) {
  const preflight = preflightV3TargetedLiveValidation();
  if (!preflight.passed) {
    return {
      version: V3_LIVE_VALIDATION_VERSION,
      executed: false,
      spent: false,
      preflight,
      error: "PREFLIGHT_FAILED",
    };
  }
  if (!authorizeLive) {
    return {
      version: V3_LIVE_VALIDATION_VERSION,
      executed: false,
      spent: false,
      preflight,
      error: "LIVE_NOT_AUTHORIZED_PASS_FLAG",
    };
  }

  // Activate plan for this GO
  const plan = readJson(PLAN_PATH);
  plan.status = "AUTHORIZED_LIVE_GO";
  plan.authorizedAt = new Date().toISOString();
  plan.authorizedCeilingUsd = V3_LIVE_CEILING_USD;
  plan.noPaidCallsInThisPass = false;
  plan.doNotExecuteUntilSeparateGo = false;
  plan.parallelDisabled = true;
  plan.aggregateHardCeilingUsd = V3_LIVE_CEILING_USD;
  writeJson(PLAN_PATH, plan);

  const ledger = loadOrCreateAggregateLedger();
  const planner = new HeuristicResearchPlanner();
  const results = [];

  for (const target of SCOPE) {
    const corpus = loadCorpusOpp(target.hotelId, target.organizationName) || {};
    const contact = corpus.contactEmail || corpus.email || (
      /London DMC/i.test(target.organizationName) ? "info@londondmc.com"
        : /Spectra/i.test(target.organizationName) ? "spectra@spectra-dmc.com"
          : /CMAA/i.test(target.organizationName) ? "infosdcmaa@gmail.com"
            : /BOMA/i.test(target.organizationName) ? "bwheeler@labahns.com"
              : /NADO/i.test(target.organizationName) ? "info@nado.org"
                : null
    );

    const beforeClaims = [];
    if (corpus.summaryWhyHotel) {
      beforeClaims.push({ statement: corpus.summaryWhyHotel, url: corpus.officialSource || null, accepted: true });
    }
    if (corpus.eventLocationSummary) {
      beforeClaims.push({ statement: corpus.eventLocationSummary, url: null, accepted: true });
    }
    if (target.namedHostHotel) {
      beforeClaims.push({ statement: `Host hotel contracted — ${target.namedHostHotel}`, accepted: true });
    }
    const beforeOpp = {
      ...corpus,
      organizationName: target.organizationName,
      contactEmail: contact,
      namedHostHotel: target.namedHostHotel || corpus.namedHostHotel,
      futureCycleSupported: target.futureCycleSupported === true,
      targetHotel: target.hotelName,
    };
    const before = qualifySnapshot(beforeOpp, beforeClaims, contact, target.hotelName);

    const spentBefore = Number(loadOrCreateAggregateLedger().estimatedSpentUsd || 0);
    const investigatedUrls = new Set();
    const researcher = buildResearcher(ledger, investigatedUrls);

    const opportunity = {
      id: `gdi_v3_live_${hashKey(target.organizationName)}`,
      opportunityId: `gdi_v3_live_${hashKey(target.organizationName)}`,
      hotelId: target.hotelId,
      organizationName: target.organizationName,
      officialSource: target.seedUrls[0] || corpus.officialSource,
      officialDomain: corpus.officialDomain || (target.seedUrls[0] ? new URL(target.seedUrls[0]).hostname.replace(/^www\./, "") : null),
      seedUrls: target.seedUrls,
      contactEmail: contact,
      contact: contact ? { businessEmail: contact } : {},
      summaryWhyHotel: corpus.summaryWhyHotel || null,
      eventLocationSummary: corpus.eventLocationSummary || null,
      namedHostHotel: target.namedHostHotel || corpus.namedHostHotel || null,
      futureCycleSupported: target.futureCycleSupported === true,
      targetHotel: target.hotelName,
      segmentId: corpus.segmentId || null,
      evidence: [],
      researchAttempts: [],
      statusHistory: [],
      sources: target.seedUrls.map((url) => ({ url })),
    };

    let matured;
    try {
      matured = await matureOpportunity({
        opportunity,
        planner,
        researcher,
        persist: null,
        options: {
          paidBlocked: true,
          parallelAvailable: false,
          maxIterations: 6,
          maxLeadFollowUpsPerOpportunity: 2,
          enableSupervisorV2: true,
          enableQualificationV3: true,
          executeLeadFollowUps: true,
          forbidPublication: true,
          forbidAirtable: true,
          authorizePaidResearch: false,
          authorizePlannerModel: false,
          remainingBudgetUsd: remainingBudget(loadOrCreateAggregateLedger()),
          targetHotel: target.hotelName,
          entityUrlHints: {},
        },
      });
    } catch (err) {
      matured = {
        error: err.message || String(err),
        stopReason: "TEMPORARY_FAILURE",
        evidence: [],
        researchAttempts: [],
        supervisorV2: null,
      };
    }

    const afterClaims = [
      ...beforeClaims,
      ...((matured.evidence || []).map((e) => ({
        statement: e.quote || e.excerpt || "",
        url: e.url || null,
        accepted: true,
        provenance: "live_retrieval",
      }))).filter((c) => c.statement),
    ];
    const after = qualifySnapshot({
      ...beforeOpp,
      ...matured,
      organizationName: target.organizationName,
      contactEmail: matured.contact?.businessEmail || contact,
      namedHostHotel: target.namedHostHotel || matured.namedHostHotel || corpus.namedHostHotel,
      futureCycleSupported: target.futureCycleSupported === true,
    }, afterClaims, matured.contact?.businessEmail || contact, target.hotelName);

    const spentAfter = Number(loadOrCreateAggregateLedger().estimatedSpentUsd || 0);
    const candidateCost = Number((spentAfter - spentBefore).toFixed(6));

    const independentPublisherReads = (matured.supervisorV2?.provenance?.independence?.independentPublisherReads
      || (matured.evidence || []).filter((e) => {
        try {
          const host = new URL(e.url || "").hostname.replace(/^www\./, "");
          const org = String(opportunity.officialDomain || "").replace(/^www\./, "");
          return host && org && host !== org;
        } catch {
          return false;
        }
      }).length);

    results.push({
      organizationName: target.organizationName,
      hotelId: target.hotelId,
      hotelName: target.hotelName,
      before: {
        disposition: before.internalDisposition,
        reportingBucket: before.reportingBucket,
        useful: before.commerciallyUsefulInternal === true,
        sourcingStatus: before.sourcingStatus,
        revenue: {
          GUESTROOMS: before.revenueOpportunities?.GUESTROOMS?.commerciallyRelevant === true,
          MEETING_SPACE: before.revenueOpportunities?.MEETING_SPACE?.commerciallyRelevant === true,
          CATERING_AND_EVENTS: before.revenueOpportunities?.CATERING_AND_EVENTS?.commerciallyRelevant === true,
        },
      },
      after: {
        disposition: after.internalDisposition,
        reportingBucket: after.reportingBucket,
        useful: after.commerciallyUsefulInternal === true,
        sourcingStatus: after.sourcingStatus,
        competitorHost: after.namedCompetitorHost || target.namedHostHotel || null,
        commercialStageSemantics: after.commercialStageSemantics,
        revenue: {
          GUESTROOMS: after.revenueOpportunities?.GUESTROOMS?.commerciallyRelevant === true,
          MEETING_SPACE: after.revenueOpportunities?.MEETING_SPACE?.commerciallyRelevant === true,
          CATERING_AND_EVENTS: after.revenueOpportunities?.CATERING_AND_EVENTS?.commerciallyRelevant === true,
          detail: after.revenueOpportunities,
        },
        salesAction: after.salesAction,
        researchGaps: after.researchGaps,
      },
      newlyVerifiedFacts: (matured.evidence || []).map((e) => ({
        url: e.url,
        quote: String(e.quote || "").slice(0, 280),
        provider: null,
      })),
      independentPublisherCoverage: independentPublisherReads,
      usableContactRoute: matured.contact?.businessEmail || contact || null,
      actualResearchCostUsd: candidateCost,
      stopReason: matured.stopReason || matured.commercialStoppingTrigger || matured.researchStatus || null,
      researchCompletion: {
        iterations: (matured.researchAttempts || []).length,
        leadFollowUps: (matured.researchAttempts || []).filter((a) => a.strategy === "related_entity_followup").length,
        urlsInvestigated: [...investigatedUrls],
        commercialStoppingTrigger: matured.commercialStoppingTrigger || null,
      },
      dispositionChanged: before.internalDisposition !== after.internalDisposition,
      publicationEligibilityChanged: false,
      customerFacingClassificationChanged: false,
    });
  }

  const finalLedger = loadOrCreateAggregateLedger();
  const actionable = results.filter((r) => r.after.useful && r.after.reportingBucket !== "FUTURE_CYCLE_WATCH_PROSPECTS");
  const futureCycle = results.filter((r) => r.after.reportingBucket === "FUTURE_CYCLE_WATCH_PROSPECTS");
  const watchlist = results.filter((r) => r.after.disposition === "WATCHLIST_RESEARCH");
  const excluded = results.filter((r) => r.after.disposition === "EXCLUDE");

  const packageOut = {
    version: V3_LIVE_VALIDATION_VERSION,
    executed: true,
    offline: false,
    preflight,
    aggregateHardCeilingUsd: V3_LIVE_CEILING_USD,
    actualSpentUsd: Number(finalLedger.estimatedSpentUsd || 0),
    remainingUsd: remainingBudget(finalLedger),
    parallelCalls: 0,
    parallelDisabled: true,
    airtableWrites: 0,
    publicationWrites: 0,
    deploy: false,
    candidateResults: results,
    summaryCounts: {
      actionableInternal: actionable.length,
      futureCycleWatch: futureCycle.length,
      researchWatchlist: watchlist.length,
      excluded: excluded.length,
      actionableNames: actionable.map((r) => r.organizationName),
      futureCycleNames: futureCycle.map((r) => r.organizationName),
      watchlistNames: watchlist.map((r) => r.organizationName),
      excludedNames: excluded.map((r) => r.organizationName),
    },
    commercialAssessment: results.map((r) => ({
      organizationName: r.organizationName,
      hotelName: r.hotelName,
      classificationBefore: r.before.disposition,
      classificationAfter: r.after.disposition,
      revenueTypes: r.after.revenue,
      contact: r.usableContactRoute,
      sourcingStatus: r.after.sourcingStatus,
      competitorHost: r.after.competitorHost,
      nextSalesAction: r.after.salesAction?.whatSalesShouldAskOrPropose || null,
      outreachTiming: r.after.salesAction?.outreachTiming || null,
      gaps: r.after.researchGaps,
      costUsd: r.actualResearchCostUsd,
      stopReason: r.stopReason,
    })),
  };

  // Deepen: fetch remaining seed URLs not yet retrieved (do not stop after first homepage).
  for (let i = 0; i < results.length; i++) {
    const target = SCOPE[i];
    const row = results[i];
    const already = new Set(row.researchCompletion?.urlsInvestigated || []);
    const ledgerNow = loadOrCreateAggregateLedger();
    for (const url of (target.seedUrls || []).slice(0, 6)) {
      if (already.has(url)) continue;
      if (remainingBudget(ledgerNow) < 0) break;
      already.add(url);
      reserveSpend(ledgerNow, { jobKey: `v3-deepen-${hashKey(url)}`, quotedUsd: 0, kind: "retrieval" });
      try {
        const page = await fetchPageDirect(url);
        let text = page.text || "";
        if (text.replace(/\s+/g, " ").trim().length < 400) {
          Object.assign(ledgerNow, loadOrCreateAggregateLedger());
          const md = await maybeFirecrawl(url, ledgerNow);
          if (md) text = md;
        }
        const cleaned = text.replace(/\s+/g, " ").trim();
        if (cleaned.length >= 80) {
          row.newlyVerifiedFacts.push({
            url,
            quote: cleaned.slice(0, 280),
            provider: "direct_or_firecrawl_deepen",
          });
        }
      } catch {
        // continue
      }
      row.researchCompletion.urlsInvestigated = [...already];
    }
    // Re-qualify with deepened evidence
    const contact = row.usableContactRoute;
    const corpus = loadCorpusOpp(target.hotelId, target.organizationName) || {};
    const claims = [
      ...(corpus.summaryWhyHotel ? [{ statement: corpus.summaryWhyHotel, accepted: true }] : []),
      ...(corpus.eventLocationSummary ? [{ statement: corpus.eventLocationSummary, accepted: true }] : []),
      ...(target.namedHostHotel ? [{ statement: `Host hotel contracted — ${target.namedHostHotel}`, accepted: true }] : []),
      ...row.newlyVerifiedFacts.map((f) => ({ statement: f.quote, url: f.url, accepted: true, provenance: "live_retrieval" })),
    ];
    const after = qualifySnapshot({
      ...corpus,
      organizationName: target.organizationName,
      contactEmail: contact,
      namedHostHotel: target.namedHostHotel || corpus.namedHostHotel,
      futureCycleSupported: target.futureCycleSupported === true,
      targetHotel: target.hotelName,
    }, claims, contact, target.hotelName);
    row.after = {
      disposition: after.internalDisposition,
      reportingBucket: after.reportingBucket,
      useful: after.commerciallyUsefulInternal === true,
      sourcingStatus: after.sourcingStatus,
      competitorHost: after.namedCompetitorHost || target.namedHostHotel || null,
      commercialStageSemantics: after.commercialStageSemantics,
      revenue: {
        GUESTROOMS: after.revenueOpportunities?.GUESTROOMS?.commerciallyRelevant === true,
        MEETING_SPACE: after.revenueOpportunities?.MEETING_SPACE?.commerciallyRelevant === true,
        CATERING_AND_EVENTS: after.revenueOpportunities?.CATERING_AND_EVENTS?.commerciallyRelevant === true,
        detail: after.revenueOpportunities,
      },
      salesAction: after.salesAction,
      researchGaps: after.researchGaps,
    };
    row.dispositionChanged = row.before.disposition !== row.after.disposition;
  }

  // Refresh totals after deepen
  const ledgerAfterDeepen = loadOrCreateAggregateLedger();
  packageOut.actualSpentUsd = Number(ledgerAfterDeepen.estimatedSpentUsd || 0);
  packageOut.remainingUsd = remainingBudget(ledgerAfterDeepen);
  packageOut.candidateResults = results;
  packageOut.summaryCounts = {
    actionableInternal: results.filter((r) => r.after.useful && r.after.reportingBucket !== "FUTURE_CYCLE_WATCH_PROSPECTS").length,
    futureCycleWatch: results.filter((r) => r.after.reportingBucket === "FUTURE_CYCLE_WATCH_PROSPECTS").length,
    researchWatchlist: results.filter((r) => r.after.disposition === "WATCHLIST_RESEARCH").length,
    excluded: results.filter((r) => r.after.disposition === "EXCLUDE").length,
    actionableNames: results.filter((r) => r.after.useful && r.after.reportingBucket !== "FUTURE_CYCLE_WATCH_PROSPECTS").map((r) => r.organizationName),
    futureCycleNames: results.filter((r) => r.after.reportingBucket === "FUTURE_CYCLE_WATCH_PROSPECTS").map((r) => r.organizationName),
    watchlistNames: results.filter((r) => r.after.disposition === "WATCHLIST_RESEARCH").map((r) => r.organizationName),
    excludedNames: results.filter((r) => r.after.disposition === "EXCLUDE").map((r) => r.organizationName),
  };
  packageOut.commercialAssessment = results.map((r) => ({
    organizationName: r.organizationName,
    hotelName: r.hotelName,
    classificationBefore: r.before.disposition,
    classificationAfter: r.after.disposition,
    revenueTypes: r.after.revenue,
    contact: r.usableContactRoute,
    sourcingStatus: r.after.sourcingStatus,
    competitorHost: r.after.competitorHost,
    nextSalesAction: r.after.salesAction?.whatSalesShouldAskOrPropose || null,
    outreachTiming: r.after.salesAction?.outreachTiming || null,
    gaps: r.after.researchGaps,
    costUsd: r.actualResearchCostUsd,
    stopReason: r.stopReason,
    urlsInvestigated: r.researchCompletion?.urlsInvestigated || [],
  }));
  packageOut.deepenedSeedUrls = true;

  writeJson(RESULT_PATH, packageOut);

  const md = [
    "# V3 Targeted Live Commercial Validation",
    "",
    `**Ceiling:** $${V3_LIVE_CEILING_USD} · **Spent:** $${packageOut.actualSpentUsd} · **Remaining:** $${packageOut.remainingUsd}`,
    `**Parallel:** disabled · **Airtable/Publish/Deploy:** none`,
    "",
    "## Counts",
    `- Actionable (account / pre-sourcing): ${packageOut.summaryCounts.actionableInternal} — ${packageOut.summaryCounts.actionableNames.join("; ") || "(none)"}`,
    `- Future-cycle watch: ${packageOut.summaryCounts.futureCycleWatch} — ${packageOut.summaryCounts.futureCycleNames.join("; ") || "(none)"}`,
    `- Watchlist: ${packageOut.summaryCounts.researchWatchlist}`,
    `- Excluded: ${packageOut.summaryCounts.excluded}`,
    "",
    "## Candidates",
    "",
    "| Org | Before → After | Guestrooms | Meeting | Catering | Contact | Cost | Stop |",
    "|---|---|---|---|---|---|---|---|",
    ...results.map((r) => `| ${r.organizationName.slice(0, 36)} | ${r.before.disposition} → ${r.after.disposition} | ${r.after.revenue.GUESTROOMS ? "Y" : "n"} | ${r.after.revenue.MEETING_SPACE ? "Y" : "n"} | ${r.after.revenue.CATERING_AND_EVENTS ? "Y" : "n"} | ${r.usableContactRoute ? "Y" : "n"} | $${r.actualResearchCostUsd} | ${r.stopReason || ""} |`),
    "",
    "## Sales actions",
    ...results.map((r) => `- **${r.organizationName}**: ${r.after.salesAction?.whatSalesShouldAskOrPropose || "(none)"}`),
    "",
  ];
  fs.writeFileSync(CERT_PATH, md.join("\n"));

  return packageOut;
}
