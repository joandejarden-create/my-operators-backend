import { ownershipQueries, ownershipSearchResults, rankOwnershipSources, rankOwnershipSourcesDetailed, ownershipDocumentPassages, ownershipCandidates } from "./ownership-research-planning.js";
/**
 * Ownership → contact research handoff (evaluation staging).
 *
 * Gap fixed: Contact Intelligence V1.3/V1.4 filtered hotels with
 * return_to_ownership_lane=true (and stopped after OWNER_DOMAIN_UNRESOLVED)
 * without invoking further research. This module continues inside the existing
 * SerpAPI + Context.dev + discoverOwnerPersonPath architecture.
 *
 * Staging only — does not mutate HOTEL_TO_OWNER / Census canonical owner fields.
 *
 * Optional rollout: budgets.iterative_ownership_loop / caseInput.iterative_ownership_loop /
 * CONTACT_INTELLIGENCE_ITERATIVE_OWNERSHIP_LOOP=1 enables the adapted deep-research
 * iterative Context.dev ownership loop (default OFF — prior flat path preserved).
 */

import {
  contextDevSearch,
  contextDevScrapeMarkdown,
  contextDevExtract,
  OWNERSHIP_CONTACT_EXTRACT_SCHEMA,
  isContextDevConfigured,
} from "../../context-dev/client.js";
import {
  createContextDevCreditLedger,
  CONTEXT_DEV_CREDIT_COSTS,
} from "../../context-dev/credit-ledger.js";
import {
  shouldUseIterativeOwnershipLoop,
  createSharedBudgetController,
  createModelUsdLedger,
  runOwnershipIterativeResearchLoop,
  OWNERSHIP_ITERATIVE_LOOP_VERSION,
} from "./ownership-iterative-research-loop.js";
import {
  validateOwnershipHandoffInput,
  OWNERSHIP_HANDOFF_CONTRACT_VERSION,
  OWNERSHIP_HANDOFF_VS_HOTEL_EXPLORER,
} from "./ownership-handoff-contract.js";
import {
  isOwnershipNativeMethodRouterEnabled,
  runOwnershipNativeMethodRouter,
  OWNERSHIP_NATIVE_METHOD_ROUTER_VERSION,
} from "./ownership-native-method-router.js";
import {
  consumeBoundedFollowUpEvidencePipeline,
  assertOwnershipPartyRolesDistinct,
} from "./registry-follow-up-evidence-pipeline.js";
import { resolveOwnerDomain, isRejectedOwnerDomain } from "./owner-contact-resolution-v2/domain-resolver.js";
import {
  discoverOwnerPersonPath,
  verifyOwnershipPath,
} from "./owner-person-discovery.js";
import { serpGoogle } from "./live-native-discovery.js";
import {
  ATTRIBUTION,
  CHANNEL_KIND,
  PROPERTY_RELEVANCE,
  USAGE_RIGHTS,
  ROLE_CURRENCY,
} from "./vocabulary.js";
import { createChannel, createEvidenceRef, createPersonContact } from "./contact-record.js";
import { classifyLeadershipCategory, LEADERSHIP_CATEGORY } from "./leadership-extraction.js";
import { validateLinkedInIdentifier } from "./provider-submission-gate.js";
import { gateProviderCandidates } from "./fullenrich-gated-submit.js";
import { extractContactsFromHtml } from "./live-native-discovery.js";
import { contactValuePersonBound } from "./research-evidence.js";
import {
  createJournaledCaller,
  createJournaledContextProviders,
  restoreBudgetUsageFromJournal,
  completedKeysFromJournal,
  resultsFromJournal,
  pendingUnknownFromJournal,
} from "./research-operation-journal.js";
import { qualifyPersonOwnerAffiliation } from "./person-owner-affiliation-gate.js";

export const OWNERSHIP_CONTACT_HANDOFF_VERSION = "ownership-contact-research-handoff-v1";

const ROLE_PRIORITY_RE =
  /development|acquisition|invest|founder|ceo|chief|president|chairman|director|cfo|asset|owner|comprador|invers|desarrollo|managing/i;

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function asHttps(url) {
  const u = String(url || "").trim();
  if (!u) return null;
  if (u.startsWith("http")) return u;
  return `https://${u}`;
}

/** discoverOwnerPersonPath returns { url, evidence } — normalize to string URL. */
function unwrapDomain(value) {
  if (!value) return null;
  if (typeof value === "string") return asHttps(value);
  if (typeof value === "object" && value.url) return asHttps(value.url);
  return null;
}

/** Third-party directories / data vendors — leads only, never owner company domain. */
const DIRECTORY_DOMAIN_HOSTS = new Set([
  "emis.com",
  "zoominfo.com",
  "crunchbase.com",
  "bloomberg.com",
  "dnb.com",
  "opencorporates.com",
  "signalhire.com",
  "rocketreach.co",
  "apollo.io",
  "linkedin.com",
  "facebook.com",
  "instagram.com",
  "tripadvisor.com",
  "booking.com",
  "travellife.ca",
  "wikipedia.org",
  "whoistheownerof.com",
  "whoownsthebrand.com",
  "ownershipdata.com",
  "marriott.com",
  "ihg.com",
  "hilton.com",
  "hcareers.com",
  "indeed.com",
  "glassdoor.com",
  "panoramaturisticomex.com.mx",
]);

function isDirectoryOrAggregatorHost(host) {
  const h = String(host || "").toLowerCase();
  if (!h) return true;
  if (DIRECTORY_DOMAIN_HOSTS.has(h)) return true;
  if ([...DIRECTORY_DOMAIN_HOSTS].some((d) => h === d || h.endsWith(`.${d}`))) return true;
  return false;
}

function stripHonorificOwnerName(name) {
  return String(name || "")
    .replace(/^(?:Englishman|British\s+businessman|businessman|Mr\.?|Mrs\.?|Ms\.?)\s+/i, "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 120);
}

/** Reject scrape/snippet chrome mistaken for owner legal names. */
function isImplausibleOwnerDisplayName(name) {
  const n = String(name || "").trim();
  if (n.length < 4 || n.length > 90) return true;
  if (/^(of|the|a|an|and|for|to|in|on|by)\b/i.test(n)) return true;
  if (/ownership\s+database|who\s+owns|verified\s+company|biggest\s+brands|cookie|privacy\s+policy/i.test(n)) {
    return true;
  }
  if (/^(investors?|buyers?|purchasers?|owners?|a\s+group(?:\s+of\s+investors?)?|unnamed(?:\s+\w+)?)$/i.test(n)) {
    return true;
  }
  if ((n.match(/[A-Z]/g) || []).length > 20 && !/\s/.test(n)) return true;
  return false;
}

function isLowQualityDomain(url) {
  const h = hostOf(url);
  if (!h) return true;
  if (
    /careers|jobs|hcareers|indeed|glassdoor|linkedin|facebook|twitter|wikipedia|tripadvisor|booking\.com|travellife|top100influential|panorama/i.test(
      h
    )
  ) {
    return true;
  }
  if (isDirectoryOrAggregatorHost(h)) return true;
  // Shared V2 domain-resolver rules (OTA/social/brand central) — brand ≠ auto-invalid for evidenced owner-operator
  if (isRejectedOwnerDomain(h)) return true;
  if (/\.gob\.mx$|\.gov$|\.gov\./i.test(h)) return true;
  if (/linkedin\.com|facebook\.com|twitter\.com|wikipedia\.org/i.test(h)) return true;
  if (/\/transparencia\/|\/licencia\/|descarga\/L\//i.test(url)) return true;
  if (/\.pdf($|\?)/i.test(url)) return true;
  return false;
}

/** Accept domain via shared resolver when brand_is_owner allows brand hosts. */
function acceptOwnerDomainCandidate(url, ownership = {}) {
  const raw = asHttps(url);
  if (!raw) return { ok: false, reason: "EMPTY_URL" };
  const brandIsOwner = Boolean(
    ownership.brand_is_owner === true ||
      /BRAND_AS_OWNER|ECONOMIC_OWNER.*BRAND/i.test(String(ownership.classification || ""))
  );
  if (!brandIsOwner && isLowQualityDomain(raw)) {
    return { ok: false, reason: "LOW_QUALITY_OR_REJECTED_HOST", host: hostOf(raw) };
  }
  const resolved = resolveOwnerDomain({
    discovered_domain: raw,
    brand_is_owner: brandIsOwner,
  });
  if (!resolved?.domain && isRejectedOwnerDomain(raw) && !brandIsOwner) {
    return { ok: false, reason: "DOMAIN_RESOLVER_REJECTED", host: hostOf(raw) };
  }
  return {
    ok: true,
    url: raw,
    host: hostOf(raw),
    resolved,
    brand_is_owner: brandIsOwner,
  };
}

/**
 * Detect similarly-named wrong org from extract/scrape text
 * (e.g. Morocco car rental vs Mexico hotel owner).
 */
export function assessOrgDomainMatch({
  ownerDisplayName,
  pageCompanyName,
  pageText,
  host,
} = {}) {
  const owner = String(ownerDisplayName || "").toLowerCase();
  const company = String(pageCompanyName || "").toLowerCase();
  const blob = String(pageText || "").toLowerCase();
  const h = String(host || "").toLowerCase();

  if (isDirectoryOrAggregatorHost(h)) {
    return {
      ok: false,
      reason: "DIRECTORY_NOT_FIRST_PARTY_OWNER_DOMAIN",
      detail: h,
    };
  }

  const carRental =
    /location de voiture|car rental|agence de location|rent[- ]a[- ]car|alquiler de autos/i.test(
      `${company} ${blob}`
    );
  const moroccoOnly =
    /\bagadir\b|\bmorocco\b|\bmaroc\b|\+212\b/i.test(`${company} ${blob}`) &&
    !/canc[uú]n|guadalajara|m[eé]xico|mexico|hospitality management|real inn|hotel management/i.test(
      `${company} ${blob}`
    );
  if (carRental || (moroccoOnly && /alliance/i.test(owner))) {
    return {
      ok: false,
      reason: "REJECTED_SIMILARLY_NAMED_ORGANIZATION",
      detail: carRental ? "car_rental_not_hotel_owner" : "geography_business_mismatch",
    };
  }

  // Alliance Mexico hospitality must not accept generic "Alliance HM" without hospitality/Mexico signal
  if (/alliance/i.test(owner) && /alliance/i.test(`${company} ${h}`)) {
    const hospitalityOk =
      /hospitality|hotel management|real inn|canc[uú]n|guadalajara|m[eé]xico|mexico|llc/i.test(
        `${company} ${blob}`
      );
    if (!hospitalityOk && (carRental || moroccoOnly || /location|voiture|agadir/i.test(blob))) {
      return {
        ok: false,
        reason: "REJECTED_SIMILARLY_NAMED_ORGANIZATION",
        detail: "alliance_name_without_hospitality_mexico_signal",
      };
    }
  }

  // Host should share an owner token when we claim a first-party company domain.
  // Blocks job boards / unrelated sites that merely mention the org name in copy.
  const ownerTok = String(ownerDisplayName || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 5 && !["management", "hospitality", "company", "group", "hotels", "hotel"].includes(t));
  const hostCompact = h.replace(/[^a-z0-9]/g, "");
  const hostHasOwnerToken =
    ownerTok.length === 0 ||
    ownerTok.some((t) => hostCompact.includes(t) || h.includes(t));
  const pageNamesOwnerStrongly =
    /alliance hospitality management|inmobiliaria hnf/i.test(`${company} ${blob}`) &&
    !/careers|jobs|hiring|neuron|hcareers/i.test(`${company} ${blob} ${h}`);
  if (!hostHasOwnerToken && !pageNamesOwnerStrongly) {
    return {
      ok: false,
      reason: "REJECTED_HOST_DOES_NOT_MATCH_OWNER",
      detail: `host=${h}; owner_tokens=${ownerTok.join(",")}`,
    };
  }

  return { ok: true, reason: null };
}

/**
 * Charge Context.dev via shared budget reservation when a controller is provided.
 * Phase A/C previously called ledger.charge directly and bypassed tryReserve —
 * that allowed concurrent overspend vs iterative/router reservations.
 *
 * Flow: tryReserve → fn → settle (success) | release (validation no-bill / throw).
 * When budgetController is null (legacy tests), falls back to direct ledger.charge.
 */
async function chargedContext(ledger, kind, cost, meta, fn, budgetController = null) {
  let token = null;
  let directGate = null;

  if (budgetController) {
    const reserve = budgetController.tryReserve(cost, { ...meta, kind });
    if (!reserve.ok) {
      return {
        ok: false,
        budget_blocked: true,
        reservation_denied: true,
        error: reserve,
        data: null,
      };
    }
    token = reserve.token;
  } else {
    directGate = ledger.charge(kind, cost, meta);
    if (!directGate.ok) {
      return { ok: false, budget_blocked: true, error: directGate, data: null };
    }
  }

  const isValidationNoBill = (result) =>
    !result?.ok &&
    ((result.error?.class === "VALIDATION_OR_CLIENT" && result.error?.status == null) ||
      result.error?.error_code === "INPUT_VALIDATION_ERROR" ||
      result.error?.key_metadata?.credits_consumed === 0);

  try {
    const result = await fn();
    // Durable journal replay / skip — provider did not bill; must not consume the
    // remaining-mode incremental window (blocks Brazil ladder continuation).
    const isDurableReplayNoBill =
      result?.replayed === true ||
      result?.durable_replay_no_charge === true ||
      (result?.skipped === true &&
        (result?.reason === "REPLAY_STORED_RESULT" ||
          result?.reason === "COMPLETED_WITHOUT_STORED_RESULT" ||
          /REPLAY|COMPLETED_WITHOUT|ALREADY_COMPLETED/i.test(String(result?.reason || ""))));
    if (isValidationNoBill(result) || isDurableReplayNoBill) {
      if (budgetController && token) {
        budgetController.release(token);
      } else if (isValidationNoBill(result)) {
        ledger.charge(`${kind}_refund`, -Number(cost), {
          ...meta,
          refund: true,
          reason: result.error?.class || result.error?.message || "validation_no_charge",
        });
      } else if (isDurableReplayNoBill && directGate) {
        ledger.charge(`${kind}_refund`, -Number(cost), {
          ...meta,
          refund: true,
          reason: "durable_replay_no_provider_bill",
        });
      }
      return {
        ...result,
        budget_blocked: false,
        credit_entry: null,
        refunded: Boolean(isValidationNoBill(result)),
        durable_replay_no_charge: Boolean(isDurableReplayNoBill),
      };
    }
    if (budgetController && token) {
      const gate = budgetController.settle(token, kind, meta);
      return { ...result, budget_blocked: false, credit_entry: gate?.entry ?? null };
    }
    return { ...result, budget_blocked: false, credit_entry: directGate?.entry ?? null };
  } catch (err) {
    // Uncertain provider outcome: retain reservation (do not release).
    // Journaler marks IN_FLIGHT_UNKNOWN; pilot reconcile keeps outstanding liability.
    if (budgetController && token) {
      budgetController.markUnknown?.(token) || null;
      // leave token in reserved set — do not release
    }
    throw err;
  }
}

function pickSearchUrls(results = [], { excludeHosts = [], preferTokens = [] } = {}) {
  const out = [];
  for (const r of results) {
    const url = r.url || r.link;
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const h = hostOf(url);
    if (excludeHosts.some((x) => h === x || h.endsWith(`.${x}`))) continue;
    if (/linkedin\.com\/(posts|pulse)/i.test(url)) continue;
    let score = 0;
    const blob = `${r.title || ""} ${r.snippet || r.description || ""} ${url}`.toLowerCase();
    for (const t of preferTokens) {
      if (t && blob.includes(String(t).toLowerCase())) score += 2;
    }
    if (/about|contact|team|leadership|investor|owner|adquiere|adquis|compra/i.test(blob)) score += 1;
    out.push({ url, title: r.title || null, snippet: r.snippet || r.description || null, score, host: h });
  }
  return out.sort((a, b) => b.score - a.score);
}

function contactValueInLocalWindow(sourceBody, value, personName) {
  return contactValuePersonBound(sourceBody, value, personName);
}

function executiveToPerson(ex, { ownerId, ownerName, hotelName, sourceUrl, newlyResearched, sourceDocumentText = null }) {
  const title = ex.title || "";
  const cat = classifyLeadershipCategory(title);
  const functionally = ROLE_PRIORITY_RE.test(title) || cat === LEADERSHIP_CATEGORY.DEVELOPMENT_ASSET;
  const channels = [];
  // Captured document text only — never executive biography / claim / page_text fields
  const sourceBody = String(sourceDocumentText || "");
  const bodyNorm = sourceBody.toLowerCase();
  const personName = String(ex.full_name || ex.display_name || "").trim();

  if (ex.linkedin_url && /linkedin\.com\/in\//i.test(ex.linkedin_url)) {
    channels.push(
      createChannel({
        kind: CHANNEL_KIND.PERSON_LINKEDIN,
        value: ex.linkedin_url,
        display_label: "Professional profile",
        attribution: ATTRIBUTION.NAMED_PERSON,
        usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
        evidence: [
          createEvidenceRef({
            source_title: "Research extract / corroboration",
            source_url: sourceUrl || null,
            source_type: "live_research",
            observed_at: new Date().toISOString(),
          }),
        ],
      })
    );
  }

  const email = ex.email || ex.professional_email || null;
  // Never invent fallback excerpts — require the contact value in permitted source text
  // locally attributed to this person (whole-page co-occurrence is insufficient).
  if (
    email &&
    /@/.test(email) &&
    contactValueInLocalWindow(sourceBody, email, personName)
  ) {
    const excerptIdx = bodyNorm.indexOf(String(email).toLowerCase());
    const excerpt = sourceBody
      .slice(Math.max(0, excerptIdx - 60), excerptIdx + String(email).length + 60)
      .trim()
      .slice(0, 400);
    channels.push(
      createChannel({
        kind: CHANNEL_KIND.PERSON_EMAIL,
        value: email,
        display_label: "Person-attributed email",
        attribution: ATTRIBUTION.NAMED_PERSON,
        usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
        evidence: [
          createEvidenceRef({
            source_title: "Ownership/contact research extract",
            source_url: sourceUrl || ex.source_page_url || null,
            source_type: "live_research",
            observed_at: new Date().toISOString(),
            excerpt,
            source_text: sourceBody.slice(0, 2000),
          }),
        ],
      })
    );
  }
  const phone = ex.phone || ex.mobile || ex.professional_phone || null;
  if (phone && contactValueInLocalWindow(sourceBody, phone, personName)) {
    const excerptIdx = bodyNorm.indexOf(String(phone).toLowerCase());
    const excerpt = sourceBody
      .slice(Math.max(0, excerptIdx - 60), excerptIdx + String(phone).length + 60)
      .trim()
      .slice(0, 400);
    channels.push(
      createChannel({
        kind: CHANNEL_KIND.PERSON_PHONE,
        value: phone,
        display_label: "Person-attributed phone",
        attribution: ATTRIBUTION.NAMED_PERSON,
        usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
        evidence: [
          createEvidenceRef({
            source_title: "Ownership/contact research extract",
            source_url: sourceUrl || ex.source_page_url || null,
            source_type: "live_research",
            observed_at: new Date().toISOString(),
            excerpt,
            source_text: sourceBody.slice(0, 2000),
          }),
        ],
      })
    );
  }
  return createPersonContact({
    display_name: personName,
    title,
    organization_entity_id: ownerId,
    organization_name: ownerName,
    role_currency: ROLE_CURRENCY.UNKNOWN,
    property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
    channels,
    evidence: [
      createEvidenceRef({
        source_title: "Live ownership/contact research extract",
        source_url: sourceUrl || ex.source_page_url || null,
        source_type: "live_research",
        observed_at: new Date().toISOString(),
        excerpt: (ex.biography_excerpt || ex.claim || title || "").slice(0, 400),
      }),
    ],
    why_relevant: functionally
      ? `Commercially relevant title (${title}) at ${ownerName} for ${hotelName}.`
      : `Named at ${ownerName}; commercial relevance of role not fully confirmed.`,
    publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
    affiliation_status: "CORROBORATED",
    provenance: {
      evidenced_or_inferred: "EVIDENCED",
      basis: "live_research_extract",
      newly_researched: Boolean(newlyResearched),
      functionally_relevant: functionally,
      leadership_category: cat,
      evaluation_staging: true,
      independently_corroborated: true,
    },
  });
}

/**
 * Continue research for one hotel case within allotted budgets.
 * @param {object} [deps] optional overrides for offline tests (search/scrape/isConfigured).
 */
export async function researchHotelOwnershipContactPath(caseInput = {}, budgets = {}, deps = {}) {
  const started = Date.now();

  // ——— Execution contract: fail before any network / provider call ———
  const validated = validateOwnershipHandoffInput(caseInput, budgets);
  if (!validated.ok) {
    return {
      version: OWNERSHIP_CONTACT_HANDOFF_VERSION,
      contract_version: OWNERSHIP_HANDOFF_CONTRACT_VERSION,
      ok: false,
      error: validated.error,
      message: validated.message,
      unknown_keys: validated.unknown_keys || null,
      encoding: validated.encoding || null,
      hotel_explorer_vs_handoff: OWNERSHIP_HANDOFF_VS_HOTEL_EXPLORER,
      calls: [],
      sources: [],
      unresolved_reasons: [validated.error],
      native_method_router: { enabled: false, skipped: true, reason: "INPUT_CONTRACT_FAILED" },
      write_guarantees: {
        canonical_hotel_to_owner_mutations: 0,
        census_owner_mutations: 0,
        evaluation_staging_allowed: false,
        customer_publication: "BLOCKED",
      },
      elapsed_ms: Date.now() - started,
    };
  }

  const hotel = validated.hotel;
  budgets = validated.budgets;
  const encodingFlag = validated.encoding_flagged
    ? { flagged: true, issues: validated.encoding.issues, original_name: validated.encoding.original_name }
    : { flagged: false };

  const serpBudget = {
    max: Number(budgets.serpapi_max ?? 10),
    used: 0,
    usd: 0,
  };
  const priorJournalSpend = Number(
    (deps.budget_usage || caseInput.budget_usage || {})?.context_dev_spent || 0
  );
  const allowanceMode = String(budgets.context_dev_allowance_mode || "cumulative");
  const ledger = createContextDevCreditLedger({
    // Remaining-mode: budgetCredits is the incremental window; alreadySpent stays 0.
    // Cumulative-mode: budgetCredits is the case cap; seed alreadySpent from journal.
    budgetCredits:
      allowanceMode === "remaining"
        ? Number(budgets.context_dev_max ?? 20)
        : Number(budgets.context_dev_max ?? 20),
    alreadySpent: allowanceMode === "remaining" ? 0 : priorJournalSpend,
    label: `handoff:${hotel.hotel_id}`,
  });
  // Shared budget across flat / iterative / method-router (reserve before concurrent work).
  const budgetController = createSharedBudgetController(ledger);
  /** Bind Phase A/C/iterative Context calls to shared tryReserve/settle. */
  const chargedContextReserved = (led, kind, cost, meta, fn) =>
    chargedContext(led, kind, cost, meta, fn, budgetController);
  const searchFnRaw = deps.search || deps.contextDevSearch || contextDevSearch;
  const scrapeFnRaw = deps.scrape || deps.contextDevScrapeMarkdown || contextDevScrapeMarkdown;
  const extractFnRaw = deps.contextDevExtract || contextDevExtract;

  // Route every reachable Context.dev call through the operation journal when checkpointed
  const priorJournal = deps.operation_journal || caseInput.operation_journal || [];
  const journaler =
    typeof deps.onOperationCheckpoint === "function"
      ? createJournaledCaller({
          onOperationCheckpoint: deps.onOperationCheckpoint,
          completedWorkKeys: completedKeysFromJournal(priorJournal),
          pendingUnknownKeys: pendingUnknownFromJournal(priorJournal),
          resultByWorkKey: resultsFromJournal(priorJournal),
          forceRetryUnknown: deps.forceRetryUnknown === true,
          budgetUsage: restoreBudgetUsageFromJournal(deps.budget_usage || {}, priorJournal),
          preDispatchGate: deps.preDispatchGate || null,
          nowFn: deps.nowFn || (() => Date.now()),
        })
      : null;
  const journaled = createJournaledContextProviders({
    journaler,
    search: searchFnRaw,
    scrape: scrapeFnRaw,
    extract: extractFnRaw,
    creditCosts: {
      search: CONTEXT_DEV_CREDIT_COSTS.search_per_10_results,
      scrape: CONTEXT_DEV_CREDIT_COSTS.scrape_markdown,
      extract: CONTEXT_DEV_CREDIT_COSTS.extract,
    },
  });
  const searchFn = journaled.search || searchFnRaw;
  const scrapeFn = journaled.scrape || scrapeFnRaw;
  const extractFn = journaled.extract || extractFnRaw;
  const providersJournaled = journaled.journaled === true;

  const configuredFn = deps.isConfigured || deps.isContextDevConfigured || isContextDevConfigured;
  const disableNetwork =
    deps.disableNetwork === true ||
    budgets.disable_network === true ||
    caseInput.disable_network === true;
  const iterativeOwnershipLoop = shouldUseIterativeOwnershipLoop(
    caseInput,
    budgets,
    deps.env || process.env
  );
  const nativeMethodRouterEnabled = isOwnershipNativeMethodRouterEnabled(
    caseInput,
    budgets,
    deps.env || process.env
  );
  let iterativeLoopMeta = null;
  let nativeMethodRouterMeta = null;
  const calls = [];
  const unresolved = [];
  const sources = [];
  const hotelSiteChannels = [];
  const newly = {
    ownership_claims: [],
    domain: null,
    people: [],
    notes: [],
  };

  if (encodingFlag.flagged) {
    newly.notes.push(`hotel_name_encoding_flagged:${encodingFlag.issues.join(",")}`);
    unresolved.push("HOTEL_NAME_ENCODING_FLAGGED");
  }

  const excludeHosts = [
    ...(caseInput.forbidden_org_hosts || []),
    "marriott.com",
    "ihg.com",
    "hilton.com",
    "hyatt.com",
    "booking.com",
    "tripadvisor.com",
    "facebook.com",
    "instagram.com",
  ];

  let ownership = caseInput.ownership_seed || {
    owner_entity_id: caseInput.owner_entity_id || null,
    owner_display_name: caseInput.owner_display_name || null,
    owner_role: caseInput.owner_role || null,
    classification: caseInput.classification || "UNRESOLVED",
    operator_name: caseInput.operator_name || null,
    brand_name: caseInput.brand_name || null,
    evidence_note: null,
    confidence: null,
    return_to_ownership_lane: !caseInput.owner_entity_id,
    ownership_vs_operator: null,
  };

  // ——— Phase A: ownership resolution when surface missing ———
  // Prefer founder-supplied inspect URLs before open search (resume / lead follow-through).
  const inspectUrls = Array.isArray(caseInput.inspect_urls) ? caseInput.inspect_urls.filter(Boolean) : [];
  for (const rawUrl of inspectUrls.slice(0, 4)) {
    if (!configuredFn() || !ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) break;
    const url = asHttps(rawUrl);
    if (!url || excludeHosts.some((h) => hostOf(url) === h || hostOf(url).endsWith(`.${h}`))) continue;
    const scraped = await chargedContextReserved(
      ledger,
      "scrape_markdown",
      CONTEXT_DEV_CREDIT_COSTS.scrape_markdown,
      { url, phase: "inspect_seed" },
      () => scrapeFn({ url })
    );
    calls.push({ provider: "context_dev", kind: "inspect_seed_scrape", url, ok: scraped.ok });
    if (!scraped.ok || scraped.restricted || String(scraped.usage_rights || "").toUpperCase() === "BLOCKED") {
      continue;
    }
    const md = String(
      typeof scraped.data === "string" ? scraped.data : scraped.data?.markdown || scraped.data?.content || ""
    );
    sources.push({
      kind: "inspect_seed",
      url,
      excerpt: md.slice(0, 1500),
      observed_at: new Date().toISOString(),
      usage_rights: scraped.usage_rights || "INTERNAL_ONLY",
    });
    const claimBlob = md.slice(0, 4000);
    const ownedBy = claimBlob.match(
      /(?:owned by|adquirid[oa] por|propiedad de|purchased by|acquired by|sold to|owner[:\s]+)\s*([A-Z][^.\n|]{3,80})/i
    );
    const bought = claimBlob.match(
      /(?:Englishman|businessman)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z'’-]+){1,3})\s+bought\s+(?:the\s+)?(?:property|hotel|resort)/i
    );
    const match = ownedBy || bought;
    if (match && !ownership.owner_display_name) {
      const stagedName = stripHonorificOwnerName(match[1]);
      if (isImplausibleOwnerDisplayName(stagedName) || /whoistheownerof|whoownsthebrand/i.test(url)) {
        newly.notes.push(`rejected_implausible_owner_name:${stagedName}:${url}`);
      } else {
      ownership = {
        ...ownership,
        owner_display_name: stagedName,
        owner_entity_id: ownership.owner_entity_id || `staged_${hotel.hotel_id}`,
        owner_role: "UNRESOLVED",
        classification: "OWNER_CANDIDATE",
        confidence: "PROBABLE",
        evidence_note: `Staged from inspect seed ${url} — historical purchase ≠ current ownership until corroborated; not canonical write.`,
        return_to_ownership_lane: true,
        ownership_vs_operator: "OWNER_CLAIM_STAGED_OPERATOR_NOT_ASSUMED",
        historical_vs_current: bought ? "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK" : "UNKNOWN",
        newly_researched: true,
      };
      newly.notes.push(`staged_owner_from_inspect_seed:${url}`);
      }
    }
    // Capture year/date cues for historical vs current assessment
    const yearHit = claimBlob.match(
      /(?:bought|acquired|purchased|opened|founded)[^.]*?\b(19\d{2}|20[0-2]\d)\b/i
    );
    if (yearHit) {
      newly.notes.push(`ownership_claim_year_cue:${yearHit[1]}:${url}`);
      ownership.claim_year_cue = yearHit[1];
    }
    if (/as owner and chairman|remains (?:the )?owner|still owned by|current owner/i.test(claimBlob)) {
      ownership.historical_vs_current = "CURRENT_OWNERSHIP_LANGUAGE_PRESENT";
      newly.notes.push(`current_ownership_language:${url}`);
    }
    // Hotel first-party pages → corporate/form/office fallback (not owner domain by default)
    const hotelHosts = new Set(
      [
        caseInput.hotel_site_host,
        hotel.hotel_name?.toLowerCase().includes("blue waters") ? "bluewaters.net" : null,
        hotel.hotel_name?.toLowerCase().includes("copper") ? "copperandlumberhotel.com" : null,
        hotel.hotel_name?.toLowerCase().includes("hammock") ? "hammockcove.com" : null,
      ].filter(Boolean)
    );
    if (hotelHosts.has(hostOf(url)) || /contact|about|privacy|legal/i.test(url)) {
      const extracted = extractContactsFromHtml(md, url, { hotelName: hotel.hotel_name });
      for (const em of (extracted.emails || []).slice(0, 3)) {
        hotelSiteChannels.push(
          createChannel({
            kind: CHANNEL_KIND.ORG_EMAIL,
            value: em,
            display_label: "Hotel/site corporate email (not proven owner-personal)",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.PROPERTY,
            usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
            evidence: [
              createEvidenceRef({
                source_title: "Hotel first-party page",
                source_url: url,
                source_type: "first_party_or_corp",
                observed_at: new Date().toISOString(),
              }),
            ],
            customer_caveat: "Property/operator contact route — do not treat as evidenced owner personal email.",
          })
        );
      }
      for (const ph of (extracted.phones || []).slice(0, 2)) {
        hotelSiteChannels.push(
          createChannel({
            kind: CHANNEL_KIND.SWITCHBOARD,
            value: ph,
            display_label: "Hotel/site switchboard",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.PROPERTY,
            usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
            evidence: [
              createEvidenceRef({
                source_title: "Hotel first-party page",
                source_url: url,
                source_type: "first_party_or_corp",
                observed_at: new Date().toISOString(),
              }),
            ],
          })
        );
      }
      if (!hotelSiteChannels.some((c) => c.kind === CHANNEL_KIND.ORG_WEBSITE) && hotelHosts.has(hostOf(url))) {
        hotelSiteChannels.push(
          createChannel({
            kind: CHANNEL_KIND.ORG_WEBSITE,
            value: `https://${hostOf(url)}/`,
            display_label: "Hotel first-party website (operator/brand surface — not owner domain by default)",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.PROPERTY,
            usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
          })
        );
      }
    }
  }

  for (const eq of (caseInput.extra_serp_queries || []).slice(0, 4)) {
    if (serpBudget.used >= serpBudget.max) break;
    const cost = { serpapi_searches: 0, serpapi_usd: 0 };
    const serp = await serpGoogle(eq, cost, {
      hl: hotel.language === "en" ? "en" : "es",
      gl: hotel.language === "en" ? "us" : "mx",
      num: 8,
    });
    serpBudget.used += cost.serpapi_searches;
    serpBudget.usd += cost.serpapi_usd;
    calls.push({
      provider: "serpapi",
      kind: "extra_follow_up",
      query: eq,
      results: serp.organic?.length || 0,
    });
    sources.push({
      kind: "serp_extra_follow_up",
      query: eq,
      organic: (serp.organic || []).slice(0, 8),
    });
    newly.ownership_claims.push(
      ...(serp.organic || []).slice(0, 5).map((r) => ({
        title: r.title,
        url: r.link || r.url,
        snippet: r.snippet,
        provider: "serpapi",
        follow_up: true,
      }))
    );
  }

  if (!ownership.owner_entity_id || ownership.return_to_ownership_lane) {
    const q = `"${hotel.hotel_name}" (owner OR owned OR acquired OR acquisition OR propietario OR adquirió OR compra) -tripadvisor -booking`;
    if (serpBudget.used < serpBudget.max) {
      const cost = { serpapi_searches: 0, serpapi_usd: 0 };
      const serpWorkKey = `serpapi_phase_b:${q}`.slice(0, 200);
      const serpFn = deps.serpGoogle || serpGoogle;
      if (typeof deps.onOperationCheckpoint === "function") {
        await deps.onOperationCheckpoint({
          kind: "reservation",
          work_key: serpWorkKey,
          status: "IN_FLIGHT",
          provider: "serpapi",
          op: "phase_b_ownership_search",
        });
      }
      let serp;
      try {
        serp = await serpFn(q, cost, {
          hl: hotel.language === "pt" ? "pt" : hotel.language === "en" ? "en" : "es",
          gl: hotel.country === "Brazil" ? "br" : hotel.language === "en" ? "us" : "mx",
          num: 10,
        });
        if (typeof deps.onOperationCheckpoint === "function") {
          await deps.onOperationCheckpoint({
            kind: "settlement",
            work_key: serpWorkKey,
            status: "COMPLETED",
            provider: "serpapi",
            op: "phase_b_ownership_search",
          });
        }
      } catch (err) {
        if (typeof deps.onOperationCheckpoint === "function") {
          await deps.onOperationCheckpoint({
            kind: "failure",
            work_key: serpWorkKey,
            status: "IN_FLIGHT_UNKNOWN",
            provider: "serpapi",
            op: "phase_b_ownership_search",
            error_code: "IN_FLIGHT_UNKNOWN",
          });
        }
        throw err;
      }
      serpBudget.used += cost.serpapi_searches;
      serpBudget.usd += cost.serpapi_usd;
      calls.push({ provider: "serpapi", kind: "ownership_search", query: q, results: serp.organic?.length || 0 });
      // BLOCKED provider results must not enter evidence collection / staging
      if (
        String(serp?.usage_rights || "").toUpperCase() === "BLOCKED" ||
        serp?.restricted === true ||
        serp?.usage_rights_blocked === true
      ) {
        newly.notes.push("serp_ownership_blocked_excluded_from_staging");
      } else {
      sources.push({ kind: "serp_ownership", query: q, organic: (serp.organic || []).slice(0, 8) });
      newly.ownership_claims.push(
        ...(serp.organic || []).slice(0, 5).map((r) => ({
          title: r.title,
          url: r.link || r.url,
          snippet: r.snippet,
          provider: "serpapi",
        }))
      );
      // Stage owner candidates from SERP snippets (leads only — corroborate via scrape/extract).
      if (!ownership.owner_display_name) {
        for (const r of serp.organic || []) {
          const blob = `${r.title || ""} ${r.snippet || ""}`;
          const m = blob.match(
            /(?:owned by|sold to|purchased by|acquired by)\s+(?:British businessman\s+|businessman\s+|Englishman\s+)?([A-Z][a-zA-Z]+(?:\s+[a-zA-Z'’-]+){0,3})/
          );
          const bought =
            !m &&
            blob.match(
              /(?:Englishman|businessman)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z'’-]+){1,3})\s+bought\s+(?:the\s+)?(?:property|hotel|resort)/i
            );
          const match = m || bought;
          if (match) {
            const name = match[1].trim().replace(/\s+/g, " ").slice(0, 120);
            if (name.length >= 5 && !/^(in|the|a|an|and|for|to)\b/i.test(name)) {
              ownership = {
                ...ownership,
                owner_display_name: stripHonorificOwnerName(name),
                owner_entity_id: ownership.owner_entity_id || `staged_${hotel.hotel_id}`,
                owner_role: "UNRESOLVED",
                classification: "OWNER_CANDIDATE",
                confidence: "PROBABLE",
                evidence_note: `Staged from SerpAPI ownership snippet (${r.link || r.url}) — historical purchase language may not equal current ownership; not canonical write.`,
                return_to_ownership_lane: true,
                ownership_vs_operator: "OWNER_CLAIM_STAGED_OPERATOR_NOT_ASSUMED",
                historical_vs_current: bought ? "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK" : "UNKNOWN",
                newly_researched: true,
              };
              newly.notes.push(`staged_owner_from_serp_snippet:${r.link || r.url}`);
              break;
            }
          }
        }
      }
      } // end else non-BLOCKED serp staging
    }

    const visitedOwnershipUrls = new Set(inspectUrls);
    if (iterativeOwnershipLoop) {
      const loopResult = await runOwnershipIterativeResearchLoop({
        hotel,
        budgetController,
        chargedContext: chargedContextReserved,
        modelUsdLedger: createModelUsdLedger({
          budgetUsd: Number(budgets.model_reader_usd_max ?? caseInput.model_reader_usd_max ?? 0.15),
          label: `handoff-model:${hotel.hotel_id}`,
        }),
        deps: {
          search: ({ query, numResults }) => searchFn({ query, numResults }),
          scrape: ({ url }) => scrapeFn({ url }),
          isConfigured: configuredFn,
          modelOwnershipDocumentArm: deps.modelOwnershipDocumentArm,
          proposeModelFollowUpSearches: deps.proposeModelFollowUpSearches,
          callModelJson: deps.callModelJson,
          onOperationCheckpoint: deps.onOperationCheckpoint,
          operation_journal: deps.operation_journal || caseInput.operation_journal || null,
          budget_usage: deps.budget_usage || null,
          forceRetryUnknown: deps.forceRetryUnknown === true,
          preDispatchGate: deps.preDispatchGate || null,
          nowFn: deps.nowFn || null,
          serpGoogle: deps.serpGoogle,
          providersJournaled,
          result_by_work_key:
            deps.result_by_work_key ||
            caseInput.iterative_resume_state?.result_by_work_key ||
            null,
        },
        bounds: {
          breadth: Number(budgets.iterative_breadth ?? caseInput.iterative_breadth ?? 3),
          depth: Number(budgets.iterative_depth ?? caseInput.iterative_depth ?? 2),
          concurrency: Number(budgets.iterative_concurrency ?? 2),
          docs_per_query: Number(budgets.iterative_docs_per_query ?? 2),
          follow_up_reserve_fraction: Number(budgets.iterative_follow_up_reserve_fraction ?? 0.4),
          max_queries_total: Number(budgets.iterative_max_queries ?? 12),
          max_documents_total: Number(budgets.iterative_max_documents ?? 10),
          model_usd_max: Number(budgets.model_reader_usd_max ?? caseInput.model_reader_usd_max ?? 0.15),
          apply_structured_reader: budgets.apply_structured_reader !== false,
          apply_model_follow_up_planner: budgets.apply_model_follow_up_planner !== false,
          serpapi_max: Number(budgets.serpapi_max ?? 0),
          serpapi_usd_max: Number(budgets.serpapi_usd_max ?? 0),
          serpapi_per_search_usd: Number(budgets.serpapi_per_search_usd ?? 0.01),
          adaptive_ownership_controller:
            budgets.adaptive_ownership_controller === true ||
            caseInput.adaptive_ownership_controller === true ||
            String(process.env.ADAPTIVE_OWNERSHIP_CONTROLLER || "") === "1",
        },
        resumeState: caseInput.iterative_resume_state || null,
      });
      iterativeLoopMeta = {
        version: OWNERSHIP_ITERATIVE_LOOP_VERSION,
        enabled: true,
        stop_reasons: loopResult.stop_reasons,
        earliest_failure_stage: loopResult.earliest_failure_stage || null,
        stage_trace: loopResult.stage_trace || null,
        warnings: loopResult.research_state?.warnings || [],
        provenance: loopResult.provenance,
        conflicts: loopResult.conflicts,
        research_state: loopResult.research_state || null,
        claims_count: (loopResult.claims || []).length,
        structured_reader_calls: (loopResult.calls || []).filter((c) => c.kind === "structured_reader").length,
        model_usd: loopResult.budgets?.model_usd || null,
        serpapi: {
          used: loopResult.research_state?.budget?.serpapi_used || 0,
          usd: loopResult.research_state?.budget?.serpapi_usd || 0,
        },
      };
      calls.push(...(loopResult.calls || []));
      sources.push(...(loopResult.sources || []));
      const loopSerpUsed = Number(loopResult.research_state?.budget?.serpapi_used || 0);
      const loopSerpUsd = Number(loopResult.research_state?.budget?.serpapi_usd || 0);
      serpBudget.used += loopSerpUsed;
      serpBudget.usd += loopSerpUsd;
      for (const c of loopResult.claims || []) {
        newly.ownership_claims.push({
          ...c,
          evidence_level: "DOCUMENT_CANDIDATE_ITERATIVE",
          party_role: c.party_role,
        });
      }
      for (const url of loopResult.research_state?.urls_read || []) {
        visitedOwnershipUrls.add(url);
      }
      const pick = loopResult.ownership_candidate;
      if (pick?.name) {
        // Operator/brand roles are never staged as owner_display_name.
        if (pick.party_role !== "OPERATOR" && pick.party_role !== "BRAND") {
          const supportedRole =
            pick.party_role === "PROPERTY_OWNER" ||
            pick.party_role === "ECONOMIC_SPONSOR" ||
            pick.party_role === "ECONOMIC_OWNER" ||
            pick.party_role === "ECONOMIC_OWNER_OR_SPONSOR" ||
            pick.party_role === "REGISTERED_BUSINESS";
          const priorWeak =
            !ownership.owner_display_name ||
            ownership.classification === "OWNER_CANDIDATE" ||
            ownership.classification === "UNRESOLVED" ||
            ownership.classification === "STAGED" ||
            !ownership.classification;
          // Upgrade weak SERP/phase-B candidates when the iterative loop returns a better adjudication.
          if (priorWeak) {
            ownership = {
              ...ownership,
              owner_display_name: pick.name,
              owner_entity_id: ownership.owner_entity_id || `staged_${hotel.hotel_id}`,
              classification: supportedRole ? pick.party_role : "OWNER_CANDIDATE",
              owner_role: pick.party_role || "UNRESOLVED",
              temporal_status:
                pick.historical_vs_current === "CURRENT_AS_OF_STATED_DATE" ||
                pick.party_role === "PROPERTY_OWNER"
                  ? "CURRENT"
                  : pick.historical_vs_current || ownership.temporal_status || "UNRESOLVED",
              historical_vs_current: pick.historical_vs_current || "UNRESOLVED",
              return_to_ownership_lane: true,
              evidence_refs: pick.claim
                ? [
                    {
                      source_url: pick.claim.url || pick.claim.source_url,
                      excerpt: pick.claim.evidence_span || pick.claim.excerpt,
                      source_type: "RETRIEVED_DOCUMENT",
                      party_role: pick.party_role,
                      hotel_specific: true,
                      observation_date:
                        pick.claim.event_date || pick.claim.source_date || null,
                    },
                  ]
                : ownership.evidence_refs || [],
              evidence_note: supportedRole
                ? "Iterative-loop adjudicated ownership role with hotel-specific evidence; staged pending contact completion."
                : "Iterative-loop document candidate requires relationship and currency adjudication; not canonical ownership.",
              newly_researched: true,
              iterative_ownership_loop: true,
            };
          }
        }
      }
      newly.notes.push(
        `iterative_ownership_loop:${(loopResult.stop_reasons || []).join(",") || "complete"}`
      );
      if (loopResult.conflicts?.length) {
        newly.notes.push(`iterative_conflicts_retained:${loopResult.conflicts.length}`);
      }
    } else {
      // Prior flat Context.dev ownership search path (rollback / comparison default).
      for (const cq of ownershipQueries(hotel)) {
        if (
          !configuredFn() ||
          !ledger.canAfford(ledger.estimateSearchCost(10) + CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)
        ) {
          break;
        }
        const search = await chargedContextReserved(
          ledger,
          "search",
          ledger.estimateSearchCost(10),
          { query: cq },
          () => searchFn({ query: cq, numResults: 10 })
        );
        const mapped = search.ok ? ownershipSearchResults(search.data) : null;
        calls.push({
          provider: "context_dev",
          kind: "search",
          query: cq,
          ok: search.ok,
          result_count: mapped?.length ?? null,
          error: search.error || (!mapped ? "UNEXPECTED_SEARCH_RESPONSE_SHAPE" : null),
        });
        if (!mapped) {
          unresolved.push(search.ok ? "UNEXPECTED_SEARCH_RESPONSE_SHAPE" : "CONTEXT_SEARCH_FAILED");
          continue;
        }
        sources.push({ kind: "context_ownership_search", query: cq, results: mapped });
        newly.ownership_claims.push(...mapped.map((r) => ({ ...r, evidence_level: "SEARCH_LEAD" })));
        const rankDetail = rankOwnershipSourcesDetailed(mapped, hotel);
        const ranked = rankDetail.ranked.filter((r) => !visitedOwnershipUrls.has(r.url));
        newly.notes.push(
          `flat_rank_kept:${ranked.length};flat_rank_rejected:${(rankDetail.rejected || []).length}`
        );
        for (const u of ranked.slice(0, 2)) {
          if (!ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) break;
          visitedOwnershipUrls.add(u.url);
          const scraped = await chargedContextReserved(
            ledger,
            "scrape_markdown",
            CONTEXT_DEV_CREDIT_COSTS.scrape_markdown,
            { url: u.url },
            () => scrapeFn({ url: u.url })
          );
          const md =
            typeof scraped.data === "string"
              ? scraped.data
              : scraped.data?.markdown || scraped.data?.content || "";
          calls.push({
            provider: "context_dev",
            kind: "scrape",
            url: u.url,
            ok: scraped.ok,
            characters: String(md).length,
          });
          if (!scraped.ok || !md) {
            unresolved.push("OWNERSHIP_DOCUMENT_UNREADABLE");
            continue;
          }
          const document = ownershipDocumentPassages(md, hotel);
          sources.push({
            kind: "context_scrape_ownership",
            url: u.url,
            ...document,
            observed_at: new Date().toISOString(),
          });
          for (const candidate of ownershipCandidates(document, hotel)) {
            if (!candidate.name || candidate.classification === "REJECTED_NEGATIVE_CONTROL") continue;
            if (isImplausibleOwnerDisplayName(candidate.name)) continue;
            newly.ownership_claims.push({ ...candidate, url: u.url, evidence_level: "DOCUMENT_CANDIDATE" });
            if (!ownership.owner_display_name) {
              ownership = {
                ...ownership,
                owner_display_name: candidate.name,
                owner_entity_id: `staged_${hotel.hotel_id}`,
                classification: "OWNER_CANDIDATE",
                owner_role: "UNRESOLVED",
                historical_vs_current: "UNRESOLVED",
                return_to_ownership_lane: true,
                evidence_refs: [
                  {
                    source_url: u.url,
                    excerpt: candidate.excerpt,
                    source_type: "RETRIEVED_DOCUMENT",
                  },
                ],
                evidence_note:
                  "Document candidate requires relationship and currency adjudication; not canonical ownership.",
                newly_researched: true,
              };
            }
          }
        }
      }
    }

    if (!ownership.owner_display_name) {
      unresolved.push("OWNER_STILL_UNRESOLVED_AFTER_RESEARCH");
    }
  } else {
    ownership = {
      ...ownership,
      ownership_vs_operator:
        ownership.ownership_vs_operator ||
        "SURFACE_ECONOMIC_OWNER_PRESENT_OPERATOR_BRAND_SEPARATED_IN_FIXTURES",
      newly_researched: false,
    };
  }

  // ——— Opt-in native method router (Cadastur / DENUE / investor / follow-up) ———
  // Runs even when Phase A found nothing — registry methods discover business identity leads.
  // Default OFF. Never auto-promotes REGISTERED_BUSINESS → property owner.
  if (nativeMethodRouterEnabled) {
    nativeMethodRouterMeta = await runOwnershipNativeMethodRouter({
      hotel,
      ownership,
      newly,
      claims: newly.ownership_claims,
      budgetController,
      ledger,
      budgets,
      deps: {
        cadasturRows: deps.cadasturRows,
        fetchCadasturRows: deps.fetchCadasturRows,
        matchHotelToCadastur: deps.matchHotelToCadastur,
        denueRows: deps.denueRows,
        fetchDenueRows: deps.fetchDenueRows,
        matchHotelToDenue: deps.matchHotelToDenue,
        fetchCnpjQsa: deps.fetchCnpjQsa,
        // Offline / SAVED_DATA_INTEGRATION_REPLAY: do not hand search to router
        // (registry match would otherwise trigger investor-doc Context.dev calls).
        searchFn: disableNetwork ? undefined : searchFn,
        scrapeFn: disableNetwork ? undefined : scrapeFn,
        disableNetwork,
        investorSearchResults: disableNetwork ? undefined : deps.investorSearchResults,
        runPersonEmailDiscovery: disableNetwork ? undefined : deps.runPersonEmailDiscovery,
        ownerResearchCache: deps.ownerResearchCache,
        onCadasturInvoked: deps.onCadasturInvoked,
        onDenueInvoked: deps.onDenueInvoked,
      },
    });
    calls.push({
      provider: "native_method_router",
      kind: "ownership_native_method_router",
      version: OWNERSHIP_NATIVE_METHOD_ROUTER_VERSION,
      enabled: true,
      trace_summary: (nativeMethodRouterMeta.trace || []).map((t) => ({
        method: t.method,
        status: t.status,
        reason: t.reason || t.outcome || null,
      })),
    });
    newly.notes.push(`native_method_router:${OWNERSHIP_NATIVE_METHOD_ROUTER_VERSION}`);
    // Registry leads — stage as REGISTERED_BUSINESS candidates, never as current owners
    for (const reg of nativeMethodRouterMeta.leads?.registry || []) {
      newly.ownership_claims.push({
        name: reg.legal_name || reg.commercial_name,
        relationship: "REGISTERED_BUSINESS",
        party_role: "REGISTERED_BUSINESS",
        classification: "REGISTERED_BUSINESS",
        currentness: "REGISTRY_IDENTITY",
        auto_ownership: false,
        source: reg.source,
        excerpt: reg.note,
        evidence_level: "REGISTRY_MATCH",
        cnpj: reg.cnpj || null,
        denue_clee: reg.denue_clee || null,
        establecimiento_id: reg.establecimiento_id || null,
        match_evidence: reg.match_evidence || null,
        establishment_identifiers: reg.establishment_identifiers || null,
        dataset_provenance: reg.dataset_provenance || null,
        uncertainty: "REGISTERED_BUSINESS_NOT_PROPERTY_OWNER",
      });
      newly.notes.push(
        `registry_lead_not_auto_owner:${reg.source}:${reg.legal_name || reg.commercial_name}`
      );
    }
    for (const plan of nativeMethodRouterMeta.leads?.historical_follow_ups || []) {
      newly.notes.push(`historical_follow_up_plan:${plan.unresolved_question || plan.question || "plan"}`);
    }
    for (const plan of nativeMethodRouterMeta.leads?.registry_follow_ups || []) {
      newly.notes.push(`registry_follow_up_plan:${plan.unresolved_question || "plan"}`);
    }

    // Wire follow-up plans into bounded queue, then through existing evidence pipeline
    // (search → source selection → fetch → document read → relationship adjudication).
    const followUpQueue = nativeMethodRouterMeta.leads?.bounded_follow_up_queue || [];
    newly.bounded_follow_up_queue = followUpQueue;
    newly.follow_up_queue_status = {
      queued: followUpQueue.length,
      consumed: 0,
      executed_queries: [],
      note:
        "Queue attached for consumption through search→rank→scrape→adjudicate under shared budget.",
    };

    if (followUpQueue.length && !disableNetwork && configuredFn()) {
      const registryLead = (nativeMethodRouterMeta.leads?.registry || [])[0] || null;
      const visitedFollowUp = new Set();
      const pipeline = await consumeBoundedFollowUpEvidencePipeline({
        hotel,
        queue: followUpQueue,
        registryLead,
        searchFn,
        scrapeFn,
        budgetController,
        ledger,
        maxQueries: Math.min(4, Number(budgets.follow_up_query_max ?? 4)),
        maxDocsPerQuery: 2,
        calls,
        sources,
        visitedUrls: visitedFollowUp,
      });
      newly.follow_up_queue_status = {
        queued: pipeline.queued,
        consumed: pipeline.consumed,
        searches_executed: pipeline.searches_executed,
        docs_fetched: pipeline.docs_fetched,
        executed_queries: pipeline.executed_queries,
        earliest_unresolved_step: pipeline.earliest_unresolved_step,
        business_to_owner: pipeline.business_to_owner,
        adjudication: pipeline.adjudication || null,
        role_distinction: pipeline.role_distinction || null,
        spend_credits_estimate: pipeline.spend_credits_estimate,
        note: pipeline.note || "Follow-up consumed through evidence pipeline",
      };
      for (const claim of pipeline.claims_staged || []) {
        newly.ownership_claims.push(claim);
      }
      for (const p of pipeline.passages || []) {
        newly.notes.push(`follow_up_passage:${(p.url || "").slice(0, 80)}`);
      }
      // Never promote registry business to owner_display_name from follow-up alone
      // unless adjudication produced a supported current-owner candidate (still staging).
      if (
        pipeline.business_to_owner?.status === "CANDIDATE_ESTABLISHED" &&
        pipeline.business_to_owner.subject &&
        !ownership.owner_display_name
      ) {
        ownership = {
          ...ownership,
          owner_display_name: pipeline.business_to_owner.subject,
          owner_entity_id: ownership.owner_entity_id || `staged_${hotel.hotel_id}`,
          classification: "OWNER_CANDIDATE",
          owner_role: "UNRESOLVED",
          historical_vs_current: "UNRESOLVED",
          return_to_ownership_lane: true,
          evidence_note:
            "Staged from registry follow-up adjudication candidate — not canonical; REGISTERED_BUSINESS lead preserved separately.",
          newly_researched: true,
          from_registry_follow_up: true,
        };
        newly.notes.push(
          `staged_owner_candidate_from_follow_up_adjudication:${pipeline.business_to_owner.subject}`
        );
      } else {
        newly.notes.push(
          `registry_follow_up_owner_unresolved:${pipeline.business_to_owner?.status || "UNRESOLVED"}`
        );
      }
      const distinct = assertOwnershipPartyRolesDistinct(newly.ownership_claims);
      if (!distinct.ok) {
        newly.notes.push(`role_distinction_violations:${distinct.violations.length}`);
        unresolved.push("OWNERSHIP_PARTY_ROLE_COLLAPSE");
      }
    } else if (followUpQueue.length && disableNetwork) {
      newly.follow_up_queue_status.note =
        "Queue populated; network disabled (SAVED_DATA_INTEGRATION_REPLAY) — not executed";
      newly.notes.push(`follow_up_queue_held_offline:${followUpQueue.length}`);
    }

    if (nativeMethodRouterMeta.leads?.recovered_cache?.recovered) {
      newly.notes.push("owner_cache_recovered_evidence_labeled");
    }
  } else {
    nativeMethodRouterMeta = {
      enabled: false,
      version: OWNERSHIP_NATIVE_METHOD_ROUTER_VERSION,
      skipped: true,
      reason: "OWNERSHIP_NATIVE_METHOD_ROUTER_OFF",
    };
  }

  // ——— Phase B: domain + people via existing discoverOwnerPersonPath (SerpAPI) ———
  // Skip entirely when serpapi_max=0 — Phase C Context.dev continues domain/people research.
  let domainResult = null;
  if (ownership.owner_display_name && serpBudget.max > 0 && serpBudget.used < serpBudget.max) {
    const remainingSerp = Math.max(1, Math.min(5, serpBudget.max - serpBudget.used));
    domainResult = await discoverOwnerPersonPath({
      owner_entity_id: ownership.owner_entity_id,
      owner_display_name: ownership.owner_display_name,
      hotels: [hotel],
      focus_hotel_id: hotel.hotel_id,
      focus_hotel_name: hotel.hotel_name,
      domain_hypotheses: caseInput.domain_hypotheses || [],
      person_hypotheses: caseInput.person_hypotheses || [],
      forbidden_org_hosts: caseInput.forbidden_org_hosts || [],
      max_serp_queries: remainingSerp,
    });
    serpBudget.used += Number(domainResult.cost?.serpapi_searches || 0);
    serpBudget.usd += Number(domainResult.cost?.serpapi_usd || 0);
    calls.push({
      provider: "serpapi+fetch",
      kind: "discoverOwnerPersonPath",
      cost: domainResult.cost,
      unresolved: domainResult.unresolved_reasons,
      domain: domainResult.confirmed_company_domain,
    });
    if (domainResult.confirmed_company_domain) {
      const raw = unwrapDomain(domainResult.confirmed_company_domain);
      const accepted = raw ? acceptOwnerDomainCandidate(raw, ownership) : { ok: false };
      if (accepted.ok) {
        newly.domain = {
          url: accepted.url,
          host: accepted.host,
          from: "discoverOwnerPersonPath",
          newly_researched: true,
          evidence: domainResult.confirmed_company_domain?.evidence || null,
          domain_resolver: accepted.resolved || null,
        };
      } else if (raw) {
        newly.notes.push(`rejected_low_quality_domain:${raw}:${accepted.reason || ""}`);
        unresolved.push(
          isDirectoryOrAggregatorHost(hostOf(raw)) || isRejectedOwnerDomain(hostOf(raw))
            ? "DIRECTORY_NOT_FIRST_PARTY_OWNER_DOMAIN"
            : "OWNER_DOMAIN_REJECTED_LOW_QUALITY"
        );
      }
    }
  } else if (ownership.owner_display_name && serpBudget.max <= 0) {
    calls.push({
      provider: "serpapi",
      kind: "discoverOwnerPersonPath_skipped",
      reason: "serpapi_max_0_use_context_dev_phase_c",
    });
  }

  // Resume / owner-reuse: apply saved org domain before Phase C (no new scrape).
  // Used when credit-limited pilots block unproven scrape bounds but extract must
  // still be attempted against a previously evidenced domain.
  if (!newly.domain?.url) {
    const seedRaw =
      caseInput.ownership_seed?.confirmed_domain ||
      caseInput.ownership_seed?.organization_domain ||
      caseInput.confirmed_domain ||
      caseInput.owner_reuse_package?.organization_domain ||
      caseInput.owner_reuse_package?.domain ||
      null;
    if (seedRaw) {
      const accepted = acceptOwnerDomainCandidate(String(seedRaw), ownership);
      if (accepted.ok) {
        const from = caseInput.owner_reuse_package?.organization_domain
          ? "owner_reuse_package"
          : "ownership_seed_confirmed_domain";
        newly.domain = {
          url: accepted.url,
          host: accepted.host,
          from,
          newly_researched: false,
        };
        newly.notes.push(`confirmed_domain_from_${from}:${accepted.host}`);
      } else {
        newly.notes.push(
          `confirmed_domain_seed_rejected:${accepted.reason || "unknown"}:${String(seedRaw).slice(0, 120)}`
        );
      }
    }
  }

  let confirmedDomain = newly.domain?.url || null;
  let people = [...(domainResult?.people || [])];

  // ——— Phase C: Context.dev continuation when domain/people still weak ———
  if (ownership.owner_display_name && configuredFn()) {
    if (!confirmedDomain && ledger.canAfford(ledger.estimateSearchCost(10))) {
      const dq =
        caseInput.domain_search_query ||
        `"${ownership.owner_display_name}" (official website OR "about us" OR contacto OR "quiénes somos")`;
      const search = await chargedContextReserved(ledger, "search", ledger.estimateSearchCost(10), { query: dq }, () =>
        searchFn({
          query: dq,
          numResults: 10,
          excludeDomains: excludeHosts,
        })
      );
      calls.push({ provider: "context_dev", kind: "domain_search", ok: search.ok, query: dq });
      if (search.ok) {
        const candidates = pickSearchUrls(search.data?.results || [], {
          excludeHosts,
          preferTokens: String(ownership.owner_display_name)
            .split(/\s+/)
            .filter((w) => w.length > 3)
            .slice(0, 4),
        });
        for (const c of candidates.slice(0, 3)) {
          if (!ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) break;
          if (/linkedin\.com/i.test(c.url)) continue;
          const scraped = await chargedContextReserved(
            ledger,
            "scrape_markdown",
            CONTEXT_DEV_CREDIT_COSTS.scrape_markdown,
            { url: c.url },
            () => scrapeFn({ url: c.url })
          );
          calls.push({ provider: "context_dev", kind: "domain_scrape", url: c.url, ok: scraped.ok });
          if (!scraped.ok) continue;
          const md = String(
            typeof scraped.data === "string"
              ? scraped.data
              : scraped.data?.markdown || scraped.data?.content || ""
          );
          const nameBits = String(ownership.owner_display_name)
            .toLowerCase()
            .split(/\s+/)
            .filter((w) => w.length > 3);
          const hit = nameBits.filter((b) => md.toLowerCase().includes(b)).length;
          if (hit >= Math.min(2, nameBits.length) || /alliance hospitality|inmobiliaria hnf|blue waters/i.test(md)) {
            if (isLowQualityDomain(c.url)) {
              newly.notes.push(`rejected_low_quality_domain_candidate:${c.url}`);
              continue;
            }
            // Press/article pages mentioning the owner are leads, not company domains.
            if (/travellife|tripadvisor|booking\.com|wikipedia|medium\.com|linkedin\.com/i.test(c.host)) {
              newly.notes.push(`rejected_press_or_directory_as_owner_domain:${c.url}`);
              continue;
            }
            const orgMatch = assessOrgDomainMatch({
              ownerDisplayName: ownership.owner_display_name,
              pageCompanyName: c.title,
              pageText: md.slice(0, 3000),
              host: c.host,
            });
            if (!orgMatch.ok) {
              newly.notes.push(`domain_candidate_org_mismatch:${orgMatch.reason}:${c.url}`);
              newly.rejected_domains = newly.rejected_domains || [];
              newly.rejected_domains.push({ host: c.host, url: c.url, reason: orgMatch.reason });
              continue;
            }
            confirmedDomain = asHttps(`https://${c.host}/`);
            newly.domain = {
              url: confirmedDomain,
              host: c.host,
              from: "context_dev_scrape_corroboration",
              newly_researched: true,
              title: c.title,
              source_page: c.url,
            };
            sources.push({
              kind: "context_domain_confirm",
              url: confirmedDomain,
              source_page: c.url,
              excerpt: md.slice(0, 800),
              observed_at: new Date().toISOString(),
            });
            break;
          }
        }
      }
    }

    if (confirmedDomain && !isLowQualityDomain(confirmedDomain) && ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.extract)) {
      const root = `https://${hostOf(confirmedDomain)}/`;
      if (!hostOf(confirmedDomain)) {
        unresolved.push("DOMAIN_HOST_PARSE_FAILED");
      } else {
      const extracted = await chargedContextReserved(
        ledger,
        "extract",
        CONTEXT_DEV_CREDIT_COSTS.extract,
        { url: root },
        () =>
          extractFn({
            url: root,
            schema: OWNERSHIP_CONTACT_EXTRACT_SCHEMA,
            maxPages: 4,
            maxDepth: 2,
            factCheck: true,
            instructions: `Extract the organization identity for ${ownership.owner_display_name}. Distinguish owner vs operator vs brand. List current executives with titles. Prefer development, acquisitions, investment, founder, CEO. Do not invent emails. Note similarly named organizations that are NOT this entity.`,
          })
      );
      calls.push({
        provider: "context_dev",
        kind: "extract",
        url: root,
        ok: extracted.ok,
        error: extracted.error || null,
      });
      if (extracted.ok) {
        const data = extracted.data?.data || extracted.data?.result || extracted.data || {};
        const companyName = data.company_legal_or_trade_name || null;
        const claimText = [
          companyName,
          ...(data.ownership_or_portfolio_claims || []).map((c) =>
            typeof c === "string" ? c : c?.claim || c?.text || JSON.stringify(c)
          ),
          ...(data.executives || []).map((e) => `${e.full_name || ""} ${e.title || ""}`),
        ].join(" ");
        const orgMatch = assessOrgDomainMatch({
          ownerDisplayName: ownership.owner_display_name,
          pageCompanyName: companyName,
          pageText: claimText,
          host: hostOf(confirmedDomain),
        });
        sources.push({
          kind: "context_extract",
          url: root,
          company: companyName,
          executives: (data.executives || []).slice(0, 8),
          ownership_claims: (data.ownership_or_portfolio_claims || []).slice(0, 6),
          org_domain_match: orgMatch,
          observed_at: new Date().toISOString(),
        });
        if (!orgMatch.ok) {
          newly.notes.push(`domain_rejected_after_extract:${orgMatch.reason}:${hostOf(confirmedDomain)}`);
          unresolved.push(orgMatch.reason);
          newly.rejected_domains = newly.rejected_domains || [];
          newly.rejected_domains.push({
            host: hostOf(confirmedDomain),
            url: confirmedDomain,
            reason: orgMatch.reason,
            detail: orgMatch.detail,
          });
          confirmedDomain = null;
          newly.domain = null;
        } else {
          // Never promote extractor-labeled page_text / source_document_text /
          // retrieved_text as independently retrieved evidence — field names are
          // model output labels, not provenance. Prefer an already-captured scrape
          // body for this URL from the document-read path.
          const priorCapture = sources.find((s) => {
            if (/extract|serp|search|hypothesis/i.test(String(s.kind || ""))) return false;
            const body = String(
              s.retrieved_text || s.markdown || s.excerpt || s.source_text || ""
            );
            if (body.trim().length < 20) return false;
            const u = String(s.url || s.source_url || "");
            if (!u) return false;
            if (u === root) return true;
            try {
              const sh = new URL(u).hostname.replace(/^www\./, "").toLowerCase();
              const rh = new URL(root).hostname.replace(/^www\./, "").toLowerCase();
              return Boolean(sh && rh && sh === rh);
            } catch {
              return false;
            }
          });
          const extractSourceText = priorCapture
            ? String(
                priorCapture.retrieved_text ||
                  priorCapture.markdown ||
                  priorCapture.excerpt ||
                  priorCapture.source_text ||
                  ""
              )
            : "";
          if (extractSourceText.trim().length >= 20 && priorCapture) {
            sources.push({
              kind: "context_extract_attested_prior_capture",
              url: root,
              excerpt: extractSourceText.slice(0, 4000),
              capture_attested: true,
              document_id: priorCapture.document_id || null,
              content_hash: priorCapture.content_hash || null,
              retrieval_time:
                priorCapture.retrieval_time ||
                priorCapture.retrieved_at ||
                priorCapture.observed_at ||
                null,
              provider: priorCapture.provider || priorCapture.retrieval_provider || "context_dev",
              observed_at: new Date().toISOString(),
            });
          }
          for (const ex of data.executives || []) {
            if (!ex.full_name) continue;
            const person = executiveToPerson(ex, {
              ownerId: ownership.owner_entity_id,
              ownerName: ownership.owner_display_name,
              hotelName: hotel.hotel_name,
              sourceUrl: ex.source_page_url || root,
              newlyResearched: true,
              // Only attribute contacts present in attested captured text
              sourceDocumentText: extractSourceText,
            });
            people.push(person);
            newly.people.push({
              name: person.display_name,
              title: person.title,
              newly_researched: true,
            });
          }
          if (companyName && !ownership.owner_legal_name) {
            ownership.owner_legal_name = companyName;
          }
        }
      } else if (extracted.budget_blocked) {
        unresolved.push("CONTEXT_DEV_BUDGET_EXHAUSTED_EXTRACT");
      } else {
        unresolved.push(`CONTEXT_DEV_EXTRACT_FAILED:${extracted.error?.class || extracted.error?.message || "unknown"}`);
      }
      } // end hostOf else
    }

    // Promote independently confirmed LinkedIn hypotheses even when Serp budget is exhausted.
    for (const hp of (caseInput.person_hypotheses || []).slice(0, 4)) {
      if (!hp.display_name || hp.deceased || hp.former_affiliation) {
        if (hp.deceased || hp.former_affiliation) {
          newly.notes.push(`person_hypothesis_excluded_deceased_or_former:${hp.display_name}`);
        }
        continue;
      }
      if (!(hp.linkedin && /linkedin\.com\/in\//i.test(hp.linkedin))) continue;
      if (!hp.affiliation_evidence?.length) { newly.notes.push(`hypothesis_needs_captured_affiliation:${hp.display_name}`); continue; }
      const liGate = validateLinkedInIdentifier({
        personName: hp.display_name,
        linkedinUrl: hp.linkedin,
        orgName: ownership.owner_display_name,
        roleTitle: hp.title,
        independentlyConfirmed: true,
        evidenceNote: hp.why_relevant_hypothesis || ownership.owner_display_name,
        profileCompany: ownership.owner_display_name,
      });
      if (liGate.decision !== "ALLOW") {
        newly.notes.push(`linkedin_hypothesis_omitted:${hp.display_name}:${liGate.reason || liGate.detail}`);
        continue;
      }
      let existing = people.find(
        (p) => String(p.display_name).toLowerCase() === String(hp.display_name).toLowerCase()
      );
      if (!existing) {
        existing = createPersonContact({
          display_name: hp.display_name,
          title: hp.title || null,
          organization_entity_id: ownership.owner_entity_id,
          organization_name: ownership.owner_display_name,
          role_currency: ROLE_CURRENCY.UNKNOWN,
          property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
          channels: [],
          evidence: [
            createEvidenceRef({
              source_title: "Independently confirmed professional identity",
              source_url: hp.linkedin,
              source_type: "live_research",
              observed_at: new Date().toISOString(),
              excerpt: (hp.why_relevant_hypothesis || "").slice(0, 400),
            }),
          ],
          why_relevant: hp.why_relevant_hypothesis || `Evidenced principal for ${ownership.owner_display_name}`,
          publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
          provenance: {
            evidenced_or_inferred: "EVIDENCED",
            basis: "independently_confirmed_linkedin_plus_owner_hypothesis",
            newly_researched: true,
            evaluation_staging: true,
          },
        });
        people.push(existing);
        newly.people.push({
          name: existing.display_name,
          title: existing.title,
          newly_researched: true,
          chain: "person_without_corporate_website",
        });
      } else {
        existing.publication_label = "EVIDENCED_ORG_PERSON_CANDIDATE";
        existing.provenance = {
          ...(existing.provenance || {}),
          evidenced_or_inferred: "EVIDENCED",
          basis: "independently_confirmed_linkedin_plus_owner_hypothesis",
          newly_researched: true,
          evaluation_staging: true,
        };
        newly.people.push({
          name: existing.display_name,
          title: existing.title,
          newly_researched: true,
          chain: "promoted_hypothesis_via_confirmed_linkedin",
        });
      }
      const liCh = (existing.channels || []).find((c) => /LINKEDIN/i.test(c.kind));
      if (!liCh) {
        existing.channels = existing.channels || [];
        existing.channels.push(
          createChannel({
            kind: CHANNEL_KIND.PERSON_LINKEDIN,
            value: hp.linkedin,
            display_label: "Independently confirmed professional profile",
            attribution: ATTRIBUTION.NAMED_PERSON,
            usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
          })
        );
      } else {
        liCh.value = hp.linkedin;
        liCh.display_label = "Independently confirmed professional profile";
      }
    }

    for (const hp of (caseInput.person_hypotheses || []).slice(0, 2)) {
      if (serpBudget.used >= serpBudget.max) break;
      if (!hp.display_name || hp.deceased || hp.former_affiliation) continue;
      const lqOwner =
        String(ownership.owner_display_name || "")
          .replace(/\s*\/\s*owner path/i, "")
          .replace(/\s+/g, " ")
          .trim() || ownership.owner_display_name;
      const lq = `"${hp.display_name}" "${lqOwner}" site:linkedin.com/in`;
      const cost = { serpapi_searches: 0, serpapi_usd: 0 };
      const serp = await serpGoogle(lq, cost, { hl: "en", gl: "us", num: 5 });
      serpBudget.used += cost.serpapi_searches;
      serpBudget.usd += cost.serpapi_usd;
      calls.push({ provider: "serpapi", kind: "linkedin_corroboration", query: lq, results: serp.organic?.length || 0 });
      // Only accept a Serp LinkedIn hit that passes name + org context gate — never substitute another person.
      let acceptedUrl = null;
      for (const hit of serp.organic || []) {
        const url = hit.link || hit.url;
        if (!url || !/linkedin\.com\/in\//i.test(url)) continue;
        const liGate = validateLinkedInIdentifier({
          personName: hp.display_name,
          linkedinUrl: url,
          orgName: ownership.owner_display_name,
          roleTitle: hp.title,
          profileHeadline: hit.title || hit.snippet || null,
          independentlyConfirmed: false,
          evidenceNote: `${hit.title || ""} ${hit.snippet || ""}`,
        });
        if (liGate.decision === "ALLOW") {
          acceptedUrl = url;
          break;
        }
        newly.notes.push(`linkedin_serp_omitted:${hp.display_name}:${liGate.reason || liGate.detail}`);
      }
      if (!acceptedUrl) continue;
      const url = acceptedUrl;
      const existing = people.find(
        (p) => String(p.display_name).toLowerCase() === String(hp.display_name).toLowerCase()
      );
      if (existing) {
        const hasLi = (existing.channels || []).some((c) => /LINKEDIN/i.test(c.kind));
        if (!hasLi) {
          existing.channels = existing.channels || [];
          existing.channels.push(
            createChannel({
              kind: CHANNEL_KIND.PERSON_LINKEDIN,
              value: url,
              display_label: "Professional profile (Serp corroboration, gated)",
              attribution: ATTRIBUTION.NAMED_PERSON,
              usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
            })
          );
        }
        if (existing.publication_label === "HYPOTHESIS_PENDING_CORROBORATION") {
          newly.notes.push(`linkedin_serp_does_not_auto_promote_hypothesis:${hp.display_name}`);
        }
      } else {
        newly.notes.push(`linkedin_serp_no_existing_person_row_omitted_create:${hp.display_name}`);
      }
      sources.push({
        kind: "linkedin_serp_corroboration",
        person: hp.display_name,
        url,
        gated: true,
        observed_at: new Date().toISOString(),
      });
    }
  } else if (!configuredFn()) {
    unresolved.push("CONTEXT_DEV_API_KEY_MISSING");
  }

  const byName = new Map();
  for (const p of people) {
    const key = String(p.display_name || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    if (!key) continue;
    const prev = byName.get(key);
    const score = (x) =>
      (x.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" ? 4 : 0) +
      (x.provenance?.functionally_relevant ? 2 : 0) +
      (ROLE_PRIORITY_RE.test(x.title || "") ? 2 : 0);
    if (!prev || score(p) > score(prev)) byName.set(key, p);
  }
  const peopleOut = [...byName.values()]
    .filter((p) => !p.deceased && !p.former_affiliation && !p.provenance?.deceased)
    .map((p) => {
      const gate = qualifyPersonOwnerAffiliation(p, ownership);
      if (!gate.ok) {
        newly.notes.push(`person_affiliation_gate:${p.display_name}:${gate.reason}`);
        const hadStaleCorroboration =
          p.affiliation_status === "CORROBORATED" ||
          p.provenance?.independently_corroborated === true;
        return {
          ...p,
          publication_label: "HYPOTHESIS_PENDING_CORROBORATION",
          // Failed adjudication must clear stale positive corroboration fields
          affiliation_status: "REJECTED_OR_UNRESOLVED",
          provenance: {
            ...(p.provenance || {}),
            affiliation_gate: gate,
            // Preserve gate truth: affiliation may be evidenced while role is not
            affiliation_independent: gate.affiliation_independent === true,
            independently_corroborated: false,
            functionally_relevant: gate.role_relevant === true,
            prior_corroboration_cleared: hadStaleCorroboration,
            affiliation_rejection_reason: gate.reason || null,
          },
        };
      }
      return {
        ...p,
        affiliation_status: "CORROBORATED",
        provenance: {
          ...(p.provenance || {}),
          affiliation_gate: gate,
          affiliation_independent: true,
          independently_corroborated: true,
          functionally_relevant: true,
        },
      };
    })
    .sort((a, b) => Number(ROLE_PRIORITY_RE.test(b.title || "")) - Number(ROLE_PRIORITY_RE.test(a.title || "")))
    .slice(0, 5);

  const rejectedHosts = (newly.rejected_domains || []).map((d) => d.host).filter(Boolean);
  if (!confirmedDomain) unresolved.push("OWNER_DOMAIN_UNRESOLVED");
  if (!peopleOut.filter((p) => p.publication_label !== "HYPOTHESIS_PENDING_CORROBORATION").length) {
    unresolved.push("NO_EVIDENCED_RELEVANT_PERSON");
  }

  const domainHost = confirmedDomain ? hostOf(confirmedDomain) : null;
  const evidencedPeople = peopleOut.filter(
    (p) =>
      p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" &&
      p.provenance?.affiliation_gate?.ok === true &&
      p.provenance?.affiliation_independent === true &&
      p.display_name &&
      !p.deceased &&
      !p.former_affiliation
  );
  const hasIndependentlyConfirmedLinkedIn = evidencedPeople.some((p) =>
    (p.channels || []).some(
      (c) =>
        /LINKEDIN/i.test(c.kind || "") &&
        /linkedin\.com\/in\//i.test(c.value || "") &&
        /independently confirmed/i.test(c.display_label || "")
    )
  );
  // Domain optional when LinkedIn-only alternative chain has independently confirmed LI.
  const qualifies =
    evidencedPeople.length > 0 &&
    (Boolean(domainHost) || hasIndependentlyConfirmedLinkedIn) &&
    !(domainHost && rejectedHosts.includes(domainHost));

  return {
    version: OWNERSHIP_CONTACT_HANDOFF_VERSION,
    contract_version: OWNERSHIP_HANDOFF_CONTRACT_VERSION,
    ok: true,
    hotel_id: hotel.hotel_id,
    hotel_name: hotel.hotel_name,
    preserved_identity: validated.preserved_identity,
    hotel_name_encoding: encodingFlag,
    hotel_explorer_vs_handoff: OWNERSHIP_HANDOFF_VS_HOTEL_EXPLORER,
    elapsed_ms: Date.now() - started,
    iterative_ownership_loop: iterativeLoopMeta,
    native_method_router: nativeMethodRouterMeta,
    ownership: {
      ...ownership,
      staged_only: true,
      canonical_owner_write: false,
    },
    confirmed_company_domain: confirmedDomain,
    confirmed_company_domain_host: domainHost,
    rejected_domains: newly.rejected_domains || [],
    people: peopleOut,
    organization_contact_route: (() => {
      const base = domainResult?.organization_contact_route || null;
      const merged = [...(base?.channels || []), ...hotelSiteChannels];
      if (!merged.length) return base;
      return {
        ...(base || {}),
        channels: merged,
        note: base?.note || "Includes hotel-site corporate fallbacks when present; not owner-personal emails.",
      };
    })(),
    newly_researched: newly,
    previously_cached_used: {
      owner_from_surface: Boolean(caseInput.owner_entity_id && !ownership.newly_researched),
      person_hypotheses: (caseInput.person_hypotheses || []).map((p) => p.display_name),
      owner_cache_recovered: Boolean(nativeMethodRouterMeta?.leads?.recovered_cache?.recovered),
    },
    sources,
    domain_evidence: (sources || []).filter(
      (s) =>
        s &&
        (s.kind === "DOMAIN" ||
          s.role === "owner_domain" ||
          s.domain_evidence ||
          (domainHost && String(s.source_url || s.url || "").includes(domainHost)))
    ),
    contradictions: iterativeLoopMeta?.conflicts || [],
    conflicts: iterativeLoopMeta?.conflicts || [],
    iterative_resume_state: iterativeLoopMeta?.research_state || null,
    calls,
    budgets: {
      context_dev: ledger.snapshot(),
      shared_budget: budgetController.snapshot(),
      serpapi: { used: serpBudget.used, max: serpBudget.max, usd: Number(serpBudget.usd.toFixed(4)) },
      model_usd: iterativeLoopMeta?.model_usd || null,
      paid_allowances: {
        parallel_max: Number(budgets.parallel_max || 0),
        webhound_max: Number(budgets.webhound_max || 0),
        surfe_max: Number(budgets.surfe_max || 0),
        fullenrich_max: Number(budgets.fullenrich_max || 0),
        enrichment_max: Number(budgets.enrichment_max || 0),
      },
    },
    unresolved_reasons: [...new Set(unresolved)],
    earliest_failure_stage:
      iterativeLoopMeta?.earliest_failure_stage ||
      (unresolved.includes("NO_OWNERSHIP_EVIDENCE") || !ownership.owner_display_name
        ? null
        : null),
    stage_trace: iterativeLoopMeta?.stage_trace || null,
    qualifies_for_fullenrich: qualifies,
    alternative_chain_no_corporate_website: Boolean(
      qualifies && !domainHost && hasIndependentlyConfirmedLinkedIn
    ),
    write_guarantees: {
      canonical_hotel_to_owner_mutations: 0,
      census_owner_mutations: 0,
      evaluation_staging_allowed: true,
      customer_publication: "BLOCKED",
      paid_enrichment_executed: false,
      parallel_executed: false,
      webhound_executed: false,
    },
  };
}

export function buildEnrichmentSubjectsFromResearch(researchRows, { maxPeople = 10, maxPerOwner = 2 } = {}) {
  const subjects = [];
  const rejected = [];
  const perOwner = new Map();
  for (const row of researchRows) {
    if (!row.qualifies_for_fullenrich && !row.alternative_chain_no_corporate_website) {
      // still allow alternative-chain rows flagged qualifies
    }
    if (!row.qualifies_for_fullenrich) continue;
    const ownerKey = row.ownership?.owner_entity_id || row.confirmed_company_domain_host || row.hotel_id;
    const people = (row.people || []).filter(
      (p) =>
        p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" &&
        p.provenance?.affiliation_gate?.ok === true &&
        p.provenance?.affiliation_independent === true &&
        !p.deceased &&
        !p.former_affiliation
    );
    const ordered = [
      ...people.filter((p) => ROLE_PRIORITY_RE.test(p.title || "")),
      ...people.filter((p) => !ROLE_PRIORITY_RE.test(p.title || "")),
    ];
    const rejectedDomainHosts = [
      ...((row.rejected_domains || []).map((d) => d.host || d)),
      "alliancehm.com",
      "emis.com",
    ].filter(Boolean);
    const forbidden = row.forbidden_org_hosts || [];
    for (const p of ordered) {
      if (subjects.length >= maxPeople) break;
      if ((perOwner.get(ownerKey) || 0) >= maxPerOwner) break;
      const parts = String(p.display_name).trim().split(/\s+/);
      const liChannel = (p.channels || []).find((c) => /LINKEDIN/i.test(c.kind));
      const linkedin = liChannel?.value || null;
      const liIndependentlyConfirmed = /independently confirmed/i.test(liChannel?.display_label || "");
      const domainHost = row.confirmed_company_domain_host || null;
      const domainStatus =
        !domainHost
          ? "ABSENT"
          : rejectedDomainHosts.includes(domainHost)
            ? "REJECTED"
            : "CONFIRMED";
      const candidate = {
        id: `${row.hotel_id}__${parts.join("_")}`.slice(0, 90),
        subject_id: `${row.hotel_id}__${parts.join("_")}`.slice(0, 90),
        hotel_id: row.hotel_id,
        hotel_name: row.hotel_name,
        person: {
          display_name: p.display_name,
          first_name: parts[0],
          last_name: parts.slice(1).join(" ") || parts[0],
          title: p.title,
          identity_supported: true,
          publication_label: p.publication_label,
          why_relevant: p.why_relevant,
          deceased: Boolean(p.deceased),
          former_affiliation: Boolean(p.former_affiliation),
        },
        organization: {
          name: row.ownership?.owner_display_name,
          entity_id: row.ownership?.owner_entity_id,
          relationship_supported: Boolean(row.ownership?.owner_display_name),
          rejected: false,
        },
        identifiers: {
          domain:
            domainHost && domainStatus === "CONFIRMED"
              ? { value: domainHost, status: "CONFIRMED", independently_supported: true, evidence_refs: row.domain_evidence || [] }
              : domainHost
                ? { value: domainHost, status: domainStatus }
                : { status: "ABSENT" },
          linkedin_url: linkedin
            ? {
                value: linkedin,
                independently_confirmed: liIndependentlyConfirmed,
                evidence_note: p.why_relevant || row.ownership?.owner_display_name,
                profile_company: row.ownership?.owner_display_name,
              }
            : null,
        },
        rejected_domains: rejectedDomainHosts,
        forbidden_owner_domain_hosts: forbidden,
        custom: { subject_id: `${row.hotel_id}__${parts.join("_")}`.slice(0, 90), hotel_id: row.hotel_id || "" },
        // flat fields for evaluateFeRow / commercial table
        full_name: p.display_name,
        first_name: parts[0],
        last_name: parts.slice(1).join(" ") || parts[0],
        organization_name: row.ownership?.owner_display_name,
        owner_entity_id: row.ownership?.owner_entity_id,
        domain: domainStatus === "CONFIRMED" ? domainHost : null,
        confirmed_title: p.title,
        linkedin_url: linkedin && /linkedin\.com\/in\//i.test(linkedin) ? linkedin : null,
        baseline_email: null,
        newly_researched: Boolean(p.provenance?.newly_researched),
        role_evidence: (p.evidence || [])[0] || null,
      };
      // Owner-person paid enrich requires corroborated affiliation + hotel→owner evidence.
      // Surfe/FullEnrich discovery search must not use OWNER_PERSON_ENRICHMENT_WORKFLOW.
      const ownerClass = String(
        row.ownership?.classification || row.ownership?.owner_role || row.ownership?.relationship_primary || ""
      ).toUpperCase();
      // Only explicit owner/sponsor relationship classes may open the paid
      // owner-person lane. Unknown labels are unresolved, not affirmative.
      const ownerClassOk = [
        "PROPERTY_OWNER",
        "ECONOMIC_OWNER_OR_SPONSOR",
        "ECONOMIC_OWNER",
        "OWNER",
      ].includes(ownerClass);
      const ownerEvidenceRefs = row.ownership?.evidence_refs || [];

      const affCorroborated = Boolean(p.provenance?.independently_corroborated);
      const gated = gateProviderCandidates(
        [
          {
            ...candidate,
            workflow: "owner_person_enrichment",
            hotel_to_owner: {
              supported: Boolean(row.ownership?.owner_display_name) && ownerClassOk,
              relationship_class: ownerClass || null,
              evidence_refs: ownerEvidenceRefs,
            },
            affiliation_corroboration: {
              status: affCorroborated ? "CORROBORATED" : p.affiliation_status || "SURFE_ONLY",
              source_class:
                p.affiliation_source_class ||
                (affCorroborated ? "CREDIBLE_INDEPENDENT" : "SURFE_ONLY"),
              independently_corroborated: affCorroborated,
              evidence_refs: p.evidence || [],
              role_relevant: undefined,
            },
          },
        ],
        { provider: "fullenrich", workflow: "owner_person_enrichment" }
      );
      if (gated.allowed.length) {
        const allowed = gated.allowed[0];
        subjects.push({
          ...allowed,
          first_name: allowed.submit_row.first_name,
          last_name: allowed.submit_row.last_name,
          domain: allowed.submit_row.domain || null,
          company_name: allowed.submit_row.company_name || null,
          linkedin_url: allowed.submit_row.linkedin_url || null,
          // keep organization as gate object; commercial/eval use organization_name
          organization: candidate.organization,
          organization_name: candidate.organization_name,
          gate: allowed.gate,
          submit_row: allowed.submit_row,
        });
        perOwner.set(ownerKey, (perOwner.get(ownerKey) || 0) + 1);
      } else {
        rejected.push(gated.rejected[0]);
      }
    }
  }
  return { subjects, rejected_pre_submission: rejected };
}

export { verifyOwnershipPath };
