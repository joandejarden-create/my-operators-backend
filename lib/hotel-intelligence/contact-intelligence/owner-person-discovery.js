/**
 * Contact Intelligence V1.4 — owner domain → org contact → relevant person path.
 * Once per distinct evidenced owner. Paid enrichment off.
 */

import "dotenv/config";
import {
  ATTRIBUTION,
  CHANNEL_KIND,
  PROPERTY_RELEVANCE,
  USAGE_RIGHTS,
  UNRESOLVED_REASON,
  DELIVERABILITY,
  ROLE_CURRENCY,
} from "./vocabulary.js";
import { createChannel, createEvidenceRef, createPersonContact } from "./contact-record.js";
import { isGenericMailboxEmail, isRoleMailboxEmail } from "./dimensions.js";
import { buildOrganizationContactRoute } from "./owner-reuse.js";
import {
  serpGoogle,
  extractContactsFromHtml,
  pickOfficialish,
  isPlausibleOwnerPage,
  classifyOfficialUrl,
} from "./live-native-discovery.js";
import { fetchContactResearchPage } from "./research-fetch-v1.3.js";
import { CI_FAILURE_CODE, addCode, createEmptyTrace } from "./failure-codes.js";
import {
  LEADERSHIP_CATEGORY,
  classifyLeadershipCategory,
} from "./leadership-extraction.js";

export const OWNER_PERSON_DISCOVERY_VERSION = "contact-owner-person-discovery-v1.4";

const PRESS_TITLE_RE = /press|media|comunicación|comunicacion|public relations|\bpr\b|comunicaciones/i;
const RESERVATIONS_RE = /reservat|booking|central de reserv/i;
const FUNCTIONAL_DM_RE =
  /asset|development|desarroll|invers|investment|brand|conversi|affiliation|franchise|chief development|managing director|ceo|coo|founder|partner|investor/i;
const BOARD_ONLY_RE = /consejero|board member|presidente|president|chairman|miembro(?:\s+del\s+consejo)?/i;
const DEV_ROLE_RE =
  /asset|development|desarroll|invers|investment|brand|conversi|affiliation|franchise|ownership|propietario|managing director|ceo|president|chairman|founder|partner|investor|board|consejero/i;

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function forbiddenHost(url, forbidden = []) {
  const h = hostOf(url);
  return (forbidden || []).some((f) => h === f || h.endsWith(`.${f}`) || h.includes(f.replace(/^www\./, "")));
}

function classifyOrgChannelKind(value, context = "") {
  const ctx = `${value} ${context}`.toLowerCase();
  if (PRESS_TITLE_RE.test(ctx)) return "PRESS";
  if (RESERVATIONS_RE.test(ctx)) return "RESERVATIONS";
  if (isRoleMailboxEmail(value) || isGenericMailboxEmail(value)) return "CORPORATE_GENERAL";
  return "CORPORATE_GENERAL";
}

function relevanceForPerson(title, hotelName, ownerName, leadershipCategory = null) {
  const t = String(title || "");
  const cat = leadershipCategory || classifyLeadershipCategory(t);

  if (cat === LEADERSHIP_CATEGORY.BOARD_OWNERSHIP || (BOARD_ONLY_RE.test(t) && !FUNCTIONAL_DM_RE.test(t))) {
    return {
      relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidenced_or_inferred: "INFERRED_FROM_TITLE",
      commercial_relevance: "OWNERSHIP_LEADERSHIP_NOT_CONFIRMED_DM",
      decision_responsibility: "UNRESOLVED",
      functionally_relevant: false,
      why: `Board / ownership leadership at ${ownerName} (${t}) — ownership-layer affiliation for ${hotelName}; not confirmed as brand-conversion or operator-selection decision maker.`,
    };
  }
  if (cat === LEADERSHIP_CATEGORY.DEVELOPMENT_ASSET || FUNCTIONAL_DM_RE.test(t)) {
    return {
      relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidenced_or_inferred: "INFERRED_FROM_TITLE",
      commercial_relevance: "FUNCTIONALLY_RELEVANT_CANDIDATE",
      decision_responsibility: "INFERRED_FROM_TITLE_ONLY",
      functionally_relevant: true,
      why: `Title (${t}) indicates development / asset / brand / operating responsibility relevant to ${hotelName} under ${ownerName}. Inference basis: title keywords only — not confirmed decision authority.`,
    };
  }
  if (DEV_ROLE_RE.test(t)) {
    return {
      relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidenced_or_inferred: "INFERRED_FROM_TITLE",
      commercial_relevance: "POSSIBLY_RELEVANT",
      decision_responsibility: "UNRESOLVED",
      functionally_relevant: false,
      why: `Title (${t}) at ${ownerName} may be relevant to ${hotelName}; role specificity not confirmed.`,
    };
  }
  return {
    relevance: PROPERTY_RELEVANCE.OWNER_ORG,
    evidenced_or_inferred: "INFERRED_FROM_AFFILIATION",
    commercial_relevance: "AFFILIATION_ONLY",
    decision_responsibility: "UNRESOLVED",
    functionally_relevant: false,
    why: `Appears affiliated with ${ownerName}; relevance to ${hotelName} inferred from organization membership — role/geography not independently evidenced.`,
  };
}

/**
 * Separate confidence layers — deed/UBO gaps must not auto-invalidate economic-owner path.
 */
export function buildOwnershipConfidenceLayers(verified) {
  const note = String(verified?.evidence_note || "");
  const hasDeedGap = /deed|legal title|titleholder|propco.*unknown|deed vehicle/i.test(note);
  const hasUboGap = /UBO|beneficial|member register|ultimate/i.test(note);
  const economic =
    verified?.owner_role === "ECONOMIC_OWNER" || verified?.owner_role === "PROPCO"
      ? verified?.confidence || "UNKNOWN"
      : "UNRESOLVED";
  return {
    hotel_to_economic_owner: {
      status: verified?.classification || "UNRESOLVED",
      confidence: economic,
      note: "Deed/UBO gaps do not automatically invalidate an evidenced economic-owner contact path.",
    },
    legal_titleholder_deed_vehicle: {
      status: hasDeedGap ? "GAP" : verified?.propco ? "PRESENT" : "UNKNOWN",
      confidence: hasDeedGap ? "UNRESOLVED" : "UNKNOWN",
    },
    ultimate_beneficial_ownership: {
      status: hasUboGap ? "GAP" : "UNKNOWN",
      confidence: hasUboGap ? "UNRESOLVED" : "UNKNOWN",
    },
    organization_to_domain: {
      status: "SET_BY_DISCOVERY",
      confidence: null,
    },
  };
}

/**
 * Verify ownership surface path — do not treat prior "evidenced" as unquestionable.
 */
export function verifyOwnershipPath(hotel, anchor) {
  const out = {
    hotel_id: hotel.hotel_id,
    hotel_name: hotel.hotel_name,
    ownership_surface_ok: Boolean(anchor?.ok && anchor?.anchor?.primary_owner_entity_id),
    owner_entity_id: anchor?.anchor?.primary_owner_entity_id || null,
    owner_display_name: anchor?.anchor?.owner_display_name || null,
    owner_role: anchor?.anchor?.owner_role || null,
    relationship_type: null,
    confidence: anchor?.anchor?.confidence || null,
    temporal_status: anchor?.anchor?.temporal_status || null,
    evidence_note: anchor?.anchor?.evidence || null,
    evidence_date: null,
    propco: anchor?.anchor?.propco || null,
    rejected_auto_anchors: anchor?.anchor?.rejected_auto_anchors || null,
    current_status_uncertainty: [],
    classification: "UNRESOLVED",
    return_to_ownership_lane: false,
  };

  if (!out.ownership_surface_ok) {
    out.classification = "UNRESOLVED";
    out.return_to_ownership_lane = true;
    out.current_status_uncertainty.push("ownership_surface_unavailable");
    return out;
  }

  // Operator/brand never count as ownership
  if (["OPERATOR", "BRAND", "DEVELOPER", "ASSET_MANAGER"].includes(String(out.owner_role || "").toUpperCase())) {
    out.classification = "UNRESOLVED_OPERATOR_OR_BRAND_NOT_OWNER";
    out.return_to_ownership_lane = true;
    out.current_status_uncertainty.push("role_is_not_economic_owner");
    return out;
  }

  out.relationship_type =
    out.owner_role === "ECONOMIC_OWNER"
      ? "OWNED_BY_OR_ECONOMIC_OWNER"
      : out.owner_role === "PROPCO"
        ? "PROPCO"
        : out.owner_role || "UNKNOWN";

  if (out.confidence === "PROBABLE" || out.confidence === "LOW") {
    out.current_status_uncertainty.push("confidence_not_high");
  }
  if (/deed|UBO|member register|legal bridge|not obtained|unknown/i.test(String(out.evidence_note || ""))) {
    out.current_status_uncertainty.push("deed_or_ubo_gap_in_evidence_note");
  }
  if (out.temporal_status && out.temporal_status !== "CURRENT") {
    out.current_status_uncertainty.push(`temporal_status=${out.temporal_status}`);
  }

  // Classification: keep researching when surface ok + economic owner, even with gaps
  if (out.owner_role === "ECONOMIC_OWNER" || out.owner_role === "PROPCO") {
    out.classification =
      out.current_status_uncertainty.length > 0
        ? "EVIDENCED_WITH_UNCERTAINTY"
        : "EVIDENCED_CURRENT_OWNER";
    out.return_to_ownership_lane = false;
  } else {
    out.classification = "UNRESOLVED";
    out.return_to_ownership_lane = true;
  }

  return out;
}

/**
 * Discover org domain + contacts + people for one distinct owner.
 */
export async function discoverOwnerPersonPath(input = {}) {
  const started = Date.now();
  const cost = { serpapi_searches: 0, serpapi_usd: 0, pages_fetched: 0, fetch_errors: 0, browser_fetches: 0 };
  const trace = createEmptyTrace({
    hotel_id: input.focus_hotel_id || null,
    hotel_name: input.focus_hotel_name || null,
  });
  const sources = [];
  const rejected = [];
  const blockers = [];

  const ownerId = input.owner_entity_id;
  const ownerName = input.owner_display_name;
  const hotels = input.hotels || [];
  const focusHotel = hotels[0] || {};
  const lang = focusHotel.language || "es";
  const hl = lang === "en" ? "en" : "es";
  const gl = lang === "en" ? "us" : "mx";
  const forbidden = input.forbidden_org_hosts || [];
  const domainHypotheses = input.domain_hypotheses || [];
  const personHypotheses = (input.person_hypotheses || []).filter((p) => !p.deceased);
  const maxQ = input.max_serp_queries ?? 5;

  if (!String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim()) {
    blockers.push({ code: "SERPAPI_KEY_MISSING", detail: "required for owner/person discovery" });
  }

  const orgChannels = [];
  const pressChannels = [];
  const reservationsChannels = [];
  const people = [];
  let confirmedDomain = null;
  let domainEvidence = null;

  try {
    if (!blockers.length && ownerName) {
      // Domain resolution — exact org identity + geography; reject other-market lookalikes
      const domainQuery =
        lang === "en"
          ? `"${ownerName}" (official website OR "about us" OR contact) hotel (Bermuda OR hospitality) -palladium -aimbridge`
          : ownerName.toLowerCase().includes("alliance")
            ? `("Alliance Hotel Management" OR "Alliance Hospitality Management") (México OR Mexico OR Cancún OR Guadalajara) (sitio oficial OR contacto OR website) -palladium -aimbridge -apex -"Apex Alliance" -Latvia -Riga`
            : ownerName.toLowerCase().includes("hnf")
              ? `"Inmobiliaria HNF" (Guadalajara OR Jalisco OR México) (sitio oficial OR contacto OR RFC OR PROFECO) -remax -aimbridge -"New Hampshire" -"HNF Inc"`
              : `"${ownerName}" (sitio oficial OR "quiénes somos" OR contacto) hotel México`;
      const serp = await serpGoogle(domainQuery, cost, { hl, gl, num: 8 });
      sources.push({
        kind: "serp_owner_domain",
        query: domainQuery,
        result_count: serp.organic.length,
        used_nested_data_shape: serp.used_nested_data_shape,
      });
      trace.queries.push({
        kind: "serp_owner_domain",
        query: domainQuery,
        result_count: serp.organic.length,
        used_nested_data_shape: serp.used_nested_data_shape,
      });

      const picks = pickOfficialish(serp.organic || [], [ownerName]);
      const candidateUrls = [];
      for (const d of domainHypotheses) candidateUrls.push({ url: d, from: "fixture_hypothesis" });
      for (const p of picks.slice(0, 6)) {
        candidateUrls.push({ url: p.url, from: "serp", title: p.title });
        trace.urls_discovered.push({ url: p.url, title: p.title, score: p.score, from: "serp_owner_domain" });
      }

      const seen = new Set();
      for (const c of candidateUrls) {
        const url = c.url;
        if (!url || seen.has(url)) continue;
        seen.add(url);
        if (forbiddenHost(url, forbidden)) {
          rejected.push({
            url,
            reason: CI_FAILURE_CODE.ATTRIBUTION_UNRESOLVED,
            detail: "forbidden_host_operator_press_or_prior_false_positive",
          });
          addCode(trace, CI_FAILURE_CODE.ATTRIBUTION_UNRESOLVED, url);
          continue;
        }
        const cls = classifyOfficialUrl(url);
        if (cls.status === "rejected") {
          rejected.push({ url, reason: cls.code, detail: cls.detail });
          continue;
        }
        if (!isPlausibleOwnerPage(url, ownerName, domainHypotheses[0] || null) && c.from === "serp") {
          // Still allow fixture hypotheses even if token weak
          rejected.push({
            url,
            reason: CI_FAILURE_CODE.ATTRIBUTION_UNRESOLVED,
            detail: "host_does_not_match_owner_tokens",
          });
          addCode(trace, CI_FAILURE_CODE.ATTRIBUTION_UNRESOLVED, url);
          continue;
        }

        const page = await fetchContactResearchPage(url, {
          timeoutMs: 14000,
          allowBrowserFallback: false,
        });
        cost.pages_fetched += 1;
        if (!page.ok) {
          cost.fetch_errors += 1;
          trace.fetch_failures.push({
            url,
            error: page.error || page.status,
            fetch_class: page.fetch_class,
          });
          continue;
        }
        if (page.browser_used) cost.browser_fetches += 1;

        confirmedDomain = page.url;
        domainEvidence = {
          url: page.url,
          from: c.from,
          title: c.title || null,
          observed_at: new Date().toISOString(),
          note:
            c.from === "fixture_hypothesis"
              ? "Domain hypothesis from ownership fixtures; page fetched successfully — association supported by host match + first-party fetch."
              : "Domain resolved via search + host/token match + first-party fetch.",
        };
        sources.push({ kind: "owner_domain_page", url: page.url, from: c.from });

        const extracted = extractContactsFromHtml(page.text, page.url);
        orgChannels.push(
          createChannel({
            kind: CHANNEL_KIND.ORG_WEBSITE,
            value: page.url,
            display_label: "Confirmed organization website",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
            evidence: [
              createEvidenceRef({
                source_title: "Owner organization first-party page",
                source_url: page.url,
                source_type: "first_party_or_corp",
                observed_at: new Date().toISOString(),
              }),
            ],
          })
        );

        await ingestOrgExtraction({
          extracted,
          pageUrl: page.url,
          pageText: page.text,
          orgChannels,
          pressChannels,
          reservationsChannels,
          people,
          ownerId,
          ownerName,
          hotelName: focusHotel.hotel_name,
          rejected,
          personHypotheses,
        });

        // Fetch contact / governance pages on same host
        const contactHints = [
          page.url.replace(/\/$/, "") + "/contacto",
          page.url.replace(/\/$/, "") + "/contact",
          page.url.replace(/\/$/, "") + "/corporativo/contacto.php",
          page.url.replace(/\/$/, "") + "/corporativo/gobierno-corporativo.php",
          page.url.replace(/\/$/, "") + "/about",
          page.url.replace(/\/$/, "") + "/team",
          page.url.replace(/\/$/, "") + "/leadership",
        ];

        let subFetched = 0;
        for (const sub of contactHints) {
          if (subFetched >= 3) break;
          if (hostOf(sub) !== hostOf(page.url) && !hostOf(sub).endsWith(hostOf(page.url))) continue;
          const subPage = await fetchContactResearchPage(sub, {
            timeoutMs: 12000,
            allowBrowserFallback: false,
          });
          cost.pages_fetched += 1;
          if (!subPage.ok) {
            cost.fetch_errors += 1;
            continue;
          }
          subFetched += 1;
          const ex2 = extractContactsFromHtml(subPage.text, subPage.url);
          sources.push({
            kind: "owner_subpage",
            url: subPage.url,
            phones: ex2.phones.length,
            emails: ex2.emails.length,
            people: ex2.people.length,
          });
          await ingestOrgExtraction({
            extracted: ex2,
            pageUrl: subPage.url,
            pageText: subPage.text,
            orgChannels,
            pressChannels,
            reservationsChannels,
            people,
            ownerId,
            ownerName,
            hotelName: focusHotel.hotel_name,
            rejected,
            preferContactPage: true,
            personHypotheses,
          });
        }
        break;
      }

      // Leadership / person search (bounded)
      if (cost.serpapi_searches < maxQ) {
        const peopleQuery =
          lang === "en"
            ? `"${ownerName}" (CEO OR President OR "Managing Director" OR "Asset Manager" OR partner OR leadership OR team)`
            : `"${ownerName}" (Director General OR Presidente OR "Asset Manager" OR socio OR equipo OR directivos)`;
        const serpP = await serpGoogle(peopleQuery, cost, { hl, gl, num: 8 });
        sources.push({
          kind: "serp_owner_people",
          query: peopleQuery,
          result_count: serpP.organic.length,
          used_nested_data_shape: serpP.used_nested_data_shape,
        });
        for (const p of (serpP.organic || []).slice(0, 4)) {
          const url = p.link || p.url;
          if (!url || forbiddenHost(url, forbidden)) continue;
          if (!confirmedDomain || !isPlausibleOwnerPage(url, ownerName, confirmedDomain)) {
            // Allow first-party host only for people pages once domain confirmed
            if (confirmedDomain && hostOf(url) !== hostOf(confirmedDomain)) {
              rejected.push({
                url,
                reason: CI_FAILURE_CODE.ATTRIBUTION_UNRESOLVED,
                detail: "people_page_not_on_confirmed_owner_domain",
              });
              continue;
            }
            if (!confirmedDomain) continue;
          }
          const page = await fetchContactResearchPage(url, { allowBrowserFallback: false });
          cost.pages_fetched += 1;
          if (!page.ok) {
            cost.fetch_errors += 1;
            continue;
          }
          const extracted = extractContactsFromHtml(page.text, page.url);
          await ingestOrgExtraction({
            extracted,
            pageUrl: page.url,
            pageText: page.text,
            orgChannels,
            pressChannels,
            reservationsChannels,
            people,
            ownerId,
            ownerName,
            hotelName: focusHotel.hotel_name,
            rejected,
            personHypotheses,
          });
        }
      }
    }
  } catch (err) {
    blockers.push({ code: "OWNER_PERSON_DISCOVERY_ERROR", detail: String(err?.message || err).slice(0, 240) });
  }

  // Person hypotheses (not deceased) — skip if already corroborated on first-party page
  for (const hp of personHypotheses.slice(0, 3)) {
    if (PRESS_TITLE_RE.test(hp.title || "")) continue;
    if (
      people.some(
        (p) =>
          String(p.display_name || "").toLowerCase() === String(hp.display_name || "").toLowerCase() &&
          p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE"
      )
    ) {
      continue;
    }
    const rel = relevanceForPerson(hp.title, focusHotel.hotel_name, ownerName);
    const channels = [];
    if (hp.linkedin) {
      channels.push(
        createChannel({
          kind: CHANNEL_KIND.PERSON_LINKEDIN,
          value: hp.linkedin,
          display_label: "Professional profile (not email/phone coverage)",
          attribution: ATTRIBUTION.NAMED_PERSON,
          verified: false,
          usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
        })
      );
    }
    people.push(
      createPersonContact({
        display_name: hp.display_name,
        title: hp.title,
        organization_entity_id: ownerId,
        organization_name: ownerName,
        role_observed_at: null,
        role_currency: ROLE_CURRENCY.UNKNOWN,
        property_relevance: rel.relevance,
        channels,
        evidence: [
          createEvidenceRef({
            source_title: "Ownership deep-research / cohort hypothesis",
            source_url: null,
            source_type: "ownership_hypothesis",
            observed_at: null,
            excerpt: hp.why_relevant_hypothesis || null,
          }),
        ],
        unresolved_reasons: [UNRESOLVED_REASON.NO_PERSON_EVIDENCE],
        why_relevant: hp.why_relevant_hypothesis || rel.why,
        publication_label: "HYPOTHESIS_PENDING_CORROBORATION",
        customer_caveat:
          "Person from ownership research hypothesis — not independently re-verified in this pass as current decision maker.",
        provenance: {
          evidenced_or_inferred: "HYPOTHESIS",
          basis: hp.source || "deep_research_hypothesis",
        },
      })
    );
  }

  // Dedup people; prefer evidenced + functionally relevant over board-only over hypothesis
  const byName = new Map();
  for (const p of people) {
    const key = String(p.display_name || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    if (!key) continue;
    const prev = byName.get(key);
    const score = (x) => {
      if (x.publication_label === "HYPOTHESIS_PENDING_CORROBORATION") return 0;
      if (x.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE") {
        if (x.provenance?.functionally_relevant) return 5;
        if (x.provenance?.leadership_category === LEADERSHIP_CATEGORY.BOARD_OWNERSHIP) return 3;
        return 4;
      }
      return /asset|development|invers|brand|ceo|founder|chief development/i.test(String(x.title || ""))
        ? 3
        : 1;
    };
    if (!prev || score(p) > score(prev)) byName.set(key, p);
  }
  // Keep board leadership AND functionally relevant (do not drop useful leadership for missing email)
  const allDeduped = [...byName.values()];
  const functional = allDeduped.filter((p) => p.provenance?.functionally_relevant);
  const board = allDeduped.filter(
    (p) =>
      p.provenance?.leadership_category === LEADERSHIP_CATEGORY.BOARD_OWNERSHIP &&
      !p.provenance?.functionally_relevant
  );
  const other = allDeduped.filter((p) => !functional.includes(p) && !board.includes(p));
  let peopleOut = [...functional, ...board, ...other].slice(0, 5);

  const corpPhoneOrEmail = orgChannels.find((c) =>
    [CHANNEL_KIND.SWITCHBOARD, CHANNEL_KIND.ORG_PHONE, CHANNEL_KIND.ORG_EMAIL, CHANNEL_KIND.ROLE_MAILBOX].includes(
      c.kind
    )
  );

  for (const p of peopleOut) {
    const hasDirect = (p.channels || []).some(
      (c) => c.kind === CHANNEL_KIND.PERSON_EMAIL || c.kind === CHANNEL_KIND.PERSON_PHONE
    );
    if (!hasDirect && corpPhoneOrEmail) {
      p.indirect_corporate_route = {
        ...corpPhoneOrEmail,
        display_label: `Corporate office; ask for ${p.display_name}. Connection not confirmed.`,
        customer_caveat:
          "Indirect corporate route — not a direct-person phone/email; connection to this person not confirmed.",
        property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
        route_class: "INDIRECT_CORPORATE_ASK_FOR_PERSON",
      };
    }
    p.deliverability_email = "NOT_CHECKED";
    p.contact_source_date = (p.evidence || [])[0]?.observed_at || null;
    p.role_check_date = p.role_observed_at || null;
  }

  const orgRoute = buildOrganizationContactRoute({
    owner_entity_id: ownerId,
    owner_display_name: ownerName,
    channels: orgChannels.filter(
      (c) =>
        c.kind === CHANNEL_KIND.CONTACT_FORM ||
        c.kind !== CHANNEL_KIND.ORG_WEBSITE ||
        !corpPhoneOrEmail
    ),
  });

  const hasOrgPhoneEmail = orgChannels.some((c) =>
    [CHANNEL_KIND.ORG_PHONE, CHANNEL_KIND.ORG_EMAIL, CHANNEL_KIND.ROLE_MAILBOX, CHANNEL_KIND.SWITCHBOARD].includes(
      c.kind
    )
  );
  const hasContactFormFallback = orgChannels.some((c) => c.kind === CHANNEL_KIND.CONTACT_FORM);
  const evidencedPeople = peopleOut.filter(
    (p) => p.publication_label !== "HYPOTHESIS_PENDING_CORROBORATION"
  );
  const hypothesisPeople = peopleOut.filter(
    (p) => p.publication_label === "HYPOTHESIS_PENDING_CORROBORATION"
  );
  const functionallyRelevant = evidencedPeople.filter((p) => p.provenance?.functionally_relevant);
  const ownershipLeadership = evidencedPeople.filter(
    (p) => p.provenance?.leadership_category === LEADERSHIP_CATEGORY.BOARD_OWNERSHIP
  );

  const unresolved = [];
  if (!confirmedDomain) unresolved.push("OWNER_DOMAIN_UNRESOLVED");
  if (!hasOrgPhoneEmail && !hasContactFormFallback) unresolved.push(UNRESOLVED_REASON.NO_ORG_ROUTE);
  if (!evidencedPeople.length && !hypothesisPeople.length) {
    unresolved.push(UNRESOLVED_REASON.NO_PERSON_EVIDENCE);
  }

  const domainConfidence = confirmedDomain
    ? {
        status: "CONFIRMED_FIRST_PARTY",
        confidence: domainEvidence?.from === "fixture_hypothesis" ? "HIGH" : "MEDIUM",
        url: confirmedDomain,
      }
    : {
        status: "UNRESOLVED",
        confidence: "UNRESOLVED",
        missing_evidence:
          "No first-party domain matched exact organization identity + geography constraints",
      };

  trace.rejected = rejected;
  trace.budget = {
    serpapi_searches: cost.serpapi_searches,
    serpapi_usd: Number(cost.serpapi_usd.toFixed(4)),
    pages_fetched: cost.pages_fetched,
    browser_fetches: cost.browser_fetches,
    fetch_errors: cost.fetch_errors,
    stopping_reason: "complete",
  };

  return {
    ok: blockers.length === 0,
    version: OWNER_PERSON_DISCOVERY_VERSION,
    owner_entity_id: ownerId,
    owner_display_name: ownerName,
    hotels_in_subset: hotels.map((h) => ({ hotel_id: h.hotel_id, hotel_name: h.hotel_name })),
    confirmed_company_domain: confirmedDomain
      ? { url: confirmedDomain, evidence: domainEvidence }
      : null,
    confidence_layers: {
      organization_to_domain: domainConfidence,
    },
    organization_contact_route: orgRoute,
    organization_website_only: orgChannels.filter((c) => c.kind === CHANNEL_KIND.ORG_WEBSITE),
    contact_form_fallbacks: orgChannels.filter((c) => c.kind === CHANNEL_KIND.CONTACT_FORM),
    press_contacts: pressChannels,
    reservations_contacts: reservationsChannels,
    people: peopleOut,
    people_evidenced: evidencedPeople,
    people_hypothesis: hypothesisPeople,
    people_functionally_relevant: functionallyRelevant,
    people_ownership_leadership: ownershipLeadership,
    inferred_email_candidates: [],
    unresolved_reasons: unresolved,
    sources,
    execution_trace: trace,
    cost,
    elapsed_ms: Date.now() - started,
    blockers,
    flags: {
      has_confirmed_domain: Boolean(confirmedDomain),
      has_org_phone_or_email: hasOrgPhoneEmail,
      has_contact_form_fallback: hasContactFormFallback,
      has_org_contact_page_fallback:
        hasContactFormFallback ||
        orgChannels.some((c) => c.kind === CHANNEL_KIND.ORG_WEBSITE),
      has_named_ownership_leadership: ownershipLeadership.length > 0,
      has_functionally_relevant_person: functionallyRelevant.length > 0,
      has_relevant_person_evidenced: evidencedPeople.length > 0,
      has_relevant_person_hypothesis: hypothesisPeople.length > 0,
      has_direct_person_email: peopleOut.some((p) =>
        (p.channels || []).some((c) => c.kind === CHANNEL_KIND.PERSON_EMAIL)
      ),
      has_direct_person_phone: peopleOut.some((p) =>
        (p.channels || []).some((c) => c.kind === CHANNEL_KIND.PERSON_PHONE)
      ),
      has_person_with_indirect_corporate_route: peopleOut.some((p) => p.indirect_corporate_route),
      deliverability_checked_email: false,
      independent_precision: "NOT_REVIEWED",
    },
    contact_source_date: new Date().toISOString(),
  };
}

async function ingestOrgExtraction({
  extracted,
  pageUrl,
  pageText = "",
  orgChannels,
  pressChannels,
  reservationsChannels,
  people,
  ownerId,
  ownerName,
  hotelName,
  rejected,
  preferContactPage = false,
  personHypotheses = [],
}) {
  // Prefer structured tel:/mailto:; body phones only with international/local markers
  const structuredPhones = extracted.phones_from_tel?.length
    ? extracted.phones_from_tel
    : (extracted.phones || []).filter((p) => /^\+|00|\(\d{2,3}\)/.test(String(p).trim()));
  const structuredEmails = extracted.emails_from_mailto?.length
    ? extracted.emails_from_mailto
    : extracted.emails || [];

  for (const phone of structuredPhones.slice(0, 2)) {
    const kindLabel = classifyOrgChannelKind(phone, pageUrl);
    const ch = createChannel({
      kind: CHANNEL_KIND.SWITCHBOARD,
      value: phone,
      display_label:
        kindLabel === "PRESS"
          ? "Press / media phone"
          : kindLabel === "RESERVATIONS"
            ? "Reservations phone"
            : "Organization switchboard",
      attribution: ATTRIBUTION.ORGANIZATION,
      line_type: kindLabel === "RESERVATIONS" ? "RESERVATIONS" : "SWITCHBOARD",
      claimed_direct_personal: false,
      deliverability: DELIVERABILITY.UNKNOWN,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      customer_caveat: "Corporate line — not a direct personal number.",
      evidence: [
        createEvidenceRef({
          source_title: "Published on organization page",
          source_url: pageUrl,
          source_type: "first_party_or_corp",
          observed_at: new Date().toISOString(),
        }),
      ],
    });
    if (kindLabel === "PRESS") pressChannels.push(ch);
    else if (kindLabel === "RESERVATIONS") reservationsChannels.push(ch);
    else orgChannels.push(ch);
  }

  for (const email of structuredEmails.slice(0, 4)) {
    const kindLabel = classifyOrgChannelKind(email, pageUrl);
    if (!(isGenericMailboxEmail(email) || isRoleMailboxEmail(email))) {
      rejected.push({
        value: email,
        reason: CI_FAILURE_CODE.POLICY_RESTRICTED,
        detail: "inferred_named_email_staged_out_of_default_coverage",
      });
      continue;
    }
    const ch = createChannel({
      kind: isRoleMailboxEmail(email) ? CHANNEL_KIND.ROLE_MAILBOX : CHANNEL_KIND.ORG_EMAIL,
      value: email,
      display_label:
        kindLabel === "PRESS"
          ? "Press mailbox"
          : kindLabel === "RESERVATIONS"
            ? "Reservations mailbox"
            : "Organization mailbox",
      attribution: isRoleMailboxEmail(email) ? ATTRIBUTION.ROLE_MAILBOX : ATTRIBUTION.ORGANIZATION,
      deliverability: DELIVERABILITY.UNKNOWN,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      evidence: [
        createEvidenceRef({
          source_title: "Published organization email",
          source_url: pageUrl,
          source_type: "first_party_or_corp",
          observed_at: new Date().toISOString(),
        }),
      ],
    });
    if (kindLabel === "PRESS") pressChannels.push(ch);
    else if (kindLabel === "RESERVATIONS") reservationsChannels.push(ch);
    else orgChannels.push(ch);
  }

  if (preferContactPage && extracted.is_contact_page && !structuredPhones.length && !structuredEmails.length) {
    const mech = extracted.contact_page_mechanism || {};
    if (mech.kind === "CONTACT_FORM_FALLBACK" || mech.has_form) {
      orgChannels.push(
        createChannel({
          kind: CHANNEL_KIND.CONTACT_FORM,
          value: pageUrl,
          display_label: "Organization contact form (fallback — not phone/email)",
          attribution: ATTRIBUTION.ORGANIZATION,
          property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
          customer_caveat:
            "Contact-form fallback only. Do not count as organization phone/email or direct person contact.",
          evidence: [
            createEvidenceRef({
              source_title: "Organization contact form page",
              source_url: pageUrl,
              source_type: "first_party_or_corp",
              observed_at: new Date().toISOString(),
            }),
          ],
          provenance: { contact_page_mechanism: mech.kind || "CONTACT_FORM_FALLBACK" },
        })
      );
    } else if (mech.kind === "CONTACT_PAGE_NO_MECHANISM") {
      orgChannels.push(
        createChannel({
          kind: CHANNEL_KIND.ORG_WEBSITE,
          value: pageUrl,
          display_label: "Contact page with no form/mailto/tel mechanism",
          attribution: ATTRIBUTION.ORGANIZATION,
          property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
          customer_caveat: "Contact URL present but no usable contact mechanism detected.",
          evidence: [
            createEvidenceRef({
              source_title: "Organization contact page (no mechanism)",
              source_url: pageUrl,
              source_type: "first_party_or_corp",
              observed_at: new Date().toISOString(),
            }),
          ],
          provenance: { contact_page_mechanism: "CONTACT_PAGE_NO_MECHANISM" },
        })
      );
    } else {
      orgChannels.push(
        createChannel({
          kind: CHANNEL_KIND.ORG_WEBSITE,
          value: pageUrl,
          display_label: "Organization contact page (fallback)",
          attribution: ATTRIBUTION.ORGANIZATION,
          property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
          evidence: [
            createEvidenceRef({
              source_title: "Organization contact page",
              source_url: pageUrl,
              source_type: "first_party_or_corp",
              observed_at: new Date().toISOString(),
            }),
          ],
        })
      );
    }
  }

  // Prefer functionally relevant people first; keep board leadership without requiring email
  const rankedPeople = [...(extracted.people || [])].sort((a, b) => {
    const score = (p) =>
      p.leadership_category === LEADERSHIP_CATEGORY.DEVELOPMENT_ASSET ||
      p.leadership_category === LEADERSHIP_CATEGORY.OPERATING_EXECUTIVE
        ? 3
        : p.leadership_category === LEADERSHIP_CATEGORY.BOARD_OWNERSHIP
          ? 1
          : 0;
    return score(b) - score(a);
  });

  for (const p of rankedPeople.slice(0, 6)) {
    if (PRESS_TITLE_RE.test(p.title || "")) {
      rejected.push({
        value: p.display_name,
        reason: CI_FAILURE_CODE.POLICY_RESTRICTED,
        detail: "press_contact_not_development_dm",
      });
      continue;
    }
    const leadership_category =
      p.leadership_category || classifyLeadershipCategory(p.title);
    const rel = relevanceForPerson(p.title, hotelName, ownerName, leadership_category);
    people.push(
      createPersonContact({
        display_name: p.display_name,
        title: p.title,
        organization_entity_id: ownerId,
        organization_name: ownerName,
        role_observed_at: p.publication_date || null,
        role_currency: ROLE_CURRENCY.UNKNOWN,
        property_relevance: rel.relevance,
        channels: [],
        evidence: [
          createEvidenceRef({
            source_title: "Team / leadership page",
            source_url: p.source_url || pageUrl,
            source_type: "first_party_or_corp",
            observed_at: p.retrieved_at || new Date().toISOString(),
            excerpt: `${p.display_name} — ${p.title}`,
          }),
        ],
        unresolved_reasons: [UNRESOLVED_REASON.NO_PERSON_EVIDENCE],
        why_relevant: rel.why,
        publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
        customer_caveat: p.role_currency_note || null,
        provenance: {
          evidenced_or_inferred: rel.evidenced_or_inferred,
          basis: "first_party_title_extraction",
          identity_affiliation: "EVIDENCED",
          leadership_category,
          commercial_relevance: rel.commercial_relevance,
          decision_responsibility: rel.decision_responsibility,
          functionally_relevant: rel.functionally_relevant,
          publication_date: p.publication_date || null,
          retrieved_at: p.retrieved_at || null,
          source_context: p.source_context || null,
          extraction_method: p.extraction_method || null,
        },
      })
    );
  }

  // Corroborate hypotheses when name appears on first-party org page
  const hay = String(pageText || "");
  for (const hp of personHypotheses) {
    if (!hp?.display_name || hp.deceased) continue;
    const nameRe = new RegExp(
      hp.display_name
        .split(/\s+/)
        .filter((w) => w.length > 2)
        .map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
        .join("[\\s\\S]{0,40}"),
      "i"
    );
    if (!nameRe.test(hay)) continue;
    const already = people.some(
      (p) =>
        String(p.display_name || "").toLowerCase() === String(hp.display_name).toLowerCase() &&
        p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE"
    );
    if (already) continue;
    const rel = relevanceForPerson(hp.title, hotelName, ownerName);
    people.push(
      createPersonContact({
        display_name: hp.display_name,
        title: hp.title,
        organization_entity_id: ownerId,
        organization_name: ownerName,
        role_observed_at: new Date().toISOString(),
        role_currency: ROLE_CURRENCY.UNKNOWN,
        property_relevance: rel.relevance,
        channels: hp.linkedin
          ? [
              createChannel({
                kind: CHANNEL_KIND.PERSON_LINKEDIN,
                value: hp.linkedin,
                display_label: "Professional profile (not email/phone coverage)",
                attribution: ATTRIBUTION.NAMED_PERSON,
                usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
              }),
            ]
          : [],
        evidence: [
          createEvidenceRef({
            source_title: "Name corroborated on first-party organization page",
            source_url: pageUrl,
            source_type: "first_party_or_corp",
            observed_at: new Date().toISOString(),
            excerpt: `Name match for ${hp.display_name} on ${pageUrl}`,
          }),
        ],
        unresolved_reasons: [UNRESOLVED_REASON.NO_PERSON_EVIDENCE],
        why_relevant: hp.why_relevant_hypothesis || rel.why,
        publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
        customer_caveat:
          "Identity corroborated on first-party page; decision authority still not verified. TITLE_NOT_AUTHORITY.",
        provenance: {
          evidenced_or_inferred: "EVIDENCED_NAME_ON_FIRST_PARTY",
          basis: "hypothesis_name_match_on_owner_domain",
        },
      })
    );
  }
}
