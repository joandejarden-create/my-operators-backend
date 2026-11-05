/**
 * Universal GDI opportunity identity — shared across all 12 demand segments.
 *
 * Separates displayTitle, primary organization, programme, related orgs, and
 * activity summary. Does not invent branded names or destructively truncate
 * legitimate identities. Does not change canonical opportunity IDs.
 */
import { deriveDisplayCompanyName } from "../scaling-display-identity-v1.js";

export const UNIVERSAL_OPPORTUNITY_IDENTITY_V1 =
  "gdi-universal-opportunity-identity-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Event / congress / forum markers → event-level opportunity. */
const EVENT_PROGRAMME_RE =
  /\b(congress|congreso|conference|conferencia|foro|forum|summit|encuentro|convenci[oó]n|expo|training\s+week|training\s+program|hybrid\s+training|seminar|seminario|workshop|taller|world\s+congress|national\s+congress)\b/i;

/** Generic activity phrases that are NOT independent programme brands. */
const GENERIC_ACTIVITY_RE =
  /^(monthly|recurring(?:\s+.+)?|desayuno\s+mensual(?:\s+y\s+.+)?|oferta\s+mensual(?:\s+.+)?|mexico\s+city\s+.+\s+(expansion|launch|scale-?up|office)|workforce\s+scale|regional\s+office|latin\s+america\s+office|ceo\s*\/\s*investor|networking\s+and\s+speed|cursos\s+gratuitos|sesiones\s+empresariales(?:\s+.+)?|destination\s+weddings?(?:\s+and\s+private\s+celebrations)?(?:\s+in\s+[\w\s]+)?|destination\s+wedding\s+planning(?:\s+and\s+coordination)?|wedding\s+planning\s+and\s+coordination|private\s+celebrations(?:\s+in\s+[\w\s]+)?|grenad[ae]?\s+destination\s+weddings?(?:\s+and\s+.+)?|grenad[ae]?\s+small\s+high-end\s+corporate[- ]?retreat(?:\s+and\s+incentive\s+feeder)?|(?:.+?\s+)?incentive\s+feeder)$/i;

/** Segments where the verified company/organization is the commercial identity. */
const ACCOUNT_LED_SEGMENT_RE =
  /^(travel_trade_group_intermediaries|social_special_events|professional_event_planners|preferred_lodging(?:_partnerships?)?|corporate_meetings_incentives|corporate_accounts_scaling|scaling_companies)$/i;

const PROJECT_RE =
  /\b(terminal|freight|construction|infraestructura|infrastructure|proyecto|project|obra)\b/i;

/**
 * Split concatenated organizer chains only when parts look like distinct orgs.
 * Never invent names — only partition an already-concatenated research field.
 */
export function partitionOrganizationField(rawOrg = "") {
  const raw = text(rawOrg);
  if (!raw) {
    return { primaryOrganizationName: null, relatedOrganizations: [], rawOrganizationField: null };
  }

  // Multi-org "A / B / C" or "A, B, C and D" patterns with 2+ separators.
  const slashParts = raw.split(/\s*\/\s*/).map(text).filter(Boolean);
  if (slashParts.length >= 2 && slashParts.every((p) => p.length >= 8 && p.length <= 120)) {
    // Avoid splitting legitimate single names that contain a slash inside a short label
    // when only one part and slash is inside parentheses — handled by length checks.
    const looksLikeOrgs = slashParts.every(
      (p) =>
        /\b(A\.?\s*C\.?|Instituto|Asociaci[oó]n|Organizaci[oó]n|Gobierno|Secretar[ií]a|Comisi[oó]n|Agency|OECD|Red\b|Municipio|Universidad|Centre|Center)\b/i.test(
          p,
        ) || p.length >= 20,
    );
    if (looksLikeOrgs && (slashParts.length >= 3 || raw.length > 70)) {
      return {
        primaryOrganizationName: slashParts[0],
        relatedOrganizations: slashParts.slice(1),
        rawOrganizationField: raw,
        namingBasis: "partitioned_slash_org_chain",
      };
    }
  }

  // "OECD, <Long Forum Name> (IGF), CIAT and GIZ" — do not split commas inside forum titles
  const acronymTrail = raw.match(
    /^([^,]+),\s*(.+?)\s*\(([A-ZÁÉÍÓÚÑ]{2,12})\),\s*([A-ZÁÉÍÓÚÑ]{2,12})\s+and\s+([A-ZÁÉÍÓÚÑ]{2,12})\s*$/i,
  );
  if (acronymTrail) {
    return {
      primaryOrganizationName: text(acronymTrail[1]),
      relatedOrganizations: [
        `${text(acronymTrail[2])} (${acronymTrail[3]})`,
        text(acronymTrail[4]),
        text(acronymTrail[5]),
      ],
      rawOrganizationField: raw,
      namingBasis: "partitioned_acronym_org_chain",
    };
  }

  // "International Energy Agency (IEA) and Mexico Secretariat of Energy (SENER)"
  const dualAnd = raw.match(
    /^(.+?\([A-ZÁÉÍÓÚÑ]{2,12}\))\s+and\s+(.+?\([A-ZÁÉÍÓÚÑ]{2,12}\))\s*$/i,
  );
  if (dualAnd) {
    return {
      primaryOrganizationName: text(dualAnd[1]),
      relatedOrganizations: [text(dualAnd[2])],
      rawOrganizationField: raw,
      namingBasis: "partitioned_dual_and_orgs",
    };
  }

  // "Municipio … – Instituto … (ICCAL)" → prefer institute with acronym when present
  const dashParts = raw.split(/\s+[–—]\s+/).map(text).filter(Boolean);
  if (dashParts.length === 2) {
    const right = dashParts[1];
    const acr = right.match(/\(([A-ZÁÉÍÓÚÑ]{2,8})\)/);
    if (acr || /\bInstituto\b/i.test(right)) {
      return {
        primaryOrganizationName: right,
        relatedOrganizations: [dashParts[0]],
        rawOrganizationField: raw,
        namingBasis: "partitioned_en_dash_institute_primary",
      };
    }
  }

  return {
    primaryOrganizationName: raw,
    relatedOrganizations: [],
    rawOrganizationField: raw,
    namingBasis: "single_organization_field",
  };
}

export function isAccountLedSegment(segmentId = "") {
  return ACCOUNT_LED_SEGMENT_RE.test(text(segmentId));
}

/**
 * Property-tied or branded commercial offering that is not a public named event
 * and not a generic activity description — titles as "Org — Opportunity".
 */
export function isCompanySpecificCommercialOpportunity(
  programme = "",
  organizationName = "",
) {
  const p = text(programme);
  if (!p || isGenericProgramme(p, organizationName)) return false;
  if (EVENT_PROGRAMME_RE.test(p)) return false;
  // Named galas / anniversary programmes keep programme-first naming elsewhere.
  if (/\b(gala|anniversary|full\s+circle|awards?\s+dinner)\b/i.test(p)) return false;
  // Verified property / resort commercial product (e.g. Kenwood × Spice Island weddings).
  if (
    /\b(resort|hotel|beach\s+resort)\b/i.test(p) &&
    /\b(wedding|vow|renewal|incentive|retreat|package)\b/i.test(p) &&
    !/^destination\s+weddings?\b/i.test(p) &&
    !/^grenad[ae]?\s+destination\s+weddings?\b/i.test(p)
  ) {
    return true;
  }
  return false;
}

export function detectOpportunityLevel({
  segmentId = "",
  programmeName = "",
  organizationName = "",
  demandMechanism = "",
} = {}) {
  const prog = text(programmeName);
  const mech = text(demandMechanism);
  const blob = `${prog} ${mech}`;

  if (EVENT_PROGRAMME_RE.test(prog) || EVENT_PROGRAMME_RE.test(mech)) {
    return "event";
  }
  if (PROJECT_RE.test(prog) && /relocation|projects_infrastructure|government/i.test(segmentId)) {
    return "project";
  }
  if (
    segmentId === "relocation_extended_stay" &&
    (GENERIC_ACTIVITY_RE.test(prog) ||
      /\b(expansion|office\s+launch|workforce|operations\s+center)\b/i.test(prog))
  ) {
    return "account";
  }
  if (
    segmentId === "local_catering_day_meetings" &&
    (GENERIC_ACTIVITY_RE.test(prog) || prog.length < 28)
  ) {
    // Branded series (Desayunos USEM, Sesión Comida, IPADE D-1) stay programmes.
    // Generic "desayuno mensual" / "oferta mensual" → account (org-led title).
    if (
      /\b(desayunos\s+usem|sesi[oó]n\s+comida|programa\s+de|convenci[oó]n|d-1\s*r|alta\s+direcci[oó]n)\b/i.test(
        prog,
      )
    ) {
      return "programme";
    }
    return "account";
  }
  if (segmentId === "corporate_accounts_scaling" || /scaling/i.test(segmentId)) {
    return "account";
  }
  // Travel trade / wedding planners / intermediaries: org is the commercial identity
  // unless a named event or branded series is present.
  if (
    isAccountLedSegment(segmentId) &&
    (isGenericProgramme(prog, organizationName) ||
      isCompanySpecificCommercialOpportunity(prog, organizationName) ||
      !prog)
  ) {
    return "account";
  }
  if (prog && !isGenericProgramme(prog, organizationName) && prog.length >= 12) {
    if (/\b(programa|desayunos?|sesi[oó]n|training\s+program|hybrid\s+training|gala|anniversary)\b/i.test(prog)) {
      return "programme";
    }
  }
  if (prog && organizationName && prog.toLowerCase() !== text(organizationName).toLowerCase()) {
    if (EVENT_PROGRAMME_RE.test(blob)) return "event";
    if (isAccountLedSegment(segmentId) && isGenericProgramme(prog, organizationName)) {
      return "account";
    }
    return "programme";
  }
  return "account";
}

function normalizeProgrammeDisplay(programme = "") {
  let p = text(programme);
  if (!p) return null;
  // Prefer slash form for bilingual "y" list programmes when already used in gold copy.
  if (/^Congreso Internacional de Regidores y Concejales/i.test(p)) {
    p = p.replace(/\by\b/i, "/");
  }
  if (/Desayunos USEM y Sesión/i.test(p)) {
    p = p.replace(/\by\b/i, "/");
  }
  return p;
}

export function isGenericProgramme(programme = "", organizationName = "") {
  const p = text(programme);
  const o = text(organizationName);
  if (!p) return true;
  // Named events/congresses are never generic activity descriptions.
  if (EVENT_PROGRAMME_RE.test(p)) return false;
  if (GENERIC_ACTIVITY_RE.test(p)) return true;
  if (o && p.toLowerCase() === o.toLowerCase()) return true;
  // Destination-wedding / celebration activity lines are service descriptions, not brands.
  if (
    !EVENT_PROGRAMME_RE.test(p) &&
    /^(destination\s+weddings?|destination\s+wedding\s+planning|wedding\s+planning|private\s+celebrations|grenad[ae]?\s+destination\s+weddings?|grenad[ae]?\s+small\s+high-end)/i.test(
      p,
    )
  ) {
    return true;
  }
  // Recurring feeder / monthly meeting activity descriptions (not branded series).
  if (
    !EVENT_PROGRAMME_RE.test(p) &&
    /^(recurring\b|desayuno\s+mensual\b|oferta\s+mensual\b)/i.test(p) &&
    !/\b(desayunos\s+usem|sesi[oó]n\s+comida|d-1|alta\s+direcci[oó]n|gala|anniversary)\b/i.test(
      p,
    )
  ) {
    return true;
  }
  if (
    !EVENT_PROGRAMME_RE.test(p) &&
    /\b(incentive\s+feeder|corporate[- ]?retreat\s+and\s+incentive|planning\s+and\s+coordination|select-group\s+programmes?|incentive-group)\b/i.test(
      p,
    ) &&
    !/\b(congress|conference|summit|gala|anniversary)\b/i.test(p)
  ) {
    return true;
  }
  // Descriptive activity without event markers and without branded series words
  if (
    !EVENT_PROGRAMME_RE.test(p) &&
    !/\b(desayunos?|programa\s+de|sesi[oó]n\s+comida|convenci[oó]n|d-1|alta\s+direcci[oó]n|gala|anniversary)\b/i.test(
      p,
    ) &&
    /\b(expansion|launch|scale-?up|office|workforce|networking|cursos|speed-networking)\b/i.test(p)
  ) {
    return true;
  }
  return false;
}

/**
 * Short customer-facing org label — strip trailing legal suffixes only.
 * Does not invent abbreviations.
 */
export function shortenOrganizationDisplay(name = "") {
  let s = text(name);
  if (!s) return null;
  // Prefer "AMEXME Capítulo Cancún" when full Asociación… chapter form is long
  const cap = s.match(
    /Asociaci[oó]n Mexicana de Mujeres Empresarias\s*\(AMEXME\)\s*,?\s*(Cap[ií]tulo\s+\S+)/i,
  );
  if (cap) return `AMEXME ${cap[1]}`;
  const amexme = s.match(
    /^Asociaci[oó]n Mexicana de Mujeres Empresarias\s*\(AMEXME\)\s*$/i,
  );
  if (amexme) return "AMEXME";
  // Keep verified legal suffixes (LLC / Ltd.) — they are part of the company identity.
  // Prefer companyName / displayCompanyName upstream when a shorter commercial
  // label is already verified (e.g. Aventa without Ltd).
  if (s.length <= 72) return s;
  // Never collapse multi-org "A (X) and B (Y)" chains to a bare trailing acronym.
  if (/\band\b|\//i.test(s)) return s;
  // Keep full "Legal Name (ACR)" form — do not invent short brands.
  return s;
}

/**
 * Build canonical identity pack from candidate + deep research.
 */
export function buildUniversalOpportunityIdentity({
  segmentId = "",
  candidate = {},
  deepResearch = {},
  preserveFields = {},
} = {}) {
  const deep = deepResearch || {};
  const rawOrg =
    text(candidate.organizationName) ||
    text(preserveFields.organizationName) ||
    "";
  const rawProgramme =
    text(candidate.programmeName) ||
    text(deep.programmeIdentity)?.split(/[.;]/)[0] ||
    text(preserveFields.programmeName) ||
    "";
  const programme = normalizeProgrammeDisplay(
    text(candidate.programmeName) ||
      // Prefer structured candidate programme over long deep.programmeIdentity essays
      (text(rawProgramme).length <= 160 ? rawProgramme : text(candidate.programmeName)) ||
      "",
  );
  // If programme still empty, try first clause after em dash in deep identity
  let verifiedProgrammeName = programme;
  if (!verifiedProgrammeName && text(deep.programmeIdentity)) {
    const di = text(deep.programmeIdentity);
    const after = di.includes("—") ? text(di.split("—").slice(1).join("—")) : di;
    verifiedProgrammeName = normalizeProgrammeDisplay(after.split(/[.;]/)[0]);
  }

  const partitioned = partitionOrganizationField(rawOrg);
  let primaryOrganizationName = partitioned.primaryOrganizationName;
  const relatedOrganizations = [...(partitioned.relatedOrganizations || [])];

  // Prefer shorter display form for long association names when acronym exists
  const primaryDisplayOrg = shortenOrganizationDisplay(primaryOrganizationName) || primaryOrganizationName;

  const level = detectOpportunityLevel({
    segmentId,
    programmeName: verifiedProgrammeName || "",
    organizationName: primaryOrganizationName || "",
    demandMechanism: text(candidate.demandMechanismSummary || deep.demandMechanism),
  });

  const activityDescription =
    text(candidate.demandMechanismSummary) ||
    text(deep.whatIsHappening)?.slice(0, 280) ||
    text(verifiedProgrammeName) ||
    null;

  let displayTitle = null;
  let namingBasis = partitioned.namingBasis || "unspecified";
  const specificCommercial = isCompanySpecificCommercialOpportunity(
    verifiedProgrammeName || "",
    primaryOrganizationName || "",
  );

  if (segmentId === "corporate_accounts_scaling" || /scaling/i.test(segmentId)) {
    // Same short brand as normalizeScalingListPresentation / deriveDisplayCompanyName
    // so tile, drawer, CSV, and share never diverge on legal-entity chains or
    // "(trading as X)" forms.
    displayTitle =
      deriveDisplayCompanyName(primaryDisplayOrg || rawOrg) ||
      primaryDisplayOrg;
    namingBasis = "scaling_company_name";
  } else if (level === "event" && verifiedProgrammeName && !isGenericProgramme(verifiedProgrammeName, primaryOrganizationName)) {
    displayTitle = verifiedProgrammeName;
    namingBasis = "event_or_congress_programme_name";
  } else if (level === "project" && verifiedProgrammeName) {
    displayTitle = verifiedProgrammeName.replace(/\s+freight terminal$/i, "").trim() || verifiedProgrammeName;
    // Keep "Terminal Multimodal Cancún" readable
    if (/Terminal Multimodal Cancún/i.test(verifiedProgrammeName)) {
      displayTitle = "Terminal Multimodal Cancún";
    }
    namingBasis = "verified_project_name";
  } else if (
    primaryDisplayOrg &&
    verifiedProgrammeName &&
    specificCommercial &&
    (level === "account" || isAccountLedSegment(segmentId))
  ) {
    // Company-specific commercial opportunity: Org — verified offering
    // Light display polish: title-case fully-lowercase English tokens in the
    // programme half (e.g. "weddings" → "Weddings") without inventing names.
    const polishedProgramme = verifiedProgrammeName.replace(
      /\b([a-z][a-z']*)\b/g,
      (word) => {
        if (
          /^(and|or|of|the|for|in|a|an|to|vs|via|with|on|at)$/i.test(word)
        ) {
          return word.toLowerCase();
        }
        return word.charAt(0).toUpperCase() + word.slice(1);
      },
    );
    displayTitle = `${primaryDisplayOrg} — ${polishedProgramme}`;
    namingBasis = "organization_led_specific_opportunity";
  } else if (
    level === "account" &&
    primaryDisplayOrg &&
    (isAccountLedSegment(segmentId) ||
      isGenericProgramme(verifiedProgrammeName || "", primaryOrganizationName || ""))
  ) {
    displayTitle = primaryDisplayOrg;
    namingBasis = "account_organization_name";
  } else if (level === "programme" && verifiedProgrammeName && !isGenericProgramme(verifiedProgrammeName, primaryOrganizationName)) {
    displayTitle = verifiedProgrammeName;
    namingBasis = "verified_programme_or_series_name";
  } else {
    displayTitle = primaryDisplayOrg || verifiedProgrammeName || "Opportunity";
    namingBasis =
      level === "account" ? "account_organization_name" : namingBasis || "organization_fallback";
  }

  // Strip accidental org — programme concatenation except intentional company-specific titles.
  if (
    namingBasis !== "organization_led_specific_opportunity" &&
    displayTitle.includes(" — ") &&
    (displayTitle === `${rawOrg} — ${verifiedProgrammeName}` ||
      displayTitle === `${primaryDisplayOrg} — ${verifiedProgrammeName}`)
  ) {
    displayTitle =
      level === "account" || isGenericProgramme(verifiedProgrammeName, primaryOrganizationName)
        ? primaryDisplayOrg
        : verifiedProgrammeName;
  }

  // Classification line: org + segment handled by UI; expose clean primary org
  const classificationOrganization = primaryDisplayOrg;

  return {
    version: UNIVERSAL_OPPORTUNITY_IDENTITY_V1,
    displayTitle: text(displayTitle),
    primaryOrganizationName: primaryDisplayOrg,
    primaryOrganizationRole:
      level === "event" || level === "programme" ? "organizer" : "account_organization",
    verifiedProgrammeName: verifiedProgrammeName || null,
    activityDescription,
    relatedOrganizations,
    demandMechanism: text(candidate.demandMechanismSummary) || null,
    segmentId: segmentId || null,
    opportunityLevel: level,
    identityEvidence: {
      rawOrganizationField: partitioned.rawOrganizationField,
      candidateProgrammeName: text(candidate.programmeName) || null,
      deepProgrammeIdentityHead: text(deep.programmeIdentity)?.slice(0, 160) || null,
    },
    identityConfidence:
      verifiedProgrammeName && primaryOrganizationName ? "HIGH" : "MEDIUM",
    namingBasis,
    researchTitlePreserved:
      text(preserveFields.title) ||
      (rawOrg && verifiedProgrammeName ? `${rawOrg} — ${verifiedProgrammeName}` : rawOrg) ||
      null,
    classificationOrganization,
    // Section 1 identity line = display title only (org lives in classification / overview fields)
    opportunityOverviewIdentity: text(displayTitle),
  };
}

/**
 * Single source of truth for customer-facing opportunity titles.
 * Rebuilds from verified org + programme + segment so stale generic
 * displayTitle / drawerHeaderTitle values cannot diverge across tile,
 * drawer, list, grid, or export surfaces.
 */
export function resolveCanonicalCustomerFacingTitle(opportunity = {}) {
  const o = opportunity || {};
  const segmentId =
    text(o.researchSegment) ||
    text(o.segmentId) ||
    text(o.demandSegment) ||
    text(o.opportunityIdentity?.segmentId) ||
    text(o.segment) ||
    "";
  // Prefer commercial short names (companyName / displayCompanyName) over legal
  // organizationName so tiles show "Aventa Group / Carlisle Laboratories"
  // rather than "... Ltd" or research dumps.
  const org =
    text(o.displayCompanyName) ||
    text(o.companyName) ||
    text(o.organizationName) ||
    text(o.opportunityIdentity?.primaryOrganizationName) ||
    text(o.classificationOrganization) ||
    "";
  const titleHint =
    text(o.eventDisplayName) ||
    text(o.eventName) ||
    text(o.canonicalEventDisplayTitle) ||
    text(o.displayTitle) ||
    text(o.drawerHeaderTitle) ||
    text(o.title) ||
    "";
  let programme =
    text(o.programmeName) ||
    text(o.opportunityIdentity?.verifiedProgrammeName) ||
    text(o.eventDisplayName) ||
    text(o.eventName) ||
    "";
  // Never treat a long research paragraph as the programme name.
  if (programme && programme.length > 160) {
    programme = "";
  }
  // When programmeName was never persisted, recover named-event titles from the
  // existing customer/research title — do not collapse "2027 U.S. IFE Conference"
  // to a bare organization acronym. Skip research essays (>160 chars).
  if (
    !programme &&
    titleHint &&
    titleHint.length <= 160 &&
    titleHint.toLowerCase() !== org.toLowerCase() &&
    (EVENT_PROGRAMME_RE.test(titleHint) ||
      /\b(20\d{2}|annual|gala|anniversary)\b/i.test(titleHint))
  ) {
    programme = titleHint;
  }
  const preserved =
    text(o.researchTitlePreserved) ||
    text(o.opportunityIdentity?.researchTitlePreserved) ||
    text(o.title) ||
    "";

  if (org || programme) {
    const rebuilt = buildUniversalOpportunityIdentity({
      segmentId,
      candidate: {
        organizationName: org,
        programmeName: programme,
        demandMechanismSummary:
          text(o.demandMechanism) ||
          text(o.opportunityIdentity?.demandMechanism) ||
          text(o.summaryWhat) ||
          "",
      },
      deepResearch: {},
      preserveFields: {
        title: preserved,
        organizationName: org,
        programmeName: programme,
      },
    });
    const rebuiltTitle = text(rebuilt.displayTitle) || null;
    // List DTOs often drop programmeName. Do not degrade an already-canonical
    // company-specific "Org — Programme" displayTitle (e.g. Kenwood Travel —
    // Spice Island …) back to a bare organization name. Never preserve
    // research essays, scaling mocks, or non-commercial suffixes.
    const existingDisplay = text(o.displayTitle) || text(o.drawerHeaderTitle) || "";
    if (
      existingDisplay &&
      org &&
      rebuiltTitle &&
      rebuiltTitle === org &&
      existingDisplay.startsWith(`${org} — `) &&
      existingDisplay.length <= 160
    ) {
      const programmeHalf = text(existingDisplay.slice(org.length + 3));
      if (
        programmeHalf &&
        isCompanySpecificCommercialOpportunity(programmeHalf, org)
      ) {
        return existingDisplay;
      }
    }
    return rebuiltTitle;
  }

  return (
    text(o.opportunityIdentity?.displayTitle) ||
    text(o.displayTitle) ||
    text(o.drawerHeaderTitle) ||
    text(o.title) ||
    null
  );
}

/**
 * Apply canonical title onto opportunity presentation fields without mutating
 * opportunity id, qualification, contacts, evidence, or commercial theses.
 */
export function applyCanonicalCustomerFacingTitle(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const canonical = resolveCanonicalCustomerFacingTitle(o);
  if (!canonical) return o;
  const priorTitle = text(o.title) || text(o.displayTitle) || null;
  o.displayTitle = canonical;
  o.drawerHeaderTitle = canonical;
  // Keep research/source title separately for auditability when it differed.
  if (priorTitle && priorTitle !== canonical) {
    o.researchTitlePreserved = o.researchTitlePreserved || priorTitle;
  }
  if (o.opportunityIdentity && typeof o.opportunityIdentity === "object") {
    o.opportunityIdentity = {
      ...o.opportunityIdentity,
      displayTitle: canonical,
      opportunityOverviewIdentity: canonical,
      researchTitlePreserved:
        o.opportunityIdentity.researchTitlePreserved || o.researchTitlePreserved || priorTitle,
    };
  }
  if (o.eventsOpportunityOverview && typeof o.eventsOpportunityOverview === "object") {
    o.eventsOpportunityOverview = {
      ...o.eventsOpportunityOverview,
      drawerHeaderTitle: canonical,
      identityLine: canonical,
    };
  }
  if (o.segmentOverview && typeof o.segmentOverview === "object") {
    o.segmentOverview = {
      ...o.segmentOverview,
      identityLine: canonical,
    };
  }
  // Do not overwrite raw `title` when it is the research/source string and
  // differs — presentation surfaces must use displayTitle / drawerHeaderTitle.
  if (!text(o.title) || text(o.title) === canonical) {
    o.title = canonical;
  }
  return o;
}

/**
 * Assert tile title and drawer title consume the same displayTitle.
 */
export function assertTileDrawerTitleConsistency({
  tileTitle = "",
  drawerTitle = "",
  displayTitle = "",
} = {}) {
  const t = text(tileTitle);
  const d = text(drawerTitle);
  const c = text(displayTitle);
  const failures = [];
  if (c && t && t !== c) failures.push("tile_title_ne_displayTitle");
  if (c && d && d !== c) failures.push("drawer_title_ne_displayTitle");
  if (t && d && t !== d) failures.push("tile_ne_drawer");
  return { ok: failures.length === 0, failures };
}

export function looksLikeConcatenatedOrganizerTitle(title = "") {
  const t = text(title);
  if (!t) return false;
  if (/\s—\s/.test(t) && (t.match(/\s\/\s/g) || []).length >= 2) return true;
  if ((t.match(/\s\/\s/g) || []).length >= 2 && t.length > 80) return true;
  return false;
}
