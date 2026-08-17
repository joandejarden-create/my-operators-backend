/**
 * Wave 12 Stage 4 — expand seeds + source packs into full Presentation packs.
 */
import {
  buildRecentMomentumCard,
  withRecentMomentumSortOrder,
  isStructuredMomentumDateLine,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";
import {
  buildOpeningsPropertyCard,
  OPENINGS_SLOT,
} from "./brand-explorer-openings-property-card-contract.js";
import { getWave12SourcePack } from "./brand-explorer-wave12-source-packs-content.js";
import {
  WAVE12_TAB_FACTORY_SEEDS,
  getWave12TabFactorySeed,
} from "./brand-explorer-wave12-tab-factory-seeds.js";
import { WAVE12_SLUGS } from "./brand-explorer-wave12-factory-plan.js";
import {
  evaluateScenarioOwnerValueBar,
  toProperCaseScenarioTitle,
  stripRepeatedScenarioDiligencePad,
  isReferenceMetaScenarioTitle,
  isReferenceMetaScenarioBody,
  REPEATED_SCENARIO_DILIGENCE_RE,
} from "./brand-explorer-scenario-owner-value-bar.js";

export const WAVE12_TAB_FACTORY_GENERATOR_VERSION = "wave12-tab-factory-generator-v2";
export const WAVE12_REPEATED_SCENARIO_DILIGENCE_RE = REPEATED_SCENARIO_DILIGENCE_RE;
export { stripRepeatedScenarioDiligencePad, toProperCaseScenarioTitle };

/** Matches golden generic_audience_prose (Luxury/Discerning adjacent to Leisure/Experience-Oriented). */
export const WAVE12_GENERIC_AUDIENCE_PROSE_RE =
  /Luxury\s*\/\s*Discerning[,\s]+(?:Experience-Oriented|Leisure)|Leisure Discerning travelers/i;

function row(slotKey, title, body, sortOrder, extra = {}) {
  return {
    slotKey,
    title: title || "",
    body,
    sortOrder,
    ...(extra.caseSummaryOverview ? { caseSummaryOverview: extra.caseSummaryOverview } : {}),
    ...(extra.caseSummaryBrandRelevance
      ? { caseSummaryBrandRelevance: extra.caseSummaryBrandRelevance }
      : {}),
    ...(extra.caseSummaryOwnerObjective
      ? { caseSummaryOwnerObjective: extra.caseSummaryOwnerObjective }
      : {}),
    ...(extra.caseSummaryInterpretation
      ? { caseSummaryInterpretation: extra.caseSummaryInterpretation }
      : {}),
    ...(extra.caseSummaryTags ? { caseSummaryTags: extra.caseSummaryTags } : {}),
  };
}

function bullets(lines) {
  return lines.filter(Boolean).join("\n");
}

function words(text) {
  return String(text || "")
    .split(/\s+/)
    .filter(Boolean).length;
}

function marketCityFromMarket(market) {
  const m = String(market || "");
  return m.split(",")[0].trim() || m;
}

function countryFromMarket(market) {
  const parts = String(market || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return parts.length > 1 ? parts[parts.length - 1] : "";
}

/** Normalize source-pack date lines to structured Mon YYYY / YYYY / Directory forms. */
export function normalizeWave12MomentumDateLine(raw) {
  const t = String(raw || "").trim();
  if (!t) return "Directory";
  if (isStructuredMomentumDateLine(t)) return t;
  const dayMonth = t.match(
    /^(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2},?\s+(\d{4})$/i
  );
  if (dayMonth) {
    const mon = dayMonth[1].slice(0, 3);
    const map = {
      jan: "January",
      feb: "February",
      mar: "March",
      apr: "April",
      may: "May",
      jun: "June",
      jul: "July",
      aug: "August",
      sep: "September",
      oct: "October",
      nov: "November",
      dec: "December",
    };
    return `${map[mon.toLowerCase()] || dayMonth[1]} ${dayMonth[2]}`;
  }
  const rangeParen = t.match(/^(\d{4})\s*[–—-]\s*(\d{4})/);
  if (rangeParen) return `${rangeParen[1]}–${rangeParen[2]}`;
  const yearParen = t.match(/^(\d{4})\s*\(/);
  if (yearParen) return yearParen[1];
  if (/^ongoing/i.test(t) || /^pipeline/i.test(t)) return "Directory";
  return isStructuredMomentumDateLine(t) ? t : "Directory";
}

function ensureMinWords(summary, minOrPad, maybePad) {
  let s = String(summary || "").trim();
  let target = 35;
  let pad = "";
  if (typeof minOrPad === "number") {
    target = minOrPad;
    pad = String(maybePad || "").trim();
  } else {
    pad = String(minOrPad || "").trim();
  }
  if (words(s) >= target) return s;
  if (!pad) return s;
  s = `${s} ${pad}`.trim();
  if (words(s) < target) s = `${s} ${pad}`.trim();
  return s;
}

/**
 * Unique owner-value closers per scenario index — Kimpton / Curio / Design Hotels bar.
 * Never reuse one diligence line on all three cards.
 * Never emit source-pack / geography-label / “reference property” meta instructions.
 */
function scenarioOwnerValueClose(seed, brandDisplayName, index) {
  const peer = seed.distinguish?.[0] || "peer brands";
  const lens = String(seed.ownerLens || "brand-specific owner economics")
    .split(",")[0]
    .trim();
  const closes = [
    `Owner value is strongest when the asset can deliver ${lens} under the ${seed.shortName} promise without stretching capital or operating capacity beyond what the market can support. Weaker when conversion PIP or prototype scope would outrun underwriting.`,
    `Confirm ${seed.parentCompany} development guidance, commercial terms, and operating depth before treating affiliation as a light cosmetic reflag. This path beats forcing a heavier peer such as ${peer} only when obligations stay achievable.`,
    `Portfolio or multi-asset owners capture value when ${seed.shortName} standardizes guest promise and systems rhythm while each asset still clears local underwriting. Weaker when markets cannot support the design, F&B, and service investment ${brandDisplayName} presentation expects.`,
  ];
  return closes[index] || closes[0];
}

/** Owner-facing stub chip scrub (golden gate). Preserve newlines for momentum bodies. */
export function scrubWave12StubChips(text) {
  return String(text || "")
    .replace(/\bconversion-friendly\.?\b/gi, "conversion-oriented")
    .replace(/\bneighborhood focus\b/gi, "neighborhood lifestyle positioning")
    .replace(/\bboutique design\b/gi, "design-led boutique identity")
    .replace(/[^\S\n]{2,}/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .trim();
}

function proofPad(seed, brandDisplayName) {
  return `Keep ${brandDisplayName} diligence specific to the ${seed.shortName} guest promise and ${seed.loyaltyProgram} obligations rather than parent-company familiarity alone.`;
}

function lifecyclePad(seed, brandDisplayName) {
  return `Confirm owner, operator, and brand responsibilities for ${brandDisplayName} so the ${seed.model} stays deliverable after affiliation.`;
}

function buildOpeningsFromSource(seed, sourcePack, brandDisplayName) {
  const calaPreferred =
    seed.calaAvailability === "strong" || seed.calaAvailability === "partial";
  const examples = [
    ...(sourcePack?.propertyExamples || []),
    ...(seed.supplementalOpenings || []).map((s) => ({
      propertyName: s.propertyName,
      url: s.url,
      geographyLabel: s.geographyLabel,
      market: s.market,
      matchKey: s.propertyName,
      teaser: s.teaser,
      marketCity: s.marketCity,
      country: s.country,
    })),
  ];
  if (calaPreferred) {
    examples.sort((a, b) => {
      const ac = a.geographyLabel === "CALA" ? 0 : 1;
      const bc = b.geographyLabel === "CALA" ? 0 : 1;
      return ac - bc;
    });
  }
  const seen = new Set();
  const cards = [];
  for (const ex of examples) {
    const key = String(ex.matchKey || ex.propertyName || "").toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const marketCity = ex.marketCity || marketCityFromMarket(ex.market);
    const country = ex.country || countryFromMarket(ex.market);
    const geo = ex.geographyLabel === "CALA" ? "CALA" : "International Reference";
    const teaser =
      ex.teaser ||
      `${geo} ${brandDisplayName} hotel in ${ex.market || marketCity} for owners comparing ${seed.ownerLens.split(",")[0]} on a named ${seed.shortName} asset with an official property page.`;
    const card = buildOpeningsPropertyCard({
      propertyName: ex.propertyName,
      brandName: brandDisplayName,
      marketCity,
      country,
      chips: [geo, marketCity || country || "Market", seed.shortName, "Property reference"],
      locationLine: [marketCity, country].filter(Boolean).join(", "),
      metaLine: `${geo} · ${seed.shortName} · official property page`,
      scenarioLine: `${geo} / ${seed.shortName}`.toUpperCase(),
      teaser,
      sourceUrl: ex.url,
      caseSummaryOverview: `${ex.propertyName} is a ${geo} ${brandDisplayName} property reference for owners comparing ${seed.model} fit in ${ex.market || marketCity}.`,
      caseSummaryBrandRelevance: `${brandDisplayName} remains a ${seed.model} inside ${seed.parentCompany}.`,
      caseSummaryOwnerObjective: `Evaluate ${seed.ownerLens.split(",")[0]} against this named ${seed.shortName} property.`,
      caseSummaryInterpretation: `Named property example for ${brandDisplayName}; URL is tied to ${ex.propertyName}.`,
      caseSummaryTags: `${geo}, ${seed.shortName}, ${marketCity || country}, Property example`,
    });
    cards.push({
      ...card,
      sortOrder: 490 + cards.length,
    });
    if (cards.length >= 3) break;
  }
  return cards;
}

function momentumPad(seed, brandName, geo) {
  return `Owners should use this ${geo} ${brandName} signal when comparing ${seed.ownerLens.split(",")[0]} and whether the ${seed.model} fits the specific asset versus ${seed.distinguish[0]}.`;
}

function buildMomentumRows(seed, sourcePack, brandDisplayName, calaAvailable) {
  const brandName = String(brandDisplayName || seed.name || seed.shortName || "").trim();
  if (!brandName) throw new Error(`buildMomentumRows missing brandDisplayName for ${seed.slug}`);
  const candidates = [];
  const pushCandidate = (c) => {
    if (!c?.announcementUrl && !c?.url) return;
    const forceIntl =
      seed.calaAvailability === "thin" ||
      seed.calaAvailability === "none" ||
      seed.calaAvailability === "international_only";
    const wantsCala =
      !forceIntl &&
      (c.geographyLabel === "CALA" || /cala/i.test(`${c.title} ${c.summary}`));
    candidates.push({
      dateLine: normalizeWave12MomentumDateLine(c.dateLine),
      title: c.title,
      summary: c.summary,
      announcementUrl: c.announcementUrl || c.url,
      geographyLabel: wantsCala ? "CALA" : "International Reference",
    });
  };
  for (const c of sourcePack?.recentMomentumCandidates || []) pushCandidate(c);
  for (const c of seed.momentumExtras || []) pushCandidate(c);

  // Property-backed Directory cards to reach ≥2 / satisfy CALA-first when needed.
  const props = [...(sourcePack?.propertyExamples || [])];
  if (calaAvailable) {
    props.sort((a, b) => (a.geographyLabel === "CALA" ? 0 : 1) - (b.geographyLabel === "CALA" ? 0 : 1));
  }
  for (const ex of props) {
    if (candidates.length >= 3) break;
    const geo = ex.geographyLabel === "CALA" ? "CALA" : "International Reference";
    const already = candidates.some((c) => c.announcementUrl === ex.url);
    if (already) continue;
    if (calaAvailable && geo !== "CALA" && candidates.filter((c) => c.geographyLabel === "CALA").length === 0) {
      // Prefer waiting for a CALA property card first when available.
      const hasCalaProp = props.some((p) => p.geographyLabel === "CALA");
      if (hasCalaProp) continue;
    }
    pushCandidate({
      dateLine: "Directory",
      title: `${brandName} ${ex.propertyName} official property proof · ${geo}`,
      summary: ensureMinWords(
        `${geo} official property page for ${ex.propertyName} under ${brandName} (directory reference as of 2025). This named ${seed.shortName} reference helps owners compare ${seed.ownerLens.split(",")[0]} and platform participation without treating sibling brands as interchangeable.`,
        40,
        momentumPad(seed, brandName, geo)
      ),
      announcementUrl: ex.url,
      geographyLabel: geo,
    });
  }

  while (candidates.length < 2) {
    const page = sourcePack?.developmentPage || sourcePack?.officialBrandPage;
    const geo = calaAvailable ? "CALA" : "International Reference";
    pushCandidate({
      dateLine: "Directory",
      title: `${brandName} official development materials · ${geo}`,
      summary: ensureMinWords(
        `${geo} ${brandName} development materials position the brand as a ${seed.model} (directory reference as of 2025). Owners should diligence ${seed.ownerLens} against current ${seed.parentCompany} development guidance for the specific market.`,
        40,
        momentumPad(seed, brandName, geo)
      ),
      announcementUrl: page?.url,
      geographyLabel: geo,
    });
  }

  // Ensure CALA-first when CALA is available.
  if (calaAvailable) {
    candidates.sort((a, b) => {
      const ac = a.geographyLabel === "CALA" ? 0 : 1;
      const bc = b.geographyLabel === "CALA" ? 0 : 1;
      if (ac !== bc) return ac - bc;
      return 0;
    });
  }

  const built = candidates.slice(0, 4).map((c, i) => {
    const geo = c.geographyLabel === "CALA" ? "CALA" : "International Reference";
    let title = String(c.title || "").trim();
    if (!new RegExp(brandName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(title)) {
      title = `${brandName}: ${title}`;
    }
    if (!/\bCALA\b|\bInternational Reference\b/i.test(title)) {
      title = `${title} · ${geo}`;
    }
    let summary = ensureMinWords(c.summary, 40, momentumPad(seed, brandName, geo));
    if (!/\bCALA\b|\bInternational Reference\b/i.test(summary)) {
      summary = `${geo}. ${summary}`;
    }
    if (!new RegExp(brandName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(summary)) {
      summary = `${brandName}: ${summary}`;
    }
    // Directory / property-proof cards: keep Directory dateLine; year in body for pattern dated gate.
    let dateLine = normalizeWave12MomentumDateLine(c.dateLine);
    if (
      /^\d{4}$/.test(dateLine) &&
      /\/overview\/?$/i.test(String(c.announcementUrl || "")) &&
      !/\b(opening|opens|opened|conversion|signing|milestone|announcement|joins|joined|inaugur)\b/i.test(
        `${title} ${summary}`
      )
    ) {
      dateLine = "Directory";
    }
    if (/^(Directory|Collection|Editorial|Affiliation|Pipeline)$/i.test(dateLine) && !/\b20\d{2}\b/.test(summary)) {
      summary = `${summary} Directory reference as of 2025.`.trim();
    }
    return buildRecentMomentumCard({
      title,
      dateLine,
      summary,
      url: c.announcementUrl,
      sort: i + 1,
    });
  });

  // Newest-first within region, but keep CALA card(s) ahead when CALA available.
  // Do not re-run withRecentMomentumSortOrder after CALA split — that undoes CALA-first.
  let sorted = withRecentMomentumSortOrder(built);
  if (calaAvailable) {
    const cala = withRecentMomentumSortOrder(
      sorted.filter((c) => /\bCALA\b/i.test(`${c.title} ${c.body}`))
    );
    const rest = withRecentMomentumSortOrder(
      sorted.filter((c) => !/\bCALA\b/i.test(`${c.title} ${c.body}`))
    );
    sorted = [...cala, ...rest].map((c, idx) => ({ ...c, sort: idx + 1 }));
  } else {
    // Force International Reference labels when CALA is not available.
    sorted = sorted.map((c, idx) => {
      let title = c.title;
      let body = c.body;
      if (/\bCALA\b/i.test(`${title} ${body}`) && !/\bInternational Reference\b/i.test(`${title} ${body}`)) {
        title = title.replace(/\bCALA\b/gi, "International Reference");
        body = body.replace(/\bCALA\b/gi, "International Reference");
      } else if (!/\bInternational Reference\b/i.test(`${title} ${body}`)) {
        title = `${title} · International Reference`;
      }
      return { ...c, title, body, sort: idx + 1 };
    });
  }

  const rows = [row("footprint.momentum_label", "", RECENT_MOMENTUM_DEFAULT_LABEL, 448)];
  for (const c of sorted) {
    rows.push(row("footprint.momentum", c.title, c.body, 448 + (c.sort || 1)));
  }
  return rows;
}

function calaRegionCopy(seed, brandDisplayName) {
  if (seed.calaAvailability === "strong") {
    return `${brandDisplayName} has meaningful CALA relevance for owners comparing ${seed.model} opportunities. Prefer named CALA property examples and keep each official property page tied to the matching property name.`;
  }
  if (seed.calaAvailability === "partial") {
    return `${brandDisplayName} has partial CALA evidence. Prefer verified CALA opens where available and label pipeline or International Reference examples clearly so owners do not confuse signed deals with operating hotels.`;
  }
  if (seed.calaAvailability === "thin") {
    return `CALA open evidence for ${brandDisplayName} is thin in the current source pack. Treat footprint examples as International Reference unless a verified CALA property page is added later.`;
  }
  return `No verified CALA opens for ${brandDisplayName} are included in the current source pack. All property examples should remain International Reference until official CALA pages are confirmed.`;
}

export function assessWave12TgsRisk(segments = []) {
  const joined = (segments || []).join(", ");
  const luxuryLeisureAdjacency =
    segments.includes("Luxury / Discerning") &&
    (segments.includes("Leisure") || segments.includes("Experience-Oriented"));
  const genericAudienceProse = WAVE12_GENERIC_AUDIENCE_PROSE_RE.test(joined);
  return {
    luxuryLeisureAdjacency,
    genericAudienceProse,
    risk: luxuryLeisureAdjacency || genericAudienceProse,
    joined,
  };
}

/**
 * @param {string} slug
 * @param {{ airtableName?: string, recordId?: string }} [opts]
 * @returns {{ brandSlug, identity, sourcePackMeta, brandLens, presentation, targetGuestSegments, tgsWriteEligible, basicsFields }}
 */
export function generateWave12TabFactoryPack(slug, opts = {}) {
  const seed = getWave12TabFactorySeed(slug);
  if (!seed) throw new Error(`Missing Wave12 tab-factory seed for ${slug}`);
  const sourcePack = getWave12SourcePack(slug);
  if (!sourcePack) throw new Error(`Missing Wave12 source pack for ${slug}`);

  const brandDisplayName = opts.airtableName || seed.name;
  const calaAvailable =
    seed.calaAvailability === "strong" || seed.calaAvailability === "partial";

  const tgs = [...(sourcePack.targetGuestSegmentsRecommendation?.recommended || [])];
  const tgsAssessment = assessWave12TgsRisk(tgs);

  const brandPositioning = scrubWave12StubChips(
    `${brandDisplayName} is a ${seed.model} within ${seed.parentCompany}. For owners, the affiliation case is ${seed.ownerLens}. It should be evaluated on its own guest promise and operating implications, not as a generic parent-brand substitute or an interchangeable peer among ${seed.distinguish.join(", ")}.`
  );
  const guestPsychographics = scrubWave12StubChips(
    `${brandDisplayName} appeals to travelers who respond to a ${seed.model} rather than a generic midscale or luxury-collection promise. Audience framing should stay specific to ${seed.shortName} demand drivers and avoid weak luxury/discerning filler that does not match the brand’s real positioning.`
  );

  const whyBullets = [
    `- Strongest where ${seed.propertyFit}.`,
    `- Owner diligence should center on ${seed.ownerLens}.`,
    `- Distinguish from ${seed.distinguish.join(", ")} before selecting this path.`,
    `- Weaker where the asset cannot support the ${seed.shortName} guest promise or platform obligations.`,
  ];

  const presentation = [
    row("Brand Positioning", "", brandPositioning, 10),
    row("Guest Psychographics Description", "", guestPsychographics, 11),
    row(
      "overview.typical_use_case",
      "",
      `Typical use cases for ${brandDisplayName} include ${seed.propertyFit}. Owners should test whether local demand, capital capacity, and operating capability can support the ${seed.shortName} promise before treating brand recognition alone as the investment thesis.`,
      20
    ),
    row(
      "overview.development_model",
      "",
      `${brandDisplayName} is relevant to conversion and new-build paths where ${seed.ownerLens}. Establish product, systems, staffing, and commercial workstreams from the asset-specific review rather than assuming a light reflag or a one-size prototype.`,
      21
    ),
    row(
      "overview.relative_positioning",
      "Relative Positioning",
      `${brandDisplayName} sits as a ${seed.model} and should be compared carefully with ${seed.distinguish.join(", ")}. Relative fit depends on guest promise, design intensity, operating complexity, and platform participation—not on parent-company familiarity alone.`,
      22
    ),
  ];

  seed.scenarios.forEach((s, i) => {
    const title = toProperCaseScenarioTitle(scrubWave12StubChips(s[0]));
    const baseBody = stripRepeatedScenarioDiligencePad(scrubWave12StubChips(s[1]));
    const body = ensureMinWords(baseBody, 48, scenarioOwnerValueClose(seed, brandDisplayName, i));
    if (isReferenceMetaScenarioTitle(title) || isReferenceMetaScenarioBody(body)) {
      throw new Error(
        `Wave12 generator refused reference-meta scenario for ${slug} overview.scenario.${i + 1}: "${title}"`
      );
    }
    presentation.push(row(`overview.scenario.${i + 1}`, title, body, 30 + i));
  });

  const scenarioBar = evaluateScenarioOwnerValueBar(
    presentation
      .filter((r) => /^overview\.scenario\.[123]$/.test(String(r.slotKey || "")))
      .map((r, i) => ({
        ...r,
        imageUrl: r.imageUrl || `https://cdn.example.com/wave12-generator/${slug}/scenario-${i + 1}.jpg`,
        active: true,
        visible: true,
      })),
    { brandSlug: slug }
  );
  const copyFailures = scenarioBar.failures.filter((f) => !String(f).startsWith("missing_image"));
  if (copyFailures.length) {
    throw new Error(
      `Wave12 generator scenario owner-value bar failed for ${slug}: ${copyFailures.join(", ")}`
    );
  }

  presentation.push(
    row(
      "overview.why_value",
      "Why Value Is Strongest",
      `Value for ${brandDisplayName} is strongest when the asset can deliver the brand’s specific guest promise and the owner can execute platform obligations with discipline.\n${whyBullets.join("\n")}`,
      33
    )
  );

  seed.proofs.forEach((p, i) => {
    presentation.push(
      row(
        `overview.proof.${i + 1}`,
        scrubWave12StubChips(p[0]),
        scrubWave12StubChips(ensureMinWords(p[1], 40, proofPad(seed, brandDisplayName))),
        40 + i
      )
    );
  });

  presentation.push(
    row(
      "overview.featured_application",
      `${seed.shortName} conversion or new-build path`,
      `A property aligned to ${seed.propertyFit} can use ${brandDisplayName} to pursue ${seed.ownerLens}. The owner case depends on product readiness, operating capability, and platform participation—not on brand awareness alone.`,
      44,
      {
        caseSummaryOverview: `Featured path for hotels evaluating ${brandDisplayName} as a ${seed.model}.`,
        caseSummaryBrandRelevance: `${brandDisplayName} remains distinct from ${seed.distinguish[0]} and peer alternatives.`,
        caseSummaryOwnerObjective: `Underwrite ${seed.ownerLens.split(",")[0]} against the specific asset.`,
        caseSummaryInterpretation: `Use as an owner-fit lens grounded in the Stage 3 source pack.`,
        caseSummaryTags: `${seed.shortName}, ${seed.calaAvailability === "strong" ? "CALA" : "International Reference"}, conversion, owner-fit`,
      }
    ),
    row(
      "overview.differentiators.identity",
      "Experience & Identity",
      bullets([
        `${seed.shortName} guest promise is tied to a clear ${seed.model} rather than a generic parent story`,
        `Owners should keep ${seed.shortName} distinct from ${seed.distinguish[0]} during underwriting`,
        `Property expression must support ${seed.shortName} positioning in rooms and public space`,
        `Local market demand must match the brand’s audience logic before affiliation`,
      ]),
      45
    ),
    row(
      "overview.differentiators.commercial",
      "Commercial & Distribution",
      bullets([
        `${seed.loyaltyProgram} participation supports the commercial case for ${seed.shortName}`,
        `${seed.parentCompany} distribution and commercial infrastructure remain conversion workstreams`,
        `Systems and loyalty readiness should be sequenced with product and staffing work`,
        `Compare commercial obligations across peer brands before selecting ${seed.shortName}`,
      ]),
      46
    ),
    row(
      "overview.bestAt.1",
      `${seed.shortName} guest promise`,
      `${brandDisplayName} is best at delivering a ${seed.model} when the asset and operator can sustain that promise consistently after opening or conversion.`,
      47
    ),
    row(
      "overview.bestAt.2",
      "Owner-relevant platform access",
      `Owners evaluate ${brandDisplayName} for ${seed.loyaltyProgram} reach and ${seed.parentCompany} commercial infrastructure while retaining a brand-specific guest story.`,
      48
    ),
    row(
      "overview.bestAt.3",
      "Clear peer separation",
      `${brandDisplayName} is most useful when owners explicitly distinguish it from ${seed.distinguish.join(", ")} instead of treating parent brands as interchangeable.`,
      49
    ),
    row(
      "overview.portfolio_context",
      "Portfolio Context",
      `Within ${seed.parentCompany}, ${brandDisplayName} functions as a ${seed.model}. Portfolio decisions should compare segment, design intensity, and operating complexity against ${seed.distinguish.join(", ")}.`,
      50
    ),
    row(
      "footprint.portfolio_context",
      "Portfolio Context",
      `${brandDisplayName} should be assessed as its own brand lane inside ${seed.parentCompany}, not as generic parent-platform proof. Use named property examples and dated momentum from the source pack to ground owner conversations.`,
      51
    ),
    row(
      "valueOwners.overview",
      "What Owners Are Buying",
      `Owners evaluating ${brandDisplayName} are buying a ${seed.model} backed by ${seed.parentCompany} distribution and ${seed.loyaltyProgram}. The practical case is ${seed.ownerLens}.`,
      51
    ),
    row(
      "valueOwners.watchouts",
      "",
      bullets([
        `Do not confuse ${seed.shortName} with ${seed.distinguish[0]}`,
        "Do not treat parent-company pages as brand-specific proof without labeling parent context",
        "Do not invent CALA presence when the source pack is International Reference only",
        "Do not underwrite lifestyle or conversion ambition without operating capacity",
      ]),
      52
    ),
    row(
      "valueOwners.lifecycle.1",
      "Evaluation",
      ensureMinWords(
        `Start with demand fit, product condition, and peer alternatives (${seed.distinguish.join(", ")}). Decide whether ${brandDisplayName}'s ${seed.model} is the right lane before detailed conversion capital is committed.`,
        40,
        lifecyclePad(seed, brandDisplayName)
      ),
      300
    ),
    row(
      "valueOwners.lifecycle.2",
      "Conversion Design",
      ensureMinWords(
        `Translate ${seed.shortName} positioning into rooms, public space, service, and technology workstreams that match the intended ${seed.model}. Sequence design, systems, and capital milestones with financing and operator decisions so conversion scope stays underwritable.`,
        40,
        lifecyclePad(seed, brandDisplayName)
      ),
      301
    ),
    row(
      "valueOwners.lifecycle.3",
      "Pre-Opening",
      ensureMinWords(
        `Coordinate ${seed.loyaltyProgram} readiness, training, staffing, and commercial launch with product completion for ${brandDisplayName}. Clarify owner, operator, and brand responsibilities before opening so the guest promise is deliverable from day one.`,
        40,
        lifecyclePad(seed, brandDisplayName)
      ),
      302
    ),
    row(
      "valueOwners.lifecycle.4",
      "Opening",
      ensureMinWords(
        `Launch with the ${seed.shortName} guest promise consistently expressed across service and channels while platform systems stabilize. Keep escalation paths clear for the first operating weeks and confirm quality readiness against the intended ${seed.model}.`,
        40,
        lifecyclePad(seed, brandDisplayName)
      ),
      303
    ),
    row(
      "valueOwners.lifecycle.5",
      "Ramp-Up",
      ensureMinWords(
        `Use early guest feedback and channel mix to refine delivery of the ${seed.model}. Watch whether staffing and public-space programming actually support the intended ${seed.shortName} promise, and adjust operator execution before assuming affiliation value is fully realized.`,
        40,
        lifecyclePad(seed, brandDisplayName)
      ),
      304
    ),
    row(
      "valueOwners.lifecycle.6",
      "Ongoing",
      ensureMinWords(
        `Maintain ${seed.shortName} product discipline while meeting applicable platform quality and commercial obligations. Revisit capital and operator alignment as the hotel stabilizes so ${brandDisplayName} remains credible versus peer alternatives such as ${seed.distinguish[0]}.`,
        40,
        lifecyclePad(seed, brandDisplayName)
      ),
      305
    ),
    row(
      "operations.model.primary_model",
      "",
      `${brandDisplayName} typically participates through the affiliation or operating path available for the market and asset within ${seed.parentCompany}. Confirm the applicable agreement structure for the specific opportunity.`,
      100
    ),
    row(
      "operations.model.management_option",
      "",
      `Third-party or owner-operated models can work when leadership can deliver the ${seed.shortName} guest promise and platform obligations. Operator fit matters as much as brand selection.`,
      101
    ),
    row(
      "operations.model.typical_ownership",
      "",
      `Owners seeking ${seed.ownerLens} and a clearer ${seed.model} than peer alternatives such as ${seed.distinguish[0]}.`,
      102
    ),
    row(
      "operations.model.brand_involvement",
      "",
      `${seed.parentCompany} development and brand teams may engage on conversion readiness, product presentation, systems, and quality expectations. Confirm current review stages for the asset.`,
      103
    ),
    row(
      "operations.model.systems_integration",
      "",
      `${brandDisplayName} hotels participate in relevant ${seed.loyaltyProgram} and ${seed.parentCompany} technology ecosystems. Validate PMS, CRS, training, and digital requirements before locking a conversion timeline.`,
      104
    ),
    row(
      "operations.model.pre_opening",
      "",
      `Expect product readiness, systems setup, team training, and commercial-launch work before opening or relaunch. Sequence these requirements with financing and construction.`,
      105
    ),
    row(
      "operations.model.staffing_intensity",
      "",
      `Staffing should match the ${seed.shortName} guest promise and public-space program. Underwrite front office, housekeeping, and any F&B or social-space coverage to the intended positioning.`,
      106
    ),
    row(
      "operations.model.fb_complexity",
      "",
      `F&B and public-space complexity varies by ${seed.shortName} site type. Review concept, hours, and operator capability against local demand rather than copying another brand’s outlet assumptions.`,
      107
    ),
    row(
      "operations.model.training",
      "",
      `Training should connect ${seed.loyaltyProgram} / platform expectations with the ${seed.shortName} service identity. Confirm modules, timing, and refresh expectations in the pre-opening plan.`,
      108
    ),
    row(
      "operations.model.reporting_discipline",
      "",
      `Platform participation creates reporting and operating rhythms owners should understand during diligence. Confirm available owner reporting and operator responsibilities for the agreement.`,
      109
    ),
    row(
      "operations.model.qa_rhythm",
      "",
      `Quality and brand review support the ${seed.shortName} guest promise at conversion and during operations. Confirm cadence, remediation process, and responsibility split before underwriting affiliation value.`,
      110
    ),
    row(
      "operations.model.technology",
      "",
      `Technology participation should be a conversion workstream. Validate required systems, loyalty integration, implementation support, and asset-specific constraints.`,
      111
    ),
    row(
      "operations.standards_philosophy",
      "",
      `${brandDisplayName} standards should protect a ${seed.model} while enabling practical owner execution.\nDesign and conversion detail: express ${seed.shortName} identity without unsupported parent-brand claims.\nPIP / lifecycle capital: establish scope from the asset review.\nSegment fit: compare against ${seed.distinguish.join(", ")}.`,
      112
    ),
    row(
      "operations.operator_compat.summary",
      "",
      ensureMinWords(
        `Operators need to deliver the ${seed.shortName} guest experience while maintaining ${seed.parentCompany} systems, loyalty, commercial, and quality obligations. Diligence operator capacity for the specific ${seed.model} rather than assuming parent-brand experience alone is enough.`,
        35,
        lifecyclePad(seed, brandDisplayName)
      ),
      113
    ),
    row(
      "operations.operator_compat.fit",
      "",
      `Best fit: operators experienced with ${seed.model} execution and platform discipline. Weaker fit: operators optimized only for unrelated prototypes or unable to sustain the ${seed.shortName} promise.`,
      114
    ),
    row(
      "operations.operator_compat.tags",
      "",
      bullets([seed.shortName, seed.model.split(" ")[0], "Platform discipline", "Conversion-ready"]),
      115
    ),
    row("operations.flexibility.design", "", seed.flex.design, 200),
    row("operations.flexibility.conversion", "", seed.flex.conversion, 201),
    row("operations.flexibility.localization", "", seed.flex.localization, 202),
    row("operations.flexibility.operational_rigidity", "", seed.flex.operational_rigidity, 203),
    row("operations.flexibility.pip", "", seed.flex.pip, 204),
    row("operations.flexibility.prototype", "", seed.flex.prototype, 205),
    row(
      "operations.compliance.qa_cadence",
      "",
      `Quality review is most important at conversion, opening, and remediation. Confirm current timing and escalation procedures for ${brandDisplayName}.`,
      210
    ),
    row(
      "operations.compliance.training_rigor",
      "",
      `Training should prepare teams for ${seed.loyaltyProgram} participation and the ${seed.shortName} guest experience. Define ownership of onboarding and refresh work.`,
      211
    ),
    row(
      "operations.compliance.reporting",
      "",
      `Clarify ${seed.parentCompany} reporting, loyalty, and distribution obligations alongside the operator’s reporting role for the specific agreement.`,
      212
    ),
    row(
      "operations.compliance.brand_interaction",
      "",
      `Brand interaction typically centers on development, conversion, systems, quality, and commercial readiness. Establish a practical decision calendar among owner, operator, and brand teams.`,
      213
    ),
    row(
      "economics.opening.step.1",
      "Application & Feasibility",
      ensureMinWords(
        `Present market context, ownership objectives, and product condition for ${brandDisplayName} review. Test whether the ${seed.model} is credible versus ${seed.distinguish[0]} before detailed conversion spend.`,
        35,
        lifecyclePad(seed, brandDisplayName)
      ),
      400
    ),
    row(
      "economics.opening.step.2",
      "Design & Standards",
      ensureMinWords(
        `Align design, rooms, public spaces, amenities, service model, and technology with ${seed.shortName} positioning and applicable brand requirements for the asset. Keep capital sequencing tied to what the ${seed.model} actually requires in this market.`,
        35,
        lifecyclePad(seed, brandDisplayName)
      ),
      401
    ),
    row(
      "economics.opening.step.3",
      "Pre-Opening Planning",
      ensureMinWords(
        `Build the plan around systems, ${seed.loyaltyProgram} readiness, training, staffing, sales, and operating procedures with clear owner/operator/brand responsibilities. Confirm timing against product completion so ${brandDisplayName} can open with a credible guest promise.`,
        35,
        lifecyclePad(seed, brandDisplayName)
      ),
      402
    ),
    row(
      "economics.opening.step.4",
      "Opening Support",
      ensureMinWords(
        `Coordinate launch communications, systems go-live, quality readiness, and service recovery with operator and brand contacts while keeping the ${seed.shortName} story prominent. Establish escalation paths for the first operating weeks.`,
        35,
        lifecyclePad(seed, brandDisplayName)
      ),
      403
    ),
    row(
      "economics.opening.step.5",
      "Stabilization",
      ensureMinWords(
        `Use the stabilized period to refine service and channel strategy against actual guest feedback for ${brandDisplayName}. Reassess capital and staffing through performance, not as a substitute for agreement-level diligence on the ${seed.model}.`,
        35,
        lifecyclePad(seed, brandDisplayName)
      ),
      404
    )
  );

  presentation.push(
    ...buildMomentumRows(seed, sourcePack, brandDisplayName, calaAvailable)
  );

  presentation.push(
    row(
      "footprint.portfolio_mix",
      "Portfolio mix",
      bullets([
        seed.model,
        seed.propertyFit.split(" and ")[0],
        `${seed.loyaltyProgram} hotels`,
        seed.calaAvailability === "strong" ? "CALA-relevant examples" : "International Reference examples",
      ]),
      460
    ),
    row(
      "footprint.geo_intro",
      "Geographic footprint",
      `${brandDisplayName} should be read through named markets and official property examples rather than inferred global ubiquity. ${calaRegionCopy(seed, brandDisplayName)} Owners should confirm local development focus and distribution relevance for the target market.`,
      470
    ),
    row(
      "footprint.region.am",
      "Americas",
      `Americas markets provide useful operating and guest-mix references for ${brandDisplayName}, especially where ${seed.loyaltyProgram} distribution is commercially meaningful. Confirm local presence and segment gaps for the specific deal.`,
      471
    ),
    row("footprint.region.cala", "CALA", calaRegionCopy(seed, brandDisplayName), 472),
    row(
      "footprint.region.eu",
      "Europe",
      `European references may illustrate ${seed.shortName} product logic internationally, but country-level systems and development rules should be tested for any specific European deal rather than copied from another region.`,
      473
    ),
    row(
      "footprint.region.mea",
      "MEA",
      `MEA relevance for ${brandDisplayName} is market-specific. Treat wider ${seed.parentCompany} presence as parent context, not proof that ${seed.shortName} is available or equivalent in every MEA market.`,
      474
    ),
    row(
      "footprint.region.apac",
      "APAC",
      `APAC can provide international brand-recognition context for travelers, but property-level feasibility for ${brandDisplayName} remains dependent on local development strategy and systems support.`,
      475
    ),
    row(
      "footprint.growth_themes",
      "",
      bullets([
        `${seed.shortName} conversion opportunities`,
        `${seed.shortName} new-build where demand fits`,
        seed.calaAvailability === "strong" ? "CALA growth where verified" : "International Reference growth diligence",
        `${seed.loyaltyProgram} distribution leverage`,
      ]),
      480
    ),
    row(
      "footprint.growth_editorial",
      "",
      `${brandDisplayName} is most compelling when ${seed.propertyFit} and the owner can execute ${seed.ownerLens}. Growth themes remain directional; diligence local comps, capital scope, operator readiness, and agreement terms independently.`,
      481
    ),
    row(
      "footprint.growth_fit",
      "",
      `Best growth fit: assets ready for a ${seed.model}. Weaker fit: hotels better aligned to ${seed.distinguish[0]} or unable to sustain the ${seed.shortName} guest promise.`,
      482
    )
  );

  const openings = buildOpeningsFromSource(seed, sourcePack, brandDisplayName);
  for (const o of openings) {
    presentation.push(
      row(OPENINGS_SLOT, o.title, o.body, o.sortOrder, {
        caseSummaryOverview: o.caseSummaryOverview,
        caseSummaryBrandRelevance: o.caseSummaryBrandRelevance,
        caseSummaryOwnerObjective: o.caseSummaryOwnerObjective,
        caseSummaryInterpretation: o.caseSummaryInterpretation,
        caseSummaryTags: o.caseSummaryTags,
      })
    );
  }

  presentation.push(
    row(
      "standards.intro",
      "",
      `${brandDisplayName} standards should support a ${seed.model} alongside ${seed.parentCompany} platform participation. Current acceptance, product, technology, training, and quality details must be confirmed for the specific asset and market.`,
      600
    ),
    row(
      "standards.requirement",
      "Design & guest promise review",
      `The property should present a credible ${seed.shortName} experience through rooms, public spaces, arrival, and overall design aligned to a ${seed.model}.`,
      601
    ),
    row(
      "standards.requirement",
      `${seed.loyaltyProgram} systems participation`,
      `Reservation, loyalty, distribution, and related platform systems may form part of affiliation. Confirm required technology and implementation sequencing.`,
      602
    ),
    row(
      "standards.requirement",
      "Public-space and amenity capital",
      `Public spaces and amenities should support the ${seed.shortName} guest promise and local demand. Establish required versus elective improvements before finalizing conversion budgets.`,
      603
    ),
    row(
      "standards.requirement",
      "Guest-room product standards",
      `Guest rooms should align with ${seed.shortName} positioning. Validate product gaps, accessibility work, and design flexibility during diligence.`,
      604
    ),
    row(
      "standards.requirement",
      "Training and service culture",
      `Team training should connect ${seed.loyaltyProgram} participation with the ${seed.shortName} service promise before opening or relaunch.`,
      605
    ),
    row(
      "standards.requirement",
      "Ongoing quality review",
      `Ongoing quality expectations preserve ${seed.shortName} positioning after conversion. Confirm review timing, remediation process, and responsibility split.`,
      606
    ),
    row(
      "standards.conversion",
      "",
      `Conversion suitability depends on delivering the ${seed.model} within platform systems, not merely seeking brand recognition. Compare ${seed.distinguish.join(", ")} carefully before committing capital.`,
      607
    ),
    row(
      "standards.questions",
      "Questions owners should ask",
      bullets([
        `What product and service characteristics distinguish ${brandDisplayName} from ${seed.distinguish[0]}?`,
        "Which improvements are required before conversion, and how are they reviewed?",
        `What ${seed.loyaltyProgram} and technology systems must the property implement?`,
        "How much design and operating flexibility remains after affiliation?",
        "What quality-review cadence and remediation responsibilities apply after opening?",
      ]),
      608
    )
  );

  seed.similar.forEach((s, i) => {
    presentation.push(
      row(
        `insight.similar.${i + 1}`,
        s[0],
        `${s[1]}. Compare this peer with ${brandDisplayName} on guest promise, conversion intensity, and platform obligations rather than assuming equivalent owner outcomes.`,
        700 + i
      )
    );
  });

  presentation.push(
    row("loyalty.hero_title", "", `${seed.loyaltyProgram}`, 800),
    row(
      "loyalty.ecosystem",
      "",
      `${brandDisplayName} participates in ${seed.loyaltyProgram} as part of the ${seed.parentCompany} commercial ecosystem. Owner value depends on local channel mix and systems readiness, not loyalty branding alone.`,
      801
    ),
    row(
      "loyalty.owner_lens",
      "",
      `Owners should diligence how ${seed.loyaltyProgram} member demand, redemption behavior, and system obligations affect ${seed.shortName} underwriting and ramp-up.`,
      802
    ),
    row(
      "loyalty.earn",
      "",
      `Members can earn ${seed.loyaltyProgram} value on eligible ${brandDisplayName} stays booked through preferred channels, subject to program rules current at the time of stay.`,
      803
    ),
    row(
      "loyalty.redeem",
      "",
      `${seed.loyaltyProgram} redemption potential can support demand for ${brandDisplayName} when inventory and rate strategy are managed deliberately with the operator.`,
      804
    ),
    row(
      "loyalty.elite",
      "Member recognition",
      `Elite or recognized ${seed.loyaltyProgram} members may expect consistent recognition at ${brandDisplayName}. Confirm property-level delivery standards during pre-opening.`,
      805
    ),
    row(
      "loyalty.elite",
      "Digital and app booking",
      `App and direct digital channels often concentrate ${seed.loyaltyProgram} demand. Align property content and offer readiness before launch.`,
      806
    ),
    row(
      "loyalty.elite",
      "Owner commercial implications",
      `Loyalty mix can shift channel costs and guest expectations. Underwrite ${seed.shortName} commercial plans with operator input rather than parent averages alone.`,
      807
    )
  );

  return {
    brandSlug: seed.slug,
    identity: {
      recordId: opts.recordId || sourcePack.recordId,
      name: brandDisplayName,
      parentCompany: seed.parentCompany,
      shortAlias: seed.shortName,
      reportSlug: seed.slug,
      loyaltyProgram: seed.loyaltyProgram,
    },
    sourcePackMeta: {
      officialBrandPage: sourcePack.officialBrandPage?.url || null,
      developmentPage: sourcePack.developmentPage?.url || null,
      calaAvailability: seed.calaAvailability,
      propertyExampleCount: (sourcePack.propertyExamples || []).length,
      openingsBuilt: openings.length,
    },
    brandLens: {
      brandModel: seed.model,
      ownerFit: seed.ownerLens,
      propertyFit: seed.propertyFit,
      distinguishFrom: seed.distinguish.join("; "),
      calaAvailability: seed.calaAvailability,
    },
    presentation,
    basicsFields: {
      "Brand Positioning": brandPositioning,
      "Guest Psychographics Description": guestPsychographics,
      ...(tgsAssessment.risk ? {} : { "Target Guest Segments": tgs }),
    },
    targetGuestSegments: tgs,
    tgsWriteEligible: !tgsAssessment.risk && tgs.length > 0,
    tgsRisk: tgsAssessment.risk,
    tgsAssessment,
  };
}

export function listWave12TabFactoryPacks() {
  return WAVE12_SLUGS.map((slug) => generateWave12TabFactoryPack(slug));
}

export { WAVE12_TAB_FACTORY_SEEDS };
