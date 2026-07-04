/**
 * Capital Provider Explorer — Airtable read + normalization (owner-safe).
 */
import Airtable from "airtable";
import { OWNER_DISCLAIMER } from "../../lib/capital-setup/capital-provider-public-seed-constants.js";
import {
  resolveExplorerHeroLabelsForUi,
} from "../../lib/capital-provider-explorer-hero-labels.js";
import { buildDefaultRequiredDocumentsFromCategories } from "../../lib/capital-setup/build-default-required-documents.js";
import {
  TABLES,
  PROVIDER_AT,
  CRITERIA_AT,
  DOCUMENT_AT,
  CONTACT_AT,
  FINANCING_AT,
  SOURCE_AT,
  OWNER_PROFILE_STATUSES,
  OWNER_VISIBILITY_LEVELS,
  ADMIN_EXTRA_VISIBILITY_LEVELS,
  OWNER_DOCUMENT_VISIBILITY,
  OWNER_FINANCING_VISIBILITY,
  LOAN_SIZE_RANGE_OPTIONS,
} from "./capital-provider-explorer-field-map.js";


export function getCapitalSetupBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) return null;
  return new Airtable({ apiKey }).base(baseId);
}

export function getField(record, fieldName, fallback = "") {
  if (!record || !record.fields) return fallback;
  const v = record.fields[fieldName];
  if (v === undefined || v === null) return fallback;
  return v;
}

export function normalizeMultiSelect(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map((x) => String(x).trim()).filter(Boolean);
  const s = String(value).trim();
  if (!s) return [];
  if (s.includes(",")) {
    return s.split(",").map((x) => x.trim()).filter(Boolean);
  }
  return [s];
}

export function normalizeCurrency(value) {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function normalizeDate(value) {
  if (!value) return null;
  const s = String(value).trim();
  return s || null;
}

export function normalizeCheckbox(value) {
  return value === true;
}

function normText(value) {
  if (value == null) return "";
  if (Array.isArray(value)) {
    const first = value[0];
    if (first == null) return "";
    if (typeof first === "string") return first.trim();
    if (typeof first === "object") {
      if (typeof first.url === "string") return first.url.trim();
      if (first.thumbnails && first.thumbnails.large && typeof first.thumbnails.large.url === "string") {
        return first.thumbnails.large.url.trim();
      }
      if (first.thumbnails && first.thumbnails.small && typeof first.thumbnails.small.url === "string") {
        return first.thumbnails.small.url.trim();
      }
    }
    return "";
  }
  if (typeof value === "object" && value !== null) {
    if (typeof value.url === "string") return value.url.trim();
    if (typeof value.label === "string") return value.label.trim();
    return "";
  }
  return String(value).trim();
}

function includesCi(haystack, needle) {
  const n = String(needle || "").trim().toLowerCase();
  if (!n) return true;
  if (Array.isArray(haystack)) {
    return haystack.some((h) => String(h).toLowerCase() === n || String(h).toLowerCase().includes(n));
  }
  return String(haystack || "").toLowerCase().includes(n);
}

function recordLinksProvider(record, providerId, linkField = "Capital Provider") {
  const links = getField(record, linkField, []);
  if (!Array.isArray(links)) return false;
  return links.includes(providerId);
}

export function isProviderVisible(fields, { isAdmin = false, isInternal = false } = {}) {
  const status = normText(getField({ fields }, PROVIDER_AT.profileStatus));
  const visibility = normText(getField({ fields }, PROVIDER_AT.visibilityLevel)) || "Public";

  if (!OWNER_PROFILE_STATUSES.has(status)) return false;

  if (OWNER_VISIBILITY_LEVELS.has(visibility)) return true;

  if ((isAdmin || isInternal) && ADMIN_EXTRA_VISIBILITY_LEVELS.has(visibility)) {
    return true;
  }

  return false;
}

export function normalizeProviderCard(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    name: normText(f[PROVIDER_AT.name]),
    institutionType: normText(f[PROVIDER_AT.institutionType]),
    profileStatus: normText(f[PROVIDER_AT.profileStatus]),
    visibilityLevel: normText(f[PROVIDER_AT.visibilityLevel]) || "Public",
    shortDescription: normText(f[PROVIDER_AT.shortDescription]),
    hotelLendingFocus: normText(f[PROVIDER_AT.hotelLendingFocus]),
    primaryRegion: normText(f[PROVIDER_AT.primaryRegion]),
    geographicCoverage: normalizeMultiSelect(f[PROVIDER_AT.geographicCoverage]),
    loanProductsOffered: normalizeMultiSelect(f[PROVIDER_AT.loanProductsOffered]),
    typicalDealTypes: normalizeMultiSelect(f[PROVIDER_AT.typicalDealTypes]),
    preferredAssetTypes: normalizeMultiSelect(f[PROVIDER_AT.preferredAssetTypes]),
    projectStageAppetite: normalizeMultiSelect(f[PROVIDER_AT.projectStageAppetite]),
    minimumLoanSize: normalizeCurrency(f[PROVIDER_AT.minimumLoanSize]),
    maximumLoanSize: normalizeCurrency(f[PROVIDER_AT.maximumLoanSize]),
    typicalLoanSizeSummary: normText(f[PROVIDER_AT.typicalLoanSizeSummary]),
    brandPreference: normText(f[PROVIDER_AT.brandPreference]),
    operatorPreference: normText(f[PROVIDER_AT.operatorPreference]),
    currentLendingAppetite: normText(f[PROVIDER_AT.currentLendingAppetite]),
    contactPathway: normText(f[PROVIDER_AT.contactPathway]),
    sourceType: normText(f[PROVIDER_AT.sourceType]),
    sourceConfidence: normText(f[PROVIDER_AT.sourceConfidence]),
    lastVerifiedDate: normalizeDate(f[PROVIDER_AT.lastVerifiedDate]),
    featuredProvider: normalizeCheckbox(f[PROVIDER_AT.featuredProvider]),
    sortOrder: Number(f[PROVIDER_AT.sortOrder]) || 0,
    website: normText(f[PROVIDER_AT.website]),
    logoUrl: normText(f[PROVIDER_AT.logoUrl]),
    disclaimer: OWNER_DISCLAIMER,
  };
}

export function normalizeProviderDetail(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    name: normText(f[PROVIDER_AT.name]),
    institutionType: normText(f[PROVIDER_AT.institutionType]),
    profileStatus: normText(f[PROVIDER_AT.profileStatus]),
    visibilityLevel: normText(f[PROVIDER_AT.visibilityLevel]) || "Public",
    shortDescription: normText(f[PROVIDER_AT.shortDescription]),
    institutionOverview: normText(f[PROVIDER_AT.institutionOverview]),
    hotelLendingFocus: normText(f[PROVIDER_AT.hotelLendingFocus]),
    headquarters: normText(f[PROVIDER_AT.headquarters]),
    website: normText(f[PROVIDER_AT.website]),
    logoUrl: normText(f[PROVIDER_AT.logoUrl]),
    primaryRegion: normText(f[PROVIDER_AT.primaryRegion]),
    geographicCoverage: normalizeMultiSelect(f[PROVIDER_AT.geographicCoverage]),
    preferredMarkets: normalizeMultiSelect(f[PROVIDER_AT.preferredMarkets]),
    typicalDealTypes: normalizeMultiSelect(f[PROVIDER_AT.typicalDealTypes]),
    loanProductsOffered: normalizeMultiSelect(f[PROVIDER_AT.loanProductsOffered]),
    preferredAssetTypes: normalizeMultiSelect(f[PROVIDER_AT.preferredAssetTypes]),
    projectStageAppetite: normalizeMultiSelect(f[PROVIDER_AT.projectStageAppetite]),
    minimumLoanSize: normalizeCurrency(f[PROVIDER_AT.minimumLoanSize]),
    maximumLoanSize: normalizeCurrency(f[PROVIDER_AT.maximumLoanSize]),
    typicalLoanSizeSummary: normText(f[PROVIDER_AT.typicalLoanSizeSummary]),
    brandPreference: normText(f[PROVIDER_AT.brandPreference]),
    operatorPreference: normText(f[PROVIDER_AT.operatorPreference]),
    sponsorPreference: normText(f[PROVIDER_AT.sponsorPreference]),
    currentLendingAppetite: normText(f[PROVIDER_AT.currentLendingAppetite]),
    contactPathway: normText(f[PROVIDER_AT.contactPathway]),
    requiredInformationSummary: normText(f[PROVIDER_AT.requiredInformationSummary]),
    processOverview: normText(f[PROVIDER_AT.processOverview]),
    ownerFacingNotes: normText(f[PROVIDER_AT.ownerFacingNotes]),
    sourceType: normText(f[PROVIDER_AT.sourceType]),
    sourceConfidence: normText(f[PROVIDER_AT.sourceConfidence]),
    lastVerifiedDate: normalizeDate(f[PROVIDER_AT.lastVerifiedDate]),
    featuredProvider: normalizeCheckbox(f[PROVIDER_AT.featuredProvider]),
    sortOrder: Number(f[PROVIDER_AT.sortOrder]) || 0,
    explorerHeroVerification: normText(f[PROVIDER_AT.explorerHeroVerification]),
    explorerHeroDataSource: normText(f[PROVIDER_AT.explorerHeroDataSource]),
    disclaimer: OWNER_DISCLAIMER,
  };
}

export function normalizeCriteria(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    criteriaName: normText(f[CRITERIA_AT.name]),
    loanProduct: normText(f[CRITERIA_AT.loanProduct]),
    dealTypeApplicability: normalizeMultiSelect(f[CRITERIA_AT.dealTypes]),
    minimumLoanSize: normalizeCurrency(f[CRITERIA_AT.minLoan]),
    maximumLoanSize: normalizeCurrency(f[CRITERIA_AT.maxLoan]),
    minimumTotalProjectCost: normalizeCurrency(f[CRITERIA_AT.minProjectCost]),
    maximumTotalProjectCost: normalizeCurrency(f[CRITERIA_AT.maxProjectCost]),
    termRange: normText(f[CRITERIA_AT.termRange]),
    recoursePreference: normText(f[CRITERIA_AT.recourse]),
    rateType: normalizeMultiSelect(f[CRITERIA_AT.rateType]),
    currency: normalizeMultiSelect(f[CRITERIA_AT.currency]),
    sponsorRequirements: normText(f[CRITERIA_AT.sponsorReq]),
    equityRequirements: normText(f[CRITERIA_AT.equityReq]),
    collateralRequirements: normText(f[CRITERIA_AT.collateralReq]),
    brandFlagRequirements: normText(f[CRITERIA_AT.brandReq]),
    operatorRequirements: normText(f[CRITERIA_AT.operatorReq]),
    marketRequirements: normText(f[CRITERIA_AT.marketReq]),
    appetiteStatus: normText(f[CRITERIA_AT.appetite]),
    ownerVisibleSummary: normText(f[CRITERIA_AT.ownerSummary]),
    sourceConfidence: normText(f[CRITERIA_AT.sourceConfidence]),
    lastVerifiedDate: normalizeDate(f[CRITERIA_AT.lastVerified]),
  };
}

export function normalizeRequiredDocument(record) {
  const f = record.fields || {};
  const visibility = normText(f[DOCUMENT_AT.visibility]);
  return {
    id: record.id,
    documentRequirementName: normText(f[DOCUMENT_AT.reqName]),
    documentName: normText(f[DOCUMENT_AT.docName]),
    documentCategory: normText(f[DOCUMENT_AT.category]),
    requiredLevel: normText(f[DOCUMENT_AT.requiredLevel]),
    appliesToDealTypes: normalizeMultiSelect(f[DOCUMENT_AT.dealTypes]),
    description: normText(f[DOCUMENT_AT.description]),
    ownerFacingInstructions: normText(f[DOCUMENT_AT.ownerInstructions]),
    visibilityLevel: visibility || "Owner Visible",
    sortOrder: Number(f[DOCUMENT_AT.sortOrder]) || 0,
  };
}

export function isOwnerVisibleDocument(record) {
  const vis = normText(getField(record, DOCUMENT_AT.visibility));
  return OWNER_DOCUMENT_VISIBILITY.has(vis);
}

export function normalizeContact(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    name: normText(f[CONTACT_AT.name]),
    title: normText(f[CONTACT_AT.title]),
    department: normText(f[CONTACT_AT.department]),
    email: normText(f[CONTACT_AT.email]),
    phone: normText(f[CONTACT_AT.phone]),
    linkedIn: normText(f[CONTACT_AT.linkedIn]),
    regionCoverage: normalizeMultiSelect(f[CONTACT_AT.regionCoverage]),
    contactRole: normText(f[CONTACT_AT.contactRole]),
    preferredMethod: normText(f[CONTACT_AT.preferredMethod]),
    contactStatus: normText(f[CONTACT_AT.contactStatus]),
    notes: normText(f[CONTACT_AT.contactNotes]),
    internalOnly: normalizeCheckbox(f[CONTACT_AT.internalOnly]),
  };
}

export function isOwnerVisibleContact(record) {
  return !normalizeCheckbox(getField(record, CONTACT_AT.internalOnly));
}

export function normalizeRepresentativeFinancing(record) {
  const f = record.fields || {};
  const loanUsd = normalizeCurrency(f[FINANCING_AT.loanAmountUsd]);
  const loanLabel = normText(f[FINANCING_AT.loanAmountLabel]);
  return {
    id: record.id,
    financingName: normText(f[FINANCING_AT.name]),
    projectName: normText(f[FINANCING_AT.projectName]),
    location: normText(f[FINANCING_AT.location]),
    dealType: normText(f[FINANCING_AT.dealType]),
    loanAmountLabel: loanLabel,
    loanAmountUsd: loanUsd,
    loanAmount:
      loanLabel ||
      (loanUsd != null
        ? new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: "USD",
            maximumFractionDigits: 0,
          }).format(loanUsd)
        : ""),
    transactionYear: normText(f[FINANCING_AT.transactionYear]),
    year: normText(f[FINANCING_AT.transactionYear]),
    summary: normText(f[FINANCING_AT.ownerSummary]),
    ownerSummary: normText(f[FINANCING_AT.ownerSummary]),
    sourceName: normText(f[FINANCING_AT.sourceName]),
    sourceUrl: normText(f[FINANCING_AT.sourceUrl]),
    imageUrl: normText(f[FINANCING_AT.imageUrl]),
    visibilityLevel: normText(f[FINANCING_AT.visibility]) || "Owner Visible",
    sortOrder: Number(f[FINANCING_AT.sortOrder]) || 0,
  };
}

export function isOwnerVisibleFinancing(record) {
  const vis = normText(getField(record, FINANCING_AT.visibility));
  return OWNER_FINANCING_VISIBILITY.has(vis);
}

export function normalizeSourceReference(record) {
  const f = record.fields || {};
  const relevant = f[SOURCE_AT.relevantFields];
  return {
    id: record.id,
    sourceName: normText(f[SOURCE_AT.name]),
    sourceType: normText(f[SOURCE_AT.sourceType]),
    sourceUrl: normText(f[SOURCE_AT.sourceUrl]),
    sourceDate: normalizeDate(f[SOURCE_AT.sourceDate]),
    retrievedReviewedDate: normalizeDate(f[SOURCE_AT.retrievedDate]),
    sourceSummary: normText(f[SOURCE_AT.summary]),
    relevantFieldsSupported: Array.isArray(relevant)
      ? relevant.map((x) => String(x).trim()).filter(Boolean)
      : normText(relevant),
    confidenceLevel: normText(f[SOURCE_AT.confidence]),
  };
}

export function sortProviders(providers) {
  return [...providers].sort((a, b) => {
    const featA = a.featuredProvider ? 1 : 0;
    const featB = b.featuredProvider ? 1 : 0;
    if (featB !== featA) return featB - featA;
    const sortA = Number(a.sortOrder) || 0;
    const sortB = Number(b.sortOrder) || 0;
    if (sortA !== sortB) return sortA - sortB;
    return String(a.name || "").localeCompare(String(b.name || ""), undefined, {
      sensitivity: "base",
    });
  });
}

export function applyListFilters(providers, query = {}) {
  const q = query || {};
  return providers.filter((p) => {
    if (q.institutionType && !includesCi(p.institutionType, q.institutionType)) return false;
    if (q.region && !includesCi(p.primaryRegion, q.region)) return false;
    if (q.geography && !includesCi(p.geographicCoverage, q.geography)) return false;
    if (q.loanProduct && !includesCi(p.loanProductsOffered, q.loanProduct)) return false;
    if (q.dealType && !includesCi(p.typicalDealTypes, q.dealType)) return false;
    if (q.assetType && !includesCi(p.preferredAssetTypes, q.assetType)) return false;
    if (q.projectStage && !includesCi(p.projectStageAppetite, q.projectStage)) return false;
    if (q.brandPreference && !includesCi(p.brandPreference, q.brandPreference)) return false;
    if (q.operatorPreference && !includesCi(p.operatorPreference, q.operatorPreference)) return false;
    if (q.contactPathway && !includesCi(p.contactPathway, q.contactPathway)) return false;
    if (q.sourceConfidence && !includesCi(p.sourceConfidence, q.sourceConfidence)) return false;

    if (q.search) {
      const blob = [
        p.name,
        p.shortDescription,
        p.hotelLendingFocus,
        p.institutionType,
        p.primaryRegion,
        ...(p.geographicCoverage || []),
        ...(p.loanProductsOffered || []),
      ].join(" ");
      if (!includesCi(blob, q.search)) return false;
    }

    return true;
  });
}

async function fetchAllProviders(base) {
  return base(TABLES.providers).select().all();
}

async function fetchChildRecords(base, tableName) {
  return base(tableName).select().all();
}

export async function loadProviderList({ isAdmin = false, isInternal = false, query = {} } = {}) {
  const base = getCapitalSetupBase();
  if (!base) {
    const err = new Error("Airtable not configured");
    err.code = "capital_provider_airtable_unavailable";
    throw err;
  }

  const records = await fetchAllProviders(base);
  const visible = records.filter((rec) =>
    isProviderVisible(rec.fields, { isAdmin, isInternal })
  );
  const cards = sortProviders(visible.map(normalizeProviderCard));
  const filtered = applyListFilters(cards, query);
  return filtered;
}

export async function loadProviderDetail(providerId, { isAdmin = false, isInternal = false } = {}) {
  const base = getCapitalSetupBase();
  if (!base) {
    const err = new Error("Airtable not configured");
    err.code = "capital_provider_airtable_unavailable";
    throw err;
  }

  let providerRec;
  try {
    providerRec = await base(TABLES.providers).find(providerId);
  } catch (e) {
    if (e.statusCode === 404) return null;
    throw e;
  }

  if (!providerRec || !isProviderVisible(providerRec.fields, { isAdmin, isInternal })) {
    return null;
  }

  const [criteriaRaw, documentsRaw, sourcesRaw, contactsRaw, financingsRaw] =
    await Promise.all([
      fetchChildRecords(base, TABLES.criteria),
      fetchChildRecords(base, TABLES.requiredDocuments),
      fetchChildRecords(base, TABLES.sourceReferences),
      fetchChildRecords(base, TABLES.contacts),
      fetchChildRecords(base, TABLES.representativeFinancings),
    ]);

  const criteria = criteriaRaw
    .filter((r) => recordLinksProvider(r, providerId, CRITERIA_AT.provider))
    .map(normalizeCriteria)
    .sort((a, b) => String(a.criteriaName).localeCompare(String(b.criteriaName)));

  const requiredDocuments = documentsRaw
    .filter((r) => recordLinksProvider(r, providerId, DOCUMENT_AT.provider))
    .filter(isOwnerVisibleDocument)
    .map(normalizeRequiredDocument)
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));

  const sourceReferences = sourcesRaw
    .filter((r) => recordLinksProvider(r, providerId, SOURCE_AT.provider))
    .map(normalizeSourceReference)
    .sort((a, b) => String(a.sourceName).localeCompare(String(b.sourceName)));

  const contacts = contactsRaw
    .filter((r) => recordLinksProvider(r, providerId, CONTACT_AT.provider))
    .filter(isOwnerVisibleContact)
    .map(normalizeContact)
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  const representativeFinancings = financingsRaw
    .filter((r) => recordLinksProvider(r, providerId, FINANCING_AT.provider))
    .filter(isOwnerVisibleFinancing)
    .map(normalizeRepresentativeFinancing)
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));

  return {
    provider: normalizeProviderDetail(providerRec),
    criteria,
    requiredDocuments,
    sourceReferences,
    contacts,
    representativeFinancings,
  };
}

/** Detect general readiness rows (not provider-specific requirements). */
export function isGeneralReadinessDocument(doc) {
  const reqName = String(doc.documentRequirementName || "");
  const desc = String(doc.description || "");
  return (
    reqName.startsWith("General Financing Readiness —") ||
    desc.includes("General hotel financing readiness item")
  );
}

/** Criteria loan sizes may reflect transaction examples — do not roll up to provider max/min. */
export function providerLoanSizeFields(provider) {
  return {
    loanSizeMinUsd: provider.minimumLoanSize ?? null,
    loanSizeMaxUsd: provider.maximumLoanSize ?? null,
    loanSizeLabel: provider.typicalLoanSizeSummary || "",
  };
}
/** Legacy list card shape for existing Explorer UI. */
export function toLegacyListItem(card) {
  return {
    id: card.id,
    slug: card.id,
    name: card.name,
    institutionType: card.institutionType,
    profileStatus: card.profileStatus,
    primaryRegion: card.primaryRegion || "",
    sourceConfidence: card.sourceConfidence || "",
    sourceType: card.sourceType || "",
    typicalDealTypes: card.typicalDealTypes || [],
    featuredProvider: card.featuredProvider === true,
    sortOrder: Number(card.sortOrder) || 0,
    website: card.website || "",
    logoUrl: card.logoUrl || "",
    shortDescription: card.shortDescription || "",
    hotelLendingFocus: card.hotelLendingFocus,
    geographicCoverage: card.geographicCoverage || [],
    loanProducts: card.loanProductsOffered || [],
    loanSizeLabel: card.typicalLoanSizeSummary || "",
    loanSizeMinUsd: card.minimumLoanSize,
    loanSizeMaxUsd: card.maximumLoanSize,
    preferredAssetTypes: card.preferredAssetTypes || [],
    contactPathway: card.contactPathway || "",
    visibility: card.visibilityLevel || "Public",
    projectStages: card.projectStageAppetite || [],
    brandPreferences: card.brandPreference ? [card.brandPreference] : [],
    operatorPreferences: card.operatorPreference ? [card.operatorPreference] : [],
  };
}

/** Legacy detail profile shape for existing Explorer UI. */
export function toLegacyDetailProfile(detail) {
  const p = detail.provider;
  const heroLabels = resolveExplorerHeroLabelsForUi({
    explorerHeroVerification: p.explorerHeroVerification,
    explorerHeroDataSource: p.explorerHeroDataSource,
  });
  const dealTypes = Array.isArray(p.typicalDealTypes)
    ? p.typicalDealTypes.join(", ")
    : p.typicalDealTypes || "";

  const providerDocs = (detail.requiredDocuments || []).map((d) => ({
    name: d.documentName || d.documentRequirementName,
    category: d.documentCategory || "",
    required: d.requiredLevel || "",
    notes: d.ownerFacingInstructions || d.description || "",
    description: d.description || "",
    appliesToDealTypes: d.appliesToDealTypes || [],
    isDefault: false,
    isGeneralReadiness: isGeneralReadinessDocument(d),
    isProviderSpecific: !isGeneralReadinessDocument(d),
  }));

  const usesDefaultRequiredDocuments = providerDocs.length === 0;
  const requiredDocuments = usesDefaultRequiredDocuments
    ? buildDefaultRequiredDocumentsFromCategories().map((d) => ({
        ...d,
        isGeneralReadiness: true,
        isProviderSpecific: false,
      }))
    : providerDocs;

  const financings = detail.representativeFinancings || [];
  const trackRecord = financings.map((f) => ({
    name: f.projectName || f.financingName,
    location: f.location || "",
    dealType: f.dealType || "",
    loanAmount: f.loanAmount || "",
    year: f.year || f.transactionYear || "",
    summary: f.summary || f.ownerSummary || "",
    imageUrl: f.imageUrl || "",
    sourceUrl: f.sourceUrl || "",
    sourceName: f.sourceName || "",
  }));

  const criteria = (detail.criteria || []).map((c) => ({
    id: c.id,
    criteriaName: c.criteriaName,
    loanProduct: c.loanProduct,
    dealTypes: c.dealTypeApplicability || [],
    minimumLoanSize: c.minimumLoanSize,
    maximumLoanSize: c.maximumLoanSize,
    minimumTotalProjectCost: c.minimumTotalProjectCost,
    maximumTotalProjectCost: c.maximumTotalProjectCost,
    termRange: c.termRange,
    recoursePreference: c.recoursePreference,
    rateType: c.rateType || [],
    currency: c.currency || [],
    sponsorRequirements: c.sponsorRequirements,
    equityRequirements: c.equityRequirements,
    collateralRequirements: c.collateralRequirements,
    brandRequirements: c.brandFlagRequirements,
    operatorRequirements: c.operatorRequirements,
    marketRequirements: c.marketRequirements,
    appetiteStatus: c.appetiteStatus,
    ownerVisibleSummary: c.ownerVisibleSummary,
    sourceConfidence: c.sourceConfidence,
    lastVerifiedDate: c.lastVerifiedDate,
    isTransactionExample:
      c.maximumLoanSize != null &&
      String(c.ownerVisibleSummary || "").toLowerCase().includes("transaction"),
  }));

  const loanSizes = providerLoanSizeFields(p);
  const loanSizeMinUsd = loanSizes.loanSizeMinUsd;
  const loanSizeMaxUsd = loanSizes.loanSizeMaxUsd;

  const contacts = (detail.contacts || []).map((c) => ({
    name: c.name,
    title: c.title,
    email: c.email,
    phone: c.phone,
    notes: [c.department, c.contactRole, c.notes].filter(Boolean).join(" · "),
    preferredMethod: c.preferredMethod,
    regionCoverage: c.regionCoverage || [],
  }));

  const sourceReferences = (detail.sourceReferences || []).map((s) => ({
    id: s.id,
    name: s.sourceName,
    sourceType: s.sourceType,
    url: s.sourceUrl,
    sourceDate: s.sourceDate,
    retrievedDate: s.retrievedReviewedDate,
    summary: s.sourceSummary,
    relevantFields: s.relevantFieldsSupported,
    confidence: s.confidenceLevel,
  }));

  return {
    id: p.id,
    slug: p.id,
    name: p.name,
    institutionType: p.institutionType,
    profileStatus: p.profileStatus,
    visibility: p.visibilityLevel || "Public",
    website: p.website || "",
    logoUrl: p.logoUrl || "",
    headquarters: p.headquarters || "",
    shortDescription: p.shortDescription || "",
    portfolioStats: null,
    currentLendingAppetiteOwner: p.currentLendingAppetite || "",
    primaryRegion: p.primaryRegion || "",
    keyDifferentiators: [],
    ownerValueProps: [],
    leadership: [],
    trackRecord,
    institutionOverview: p.institutionOverview || "",
    hotelLendingFocus: p.hotelLendingFocus || "",
    geographicCoverage: p.geographicCoverage || [],
    preferredMarkets: p.preferredMarkets || [],
    typicalDealTypes: dealTypes,
    loanProducts: p.loanProductsOffered || [],
    loanSizeMinUsd,
    loanSizeMaxUsd,
    loanSizeLabel: loanSizes.loanSizeLabel,
    assetTypeAppetite:
      (p.preferredAssetTypes || []).join(", ") || p.hotelLendingFocus || "",
    preferredAssetTypes: p.preferredAssetTypes || [],
    processOverview: p.processOverview || "",
    requiredInformation: p.requiredInformationSummary
      ? [{ label: "Required information", detail: p.requiredInformationSummary }]
      : [],
    requiredDocuments,
    usesDefaultRequiredDocuments,
    contactPathway: p.contactPathway || "",
    contactPathwayDetail: p.processOverview || "",
    contacts,
    projectStages: p.projectStageAppetite || [],
    brandPreferences: p.brandPreference ? [p.brandPreference] : [],
    operatorPreferences: p.operatorPreference ? [p.operatorPreference] : [],
    brandPreference: p.brandPreference || "",
    operatorPreference: p.operatorPreference || "",
    sponsorPreference: p.sponsorPreference || "",
    ownerFacingNotes: p.ownerFacingNotes || "",
    sourceType: p.sourceType || "",
    sourceConfidence: p.sourceConfidence || "",
    lastVerifiedDate: p.lastVerifiedDate || "",
    disclaimer: p.disclaimer,
    criteria,
    sourceReferences,
    explorerHeroVerification: heroLabels.explorerHeroVerification,
    explorerHeroDataSource: heroLabels.explorerHeroDataSource,
  };
}

export function collectFilterOptionsFromCards(cards) {
  const uniq = (arr) => [...new Set(arr.filter(Boolean))].sort();
  return {
    institutionTypes: uniq(cards.map((c) => c.institutionType)),
    geographies: uniq(cards.flatMap((c) => c.geographicCoverage || [])),
    loanProducts: uniq(cards.flatMap((c) => c.loanProductsOffered || [])),
    assetTypes: uniq(cards.flatMap((c) => c.preferredAssetTypes || [])),
    projectStages: uniq(cards.flatMap((c) => c.projectStageAppetite || [])),
    brandPreferences: uniq(cards.map((c) => c.brandPreference).filter(Boolean)),
    operatorPreferences: uniq(cards.map((c) => c.operatorPreference).filter(Boolean)),
    contactPathways: uniq(cards.map((c) => c.contactPathway).filter(Boolean)),
    sourceConfidence: uniq(cards.map((c) => c.sourceConfidence).filter(Boolean)),
    regions: uniq(cards.map((c) => c.primaryRegion).filter(Boolean)),
    loanSizeRanges: LOAN_SIZE_RANGE_OPTIONS,
    visibility: ["Public", "Limited"],
  };
}
