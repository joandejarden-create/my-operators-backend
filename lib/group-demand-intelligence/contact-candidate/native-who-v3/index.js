export {
  resolveOfficialDomains,
  classifyDomain,
  DOMAIN_CLASS,
  discoverOfficialDomainsFromSerpHits,
} from "./official-domain.js";export { crawlOfficialStaffPages, scoreStaffUrl } from "./staff-directory-crawler.js";
export {
  fetchPdfText,
  extractContactsFromPdfText,
  extractFromPdfUrls,
  PDF_ROLE_CONTEXT,
} from "./pdf-contact-extraction.js";
export {
  inferRoleEvidenceTier,
  applyRejectionGates,
  confirmWhoFromEvidence,
  buildEvidenceMatrix,
  roleCoverageSatisfied,
  ROLE_EVIDENCE_TIER,
  REJECTION_GATE,
} from "./role-evidence-gates.js";
export { extractContactsFromHtml } from "./html-contact-extract.js";
export { discoverWhoV3, discoverWhoV4, buildV3Queries } from "./discover-who-v3.js";
export { discoverWhoV5 } from "./discover-who-v5.js";
export { discoverWhoV6, RESEARCH_GAP } from "./discover-who-v6.js";
export { discoverWhoV7 } from "./discover-who-v7.js";
export { resolveOfficialDomainsV7, DOMAIN_ROLE } from "./domain-resolver-v7.js";
export {
  expandEntityAliasesV7,
  buildDiscoveryQueriesV7,
  cleanEventTitle,
  eventCoreName,
} from "./entity-alias-expansion-v7.js";
export {
  classifySearchResultV7,
  rankUrlsForFetchV7,
  RESULT_TIER,
} from "./search-result-classifier-v7.js";
export {
  detectDynamicPageV7,
  extractStructuredPeopleV7,
} from "./structured-data-v7.js";
export {
  expandEntityAliases,
  buildRoleSearchQueries,
  buildDeepLinkQueries,
} from "./entity-alias-expansion.js";
export {
  isStrictPersonName,
  splitMultiPersonBlock,
  expandMultiPersonWithSharedRole,
  splitSequentialNameRoleString,
} from "./person-boundary.js";
