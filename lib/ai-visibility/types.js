/**
 * AI Visibility — normalized TypeScript-style JSDoc shapes (Foundation Phase 1).
 */

/**
 * @typedef {Object} VisibilityPrompt
 * @property {string} promptId
 * @property {string} version
 * @property {string} text
 * @property {string} [intentTerritory]
 * @property {string[]} [stakeholderRelevance]
 * @property {string} [geography]
 * @property {string} [country]
 * @property {string} [region]
 * @property {"global"|"region"|"subregion"|"country"|"market"} [geographyScope]
 * @property {string} [subregion]
 * @property {string} [market]
 * @property {string} [chainScale]
 * @property {string} [assetType]
 * @property {"brand"|"operator"|"both"} [entityScope]
 * @property {boolean} [active]
 */

/**
 * @typedef {Object} GeographyScopeFields
 * @property {string} [geographyModelVersion]
 * @property {"global"|"region"|"subregion"|"country"|"market"|"unknown"} geographyScope
 * @property {string|null} [regionId]
 * @property {string|null} [regionName]
 * @property {string|null} [subregionId]
 * @property {string|null} [subregionName]
 * @property {string|null} [countryCode]
 * @property {string|null} [countryName]
 * @property {string|null} [marketId]
 * @property {string|null} [marketName]
 */

/**
 * @typedef {Object} MonitoringRun
 * @property {string} runId
 * @property {string} promptId
 * @property {string} promptVersion
 * @property {string} provider
 * @property {string} model
 * @property {string} startedAt
 * @property {string|null} [completedAt]
 * @property {"pending"|"running"|"completed"|"failed"|"partial"} status
 * @property {number|null} [latencyMs]
 * @property {object|null} [usage]
 * @property {number|null} [estimatedCost]
 * @property {{ type?: string, message?: string }|null} [error]
 * @property {object} [providerMeta]
 */

/**
 * @typedef {Object} NormalizedCitation
 * @property {string} [url]
 * @property {string} [domain]
 * @property {string} [title]
 * @property {number} [citationPosition]
 * @property {boolean} providerSupplied
 * @property {boolean} [firstParty]
 * @property {string|null} [entityAssociation]
 * @property {string} [sourceType]
 */

/**
 * @typedef {Object} NormalizedResponse
 * @property {string} responseId
 * @property {string} runId
 * @property {string} promptId
 * @property {string} provider
 * @property {string} model
 * @property {string} text
 * @property {NormalizedCitation[]} citations
 * @property {object|null} [usage]
 * @property {object} [providerMeta]
 * @property {object|null} raw
 * @property {string} createdAt
 * @property {string} parserVersion
 * @property {"supported"|"unsupported"|"unavailable"|"partial"} [citationCapability]
 */

/**
 * @typedef {Object} Mention
 * @property {string} mentionId
 * @property {string} responseId
 * @property {"brand"|"operator"|"unresolved"} entityType
 * @property {string|null} canonicalEntityId
 * @property {string|null} canonicalEntityName
 * @property {string} rawMention
 * @property {number} mentionPosition
 * @property {number|null} [recommendationPosition]
 * @property {boolean} firstMention
 * @property {boolean} explicitRecommendation
 * @property {"first_recommendation"|"ranked_recommendation"|"explicit_recommendation"|"associated_option"|"comparator"|"passing_mention"|"negative_or_qualified"|"discussed"|"source_only"} [role]
 * @property {"recommendation"|"comparison"|"context"|"source"|"unknown"} [sectionRole]
 * @property {string} [contextSnippet]
 * @property {"positive"|"neutral"|"negative"|"unclear"|"associated"|"recommended"|"negative_or_qualified"} [sentimentOrContext]
 * @property {string} extractionMethod
 * @property {string} resolverVersion
 * @property {string} [classifierVersion]
 */

/**
 * @typedef {Object} Citation
 * @property {string} citationId
 * @property {string} responseId
 * @property {string|null} url
 * @property {string|null} domain
 * @property {string|null} title
 * @property {number} citationPosition
 * @property {boolean} providerSupplied
 * @property {boolean|null} firstParty
 * @property {string|null} entityAssociation
 * @property {string} sourceType
 */

/**
 * @typedef {Object} EvidenceRecord
 * @property {string} evidenceId
 * @property {string} promptId
 * @property {string} promptVersion
 * @property {string} [geographyScope]
 * @property {string|null} [regionName]
 * @property {string|null} [subregionName]
 * @property {string|null} [countryName]
 * @property {string|null} [marketName]
 * @property {string} runId
 * @property {string} responseId
 * @property {string} provider
 * @property {string} model
 * @property {string} timestamp
 * @property {string[]} mentionIds
 * @property {string[]} citationIds
 * @property {string} metricVersion
 * @property {object} [payload]
 */

/**
 * @typedef {Object} CanonicalEntity
 * @property {string} id
 * @property {string} name
 * @property {"brand"|"operator"} entityType
 * @property {string[]} [aliases]
 * @property {string[]} [firstPartyDomains]
 * @property {string|null} [parentCompany]
 * @property {boolean} [isParentCompanyLabel]
 */

export {};
