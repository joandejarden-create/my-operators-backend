# Operator Setup Form Section Structure Recommendation

- Generated: 2026-06-02T14:40:53.565Z
- Approach: keep current flow where possible; group by operator task intent and downstream ownership surfaces.

## 1. Company Profile & Positioning
- Purpose: Core identity and narrative
- Editable fields: companyName, companyDescription, website, headquarters, yearEstablished, companyTagline, companyHistory, differentiators, managementPhilosophy, missionStatement
- Read-only fields: yearsInBusiness, numberOfBrands
- Hidden fields: operator_id, created_at, updated_at
- Feeds Explorer: High
- Feeds OAS/Score: Medium
- Feeds Capability Snapshot: Low/Medium
- Business-review fields: parentCompany

## 2. Operating Platform & Services
- Purpose: Service model, chain scale, and capabilities
- Editable fields: primaryServiceModel, serviceModelsSupported, chainScalesSupported, offeredServices, revenueManagementCapability, salesPlatform, fBCapabilityLevel
- Read-only fields: None
- Hidden fields: None
- Feeds Explorer: High
- Feeds OAS/Score: High
- Feeds Capability Snapshot: High
- Business-review fields: managementStructuresSupported select taxonomy

## 3. Markets, Segments & Asset Focus
- Purpose: Geography and asset scope
- Editable fields: regions, specificMarkets, activeCountries, activeMarkets, propertyTypes, additionalExperience
- Read-only fields: None
- Hidden fields: None
- Feeds Explorer: High
- Feeds OAS/Score: High
- Feeds Capability Snapshot: Medium
- Business-review fields: marketPresenceType

## 4. Brand Relationships & Portfolio Experience
- Purpose: Brand mix and project experience
- Editable fields: brands, additionalBrands, brand_conversion_project_count, conversionReflagExperience, newBuildOpeningExperience
- Read-only fields: numberOfBrands
- Hidden fields: brandsPortfolioDetail (legacy JSON)
- Feeds Explorer: High
- Feeds OAS/Score: Medium
- Feeds Capability Snapshot: Medium
- Business-review fields: None

## 5. Governance, Compliance & Diligence
- Purpose: Governance signals and QA evidence
- Editable fields: governanceCadence, ownerReportingLevel, dataConfidenceLevel, emergencyResponse, businessContinuity, support24x7, crisisExperience, insuranceCoverage
- Read-only fields: None
- Hidden fields: submission_status
- Feeds Explorer: Medium
- Feeds OAS/Score: High
- Feeds Capability Snapshot: Medium
- Business-review fields: minimumKeyCount

## 6. Leadership Team
- Purpose: Leadership roster and bios
- Editable fields: exec_*_name, exec_*_role, exec_*_summary, exec_*_bio, exec_*_headshot
- Read-only fields: leadership profile derived strings
- Hidden fields: legacy leadership aliases
- Feeds Explorer: High
- Feeds OAS/Score: Low/Medium
- Feeds Capability Snapshot: Low
- Business-review fields: None

## 7. Case Studies / Track Record
- Purpose: Proof points and outcomes
- Editable fields: caseStudiesDetail[]
- Read-only fields: None
- Hidden fields: legacy case-study mirror fields
- Feeds Explorer: High
- Feeds OAS/Score: Medium
- Feeds Capability Snapshot: Low
- Business-review fields: None

## 8. Explorer Profile / Marketing Materials
- Purpose: Owner-facing narrative cards and materials
- Editable fields: overview_*, cap_*, materials_*
- Read-only fields: diagnostic provenance
- Hidden fields: explorerProfileJson mirror
- Feeds Explorer: High
- Feeds OAS/Score: No direct score change
- Feeds Capability Snapshot: High
- Business-review fields: mirror masking follow-up

## 9. Internal/System Review
- Purpose: Operational metadata and compatibility controls
- Editable fields: None
- Read-only fields: submission_status, sourceType, lastUpdatedDate
- Hidden fields: operator_id, linked-record fields, timestamps
- Feeds Explorer: Low
- Feeds OAS/Score: Low
- Feeds Capability Snapshot: Low
- Business-review fields: any field without canonical mapping
