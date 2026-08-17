# Operator Setup Form Completeness Summary

1. **Are all important Operator Setup fields currently represented on the form?**
No. Core profile fields are represented, but a meaningful set of canonically mapped fields are still not surfaced in My Operator.

2. **Which important fields are missing from the form?**
Examples (from recommendation set): conversionReflagExperience, managementStructuresSupported, minimumKeyCount, newBuildOpeningExperience, preOpeningSupportCapability, similarProjectCaseStudies, fbCapabilityLevel, governanceCadence, infra_technology_maturity_level, offeredServices, ownerReportingLevel, revenueManagementCapability.

3. **Which missing fields are safe to add now?**
Lowest-risk F1 candidates: minimumKeyCount, newBuildOpeningExperience, similarProjectCaseStudies, governanceCadence, infra_technology_maturity_level, offeredServices, salesPlatform, brandsPortfolioDetail, marketPresenceType, brandFamiliesOperated, brand_soft_independent_narrative, readyForInvestorPublication.

4. **Which fields should stay hidden/system-only?**
Operator, Operator, Operator, Operator, Operator, Operator, created_at, Operator Setup - Case Studies, Operator Setup - Commercial Fit & Terms, Operator Setup - Diligence QA, Operator Setup - Explorer Materials, Operator Setup - Governance, Delivery & Diligence, Operator Setup - Leadership Team Members, Operator Setup - Platform & Markets.

5. **Which fields need business review?**
branded_independent, display_order, hotel_type, image_url, outcome, owner_relevance, property_name, region, services, situation, additionalNotes, annualRevenueManaged.

6. **Which fields should not be deleted yet?**
All Operator Setup fields should be retained for now; especially canonical, alias, linked-record, and downstream-scoring-related fields. No safe hard-delete set is confirmed in this audit pass.

7. **What is the recommended first implementation batch?**
Batch F1 (safe missing fields with existing writer/read mappings), followed by targeted validation of save/reload/detail/explorer parity.

8. **What is the risk of adding these fields before demo?**
Moderate if select options are unstable; low-to-medium for text/number fields that already have canonical mappings.

9. **What should wait until after demo?**
F2 mapping repairs without clear contracts, F4 business-definition fields, and F5 legacy/deferred compatibility surfaces.
