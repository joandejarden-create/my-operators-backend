# HE Tabs 1–5 Field Inventory V1

Audited: 2026-11-19T13:25:29.256Z

## Identity resolution

- URL `hotelExplorer=recUNycnMwOVFX0hc` → **Krystal Grand Puerto Vallarta** (NOT Bethesda Marriott).
- Bethesda Marriott canonical HPC id: `recLuxvwwxID7U2B8`.

## Tab model

Hotel Explorer overlay has 11 tabs; primary research scope is tabs 1–5: hotel, ownership, contacts, organization, relationships. Tabs 7–9 (area/demand/access) reuse HPC/Radar — out of research scope.

## Source-of-truth hierarchy

- 1. Verified HI Airtable graph (orgs/relationships/contacts/channels/evidence) on appCCUsuGsE1ifoLk
- 2. Hotel Property Census / Hotel Census property attributes
- 3. ADP verified attributes (amenities/meeting) — display overlay, do not mutate publication gates
- 4. Brand Explorer verified brand facts (brand entity only)
- 5. Golden-demo / showcase fixtures — regression only until migrated
- 6. New research — only for genuine gaps

## Krystal Grand Puerto Vallarta (golden-demo reference)

- HPC id: `recUNycnMwOVFX0hc`
- Brand-presence: **Krystal Grand Puerto Vallarta** · brand=Breathless Resorts & Spas · rooms=451
- Ownership in_cohort: **true** · data_status=DEEP_RESEARCH_PARTIAL
- Contact payload bytes: 17006
- HI graph relationships for hotel: **0** (OK)

| Tab | Applicable | Populated | Empty | Fixture-only | Substantive % | Non-fixture populated % |
|---|---:|---:|---:|---:|---:|---:|
| hotel | 25 | 19 | 6 | 19 | 76% | 0% |
| ownership | 20 | 7 | 13 | 7 | 35% | 0% |
| contacts | 7 | 7 | 0 | 7 | 100% | 0% |
| organization | 8 | 2 | 6 | 2 | 25% | 0% |
| relationships | 3 | 2 | 1 | 2 | 66.7% | 0% |

| Field | Tab | Current | Existing source | Airtable canonical | Research needed | Verification | Recommended action |
|---|---|---|---|---|---|---|---|
| name | hotel | POPULATED (Krystal Grand Puerto Vallarta) | brand-presence/HPC-or-fixture | HPC/Hotel Census:name|Property Name | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| brand | hotel | POPULATED (Breathless Resorts & Spas) | brand-presence/HPC-or-fixture | HPC:Current Brand|Affiliation | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| status | hotel | POPULATED (Open) | brand-presence/HPC-or-fixture | HPC:project_phase|status | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| website | hotel | POPULATED (https://en.krystalgrand-puertovallarta.com/) | brand-presence/HPC-or-fixture | HPC:Website|Official Property URL | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| telephone | hotel | POPULATED (52523221761176) | brand-presence/HPC-or-fixture | HPC:Phone|Telephone | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| address1 | hotel | POPULATED (136 Paseo De Las Garzas) | brand-presence/HPC-or-fixture | HPC:Address 1 | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| city | hotel | POPULATED (Puerto Vallarta) | brand-presence/HPC-or-fixture | HPC:city | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| country | hotel | POPULATED (Mexico) | brand-presence/HPC-or-fixture | HPC:country | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| hotelHeadline | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:Hotel Headline / enrichment | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| hotelDescription | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:Short Property Summary|description | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| parentCompany | hotel | POPULATED (Hyatt Hotels Corporation) | brand-presence/HPC-or-fixture | HPC:Parent Company | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| chainScale | hotel | POPULATED (Luxury Chain) | brand-presence/HPC-or-fixture | HPC:Chain Scale | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| censusPropertyType | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:Property Type | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| rooms | hotel | POPULATED (451) | brand-presence/HPC-or-fixture | HPC:Rooms / Keys|rooms | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| totalMeetingSpace | hotel | POPULATED (19353.51) | brand-presence/HPC-or-fixture | HPC:Total Meeting Space | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| operationType | hotel | POPULATED (Franchise) | brand-presence/HPC-or-fixture | HPC:Operation Type | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| managementCompany | hotel | POPULATED (Grupo Hotelero Santa Fe) | brand-presence/HPC-or-fixture | HPC:Operator / Management Company | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| market | hotel | POPULATED (Pacific Central) | brand-presence/HPC-or-fixture | HPC:Market | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| submarket | hotel | POPULATED (Puerto Vallarta / Riviera Nayarit) | brand-presence/HPC-or-fixture | HPC:Submarket | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| locationType | hotel | POPULATED (Resort) | brand-presence/HPC-or-fixture | HPC:Location | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| projectPhase | hotel | POPULATED (Open) | brand-presence/HPC-or-fixture | HPC:project_phase | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| projectedOpenDate | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:projected_open_date | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| starRating | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:Star Rating | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| dataConfidence | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:Data Confidence | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| amenitiesDisplay | hotel | POPULATED (object/array keys=16) | brand-presence/HPC-or-fixture | HPC amenities + ADP Attributes (verified) | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| executive_summary.text | ownership | POPULATED (Economic Owner: Grupo Hotelero Santa Fe, S.A.B. de C.V. (BMV: HOTEL) (HIGH). Legal PropCo / property vehicle: Inmobiliar) | golden-demo/fixtures | HI Ownership Research Runs + HI Graph Evidence | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| what_remains_uncertain | ownership | EMPTY | golden-demo/fixtures | HI Graph Evidence / research gaps | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| economic_owner | ownership | EMPTY | golden-demo/fixtures | HI Hotel–Org Relationships (Economic Owner) → HI Organizations | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| property_company | ownership | EMPTY | golden-demo/fixtures | HI Hotel–Org Relationships (Legal Owner) → HI Organizations | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| operator | ownership | POPULATED (object/array keys=5) | golden-demo/fixtures | HI Hotel–Org Relationships (Operator) → HI Organizations | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| control_status | ownership | POPULATED (present) | golden-demo/fixtures | derived from relationship Verification State | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| source_coverage | ownership | POPULATED (object/array keys=11) | golden-demo/fixtures | HI Graph Evidence count | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| what_we_know[] | ownership | EMPTY | golden-demo/fixtures | HI Graph Evidence / run summary | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| control_rows | ownership | EMPTY | golden-demo/fixtures | HI Hotel–Org Relationships by role | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| ubo_group | ownership | EMPTY | golden-demo/fixtures | HI Organizations (Parent / UBO) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| portfolio_preview | ownership | EMPTY | golden-demo/fixtures | HI Hotel–Org Relationships for org across HPC | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| brand_status | ownership | EMPTY | golden-demo/fixtures | HI Hotel–Org Relationships (Brand) + HPC Affiliation | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| decision_authority.people | ownership | POPULATED (object/array keys=10) | golden-demo/fixtures | HI Org Contacts | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| qualified_signatories | ownership | EMPTY | golden-demo/fixtures | HI Org Contacts (authority flags) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| corporate_contact_channels | ownership | EMPTY | golden-demo/fixtures | HI Contact Channels | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| asset_agreement_profile | ownership | EMPTY | golden-demo/fixtures | HPC + HI relationships | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| property_history | ownership | POPULATED (object/array keys=8) | golden-demo/fixtures | HI Graph Evidence (historical) | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| development_pursuit | ownership | EMPTY | golden-demo/fixtures | HI Ownership Research Runs notes | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| research_gaps | ownership | POPULATED (object/array keys=5) | golden-demo/fixtures | HI Ownership Research Runs | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| sources | ownership | EMPTY | golden-demo/fixtures | HI Graph Evidence | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| quality_summary | contacts | POPULATED (undefined) | showcase JSON / contact-intelligence API | derived from HI Org Contacts + Channels | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| ownershipSummary | contacts | POPULATED (undefined) | showcase JSON / contact-intelligence API | HI relationships projection | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| organization_contacts | contacts | POPULATED (undefined) | showcase JSON / contact-intelligence API | HI Organizations + Channels | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| people_groups | contacts | POPULATED (undefined) | showcase JSON / contact-intelligence API | HI Org Contacts | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| emails_phones | contacts | POPULATED (payload_bytes=17006) | showcase JSON / contact-intelligence API | HI Contact Channels | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| fallback_paths | contacts | POPULATED (undefined) | showcase JSON / contact-intelligence API | HI Contact Channels (public/fallback) | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| research_unresolved | contacts | POPULATED (undefined) | showcase JSON / contact-intelligence API | HI Ownership Research Runs | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| display_name | organization | POPULATED (Grupo Hotelero Santa Fe) | golden-demo/groups | HI Organizations:Trading/Legal Name | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| legal_name | organization | POPULATED (Grupo Hotelero Santa Fe, S.A.B. de C.V.) | golden-demo/groups | HI Organizations:Legal Name | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| ticker | organization | EMPTY | golden-demo/groups | HI Organizations:Registry / Identifier | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| portfolio_kpis | organization | EMPTY | golden-demo/groups | derived from HI Hotel–Org Relationships | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| owned_controlled_hotels | organization | EMPTY | golden-demo/groups | HI Hotel–Org Relationships (owner roles) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| operated_managed_hotels | organization | EMPTY | golden-demo/groups | HI Hotel–Org Relationships (Operator) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| key_people | organization | EMPTY | golden-demo/groups | HI Org Contacts | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| why_this_matters | organization | EMPTY | golden-demo/groups | HI Organizations:Notes / Evidence | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| structure_chart | relationships | POPULATED (object/array keys=12) | ownership payload edges/fixtures | HI Hotel–Org Relationships graph | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| relationship_edges | relationships | POPULATED (object/array keys=12) | ownership payload edges/fixtures | HI Hotel–Org Relationships | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| org_network | relationships | EMPTY | ownership payload edges/fixtures | HI Organizations + relationships | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |

## Bethesda Marriott (repeatability / pilot)

- HPC id: `recLuxvwwxID7U2B8`
- Brand-presence: **Bethesda Marriott** · brand=Marriott Hotels · rooms=407
- Ownership in_cohort: **false** · data_status=AIRTABLE_GRAPH_PARTIAL
- Contact payload bytes: 1448
- HI graph relationships for hotel: **2** (OK)

| Tab | Applicable | Populated | Empty | Fixture-only | Substantive % | Non-fixture populated % |
|---|---:|---:|---:|---:|---:|---:|
| hotel | 25 | 21 | 4 | 21 | 84% | 0% |
| ownership | 20 | 0 | 20 | 0 | 0% | 0% |
| contacts | 7 | 0 | 7 | 0 | 0% | 0% |
| organization | 8 | 0 | 8 | 0 | 0% | 0% |
| relationships | 3 | 0 | 3 | 0 | 0% | 0% |

| Field | Tab | Current | Existing source | Airtable canonical | Research needed | Verification | Recommended action |
|---|---|---|---|---|---|---|---|
| name | hotel | POPULATED (Bethesda Marriott) | brand-presence/HPC-or-fixture | HPC/Hotel Census:name|Property Name | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| brand | hotel | POPULATED (Marriott Hotels) | brand-presence/HPC-or-fixture | HPC:Current Brand|Affiliation | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| status | hotel | POPULATED (Open) | brand-presence/HPC-or-fixture | HPC:project_phase|status | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| website | hotel | POPULATED (https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/overview/) | brand-presence/HPC-or-fixture | HPC:Website|Official Property URL | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| telephone | hotel | POPULATED (+1 301-897-9400) | brand-presence/HPC-or-fixture | HPC:Phone|Telephone | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| address1 | hotel | POPULATED (5151 Pooks Hill Road, Bethesda, MD 20814, United States) | brand-presence/HPC-or-fixture | HPC:Address 1 | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| city | hotel | POPULATED (Bethesda) | brand-presence/HPC-or-fixture | HPC:city | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| country | hotel | POPULATED (United States) | brand-presence/HPC-or-fixture | HPC:country | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| hotelHeadline | hotel | POPULATED (Marriott Hotels property — serving Bethesda / Montgomery County — signals: bethesda md location; pooks hill; montgomery ) | brand-presence/HPC-or-fixture | HPC:Hotel Headline / enrichment | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| hotelDescription | hotel | POPULATED (Bethesda Marriott (Marriott Hotels) in Bethesda — 407 keys) | brand-presence/HPC-or-fixture | HPC:Short Property Summary|description | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| parentCompany | hotel | POPULATED (Marriott International) | brand-presence/HPC-or-fixture | HPC:Parent Company | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| chainScale | hotel | POPULATED (Upper Upscale) | brand-presence/HPC-or-fixture | HPC:Chain Scale | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| censusPropertyType | hotel | POPULATED (Hotel) | brand-presence/HPC-or-fixture | HPC:Property Type | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| rooms | hotel | POPULATED (407) | brand-presence/HPC-or-fixture | HPC:Rooms / Keys|rooms | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| totalMeetingSpace | hotel | POPULATED (18719) | brand-presence/HPC-or-fixture | HPC:Total Meeting Space | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| operationType | hotel | POPULATED (Management Agreement) | brand-presence/HPC-or-fixture | HPC:Operation Type | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| managementCompany | hotel | POPULATED (Marriott International (Managed)) | brand-presence/HPC-or-fixture | HPC:Operator / Management Company | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| market | hotel | POPULATED (Bethesda / Montgomery County) | brand-presence/HPC-or-fixture | HPC:Market | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| submarket | hotel | POPULATED (Pooks Hill / Rockville Pike) | brand-presence/HPC-or-fixture | HPC:Submarket | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| locationType | hotel | POPULATED (Suburban) | brand-presence/HPC-or-fixture | HPC:Location | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| projectPhase | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:project_phase | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| projectedOpenDate | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:projected_open_date | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| starRating | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:Star Rating | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| dataConfidence | hotel | EMPTY | brand-presence/HPC-or-fixture | HPC:Data Confidence | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| amenitiesDisplay | hotel | POPULATED (object/array keys=29) | brand-presence/HPC-or-fixture | HPC amenities + ADP Attributes (verified) | VERIFY_THEN_MIGRATE | FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL | MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC |
| executive_summary.text | ownership | EMPTY | stub/out_of_cohort | HI Ownership Research Runs + HI Graph Evidence | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| what_remains_uncertain | ownership | EMPTY | stub/out_of_cohort | HI Graph Evidence / research gaps | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| economic_owner | ownership | EMPTY | stub/out_of_cohort | HI Hotel–Org Relationships (Economic Owner) → HI Organizations | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| property_company | ownership | EMPTY | stub/out_of_cohort | HI Hotel–Org Relationships (Legal Owner) → HI Organizations | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| operator | ownership | EMPTY | stub/out_of_cohort | HI Hotel–Org Relationships (Operator) → HI Organizations | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| control_status | ownership | EMPTY | stub/out_of_cohort | derived from relationship Verification State | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| source_coverage | ownership | EMPTY | stub/out_of_cohort | HI Graph Evidence count | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| what_we_know[] | ownership | EMPTY | stub/out_of_cohort | HI Graph Evidence / run summary | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| control_rows | ownership | EMPTY | stub/out_of_cohort | HI Hotel–Org Relationships by role | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| ubo_group | ownership | EMPTY | stub/out_of_cohort | HI Organizations (Parent / UBO) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| portfolio_preview | ownership | EMPTY | stub/out_of_cohort | HI Hotel–Org Relationships for org across HPC | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| brand_status | ownership | EMPTY | stub/out_of_cohort | HI Hotel–Org Relationships (Brand) + HPC Affiliation | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| decision_authority.people | ownership | EMPTY | stub/out_of_cohort | HI Org Contacts | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| qualified_signatories | ownership | EMPTY | stub/out_of_cohort | HI Org Contacts (authority flags) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| corporate_contact_channels | ownership | EMPTY | stub/out_of_cohort | HI Contact Channels | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| asset_agreement_profile | ownership | EMPTY | stub/out_of_cohort | HPC + HI relationships | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| property_history | ownership | EMPTY | stub/out_of_cohort | HI Graph Evidence (historical) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| development_pursuit | ownership | EMPTY | stub/out_of_cohort | HI Ownership Research Runs notes | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| research_gaps | ownership | EMPTY | stub/out_of_cohort | HI Ownership Research Runs | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| sources | ownership | EMPTY | stub/out_of_cohort | HI Graph Evidence | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| quality_summary | contacts | EMPTY | contact-intelligence API (sparse) | derived from HI Org Contacts + Channels | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| ownershipSummary | contacts | EMPTY | contact-intelligence API (sparse) | HI relationships projection | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| organization_contacts | contacts | EMPTY | contact-intelligence API (sparse) | HI Organizations + Channels | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| people_groups | contacts | EMPTY | contact-intelligence API (sparse) | HI Org Contacts | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| emails_phones | contacts | EMPTY | contact-intelligence API (sparse) | HI Contact Channels | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| fallback_paths | contacts | EMPTY | contact-intelligence API (sparse) | HI Contact Channels (public/fallback) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| research_unresolved | contacts | EMPTY | contact-intelligence API (sparse) | HI Ownership Research Runs | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| display_name | organization | EMPTY | unavailable | HI Organizations:Trading/Legal Name | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| legal_name | organization | EMPTY | unavailable | HI Organizations:Legal Name | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| ticker | organization | EMPTY | unavailable | HI Organizations:Registry / Identifier | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| portfolio_kpis | organization | EMPTY | unavailable | derived from HI Hotel–Org Relationships | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| owned_controlled_hotels | organization | EMPTY | unavailable | HI Hotel–Org Relationships (owner roles) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| operated_managed_hotels | organization | EMPTY | unavailable | HI Hotel–Org Relationships (Operator) | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| key_people | organization | EMPTY | unavailable | HI Org Contacts | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| why_this_matters | organization | EMPTY | unavailable | HI Organizations:Notes / Evidence | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| structure_chart | relationships | EMPTY | stub | HI Hotel–Org Relationships graph | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| relationship_edges | relationships | EMPTY | stub | HI Hotel–Org Relationships | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |
| org_network | relationships | EMPTY | stub | HI Organizations + relationships | YES | EMPTY_OR_STUB | RESEARCH_THEN_PERSIST_TO_CANONICAL |

## Baseline verdict

- KGPV tabs 2–5 are **fixture-rich** but **not Airtable-canonical** (HI graph relationship count expected 0 until migration).
- Bethesda tabs 2–5 are **out of golden cohort** (ownership stub); tab 1 has HPC/API gaps (rooms/website/phone often empty).
- Do not treat UI population as durable persistence.
- Next: reuse/migrate verified KGPV fixture facts into HI graph; research Bethesda genuine gaps within authorized budget.
