# CONTROL_HOTEL_COMPARE

Control: Bethesda Marriott (`adp_bethesda_marriott`) · Subject: Radisson Blu Toronto Downtown

| FIELD | WORKING HOTEL (Bethesda) | TORONTO (after activation) | MISSING ACTION |
|-------|--------------------------|----------------------------|----------------|
| founderApprovedForCustomer | false (share works via sealed registry; AVA may lag) | **true** | none |
| founderApprovedForCustomerAt | null | set | none |
| publicationAuthorized | false/legacy | **true** | none |
| customerReadyAuthorized | false/legacy | **true** | none |
| customerPublicationAuthorized | false/legacy | **true** | none |
| publicationAuthorizationStatus | NOT_AUTHORIZED | **AUTHORIZED** | none |
| customerReadinessStatus | NOT_CUSTOMER_READY | **CUSTOMER_READY** | none |
| customerPublished | false/legacy | **true** (status PUBLISHED) | none |
| share token | ACTIVE sealed contract token | **sht_a07147cf287fc81fa9afca26** ACTIVE | none |
| share registry | ACTIVE row | ACTIVE row | none |
| published-current / classic manifest | present | present (c8bd8b + 308ba1 portfolio) | none |
| external URL | production owner-ai-demand-share | production owner-ai-demand-share | none |
| PDF permissions | Admin current-report-pdf / AVA pdf (auth) | Same; share page has no PDF button (by design) | none |
| Open ADP client probe | available=true | **available=true** | none |

Spice Island: same pattern — sealed ACTIVE token enables Open/Copy even when AVA meta is not PUBLISHED. Toronto now has both share + explicit founder AVA publication authorization.
