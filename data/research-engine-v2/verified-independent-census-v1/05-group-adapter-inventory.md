# Group Adapter Inventory

```json
[
  {
    "group": "IHG",
    "adapter": "ihg_destination_directory",
    "discovery": "scalable",
    "status": "production_ready_for_reconstruction",
    "extract": "reports/ihg-cala-directory-extract.json",
    "supports": [
      "name",
      "brand",
      "city",
      "country",
      "url",
      "propertyId",
      "parent",
      "status_via_hoteldetail"
    ],
    "notes": "Pilot + VIC wave 1 benchmark"
  },
  {
    "group": "Marriott",
    "adapter": "marriott_soft_brand_directory",
    "discovery": "partial",
    "status": "soft_brands_ready",
    "extract": "fixtures / marriott soft brand extracts",
    "rowCountHint": 18,
    "supports": [
      "name",
      "brand",
      "url",
      "marsha_where_present"
    ],
    "notes": "Expand to full Marriott CALA directory extract for wave scale"
  },
  {
    "group": "Hilton",
    "adapter": "hilton_graphql_ctyhocn",
    "discovery": "code_backed",
    "status": "status_ready_identity_gap",
    "supports": [
      "status",
      "openDate",
      "name_via_graphql"
    ],
    "notes": "Needs ctyhocn / directory extract for discovery-first waves"
  },
  {
    "group": "Choice",
    "adapter": "choice_sitemap_cala",
    "discovery": "scalable_cala",
    "status": "ready_with_extract",
    "extract": "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json",
    "supports": [
      "url",
      "inferred_name",
      "brand_from_url"
    ],
    "notes": "403 pages must remain Blocked — never reflag from block"
  },
  {
    "group": "Hyatt",
    "adapter": "hyatt_directory_match",
    "discovery": "planned",
    "status": "enrichment_scripts_exist",
    "notes": "Reuse plan-hyatt-census-enrichment patterns for discovery extract"
  },
  {
    "group": "Accor",
    "adapter": "accor_directory",
    "discovery": "planned",
    "status": "partial_lib_exists",
    "notes": "accor-directory-name-normalize.js present"
  },
  {
    "group": "Wyndham",
    "adapter": "wyndham_directory",
    "discovery": "planned",
    "status": "not_wired_to_re_v2"
  },
  {
    "group": "Minor Hotels",
    "adapter": "minor_avani_etc",
    "discovery": "planned",
    "status": "brand_pages_bot_blocked_often",
    "notes": "Census Open hotels can corroborate existence post-rules; homepage often 403"
  },
  {
    "group": "Radisson / Choice regional",
    "adapter": "choice_radisson_individuals",
    "discovery": "partial",
    "status": "individuals_gap_engine_exists"
  }
]
```
