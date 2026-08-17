# D.4B Fit Map (retained fields only)

| Field | Explorer? | Fit? | Canonical OE? | Setup role |
| ----- | --------- | ---- | ------------- | ---------- |
| Active Countries | Yes | Yes (geo) | Market Presence + Assignments | Materialized summary |
| Brand Families Operated | Yes | Yes (brand) | Brand Relationships | Materialized summary |
| Service Models / propertyTypes / additionalExperience | Yes | Yes (segment/asset) | Assignments | Materialized summary |
| Operating Model / Management Availability | Yes | Yes | Master | Canonical on Master |
| companySize | Yes | Weak | Official/packs | Summary band |
| headquarters / website | Yes | No | Master/Profile | Identity |
| cap_profile_operational | Yes | Future | Writer v2 | Narrative summary |
| geo_*/% / cap_kpi_* | No | **Do not use** | — | Deprecated |

Fit remains **BLOCKED**.
