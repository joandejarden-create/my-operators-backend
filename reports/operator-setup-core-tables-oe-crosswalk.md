# D.4A OE Crosswalk

| Setup Field | Canonical OE Source | Available? | Safe Derivation? | Writer Needed? |
| ----------- | ------------------- | ---------- | ---------------- | -------------- |
| website | Master.Operator Website | Yes (most) | Yes DIRECT | No |
| company_name | Master.company_name | Yes | Yes DIRECT | No |
| Operating Model (Master) | Master | Yes | N/A (lives on Master) | No |
| Management Availability (Master) | Master | Yes | N/A (lives on Master) | No |
| Active Countries | Market Presence current ∪ Assignments Current | Partial | Yes DERIVED | No |
| Brand Families Operated | Brand Relationships + Assignments.Brand | Partial | Yes DERIVED (taxonomy map) | No |
| propertyTypes / Service Models / additionalExperience | Assignments Hotel Type / AI / Development Context | Partial | Yes DERIVED | No |
| chainScalesSupported | Assignments Chain Scale | Sparse | Only when field present | No |
| Active Markets / Cities | Assignments cities | Partial | Not this phase (taxonomy) | No |
| geo_*/luxury*/% / cap_kpi_* | Assignments sample | Insufficient | **No** | No |
| cap_profile_operational | Claims / official / Writer v2 | Sparse | No auto | Yes — HIGH VALUE |
| cap_profile_commercial | Claims / official | Sparse | No auto | Contract refine |
| cap_profile_transition | — | Sparse | No | MOVE TO CLAIMS |
| companyDescription / differentiators | Claims / packs | Partial | No auto | Yes — HIGH VALUE |
| overview_* / op_* JSON / brand_* JSON | Presentation scaffolds | Mixed | No (presentation) | No |

## Per-operator derived snapshot

| Operator | Countries | Brand families | Named current asg | Profile row | Platform row |
| -------- | --------- | -------------- | ----------------: | ----------- | ------------ |
| Barceló Hotel Group | Dominican Republic; Mexico | Other | 2 | false | false |
| Meliá Hotels International | Dominican Republic; Mexico; Spain | Other | 3 | false | false |
| Playa Hotels & Resorts | Dominican Republic; Jamaica; Mexico | Hilton; Hyatt; Marriott; Wyndham | 6 | true | true |
| Hilton (Managed) | Mexico; Panama | Hilton; Other | 5 | true | true |
| Mandarin Oriental Hotel Group | United States | Other | 2 | false | false |
| Remington Hospitality | Dominican Republic; United States | Hilton; Marriott | 5 | true | true |
| IHG Hotels & Resorts (Managed) | Colombia; Mexico; Panama | IHG | 5 | true | true |
| Minor Hotels (Managed) | Mexico | Independent; Other | 3 | true | true |
| Shangri-La Group | — | Other | 2 | false | false |
| AADESA | Argentina | Other; Wyndham | 4 | true | true |
| Accor (Managed) | Colombia; Mexico | Accor; Other | 5 | true | true |
| Arbor Lodging (CALA) | United States | Hilton; Hyatt; IHG; Independent; Marriott; Other | 11 | true | true |
| Aimbridge Hospitality (LATAM) | Dominican Republic; Mexico | Accor; Hilton; Hyatt; IHG; Marriott; Wyndham | 8 | true | true |
| Marriott International (Managed) | Mexico; Panama | Marriott | 5 | true | true |
| Tremun Hoteles | Argentina | Independent | 4 | true | true |
| Sonesta International | United States | Sonesta | 3 | false | false |
| Tafer Hotels & Resorts | Mexico | Independent; Other | 2 | true | true |
| Grupo Presidente | Mexico | IHG | 2 | true | true |
| Driftwood Hospitality Management | Costa Rica; Puerto Rico | Hilton; Marriott | 5 | true | true |
| Highgate | Dominican Republic; Jamaica; Mexico; Peru; Puerto Rico | Independent; Marriott | 6 | true | true |
| Royalton Hotels & Resorts | Dominican Republic; Mexico | Other | 3 | true | true |
| Cenote Azul Operadores | — | Independent; Other | 4 | true | true |
| Auberge Resorts Collection | Mexico; United States | Other | 2 | false | false |
| Hotel Equities (CALA) | Dominican Republic; Jamaica; Mexico; U.S. Virgin Islands | Hilton; Hyatt; Independent; Marriott; Other | 9 | true | true |
| Brittain Resorts & Hotels (BRH) | United States | IHG; Independent; Marriott | 5 | true | true |
| Atlantica Hotels International (AHI) | Brazil | Choice; Hilton; Wyndham | 2 | true | true |
| Four Seasons Hotels and Resorts | Colombia; Costa Rica; Mexico | Other | 3 | false | false |
| GHL Hoteles (GHL Holding) | Chile; Colombia; Panama; Peru | Choice; Hyatt; IHG; Independent; Marriott; Sonesta | 6 | true | true |
| Álvarez Argüelles Hoteles | Argentina | Independent; Other | 4 | true | true |
| Rosewood Hotel Group | Brazil; Mexico | Other | 2 | false | false |
| Arriva Hospitality Group (AHG) | Mexico | Other | 4 | true | true |
| Grupo Hotelero Santa Fe | Mexico | Hilton; Other | 2 | true | true |
| OxoHotel | Colombia | Hilton; IHG; Marriott; Other | 5 | true | true |
| Grupo Marta Hospitality | Costa Rica | IHG; Independent; Other | 4 | true | true |
| Hyatt (Managed) | Brazil; Mexico; United States | Hyatt | 3 | false | false |
| Grupo Iberostar | Dominican Republic; Mexico | Other | 5 | true | true |
