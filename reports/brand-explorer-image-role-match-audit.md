# Brand Explorer Image Role-Match Audit

Generated: 2026-08-05T00:07:54.272Z
auditPass: **true**

- Brands: **8**
- Pass: **8**
- Fail: **0**

### hilton-hotels-and-resorts
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| hilton-hotels-and-resorts | property_example | footprint.openings | Hilton Panama — CALA | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| hilton-hotels-and-resorts | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Hilt | food_beverage_experience | food_beverage_experience | pass | — | — |
| hilton-hotels-and-resorts | scenario | overview.scenario.3 | CALA Flagship Conversion Confidence | unknown | food_beverage_experience | needs_caption_patch | unrecognized_caption_role | F&B / Bar / Restaurant / Local Experienc |
| hilton-hotels-and-resorts | property_example | footprint.openings | Hilton Cancun, an All-Inclusive Resort (Mar Cari | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| hilton-hotels-and-resorts | gallery | materials.gallery.5 | Property Setting / Destination Context — Hilton  | property_setting | property_setting | pass | — | — |
| hilton-hotels-and-resorts | property_example | footprint.openings | Hilton Bogota — CALA | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| hilton-hotels-and-resorts | gallery | materials.gallery.3 | Public Space / Lobby — Hilton Bogota | public_space_lobby | public_space_lobby | pass | — | — |
| hilton-hotels-and-resorts | scenario | overview.scenario.1 | Meetings-Capable Full-Service Assets | meeting_event | public_space_lobby | caption_overclaim | caption_meeting_event_weak_for_public_space_lobby | Public Space / Lobby |
| hilton-hotels-and-resorts | scenario | overview.scenario.2 | Urban Gateway And Resort Depth | unknown | guest_room_suite | needs_caption_patch | unrecognized_caption_role | Guest Room / Suite |
| hilton-hotels-and-resorts | gallery | materials.gallery.2 | Guest Room / Suite — Hilton Cancun, an All-Inclu | guest_room_suite | guest_room_suite | pass | — | — |
| hilton-hotels-and-resorts | gallery | materials.gallery.1 | Exterior / Arrival — Hilton Panama | exterior_arrival | exterior_arrival | pass | — | — |
| hilton-hotels-and-resorts | gallery | materials.gallery.6 | Gallery — Hilton Bogota | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |

### homewood-suites-by-hilton
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| homewood-suites-by-hilton | gallery | materials.gallery.6 | Wellness / Pool / Spa — Homewood Suites by Hilto | guest_room_suite | wellness_pool_spa | caption_overclaim | possible_caption_guest_room_suite_on_wellness_pool_spa | Wellness / Pool / Spa |
| homewood-suites-by-hilton | gallery | materials.gallery.3 | Public Space / Lobby — Homewood Suites by Hilton | guest_room_suite | public_space_lobby | caption_overclaim | possible_caption_guest_room_suite_on_public_space_lobby | Public Space / Lobby |
| homewood-suites-by-hilton | property_example | footprint.openings | Homewood Suites by Hilton Nashville-Downtown — N | guest_room_suite | unknown | ambiguous | visual_category_unsupported_by_metadata | — |
| homewood-suites-by-hilton | scenario | overview.scenario.2 | Kitchen And Breakfast Length-Of-Stay Economics | unknown | food_beverage_experience | needs_caption_patch | unrecognized_caption_role | F&B / Bar / Restaurant / Local Experienc |
| homewood-suites-by-hilton | gallery | materials.gallery.2 | Guest Room / Suite — Homewood Suites by Hilton M | guest_room_suite | guest_room_suite | pass | — | — |
| homewood-suites-by-hilton | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Home | guest_room_suite | food_beverage_experience | caption_overclaim | possible_caption_guest_room_suite_on_food_beverage_experience | F&B / Bar / Restaurant / Local Experienc |
| homewood-suites-by-hilton | scenario | overview.scenario.1 | Longer-Stay Demand Near Anchors | unknown | exterior_arrival | needs_caption_patch | unrecognized_caption_role | Exterior / Arrival |
| homewood-suites-by-hilton | property_example | footprint.openings | Homewood Suites by Hilton Orlando at Flamingo Cr | guest_room_suite | property_setting | caption_overclaim | possible_caption_guest_room_suite_on_property_setting | Property Setting / Destination Context |
| homewood-suites-by-hilton | property_example | footprint.openings | Homewood Suites by Hilton Miami Downtown/Brickel | guest_room_suite | unknown | ambiguous | visual_category_unsupported_by_metadata | — |
| homewood-suites-by-hilton | scenario | overview.scenario.3 | Upscale Extended-Stay Coverage Depth | unknown | guest_room_suite | needs_caption_patch | unrecognized_caption_role | Guest Room / Suite |
| homewood-suites-by-hilton | gallery | materials.gallery.5 | Gallery — Homewood Suites by Hilton Lake Mary Or | guest_room_suite | unknown | ambiguous | visual_category_unsupported_by_metadata | — |
| homewood-suites-by-hilton | gallery | materials.gallery.1 | Exterior / Arrival — Homewood Suites by Hilton N | guest_room_suite | exterior_arrival | caption_overclaim | possible_caption_guest_room_suite_on_exterior_arrival | Exterior / Arrival |

### home2-suites-by-hilton
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| home2-suites-by-hilton | gallery | materials.gallery.5 | Property Setting / Destination Context — Home2 S | guest_room_suite | property_setting | caption_overclaim | possible_caption_guest_room_suite_on_property_setting | Property Setting / Destination Context |
| home2-suites-by-hilton | gallery | materials.gallery.2 | Guest Room / Suite — Home2 Suites by Hilton Miam | guest_room_suite | guest_room_suite | pass | — | — |
| home2-suites-by-hilton | scenario | overview.scenario.2 | Flexible Suite Configuration Efficiency | guest_room_suite | public_space_lobby | caption_overclaim | possible_caption_guest_room_suite_on_public_space_lobby | Public Space / Lobby |
| home2-suites-by-hilton | gallery | materials.gallery.1 | Exterior / Arrival — Home2 Suites by Hilton Phoe | guest_room_suite | exterior_arrival | caption_overclaim | possible_caption_guest_room_suite_on_exterior_arrival | Exterior / Arrival |
| home2-suites-by-hilton | scenario | overview.scenario.3 | Efficient Dual-Brand Development Pattern | unknown | guest_room_suite | needs_caption_patch | unrecognized_caption_role | Guest Room / Suite |
| home2-suites-by-hilton | property_example | footprint.openings | Home2 Suites by Hilton Ft. Lauderdale Airport-Cr | guest_room_suite | guest_room_suite | pass | — | — |
| home2-suites-by-hilton | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Home | guest_room_suite | food_beverage_experience | caption_overclaim | possible_caption_guest_room_suite_on_food_beverage_experience | F&B / Bar / Restaurant / Local Experienc |
| home2-suites-by-hilton | property_example | footprint.openings | Home2 Suites by Hilton Phoenix Downtown — Phoeni | guest_room_suite | guest_room_suite | pass | — | — |
| home2-suites-by-hilton | gallery | materials.gallery.6 | Wellness / Pool / Spa — Home2 Suites by Hilton P | guest_room_suite | wellness_pool_spa | caption_overclaim | possible_caption_guest_room_suite_on_wellness_pool_spa | Wellness / Pool / Spa |
| home2-suites-by-hilton | gallery | materials.gallery.3 | Public Space / Lobby — Home2 Suites by Hilton Ft | guest_room_suite | public_space_lobby | caption_overclaim | possible_caption_guest_room_suite_on_public_space_lobby | Public Space / Lobby |
| home2-suites-by-hilton | scenario | overview.scenario.1 | Cost-Conscious Longer-Stay Demand | unknown | exterior_arrival | needs_caption_patch | unrecognized_caption_role | Exterior / Arrival |
| home2-suites-by-hilton | property_example | footprint.openings | Home2 Suites by Hilton Miami Doral West Airport  | guest_room_suite | unknown | ambiguous | visual_category_unsupported_by_metadata | — |

### tru-by-hilton
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tru-by-hilton | gallery | materials.gallery.2 | Guest Room / Suite — Tru by Hilton Miami Airport | guest_room_suite | guest_room_suite | pass | — | — |
| tru-by-hilton | scenario | overview.scenario.1 | Spirited New-Build Value Product | unknown | public_space_lobby | needs_caption_patch | unrecognized_caption_role | Public Space / Lobby |
| tru-by-hilton | scenario | overview.scenario.3 | New-Build Growth-Pipeline Discipline | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| tru-by-hilton | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Tru  | food_beverage_experience | food_beverage_experience | pass | — | — |
| tru-by-hilton | gallery | materials.gallery.1 | Exterior / Arrival — Tru by Hilton Atlanta Airpo | exterior_arrival | exterior_arrival | pass | — | — |
| tru-by-hilton | gallery | materials.gallery.5 | Gallery — Tru by Hilton Atlanta Galleria Ballpar | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| tru-by-hilton | property_example | footprint.openings | Tru by Hilton Scottsdale Salt River — Scottsdale | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| tru-by-hilton | gallery | materials.gallery.3 | Public Space / Lobby — Tru by Hilton Scottsdale  | public_space_lobby | public_space_lobby | pass | — | — |
| tru-by-hilton | gallery | materials.gallery.6 | Wellness / Pool / Spa — Tru by Hilton Atlanta Ai | wellness_pool_spa | wellness_pool_spa | pass | — | — |
| tru-by-hilton | scenario | overview.scenario.2 | Cross-Generational Leisure And Bleisure Capture | unknown | food_beverage_experience | needs_caption_patch | unrecognized_caption_role | F&B / Bar / Restaurant / Local Experienc |
| tru-by-hilton | property_example | footprint.openings | Tru by Hilton Atlanta Airport College Park — Col | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| tru-by-hilton | property_example | footprint.openings | Tru by Hilton Atlanta Galleria Ballpark — Atlant | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |

### doubletree-by-hilton
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| doubletree-by-hilton | gallery | materials.gallery.5 | Gallery — DoubleTree by Hilton Buenos Aires | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| doubletree-by-hilton | property_example | footprint.openings | DoubleTree by Hilton Lima San Isidro — CALA | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| doubletree-by-hilton | gallery | materials.gallery.1 | Exterior / Arrival — DoubleTree by Hilton Lima S | exterior_arrival | exterior_arrival | pass | — | — |
| doubletree-by-hilton | gallery | materials.gallery.6 | Wellness / Pool / Spa — El Pardo Lima - A Double | wellness_pool_spa | wellness_pool_spa | pass | — | — |
| doubletree-by-hilton | gallery | materials.gallery.3 | Public Space / Lobby — El Pardo Lima - A DoubleT | public_space_lobby | public_space_lobby | pass | — | — |
| doubletree-by-hilton | scenario | overview.scenario.3 | Legacy Full-Service Repositioning With Signature | unknown | guest_room_suite | needs_caption_patch | unrecognized_caption_role | Guest Room / Suite |
| doubletree-by-hilton | scenario | overview.scenario.1 | Signature Warm-Welcome Full-Service Product | unknown | exterior_arrival | needs_caption_patch | unrecognized_caption_role | Exterior / Arrival |
| doubletree-by-hilton | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Doub | food_beverage_experience | food_beverage_experience | pass | — | — |
| doubletree-by-hilton | scenario | overview.scenario.2 | CALA Urban Full-Service Expansion | unknown | public_space_lobby | needs_caption_patch | unrecognized_caption_role | Public Space / Lobby |
| doubletree-by-hilton | property_example | footprint.openings | DoubleTree by Hilton Buenos Aires — CALA | unknown | guest_room_suite | needs_caption_patch | unrecognized_caption_role | Guest Room / Suite |
| doubletree-by-hilton | property_example | footprint.openings | DoubleTree by Hilton Mexico City Santa Fe — Mexi | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| doubletree-by-hilton | gallery | materials.gallery.2 | Guest Room / Suite — DoubleTree by Hilton Buenos | guest_room_suite | guest_room_suite | pass | — | — |

### hampton-by-hilton
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| hampton-by-hilton | property_example | footprint.openings | Hampton by Hilton San Jose Airport — Alajuela | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| hampton-by-hilton | scenario | overview.scenario.1 | Reliable Focused-Service Consistency | unknown | exterior_arrival | needs_caption_patch | unrecognized_caption_role | Exterior / Arrival |
| hampton-by-hilton | gallery | materials.gallery.3 | Public Space / Lobby — Hampton by Hilton Bogotá  | public_space_lobby | public_space_lobby | pass | — | — |
| hampton-by-hilton | scenario | overview.scenario.2 | Hamptonality And Breakfast Guarantee Coverage | unknown | food_beverage_experience | needs_caption_patch | unrecognized_caption_role | F&B / Bar / Restaurant / Local Experienc |
| hampton-by-hilton | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Hamp | food_beverage_experience | food_beverage_experience | pass | — | — |
| hampton-by-hilton | gallery | materials.gallery.1 | Exterior / Arrival — Hampton by Hilton San Jose  | exterior_arrival | exterior_arrival | pass | — | — |
| hampton-by-hilton | gallery | materials.gallery.5 | Wellness / Pool / Spa — Hampton by Hilton Guanac | wellness_pool_spa | wellness_pool_spa | pass | — | — |
| hampton-by-hilton | gallery | materials.gallery.6 | Gallery — Hampton by Hilton Bogotá - Usaquén | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| hampton-by-hilton | scenario | overview.scenario.3 | CALA Market-Entry Momentum Advantage | unknown | food_beverage_experience | needs_caption_patch | unrecognized_caption_role | F&B / Bar / Restaurant / Local Experienc |
| hampton-by-hilton | property_example | footprint.openings | Hampton by Hilton Panama — CALA | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| hampton-by-hilton | property_example | footprint.openings | Hampton by Hilton San Jose Airport — Alajuela | unknown | unknown | needs_caption_patch | unrecognized_caption_role | — |
| hampton-by-hilton | gallery | materials.gallery.2 | Guest Room / Suite — Hampton by Hilton Guanacast | guest_room_suite | guest_room_suite | pass | — | — |

### hilton-garden-inn
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| hilton-garden-inn | gallery | materials.gallery.3 | Public Space / Lobby — Hilton Garden Inn Bogota  | public_space_lobby | public_space_lobby | pass | — | — |
| hilton-garden-inn | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Hilt | food_beverage_experience | food_beverage_experience | pass | — | — |
| hilton-garden-inn | property_example | footprint.openings | Hilton Garden Inn Bogota Airport — Bogotá | unknown | food_beverage_experience | needs_caption_patch | unrecognized_caption_role | F&B / Bar / Restaurant / Local Experienc |
| hilton-garden-inn | scenario | overview.scenario.2 | Bleisure And Meetings Cross-Coverage | meeting_event | public_space_lobby | caption_overclaim | caption_meeting_event_weak_for_public_space_lobby | Public Space / Lobby |
| hilton-garden-inn | gallery | materials.gallery.2 | Guest Room / Suite — Hilton Garden Inn Guanacast | guest_room_suite | guest_room_suite | pass | — | — |
| hilton-garden-inn | scenario | overview.scenario.3 | Milestone-Driven CALA Expansion Corridor | exterior_arrival | food_beverage_experience | caption_overclaim | possible_caption_exterior_arrival_on_food_beverage_experience | F&B / Bar / Restaurant / Local Experienc |
| hilton-garden-inn | property_example | footprint.openings | Hilton Garden Inn San Jose Airport City Mall — C | unknown | property_setting | needs_caption_patch | unrecognized_caption_role | Property Setting / Destination Context |
| hilton-garden-inn | property_example | footprint.openings | Hilton Garden Inn Guanacaste Airport — CALA | unknown | property_setting | needs_caption_patch | unrecognized_caption_role | Property Setting / Destination Context |
| hilton-garden-inn | gallery | materials.gallery.1 | Exterior / Arrival — Hilton Garden Inn San Jose  | exterior_arrival | exterior_arrival | pass | — | — |
| hilton-garden-inn | gallery | materials.gallery.6 | Wellness / Pool / Spa — Hilton Garden Inn San Jo | wellness_pool_spa | wellness_pool_spa | pass | — | — |
| hilton-garden-inn | gallery | materials.gallery.5 | Property Setting / Destination Context — Hilton  | property_setting | property_setting | pass | — | — |
| hilton-garden-inn | scenario | overview.scenario.1 | Upscale Focused-Service Balance | unknown | exterior_arrival | needs_caption_patch | unrecognized_caption_role | Exterior / Arrival |

### spark-by-hilton
- pass: **true** · roleMatch=true · uniqueness=true · unresolved=0
- galleryDistinct=6 scenario=3 property=3
- action: `no_action`

| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| spark-by-hilton | property_example | footprint.openings | Spark by Hilton Duluth — Duluth | wellness_pool_spa | exterior_arrival | caption_overclaim | caption_wellness_pool_spa_weak_for_exterior_arrival | Exterior / Arrival |
| spark-by-hilton | gallery | materials.gallery.1 | Exterior / Arrival — Spark by Hilton Nashville a | exterior_arrival | exterior_arrival | pass | — | — |
| spark-by-hilton | gallery | materials.gallery.4 | F&B / Bar / Restaurant / Local Experience — Spar | food_beverage_experience | food_beverage_experience | pass | — | — |
| spark-by-hilton | scenario | overview.scenario.3 | Efficient Reflag For Aging Independents | unknown | food_beverage_experience | needs_caption_patch | unrecognized_caption_role | F&B / Bar / Restaurant / Local Experienc |
| spark-by-hilton | property_example | footprint.openings | Spark by Hilton Nashville at Opryland — Nashvill | wellness_pool_spa | guest_room_suite | caption_overclaim | caption_wellness_pool_spa_weak_for_guest_room_suite | Guest Room / Suite |
| spark-by-hilton | gallery | materials.gallery.3 | Public Space / Lobby — Spark by Hilton Nashville | public_space_lobby | public_space_lobby | pass | — | — |
| spark-by-hilton | gallery | materials.gallery.6 | Gallery — Spark by Hilton Duluth | wellness_pool_spa | unknown | ambiguous | visual_category_unsupported_by_metadata | — |
| spark-by-hilton | property_example | footprint.openings | Spark by Hilton Atlanta Cumberland Ballpark — At | wellness_pool_spa | unknown | ambiguous | visual_category_unsupported_by_metadata | — |
| spark-by-hilton | gallery | materials.gallery.2 | Guest Room / Suite — Spark by Hilton Duluth | guest_room_suite | guest_room_suite | pass | — | — |
| spark-by-hilton | gallery | materials.gallery.5 | Wellness / Pool / Spa — Spark by Hilton Atlanta  | wellness_pool_spa | wellness_pool_spa | pass | — | — |
| spark-by-hilton | scenario | overview.scenario.1 | Conversion-Only Premium Economy Discipline | unknown | public_space_lobby | needs_caption_patch | unrecognized_caption_role | Public Space / Lobby |
| spark-by-hilton | scenario | overview.scenario.2 | Value-Ladder Portfolio Bottom Fill | unknown | guest_room_suite | needs_caption_patch | unrecognized_caption_role | Guest Room / Suite |
