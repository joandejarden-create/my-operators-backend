# v40 Remediation — kimpton

- Blockers: 29
- Patches: 22 (22 copy, 0 hide)
- Safe for generic apply: 22
- Property review: extras_acceptable_founder_confirm — 5 visible examples exceed minimum 3; UI should handle extras. Founder confirm sort order is clean. No hide required.

## Blocker types
- `founder_review_missing`: 1
- `active_approval_missing`: 1
- `render_contract_issue`: 1
- `fee_stack_language`: 6
- `loi_language`: 2
- `visible_url`: 5
- `fdd_item19_language`: 5
- `forbidden_owner_copy`: 3
- `net_contribution_language`: 5

## DOM projection
- forbidden strings: 24 → 0
- visible URLs: 5 → 0
- internal notes: 0 → 0
- empty cards: 0 → 0
- expected displayState after remediation: `draft_applied_with_defects`
- still blocked by founder review: **yes**
- still blocked by active approval: **yes**
- unlock in v40: **no**

## Sample patches (first 15)
### footprint.openings · Body (`rec5FqPlYHgQ0LiFZ`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Urban, Mexico, CALA, Polanco, Lifestyle boutique

Mexico City, Mexico (Polanquito / Polanco)

Adaptive reuse · 48-room boutique · restaurant-forward

Mexico City debut · Polanquito
- After: Urban, Mexico, CALA, Polanco, Lifestyle boutique
Mexico City, Mexico (Polanquito / Polanco)
Adaptive reuse · 48-room boutique · restaurant-forward
Mexico City debut · Polanquito li
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.openings · Case Summary Interpretation (`rec5FqPlYHgQ0LiFZ`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Small-key boutiques live on ADR and F&B margin—match Polanco-style demand and operator capability to your building before signing.
- After: Small-key boutiques live on average daily rate and F&B margin—match Polanco-style demand and operator capability to your building before signing.
- Checks: owner=pass forbidden=pass safeApply=true

### standards.intro · Body (`rec7WfKylobSTpvaH`)
- Reason: Owner-copy scrub: loi_language
- Before: Kimpton Hotels standards vary by conversion path, F&B program, and agreement vintage. Use the table as a planning checklist—confirm every row in the FDD, design manual, and LOI bef
- After: Kimpton Hotels standards vary by conversion path, F&B program, and agreement vintage. Use the table as a planning checklist—confirm every row in the commercial agreement review mat
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Body (`rec9kV27xSumrl3Ul`)
- Reason: Owner-copy scrub: visible_url
- Before: Resort, Mexico, CALA, Baja Sur, Beachfront lifestyle

Todos Santos, Baja California Sur, Mexico (Pacific coast)

Resort lifestyle · adults-oriented · spa and F&B forward

Kimpton M
- After: Resort, Mexico, CALA, Baja Sur, Beachfront lifestyle
Todos Santos, Baja California Sur, Mexico (Pacific coast)
Resort lifestyle · adults-oriented · spa and F&B forward
Kimpton Mas 
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.region.mea · Body (`recCv4rrMam4T3qOX`)
- Reason: Owner-copy scrub: fdd_item19_language, forbidden_owner_copy
- Before: MEA

Limited direct Kimpton footprint—confirm counts in your franchise disclosure document if evaluating international portfolio context.
- After: MEA
Limited direct Kimpton footprint—confirm counts in your commercial agreement materials if evaluating international portfolio context.
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Body (`recKWkDTNvLxknx8H`)
- Reason: Owner-copy scrub: visible_url
- Before: Resort, Honduras, CALA, Western Caribbean, Beach repositioning

Roatán, Bay Islands, Honduras (West Bay Beach)

Conversion repositioning · 119 keys · reef-access leisure

Kimpton G
- After: Resort, Honduras, CALA, Western Caribbean, Beach repositioning
Roatán, Bay Islands, Honduras (West Bay Beach)
Conversion repositioning · 119 keys · reef-access leisure
Kimpton Gran
- Checks: owner=pass forbidden=pass safeApply=true

### overview.bestAt.3 · Body (`recL1VdG6HPNYQpJs`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: IHG One Rewards and enterprise demand participation—model net contribution after ~6% royalty and mandatory IHG programs.
- After: IHG One Rewards and enterprise demand participation—model owner economics after ~6% royalty and mandatory IHG programs.
- Checks: owner=pass forbidden=pass safeApply=true

### overview.owner_experience · Body (`recLaMkNyMJhMrtg5`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Typical guest: experience-led leisure and business travelers who value design, F&B, and neighborhood authenticity.
Owner journey: feasibility on conversion PIP → design narrative a
- After: Typical guest: experience-led leisure and business travelers who value design, F&B, and neighborhood authenticity.
Owner journey: feasibility on conversion PIP → design narrative a
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Body (`recLdg63DjmKPhTPZ`)
- Reason: Owner-copy scrub: visible_url
- Before: Urban, Dominican Republic, CALA, Colonial City, Lifestyle boutique

Santo Domingo, Dominican Republic (Colonial City / Zona Colonial)

Historic conversion · lifestyle boutique · re
- After: Urban, Dominican Republic, CALA, Colonial City, Lifestyle boutique
Santo Domingo, Dominican Republic (Colonial City / Zona Colonial)
Historic conversion · lifestyle boutique · rest
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Case Summary Interpretation (`recLdg63DjmKPhTPZ`)
- Reason: Owner-copy scrub: visible_url
- Before: Colonial-city assets need F&B and design capex matched to local ADR—do not assume U.S. urban Kimpton ramp from one opening headline.
- After: Colonial-city assets need F&B and design capex matched to local average daily rate—do not assume U.S. urban Kimpton ramp from one opening headline.
- Checks: owner=pass forbidden=pass safeApply=true

### commercial.kpi.lens · Body (`recOTNxvfyg40LqHa`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Net contribution after fees and channel costs
- After: owner economics after fees and channel costs
- Checks: owner=pass forbidden=pass safeApply=true

### economics.intro · Body (`recSiHoaLhA2eecAp`)
- Reason: Owner-copy scrub: loi_language, fdd_item19_language, forbidden_owner_copy
- Before: Kimpton Hotels (upper-upscale lifestyle, IHG) economics below reflect typical disclosed ranges for diligence—not a quote or substitute for your franchise disclosure document, LOI, 
- After: Kimpton Hotels (upper-upscale lifestyle, IHG) economics below reflect typical disclosed ranges for diligence—not a quote or substitute for your commercial agreement materials, lett
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.openings · Body (`recTDYl3FNT1WEVxD`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Resort, Cayman Islands, CALA, Seven Mile Beach, Flagship Caribbean

Grand Cayman, Cayman Islands (Seven Mile Beach)

New-build resort · 266 rooms · spa and signature F&B

Caribbean
- After: Resort, Cayman Islands, CALA, Seven Mile Beach, Flagship Caribbean
Grand Cayman, Cayman Islands (Seven Mile Beach)
New-build resort · 266 rooms · spa and signature F&B
Caribbean fl
- Checks: owner=pass forbidden=pass safeApply=true

### loyalty.owner_lens · Body (`recUJ0HZQzWrdV00t`)
- Reason: Owner-copy scrub: fdd_item19_language, net_contribution_language
- Before: Model loyalty as net contribution after member discounts, elite benefits, and IHG program chargebacks — not headline ADR.
IHG One Rewards generated an average of 50.8% of Kimpton H
- After: Model loyalty as owner economics after member discounts, elite benefits, and IHG program chargebacks — not headline average daily rate.
IHG One Rewards generated an average of 50.8
- Checks: owner=pass forbidden=pass safeApply=true

### overview.proof.5 · Body (`recVt5GnSWF0g8Tkv`)
- Reason: Owner-copy scrub: fdd_item19_language
- Before: IHG One Rewards spans 6,600+ hotels worldwide; member pricing, elite tiers, and partner ecosystem. Model this brand's loyalty contribution from your FDD Item 19 sample and local co
- After: IHG One Rewards spans 6,600+ hotels worldwide; member pricing, elite tiers, and partner ecosystem. Model this brand's loyalty contribution from your commercial agreement review mat
- Checks: owner=pass forbidden=pass safeApply=true
