# Wave 17 Batch A — Openings Remediation

- Generated: 2026-08-14T02:06:56.232Z
- Mode: **APPLY**
- Ready statement: `wave17_batch_a_images_complete_ready_for_post_image_review`
- Active universe: **65** → **65** (expected 65)
- Presentation writes: **1** (max 3 openings cards)
- Recent Momentum writes: **0**
- Protected-field writes: **0**
- Active 65 writes: **0**

## Research

### Hyatt Regency

| Field | Value |
| --- | --- |
| Invalid current | Hyatt Regency Cancun |
| Reason invalid | No current live Hyatt Regency Cancun property page safely validated; do not force Cancun identity. |
| Replacement | Hyatt Regency Orlando |
| City / Country | Orlando, USA |
| Current brand identity | Hyatt Regency |
| Official property | https://www.hyatt.com/hyatt-regency/en-US/mcoro-hyatt-regency-orlando |
| Image candidate | https://v5.airtableusercontent.com/v3/u/56/56/1786687200000/9Fgq7JBhTDJmD-LmPur62A/xytkEBEjFPW1sFnsvoHYG5uIxqMsJpwMagjcTVQBWXeGmoSJRjtFQXuUApKRRPWlca9SOtyBAECpqWEF_KwnDkx0SaomLbAZmKIrA27SQS5w6H_blT06jVaImoWMIOtqyF7JK15ropk9DxUUValV0w/UgvITCYwbNOeRDUFzvU1fzieDRj7vTEGfjZQaHBOcM0 |
| Identity confidence | **HIGH** |
| Replacement recommended | **yes** |

### Hyatt Centric

| Field | Value |
| --- | --- |
| Invalid current | Hyatt Centric Midtown 5th Avenue New York |
| Reason invalid | Expected live property URL (nycmt) unavailable/404; alternate nycct path not positively HTML-validated (rate-limited); no Midtown DAM harvest for HIGH-confidence image attach. |
| Replacement | Hyatt Centric Brickell Miami |
| City / Country | Miami, USA |
| Current brand identity | Hyatt Centric |
| Official property | https://www.hyatt.com/hyatt-centric/en-US/miact-hyatt-centric-brickell-miami |
| Image candidate | https://v5.airtableusercontent.com/v3/u/56/56/1786687200000/L_bNRA2a7nkRJItkpWFEqA/7rqgrGNqDVB1NBXK_wN42EvuT193jhbsVN0GvzKeROjui-tZ_4ul_uC_NUvDKd7x50QTUGitkhG54JvS6jEOxcwjdn4uCV186gfMKIsmPcZUYZefuhRC31ykuPLJ3ugCFjIA_4nRDe6MxnFzf8V8jA/s5xM65Bdhcioc1HodDeaMpUUxZCgKBMyOUDBIsVpreQ |
| Identity confidence | **HIGH** |
| Replacement recommended | **yes** |

### Thompson Hotels

| Field | Value |
| --- | --- |
| Invalid current | Thompson Playa del Carmen |
| Reason invalid | Property rebranded to Hyatt Centric — historical Thompson identity must not be used as a current Thompson example. |
| Replacement | The Cape, A Thompson Hotel |
| City / Country | Cabo San Lucas, Mexico |
| Current brand identity | Thompson Hotels |
| Official property | https://www.hyatt.com/thompson-hotels/en-US/cslth-the-cape |
| Image candidate | https://v5.airtableusercontent.com/v3/u/56/56/1786687200000/PmbrpbdFxML27mKR9J_D9g/aytcjZevoyosL4icfwTbOssIAFMtrHyHCVS1Q6-FPnyzqP_cFnb_iM1TXtF-dAZFsLcNm4y6c995SdRHFJ5zcLlGDwXzGKJMAdmVO5z1vFRgjonZfDKz2h6sUSOb8lwzWI0azceVwA6FmY5_CBRMPg/eAPlbLJ_0AcSz6R3RZmqBk0_PUDvea5g7RXTsrW2MZ0 |
| Identity confidence | **HIGH** |
| Replacement recommended | **yes** |

## Dry-run / apply diffs

- **Hyatt Regency** `recQ1zGQRhSedhTdO`: Hyatt Regency Cancun → **Hyatt Regency Orlando**
  - Fields: 
  - Image: (none) → https://v5.airtableusercontent.com/v3/u/56/56/1786687200000/9Fgq7JBhTDJmD-LmPur62A/xytkEBEjFPW1sFnsvoHYG5uIxqMsJpwMagjcTVQBWXeGmoSJRjtFQXuUApKRRPWlca9SOtyBAECpqWEF_KwnDkx0SaomLbAZmKIrA27SQS5w6H_blT06jVaImoWMIOtqyF7JK15ropk9DxUUValV0w/UgvITCYwbNOeRDUFzvU1fzieDRj7vTEGfjZQaHBOcM0
  - Allowed: true

- **Hyatt Centric** `rechFlli0qV5Jq6h6`: Hyatt Centric Midtown 5th Avenue New York → **Hyatt Centric Brickell Miami**
  - Fields: 
  - Image: (none) → https://v5.airtableusercontent.com/v3/u/56/56/1786687200000/L_bNRA2a7nkRJItkpWFEqA/7rqgrGNqDVB1NBXK_wN42EvuT193jhbsVN0GvzKeROjui-tZ_4ul_uC_NUvDKd7x50QTUGitkhG54JvS6jEOxcwjdn4uCV186gfMKIsmPcZUYZefuhRC31ykuPLJ3ugCFjIA_4nRDe6MxnFzf8V8jA/s5xM65Bdhcioc1HodDeaMpUUxZCgKBMyOUDBIsVpreQ
  - Allowed: true

- **Thompson Hotels** `rec4XJtwzRNsh8M7i`: Thompson Playa del Carmen → **The Cape, A Thompson Hotel**
  - Fields: Title, Body, Case Summary Overview, Case Summary Tags, Case Summary Brand Relevance, Case Summary Owner Objective, Case Summary Interpretation
  - Image: (none) → https://v5.airtableusercontent.com/v3/u/56/56/1786687200000/PmbrpbdFxML27mKR9J_D9g/aytcjZevoyosL4icfwTbOssIAFMtrHyHCVS1Q6-FPnyzqP_cFnb_iM1TXtF-dAZFsLcNm4y6c995SdRHFJ5zcLlGDwXzGKJMAdmVO5z1vFRgjonZfDKz2h6sUSOb8lwzWI0azceVwA6FmY5_CBRMPg/eAPlbLJ_0AcSz6R3RZmqBk0_PUDvea5g7RXTsrW2MZ0
  - Allowed: true

## Post-remediation counts

- **Hyatt Regency**: gallery 6/6 · scenario 3/3 · openings 3/3 · uniqueness PASS · role-match PASS · identity PASS
- **Hyatt Centric**: gallery 6/6 · scenario 3/3 · openings 3/3 · uniqueness PASS · role-match PASS · identity PASS
- **Thompson Hotels**: gallery 6/6 · scenario 3/3 · openings 3/3 · uniqueness PASS · role-match PASS · identity PASS

## Gate totals

- wrong-brand: **0**
- wrong-property: **0**
- broken images: **0**
- uniqueness: **PASS**
- role-match: **PASS**
- non-Momentum completeness: **PASS**


## Cumulative Presentation writes (both apply passes)

1. `recQ1zGQRhSedhTdO` Hyatt Regency → Hyatt Regency Orlando (Title/Body/Image + Case Summary*)
2. `rechFlli0qV5Jq6h6` Hyatt Centric → Hyatt Centric Brickell Miami (Title/Body/Image + Case Summary*)
3. `rec4XJtwzRNsh8M7i` Thompson → The Cape, A Thompson Hotel (Title/Body/Image + Case Summary*)
4. `rec4XJtwzRNsh8M7i` title-only repair (remove double-brand suffix; Image unchanged)

Gallery writes: **0** · Scenario writes: **0** · Recent Momentum writes: **0** · Protected-field writes: **0** · Active 65 writes: **0**

## Recommended next stage

- Proceed to post-image review (not Batch B / Dream / promote / release / Recent Momentum).
