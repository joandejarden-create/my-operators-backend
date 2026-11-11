# PR #173 final commercial audit

Zero-write audit of the fifteen shadow Sales Ready rows after demotion gates. No opportunity store, feedback, publication registry, or share-token write.

## Verdict

**2 of 15 genuinely qualify as Sales Ready.**
**3 qualify as Account Development.**
**10 demoted to research continuation (or already speculative).**

## Remaining Sales Ready

| Hotel | ID | Org | Thesis | Contact | Why it qualifies | Next action |
| --- | --- | --- | --- | --- | --- | --- |
| Bethesda Marriott | `gdi_opp_acts_ts27` | ACTS | RELIABLE_INFERRED | Elizabeth Lancaster, Events Manager (DIRECT_BUYER) | Independent dates/city + open venue/hotel status + peak-room estimate; person-direct email | Ask for 2027 venue/housing status and Bethesda candidacy |
| Bethesda Marriott | `gdi_opp_loudoun_college_showcase_2027` | Loudoun Soccer | CONFIRMED | Dave Bucciero, Tournament Director (ENTRY_POINT) | Stay-to-play via HBC on official page; functional `collegeshowcase@` is ask-for route only | Request HBC hotel-list inclusion for Bethesda |

## Account Development

| Hotel | ID | Reason retained as AD |
| --- | --- | --- |
| Bethesda | `gdi_opp_nado_ddaa_washcon_2028` | Credible 2028 Arlington conference + named events buyer, but stretch geography / no primary-host advantage |
| Bethesda | `gdi_opp_nado_ddaa_washcon_2027_overflow` | Competitor host (Crystal Gateway) contracted; overflow only hypothesized |
| Bethesda | `gdi_opp_bebpa_usb_2027` | College Park Marriott named home base; Bethesda overflow not evidenced |

## Demotions from the original 15

| ID | Hotel | Demotion reason |
| --- | --- | --- |
| `gdi_opp_nado_ddaa_washcon_2027_overflow` | Bethesda | Competitor host without evidenced overflow path |
| `gdi_opp_nado_ddaa_washcon_2028` | Bethesda | Stretch-only / no primary-host advantage |
| `gdi_opp_bebpa_usb_2027` | Bethesda | Competitor host (College Park Marriott) |
| `gdi_opp_asae_annual_2029` | Bethesda | Vague “Annual meeting organizers” contact; stretch overflow watch |
| `gdi_opp_potomac_memorial_2027` | Bethesda | Tournament Director without evidenced demand-link; functional mailbox not person-direct |
| `gdi_opp_76th_annual_meeting_3` + Hilton twin | Renaissance / Hilton | 2026 source titles + estimates only; Westin 2026 host noted; no independent 2027 demand text |
| `gdi_opp_namt_fall_conference_*` + Hilton twin | Renaissance / Hilton | Source titles / estimates only; insufficient independent fact text |
| `gdi_opp_arias_*` + Hilton twin | Renaissance / Hilton | Source titles / estimates only; overflow hypothesized |
| `gdi_opp_15th_annual_boutique_*` + Hilton twin | Renaissance / Hilton | Sponsorship/relationship only — not group-room pursuit |

## Portfolio shadow after demotions (649 rows)

| Metric | Value |
| --- | --- |
| CONFIRMED | 12 |
| RELIABLE_INFERRED | 3 |
| SPECULATIVE | 632 |
| CONTRADICTORY | 2 |
| Sales Ready | **2** |
| Account Development | **3** |

### Per priority hotel

| Hotel | Sales Ready | Account Development |
| --- | --- | --- |
| Bethesda Marriott | 2 | 3 |
| Renaissance NYTS | 0 | 0 |
| Hilton NYTS | 0 | 0 |
| Wyndham San Diego Bayside | 0 | 0 |

## Engine corrections added in this audit

- `target-hotel-demand-availability-v1`: competitor/official host ≠ target demand; sponsorship-only and stretch-only demotions; stay-to-play housing channel exception
- Stop treating bare `estimatedAttendance` / `estimatedPeakRooms` as independent scale evidence
- Prefer `primaryContact.role` with job-title shape over short remit labels
- Tighten lodging-remit exemption so vague “organizers” roles do not auto-pass

## Safeguards

- No speculative row remains Sales Ready
- Spice Island seven approved IDs unchanged in durable publication source
- 14 workflows / 11 segments unchanged (certification readiness accounting)
- No paid research; no publication; no operational-data migration
