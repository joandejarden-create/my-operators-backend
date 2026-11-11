# GDI evidence audit after reading the stored fact text

Shadow evaluation only. No hotel store, publication registry, or Airtable record was written. The earlier count of 31 substantiated opportunities came from fact codes and source titles. Those are labels, not the source fact.

## What the fact text says

The persisted fact is `evidence.value` and, where present, `sources[].claim` / `establishes`. `extractedText` is empty. Run journals under each hotel store are small metadata files and do not add excerpts.

Of 649 canonical opportunities, 331 already contain that fact text and 318 do not. Reading the text, instead of the label, changes the result:

| Class | PR #170 | After this audit |
| --- | --- | --- |
| CONFIRMED_DEMAND | 29 | 3 |
| RELIABLE_INFERRED_DEMAND | 2 | 0 |
| SPECULATIVE_DEMAND | 617 | 643 |
| CONTRADICTORY_OR_NEGATIVE | 1 | 3 |
| Sales Ready | 3 | 0 |
| Account Development | 7 | 0 |
| Research Continuation | 639 | 649 |

All three confirmed rows are Bethesda Marriott. No other hotel has an independent demand statement in its stored fact text. Verified direct-buyer and intermediary labels still exist on 42 and 17 rows. Those labels do not create Sales Ready without an evidence-grounded demand statement and a contact whose role attribution passes the existing eligibility contract.

## The three previous Sales Ready rows

None survived.

- Waterstone `gdi_s4912_26ef08d1ac86`. The stored claims describe staff duties, a venue at another hotel, and one claim that a session does not establish a Boca venue. Catering and lodging words were in a job description and in the hotel's own inventory.
- Bethesda `gdi_opp_nice_2027`. The 2027 location is explicitly unannounced. Attendance figures are for prior meetings in other cities.
- Bethesda `gdi_opp_bethesda_premier_cup_2026`. The housing fact names a vendor and a URL. It does not state a lodging requirement.

A fourth row, `gdi_opp_msysa_state_cup_2027`, briefly matched because the words "stay-to-play" appear in a sentence that says that language was not found. It is contradictory and stays in research continuation. The named contact remains an entry point, not a reason to call the demand confirmed.

## The three rows that stay confirmed

Each states a lodging rule. None is Sales Ready.

| Opportunity | Fact | Why it is not Sales Ready |
| --- | --- | --- |
| `gdi_opp_potomac_memorial_2027` | Teams beyond a set distance must book through the housing system. | The named person is leadership without a demand-link. |
| `gdi_opp_loudoun_college_showcase_2027` | March 2027, stay-to-play through the housing bureau. | A relevant entry point is named, but the eligibility contract rejects the buyer attribution. The fact text does not place the housing at Bethesda Marriott. |
| `gdi_opp_alexandria_soccer_kickoff_2027` | Reservations must be made through the traveling-team channel. | The named contact is unrelated to the demand. |

## Research continuation priority

These are the strongest rows to research next. They have a future date, stored fact text, and a direct-buyer contact class. They are still speculative. The list does not suppress the other 631 continuation rows.

1. Renaissance `gdi_opp_2026_education_engagement_forum_1` — 2026-11-12
2. Waterstone `gdi_s4912_41378ee1c113` — 2027-02-08
3. Waterstone `gdi_opp_iaadfs_summit_of_the_americas_2027_6` — 2027-03-07
4. Bethesda `gdi_opp_nado_ddaa_washcon_2027_overflow` — 2027-03-07
5. Bethesda `gdi_opp_bebpa_usb_2027` — 2027-03-08
6. Renaissance `gdi_opp_forum_on_education_abroad_annual_conference_14` — 2027-03-10
7. Bethesda `gdi_opp_cmss_spring_2027` — 2027-03-30
8. Bethesda `gdi_opp_ndss_advocacy_2027` — 2027-04-19
9. Bethesda `gdi_opp_acts_ts27` — 2027-04-20
10. Renaissance `gdi_opp_spring_road_conference_2` — 2027-05-01
11. Waterstone `gdi_opp_uca_convergence_2027_overflow_vip_housing_9` — 2027-05-01
12. Renaissance `gdi_opp_76th_annual_meeting_3` — 2027-08-01

318 opportunities have no recoverable fact text. New discovery would be required before they can be qualified. That work was not started.

## Aimbridge dossiers

The dry-run reads the existing research-state files and does not create `opportunities.json`. Every dossier publication status is `NOT_AUTHORIZED`.

| Hotel | Dossiers | Account-development disposition | Evidence URLs | Named-contact rows |
| --- | --- | --- | --- | --- |
| Sheraton Guadalajara Expo | 434 | 19 | 51 | 3 |
| voco Cancún | 381 | 14 | 51 | 0 verified named buyers |
| Wyndham Alltra Punta Cana | 227 | 4 | 18 | 4 |

Most dossiers have no disposition. They stay research-only. A later approved import could copy the directories. It must not turn `whyHotel` text into customer opportunities.

## Publication

The published registry still contains one hotel, Spice Island, with these seven ids and no others: `gdi_s4912_ba7b1cb8a3ab`, `gdi_s4912_b956aa67a4a2`, `gdi_s4912_b6d9b1929c05`, `gdi_s4912_cb2d71ab1b15`, `gdi_s4912_cc2a8da6ca6e`, `gdi_s4912_03deda917778`, `gdi_s4912_3cafec77a21f`. Bethesda's research store was not merged into that registry.

## Code

`classifyDemandMechanismEvidence` now classifies from the fact text. A fact code or a source title cannot confirm demand. A sentence that says the lodging language was not found cannot confirm demand. The returned excerpt is that fact text, not the generated thesis. Shadow Account Development now requires the eligibility contract's buyer-responsibility gate, so an unsupported contact is not promoted.

## Scorecard

| Layer | Result |
| --- | --- |
| Engine execution | PASS for the zero-write audit |
| Research quality | NOT VERIFIED. 643 opportunities still have no independent demand statement, and 318 have no fact text at all. |
| Commercial qualification | NOT VERIFIED. No opportunity is Sales Ready after the audit. |
| Customer delivery | NOT VERIFIED. Authenticated detail, Need-Date, and contact presentation were not re-tested in this pass. |
| Publication safety | PASS. The seven Spice Island records were not rewritten. |
| Full E2E | NOT VERIFIED |
