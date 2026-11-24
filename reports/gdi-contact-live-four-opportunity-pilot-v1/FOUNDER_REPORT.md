# GDI Live Contact Intelligence Pilot — Four Opportunities

**Status:** STOP — live pilot complete under $3 Exa hard cap.

**Mode:** LIVE (not mocked) · **Canonical main:** `c6272fb737fbacf0cf9916c331465659351a65b6`  
**Preflight:** criticalFail=false · **Halt:** none  
**Exa spent:** $0.0160 / $3.00 · **Remaining:** $2.9840 · **Exa calls:** 4 · **Direct retrievals:** 20

---

## Scorecard

| Opportunity | Exa spend | Primary verified | Secondary verified | Public HOW found | Finalized eligible | Sales Ready |
|---|---:|---|---|---|---|---|
| Rainbow Cup / Jason Gooding | $0.0040 | Jason Gooding | — | yes | yes | yes |
| Meeting Point partners | $0.0040 | Filomena Marcelo | Luísa Marques | no | yes | no |
| IPBA 2026 organizing leadership | $0.0040 | João Freire | Robert Govers | no | yes | no |
| GA Society / Kern and Kelber | $0.0040 | Cornelia Kern | Olaf Kelber | yes | yes | yes |

WHO_COMPLETE / HOW_COMPLETE / SALES_READY classified independently. Missing email does not invalidate WHO.

---

## 1. Verified people, positions, employers

| Rank | Person | Role | Employer | Opportunity |
|---|---|---|---|---|
| PRIMARY | Jason Gooding | Event organizer / Odessey Timing & Events | Odessey Timing & Events | disc_69a16d5c89f9 |
| PRIMARY | Filomena Marcelo | Partner | MeetingPoint (PT) | disc_e93837f2b647 |
| SECONDARY | Luísa Marques | Partner | MeetingPoint (PT) | disc_e93837f2b647 |
| PRIMARY | João Freire | Organizing Committee Chair | IPAM Lisboa / IPBA 2026 | disc_f37cfea74d4f |
| SECONDARY | Robert Govers | Representing IPBA | International Place Branding Association | disc_f37cfea74d4f |
| PRIMARY | Cornelia Kern | GA Administrative Officer | Society for Medicinal Plant and Natural Product Research | disc_6280e5bb17d6 |
| SECONDARY | Olaf Kelber | GA Secretary | Society for Medicinal Plant and Natural Product Research | disc_6280e5bb17d6 |

---

## 2. Commercial relevance

- **Jason Gooding** — listed Rainbow Cup Grenada event contact; path for participant housing / event partnerships at LaLuna.
- **Filomena Marcelo** — Partner at Portuguese MeetingPoint PCO (Linda-a-Velha); hotel sourcing authority for Memmo. Not confused with foreign same-name orgs.
- **Luísa Marques** — Co-partner secondary on the same team page.
- **João Freire** — IPBA 2026 Lisbon organizing chair; best hotel approach for this edition (not a keynote speaker).
- **Robert Govers** — IPBA-side organizing representative (secondary).
- **Cornelia Kern** — GA admin officer with public `events@` channel; logistics/events contact.
- **Olaf Kelber** — Current GA Secretary (secondary association officer).

---

## 3. Rejected candidates (sample)

- Professional Conference Organizer — JUNK_OR_NON_PERSON_NAME
- Steve Millington — KNOWN_CHROME_FALSE_POSITIVE (academic keynote, not lodging buyer)
- Chakrit Pichyangkul / Senna Fernandes — EMPLOYER_NOT_MATCHED (wrong geography/org for IPBA Lisbon lodging)
- Marriot Hotel. Mari Stevens — JUNK_OR_NON_PERSON_NAME

---

## 4. Alternative-person strategies

CONTACT_RESEARCH executed with live Exa (1 query/opp). BUYER_ALTERNATIVE_PERSON_RESEARCH ran with `rankingRan=true`. Paid ALT strategy walk was **not** invoked (`researcherInvoked=false`, `strategiesAttempted=[]`) because harvest already contained a qualifying DIRECT_BUYER after CONTACT + seeded known-lead evidence — matching the module’s saved-evidence-first design.

HOW_CONTACT_CHANNEL_DISCOVERY ran for all four (public-first; Findymail/Surfe refused).

---

## 5. Public HOW coverage

| Person | Email | Phone | LinkedIn |
|---|---|---|---|
| Jason Gooding | odesseytiming@gmail.com | 1-473-417-4386 | — |
| Filomena Marcelo | — | — | — |
| Luísa Marques | — | — | — |
| João Freire | — | — | — |
| Robert Govers | — | — | — |
| Cornelia Kern | events@ga-online.org | +49 (0)228 365 640 | — |
| Olaf Kelber | ga-secretary@ga-online.org | — | — |

---

## 6–7. Provider expenditure / remaining

- Spent: **$0.0160** (4 Exa calls × ~$0.004)
- Remaining under this GO: **$2.9840**
- No prior-authorization carry-forward used
- Findymail/Surfe: not called
- Firecrawl: 0

---

## 8. Existing evidence reused

- Phase-1 known leads (Jason Gooding, MeetingPoint partners, IPBA organizers, Kern/Kelber)
- V3 opportunity identities and demand evidence
- Public team/event pages as seed URLs
- Direct retrievals (20) alongside Exa

---

## 9. Newly finalizable opportunities

- `disc_69a16d5c89f9` (Rainbow Cup)
- `disc_e93837f2b647` (Meeting Point)
- `disc_f37cfea74d4f` (IPBA 2026)
- `disc_6280e5bb17d6` (GA Society)

**4 / 4** finalized eligible with ≥1 verified relevant named person.

---

## 10. Execution defects / limitations

- ALT ladder short-circuited after CONTACT + seed harvest produced a qualifying named person (by design of saved-evidence-first ranking; not a fixture fallback).
- Secondary initially duplicated primary (seeded-pool dedupe bug); corrected post-run to distinct verified secondaries from the same public team pages **without additional spend**.
- Exa spend was low because seeds + direct retrievals carried verification; CONTACT still issued one live Exa query per opportunity.
- HOW found channels for Rainbow Cup and GA Society; Meeting Point and IPBA remain WHO_COMPLETE without personal email.
- STAGED package `coverageOk=false` reflects secondary-attempt documentation gate on inherited prior rows — packages still `integrityOk`; **pointers unchanged**.

---

## 11. Scale recommendation

**SCALE_CAUTIOUSLY** — pilot produced 4/4 finalized named contacts with commercial relevance and chrome rejection working (e.g. Steve Millington). Expand next wave with the same employerOk gate, per-opp stop-loss, and explicit force-ladder mode when testing ALT paid strategies on opportunities that lack strong seeds.

---

## STAGED candidates (pointers unchanged)

| Hotel | Package ID | Pointer changed |
|---|---|---|
| LaLuna | `gdi_dh_contact_pilot4_recaokE8psIbuVeuH_20261124` | **false** |
| Memmo | `gdi_dh_contact_pilot4_rec1t35e3sPerzxYu_20261124` | **false** |

Publication IDs: `gdi_pubpkg_recaokE8psIbuVeuH_1bab3d428443bb95` · `gdi_pubpkg_rec1t35e3sPerzxYu_f05de16b6d44ca5b`

No CURRENT · no customer publication · historical V3 / best-contact / buyer-who-recovery snapshots preserved.

---

## Regression certification

- `test:gdi-buyer-alternative-person-research-v1` — CERTIFICATION_PASS (18)
- `test:gdi-who-how-contact-intelligence-restore-v1` — CERTIFICATION_PASS (10)
- `test:gdi-buyer-who-permanent-full-e2e-v1` — CERTIFICATION_PASS (16)

---

## Governance

- Live Exa only within new $3 GO (no prior carry-forward)
- No Findymail / Surfe / Airtable / CURRENT / auto STAGED pointer
- Historical packages preserved
- STOP after four-opportunity pilot
