# HE Contact Intelligence CX V2 — Live verify

- Surface: `http://localhost:8080/hotel-explorer-preview.html?hotelId=recLuxvwwxID7U2B8&mode=proposed&hotelExplorer=recLuxvwwxID7U2B8&hexTab=contacts`
- Hotel: Bethesda Marriott
- Observed: purpose section **Ownership & Asset Management**; Business purpose filter; uniform full-width `hci-person` cards (1312px); Company email/phone/website labels (not Direct); Find Contact Details; Details collapsed; no Recommended First Contact
- API evidence: `HE-CONTACT-CX-V2-BETHESDA-API-EVIDENCE.json`
- Screenshot: `HE-CONTACT-CX-V2-BETHESDA-SCREENSHOT.png`
- Radar/HE parity: shared `HotelContactIntelligence` renderer (`data-hci-cx=v2`)
- Acceptance: **PASS** (founder-preview HE Contact tab). Authenticated Radar iframe uses the same renderer; popup open was not required beyond shared-shell gate.
