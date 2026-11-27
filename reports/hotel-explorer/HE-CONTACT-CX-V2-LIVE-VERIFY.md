# HE Contact Intelligence CX V2 — Live verify

- Surface: `http://localhost:8080/hotel-explorer-preview.html?hotelId=recLuxvwwxID7U2B8&mode=proposed&hotelExplorer=recLuxvwwxID7U2B8&hexTab=contacts`
- Hotel header: **Bethesda Marriott** (not HPC id)
- Ownership label: **Ownership organization (pending certification): Mullen Real Estate Capital** (not “Owner: …”)
- Observed sections: Hotel Leadership (empty hint), Sales & Commercial Leadership (empty hint), Ownership & Asset Management (3 people)
- People: Katherine Button, Patrick Reece, Jeff Mullen — company routes only; **Hotel responsibility not confirmed**; Find Contact Details present; no invented direct email/phone
- Missing (not fabricated): property GM, DOSM / Sales, Revenue leadership
- Uniform full-width `hci-person` cards (1312px); Business purpose filter; no Recommended First Contact / Suggested Contact hierarchy
- API evidence: `HE-CONTACT-CX-V2-BETHESDA-API-EVIDENCE.json`
- Screenshot: `HE-CONTACT-CX-V2-BETHESDA-SCREENSHOT.png`
- Findymail: absent/OFF — `HE-FINDYMAIL-IMPLEMENTATION-AUDIT-V1.md`
- Persistence: designed only — `HE-CONTACT-CX-V2-PERSISTENCE-READINESS.md` (`PERSISTENCE_DESIGNED`)
- Surfe: explicit-click, session-only, not persisted
- Radar/HE parity: shared `HotelContactIntelligence` renderer (`data-hci-cx=v2`)
- Acceptance: **PASS** for CX presentation. Direct-channel recovery remains incomplete until verified personal channels exist (no invention).
