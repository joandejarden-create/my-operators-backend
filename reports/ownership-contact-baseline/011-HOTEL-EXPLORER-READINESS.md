# 011 — Hotel Explorer Readiness

```json
{
  "chain": "Hotel → PropCo → Sponsor → Contactable Org → Person → Email/Phone/Profile",
  "overall": "PARTIAL",
  "prevents_full_flow": [
    "Only 4 hotels in HOTEL_TO_OWNER",
    "hotel-ownership-intelligence.js hardcodes PropCo/Economic owner as Not yet verified",
    "Contact Intelligence runtime store empty — KGPV showcase only",
    "Demo fixtures bypass canonical CI store for display"
  ],
  "controls": {
    "Krystal Grand Puerto Vallarta": {
      "structure": "YES",
      "contactable_org": "YES",
      "people": "YES",
      "email": "YES (showcase)",
      "phone": "YES (showcase)",
      "profile": "PARTIAL",
      "provenance": "YES",
      "demo_fixture_dependency": "YES",
      "source": "CANONICAL map + DEMO showcase"
    },
    "Cambridge Beaches": {
      "structure": "YES",
      "contactable_org": "PARTIAL",
      "people": "YES (names)",
      "email": "NO",
      "phone": "PARTIAL (hotel phone in research)",
      "profile": "PARTIAL (LinkedIn)",
      "provenance": "YES",
      "demo_fixture_dependency": "YES",
      "source": "CANONICAL map + DEMO research"
    },
    "Sheraton Guadalajara Expo": {
      "structure": "YES",
      "contactable_org": "PARTIAL",
      "people": "YES",
      "email": "PARTIAL (operator Aimbridge — not owner)",
      "phone": "PARTIAL",
      "profile": "PARTIAL",
      "provenance": "YES",
      "demo_fixture_dependency": "YES",
      "source": "CANONICAL map + DEMO research"
    },
    "voco / Real Inn Cancún": {
      "structure": "PARTIAL (PropCo UNKNOWN in research)",
      "contactable_org": "PARTIAL (Alliance)",
      "people": "YES",
      "email": "NO",
      "phone": "NO",
      "profile": "PARTIAL",
      "provenance": "YES",
      "demo_fixture_dependency": "YES",
      "source": "CANONICAL map + DEMO research"
    }
  }
}
```
