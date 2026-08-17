# HBX Support — Room Count / Content Clarification Pack

Generated: 2026-08-09T14:45:28.303Z
Objective: hbx-content-inventory-and-rooms-field-hunt-v1

## Context
Dealality is evaluating Hotelbeds Content API + Booking API as a **read-only** enrichment source for Hotel Property Census.
We need confirmation on whether a **true hotel-level total Rooms / Keys** field exists anywhere in the API surface.

## What we already observed
- Content `hotels` / `hotels/{code}/details` expose `rooms[]` as a **room-type catalog** (codes, descriptions, min/max pax, room facilities).
- We do **not** treat `rooms.length` as total keys.
- Facility `number` appears to be amenity/bed quantity, not hotel keys.
- Booking Availability exposes `rates[].allotment` (date/contract allotment; often sentinel 999) and `rates[].rooms` (requested occupancy rooms) — **not** property keys.
- Official Content API docs describe room occupancy mins/maxs and facilities; they do **not** document `totalRooms` / `numberOfRooms` as hotel-level totals.
- Hunt status this run: **unsupported_or_needs_hbx_support_confirmation**

## Questions for Hotelbeds support
1. Does Content API (or any other Hotelbeds API we are licensed for) expose a **hotel-level total number of rooms / keys**?
2. If yes: exact endpoint, JSON/XML path, field name, update cadence, and whether values are contractual inventory vs physical keys?
3. If no: is Giata or another linked dataset the intended source for physical room counts?
4. Can `facilities[].number` ever represent total hotel rooms for any facility code/group? Which codes?
5. Are description free-text room-count claims (e.g. "120 rooms") considered licensed/reliable for storage?
6. Confirm license terms for permanent storage of: address, phone, website, coordinates, facilities, descriptions, images, room-type catalogs.
7. Confirm whether Cache API / content dump includes any room-count field not present in REST Content API.
8. For CALA countries (MX, DO, CO, CR, PA), is content completeness equal to other regions for identity/location/contact fields?

## Sample hotels for support to inspect
- HBX 4000 / Census reckoKN1STiFnzQgo: "Grand Fiesta Americana Coral Beach All Inclusive" — Census Rooms/Keys=602 (High)
- HBX 4893 / Census reciMoHhYpXDoYPvD: "Grand Velas Riviera Nayarit" — Census Rooms/Keys=267 (High)
- HBX 5303 / Census recCpewB5mIRsIkNz: "Ibis Monterrey Valle" — Census Rooms/Keys=105 (High)
- HBX 5308 / Census recYwdabT0eURnQpb: "Galeria Plaza Reforma" — Census Rooms/Keys=436 (High)
- HBX 6855 / Census recH7ilH8wXpfDzmC: "Las Brisas Ixtapa" — Census Rooms/Keys=416 (High)
- HBX 6922 / Census rec0qmO7Xj7uyjWLZ: "Comfort Inn Queretaro Querétaro" — Census Rooms/Keys=70 (Medium)
- HBX 7789 / Census rec3VtNHyHrW0xDk8: "Radisson Tapatio Guadalajara" — Census Rooms/Keys=127 (Medium)
- HBX 8201 / Census rec4oP5nj76kK9D0s: "Las Brisas Huatulco" — Census Rooms/Keys=494 (High)
- HBX 20078 / Census reciRjhv1WdCx9Mrk: "Las Brisas Acapulco" — Census Rooms/Keys=251 (High)
- HBX 25282 / Census recc9EZokbFZjwtez: "Novotel Mexico City Santa Fe" — Census Rooms/Keys=1024 (High)

## Our current policy (until confirmed)
- **Do not write** Rooms / Keys from HBX.
- Do not use `rooms.length`, occupancy mins/maxs, allotment, or booking availability as total keys.
- Identity / location / PHONEHOTEL / website remain the primary write candidates after policy review.
