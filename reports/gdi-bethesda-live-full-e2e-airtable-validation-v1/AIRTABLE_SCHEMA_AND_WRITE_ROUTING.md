# Airtable schema and write routing

Base: **AI Demand Positioning** (`appa2cE7FTRmIbB32`)

## Canonical tables (existing — no new tables invented)

| Table | ID | Identity |
| --- | --- | --- |
| Group Demand Opportunities | `tblRuReslJMwsfRQj` | opportunityId |
| Demand Generators | `tblykUVOjGVkawvdD` | Demand Generator ID |
| Demand Programs | `tblxm7wqfFNG37Z2m` | Program ID |
| Demand Generator Signals | `tblZ0cOM78uYMCTXx` | Demand Generator Signal ID |
| Hotel Demand Generator Fit | `tblqamXbIk9n6gXfV` | Hotel Generator Fit ID |
| Private Event Venues | `tblE5p4HjVHfcMDna` | Venue ID |
| Private Event Signals | `tbl5rSZxKv218GuOP` | Event Signal ID |
| Hotel Venue Fit | `tbluXqX7ecocCrdE1` | Hotel Venue Fit ID |
| GDI Research Targets | `tblVyuEf5vjooWDKX` | Target ID |
| GDI Research Runs | `tblzNMUIo2T6onaKH` | Run ID |
| GDI Research Target Runs | `tblSA0cFVplNfWMjp` | Target Run ID |
| GDI Hotel Commercial Profiles | `tbl8KSNUCRbIzEv15` | Profile ID |
| GDI Hotel Need Periods | `tbl50kj5XkGs22C6j` | Need Period ID |
| GDI Need-Date Campaign Runs | `tblghbxp4reaQWx2b` | Campaign ID |
| GDI Need Period Opportunity Matches | `tblOw5r7r95eQ1dPq` | Match Key |
| GDI Hotel Research Opportunities | `tblbkDj0vhQLfrssV` | compositeKey / (hotelId, opportunityId) |

## Founder-known destinations

- Hotel Research Opportunities: `tblbkDj0vhQLfrssV`
- Qualified GDO: `tblRuReslJMwsfRQj`

## Contact intelligence

Embedded on research payload + GDO typed primaryContact* columns; no separate Contacts table.

## Weekly Change

Storage: filesystem.hotel.opportunities, airtable.gdo.opportunityPayloadJson, airtable.gdo.firstDiscoveredAt, airtable.gdo.firstDiscoveredRunId, airtable.gdo.lastMaterialChangeAt, airtable.gdo.lastMaterialChangeRunId

## Hotel-specific feedback

Hotel-specific sales feedback remains filesystem feedback.json + Decision Outcomes; not shared across hotels.

## Approved Full E2E write tables

- Demand Generators
- Demand Programs
- Demand Generator Signals
- Group Demand Opportunities
- Private Event Venues
- Private Event Signals
- Hotel Venue Fit
- Hotel Demand Generator Fit
- GDI Research Runs
- GDI Research Targets
- GDI Hotel Research Opportunities

Production guard: `AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED`
