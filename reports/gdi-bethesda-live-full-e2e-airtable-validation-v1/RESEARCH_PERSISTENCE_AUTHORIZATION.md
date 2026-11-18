# Authorization B — Production Airtable research persistence

| Field | Value |
| --- | --- |
| Token | `gdi-bethesda-14segment-research-persistence-v1` |
| Hotel scope | Bethesda Marriott (`recLuxvwwxID7U2B8`) + shared identities derived from this run |
| Destination | GDI Hotel Research Opportunities (`tblbkDj0vhQLfrssV`) |
| Env gate | `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1` |
| GDO mutations | SEPARATELY_GATED |
| Publication | FORBIDDEN_IN_THIS_BENCHMARK |
| Status | **PENDING_FOUNDER_APPROVAL** |

## Required conjunction

1. `authorizeResearchPersistence=true`
2. `exactWriteSetAuthorization=gdi-bethesda-14segment-research-persistence-v1` (or successor write-set id)
3. `applyResearchPersistence=true`
4. `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1`
5. Exact `EXACT_PRODUCTION_WRITE_MANIFEST.json` founder-approved after research dry-run

Paid discovery authorization alone **never** unlocks Airtable mutation.

**Do not approve blanket rewrite of 87 Bethesda or 687 portfolio research rows.**
