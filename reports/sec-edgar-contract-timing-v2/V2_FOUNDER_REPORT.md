# Hotel Contract Timing Intelligence V2 — Provider Pilot

**Stage:** full · **Mode:** APPLY · **Publication:** CUSTOMER_BLOCKED  
**Ceiling:** $25 · **Spent:** $0.024 (Exa paid; SEC-API.io subscription calls=30)

## Provider auth
- Exa: OK
- SEC-API.io: OK

## Baseline regression
- JPMCC `0001539497-20-000579` known 57 dates · observed this run: **57**
- Excluded from incremental yield: **yes**

## Incremental yield (new unique dates)

| Provider | Unique dates | Dates/100 docs | Excl. top deal /100 |
|---|---:|---:|---:|
| SEC-API.io | 19 | 9.45 | 4.48 |
| Exa | 0 | 0 | 0 |
| Combined (deduped) | 19 | 9.5 | 4.5 |
| Exa incremental vs SEC-API | 0 | — | — |

- Documents OK (excl. baseline): **200**
- Horizons 6/12/24/36m: 0 / 0 / 1 / 4
- Historical needing revalidation: **8**
- Cost per new date: **$0.0013**

## Persistence
- Created: 0 · Read-back OK: 18 · Dupes prevented: 36

## Scalability
MODERATE — incremental dates found; expand period/form partitions and high-yield CMBS issuers.

Recommended: Primary: SEC-API.io full-text for CMBS 424B Franchise Expiration columns → free SEC document fetch → existing HTML extractor. Secondary: Exa for non-SEC renewals/reflags (leads until verified). Keep free EDGAR for retrieval.

## Examples
1. **Hampton Inn Joplin** — FRANCHISE_EXPIRATION 2033-01-31 · UBS Commercial Mortgage Securitization Corp. (Filer) · SEC_API_IO
2. **Hampton Inn Marion** — FRANCHISE_EXPIRATION 2033-02-28 · UBS Commercial Mortgage Securitization Corp. (Filer) · SEC_API_IO
3. **Hilton Garden Inn Joplin** — FRANCHISE_EXPIRATION 2026-10-31 · UBS Commercial Mortgage Securitization Corp. (Filer) · SEC_API_IO
4. **Hampton Inn Mount Vernon** — FRANCHISE_EXPIRATION 2020-10-31 · UBS Commercial Mortgage Securitization Corp. (Filer) · SEC_API_IO
5. **Hampton Inn Anderson** — FRANCHISE_EXPIRATION 2020-05-31 · UBS Commercial Mortgage Securitization Corp. (Filer) · SEC_API_IO
