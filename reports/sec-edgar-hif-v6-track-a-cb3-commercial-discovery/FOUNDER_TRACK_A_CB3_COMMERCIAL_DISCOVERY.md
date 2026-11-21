# HIF Track A — CB3 Commercial Discovery Scale & Qualification Depth

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research spend:** **$0**  
**Branch:** `feat/hif-v6-track-a-cb2-commercial-discovery` (preserves PR **#544**)  
**Baseline:** `origin/main` `81425a067`  
**E2E:** **PERSISTENCE_VALIDATED** (apply + idempotent) — not FULL_E2E / not CUSTOMER_PUBLISHED

---

## Checkpoint

| Item | Result |
|------|--------|
| PR #544 | **OPEN** — preserved; CB3 appended on same branch |
| CB2 Airtable writes | Preserved (NOOP on prior IDs) |
| Local main / other WTs | **Not modified** |
| Track B | Active opportunity inventory empty; SEC expirations consumed as contractual signals only |
| Track C | Handoff examples classified COMMITTED — no duplicate opportunity writes |

---

## Research volume

| Metric | Value |
|--------|------:|
| Distinct hotels / sites investigated | **133** |
| Volume target | 100 |
| Stop reason | **RESEARCH_VOLUME_TARGET_MET** |
| Cost | **$0** |

See `INVESTIGATED_HOTELS.json`.

---

## Qualification taxonomy (enhancement)

| Class | Examples |
|-------|----------|
| Open competitive | Hotel Oromana; Aena airport hotel sites |
| Private probable access | Shorebreak; Courtyard Denver; **Embassy One Bengaluru MCT** |
| Preferred-brand | **Legacy Green Bay → Marriott soft brand** (reclassified out of first-100) |
| Intra-brand-family | Sheraton Raleigh→Westin; Houston Grand→Luxury Collection |
| Committed | Legend Minot→Fairfield; Kissimmee→City Express; Custer→Xanterra |
| Award-pending | Marriott Marquis Chicago; Honey Creek |
| Contractual change only | Wave34 SEC expirations; DRH portfolio scan (non-decision hotels) |
| Completed / historical | Garysburg Quality Inn; Belvedere Pullman; Mont-Royal deflag |

**Rule applied:** unnamed soft-brand collection does **not** keep a preferred-family decision as open competitive BCT.

---

## First-100

| Metric | Count |
|--------|------:|
| **First-100** | **5 / 100** |
| BCT | 3 (Lakeville, Shorebreak, Courtyard Denver) |
| MCT | 2 (Trinidad, **Embassy One Bengaluru**) |
| Δ vs CB2b | **0** (Legacy out −1; Embassy in +1) |
| Yield / 100 hotels researched | **3.76** |
| Outreach-ready | **0** |

---

## Expanded pipeline (persist batch)

| Opportunity | Type | Class |
|-------------|------|-------|
| Embassy One Bengaluru | MCT | Private open — **first-100** |
| Hotel Oromana (ES) | Concession | Open (appeal caveat) |
| Honey Creek Resort (IA) | Concession | Award-pending |
| Marriott Marquis Chicago | MCT | Award-pending |
| Aena Recife / Maceió / João Pessoa / Aracaju | Development | Open |

---

## Airtable

| Action | Count |
|--------|------:|
| CB3 creates | **16** (8 inv + 8 findings) |
| First-apply NOOPs | **2** (Legacy reclass preserve) |
| Idempotent retry | **18** NOOPs |
| Conflicts | **0** |
| HPC / schema | **0** |
| Publication | CUSTOMER_BLOCKED |

Created IDs: Embassy `recoF4KfmJj2LYRET` / `recQmiSbQOyMmzalO` · Oromana `recDt9UuO1yVK4Ceq` / `recBhczjMLg2pkd9Y` · Honey Creek `rectfAr9PFXtqB49T` / `reciwGluTaai3C7kC` · Marquis Chicago `recSAf7iOCOeS7izK` / `recAeqiw7Lf5qT0nO` · Aena Recife `recvxXotqlPXZKpMB` / `recZ0fzCjz5FENdqE` · Maceió `recLBxgh25p0Iy5gM` / `reci1LKvZlHnrVgBK` · João Pessoa `rec71fqUIlHfkGNBY` / `recew4iYhb6d1V8ui` · Aracaju `recGHTmIv5TkL6gde` / `recV8b8rTpKKZvwiF`

---

## Tests / PR

- `test:hif-track-a-continuous-foundation`
- `test:hif-foundation-persistence`
- `test:hif-v7-commercial-dimensions`
- PR **#544** for Integration Controller only

**CUSTOMER_BLOCKED**
