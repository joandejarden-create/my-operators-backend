# AI Visibility — Spanish Hospitality Prompt Style Guide

> **Status:** Guidance · production Spanish prompts authored in Phase 3A.9  
> **Audience:** Bilingual prompt authors and reviewers  
> **Related:** `BRAND_AI_VISIBILITY_PHASE_3A9_BILINGUAL_PROMPT_GOVERNANCE.md`, `language-dimension.js`, `semantic-pair.js`  
> **Note:** Methodological term is **Eligibility** (not Suitability). Do not put Eligibility jargon into natural owner prompt text.

---

## Purpose

Establish how future **Spanish** owner/developer intent prompts are written for Dealality AI Visibility monitoring in CALA (and Mexico), so they sound natural to bilingual hospitality professionals—not like machine-translated English.

This guide does **not** create prompts, run providers, or change Airtable.

---

## Core rules

1. **Natural bilingual CALA hospitality voice** — Write as a Spanish-speaking hotel owner, developer, or investor would ask a trusted advisor. Prefer idiomatic Spanish hospitality/real-estate register used in Mexico, Caribbean, and Latin America deal conversations.
2. **No literal machine translation** — Do not EN→ES line-by-line. Preserve the same **semantic decision** (via `semanticPairId`); allow natural wording differences.
3. **Do not over-translate** globally understood hotel-development terms when bilingual professionals keep them in English in speech and decks.
4. **Language ≠ geography** — `language=es` is linguistic only. Geography stays `CALA`, `Mexico`, etc. Never encode market as `es-MX` / `es-ES` in the language field.
5. **Semantic pairs, not clones** — EN and ES members of a pair share stakeholder, intent territory, geography scope, decision context, and peer-cohort intent. Prompt text may differ; meaning must not.
6. **No production Spanish library in this phase** — Style decisions only until bilingual prompt governance.

---

## Terminology

| Concept | Preferred Spanish / bilingual practice | Notes |
|---------|----------------------------------------|--------|
| Owner | **propietario** | Primary stakeholder voice for most Brand AI Visibility prompts. |
| Developer | **desarrollador** | Use when the asker is building or converting assets; pair with propietario when both apply. |
| Conversion | **conversión** | Natural and preferred; keep decision framing (brand conversion of an existing hotel). |
| Hotel brand | **marca hotelera** | Prefer over bare “marca” when ambiguity with consumer brands is possible. |
| Franchise | **franquicia** | Use where the commercial model is franchise; do not force HMA/franchise jargon where the intent is broader brand selection. |
| Soft brand | **soft brand** (keep EN) | Globally understood; optional gloss once as *marca soft / soft brand* only if clarity requires—do not invent awkward calques. |
| Collection | **colección** or **collection** | Either is acceptable; **colección** is natural in Spanish decks; EN *collection* is fine when naming a family (e.g. Autograph Collection). Prefer consistency within a semantic pair’s ES text. |
| Lifestyle | **lifestyle** (keep EN) | Industry positioning term; do not force *estilo de vida* as the primary label in chain-scale prompts. |
| Upper-upscale | **upper-upscale** (keep EN) | Chain-scale label retained in EN; Spanish can surround it (*segmento upper-upscale*). |
| Branded residences | **branded residences** (keep EN) | Widely used as-is; Spanish framing: *residencias de marca / branded residences*. Prefer EN term in the decision noun phrase when natural. |
| Mixed use | **uso mixto** or **mixed use** | Both acceptable; *uso mixto* is clear in Spanish development speech. |
| Positioning | **posicionamiento** | Natural for brand/segment positioning questions. |
| Operator | **operador** | Hotel operator / management company context. |
| Development | **desarrollo hotelero** | Prefer when meaning hotel development activity; bare *desarrollo* is OK if context is already hotels. |

### Keep-in-English shortlist (default)

Unless a specific prompt reads unnaturally, prefer leaving these in English inside Spanish sentences:

- soft brand  
- lifestyle  
- upper-upscale  
- branded residences  
- PIP (when used)  
- HMA (when the intent territory requires it; do not invent Spanish legalese)

### Prefer-Spanish shortlist (default)

- propietario, desarrollador  
- conversión  
- marca hotelera  
- franquicia  
- posicionamiento  
- operador  
- desarrollo hotelero  
- uso mixto (or mixed use)  
- colección (or collection, consistently)

---

## Voice examples (illustrative only — not production prompts)

**Avoid (literal):**  
“¿Qué soft brands debería considerar para una conversión upper-upscale en CALA?” *translated word-for-word from a stiff English template.*

**Prefer (natural):**  
A propietario/desarrollador asking which **marcas hoteleras** (including soft brand / colección options) fit a **conversión** in a named geography, with **posicionamiento** upper-upscale made explicit without sounding like a glossary dump.

Do not paste these as seed prompts; author under Phase 3A.7 governance with `language`, `semanticPairId`, and geography fields filled.

---

## Pairing with English

| Check | Rule |
|-------|------|
| Same decision? | Yes — same owner problem |
| Same intent territory? | Yes |
| Same geography scope? | Yes (language does not change market) |
| Identical wording? | No — natural ES, natural EN |
| Auto-translate? | Never |
| Cross-language metrics? | Never blend; compare only via future governed gap features |

---

## Out of scope

- Production Spanish prompt seeding  
- Provider runs  
- UI localization (monitoring language ≠ product UI language)  
- Locale tags as language values  
- “All Languages” blended metrics  

---

## Next phase

**Phase 3A.10 — Showcase monitoring dry-run** (after Founder review of Wave-1 prompt library and cost).
