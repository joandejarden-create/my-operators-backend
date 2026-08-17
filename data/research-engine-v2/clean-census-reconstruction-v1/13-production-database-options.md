# Production Database Options

## Recommendation: **Option B (aligned with existing design)**

Keep legacy `Hotel Census` as **quarantined reference**.

Grow **Verified Independent Hotel Census** (+ Candidates / Evidence) as the Dealality independent master — already proposed in `docs/verified-independent-hotel-census-schema.md`.

| Option | Pros | Cons |
|--------|------|------|
| **A. Retrofit provenance on existing Hotel Census** | Least table churn | Hard to prove non-reliance; STR fields remain entangled; high contamination risk |
| **B. New/verified independent master + quarantine legacy** | Clear lineage; matches existing independent census pipeline; safer product gates | Dual-read period; migration of product read paths |
| **C. Other** | — | Unnecessary complexity now |

**Do not migrate yet.** Pilot artifacts prove the research path first.
