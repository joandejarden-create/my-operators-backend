# Moxy Hotels — Parent-Platform Allowlist Diagnosis

## Verdict: **B — validator false positive**

Moxy Hotels is Marriott-affiliated. Owner-facing copy correctly references Marriott Bonvoy as parent-platform loyalty context.
The quality-audit wrong-brand marker only exempted slugs containing `marriott` (plus a few soft brands). `moxy-hotels` does not include `marriott`, so valid Bonvoy references were flagged as carryover.

## Fix (targeted; not global disable)

1. Add `moxy` to Marriott Bonvoy `unlessSlugIncludes`.
2. Add `PARENT_PLATFORM_LOYALTY_SLUG_EXEMPTIONS["moxy-hotels"] = ["marriott"]`.
3. Also honor Brand Basics `parentCompany` containing Marriott for the Bonvoy marker.

## Not done

- Did not scrub valid Marriott Bonvoy references from Moxy copy.
- Did not allow Marriott Bonvoy for unrelated brands.
- Did not disable wrong-brand detection globally.

