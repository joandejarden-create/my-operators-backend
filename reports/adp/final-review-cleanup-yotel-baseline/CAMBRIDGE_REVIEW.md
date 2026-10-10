# Cambridge Beaches Review

## Root cause
**FALSE_POSITIVE_REVIEW** / independent property-domain mapping

- Brand: Independent
- Official/owned domain: `cambridgebeaches.com` (property-owned)
- `officialBrandDomain`: null
- Validator incorrectly treated property domain as brand-host mismatch

## Fix applied
YES — skip `BRAND_DOMAIN_MISMATCH_SUSPECT` for Independent brands and property-owned official domains

## Rerun required
NO

## Post-fix certification
**CERTIFIED**
