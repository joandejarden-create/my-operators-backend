# Permanent test results

```bash
node scripts/test-gdi-finalized-qualified-opportunity-v1.mjs
```

**Result:** PASS (19 checks)

Covered cases:

- Valid named buyer + verified email  
- Valid alternative + LinkedIn only  
- Named contact, no person-specific channel  
- Generic org inbox only  
- Org switchboard only  
- Missing hotel fit  
- Historical without future pathway  
- Competitive without incumbent pathway  
- Competitive with incumbent + overflow  
- Sales Ready false does **not** block FQO  
- Founder hold / reject binding  
- Quality gate opt-in FQO  
- Legacy packages not retroactively broken  
- Historical STAGED/CURRENT pointers preserved  
- No Research tab in `public/app.html`
