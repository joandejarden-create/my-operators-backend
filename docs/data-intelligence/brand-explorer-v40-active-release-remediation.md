# v40 Brand Explorer Active Release Remediation

Generic batch remediation for Everhome, Kimpton, and Radisson Individuals.
Removes LOI/FDD/Item 19/fee-stack/net-contribution/URL language from owner-facing Presentation copy.

```bash
npm run brand-explorer-v40-active-release-remediation -- --brands everhome-suites,kimpton,radisson-individuals-by-choice --dry-run
```

## Rules
- Dry-run by default
- No active-profile approval
- No Company Validated / Source Library / Registry / image-field writes
- No incomplete brand unlock
- Founder review + active approval remain required after copy scrub

## Property examples
Minimum 3 visible with imageUrl. Extras allowed unless duplicates (then Do Not Display hide).