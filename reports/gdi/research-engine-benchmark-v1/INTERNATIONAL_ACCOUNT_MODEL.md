# International Account Model Verdict

## Does current GDI incorrectly require ultimate end-client identity?
**PARTIAL**

### Evidence
1. `missing-pillar-audit.js` `isOrganizerShell` regex treats `pco|dmc` names as organizer shells — **downgrades intermediary identity** even when lodging control may exist.
2. Mallorca ACCOUNT_FIRST = 0; CONTROLLER_FIRST empty; child named accounts = 0 despite Golf Planet Holidays having confirmed lodging package control + dated Sheraton motion.
3. Yield forensic: GPH is the only Complete Plausible on Sheraton — still 0 Valid Watch / 0 Ready because other pillars fail, **and** account identity was not promoted as named commercial account in spine outputs.
4. US Bethesda Ready=28 shows end-client/assoc public identity is abundant in US — international markets rely more on intermediary buyers.

## Is that hurting international yield?
**YES** (account pillar / named-account promotion), **but not the sole Ready blocker**.

Even with intermediary-as-account accepted:
- Golf Planet Holidays would improve **NAMED_COMMERCIAL_ACCOUNT** counts.
- Would **not** auto-create Ready without contact path, decision window, Complete Strong, external-demand checks.

## Counts (audit, not production change)
| Metric | Count |
|---|---|
| Candidates lost ONLY because intermediary rejected as commercial account | **1–3** (GPH primary; YGT/GreenGolf conditional) |
| That would pass **account rule only** with new rule | **1 solid (GPH Sheraton)** + **2 conditional catalogs** |
| That would become Valid Watch solely from account rule | **0** |
| That would become Ready solely from account rule | **0** |
| That would become false positives if motion not required | **3+ DMCs** (LifeXperiences, Tuset, Insiders) |

## Recommended canonical rule (proposal — do not ship without regression)
```
NAMED_COMMERCIAL_ACCOUNT may equal an intermediary
(DMC / PCO / golf tour operator / sports travel / incentive / housing company / event agency)
ONLY IF ALL of:
1. lodging-buying OR lodging-control authority is evidenced
2. a NAMED future group/travel program/departure exists (not capability brochure)
3. decision window or booking cycle is identifiable
4. incremental room demand thesis exists
5. reachable buyer/controller path exists OR explicit next-ask to obtain it
6. target hotel fit is non-contradicted
```

**Ready/Watch standards unchanged.** Intermediary without named future movement = NOT an opportunity.

## Bethesda control
Applying the rule must produce **0** new Ready from bare DMC/incentive listings. Ready remains **28**.
