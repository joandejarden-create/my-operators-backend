# Contact relationship data review

## Existing

- Typed primary contact fields on GDO  
- Rich `primaryContact` object + multi-contact arrays in payload/FS  
- Commercial eligibility contact classes (DIRECT_BUYER, INTERMEDIARY, …)  
- Planner orgs → Demand Generators (+ optional Research Target if monitoring committed)

## Policy (preserve)

Verified named contact meaningfully connected to demand may qualify as commercial entry point without confirmed booking authority. Reject unrelated contacts and generic org-as-person.

## Recommendation

- Keep embedded contacts (no Contacts table now)  
- Populate `contactClass` / professional title in payload  
- Multiple contacts: lightweight JSON array, not new DB  
- No automatic paid enrichment
