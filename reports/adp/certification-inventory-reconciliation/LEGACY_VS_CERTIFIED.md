# Legacy vs Certified

## Intended rule (preserved)
A legacy period may pass deterministic QA today (**LEGACY_QA_PASS**) without becoming retroactively **CERTIFIED**.

## Disk note
Some pre-framework published manifests still carry `certificationStatus: "CERTIFIED"` without `globalCertificationEngineVersion`.  
Inventory treats these as **LEGACY_OFFICIAL** + **LEGACY_QA_PASS** (or LEGACY_UNCERTIFIED for publish math).  
**We do not overwrite historical manifests** in this task.

## Formal comparison
Only certification-era CERTIFIED periods are marked `formalComparisonUsable=YES`.
