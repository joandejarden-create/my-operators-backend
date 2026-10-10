# Group Motion Persistence

## Bug found: YES

`groupMotionType` / `groupMotionEvidence` were not stamped on campaign children.
Packet quality previously inferred motion from prose / `opportunityType` lifecycle labels.

## Fix

- Map `travelingEntityType/Evidence/Confidence` → `groupMotionType/Evidence/Confidence` on child build + final-mile reconcile.
- Packet schema prefers structured traveling-entity stamps; ignores `FUTURE_CYCLE` as motion.

## Counts

- Group motion structured BEFORE: 11
- Group motion structured AFTER: 11
