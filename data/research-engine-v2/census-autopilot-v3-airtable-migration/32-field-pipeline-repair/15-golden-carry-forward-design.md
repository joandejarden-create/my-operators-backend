# Golden carry-forward

Problem: V1.2 geography completeness did not survive into V3 discovery→write.

Design: ONE canonical claim store per property_identity_id. Waves **upsert** claims; incomplete later objects must not erase prior verified claims (`mergeClaimStores`).
