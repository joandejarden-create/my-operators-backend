# Stage 8 — End-to-End Validation Plan

1. Create/update operator in My Operator (all tabs).
2. Save and refresh; values reload correctly.
3. Verify writes in Operator Setup tables (Master + 1:1 + child rows).
4. Verify Operator Explorer uses same values (no fallback/mock).
5. Verify Operator Capability Snapshot uses same values.
6. Verify Operator Alignment Snapshot uses same values.
7. Verify score breakdown consumes mapped operator fields.
8. Verify select/multi-select options validate and persist.
9. Verify blank fields degrade gracefully in downstream UI.
10. Verify one realistic profile supports end-to-end lifecycle.
