# Compatibility / migration approach

1. **Preserve** all historical STAGED and CURRENT packages and pointers.
2. **Add** FQO as the deterministic gate for *new* local STAGED candidacy (`enforceFinalizedQualifiedOpportunity: true` on package quality gate when staging).
3. **Do not** delete or relabel `salesReady` / `SALES_READY` / disposition fields.
4. **Do not** silently promote research rows into customer CURRENT.
5. **Remove** Research from local presentation chrome; admin research remains URL-gated.
6. **Hilton** and other hotels missing STAGED show empty finalized inventory until a separate founder-approved STAGED activation of an FQO-certified package.
7. Open PRs that restore Research browsing (#590 / #591) should be closed or rewritten against this policy before merge.
