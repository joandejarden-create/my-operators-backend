# Release status

- Branch: `fix/admin-gdi-client-inventory`
- PR: https://github.com/joandejarden-create/my-operators-backend/pull/133
- Head: `be598155`
- Merge SHA: not merged. `gh` is not logged in, and the merge command was blocked pending approval.
- Deployment ID: not deployed. Production `/health` is `{ok:true}` with no build SHA. Production Admin script still serves the pre-change `Open GDI` gate until this PR is merged and `npm run deploy:production` runs from clean `main`.
- Nobu London Portman work remains in stash `preserve nobu london portman e2e wip — do not pop during admin client inventory`. It was not included in this branch.
