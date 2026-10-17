# Release status

- Branch: `fix/admin-gdi-client-inventory`
- PR: https://github.com/joandejarden-create/my-operators-backend/pull/133
- Head: `aad5b378` (sealed-link code `583848bd`)
- Merge SHA: not merged. GitHub authentication still has to be restored by the founder before this PR can be pushed and merged. Do not recover the previously exposed credential from logs.
- Deployment ID: not deployed. This PR is not on the production Admin page.
- buildSha follow-up: `GET /health` returned `{ok:true}` and did not include `buildSha`. No production SHA was observed from the running service. Do not treat a deploy-record SHA as the serving revision until `/health` (or another runtime field) returns it after Railway SUCCESS.
- Nobu London Portman work remains in stash `preserve nobu london portman e2e wip — do not pop during admin client inventory`. It was not included in this branch.
