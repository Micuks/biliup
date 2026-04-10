# Local Maintenance Notes

This repository is no longer a clean upstream checkout. It carries production patches used by
`/mnt/data1/wuql/.local/bin/biliup` on the recorder host.

## Current Patch Stack

Base upstream:
- `d666dc7` `New Configuration File Parameters (#1600)`

Local commits currently required in production:
- `5c3911c` `fix(upload): guard title length and split oversized submissions`
- `145af8b` `fix(huya): dedupe repeated segment uploads`
- `f0d11b7` `fix(upload): reject segments without video streams`

## Branches

Recommended local layout:
- `master`: mirror of upstream `biliup/biliup`
- `wuql/prod`: production-ready branch with local patch stack
- topic branches: short-lived branches for single fixes

Current working branch:
- `fix/upload-title-and-part-limit`

## Remotes

The repository should eventually use:
- `origin`: personal fork, for example `git@github.com:<user>/biliup.git`
- `upstream`: official upstream `https://github.com/biliup/biliup.git`

Suggested commands after the fork exists:

```bash
git remote rename origin upstream
git remote add origin git@github.com:<user>/biliup.git
git fetch --all --prune
git branch -u upstream/master master
git push -u origin wuql/prod
git push origin --tags
```

## Release Flow

1. Develop each fix on a topic branch.
2. Validate with `cargo check -p biliup-cli` and targeted tests.
3. Merge or fast-forward into `wuql/prod`.
4. Tag deployable revisions, for example `wuql-2026-04-10`.
5. Install locally with:

```bash
uv tool install --force /mnt/data1/wuql/services/biliup-src
```

6. Restart `biliup` only when active streams are drained.

## Upstream Sync Flow

1. `git fetch upstream`
2. Update local `master` from `upstream/master`
3. Rebase `wuql/prod` onto the new `master`, or replay commits one by one
4. Re-run validation
5. Deploy and tag

## Operational Caveat

Installing a new build into the `uv` tool does not update an already-running `biliup server`
process. A successful restart is required before runtime behavior changes.
