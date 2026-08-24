# ASMR One

Batch-download your [asmr.one](https://asmr.one) favorites using the site's official per-work download URLs.

Nix is required for direct execution. The script's Nix shebang provides Python
and the `blake3` module automatically.

## Install

```bash
chmod +x asmr_one.py
# optional
ln -sf "$(pwd)/asmr_one.py" ~/.local/bin/asmr-one
```

## Usage

```bash
# token goes to ~/.config/asmr-one/token.json (0600)
./asmr_one.py login
# or: ASMR_NAME=... ASMR_PASSWORD=... ./asmr_one.py login

# one-shot automatic login; the resulting token is not saved
ASMR_NAME=... ASMR_PASSWORD=... ./asmr_one.py list
ASMR_NAME=... ASMR_PASSWORD=... ./asmr_one.py download --out ~/Music/asmr.one

./asmr_one.py whoami
./asmr_one.py playlists
./asmr_one.py list
./asmr_one.py list --playlist liked
./asmr_one.py list --playlist marked --playlist "My playlist"
./asmr_one.py download --out ~/Music/asmr.one
./asmr_one.py download --playlist PLAYLIST_ID --out ~/Music/asmr.one
./asmr_one.py download --dry-run --limit 3
./asmr_one.py download --verify --playlist liked
./asmr_one.py download --work RJ01657200
```

By default, `list` and `download` combine every playlist shown by the site's
**All** playlist filter, preserving playlist order and downloading duplicate works
only once. Run `playlists` to see each playlist's ID, name, and work count. Use a
repeatable `--playlist` with an ID, exact name, `liked`, or `marked` to narrow the
collection.

Default download is **every file in the work** (wav/mp3/video/subs, all
language folders) so high-res is kept. Collection pages, work information,
track lists, per-file local checks, requested hashes, downloads, and checksum
manifest writes all share one dynamic global task queue. `--jobs N` is the
process-wide limit: as soon as one task finishes, its slot can run any ready task
from the same or another work. A one-file work therefore does not leave the
remaining workers idle while other works are waiting.

Selected files are classified as `valid`, `adopt`, `missing`, `partial`,
`corrupt`, or `stale`; only files that need work are downloaded. A failure in one
file is reported without cancelling its sibling files or other works. Structural
failures such as an unavailable work-info or track-list request fail that work.
Each non-dry-run work holds an advisory `.asmr-one.lock` for its whole operation,
so two downloader processes cannot update the same work directory concurrently.
Lock contention uses the same delayed retry schedule as transient network errors.

Each work directory has a visible `checksums.json` containing BLAKE3 digests and
the associated remote identity, size, and local mtime. Existing files with a
matching checksum record use a fast size/mtime check. Existing legacy files whose
size matches the remote file are hashed once and adopted without downloading.
Use `--verify` to recompute BLAKE3 for every selected existing file. The remote
service does not publish a cryptographic content checksum, so adopting a legacy
file establishes a trust-on-first-use baseline rather than proving its earlier
contents. The API `hash` field is treated only as a stable remote-file identity.
Hash tasks run in the same worker pool as other tasks; each BLAKE3 instance uses
one native thread, so the effective concurrency remains bounded by `--jobs`.

Interrupted downloads use a `.part` data file plus an atomic `.part.json` state
file. The state records the remote identity, committed byte count, and BLAKE3 of
the committed prefix. Progress is flushed and checkpointed every 64 MiB and again
at EOF or a short/failed read. On restart, the prefix is rehashed before a Range
request is allowed; uncheckpointed tail bytes are discarded. A legacy, malformed,
mutated, or identity-mismatched partial restarts safely from zero. Resume requires
either the matching API `hash` identity (with unchanged source path and size) or a
strong HTTP ETag. A stable API identity remains resumable if only the CDN URL
pathname changes. Strong ETags are sent back with `If-Range`.

A resumed response must have a matching `Content-Range` start and total, and its
body is checked against the remaining range length rather than the whole-file
length. A server that ignores or mishandles the Range request triggers a safe full
download. A genuinely short body retains the newly received bytes for the next
attempt. The old destination remains in place until the replacement is completely
validated and atomically installed.

During `download`, transient transport failures and HTTP 408, 425, 429, and 5xx
responses are retried after 1 minute, 5 minutes, 30 minutes, 4 hours, and 1 day.
The longest server `Retry-After` seen across mirrors wins, capped at 1 day. File
retries fetch a fresh track list and signed download URL first, while keeping the
local destination chosen from the initial track snapshot. Waiting retries do not
occupy a `--jobs` worker and, when due, return at the end of the ready queue so
later works can continue. Active work states (and therefore held lock descriptors)
are capped at twice `--jobs` even during a widespread outage. Invalid
successful-response payloads and permanent HTTP errors fail immediately; after
the five delayed retries, the final transient failure is reported once. `--dry-run`
performs the same local classification (and requested verification) but creates
neither files nor locks.

Filters (opt-in):

- `--format all|mp3|wav|flac|best` — `all` is the default and keeps every
  file; the other values filter audio formats
- `--ja-only` — Japanese edition only
- `--no-subs` — drop recognized subtitle files
- `--jobs 8` — eight global workers for discovery, local checks, hashes, and downloads
- `--verify` — fully hash selected existing files before filtering downloads
- `--source auto|playlists|favorites|review` — `auto` is playlists; `favorites`
  is a compatibility alias; `review` selects the legacy review list explicitly

Auth priority is `ASMR_TOKEN`, the saved token, then automatic login with both
`ASMR_NAME` and `ASMR_PASSWORD`. Automatic login keeps its token in memory only;
run `login` explicitly to save it. Downloads using `--work` do not need collection
authentication and therefore do not trigger automatic login. Do not paste the
password or token into a shared channel. Set `ASMR_ONE_HOME` to override the
default `~/.config/asmr-one` configuration directory.
