# Local archive companions for tg-cli

Reconcile a tg-cli message cache with a second SQLite message store, or clone media already on disk. Both commands work offline with the Python standard library. Use Python 3.9+ and SQLite with JSON functions and aggregate filters. Media cloning needs macOS and source and destination media on the same APFS volume.

## Inputs

| Input | Expected layout |
| --- | --- |
| `--tg-db` | tg-cli SQLite file with `messages(platform, chat_id, msg_id, timestamp, content, raw_json)`. Required for reconciliation. |
| `--anti-db` | Local SQLite file with `messages(chat_id, msg_id, date, text, media_type, media_path)`. The copier uses only the IDs and path. Defaults to `$TGA_HOME/antidelete.db`. |
| `--media-dir` | Parent of the bare-ID and marked-ID media directories. Defaults to `$TGA_HOME/media`. Files sit directly in a peer directory. |
| `--tga-home` | Overrides `TGA_HOME`. Relative paths stored in the source resolve from this directory, even when the database or media directory is elsewhere. |

Supply a channel or supergroup ID (bare or Telegram's marked form) and an inclusive first message ID on every invocation. Use a cache where the bare ID identifies that channel unambiguously. Each source must have at most one row per peer and message ID within the selected platform.

## Populate the media source

For this fork's media collector, use a dedicated output directory. Set `CHAT_ID`
to the marked channel ID and `SINCE` to an ISO 8601 timestamp with a timezone.

```sh
tg sync-media --chat="$CHAT_ID" --since="$SINCE" --output="$TGA_HOME" --limit=100
```

The collector creates `$TGA_HOME/messages.db` and `media/<marked-id>/`.
Pass `--anti-db "$TGA_HOME/messages.db"` to the reconciliation and clone commands
when using this layout. The text cache supplied through `--tg-db` is a separate input.
Run the collector again with the same scope to advance its cursor and retry pending
downloads. The single-file limit is 64 MiB and each attachment gets up to three
ordinary download attempts. The JSON summary reports skipped and failed transfers.

## Run

From this directory, set `TGA_HOME`, `TG_DB`, `CHAT_ID` and `FIRST_MSG_ID` to local values. Use `--anti-db` and `--media-dir` for a non-default layout.

```sh
export TGA_HOME
set -- --chat-id "$CHAT_ID" --first-msg-id "$FIRST_MSG_ID"
python3 -B archive_reconcile.py "$@" --tg-db "$TG_DB"
python3 -B archive_reconcile.py "$@" --status
python3 -B archive_media_copy.py "$@" --init --limit 32
python3 -B archive_media_copy.py "$@" --limit 32
```

The archive is `$TGA_HOME/state/<marked-id>/<first-id>/archive.sqlite3`. Use `--output FILE` to override it, including for standalone `--status`. The first import covers the selected range. Later runs read one forward page and one rotating historical page per source and ID encoding. Use `--batch-size N` to set the page size or `--full` to revisit every row. The `messages` view summarizes coverage. `observations` preserves the latest text, path and media state seen for each source row, including observations whose source row later disappears.

Clones and `manifest.sqlite3` live in `$TGA_HOME/media-backup/<marked-id>/<first-id>/`. Override the parent with `--backup-dir DIR`. A verified database path takes precedence over a unique numbered orphan. Initialization inventories the directories. Later invocations drain the queue, check new rows and repeat the inventory daily. The limit bounds copy attempts. A failed attempt returns a nonzero exit code and becomes eligible again after five minutes. A previously created clone at the expected name is accepted after byte comparison and inode checks. Conflicting files are preserved.

Each output binds its cursors to its scope, input database paths and file identities, and media configuration. Use a fresh output when replacing a database. Each source opens with SQLite `mode=ro` and `query_only=ON` in its own read transaction. The first query sees committed rows, including WAL commits. Later queries on that connection keep the same snapshot while WAL writers can commit new rows for the next run. SQLite manages its usual locks and WAL shared memory. Incremental polling reads the indexed message ranges directly. For efficient seeks, index the two sources on `(platform, chat_id, msg_id)` and `(chat_id, msg_id)`, respectively. Keep the output directory private.

Run the synthetic tests with `python3 -B -m unittest discover -s . -v`. The clone tests run on macOS with an APFS temporary directory.
