# Structured Output Schema

`tg-cli` uses a shared agent-friendly envelope for machine-readable output.

## Success

```yaml
ok: true
schema_version: "1"
data: ...
```

## Error

```yaml
ok: false
schema_version: "1"
error:
  code: chat_not_found
  message: Chat 'foo' not found in database.
```

## Notes

- `--yaml` and `--json` both use this envelope
- non-TTY stdout defaults to YAML
- query commands usually return lists or dicts inside `data`
- `status` returns `data.authenticated` plus `data.user`
- `whoami` returns `data.user`

## Selected-chat sync

`tg sync-many --json` returns one entry per request in `data.results`, in input order.
Each entry includes the requested chat, the resolved numeric chat ID when available,
the number of inserted messages, and a status. A scheduler should check each status.
A successful poll can insert zero messages. A rate-limited entry includes
`retry_after_seconds`.
