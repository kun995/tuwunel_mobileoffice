# Workspace Statistics API

## Endpoint

```http
GET /_matrix/client/unstable/org.tuwunel/workspaces/{workspace_id}/stats
Authorization: Bearer <admin_access_token>
```

The endpoint requires an ordinary Matrix access token belonging to a user that
is configured as a Tuwunel admin. It is intended for checking whether a
workspace has indexed users, rooms, or timeline data.

Example:

```bash
curl --location \
  'https://chat-moffice.vnptit.vn/_matrix/client/unstable/org.tuwunel/workspaces/04c644c3-b41c-4b76-92bb-10ddca775434/stats' \
  --header 'Authorization: Bearer <admin_access_token>'
```

## Response

```json
{
  "workspace_id": "04c644c3-b41c-4b76-92bb-10ddca775434",
  "has_data": true,
  "has_message_data": true,
  "indexed_user_count": 12,
  "active_user_count": 12,
  "stale_user_count": 0,
  "room_count": 4,
  "active_room_count": 4,
  "stale_room_count": 0,
  "rooms_with_messages": 3,
  "timeline_event_count": 248,
  "message_count": 91,
  "last_data_ts": 1783945200123,
  "last_data_at": "2026-07-13T12:20:00.123Z",
  "last_message_ts": 1783945200123,
  "last_message_at": "2026-07-13T12:20:00.123Z",
  "complete": true,
  "scan_errors": []
}
```

Field semantics:

- `has_data`: true when at least one user mapping, room mapping, or timeline
  event exists. Use `has_message_data` to check for actual chat messages.
- `indexed_user_count`: number of user IDs in the workspace mapping index.
- `active_user_count`: indexed users that still exist in Tuwunel.
- `stale_user_count`: indexed users whose user record no longer exists.
- `room_count`: number of room IDs in the workspace mapping index.
- `active_room_count`: mapped rooms with stored timeline data.
- `stale_room_count`: mapped room IDs without stored room data.
- `rooms_with_messages`: active rooms containing at least one
  chat message event (`m.room.message` or `m.room.encrypted`).
- `timeline_event_count`: all stored timeline events in active mapped rooms,
  including state, membership, call, reminder, and message events.
- `message_count`: stored chat message events (`m.room.message` and
  `m.room.encrypted`), including text and file messages.
- `last_data_ts` / `last_data_at`: latest `origin_server_ts` among all stored
  timeline events. Values are milliseconds since Unix epoch and UTC RFC3339.
- `last_message_ts` / `last_message_at`: latest timestamp among only
  chat message events (`m.room.message` or `m.room.encrypted`).
- `complete`: false when at least one room could not be scanned completely.
- `scan_errors`: room IDs and errors for incomplete room scans. Other room
  statistics are still returned.

`last_data_at` and `last_message_at` are `null` when no corresponding event is
available. The room timeline scan uses bounded concurrency and can be more
expensive for a workspace with a large history, so this endpoint should be
called for administration/diagnostics rather than on every client refresh.
