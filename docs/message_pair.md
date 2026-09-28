# Private translation API

Translations saved through `send_message_pair` belong to the authenticated
Matrix user. They are stored on this homeserver, outside the room timeline.
They do not appear in ordinary `/sync`, `/messages`, room search, push, or
federation. Mobile must call the GET endpoint below to restore translations
when opening a room.

## Save translation

`POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair`

Use `Authorization: Bearer <matrix access token>` and `Content-Type:
application/json`.

```json
{
  "target_event_id": "$originalEventId",
  "content": {
    "msgtype": "m.text",
    "body": "Bạn đang làm gì thế",
    "org.tuwunel.lang": "vi"
  }
}
```

The response retains the `event_id` field mobile stores as
`translationEventId`. The returned ID identifies a private translation
record; it is not a room PDU and cannot be read from the ordinary Matrix
room-event endpoint.

```json
{
  "batch_id": "msg_abc123",
  "target_event_id": "$originalEventId",
  "event_id": "$translationEventId"
}
```

The server adds `message_batch_id`, `message_batch_sequence`, and an
`m.reference` relation to the target in the stored `content`.

## Restore translations

`GET /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair`

Use the same bearer token. The result contains only the caller's saved
translations in this room, ordered by creation time:

```json
{
  "translations": [
    {
      "event_id": "$translationEventId",
      "room_id": "!roomId:server",
      "sender": "@translator:server",
      "target_event_id": "$originalEventId",
      "event_type": "m.room.message",
      "content": {
        "msgtype": "m.text",
        "body": "Bạn đang làm gì thế",
        "org.tuwunel.lang": "vi",
        "message_batch_id": "msg_abc123",
        "message_batch_sequence": 0,
        "m.relates_to": {
          "rel_type": "m.reference",
          "event_id": "$originalEventId"
        }
      },
      "batch_id": "msg_abc123",
      "origin_server_ts": 1770000000000
    }
  ]
}
```

Mobile should key each result by `target_event_id`, display `content.body`,
and keep `event_id -> target_event_id` for immediate cleanup after deletion.
For a translation just returned by the BFF, mobile can display it at once and
then save its returned `event_id` as before.

## Delete translation

`DELETE /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair/{eventId}`

Use the same bearer token. Only the owner can delete the private record.
Mobile should remove it from the UI after a successful response.

```json
{
  "deleted_event_id": "$translationEventId",
  "batch_id": "msg_abc123"
}
```

If the translation was only in local state and has no `translationEventId`,
mobile can clear it without calling DELETE. Other users get `M_NOT_FOUND` for
the private event ID.

## Existing public pairs

Pairs created by earlier versions are room events and are not returned by
this private GET. Their content may already have reached other clients or
homeservers. DELETE still accepts an old pair ID from its original sender
and hides that room event for that sender. Moving old pairs into private
storage requires a separate migration and cannot retract received copies.
