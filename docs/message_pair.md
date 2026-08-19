# Message Pair API

This unstable API creates a new timeline event that can be mapped back to an
existing message, then allows deleting that mapped message later.

## Create mapped message

`POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair`

### Request

```json
{
  "target_event_id": "$original:example.org",
  "event_type": "m.room.message",
  "batch_id": "pair_abc123",
  "content": {
    "msgtype": "m.text",
    "body": "Tin đã sửa"
  }
}
```

### Response

```json
{
  "batch_id": "pair_abc123",
  "target_event_id": "$original:example.org",
  "event_id": "$update:example.org",
  "event_ids": ["$update:example.org"],
  "mapping": {
    "original_event_id": "$original:example.org",
    "update_event_id": "$update:example.org",
    "relation": {
      "rel_type": "m.replace",
      "event_id": "$original:example.org"
    },
    "async_fields": {
      "content": [
        "message_batch_id",
        "message_batch_sequence",
        "m.relates_to",
        "m.new_content"
      ],
      "unsigned": [
        "transaction_id",
        "message_batch_id",
        "message_batch_sequence"
      ]
    }
  }
}
```

## Delete mapped message

`DELETE /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair/{eventId}`

This redacts the generated update message itself.

### Response

```json
{
  "deleted_event_id": "$update:example.org",
  "redaction_event_id": "$redact:example.org",
  "batch_id": "pair_abc123"
}
```

## Async sync mapping

When the client receives `/sync`, the mapping data is visible in the event
payload:

```json
{
  "event_id": "$update:example.org",
  "type": "m.room.message",
  "content": {
    "msgtype": "m.text",
    "body": "Tin đã sửa",
    "message_batch_id": "pair_abc123",
    "message_batch_sequence": 0,
    "m.relates_to": {
      "rel_type": "m.replace",
      "event_id": "$original:example.org"
    },
    "m.new_content": {
      "msgtype": "m.text",
      "body": "Tin đã sửa",
      "message_batch_id": "pair_abc123",
      "message_batch_sequence": 0
    }
  },
  "unsigned": {
    "transaction_id": "pair:DEVICE:0",
    "message_batch_id": "pair_abc123",
    "message_batch_sequence": 0
  }
}
```

Notes:

- The original message only has `message_batch_id` if it was also created by a
  mapped flow.
- If the original message came from normal `/send`, it will not gain a batch id
  later.
- `message_batch_id` is the client-side grouping key; `m.relates_to.event_id`
  is the actual link to the original event.