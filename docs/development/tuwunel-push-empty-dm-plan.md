# Tuwunel: Push Metadata Và Lọc DM Rỗng

## Phạm vi

Tài liệu này mô tả phần triển khai trong Tuwunel. PushGateway chỉ nhận
notification contract do Tuwunel gửi và không thuộc phạm vi thay đổi của
repository này.

## Đăng ký pusher theo thiết bị

Giữ nguyên API Matrix:

```http
POST /_matrix/client/v3/pushers/set
Authorization: Bearer <access_token>
Content-Type: application/json
```

Ví dụ Android:

```json
{
  "kind": "http",
  "app_id": "vnpt.digitaloffice.android",
  "pushkey": "<FCM_TOKEN>",
  "app_display_name": "VNPT Digital Office",
  "device_display_name": "Samsung S24",
  "lang": "vi",
  "data": {
    "url": "https://pushgateway-service:8090/_matrix/push/v1/notify",
    "platform": "android"
  }
}
```

Ví dụ iOS:

```json
{
  "kind": "http",
  "app_id": "com.vnpt.digitaloffice",
  "pushkey": "<FCM_TOKEN>",
  "app_display_name": "VNPT Digital Office",
  "device_display_name": "iPhone",
  "lang": "vi",
  "data": {
    "url": "https://pushgateway-service:8090/_matrix/push/v1/notify",
    "platform": "ios"
  }
}
```

Quy ước:

- `lang` là trường chuẩn của Matrix pusher và được lưu riêng theo từng
  pusher/device.
- Hỗ trợ `vi`, `vi-VN`, `en`, `en-US`, `en-GB`; Tuwunel chuẩn hóa về `vi` hoặc
  `en`, fallback là `vi`.
- `platform` là custom field trong `data`, chỉ chấp nhận `ios` hoặc `android`.
- Pusher cũ thiếu `platform` vẫn được chấp nhận để không ảnh hưởng client hiện
  tại.
- Client gọi lại cùng API với cùng `pushkey` để cập nhật ngôn ngữ hoặc nền tảng.

## Notification contract

Tuwunel giữ nguyên Matrix Push Gateway payload và bổ sung metadata custom ở
`notification`:

```json
{
  "notification": {
    "event_id": "$event",
    "room_id": "!room:chat-moffice.vnptit.vn",
    "type": "m.room.message",
    "sender": "@user:chat-moffice.vnptit.vn",
    "sender_display_name": "Nguyễn Văn A",
    "sender_avatar_url": "mxc://chat-moffice.vnptit.vn/userAvatar",
    "room_name": "Nhóm dự án",
    "room_avatar_url": "mxc://chat-moffice.vnptit.vn/roomAvatar",
    "is_direct_chat": false,
    "workspaceId": "workspace-id",
    "prio": "high",
    "counts": {
      "unread": 3,
      "missed_calls": 0
    },
    "content": {
      "msgtype": "m.text",
      "body": "Nội dung tin nhắn"
    },
    "devices": [
      {
        "app_id": "vnpt.digitaloffice.android",
        "pushkey": "<FCM_TOKEN>",
        "data": {
          "platform": "android",
          "lang": "vi"
        }
      }
    ]
  }
}
```

Các trường Tuwunel đã có và tiếp tục giữ:

- `event_id`, `room_id`, `type`, `sender`, `content`.
- `sender_display_name`, `room_name`, `room_alias`.
- `prio`, `counts.unread`, `counts.missed_calls`.
- `workspaceId`.
- `call_kind` ở notification cho `m.call.invite`.
- `devices[].app_id`, `devices[].pushkey`, `devices[].data`, `format` và
  `tweaks`.

Các trường được bổ sung:

- `notification.is_direct_chat`.
- `notification.sender_avatar_url`.
- `notification.room_avatar_url`.
- `devices[].data.lang`.

Avatar được gửi nguyên dạng Matrix `mxc://`; PushGateway tự chuyển đổi khi cần
URL HTTPS cho FCM/APNs. Tuwunel không dựng payload FCM và không dịch nội dung
người dùng.

## Reminder cá nhân

Luồng reminder cá nhân phải gửi cùng notification shape với message push:

- Có `workspaceId`, `is_direct_chat`, avatar người gửi/phòng.
- Có `counts` và metadata `lang` trong `devices[].data`.
- Giữ `content` chứa `reminder_id`, `title`, `remind_at`, `repeat`, `scope` và
  trạng thái reminder.
- `platform` tiếp tục lấy từ pusher custom data.

Reminder trong scope phòng được append thành timeline event và đi qua luồng
pusher chuẩn. Reminder scope cá nhân gửi trực tiếp tới các HTTP pusher của người
tạo nhưng vẫn dùng cùng contract JSON.

## Lọc DM chưa có tin nhắn

Tuwunel lưu marker persistent `roomid_hasusermessage`:

- `1`: room đã từng có `m.room.message`.
- `0`: room đã được kiểm tra nhưng chưa có `m.room.message`.

Marker được cập nhật ngay khi PDU được ghi vào timeline, trước khi nhả room
insert lock. Khi gặp room cũ chưa có marker, Tuwunel quét timeline một lần rồi
cache kết quả dưới cùng room ID.

Được tính là message:

- Text, file, ảnh, audio và video dạng `m.room.message`.
- Message do bot gửi, vì bot vẫn là một Matrix user trong room.

Không được tính:

- State event và membership event.
- `im.tuwunel.room.created`.
- Reaction, call và reminder event.

Marker có tính đơn điệu: redact message không chuyển trạng thái về `0`, vì room
đã từng phát sinh hội thoại.

Bộ lọc DM dùng đồng thời:

- `m.direct` của user.
- Fallback `is_direct` trong room member state.

Phạm vi lọc:

- Joined rooms trong sync v3 initial và incremental.
- Joined rooms trong sliding sync v5 room list.
- Không lọc group.
- Không lọc explicit room subscription/loading bằng `room_id`.
- Invite room vẫn được giữ để không làm mất luồng nhận lời mời nếu server không
  auto-join.

## Kiểm thử

- Đăng ký pusher với `lang=vi`, `lang=en-US`, thiếu `platform` và platform sai.
- Kiểm tra client cũ thiếu `platform` vẫn lưu và gửi push được.
- Kiểm tra metadata DM/group, `workspaceId`, `call_kind` và avatar.
- Kiểm tra reminder scope cá nhân và scope phòng.
- Tạo DM mới, xác nhận chưa xuất hiện trước message đầu tiên.
- Gửi message đầu tiên, xác nhận room xuất hiện ngay ở incremental sync của hai
  user.
- Kiểm tra sync v3 và sliding sync v5.
- Redact message đầu tiên, xác nhận room vẫn hiển thị.
- Kiểm tra log push không chứa đầy đủ pushkey/FCM token.

## Các file chính

- `src/service/pusher/mod.rs`: validate platform và chuẩn hóa ngôn ngữ.
- `src/service/pusher/send.rs` và `src/service/pusher/request.rs`: xây dựng và
  mở rộng notification contract.
- `src/service/rooms/timeline/mod.rs`, `append.rs`, `backfill.rs`: marker
  persistent cho message.
- `src/api/client/sync/v3.rs` và `src/api/client/sync/v5/filter.rs`: lọc DM
  rỗng.
- `src/service/rooms/reminders/mod.rs`: reminder cá nhân theo contract mới.
