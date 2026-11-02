# Changelog

## 0.4.2

- Volume and mute are set over UPnP. The Sonos refuses them over its
  websocket API while Authentication is on in its Connection Security
  settings.

## 0.4.1

- The stream restarts by itself when the Sonos comes back after a reboot or a
  network drop.
- Volume reports right after the Sonos (re)connects are its start-up state,
  not someone changing its level, and are ignored.

## 0.4.0

- Volume law of 0.5 dB per point below 100 (was 10 dB per halving), so every
  volume step is the same size. To keep the same loudness, set the group to
  100 + 20 × log2(old / 100), for example 56 → 83 and 13 → 41.

## 0.3

- The Sonos follows the group's volume on the other players' volume law, with
  a level trim, and the Sonos app sets that level.
- No cap on the Sonos volume.
- Restarts no longer reset the Sonos's volume.

## 0.2

- Clock drift corrected one sample at a time; the stream kept up between
  songs; the Sonos muted while dropped audio drains.

## 0.1

- First version.
