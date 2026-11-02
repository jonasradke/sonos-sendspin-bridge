# Sonos Sendspin Bridge

Makes a Sonos speaker a Sendspin player for Music Assistant, playing in sync
with the other Sendspin players in a group.

## Setup

Set `sonos_ip` and start the add-on. It connects to the Sendspin server of the
Music Assistant add-on on the same host and appears there as a player named
`name`. Group it with other players, or add it as a Spotify Connect target, in
Music Assistant as usual.

On the first start of a stream the Sonos needs about 7 seconds before it
joins in; it plays near-silence meanwhile and then comes in already in sync.
The stream to the Sonos is kept up for `idle_keep_s` after playback stops, so
later starts are immediate.

In the Sonos app, under **Account → Privacy & Security → Connection
Security**:

- **Authentication** must be **off** (the default). With it on, the Sonos
  refuses to start the bridge's stream.
- **UPnP** must be **on** (the default). Volume and mute are set over UPnP.

## Options

| Option | Default | Meaning |
|---|---|---|
| `sonos_ip` | | The Sonos's IP address. Required. |
| `name` | `Sonos` | The player's name in Music Assistant. |
| `sonos_latency_ms` | `0` | Fine timing correction for the Sonos, in ms. Negative plays it earlier. |
| `volume_trim_db` | `0` | How much louder (+) or quieter (-) the Sonos plays than the other players at the same volume number. |
| `sonos_app_sets_level` | `true` | Turning the Sonos itself (Sonos app or its buttons) sets its level relative to the group, instead of changing the volume number. |
| `lead_ms` | `2000` | How far ahead the Sendspin server sends audio. |
| `queue_ms` | `1000` | How much audio is kept queued in the Sonos while playing. Must be below `lead_ms`. |
| `idle_keep_s` | `900` | Keep the stream to the Sonos up this long after the last playback. |
| `host_ip` | found | The address the Sonos fetches the stream from. |
| `server_url` | `ws://127.0.0.1:8927/sendspin` | The Sendspin server. |
| `http_port` | `8199` | Port of the stream the Sonos fetches. |
| `hold_mute` | `false` | Testing only: keep the Sonos muted at volume 0. |

## Volume

Volume numbers follow the volume law of the Echo Dot port this was built for:
**0.5 dB per point below 100**, so every step of a slider or a Spotify button
is the same size. The bridge translates that to the Sonos's own volume curve
(measured on a Play:1), so the same number sounds equally loud on the Sonos
and the other players, and a group volume change moves every room by the same
amount.

To make the Sonos louder or quieter than the rest, either set
`volume_trim_db`, or turn the Sonos itself in the Sonos app with
`sonos_app_sets_level` on: the bridge keeps the group's number and remembers
the difference as the Sonos's level.

The Sonos cannot go as quiet as the bottom of that scale; near it the Sonos
stays at its lowest setting.

## Sync

The bridge measures the Sonos's playback position many times a second and
places the audio so that the Sonos plays each block at the time the Sendspin
server scheduled it. If the Sonos is consistently a little early or late
against your other speakers, correct it with `sonos_latency_ms`.

If the Sonos reboots or drops off the network while playing, the bridge
restarts the stream once it is back.

## Troubleshooting

- **The Sonos never starts playing**: check that Authentication is off in the
  Sonos app, and that the Sonos can reach `host_ip:http_port`.
- **The volume does not change on the Sonos**: check that UPnP is on in the
  Sonos app.
- **The log** shows every stream start, and the Sonos's queue and clock drift
  every 10 seconds.
