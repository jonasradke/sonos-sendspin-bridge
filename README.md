# Sonos Sendspin Bridge

A Home Assistant add-on that makes a Sonos speaker a
[Sendspin](https://www.sendspin-audio.com/) player, so
[Music Assistant](https://music-assistant.io/) can play it **in sync** with
other Sendspin players: in a sync group, as a Spotify Connect target, with
one volume for the whole group.

It was built to play a Sonos Play:1 together with an Amazon Echo Dot running
[pmaports-biscuit](https://github.com/jonasradke/pmaports-biscuit), and keeps
the two within a few milliseconds of each other.

## How it works

A Sonos cannot be told *when* to play a sample: it plays an HTTP stream as
soon as it has buffered enough of it. What it does report, precisely, is how
far into that stream it is. So the bridge serves the Sonos its own live
stream and, for every block it is about to write, works out from the measured
position when the Sonos will play that block, and fills it with the Sendspin
audio scheduled for exactly that moment. Clock drift between the Sonos and
the host is corrected one sample at a time.

The stream stays up between songs and pauses, so playback after the first
start begins in sync straight away.

## Install

1. In Home Assistant, go to **Settings → Add-ons → Add-on store → ⋮ →
   Repositories** and add `https://github.com/jonasradke/sonos-sendspin-bridge`.
2. Install **Sonos Sendspin Bridge**, set `sonos_ip` to your Sonos's address,
   and start it.
3. The Sonos appears in Music Assistant as a Sendspin player, under the name
   you set.

See the [add-on documentation](sonos_sendspin_bridge/DOCS.md) for the options,
volume behaviour and troubleshooting.

## Requirements

- Home Assistant OS or Supervised, with the Music Assistant add-on and its
  Sendspin provider.
- A Sonos (S2) on the same network. In the Sonos app, under **Account →
  Privacy & Security → Connection Security**, **Authentication** must stay
  **off** and **UPnP** **on** (both are the defaults).

## Licence

MIT, see [LICENSE](LICENSE).
