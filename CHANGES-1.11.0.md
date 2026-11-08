# FraudCom 1.11.0 (Mac) — built on 1.10.1

Everything in 1.10.1 is unchanged except the items below.

## Added
- Console shortcuts in Automations ("When heard, do" -> Yamaha RIVAGE console / DiGiCo console).
  Saved as a normal OSC rule (action "osc", UDP) plus a `console` field {type, target, num, second, state, scene},
  so the engine and the Remote API see plain OSC.
  - RIVAGE (Yamaha "RIVAGE PM OSC Specifications" v1.0.2, firmware V5.00+, UDP 49900, 1-based):
    InCh/Mix/Mtrx/St/DCA `/yosc:req/set/MIXER:Current/<X>/Fader/On/<n>/1 <0|1>` (int),
    input->mix send `/yosc:req/set/MIXER:Current/InCh/ToMix/On/<in>/<mix>`, scene `/yosc:req/ssrecallt_ex MIXER:Lib/Scene s:<n.nn>`.
  - DiGiCo SD/Quantum ("DiGiCo Pad" External Control command set, per the open-source Bitfocus Companion DiGiCo module):
    `/<Input_Channels|Aux_Outputs|Group_Outputs|Matrix_Outputs|Control_Groups>/<n>/mute f:<0|1>` (ON = unmuted),
    `/Input_Channels/<n>/Aux_Send/<aux>/send_on f:<0|1>`, `/Snapshots/Fire_Next_Snapshot`, `/Fire_Prev_Snapshot`,
    `/Fire_Selected_Snapshot`, `/Macros/Recall_Macro/<n>`. Default port 7001 (the console's Rcv port).
  - 1.9.6 rules saved with `rivage` are migrated to `console` on load (and the scene argument rebuilt as s:).
- MIDI action in Automations (action "midi"): note (on + off after 100 ms), CC, program change, raw hex (MMC/SysEx);
  to any CoreMIDI destination or FraudCom's own virtual source "FraudCom". macOS via ctypes CoreMIDI (no new package);
  elsewhere python-rtmidi if installed. app.py: _midi_bytes, _midi_describe, midi_from_api, _CoreMIDI, MIDIBus;
  local-only GET /midi/ports and POST /automations/midi-test.
- Remote API: /midi and /midi/devices list MIDI outputs (+ "FraudCom" virtual on Mac); POST /midi/send and the "midi"
  action take a SendMIDIRequest; MIDI rules are listed as a "midi" action. MIDI triggers still 501.
- Notes and markers are right-aligned in the feed (CSS only).
- Input icons: drums, guitar, bass, keys, beltpack (intercom), stagemgr. "clip" relabeled "Clipboard / paperwork",
  "phone" relabeled "Phone".
- Device open: the real PortAudio error is appended ("System said: ..."); a Dante device gets a "start DVS" message;
  on macOS a refused partial channel layout is retried with the device's full channel count.

## Not carried over from 1.9.6 (1.10.x already has its own)
- Mute button (1.10.0 has M), hidden-row CSS fix, quoted-string OSC args (1.10.0 has s: prefixes).

## Tested here (not on hardware)
- Real server code with stand-in audio: console OSC bytes + type tags, phrase firing, MIDI byte building,
  Remote API MIDI endpoints, device-open messages, Mac full-layout retry. UI rendered and exercised in Chromium.
- NOT tested: real RIVAGE / DiGiCo consoles, CoreMIDI on a Mac.
