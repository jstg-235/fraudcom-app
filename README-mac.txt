FraudCom {VER} for macOS
========================

Multichannel voice transcriber. Everything runs on this Mac. No audio or
text ever leaves it.

Needs: a Mac with Apple Silicon (M1 or newer), macOS 11 or later,
about 1 GB free space.


FIRST-TIME INSTALL (the FULL download)
--------------------------------------
1. Double-click the zip to unzip it. You get a folder called "FraudCom" with:
     FraudCom.app   model   whisper   README.txt
   Keep these together until FraudCom has opened once.

2. Open the FraudCom folder and double-click FraudCom.app.
   macOS will say it "could not verify" FraudCom. That's expected: the app
   isn't registered with Apple. Click "Done" (not "Move to Trash").

3. Open System Settings > Privacy & Security. Scroll down to the Security
   section, find "FraudCom was blocked..." and click "Open Anyway".
   Enter your password, then click "Open Anyway" once more.
   (On macOS 14 and earlier you can instead right-click FraudCom.app >
   Open > Open.)
   You only need to do this once.

4. The first launch sets up the speech models. It copies about 650 MB into
   your Library, which takes 10-30 seconds before the window appears.
   Don't open the app again while you wait.

5. Allow the prompts when they appear:
   - Microphone: required. This is how FraudCom hears your audio inputs,
     including Dante Virtual Soundcard. If you clicked "Don't Allow", turn
     FraudCom on in System Settings > Privacy & Security > Microphone and
     reopen the app.
   - Local Network / incoming connections: allow these if you want other
     devices to watch the feed, or to send OSC to show systems.

6. Optional, once FraudCom has opened: drag FraudCom.app into your
   Applications folder and delete the rest of the FraudCom folder and the
   zip. The models are already copied.


UPDATING (the small UPDATE download)
------------------------------------
1. Quit FraudCom.
2. Unzip the UPDATE zip and drag the new FraudCom.app over the old one
   (in Applications, or wherever you keep it). Choose "Replace".
3. Open it. If macOS blocks it again, repeat step 3 above.
   macOS may also ask for microphone access again.
Your models, inputs, groups, presets, hot words and transcripts are kept.


DANTE VIRTUAL SOUNDCARD
-----------------------
On a Mac, DVS is one Core Audio device with all of its channels. Pick it
in FraudCom and check the channels you want transcribed. 48 kHz is
recommended.


WHERE YOUR DATA LIVES
---------------------
~/Library/Application Support/FraudCom/
  transcripts/   a transcript-YYYY-MM-DD.txt for each day
  models/        speech models
  roster.json, presets.json, lexicon.json, state.json, automations.json
To open it: in Finder press Shift-Cmd-G and paste the path above.


UNINSTALL
---------
Delete FraudCom.app, then delete the folder
~/Library/Application Support/FraudCom (this also deletes your transcripts).


TROUBLESHOOTING
---------------
"FraudCom can't find its speech models"
  The app was opened without the model and whisper folders beside it.
  Unzip the FULL download again and open FraudCom.app from inside that
  FraudCom folder.

"FraudCom is damaged and can't be opened"
  This sometimes happens when the zip was unzipped by another tool. In
  Terminal, run the line below (adjust the path to where the app is),
  then open the app again:
    xattr -dr com.apple.quarantine /Applications/FraudCom.app

No inputs listed, or the meters stay silent
  Check System Settings > Privacy & Security > Microphone > FraudCom is on.
