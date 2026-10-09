#!/usr/bin/env python3
"""Build the crypto-medallion narration track (design §8).

Synthesizes one clip per beat, concatenates them into a single narration track,
and reports the total duration so the take can be cut to it. Google Cloud TTS
needs a billing/quota project the demo environment does not have, so this uses
edge-tts (a real neural voice, no API key) and falls back to a clear error if
the online service is unreachable.

    python3 demo/crypto-medallion/scripts/make-narration.py [--out narration.mp3]

Outputs (gitignored): narration/clips/<id>.mp3 and narration/narration.mp3.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

NARRATION_DIR = Path(__file__).resolve().parent.parent / "narration"
CLIPS_DIR = NARRATION_DIR / "clips"


def ffprobe_duration(path: Path) -> float:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True, text=True, check=True).stdout.strip()
    return float(out)


def synth(beat: dict, voice: str, rate: str) -> Path:
    clip = CLIPS_DIR / f"{beat['id']}.mp3"
    cmd = [sys.executable, "-m", "edge_tts", "--voice", voice,
           f"--rate={rate}", "--text", beat["text"], "--write-media", str(clip)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise SystemExit(f"edge-tts failed for {beat['id']}: {proc.stderr.strip()}")
    return clip


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(NARRATION_DIR / "narration.mp3"))
    ap.add_argument("--beats", default=str(NARRATION_DIR / "beats.json"))
    args = ap.parse_args()

    spec = json.loads(Path(args.beats).read_text())
    voice = spec.get("voice", "en-US-AriaNeural")
    rate = spec.get("rate", "+0%")
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)

    clips = []
    for beat in spec["beats"]:
        clip = synth(beat, voice, rate)
        clips.append(clip)
        print(f"  {beat['id']}: {ffprobe_duration(clip):.1f}s  {beat['text'][:48]}…")

    # Concatenate with ffmpeg (re-encode so clips join seamlessly).
    listing = CLIPS_DIR / "concat.txt"
    listing.write_text("".join(f"file '{c.resolve()}'\n" for c in clips))
    out = Path(args.out)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat",
                    "-safe", "0", "-i", str(listing), "-c:a", "libmp3lame",
                    "-q:a", "4", str(out)], check=True)
    total = ffprobe_duration(out)
    print(f"narration: {out} ({total:.1f}s, {len(clips)} beats)")


if __name__ == "__main__":
    main()
