#!/usr/bin/env python3
"""
Live demo for the YouTube Top-K system.

Starts the FastAPI server with a short Flink flush interval, then pumps
weighted random view events and continuously prints a dashboard showing:
  - Local event counts (ground truth of what was sent)
  - Per-shard assignment for each video
  - Top-k returned by the server (from Redis, rebuilt via shard merge)
  - Lag between local counts and server counts (how fresh the cache is)

Weight scheme
  vid_1 and vid_2: each 25% more likely than any single vid_3..vid_10
  e.g. if vid_3..vid_10 each have weight 1.0 then vid_1/vid_2 have weight 1.25.
  Normalised probabilities:
    vid_1, vid_2  ≈ 11.9 % each
    vid_3..vid_10 ≈  9.5 % each

Usage
  python demo.py                      # defaults below
  python demo.py --rate 10 --flush 5  # 10 events/sec, flush every 5 s
  python demo.py --port 9090          # different server port
"""
from __future__ import annotations

import argparse
import hashlib
import os
import random
import signal
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

VIDEO_IDS = [f"vid_{i}" for i in range(1, 11)]
NUM_SHARDS = 10

# vid_1 and vid_2 are 25 % heavier than the rest
_WEIGHTS = [1.25 if i < 2 else 1.0 for i in range(len(VIDEO_IDS))]
_WEIGHT_TOTAL = sum(_WEIGHTS)
_PROBS = [w / _WEIGHT_TOTAL for w in _WEIGHTS]

# ANSI colours
_RESET = "\033[0m"
_BOLD  = "\033[1m"
_GREEN = "\033[32m"
_CYAN  = "\033[36m"
_YELLOW = "\033[33m"
_RED   = "\033[31m"
_DIM   = "\033[2m"

BAR_WIDTH = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def shard_for(video_id: str) -> int:
    """Deterministic shard assignment matching kafka_queue.py."""
    return int(hashlib.md5(video_id.encode()).hexdigest(), 16) % NUM_SHARDS


def weighted_choice() -> str:
    r = random.random() * _WEIGHT_TOTAL
    cumulative = 0.0
    for vid, w in zip(VIDEO_IDS, _WEIGHTS):
        cumulative += w
        if r < cumulative:
            return vid
    return VIDEO_IDS[-1]


def bar(value: int, max_value: int, width: int = BAR_WIDTH) -> str:
    if max_value == 0:
        return " " * width
    filled = int(round(width * value / max_value))
    return "█" * filled + "░" * (width - filled)


def clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def fetch_topk(base_url: str, k: int, timeframe: str) -> list[dict] | None:
    try:
        import urllib.request, json, urllib.error
        url = f"{base_url}/top_videos?k={k}&timeframe={urllib.parse.quote(timeframe)}"
        with urllib.request.urlopen(url, timeout=2) as resp:
            return json.loads(resp.read())["videos"]
    except Exception:
        return None


def post_event(base_url: str, video_id: str, ts: str) -> bool:
    try:
        import urllib.request, json
        data = json.dumps({"videoId": video_id, "timestamp": ts}).encode()
        req = urllib.request.Request(
            f"{base_url}/watched",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2):
            return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Dashboard renderer
# ---------------------------------------------------------------------------

def render(
    local_counts: dict[str, int],
    server_videos: list[dict] | None,
    elapsed: float,
    total_sent: int,
    flush_interval: float,
    next_flush_in: float,
    poll_interval: float,
    next_poll_in: float,
) -> None:
    clear_screen()
    max_local = max(local_counts.values(), default=1)

    # Build shard → [video_id] map
    shard_map: dict[int, list[str]] = defaultdict(list)
    for vid in VIDEO_IDS:
        shard_map[shard_for(vid)].append(vid)

    # Build server count lookup
    server_counts: dict[str, int] = {}
    if server_videos:
        for entry in server_videos:
            server_counts[entry["video_id"]] = entry["count"]

    print(f"{_BOLD}{'═' * 70}{_RESET}")
    print(f"{_BOLD}  YouTube Top-K Live Demo{_RESET}")
    print(
        f"  {_DIM}elapsed {elapsed:.0f}s │ "
        f"sent {total_sent} events │ "
        f"next Flink flush ~{max(next_flush_in, 0):.0f}s │ "
        f"next poll ~{max(next_poll_in, 0):.0f}s{_RESET}"
    )
    print(f"{_BOLD}{'═' * 70}{_RESET}")

    # ── Local send counts ────────────────────────────────────────────────────
    print(f"\n{_BOLD}  Events sent (ground truth)                    shard{_RESET}")
    for vid in VIDEO_IDS:
        count = local_counts[vid]
        pct = 100 * count / total_sent if total_sent else 0
        shard = shard_for(vid)
        b = bar(count, max_local)
        marker = _YELLOW + "▲" + _RESET if vid in ("vid_1", "vid_2") else " "
        print(f"  {marker} {vid:<8}  {count:>5}  ({pct:4.1f}%)  {_CYAN}{b}{_RESET}  {_DIM}#{shard}{_RESET}")

    # ── Server top-k ────────────────────────────────────────────────────────────────────
    print(f"\n{_BOLD}  Server top-10 'last hour'  (Redis ← k-way shard merge){_RESET}")
    if server_videos is None:
        print(f"  {_RED}  (server unreachable or cache not yet populated){_RESET}")
    elif not server_videos:
        print(f"  {_DIM}  (empty — Flink hasn't flushed yet){_RESET}")
    else:
        max_server = max(e["count"] for e in server_videos) if server_videos else 1
        for rank, entry in enumerate(server_videos, 1):
            vid  = entry["video_id"]
            cnt  = entry["count"]
            local = local_counts.get(vid, 0)
            lag = local - cnt
            lag_str = (
                f"{_GREEN}in sync{_RESET}" if lag == 0
                else f"{_YELLOW}+{lag} unflushed{_RESET}"
            )
            b = bar(cnt, max_server)
            shard = shard_for(vid)
            print(
                f"  {_BOLD}#{rank:<2}{_RESET}  {vid:<8}  {cnt:>5}  "
                f"{_GREEN}{b}{_RESET}  {_DIM}shard#{shard}{_RESET}  {lag_str}"
            )

    # ── Shard map ─────────────────────────────────────────────────────────────────────────────────
    print(f"\n{_BOLD}  Shard → video mapping{_RESET}")
    for shard_id in range(NUM_SHARDS):
        vids = shard_map.get(shard_id, [])
        owned = ", ".join(vids) if vids else _DIM + "(empty)" + _RESET
        total_in_shard = sum(local_counts[v] for v in vids)
        print(f"  shard {shard_id}  {owned:<24}  local total: {total_in_shard}")

    print(f"\n{_DIM}  Press Ctrl+C to stop.{_RESET}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace, server_proc: subprocess.Popen | None) -> None:
    import urllib.parse  # noqa: F401 — needed by fetch_topk

    base_url = f"http://{args.host}:{args.port}"
    local_counts: dict[str, int] = defaultdict(int)
    total_sent = 0
    start = time.monotonic()

    last_flush_trigger = start  # approximate: when the first shard flushes
    last_poll = 0.0
    server_videos: list[dict] | None = None

    print(f"Waiting for server at {base_url} …", end="", flush=True)
    for _ in range(30):
        import urllib.request
        try:
            urllib.request.urlopen(f"{base_url}/top_videos?k=1&timeframe=all+time", timeout=1)
            break
        except Exception:
            time.sleep(1)
            print(".", end="", flush=True)
    print(" ready!\n")

    interval = 1.0 / args.rate  # seconds between events

    try:
        while True:
            now = time.monotonic()
            elapsed = now - start

            # Send one event
            vid = weighted_choice()
            ts  = datetime.now().isoformat()
            if post_event(base_url, vid, ts):
                local_counts[vid] += 1
                total_sent += 1

            # Poll server periodically
            if now - last_poll >= args.poll:
                server_videos = fetch_topk(base_url, len(VIDEO_IDS), "last hour")
                last_poll = now

            next_flush_in = args.flush - (elapsed % args.flush)
            next_poll_in  = args.poll  - (now - last_poll)

            render(
                local_counts=local_counts,
                server_videos=server_videos,
                elapsed=elapsed,
                total_sent=total_sent,
                flush_interval=args.flush,
                next_flush_in=next_flush_in,
                poll_interval=args.poll,
                next_poll_in=next_poll_in,
            )

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping …")
    finally:
        if server_proc:
            server_proc.terminate()
            server_proc.wait()
            for f in __import__("glob").glob("topk_shard_*.db"):
                os.remove(f)
            print("Server stopped and shard DBs cleaned up.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="YouTube Top-K live demo")
    parser.add_argument("--host",  default="localhost", help="Server host")
    parser.add_argument("--port",  default=8080, type=int, help="Server port")
    parser.add_argument("--rate",  default=5,    type=int, help="Events per second")
    parser.add_argument("--flush", default=10,   type=int, help="Flink flush interval (s)")
    parser.add_argument("--poll",  default=3,    type=int, help="Top-k poll interval (s)")
    parser.add_argument(
        "--no-server",
        action="store_true",
        help="Don't start the server (assumes it's already running)",
    )
    args = parser.parse_args()

    server_proc = None
    if not args.no_server:
        # Clean up any stale shard DBs from a previous run
        import glob
        for f in glob.glob("topk_shard_*.db"):
            os.remove(f)

        env = {**os.environ, "FLINK_FLUSH_INTERVAL": str(args.flush)}
        server_proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", f"--port={args.port}"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"Started server (PID {server_proc.pid}) on port {args.port} "
              f"with FLINK_FLUSH_INTERVAL={args.flush}s")

    run(args, server_proc)


if __name__ == "__main__":
    main()
