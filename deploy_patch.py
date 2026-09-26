#!/usr/bin/env python3
"""One-shot patch: v15.79 -> v15.81 (Rounds 22 + 23).

R22: botpm spam fix - flood/timeout errors count as started,
genuine not-started note max once per user per 30 min.
R23: Phase-A YT throughput - force-upgrade yt-dlp, install
deno JS runtime, bgutil PO-token server + plugin, politeness,
fan-out default 16 -> 6.
"""
import ast
import hashlib
import sys

INPUT_SHA256 = "bd113d747b61aa2a6b708ce594f1ed8f6cdd515f5d0f27e61cd7b9b103c52d1b"
EXPECT_SHA256 = "69cb53ecd67b8a3045e3330ca884016531ce6ce71a1e9ebf86a2d1aa8dec1891"

PLACEHOLDER
