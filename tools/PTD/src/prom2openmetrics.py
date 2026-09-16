#!/usr/bin/env python3
# Copyright (c) 2026 BAAI. All rights reserved.
"""Convert captured metrics snapshots into an OpenMetrics file promtool can load.

Each snapshot in the log starts with a header line written by the capture loop:

    # ==== 1787122159 2026-08-19T06:49:19+00:00 ====

Everything until the next header is one scrape. Samples get an `instance` label
so prefill and decode stay distinguishable, and are emitted sorted by timestamp
because promtool requires them non-decreasing.

Conversion is streaming: each log holds one snapshot in memory at a time and
the per-log streams are merged by timestamp, so multi-GB captures never need
to fit in RAM.
"""

import heapq
import re
import sys

HEADER = re.compile(r"^#\s*====\s*(\d+)\s")
TAIL_TS = re.compile(r"\s(\d{10})\s*$")
SAMPLE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)"
    r"(?P<labels>\{.*\})?"
    r"[ \t]+(?P<value>[-+]?(?:[0-9.]+(?:[eE][-+]?[0-9]+)?|Inf|NaN))[ \t]*$"
)


def _with_instance(labels: str | None, instance: str) -> str:
    tag = f'instance="{instance}"'
    if not labels or labels == "{}":
        return "{" + tag + "}"
    return labels[:-1] + "," + tag + "}"


def _snapshots(path, instance: str, stats: dict):
    """Yield (ts, {series: value}) per distinct timestamp, in file order.

    Consecutive snapshots sharing a timestamp - a stray second capture loop
    against the same port yields those, which promtool rejects - are merged
    into one group with the last value winning, same as Prometheus would do.
    Only the group being built is held in memory.
    """
    ts = None
    group: dict[str, str] = {}
    with open(path, errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            header = HEADER.match(line)
            if header:
                new_ts = int(header.group(1))
                if ts is not None:
                    if new_ts < ts:
                        sys.exit(f"{path.name}: snapshot timestamp {new_ts} predates "
                                 f"{ts}; capture logs must stay in scrape order")
                    if new_ts != ts and group:
                        yield ts, group
                        group = {}
                ts = new_ts
                stats["snapshots"] += 1
            elif not line or line.startswith("#") or ts is None:
                continue                      # HELP/TYPE lines and pre-header noise
            elif m := SAMPLE.match(line):
                series = m.group("name") + _with_instance(m.group("labels"), instance)
                group[series] = m.group("value")
                stats["samples"] += 1
            else:
                stats["junk"] += 1            # 404 pages, proxy errors, curl output
    if group:
        yield ts, group


def bounds(path) -> tuple[int, int]:
    """First and last timestamp of an existing OpenMetrics file, in ms."""
    lo = hi = None
    with open(path, errors="replace") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            if m := TAIL_TS.search(line):
                ts = int(m.group(1))
                if lo is None or ts < lo:
                    lo = ts
                if hi is None or ts > hi:
                    hi = ts
    if lo is None:
        sys.exit(f"{path} has no timestamped samples")
    return lo * 1000, hi * 1000


def convert(inputs, output) -> tuple[int, int]:
    """Write `inputs` [(path, instance), ...] to `output`. Returns (start, end) ms."""
    reported = []
    streams = []
    for path, instance in inputs:
        stats = {"snapshots": 0, "samples": 0, "junk": 0}
        reported.append((path, instance, stats))
        streams.append(_snapshots(path, instance, stats))

    # promtool requires timestamps to be non-decreasing; per-log snapshot
    # streams are already ordered, so a k-way merge is enough.
    total = 0
    start = end = None
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as fh:
        for ts, group in heapq.merge(*streams, key=lambda item: item[0]):
            for series, value in group.items():
                fh.write(f"{series} {value} {ts}\n")
            if start is None:
                start = ts
            end = ts
            total += len(group)
        fh.write("# EOF\n")

    for path, instance, stats in reported:
        print(f"  {path.name:24s} instance={instance:8s} "
              f"snapshots={stats['snapshots']:5d} samples={stats['samples']:8d} "
              f"junk={stats['junk']}")
    if not total:
        sys.exit("no samples parsed - are the logs full of 404s? "
                 "The server needs --enable-metrics.")

    print(f"  -> {output.name}  {total} samples  "
          f"spanning {(end - start) / 60:.1f} min")
    return start * 1000, end * 1000


def _cli(argv) -> None:
    from pathlib import Path

    args = argv[1:]
    output_flags = [i for i, arg in enumerate(args) if arg in ("-o", "--output")]
    if len(output_flags) > 1:
        sys.exit("-o/--output may only be specified once")

    if output_flags:
        index = output_flags[0]
        if index + 1 >= len(args):
            sys.exit("-o/--output requires a path")
        out = Path(args[index + 1])
        args = args[:index] + args[index + 2:]
    else:
        if len(args) < 3:
            sys.exit(
                "usage:\n"
                "  prom2openmetrics.py <prefill.log> <decode.log> -o <out.txt>\n"
                "  prom2openmetrics.py <instance>=<log> [...] -o <out.txt>"
            )
        out = Path(args.pop())

    if len(args) == 2 and all("=" not in arg for arg in args):
        prefill, decode = (Path(arg) for arg in args)
        convert([(prefill, "prefill"), (decode, "decode")], out)
        return

    if not args:
        sys.exit(
            "usage:\n"
            "  prom2openmetrics.py <prefill.log> <decode.log> -o <out.txt>\n"
            "  prom2openmetrics.py <instance>=<log> [...] -o <out.txt>"
        )

    inputs = []
    instances = set()
    for spec in args:
        if "=" not in spec:
            sys.exit(f"invalid input {spec!r}; expected <instance>=<log>")
        instance, path_arg = spec.split("=", 1)
        if not instance or not path_arg:
            sys.exit(f"invalid input {spec!r}; instance and log must be non-empty")
        if instance in instances:
            sys.exit(f"duplicate instance {instance!r}; every log needs a unique label")
        instances.add(instance)
        inputs.append((Path(path_arg), instance))

    convert(inputs, out)


if __name__ == "__main__":
    _cli(sys.argv)
