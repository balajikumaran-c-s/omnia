#!/usr/bin/env python3
# Copyright 2026 Dell Inc. or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Empty the Slurm and Kubernetes NFS shares of the cluster; runs on the OIM as root.

    nfs_cleanup.py check --storage-config <file|-> [--share NAME ...] [--protect PATH ...]
    nfs_cleanup.py clear --storage-config <file|-> [--share NAME ...] [--protect PATH ...]

``check`` mounts every share read-only and unmounts it (nothing is deleted); ``clear``
checks every share first, then empties each one (the export itself stays).  The storage
config is the cluster's ``storage_config.yml`` (``-`` reads it from stdin); the shares are
its ``mounts`` named by ``--share`` (default nfs_slurm and nfs_k8s).  The safety rules
are those of ``library/functions/nfs_func.py``.
"""

import argparse
import os
import sys
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from library.functions import nfs_func  # noqa: E402  pylint: disable=wrong-import-position
from library.messages.nfs_msgs import NFS_MSGS  # noqa: E402  pylint: disable=wrong-import-position

DEFAULT_SHARES = ("nfs_slurm", "nfs_k8s")
CHECKOUT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _say(text: str) -> None:
    print(text, flush=True)


def targets(storage_config: str, names: List[str]) -> List[Tuple[str, str, str]]:
    """Return (name, name, source) of each wanted share that is an NFS source."""
    sources = nfs_func.configured_sources(storage_config, _say)
    found = []
    for name in names:
        if name not in sources:
            if sources:
                _say(NFS_MSGS["not_configured"].format(name=name, path=storage_config))
        elif not nfs_func.looks_like_nfs(sources[name]):
            _say(NFS_MSGS["not_nfs"].format(name=name, source=sources[name], path=storage_config))
        else:
            nfs_func.parse_source(name, sources[name])
            found.append((name, name, sources[name]))
    return found


def run(action: str, storage_config: str, names: List[str], protect: List[str]) -> int:
    """Check or clear the shares; return the exit code (0 ok, 1 a share failed)."""
    protected = [*protect, CHECKOUT_ROOT, "/etc/omnia"]
    try:
        shares = targets(storage_config, names)
        if not shares:
            _say("No NFS share to empty")
            return 0
        _say(NFS_MSGS["preflight"])
        for name, flag, source in shares:
            nfs_func.check_share(name, flag, source, _say, protected)
        if action == "clear":
            for name, flag, source in shares:
                nfs_func.wipe_share(name, flag, source, _say, protected)
    except nfs_func.NfsError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=("check", "clear"))
    parser.add_argument("--storage-config", required=True)
    parser.add_argument("--share", action="append", default=[])
    parser.add_argument("--protect", action="append", default=[])
    args = parser.parse_args(argv)
    config = "/dev/stdin" if args.storage_config == "-" else args.storage_config
    return run(args.action, config, args.share or list(DEFAULT_SHARES), args.protect)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
