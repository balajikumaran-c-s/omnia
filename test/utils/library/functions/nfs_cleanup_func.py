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

"""Helpers of the cleanup_nfs_shares scenario: resolve the shares of the orchestrator
storage_config.yml, check them, empty them and verify they are empty.

The shares are mounted by the execution OIM (mount, umount), so the scenario needs a local
run as root on that OIM. The safety rules (read-only preflight of every share first, protected
exports, no symbolic links followed) are those of ``nfs_func``.
"""

import os
from typing import Any, Dict, List, Tuple

from omnia_auto import (
    load_test_config,
    resolve_domain_input_path,
    read_remote_env,
    is_local_execution,
    log,
)

from ..vars.common_vars import (
    MONOREPO_ROOT,
    ENV_OMNIA_DATA_PATH,
    ENV_OMNIA_PROJECT_NAME,
)
from ..vars.nfs_vars import (
    NFS_SHARE_NAMES,
    STORAGE_CONFIG_DOMAIN,
    STORAGE_CONFIG_FILE,
)
from ..messages.nfs_msgs import NFS_MSGS
from . import nfs_func

Share = Tuple[str, str, str]


def _emit(text: str) -> None:
    log(text, "INFO")


def can_run_locally() -> bool:
    """Return whether this run is local and root: mount needs both."""
    return is_local_execution() and os.geteuid() == 0


def get_storage_config_path(host) -> str:
    """Return the storage_config.yml to read.

    ``nfs_storage_config`` of test_config.yml wins; otherwise the orchestrator input folder of
    the execution OIM (<OMNIA_DATA_PATH>/orchestrator/input/<OMNIA_PROJECT_NAME>/).
    """
    configured = str(load_test_config().get("nfs_storage_config") or "").strip()
    if configured:
        return configured
    folder = resolve_domain_input_path(
        host, STORAGE_CONFIG_DOMAIN, ENV_OMNIA_DATA_PATH, ENV_OMNIA_PROJECT_NAME,
    )
    return os.path.join(folder, STORAGE_CONFIG_FILE)


def get_share_names() -> List[str]:
    """Return the mounts to empty (``nfs_shares`` of test_config.yml, default both)."""
    names = load_test_config().get("nfs_shares") or list(NFS_SHARE_NAMES)
    return [str(name) for name in names]


def get_protected_paths(host) -> List[str]:
    """Return the paths an export on this OIM must not contain."""
    paths = [MONOREPO_ROOT, "/etc/omnia"]
    clone_path = str(load_test_config().get("clone_path") or "")
    if clone_path:
        paths.append(clone_path)
    try:
        paths.append(read_remote_env(host, ENV_OMNIA_DATA_PATH, required=False) or "")
    except Exception:  # pylint: disable=broad-except
        pass
    return [path for path in paths if path]


def get_nfs_targets(storage_config: str) -> Dict[str, Any]:
    """Return {"success", "shares": [(name, name, source)], "error"} for the wanted mounts.

    A missing file or a mount that is not an NFS source is not an error: there is nothing to
    empty. An invalid or forbidden source is an error.
    """
    result: Dict[str, Any] = {"success": True, "shares": [], "error": ""}
    if not os.path.isfile(storage_config):
        result["missing"] = True
        return result
    try:
        sources = nfs_func.configured_sources(storage_config, _emit)
        for name in get_share_names():
            if name not in sources:
                if sources:
                    _emit(NFS_MSGS["not_configured"].format(name=name, path=storage_config))
            elif not nfs_func.looks_like_nfs(sources[name]):
                _emit(NFS_MSGS["not_nfs"].format(
                    name=name, source=sources[name], path=storage_config))
            else:
                nfs_func.parse_source(name, sources[name])
                result["shares"].append((name, name, sources[name]))
    except nfs_func.NfsError as exc:
        result.update(success=False, error=str(exc))
    return result


def check_nfs_shares_reachable(shares: List[Share], protected: List[str]) -> Dict[str, Any]:
    """Mount every share read-only and unmount it (nothing is deleted)."""
    result: Dict[str, Any] = {"success": True, "entries": {}, "error": ""}
    _emit(NFS_MSGS["preflight"])
    try:
        for name, flag, source in shares:
            result["entries"][name] = nfs_func.check_share(name, flag, source, _emit, protected)
    except nfs_func.NfsError as exc:
        result.update(success=False, error=str(exc))
    return result


def empty_nfs_shares(shares: List[Share], protected: List[str]) -> Dict[str, Any]:
    """Check every share first, then empty each one (the exports themselves stay)."""
    result = check_nfs_shares_reachable(shares, protected)
    if not result["success"]:
        return result
    try:
        for name, flag, source in shares:
            nfs_func.wipe_share(name, flag, source, _emit, protected)
    except nfs_func.NfsError as exc:
        result.update(success=False, error=str(exc))
    return result


def check_nfs_shares_empty(shares: List[Share], protected: List[str]) -> Dict[str, Any]:
    """Check that every share has no entry left."""
    result = check_nfs_shares_reachable(shares, protected)
    if result["success"]:
        left = {name: count for name, count in result["entries"].items() if count}
        if left:
            result.update(success=False, error=f"entries left in the shares: {left}")
    return result
