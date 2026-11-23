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

"""
Cleanup NFS Shares Scenario - Test Automation.

Empties the Slurm and Kubernetes NFS shares of the orchestrator storage_config.yml
(``nfs_slurm`` and ``nfs_k8s``) so that a reinstall starts without stale data.

- V001 (sanity) mounts every share read-only and unmounts it: nothing is deleted.
- E001 (destructive) checks every share first, then empties each one (the exports stay).
- V002 (destructive) checks that no entry is left.

The destructive tests run only with ``--marker destructive``. The scenario needs a local run
as root on the execution OIM, because the shares are mounted there.
"""

import pytest

from library.functions import (
    TestLogger,
    can_run_locally,
    get_storage_config_path,
    get_protected_paths,
    get_nfs_targets,
    check_nfs_shares_reachable,
    empty_nfs_shares,
    check_nfs_shares_empty,
)
from library.vars import TEST_CASES as TC
from library.messages import NFS_MSGS


def _targets(host, tl):
    """Return (shares, protected) or skip when there is nothing to do or the run cannot mount."""
    if not can_run_locally():
        tl.skipped(NFS_MSGS["need_local_root"])
        pytest.skip(NFS_MSGS["need_local_root"])
    storage_config = get_storage_config_path(host)
    found = get_nfs_targets(storage_config)
    assert found["success"], found["error"]
    if found.get("missing"):
        reason = NFS_MSGS["no_storage_config"].format(path=storage_config)
        tl.skipped(reason)
        pytest.skip(reason)
    if not found["shares"]:
        tl.skipped(NFS_MSGS["no_shares"])
        pytest.skip(NFS_MSGS["no_shares"])
    return found["shares"], get_protected_paths(host)


@pytest.mark.deploy
@pytest.mark.functional
@pytest.mark.cleanup_nfs_shares
@pytest.mark.destructive
@pytest.mark.order(0)
def test_deploy_cleanup_nfs_shares(host):
    """Check every NFS share, then empty each one (opt-in: --marker destructive)."""
    tc = TC["deploy_cleanup_nfs_shares"]
    tl = TestLogger(tc["title"], tc["id"])
    shares, protected = _targets(host, tl)

    result = empty_nfs_shares(shares, protected)

    if result["success"]:
        tl.passed(NFS_MSGS["emptied"].format(names=", ".join(name for name, _, _ in shares)))
    else:
        tl.failed(result["error"])
    assert result["success"], result["error"]


@pytest.mark.sanity
@pytest.mark.cleanup_nfs_shares
@pytest.mark.order(1)
def test_cleanup_nfs_shares_reachable(host):
    """Verify every configured NFS share can be mounted read-only (nothing is deleted)."""
    tc = TC["cleanup_nfs_shares_reachable"]
    tl = TestLogger(tc["title"], tc["id"])
    shares, protected = _targets(host, tl)

    result = check_nfs_shares_reachable(shares, protected)

    if result["success"]:
        tl.passed(f"Reachable: {result['entries']}")
    else:
        tl.failed(result["error"])
    assert result["success"], result["error"]


@pytest.mark.functional
@pytest.mark.cleanup_nfs_shares
@pytest.mark.destructive
@pytest.mark.order(2)
def test_cleanup_nfs_shares_empty(host):
    """Verify the Slurm and K8s NFS shares have no entry left (run after the cleanup)."""
    tc = TC["cleanup_nfs_shares_empty"]
    tl = TestLogger(tc["title"], tc["id"])
    shares, protected = _targets(host, tl)

    result = check_nfs_shares_empty(shares, protected)

    if result["success"]:
        tl.passed(NFS_MSGS["all_empty"])
    else:
        tl.failed(result["error"])
    assert result["success"], result["error"]
