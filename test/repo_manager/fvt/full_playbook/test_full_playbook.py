"""
repo_manager Full Playbook - Deploy (no tags).

Deploy repo_manager.yml with no --tags: the complete playbook.
Run it with: ./run_validation.sh fvt_repo_manager exec --full-playbook
"""

import pytest
from omnia_auto import run_full_playbook_test

from library.functions import run_playbook
from library.vars.test_case_vars import TEST_CASES as TC


@pytest.mark.deploy
@pytest.mark.order(0)
def test_deploy_full_playbook(host):  # pylint: disable=unused-argument
    """Deploy repo_manager.yml without --tags."""
    tc = TC["deploy_full_playbook"]
    run_full_playbook_test(run_playbook, "repo_manager.yml", tc["title"], tc["id"])
