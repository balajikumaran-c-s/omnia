"""
discovery Full Playbook - Deploy (no tags).

Deploy discovery.yml with no --tags: the complete playbook.
Run it with: ./run_validation.sh fvt_discovery exec --full-playbook
"""

import pytest
from omnia_auto import run_full_playbook_test

from library.functions import run_playbook
from library.vars.test_case_vars import TEST_CASES as TC


@pytest.mark.deploy
@pytest.mark.order(0)
def test_deploy_full_playbook(host):  # pylint: disable=unused-argument
    """Deploy discovery.yml without --tags."""
    tc = TC["deploy_full_playbook"]
    run_full_playbook_test(run_playbook, "discovery.yml", tc["title"], tc["id"])
