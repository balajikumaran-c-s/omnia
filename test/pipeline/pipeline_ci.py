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

"""Job logic of the Omnia GitLab pipeline (test/pipeline), standard library only.

The generated jobs of the cluster pipeline are one-line calls to this script, so
the logic is testable and the YAML stays small.  Every command runs on the OIM
over SSH:

    init                  check the CI/CD variables, the SSH trust and the inputs; writes the
                          marker the cleanup jobs need
    checkout              record the commit of the clone and copy omnia.env (the clone itself is
                          made by the bootstrap of the initialization job)
    nfs-check             read-only: every Slurm/K8s NFS share of the OIM's storage_config.yml mounts
                          (test/utils: fvt_utils cleanup --suite cleanup_nfs_shares)
    cleanup-nfs           empty those shares (every share is checked first), then check they are empty
    cleanup <domain>      fvt_<domain> cleanup exec in the Omnia venv (utils: its playbook)
    cleanup-omnia         omnia.sh --cleanup --all --skip-approval
    install               omnia.sh -s (recreates the Omnia venv, runs every domain-init)
    inputs                ONE stage: copy clusters/<cluster>/inputs/<domain>/ of every deploy
                          domain to <data>/<domain>/input/<project>/ and the catalog
    credentials <domain>  OpenBao credentials of the domain, vault-encrypted on the OIM
    deploy <domain>       credentials, then every FVT tag in exec mode (<DOMAIN>_TAGS overrides)
    stage <domain> <tag>  fvt_<domain> <tag> exec
    verify <domain|main>  test config and credentials, then TEST_<DOMAIN>_CMD (default
                          ./run_validation.sh fvt_<domain> verify)
    summary               status of every job; fails when one failed

DRY_RUN=true prints what would run on the OIM and changes nothing.

The OIM password is read only from the cluster CI/CD variable and handed to ssh
through SSHPASS; it is never written to a file, an artifact or a command line.
A host key is required: PIPELINE_SSH_KNOWN_HOSTS (a File variable with the OIM host
key line), or PIPELINE_SSH_SKIP_HOST_KEY=true to accept the risk of an unchecked key.
"""

import glob
import json
import os
import re
import shlex
import smtplib
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple

DOMAINS = ("repo_manager", "image_build_manager", "discovery", "orchestrator", "telemetry",
           "utils", "build_stream")
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
SAFE_PATH = re.compile(r"^/[A-Za-z0-9._/-]+$")
SAFE_BRANCH = re.compile(r"^[A-Za-z0-9._/-]{1,200}$")
SAFE_TAG = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
SAFE_URL = re.compile(r"^(https?://|git@)[A-Za-z0-9._:/@~+-]+$")
PROTECTED_PATHS = ("/", "/bin", "/boot", "/dev", "/etc", "/home", "/lib", "/lib64", "/media",
                   "/mnt", "/opt", "/opt/omnia", "/proc", "/root", "/run", "/sbin", "/srv",
                   "/sys", "/tmp",  # nosec B108
                   "/usr", "/var")  # deny list entries, never used as a path
STATUS_DIR = "pipeline_job_status"
COMMIT_FILE = "actual_commit_sha.txt"
REPORT_DIR = "pipeline_reports"
MAX_REPORTS = 50
SAFE_TEST_CMD = re.compile(r"^\./run_validation\.sh [A-Za-z0-9_./ =:,@-]{1,200}$")
DOMAIN_STEPS = {
    "repo_manager": ("precheck", "prepare", "execute", "status"),
    "image_build_manager": ("validate", "prepare", "build"),
    "discovery": ("precheck", "validate", "execute"),
    "orchestrator": ("precheck", "prepare", "provision"),
    "telemetry": ("precheck", "validate", "deploy"),
}
CREDENTIAL_FILES = {
    "repo_manager": "repo_manager_config_credentials.yml",
    "image_build_manager": "image_build_credentials.yml",
    "discovery": "discovery_credentials.yml",
    "orchestrator": "orchestrator_credentials.yml",
    "telemetry": "telemetry_credentials.yml",
}
PLAYBOOK_CLEANUP = ("utils",)
OPTIONAL_CREDENTIALS = ("discovery",)
TEST_CREDENTIALS = ("orchestrator", "telemetry")
BAO_TIMEOUT = 30
SSH_RETRIES = 5
SSH_RETRY_WAIT = 10
INPUTS_ROOT = "clusters/{cluster}/inputs"
CATALOG_FILE = "clusters/{cluster}/catalogs/catalog_rhel.json"
ACTIVATE = """DATA=$(grep '^OMNIA_DATA_PATH=' {install}/src/main/omnia.env 2>/dev/null | cut -d= -f2)
DATA="${{DATA:-/opt/omnia}}"; DATA="${{DATA%/}}"
if [ ! -f "$DATA/activate-omnia.sh" ]; then {missing}; fi
source "$DATA/activate-omnia.sh"
"""


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _say(message: str) -> None:
    print(message, flush=True)


def _fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr, flush=True)
    raise SystemExit(1)


def apply_cluster() -> str:
    """Map <PREFIX>_TARGET_* of the cluster to PIPELINE_TARGET_*; return the cluster name."""
    cluster = _env("CLUSTER")
    if not SAFE_NAME.match(cluster):
        _fail("CLUSTER is empty or not a plain name. HOW TO FIX: the parent trigger job passes it")
    prefix = cluster.upper().replace("-", "_")
    for name in ("IP", "USER", "PASS"):
        value = os.environ.get(f"{prefix}_TARGET_{name}", "")
        if not value:
            _fail(f"{prefix}_TARGET_{name} is not set. HOW TO FIX: add it as a CI/CD variable "
                  "(setup_gitlab_project.py --update sets them)")
        os.environ[f"PIPELINE_TARGET_{name}"] = value
    known = os.environ.get(f"{prefix}_SSH_KNOWN_HOSTS", "")
    if known and not _env("PIPELINE_SSH_KNOWN_HOSTS"):
        os.environ["PIPELINE_SSH_KNOWN_HOSTS"] = known
    return cluster


def install_path() -> str:
    path = _env("OMNIA_INSTALL_PATH", "/root/omnia").rstrip("/")
    if not SAFE_PATH.match(path) or path in PROTECTED_PATHS:
        _fail(f"OMNIA_INSTALL_PATH '{path}' is not a safe folder (a folder of its own, "
              "e.g. /root/omnia or /pipeline)")
    return path


def _ssh_options() -> List[str]:
    check: List[str] = []
    known = _env("PIPELINE_SSH_KNOWN_HOSTS")
    if known:
        check = ["-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known}"]
    elif _env("PIPELINE_SSH_SKIP_HOST_KEY").lower() == "true":
        _say("WARNING: PIPELINE_SSH_SKIP_HOST_KEY=true, the OIM host key is not checked, so the "
             "connection can be intercepted. Set PIPELINE_SSH_KNOWN_HOSTS instead.")
        check = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
    else:
        _fail("PIPELINE_SSH_KNOWN_HOSTS (a File variable with the OIM host key line) is required. "
              "HOW TO FIX: ssh-keyscan -t ed25519 <OIM> and add the line as the variable, or set "
              "PIPELINE_SSH_SKIP_HOST_KEY=true to accept the risk")
    return [*check, "-o", "LogLevel=ERROR", "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=10",
            "-o", f"ConnectTimeout={_env('SSH_CONNECT_TIMEOUT', '30')}"]


def _target() -> str:
    return f"{_env('PIPELINE_TARGET_USER', 'root')}@{_env('PIPELINE_TARGET_IP')}"


def _environment() -> Dict[str, str]:
    env = dict(os.environ)
    env["SSHPASS"] = os.environ.get("PIPELINE_TARGET_PASS", "")
    return env


def ssh_command() -> List[str]:
    return ["sshpass", "-e", "ssh", *_ssh_options(), _target()]


def _redact(text: str) -> str:
    secret = os.environ.get("PIPELINE_TARGET_PASS", "")
    return text.replace(secret, "********") if len(secret) >= 4 else text


def remote(script: str, stdin: Optional[bytes] = None, capture: bool = False,
           retry: bool = False) -> Tuple[int, str]:
    """Run a bash script on the OIM (script on stdin of `bash -s`); stream or capture output."""
    attempts = SSH_RETRIES if retry else 1
    rc, out = 255, ""
    for attempt in range(1, attempts + 1):
        command = [*ssh_command(), "bash", "-s"] if stdin is None else [*ssh_command(), script]
        data = script.encode() if stdin is None else stdin
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, env=_environment())
        assert proc.stdin and proc.stdout
        proc.stdin.write(data)
        proc.stdin.close()
        lines: List[str] = []
        for raw in proc.stdout:
            line = _redact(raw.decode(errors="replace"))
            if capture:
                lines.append(line)
            else:
                print(line, end="", flush=True)
        rc = proc.wait()
        out = "".join(lines)
        if rc != 255 or attempt == attempts:
            break
        _say(f"SSH connection failed, retry {attempt}/{attempts - 1} in {SSH_RETRY_WAIT}s")
        time.sleep(SSH_RETRY_WAIT)
    return rc, out


def _dry_run(what: str, script: str = "") -> bool:
    """With DRY_RUN=true print what would run on the OIM and return True."""
    if _env("DRY_RUN").lower() != "true":
        return False
    _say(f"DRY_RUN: would run on the OIM: {what}")
    if script:
        _say(script)
    return True


def _run(script: str, what: str) -> int:
    if _dry_run(what, script):
        return 0
    rc, _ = remote(script)
    if rc != 0:
        _say(f"ERROR: {what} failed (exit code {rc})")
    return rc


def _domain(name: str) -> str:
    if name not in DOMAINS:
        _fail(f"unknown domain '{name}'. Valid: {', '.join(DOMAINS)}")
    return name


def _tag(name: str) -> str:
    if not SAFE_TAG.match(name):
        _fail(f"'{name}' is not a plain tag name")
    return name


def _verbosity() -> str:
    return "-vvv" if _env("VERBOSE").lower() == "true" else "-v"


def _activate(missing: str) -> str:
    return ACTIVATE.format(install=shlex.quote(install_path()), missing=missing)


def init() -> int:
    cluster = apply_cluster()
    _ssh_options()
    install_path()
    for name, pattern in (("OMNIA_REPO", SAFE_URL), ("OMNIA_BRANCH", SAFE_BRANCH)):
        if not pattern.match(_env(name)):
            _fail(f"{name} is empty or has unsafe characters. HOW TO FIX: set the CI/CD variable")
    root = INPUTS_ROOT.format(cluster=cluster)
    if not os.path.isfile(os.path.join(root, "omnia.env")):
        _fail(f"{root}/omnia.env not found in the project")
    present = [d for d in DOMAIN_STEPS if os.path.isdir(os.path.join(root, d))]
    _say(f"Cluster {cluster}: OIM {_env('PIPELINE_TARGET_IP')}, user {_env('PIPELINE_TARGET_USER')}")
    _say(f"Omnia {_env('OMNIA_REPO')} branch {_env('OMNIA_BRANCH')} at {install_path()}")
    _say(f"Input folders in the project: {', '.join(present) or 'none'}")
    rc, _ = remote("echo SSH connection successful", retry=True)
    return _mark("initialization", rc)


def _mark(name: str, rc: int) -> int:
    """Leave the marker artifact of a passed job (the always-running cleanup jobs check them)."""
    if rc == 0:
        os.makedirs(STATUS_DIR, exist_ok=True)
        with open(f"{STATUS_DIR}/{name}.ok", "w", encoding="utf-8") as handle:
            handle.write("ok\n")
    return rc


def require_ready() -> None:
    """Fail fast when a job the cleanups depend on did not pass (its marker is missing).

    The cleanup jobs run even after another cleanup failed, so GitLab alone cannot stop them
    when initialization, the checkout or the NFS check failed.
    """
    needed = ["initialization", "setup_environment"]
    if _env("PIPELINE_MODE") in ("cleanup", "default") and _env("DOMAINS") == "default":
        needed.append("nfs_check")
    missing = [n for n in needed if not os.path.isfile(f"{STATUS_DIR}/{n}.ok")]
    if missing and _env("DRY_RUN").lower() != "true":
        _fail(f"{', '.join(missing)} did not pass, so nothing was cleaned. HOW TO FIX: fix that "
              "job (SSH, variables, an unreachable NFS share) and run the pipeline again")


def checkout() -> int:
    """Finish the checkout the initialization job cloned: record the commit, copy omnia.env."""
    cluster = apply_cluster()
    path = install_path()
    if _dry_run(f"record the commit of {path} and copy omnia.env"):
        return 0
    rc, out = remote(f"git -C {shlex.quote(path)} rev-parse HEAD", capture=True, retry=True)
    commit = out.strip().splitlines()[-1].strip() if out.strip() else ""
    if rc or not re.fullmatch(r"[0-9a-f]{40}", commit):
        _say(f"ERROR: {path} on the OIM is not a git checkout. HOW TO FIX: the initialization "
             "job clones it; run the pipeline again")
        return rc or 1
    _say(f"Omnia commit {commit}")
    with open(COMMIT_FILE, "w", encoding="utf-8") as handle:
        handle.write(commit + "\n")
    env_file = os.path.join(INPUTS_ROOT.format(cluster=cluster), "omnia.env")
    with open(env_file, "rb") as handle:
        rc, _ = remote(f"cat > {shlex.quote(path)}/src/main/omnia.env", stdin=handle.read())
    return _mark("setup_environment", rc)


def cleanup(domain: str) -> int:
    apply_cluster()
    _domain(domain)
    require_ready()
    path = install_path()
    missing = f"echo 'No Omnia venv on the OIM, nothing to clean for {domain}'; exit 0"
    if domain in PLAYBOOK_CLEANUP:
        return _run(f"""set -e
{_activate(missing)}
cd {shlex.quote(path)}/src/{domain}/playbooks
ansible-playbook {domain}.yml --tags cleanup {_verbosity()}
""", f"cleanup of {domain}")
    return _run(f"""set -e
{_activate(missing)}
cd {shlex.quote(path)}/test/{domain}
pip install -q -r requirements.txt
./run_validation.sh fvt_{domain} cleanup exec
""", f"cleanup of {domain}")


NFS_SUITE = "--suite cleanup_nfs_shares"


def _nfs_scenario(command: str, marker: str, what: str) -> int:
    """Run the cleanup_nfs_shares scenario of test/utils on the OIM (nothing to do without a venv)."""
    apply_cluster()
    path = install_path()
    missing = f"echo 'No Omnia venv on the OIM, no NFS share to {what}'; exit 0"
    return _run(f"""set -e
{_activate(missing)}
cd {shlex.quote(path)}/test/utils
pip install -q -r requirements.txt
./run_validation.sh fvt_utils cleanup {command} {NFS_SUITE} --marker {marker}
""", f"NFS share {what}")


def nfs_check() -> int:
    """Read-only: every Slurm and K8s share of the OIM's storage_config.yml mounts."""
    return _mark("nfs_check", _nfs_scenario("verify", "sanity", "check"))


def cleanup_nfs() -> int:
    """Empty the Slurm and K8s shares (check all first), then check they are empty."""
    require_ready()
    rc = _nfs_scenario("exec", "destructive", "empty")
    return rc or _nfs_scenario("verify", "destructive", "verify as empty")


def cleanup_omnia() -> int:
    apply_cluster()
    require_ready()
    path = install_path()
    return _run(f"""set -e
cd {shlex.quote(path)}/src/main && chmod +x omnia.sh
./omnia.sh --cleanup --all --skip-approval
""", "omnia.sh --cleanup --all")


def install() -> int:
    apply_cluster()
    path = install_path()
    return _run(f"""set -e
cd {shlex.quote(path)}/src/main && chmod +x omnia.sh
./omnia.sh -s
DATA=$(grep '^OMNIA_DATA_PATH=' omnia.env 2>/dev/null | cut -d= -f2); DATA="${{DATA:-/opt/omnia}}"
DATA="${{DATA%/}}"
source "$DATA/activate-omnia.sh"
python3 --version
ansible --version | head -1
""", "omnia.sh -s")


def _resolve_layout() -> Tuple[str, str]:
    """Return (OMNIA_DATA_PATH, OMNIA_PROJECT_NAME) of the OIM, with the Omnia venv active."""
    missing = "echo 'ERROR: no Omnia venv, run the install stage first' >&2; exit 1"
    rc, out = remote(f"""set -e
{_activate(missing)}
echo "$OMNIA_DATA_PATH"
echo "${{OMNIA_PROJECT_NAME:-project_default}}"
""", capture=True)
    lines = [x.strip().rstrip("/") for x in out.splitlines() if x.strip()]
    if rc or len(lines) < 2:
        _fail("could not resolve OMNIA_DATA_PATH and OMNIA_PROJECT_NAME on the OIM")
    data, project = lines[-2], lines[-1]
    if not SAFE_PATH.match(data) or not SAFE_NAME.match(project):
        _fail("OMNIA_DATA_PATH or OMNIA_PROJECT_NAME on the OIM has unsafe characters")
    return data, project


def _push_folder(source: str, destination: str) -> int:
    """tar the local folder through ssh into destination (a safe path)."""
    tar = subprocess.Popen(["tar", "-C", source, "-czf", "-", "."], stdout=subprocess.PIPE)
    command = [*ssh_command(), f"mkdir -p {shlex.quote(destination)} && "
                               f"tar -xzf - -C {shlex.quote(destination)}"]
    rc = subprocess.run(command, stdin=tar.stdout, env=_environment(), check=False).returncode
    tar.stdout.close()
    return rc or tar.wait()


def inputs() -> int:
    cluster = apply_cluster()
    if _dry_run("copy clusters/<cluster>/inputs/<domain>/ of every domain to the OIM"):
        return 0
    data, project = _resolve_layout()
    root = INPUTS_ROOT.format(cluster=cluster)
    folders = [d for d in DOMAIN_STEPS if os.path.isdir(os.path.join(root, d))]
    if not folders:
        _fail(f"no domain folder under {root}/. HOW TO FIX: add clusters/{cluster}/inputs/<domain>/")
    failed = []
    for domain in folders:
        destination = f"{data}/{domain}/input/{project}"
        _say(f"Copy {root}/{domain}/ -> {destination}/")
        if _push_folder(os.path.join(root, domain), destination):
            failed.append(domain)
    catalog = CATALOG_FILE.format(cluster=cluster)
    if os.path.isfile(catalog):
        with open(catalog, "rb") as handle:
            rc, _ = remote(f"mkdir -p {shlex.quote(data)}/catalog && "
                           f"cat > {shlex.quote(data)}/catalog/catalog_rhel.json",
                           stdin=handle.read())
        if rc:
            failed.append("catalog")
    else:
        _say(f"No catalog at {catalog}, skipped")
    if failed:
        _say(f"ERROR: copy failed for: {', '.join(failed)}")
        return 1
    _say(f"Inputs copied for: {', '.join(folders)}")
    return 0


_NO_VENV = "echo 'ERROR: no Omnia venv, run the install stage first' >&2; exit 1"


def _put(path: str, data: bytes, mode: str = "600") -> int:
    """Write data to a file on the OIM (private by default)."""
    folder = os.path.dirname(path)
    rc, _ = remote(f"umask 077; mkdir -p {shlex.quote(folder)} && cat > {shlex.quote(path)} && "
                   f"chmod {mode} {shlex.quote(path)}", stdin=data)
    return rc


def _bao_context() -> ssl.SSLContext:
    ca = _env("BAO_CA_CERT")
    if ca:
        return ssl.create_default_context(cafile=ca)
    if _env("BAO_TLS_SKIP_VERIFY", "true").lower() == "false":
        return ssl.create_default_context()
    _say("WARNING: OpenBao TLS is not verified (like test/pipeline always did). Set BAO_CA_CERT "
         "or BAO_TLS_SKIP_VERIFY=false to verify it.")
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def _bao_call(url: str, data: Optional[dict] = None, token: str = "") -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Vault-Token"] = token
    body = json.dumps(data).encode() if data is not None else None
    request = urllib.request.Request(url, data=body, headers=headers,
                                     method="POST" if body else "GET")
    try:
        with urllib.request.urlopen(request, timeout=BAO_TIMEOUT,  # nosec B310  # https url
                                    context=_bao_context()) as response:
            return json.loads(response.read().decode())
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise RuntimeError(f"OpenBao request failed: {exc.__class__.__name__}") from exc


def bao_secret(name: str) -> Optional[str]:
    """Return the credential file text of OpenBao secret `name`, or None (names only in logs)."""
    server, role = _env("BAO_SERVER_URL").rstrip("/"), _env("BAO_AUTH_ROLE")
    jwt, path = _env("VAULT_ID_TOKEN"), _env("BAO_DATA_PATH").strip("/")
    if not server.startswith("https://") and not server.startswith("http://"):
        _say("ERROR: BAO_SERVER_URL is not set. HOW TO FIX: set the BAO_* CI/CD variables")
        return None
    if not (jwt and role and path and SAFE_NAME.match(name)):
        _say("ERROR: VAULT_ID_TOKEN, BAO_AUTH_ROLE or BAO_DATA_PATH is missing")
        return None
    try:
        login = _bao_call(f"{server}/v1/auth/jwt/login", {"jwt": jwt, "role": role})
        token = login["auth"]["client_token"]
        reply = _bao_call(f"{server}/v1/{path}/{name}", token=token)
    except (RuntimeError, KeyError, TypeError) as exc:
        _say(f"ERROR: OpenBao: {exc}")
        return None
    data = reply.get("data") or {}
    fields = data["data"] if isinstance(data.get("data"), dict) else data
    fields = {k: v for k, v in fields.items() if k != "metadata"}
    if not fields:
        return None
    if len(fields) == 1 and isinstance(next(iter(fields.values())), str):
        return json.dumps(next(iter(fields.values()))) + "\n"
    return "".join(f"{key}: {json.dumps(value)}\n" for key, value in fields.items())


def _encrypt(file_path: str, key_path: str, venv: bool) -> str:
    """Remote script that vault-encrypts a credential file unless it already is."""
    return f"""{_activate(_NO_VENV) if venv else ""}
f={shlex.quote(file_path)}; k={shlex.quote(key_path)}
if [ -f "$f" ]; then
  if head -n1 "$f" | grep -q '$ANSIBLE_VAULT;'; then echo 'Credential file is already encrypted'
  else
    [ -f "$k" ] || {{ umask 077; openssl rand -base64 32 > "$k"; chmod 600 "$k"; }}
    ansible-vault encrypt --vault-password-file="$k" "$f" && echo 'Credential file encrypted'
  fi
fi
"""


def credentials(domain: str) -> int:
    """Fetch the domain credentials from OpenBao into <data>/<domain>/input/<project>/ (encrypted)."""
    apply_cluster()
    _domain(domain)
    name = CREDENTIAL_FILES.get(domain)
    if not name:
        return 0
    if _dry_run(f"fetch {domain} credentials from OpenBao and encrypt {name}"):
        return 0
    text = bao_secret(domain)
    if text is None:
        if domain in OPTIONAL_CREDENTIALS:
            _say(f"WARNING: no {domain} credentials in OpenBao, using the input files only")
            return 0
        _say(f"ERROR: could not fetch the {domain} credentials from OpenBao")
        return 1
    data, project = _resolve_layout()
    folder = f"{data}/{domain}/input/{project}"
    if _put(f"{folder}/{name}", text.encode()):
        return 1
    _say(f"{domain} credentials written ({len(text.splitlines())} fields), encrypting")
    return _run(_encrypt(f"{folder}/{name}", f"{folder}/.{name.replace('.yml', '')}_key", True),
                f"encrypt {name}")


def _fvt(domain: str, tag: str, command: str) -> int:
    apply_cluster()
    _domain(domain)
    _tag(tag)
    path = install_path()
    return _run(f"""set -e
{_activate(_NO_VENV)}
cd {shlex.quote(path)}/test/{domain}
pip install -q -r requirements.txt
./run_validation.sh fvt_{domain} {tag} {command}
""", f"fvt_{domain} {tag} {command}")


def deploy(domain: str) -> int:
    """Domain stage: its credentials, then every FVT tag in exec mode (DOMAIN_TAGS overrides)."""
    apply_cluster()
    _domain(domain)
    if domain not in DOMAIN_STEPS:
        _fail(f"{domain} has no deploy steps. Valid: {', '.join(DOMAIN_STEPS)}")
    rc = credentials(domain)
    if rc:
        return rc
    tags = [t.strip() for t in _env(f"{domain.upper()}_TAGS").split(",") if t.strip()]
    for tag in tags or DOMAIN_STEPS[domain]:
        _say(f"===== {domain}: {tag} (exec) =====")
        rc = _fvt(domain, _tag(tag), "exec")
        if rc:
            return rc
    return 0


def _test_command(domain: str) -> str:
    command = _env(f"TEST_{domain.upper()}_CMD") or f"./run_validation.sh fvt_{domain} verify"
    if not SAFE_TEST_CMD.match(command):
        _fail(f"TEST_{domain.upper()}_CMD is not a ./run_validation.sh command of plain characters")
    return command


def verify_domain(domain: str) -> int:
    """Test stage: the cluster's test config and credentials, then the verify command."""
    cluster = apply_cluster()
    if domain != "main":
        _domain(domain)
    path = install_path()
    command = _test_command(domain)
    if _dry_run(f"copy the test config and run: {command}"):
        return 0
    target = f"{path}/test/{domain}"
    source = os.path.join(INPUTS_ROOT.format(cluster=cluster), "test", domain)
    for name in ("test_config.yml", "test_run_config.yml"):
        file_path = os.path.join(source, name)
        if os.path.isfile(file_path):
            with open(file_path, "rb") as handle:
                if _put(f"{target}/{name}", handle.read(), "644"):
                    return 1
        else:
            _say(f"{name} not in the project, using the defaults of the checkout")
    creds_file = _env(f"{cluster.upper().replace('-', '_')}_TEST_CREDS")
    if creds_file and os.path.isfile(creds_file):
        with open(creds_file, "rb") as handle:
            if _put(f"{target}/test_creds.yml", handle.read()):
                return 1
    if domain in TEST_CREDENTIALS:
        text = bao_secret(f"test_{domain}")
        if text is None:
            _say(f"WARNING: no test_{domain} credentials from OpenBao, using the existing ones")
        elif _put(f"{target}/test_creds.yml", text.encode()):
            return 1
    if (creds_file and os.path.isfile(creds_file)) or domain in TEST_CREDENTIALS:
        rc = _run(f"cd {shlex.quote(target)}\n" + _encrypt("test_creds.yml", ".test_creds.key", True),
                  "encrypt test_creds.yml")
        if rc:
            return rc
    return _run(f"""set -e
{_activate(_NO_VENV)}
cd {shlex.quote(target)}
pip install -q -r requirements.txt
export CI_PIPELINE_ID={shlex.quote(_env("CI_PIPELINE_ID", "0"))}
{command}
""", command)


def _statuses() -> List[Tuple[str, str, str]]:
    """Return (job, status, url) of every job that left a status file (the latest per job)."""
    found: Dict[str, Tuple[str, str, str]] = {}
    for name in sorted(glob.glob(f"{STATUS_DIR}/**/*.json", recursive=True)):
        try:
            with open(name, encoding="utf-8") as handle:
                item = json.load(handle)
            found[str(item["name"])] = (str(item["name"]), str(item["status"]),
                                        str(item.get("url", "")))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return list(found.values())


def _fetch_reports() -> List[Tuple[str, bytes]]:
    """Copy the test reports written during this pipeline from the OIM to pipeline_reports/."""
    path = _env("TEST_REPORTS_PATH", "/opt/omnia/reports")
    since = _env("CI_PIPELINE_CREATED_AT")
    if _env("DRY_RUN").lower() == "true" or not SAFE_PATH.match(path):
        return []
    newer = f"-newermt {shlex.quote(since)}" if re.fullmatch(r"[0-9T:.+Z -]{10,40}", since) else ""
    rc, listing = remote(f"find {shlex.quote(path)} -maxdepth 3 -type f \\( -name '*.html' -o "
                         f"-name '*.json' \\) {newer} 2>/dev/null | sort | head -{MAX_REPORTS}",
                         capture=True)
    reports: List[Tuple[str, bytes]] = []
    os.makedirs(REPORT_DIR, exist_ok=True)
    for remote_path in listing.split() if rc == 0 else []:
        if not SAFE_PATH.match(remote_path):
            continue
        rc, content = remote(f"cat {shlex.quote(remote_path)}", capture=True)
        if rc == 0:
            name = os.path.basename(remote_path)
            with open(os.path.join(REPORT_DIR, name), "w", encoding="utf-8") as handle:
                handle.write(content)
            reports.append((name, content.encode()))
    _say(f"{len(reports)} test report(s) fetched from the OIM")
    return reports


def _send_summary_email(jobs: List[Tuple[str, str, str]], reports: List[Tuple[str, bytes]]) -> bool:
    """Send the summary email; True when it is not configured or was sent."""
    recipients = [r.strip() for r in _env("EMAIL_RECIPIENTS").split(",") if r.strip()]
    server, sender = _env("SMTP_SERVER"), _env("EMAIL_SENDER")
    if not (recipients and server and sender):
        _say("email not configured (EMAIL_RECIPIENTS, EMAIL_SENDER, SMTP_SERVER): not sent")
        return True
    from library.functions import email_func  # pylint: disable=import-outside-toplevel
    commit = ""
    if os.path.isfile(COMMIT_FILE):
        with open(COMMIT_FILE, encoding="utf-8") as handle:
            commit = handle.read().strip()
    info = {"cluster": _env("CLUSTER"), "mode": _env("PIPELINE_MODE"), "domains": _env("DOMAINS"),
            "repo": _env("OMNIA_REPO"), "branch": _env("OMNIA_BRANCH"), "commit": commit,
            "pipeline_url": _env("CI_PIPELINE_URL"), "job_url": _env("CI_JOB_URL"),
            "trigger_time": _env("CI_PIPELINE_CREATED_AT")}
    order = [x for x in _env("PIPELINE_JOB_ORDER").split(",") if x]
    message = email_func.build_message(sender=sender, recipients=recipients, info=info, jobs=jobs,
                                       order=order, attachments=reports)
    try:
        email_func.send_message(message, server, int(_env("SMTP_PORT", "25") or 25),
                                _env("SMTP_USER"), os.environ.get("SMTP_PASSWORD"))
    except (OSError, ValueError, smtplib.SMTPException) as exc:
        _say("=" * 72 + f"\nERROR: the summary email was NOT sent to {', '.join(recipients)}: "
             f"{type(exc).__name__}: {exc}\n" + "=" * 72)
        return False
    _say(f"email sent to {', '.join(recipients)}")
    return True


def summary() -> int:
    """Print the status of every job, send the summary email, fail when a job failed."""
    apply_cluster()
    jobs = _statuses()
    width = max([len(n) for n, _, _ in jobs] + [4])
    _say(f"Pipeline summary, cluster {_env('CLUSTER')}")
    for name, status, _ in jobs:
        _say(f"  {name:<{width}}  {status}")
    failed = [n for n, st, _ in jobs if st == "failed"]
    _say(f"{len(jobs)} jobs, {len(failed)} failed" + (f": {', '.join(failed)}" if failed else ""))
    _send_summary_email(jobs, _fetch_reports())
    return 1 if failed else 0


def main(argv: List[str]) -> int:
    one = {"cleanup": cleanup, "credentials": credentials, "deploy": deploy,
           "verify": verify_domain}
    none = {"init": init, "checkout": checkout, "cleanup-omnia": cleanup_omnia,
            "nfs-check": nfs_check, "cleanup-nfs": cleanup_nfs, "install": install, "inputs": inputs, "summary": summary}
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    name, args = argv[0], argv[1:]
    if name in none and not args:
        return none[name]()
    if name in one and len(args) == 1:
        return one[name](args[0])
    if name == "stage" and len(args) == 2:
        return _fvt(args[0], args[1], "exec")
    print(f"usage error: {' '.join(argv)}\n\n{__doc__}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
