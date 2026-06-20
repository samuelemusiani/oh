#!/usr/bin/env python3
"""
MFTP multicast test environment manager using Linux network namespaces.
Must be run as root (sudo). Compilation is delegated to the local user.

Usage:
  ./runner.py start server --iface veth-srv [--addr ADDR] [--port PORT] [--files f1 f2 ...]
  ./runner.py start client --iface veth-cli [--addr ADDR] [--port PORT] [--dir DIR]
  ./runner.py start wireshark {server,client}
  ./runner.py clean
"""

import os
import sys
import shutil
import random
import string
import tempfile
import argparse
import subprocess

# ── Configuration ─────────────────────────────────────────────────────────────

NS_SERVER   = "mcast-server"
NS_CLIENT   = "mcast-client"
VETH_SERVER = "veth-srv"       # lives inside NS_SERVER
VETH_CLIENT = "veth-cli"       # lives inside NS_CLIENT
IP_SERVER   = "10.0.0.1/24"
IP_CLIENT   = "10.0.0.2/24"

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
BINARY      = os.path.join(PROJECT_DIR, "oh")
BINARY_SRC  = "./cmd/oh.go"

DEFAULT_CLIENT_DIR = "/tmp/mftp-received"

# Random file generation defaults
GEN_FILE_COUNT   = 5
GEN_FILE_MAX_BYTES = 1024 * 4
GEN_FILE_STRICY = False # generate files with exactly GEN_FILE_MAX_BYTES if True, otherwise random sizes up to that limit

GEN_FILENAME_LENGTH = 8

# ── Colours ───────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
CYAN   = "\033[36m"

def ok(msg):   print(f"{GREEN}[✓]{RESET} {msg}")
def info(msg): print(f"{CYAN}[*]{RESET} {msg}")
def warn(msg): print(f"{YELLOW}[!]{RESET} {msg}")
def err(msg):  print(f"{RED}[✗]{RESET} {msg}")

def die(msg, code=1):
    err(msg)
    sys.exit(code)

# ── Shell helpers ─────────────────────────────────────────────────────────────

def run(cmd: str, check=True, capture=False) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, shell=True, check=check,
        capture_output=capture, text=capture
    )

def run_as_user(cmd: str, user: str, cwd: str = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["sudo", "-u", user, "bash", "-c", cmd],
        cwd=cwd, check=False
    )

def local_user() -> str:
    user = os.environ.get("SUDO_USER") or os.environ.get("USER")
    if not user or user == "root":
        die("Could not determine the local (non-root) user. Run with sudo.")
    return user

def check_root():
    if os.geteuid() != 0:
        die("This script must be run as root. Use: sudo ./runner.py ...")

def get_display() -> str:
    return os.environ.get("DISPLAY", ":0")

# ── Network namespace helpers ─────────────────────────────────────────────────

def ns_exists(name: str) -> bool:
    r = run("ip netns list", capture=True, check=False)
    return any(line.split()[0] == name for line in r.stdout.splitlines() if line.strip())

def iface_in_ns(ns: str, iface: str) -> bool:
    r = run(f"ip netns exec {ns} ip link show {iface}", check=False, capture=True)
    return r.returncode == 0

def setup_is_complete() -> bool:
    return (
        ns_exists(NS_SERVER) and ns_exists(NS_CLIENT) and
        iface_in_ns(NS_SERVER, VETH_SERVER) and
        iface_in_ns(NS_CLIENT, VETH_CLIENT)
    )

def teardown_partial():
    for ns in [NS_SERVER, NS_CLIENT]:
        if ns_exists(ns):
            run(f"ip netns del {ns}", check=False)

def setup_namespaces():
    if setup_is_complete():
        ok("Network namespaces already configured")
        return

    warn("Incomplete or missing setup — rebuilding...")
    teardown_partial()

    info("Creating namespaces...")
    run(f"ip netns add {NS_SERVER}")
    run(f"ip netns add {NS_CLIENT}")

    info("Creating veth pair...")
    run(f"ip link add {VETH_SERVER} type veth peer name {VETH_CLIENT}")
    run(f"ip link set {VETH_SERVER} netns {NS_SERVER}")
    run(f"ip link set {VETH_CLIENT} netns {NS_CLIENT}")

    info("Configuring server namespace...")
    run(f"ip netns exec {NS_SERVER} ip addr add {IP_SERVER} dev {VETH_SERVER}")
    run(f"ip netns exec {NS_SERVER} ip link set {VETH_SERVER} up")
    run(f"ip netns exec {NS_SERVER} ip link set lo up")
    run(f"ip netns exec {NS_SERVER} ip route add 224.0.0.0/4 dev {VETH_SERVER}")

    info("Configuring client namespace...")
    run(f"ip netns exec {NS_CLIENT} ip addr add {IP_CLIENT} dev {VETH_CLIENT}")
    run(f"ip netns exec {NS_CLIENT} ip link set {VETH_CLIENT} up")
    run(f"ip netns exec {NS_CLIENT} ip link set lo up")
    run(f"ip netns exec {NS_CLIENT} ip route add 224.0.0.0/4 dev {VETH_CLIENT}")

    ok("Network setup complete")

# ── Compilation ───────────────────────────────────────────────────────────────

def compile_binary(user: str):
    os.makedirs(os.path.dirname(BINARY) or ".", exist_ok=True)
    info(f"Compiling mftpc as user '{user}'...")
    result = run_as_user(f"go build -o {BINARY} {BINARY_SRC}", user=user, cwd=PROJECT_DIR)
    if result.returncode != 0:
        die("Compilation failed.")
    ok(f"Binary ready: {BINARY}")

# ── Random file generation ────────────────────────────────────────────────────

def generate_random_files(count: int = GEN_FILE_COUNT) -> tuple[str, list[str]]:
    """
    Create a temp directory with `count` random binary files (<1 MB each).
    Returns (dir_path, [file_paths]).
    """
    tmp_dir = tempfile.mkdtemp(prefix="mftp-server-files-")
    files   = []
    for _ in range(count):
        name = "".join(random.choices(string.ascii_lowercase, k=GEN_FILENAME_LENGTH)) + ".bin"
        path = os.path.join(tmp_dir, name)

        if GEN_FILE_STRICY:
            size = GEN_FILE_MAX_BYTES
        else:
            size = random.randint(1, GEN_FILE_MAX_BYTES)

        with open(path, "wb") as f:
            f.write(os.urandom(size))
        files.append(path)
        info(f"  Generated {name} ({size } B)")
    ok(f"Generated {count} files in {tmp_dir}")
    return tmp_dir, files

# ── Binary invocation builder ─────────────────────────────────────────────────

def build_server_cmd(iface: str, addr: str = None, port: int = None,
                     files: list[str] = None) -> tuple[str, str | None]:
    """
    Returns (shell_command, tmp_dir_or_None).
    tmp_dir is set when we generated the files so the caller can clean up.
    """
    tmp_dir = None
    if not files:
        tmp_dir, files = generate_random_files()

    flags = f"-s -i {iface}"
    if addr:
        flags += f" -a {addr}"
    if port:
        flags += f" -t {port}"

    file_args = " ".join(files)
    return f"{BINARY} {flags} {file_args}", tmp_dir

def build_client_cmd(iface: str, addr: str = None, port: int = None,
                     save_dir: str = None) -> str:
    save_dir = save_dir or DEFAULT_CLIENT_DIR
    os.makedirs(save_dir, exist_ok=True)

    flags = f"-i {iface}"
    if addr:
        flags += f" -a {addr}"
    if port:
        flags += f" -t {port}"

    return f"{BINARY} {flags} {save_dir}"

# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_start_server(args):
    setup_namespaces()
    compile_binary(local_user())

    iface = args.iface or VETH_SERVER
    binary_cmd, tmp_dir = build_server_cmd(
        iface  = iface,
        addr   = args.addr,
        port   = args.port,
        files  = args.files,
    )

    full_cmd = f"ip netns exec {NS_SERVER} {binary_cmd}"
    info(f"Running: {full_cmd}")
    info("Press Ctrl-C to stop.")
    try:
        proc = subprocess.Popen(full_cmd, shell=True)
        proc.wait()
    except KeyboardInterrupt:
        info("Stopping server...")
        proc.terminate()
        proc.wait()
    # finally:
    #     if tmp_dir:
    #         shutil.rmtree(tmp_dir, ignore_errors=True)
    #         info(f"Removed temp files: {tmp_dir}")

def cmd_start_client(args):
    setup_namespaces()
    compile_binary(local_user())

    iface = args.iface or VETH_CLIENT
    binary_cmd = build_client_cmd(
        iface    = iface,
        addr     = args.addr,
        port     = args.port,
        save_dir = args.dir,
    )

    full_cmd = f"ip netns exec {NS_CLIENT} {binary_cmd}"
    info(f"Running: {full_cmd}")
    info("Press Ctrl-C to stop.")
    try:
        proc = subprocess.Popen(full_cmd, shell=True)
        proc.wait()
    except KeyboardInterrupt:
        info("Stopping client...")
        proc.terminate()
        proc.wait()

def cmd_wireshark(args):
    side = args.side
    if not shutil.which("wireshark"):
        die("wireshark not found in PATH.")
    if not shutil.which("tcpdump"):
        die("tcpdump not found in PATH.")
    if not setup_is_complete():
        die("Namespaces not set up. Run 'start server' or 'start client' first.")

    user  = local_user()
    ns    = NS_SERVER   if side == "server" else NS_CLIENT
    iface = VETH_SERVER if side == "server" else VETH_CLIENT

    info(f"Attaching Wireshark to '{iface}' in namespace '{ns}'...")
    cmd = (
        f"ip netns exec {ns} tcpdump -i {iface} -U -w - 2>/dev/null"
        f" | sudo -u {user} DISPLAY={get_display()} wireshark -k -i -"
    )
    try:
        proc = subprocess.Popen(cmd, shell=True)
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        proc.wait()

def cmd_start_socat(args):
    """
    Forwards a port from the mcast-server namespace to localhost.
    Example usage:
      sudo ./runner.py start socat --from 8080 --to 8000
    This will forward localhost:8080 to port 8000 in the server namespace.
    """
    from_port = args.from_port
    to_port = args.to_port
    side = args.side

    if from_port is None or to_port is None:
        die("Both --from and --to ports must be specified.")

    ns    = NS_SERVER   if side == "server" else NS_CLIENT

    socat_cmd = f"socat TCP-LISTEN:{from_port},reuseaddr,fork EXEC:\"ip netns exec {ns} socat STDIO TCP4\\:127.0.0.1\\:{to_port}\""
    info(f"Forwarding localhost:{from_port} → {NS_SERVER}:127.0.0.1:{to_port}")
    info(f"Running: {socat_cmd}")
    try:
        proc = subprocess.Popen(socat_cmd, shell=True)
        proc.wait()
    except KeyboardInterrupt:
        info("Stopping socat forwarder...")
        proc.terminate()
        proc.wait()

def cmd_clean(_args=None):
    info("Cleaning up namespaces...")
    removed = False
    for ns in [NS_SERVER, NS_CLIENT]:
        if ns_exists(ns):
            run(f"ip netns del {ns}", check=False)
            ok(f"Deleted namespace: {ns}")
            removed = True
        else:
            warn(f"Namespace '{ns}' does not exist — skipping")
    ok("Cleanup complete" if removed else "Nothing to clean")

# ── Argument parser ───────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="runner.py",
        description="MFTP multicast test environment (requires sudo)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  sudo ./runner.py start server --iface veth-srv
  sudo ./runner.py start server --files /tmp/a.bin /tmp/b.bin
  sudo ./runner.py start server --addr 224.0.1.1 --port 9000
  sudo ./runner.py start client --dir /tmp/received
  sudo ./runner.py start wireshark server
  sudo ./runner.py clean
        """,
    )

    sub = parser.add_subparsers(dest="command", metavar="command")
    sub.required = True

    # ── start ──────────────────────────────────────────────────────────────────
    start_p = sub.add_parser("start", help="Compile and start a component")
    start_sub = start_p.add_subparsers(dest="role", metavar="role")
    start_sub.required = True

    # shared network flags (reused across server/client/wireshark)
    def add_net_flags(p, default_iface: str):
        p.add_argument(
            "--iface", metavar="IFACE", default=None,
            help=f"network interface inside the namespace (default: {default_iface})",
        )
        p.add_argument("--addr",  metavar="ADDR", default=None, help="multicast address")
        p.add_argument("--port",  metavar="PORT", default=None, type=int, help="port number")

    # start server
    srv = start_sub.add_parser("server", help="Start the server in its namespace")
    add_net_flags(srv, VETH_SERVER)
    srv.add_argument(
        "--files", nargs="+", metavar="FILE", default=None,
        help="files to send (if omitted, random files are generated)",
    )
    srv.set_defaults(func=cmd_start_server)

    # start client
    cli = start_sub.add_parser("client", help="Start the client in its namespace")
    add_net_flags(cli, VETH_CLIENT)
    cli.add_argument(
        "--dir", metavar="DIR", default=None,
        help=f"directory to save received files (default: {DEFAULT_CLIENT_DIR})",
    )
    cli.set_defaults(func=cmd_start_client)

    # start wireshark
    ws = start_sub.add_parser("wireshark", help="Attach Wireshark to a namespace")
    ws.add_argument("side", choices=["server", "client"], help="which namespace to capture")
    ws.set_defaults(func=cmd_wireshark)

    # start socat
    soc = start_sub.add_parser("socat", help="Forward localhost port to server namespace port using socat")
    soc.add_argument("side", choices=["server", "client"], help="which namespace to capture")
    soc.add_argument("--from", dest="from_port", required=True, type=int, help="Local port to listen on")
    soc.add_argument("--to", dest="to_port", required=True, type=int, help="Namespace port to forward to")
    soc.set_defaults(func=cmd_start_socat)

    # ── clean ──────────────────────────────────────────────────────────────────
    cln = sub.add_parser("clean", help="Remove namespaces and interfaces")
    cln.set_defaults(func=cmd_clean)

    return parser

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    check_root()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
