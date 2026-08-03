"""Execution primitives connectors use to do work on targets.

A :class:`JobRunner` is handed to each connector via ``ConnectorContext.runner``.
It exposes three ways to act on a target, each streaming output back through the
connector's log emitter so the UI sees progress live:

* :meth:`run_ssh`      — run shell commands over SSH (Proxmox, XCP-ng).
* :meth:`run_ansible`  — run an Ansible playbook (vSphere array-side, etc.).
* :meth:`run_http`     — call a REST API (k8s, vCenter, OpenStack, VME manager).

In ``PHIF_MOCK_MODE`` the runner does not touch the network; it logs the intended
action and returns a synthetic success, so flows are exercisable end-to-end.
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
from typing import Any, Awaitable, Callable

from phif.config import get_settings

LogEmitter = Callable[[str], Awaitable[None]]


class CommandError(Exception):
    def __init__(self, message: str, *, rc: int | None = None, output: str = ""):
        super().__init__(message)
        self.rc = rc
        self.output = output


# Synthetic interface inventory returned in mock/dry-run so the UI dropdowns are
# exercisable without a real host.
_MOCK_INTERFACES = {
    "nics": [
        {"value": "eth0", "label": "eth0 (10.10.10.5)",
         "address": "10.10.10.5", "cidr": "10.10.10.5/24"},
        {"value": "eth1", "label": "eth1 (10.20.20.5)",
         "address": "10.20.20.5", "cidr": "10.20.20.5/24"},
        {"value": "ens192", "label": "ens192 (192.168.1.5)",
         "address": "192.168.1.5", "cidr": "192.168.1.5/24"},
    ],
    "nvme_sources": [
        {"value": "192.168.10.11", "label": "192.168.10.11 (eth1)",
         "interface": "eth1", "address": "192.168.10.11", "cidr": "192.168.10.11/24"},
        {"value": "192.168.20.11", "label": "192.168.20.11 (ens192)",
         "interface": "ens192", "address": "192.168.20.11", "cidr": "192.168.20.11/24"},
    ],
    "fc_hbas": [
        {"value": "21000024ff000001", "label": "host7 21000024ff000001 (Online 16 Gbit)",
         "host": "host7", "wwpn": "21000024ff000001", "state": "Online"},
        {"value": "21000024ff000002", "label": "host8 21000024ff000002 (Online 16 Gbit)",
         "host": "host8", "wwpn": "21000024ff000002", "state": "Online"},
    ],
}


class JobRunner:
    def __init__(self, log: LogEmitter, *, dry_run: bool = False):
        self.log = log
        self.dry_run = dry_run
        self.mock = get_settings().mock_mode

    # ----------------------------------------------------------------- SSH ---
    # Hard ceiling on a single SSH command. A command that restarts/spawns a daemon
    # which inherits the channel's stdout fd never sends EOF, so the stdout read
    # loop would otherwise hang the whole job forever (this is what hung the XCP-ng
    # wizard at `xe sr-create`). Bound it; callers that legitimately run long can
    # raise it per call.
    SSH_TIMEOUT = 600

    async def run_ssh(self, host: str, command: str, *, username: str,
                      password: str | None = None, key: str | None = None,
                      port: int = 22, check: bool = True,
                      timeout: float | None = None, sudo: bool = False,
                      redact: list[str] | None = None) -> str:
        # When the SSH user isn't root (e.g. a VME/KVM host login), privileged
        # storage commands (iscsiadm, multipath, nvme, /sys + /etc writes) need
        # sudo. Wrap the WHOLE command in `sudo -n sh -c '<cmd>'` so pipelines and
        # redirections also run as root; -n fails fast (no password prompt) if
        # passwordless sudo isn't configured.
        if sudo:
            import shlex
            command = "sudo -n sh -c " + shlex.quote(command)

        # ``redact`` masks secret substrings (e.g. an embedded OS_PASSWORD) in the
        # logged command line AND in streamed output, so a credential carried on
        # the argv never lands in the job log. The command is still executed
        # verbatim.
        secrets = [s for s in (redact or []) if s]

        def _mask(text: str) -> str:
            for s in secrets:
                text = text.replace(s, "***")
            return text

        await self.log(f"[ssh {username}@{host}] $ {_mask(command)}")
        if self.mock or self.dry_run:
            await self.log("[ssh] (mock/dry-run) skipped")
            return ""

        import asyncssh

        conn_kwargs: dict[str, Any] = {"username": username, "port": port, "known_hosts": None}
        if key:
            conn_kwargs["client_keys"] = [asyncssh.import_private_key(key)]
        if password:
            conn_kwargs["password"] = password

        output_lines: list[str] = []
        rc_holder: dict[str, Any] = {}

        async def _run() -> None:
            # stdin=DEVNULL so a command that reads stdin can't block waiting on it.
            async with asyncssh.connect(host, **conn_kwargs) as conn:
                # stderr=STDOUT merges the command's stderr into the stream we
                # read, so failures (e.g. iscsiadm's diagnostic on a non-zero rc)
                # surface in the job log instead of being silently discarded.
                async with conn.create_process(command,
                                               stdin=asyncssh.DEVNULL,
                                               stderr=asyncssh.STDOUT) as proc:
                    async for line in proc.stdout:
                        line = line.rstrip("\n")
                        output_lines.append(line)
                        await self.log(f"  {_mask(line)}")
                    await proc.wait()
                    rc_holder["rc"] = proc.exit_status

        limit = self.SSH_TIMEOUT if timeout is None else timeout
        try:
            if limit:
                await asyncio.wait_for(_run(), timeout=limit)
            else:
                await _run()
        except asyncio.TimeoutError:
            msg = (f"[ssh] command exceeded {limit}s and was abandoned "
                   "(a restarted/spawned daemon likely holds the channel open)")
            await self.log("  " + msg)
            if check:
                raise CommandError(f"SSH command timed out after {limit}s",
                                   rc=124, output="\n".join(output_lines))
            return "\n".join(output_lines)
        rc = rc_holder.get("rc")
        output = "\n".join(output_lines)
        if check and rc not in (0, None):
            raise CommandError(f"SSH command failed (rc={rc})", rc=rc, output=output)
        return output

    async def run_ssh_script(self, host: str, lines: list[str], **kwargs) -> str:
        script = " && ".join(lines)
        return await self.run_ssh(host, script, **kwargs)

    async def discover_initiators(self, host: str, *, username: str,
                                  password: str | None = None, key: str | None = None,
                                  port: int = 22, sudo: bool = False) -> dict[str, Any]:
        """Auto-discover a Linux host's storage initiators over SSH.

        Returns ``{"iqn": str|None, "nqn": str|None, "wwns": [str, ...]}`` by
        reading the standard locations:
          * iSCSI IQN  -> /etc/iscsi/initiatorname.iscsi
          * NVMe  NQN  -> /etc/nvme/hostnqn
          * FC    WWNs -> /sys/class/fc_host/*/port_name (0x-stripped)

        Connectors call this when explicit initiator IDs aren't supplied, so the
        operator never has to type them. Mock/dry-run returns synthetic values so
        flows stay exercisable without a real host.
        """
        if self.mock or self.dry_run:
            await self.log(f"[discover] (mock/dry-run) synthetic initiators for {host}")
            tag = host.replace(".", "-")
            return {
                "iqn": f"iqn.1993-08.org.debian:01:{tag}",
                "nqn": f"nqn.2014-08.org.nvmexpress:uuid:{tag}",
                "wwns": ["21000024ff000001", "21000024ff000002"],
            }

        ssh = dict(username=username, password=password, key=key, port=port,
                   check=False, sudo=sudo)
        iqn_raw = await self.run_ssh(
            host,
            "sed -n 's/^InitiatorName=//p' /etc/iscsi/initiatorname.iscsi 2>/dev/null",
            **ssh,
        )
        nqn_raw = await self.run_ssh(host, "cat /etc/nvme/hostnqn 2>/dev/null", **ssh)
        wwn_raw = await self.run_ssh(
            host, "cat /sys/class/fc_host/*/port_name 2>/dev/null", **ssh
        )

        def _first(text: str) -> str | None:
            for line in text.splitlines():
                line = line.strip()
                if line:
                    return line
            return None

        wwns: list[str] = []
        for line in wwn_raw.splitlines():
            line = line.strip().lower()
            if line.startswith("0x"):
                line = line[2:]
            if line:
                wwns.append(line)

        result = {"iqn": _first(iqn_raw), "nqn": _first(nqn_raw), "wwns": wwns}
        await self.log(
            f"[discover] {host}: iqn={'yes' if result['iqn'] else 'no'} "
            f"nqn={'yes' if result['nqn'] else 'no'} wwns={len(wwns)}"
        )
        return result

    async def discover_iscsi_state(self, host: str, *, username: str,
                                   password: str | None = None, key: str | None = None,
                                   port: int = 22, sudo: bool = False) -> dict[str, Any]:
        """Discover a host's EXISTING iSCSI configuration over SSH.

        Returns ``{"bound_nics": [...], "ifaces": [{iface, nic}],
        "sessions": [{target, portal}], "targets": [{target, portal}]}`` by
        parsing ``iscsiadm -m iface`` (NIC-bound ifaces), ``-m session`` (active
        sessions), and ``-m node`` (known targets — connectivity already set up to
        this or other arrays). Lets the connector reuse an existing binding's
        subnets for cluster consistency and detect prior array connectivity.
        Mock/dry-run returns a synthetic already-bound host.
        """
        if self.mock or self.dry_run:
            await self.log(f"[iscsi-state] (mock/dry-run) synthetic state for {host}")
            return {
                "bound_nics": ["eth0"],
                "ifaces": [{"iface": "phif_eth0", "nic": "eth0"}],
                "sessions": [{"target": "iqn.2010-06.com.purestorage:flasharray.mock0001",
                              "portal": "10.10.10.10"}],
                "targets": [{"target": "iqn.2010-06.com.purestorage:flasharray.mock0001",
                             "portal": "10.10.10.10"}],
            }
        ssh = dict(username=username, password=password, key=key, port=port,
                   check=False, sudo=sudo)
        iface_raw = await self.run_ssh(host, "iscsiadm -m iface 2>/dev/null", **ssh)
        sess_raw = await self.run_ssh(host, "iscsiadm -m session 2>/dev/null", **ssh)
        node_raw = await self.run_ssh(host, "iscsiadm -m node 2>/dev/null", **ssh)

        ifaces: list[dict[str, str]] = []
        for line in iface_raw.splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            fields = parts[1].split(",")
            nic = fields[3].strip() if len(fields) > 3 else ""
            if nic and nic not in ("<empty>", "default", "(null)"):
                ifaces.append({"iface": parts[0].strip(), "nic": nic})

        def _parse_targets(text: str) -> list[dict[str, str]]:
            out = []
            for line in text.splitlines():
                # "tcp: [1] 10.0.0.1:3260,1 iqn.x" (session) or
                # "10.0.0.1:3260,1 iqn.x" (node)
                toks = line.replace("tcp:", "").split()
                portal = next((t.split(",")[0] for t in toks if ":" in t and "iqn" not in t), "")
                target = next((t for t in toks if t.startswith(("iqn.", "eui.", "naa."))), "")
                if target:
                    out.append({"target": target, "portal": portal.rsplit(":", 1)[0]})
            return out

        return {
            "bound_nics": sorted({i["nic"] for i in ifaces}),
            "ifaces": ifaces,
            "sessions": _parse_targets(sess_raw),
            "targets": _parse_targets(node_raw),
        }

    async def discover_interfaces(self, host: str, kind: str, *, username: str,
                                  password: str | None = None, key: str | None = None,
                                  port: int = 22, sudo: bool = False) -> list[dict[str, Any]]:
        """Enumerate bindable interfaces/HBAs on a Linux host over SSH.

        ``kind`` is a DiscoveryKind value:
          * "nics"         -> Ethernet NICs for iSCSI iface binding
                              [{value: ifname, label: "ifname (mac, state)"}]
          * "nvme_sources" -> IP-bearing interfaces usable as NVMe-TCP host-traddr
                              [{value: ip, label: "ip (ifname)", interface, address}]
          * "fc_hbas"      -> Fibre Channel HBAs
                              [{value: wwpn, label: "hostX wwpn (state, speed)", ...}]

        Returns a list of {"value","label", ...} dicts for the UI dropdowns.
        Mock/dry-run returns synthetic entries so the UI is exercisable.
        """
        if self.mock or self.dry_run:
            await self.log(f"[discover] (mock/dry-run) synthetic {kind} for {host}")
            return _MOCK_INTERFACES.get(kind, [])

        ssh = dict(username=username, password=password, key=key, port=port,
                   check=False, sudo=sudo)
        if kind == "nics":
            # Only IP-bearing interfaces, each with its address + CIDR, so the
            # caller can filter to NICs on the storage subnet (and skip VM
            # taps/bridges/vlans that carry no IP).
            raw = await self.run_ssh(
                host, "ip -o -4 addr show 2>/dev/null | awk '{print $2\" \"$4}'", **ssh)
            out = []
            for line in raw.splitlines():
                parts = line.split()
                if len(parts) < 2:
                    continue
                name = parts[0].split("@")[0].strip()
                if not name or name == "lo":
                    continue
                cidr = parts[1]
                addr = cidr.split("/")[0]
                out.append({"value": name, "label": f"{name} ({addr})",
                            "address": addr, "cidr": cidr})
            return out
        if kind == "nvme_sources":
            raw = await self.run_ssh(
                host,
                "ip -o -4 addr show 2>/dev/null | awk '{print $2\" \"$4}'",
                **ssh,
            )
            out = []
            for line in raw.splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[0] != "lo":
                    ifname, cidr = parts[0], parts[1]
                    ip = cidr.split("/")[0]
                    out.append({"value": ip, "label": f"{ip} ({ifname})",
                                "interface": ifname, "address": ip,
                                "cidr": cidr})
            return out
        if kind == "fc_hbas":
            raw = await self.run_ssh(
                host,
                "for h in /sys/class/fc_host/*; do "
                "[ -e \"$h\" ] || continue; "
                "echo \"$(basename $h) $(cat $h/port_name 2>/dev/null) "
                "$(cat $h/port_state 2>/dev/null) $(cat $h/speed 2>/dev/null)\"; done",
                **ssh,
            )
            out = []
            for line in raw.splitlines():
                p = line.split()
                if len(p) >= 2:
                    fchost, wwpn = p[0], p[1].lower().removeprefix("0x")
                    state = p[2] if len(p) > 2 else ""
                    speed = " ".join(p[3:]) if len(p) > 3 else ""
                    out.append({"value": wwpn,
                                "label": f"{fchost} {wwpn} ({state} {speed})".strip(),
                                "host": fchost, "wwpn": wwpn, "state": state})
            return out
        return []

    # ------------------------------------------------------------- Ansible ---
    async def run_ansible(self, playbook: str, *, extravars: dict[str, Any] | None = None,
                          inventory: dict[str, Any] | str | None = None) -> dict[str, Any]:
        await self.log(f"[ansible] playbook: {playbook}")
        if extravars:
            safe = {k: ("***" if "token" in k or "pass" in k else v) for k, v in extravars.items()}
            await self.log(f"[ansible] extravars: {json.dumps(safe)}")
        if self.mock or self.dry_run:
            await self.log("[ansible] (mock/dry-run) skipped")
            return {"status": "successful", "rc": 0}

        import ansible_runner

        settings = get_settings()
        os.makedirs(settings.job_workspace, exist_ok=True)
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        def _event_handler(event: dict) -> bool:
            stdout = event.get("stdout")
            if stdout:
                loop.call_soon_threadsafe(queue.put_nowait, stdout)
            return True

        def _run():
            r = ansible_runner.run(
                playbook=playbook,
                extravars=extravars or {},
                inventory=inventory,
                private_data_dir=settings.job_workspace,
                event_handler=_event_handler,
                quiet=True,
            )
            loop.call_soon_threadsafe(queue.put_nowait, None)
            return r

        task = asyncio.create_task(asyncio.to_thread(_run))
        while True:
            line = await queue.get()
            if line is None:
                break
            await self.log(line)
        r = await task
        if r.rc != 0:
            raise CommandError(f"Ansible playbook failed (rc={r.rc})", rc=r.rc)
        return {"status": r.status, "rc": r.rc}

    # ---------------------------------------------------------------- HTTP ---
    async def run_http(self, method: str, url: str, *, headers: dict | None = None,
                       json_body: Any = None, data: Any = None, files: Any = None,
                       verify: bool = False,
                       expected: tuple[int, ...] = (200, 201, 202, 204)) -> dict[str, Any]:
        await self.log(f"[http] {method.upper()} {url}")
        if self.mock or self.dry_run:
            await self.log("[http] (mock/dry-run) skipped")
            return {"status_code": 200, "json": {}}

        import httpx

        async with httpx.AsyncClient(verify=verify, timeout=120) as client:
            # `data` sends an application/x-www-form-urlencoded body (e.g. OAuth
            # token requests); `json_body` sends an application/json body; `files`
            # sends a multipart/form-data body (e.g. uploading a plugin JAR).
            resp = await client.request(method, url, headers=headers,
                                        json=json_body, data=data, files=files)
            await self.log(f"[http] -> {resp.status_code}")
            if resp.status_code not in expected:
                raise CommandError(
                    f"HTTP {method} {url} returned {resp.status_code}",
                    rc=resp.status_code,
                    output=resp.text[:2000],
                )
            try:
                body = resp.json()
            except Exception:
                body = {}
            return {"status_code": resp.status_code, "json": body, "text": resp.text}

    # --------------------------------------------------------------- local ---
    async def run_local(self, command: str, *, check: bool = True,
                        redact: list[str] | None = None,
                        log_output: bool = True) -> str:
        """Run a local CLI (helm, oc, openstack, kubectl) on the management host.

        ``redact`` is a list of secret substrings (e.g. a password) to mask in
        the streamed/logged command line; the real command is still executed
        verbatim. Use this for any command that must carry a credential on its
        argv (e.g. ``oc login --password``).

        ``log_output=False`` runs the command but does NOT stream its stdout to
        the job log (the output is still captured + returned). Use this for
        commands whose OUTPUT is itself a secret — e.g. ``oc create token``,
        which prints a bearer token — so the secret never lands in the log.
        """
        log_command = command
        for secret in redact or []:
            if secret:
                log_command = log_command.replace(secret, "***")
        await self.log(f"[local] $ {log_command}")
        if self.mock or self.dry_run:
            await self.log("[local] (mock/dry-run) skipped")
            return ""
        argv = shlex.split(command)
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError as exc:
            # The CLI isn't installed / not on PATH. Raise a clear, actionable
            # error instead of a bare FileNotFoundError (e.g. `oc`, `kubectl`,
            # `helm`, `openstack` missing from the backend image).
            binary = argv[0] if argv else command
            msg = (f"Command not found: {binary!r} is not installed on the "
                   f"management host (PATH). Install it in the backend image.")
            await self.log(f"[local] ! {msg}")
            raise CommandError(msg, rc=127, output=str(exc)) from exc
        assert proc.stdout is not None
        lines: list[str] = []
        async for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip("\n")
            lines.append(line)
            if log_output:
                await self.log(f"  {line}")
        await proc.wait()
        output = "\n".join(lines)
        if check and proc.returncode != 0:
            raise CommandError(f"Command failed (rc={proc.returncode})", rc=proc.returncode,
                               output=output)
        return output
