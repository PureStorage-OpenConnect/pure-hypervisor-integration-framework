"""Reference implementation of the HypervisorConnector contract."""

from __future__ import annotations

from typing import Any

from phif.connectors.base import (
    ActionSpec,
    Capability,
    ConnectionValidationError,
    FieldType,
    FormField,
    HypervisorConnector,
    OpResult,
    Protocol,
)


class ExampleConnector(HypervisorConnector):
    # --- static metadata ---
    key = "example"
    name = "Example / Reference"
    description = (
        "Reference connector demonstrating the full contract against mock clients. "
        "Use as the template for real connectors."
    )
    maturity = "scaffold"
    CAPABILITIES = {
        Capability.DEPLOY_PLUGIN,
        Capability.PROVISION_VOLUME,
        Capability.SNAPSHOT,
        Capability.CLONE,
        Capability.RESIZE,
        Capability.HOST_REGISTER,
        Capability.HEALTH,
        Capability.REMOVE,
    }
    SUPPORTED_PROTOCOLS = {Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP}

    # --- UI: how to connect to this hypervisor ---
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("host", "Manager host / IP", FieldType.STRING,
                      placeholder="mgr.example.local"),
            FormField("username", "Username", FieldType.STRING, default="admin"),
            FormField("password", "Password", FieldType.SECRET),
            FormField("protocol", "Storage protocol", FieldType.ENUM, default="iscsi",
                      options=["iscsi", "fc", "nvme-tcp"]),
        ]

    # --- UI: day-2 actions ---
    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(Capability.DEPLOY_PLUGIN, "deploy", "Deploy integration",
                       "Install and register the (mock) integration on the target."),
            ActionSpec(
                Capability.HOST_REGISTER, "register_hosts", "Register hosts on array",
                "Create a host group on the FlashArray for this hypervisor's nodes.",
                fields=[
                    FormField("host_group", "Host group name", FieldType.STRING),
                    FormField("iqns", "Host IQNs (iSCSI, comma-separated)", FieldType.STRING,
                              required=False),
                    FormField("wwns", "Host WWNs (Fibre Channel, comma-separated)",
                              FieldType.STRING, required=False),
                ],
            ),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision", "Provision volume / datastore",
                "Create a FlashArray volume and attach it to the host group.",
                fields=[
                    FormField("name", "Volume name", FieldType.STRING),
                    FormField("size", "Size", FieldType.SIZE, default="1T"),
                    FormField("host_group", "Attach to host group", FieldType.STRING,
                              required=False),
                ],
            ),
            ActionSpec(Capability.SNAPSHOT, "snapshot", "Snapshot volume",
                       fields=[FormField("volume", "Volume name", FieldType.STRING)]),
            ActionSpec(Capability.CLONE, "clone", "Clone volume",
                       fields=[FormField("source", "Source volume", FieldType.STRING),
                               FormField("dest", "New volume name", FieldType.STRING)]),
            ActionSpec(Capability.RESIZE, "resize", "Resize volume",
                       fields=[FormField("volume", "Volume name", FieldType.STRING),
                               FormField("size", "New size", FieldType.SIZE)]),
            ActionSpec(Capability.HEALTH, "health_check", "Health check", long_running=False),
            ActionSpec(Capability.REMOVE, "teardown", "Remove integration",
                       destructive=True),
        ]

    # --- operations ---
    async def validate_connection(self) -> OpResult:
        host = self.ctx.target.get("host")
        await self.ctx.emit(f"Validating connection to {host} ...")
        if not host:
            raise ConnectionValidationError("No host configured")
        # A real connector would open a session here. Mock: confirm array reachable.
        if self.ctx.array is not None:
            info = await self.ctx.array.info()
            await self.ctx.emit(f"FlashArray reachable: {info}")
        return OpResult.ok(f"Connected to {host}", host=host)

    async def deploy_integration(self, **params: Any) -> OpResult:
        await self.ctx.emit("Deploying example integration ...")
        await self.ctx.runner.run_ssh(
            self.ctx.target.get("host", "localhost"),
            "echo install-plugin",
            username=self.ctx.target.get("username", "root"),
            password=self.ctx.target.get("password"),
        )
        return OpResult.ok("Integration deployed", status="deployed")

    async def register_hosts(self, host_group: str, iqns: str = "", wwns: str = "",
                             **_: Any) -> OpResult:
        """Register host initiators on the array.

        Protocol-aware: iSCSI/NVMe-TCP hosts are registered by IQN/NQN, Fibre
        Channel hosts by WWN. For FC there is no IP login step — the host's HBA
        WWNs must be zoned to the array on the fabric (done switch-side).
        """
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        protocol = self.ctx.target.get("protocol", "iscsi")
        iqn_list = [s.strip() for s in iqns.split(",") if s.strip()]
        wwn_list = [s.strip() for s in wwns.split(",") if s.strip()]
        if protocol == "fc" and not wwn_list:
            return OpResult.fail("Fibre Channel selected but no host WWNs provided")

        await self.ctx.emit(f"Creating host group {host_group} (protocol={protocol})")
        # Reuse a pre-existing FA host (by initiator) + adopt an existing host group
        # via the shared apply_host_group helper; fail descriptively on a conflict.
        spec = {"name": f"{host_group}-h1",
                "iqns": iqn_list if protocol == "iscsi" else None,
                "wwns": wwn_list if protocol == "fc" else None}
        res = await self.apply_host_group(host_group, [spec])
        if res.get("conflict"):
            return OpResult.fail(res["conflict"],
                                 artifacts={"host_group": host_group, "protocol": protocol})
        return OpResult.ok(
            f"Host group {res['host_group']} registered ({protocol})",
            artifacts={"host_group": res["host_group"], "protocol": protocol,
                       "adopted_host_group": res["adopted"]},
        )

    async def provision(self, name: str, size: str = "1T", host_group: str = "",
                        **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Creating volume {name} ({size})")
        await self.ctx.array.create_volume(name, size)
        if host_group:
            await self.ctx.array.connect_volume(host_group, name)
            await self.ctx.emit(f"Attached {name} to {host_group}")
        return OpResult.ok(f"Provisioned {name}", artifacts={"volume": name})

    async def snapshot(self, volume: str, **_: Any) -> OpResult:
        await self.ctx.array.create_snapshot(volume)
        return OpResult.ok(f"Snapshot of {volume} created")

    async def clone(self, source: str, dest: str, **_: Any) -> OpResult:
        await self.ctx.array.clone_volume(source, dest)
        return OpResult.ok(f"Cloned {source} -> {dest}", artifacts={"volume": dest})

    async def resize(self, volume: str, size: str, **_: Any) -> OpResult:
        await self.ctx.array.extend_volume(volume, size)
        return OpResult.ok(f"Resized {volume} to {size}")

    async def health_check(self, **_: Any) -> OpResult:
        info = await self.ctx.array.info() if self.ctx.array else {}
        return OpResult.ok("Healthy", data={"array": info})

    async def teardown(self, **_: Any) -> OpResult:
        await self.ctx.emit("Removing example integration ...")
        return OpResult.ok("Integration removed", status="not_deployed")
