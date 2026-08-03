"""Reference connector.

This is the canonical, fully-worked example every real connector is modeled on.
It exercises every part of the contract — static metadata, ``target_schema``,
``action_schemas``, ``dispatch`` routing, and use of ``ctx.array`` (FlashArray)
and ``ctx.runner`` (Ansible/SSH/HTTP) — against the mock clients, so it also
serves as an end-to-end smoke-test connector in ``PHIF_MOCK_MODE``.

Copy this package to ``phif/connectors/<your_hypervisor>/`` and replace the
bodies. Auto-discovery picks it up with no central registration.
"""

from phif.connectors.example.connector import ExampleConnector

__all__ = ["ExampleConnector"]
