"""Hypervisor connector plugins.

Each hypervisor lives in its own subpackage and exposes a single
``HypervisorConnector`` subclass. Connectors are discovered automatically by
:mod:`phif.connectors.registry` — no central file needs editing to add one.
"""
