"""XCP-ng / XenServer connector (preview).

Auto-discovery imports this subpackage and picks up :class:`XcpngConnector`
with no central registration.
"""

from phif.connectors.xcpng.connector import XcpngConnector

__all__ = ["XcpngConnector"]
