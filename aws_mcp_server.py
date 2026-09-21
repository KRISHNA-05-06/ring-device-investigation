"""Compatibility entry point for existing Claude configurations.
Canonical implementation: services/mcp/aws_mcp_server.py.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from services.mcp import aws_mcp_server as implementation

if __name__ == "__main__":
    implementation.server.run(transport="stdio")
else:
    sys.modules[__name__] = implementation
