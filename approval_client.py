"""Compatibility entry point. Edit tools/operator/approval_client.py."""
import sys
from tools.operator import approval_client as implementation

if __name__ == "__main__":
    raise SystemExit(implementation.main())
else:
    sys.modules[__name__] = implementation
