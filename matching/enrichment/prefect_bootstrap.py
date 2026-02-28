"""
Prefect startup bootstrap — fixes known issues with Prefect 3.6.x.

Import this module BEFORE running any Prefect flows or tasks::

    import matching.enrichment.prefect_bootstrap  # noqa: F401
    from matching.enrichment.flows.enrichment_flow import enrichment_flow

Fixes applied:
    1. Port assignment bug: SubprocessASGIServer.__init__ leaves ``self.port``
       as ``None``, causing uvicorn to crash with ``--port None``.
    2. UI static file copy hang: ``create_app`` unconditionally copies 17 MB of
       UI static files via ``shutil.copytree``, which hangs on some systems.
       Setting ``PREFECT_UI_ENABLED=0`` in the subprocess env prevents this.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fix 1: Disable UI before any Prefect import triggers server start
# ---------------------------------------------------------------------------
os.environ.setdefault("PREFECT_UI_ENABLED", "0")

# ---------------------------------------------------------------------------
# Fix 2: Patch SubprocessASGIServer to assign a port when None
# ---------------------------------------------------------------------------
from prefect.server.api.server import SubprocessASGIServer  # noqa: E402

_original_init = SubprocessASGIServer.__init__


def _patched_init(self: SubprocessASGIServer, port: int | None = None) -> None:
    """Call original __init__, then ensure self.port is assigned."""
    _original_init(self, port=port)
    if self.port is None:
        self.port = self.find_available_port()
        logger.debug("Prefect bootstrap: assigned ephemeral port %d", self.port)


SubprocessASGIServer.__init__ = _patched_init  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Fix 3: Ensure subprocess also disables UI
# ---------------------------------------------------------------------------
_original_run_cmd = SubprocessASGIServer._run_uvicorn_command


def _patched_run_cmd(self: SubprocessASGIServer):
    """Inject PREFECT_UI_ENABLED=0 into the subprocess env."""
    # Ensure the parent env var is set so it propagates via **os.environ
    os.environ["PREFECT_UI_ENABLED"] = "0"
    return _original_run_cmd(self)


SubprocessASGIServer._run_uvicorn_command = _patched_run_cmd  # type: ignore[assignment]

logger.debug("Prefect bootstrap: patches applied (port fix + UI disabled)")
