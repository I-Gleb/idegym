"""Check that each watcher reaps only its own sandbox component."""

import importlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from idegym.api.type import Duration
from idegym.backend.utils import kubernetes_client
from idegym.watcher import crash_detector, reconcile


@pytest.mark.parametrize("component", ["sandbox", "sandbox-canary"])
async def test_orphan_cleanup_excludes_other_stack(monkeypatch, component):
    pods = [
        SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                annotations={kubernetes_client.SERVER_ANNOTATION: name},
                labels={"app.kubernetes.io/component": name},
                deletion_timestamp=None,
                creation_timestamp=datetime.now(timezone.utc) - timedelta(hours=1),
            )
        )
        for name in ("sandbox", "sandbox-canary")
    ]

    async def list_pods(selector, namespace):
        assert namespace == "mellum"
        key, value = selector.split("=")
        return [pod for pod in pods if pod.metadata.labels[key] == value]

    try:
        with monkeypatch.context() as patch:
            patch.setenv("IDEGYM_SANDBOX_COMPONENT", component)
            for module in (kubernetes_client, reconcile, crash_detector):
                importlib.reload(module)
            assert kubernetes_client.SANDBOX_LABELS["app.kubernetes.io/component"] == component
            assert crash_detector.SANDBOX_POD_SELECTOR == reconcile.SANDBOX_POD_SELECTOR
            patch.setattr(reconcile, "list_pods", list_pods)
            patch.setattr(reconcile, "get_servers_by_generated_names", AsyncMock(return_value=[]))
            delete = AsyncMock()
            patch.setattr(reconcile, "clean_up_server", delete)
            result = await reconcile._reconcile_namespace(None, "mellum", Duration(minutes=5), [])
            assert result.orphans_deleted == 1
            delete.assert_awaited_once_with(name=component, namespace="mellum")
    finally:
        for module in (kubernetes_client, reconcile, crash_detector):
            importlib.reload(module)
