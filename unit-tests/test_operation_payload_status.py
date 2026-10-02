"""Check that expired operation results remain explicit HTTP failures."""

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from idegym.orchestrator.database.models import AsyncOperation
from idegym.orchestrator.router import async_operation


@pytest.mark.parametrize("expired_at", [None, 100])
def test_operation_status_preserves_metadata_and_reports_expired_payloads(monkeypatch, expired_at):
    row = AsyncOperation(
        id=1,
        request_type="FORWARD_REQUEST",
        status="SUCCEEDED",
        request="request",
        result="result",
        scheduled_at=1,
        finished_at=2,
        payloads_expired_at=expired_at,
    )
    monkeypatch.setattr(async_operation, "find_async_operation", AsyncMock(return_value=row))
    app = FastAPI()
    app.include_router(async_operation.router)
    with TestClient(app) as client:
        response = client.get("/api/operations/status/1")
    if expired_at is None:
        assert response.status_code == 200
        assert response.json()["result"] == "result"
    else:
        assert response.status_code == 410
        detail = response.json()["detail"]
        assert detail["code"] == "operation_payload_expired"
        assert detail["operation"]["id"] == 1
        assert detail["operation"]["status"] == "SUCCEEDED"
        assert detail["operation"]["payloads_expired_at"] == expired_at
        assert detail["operation"]["request"] is detail["operation"]["result"] is None
