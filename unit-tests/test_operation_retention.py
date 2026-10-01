"""Validate retention configuration and expired-result responses."""

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from idegym.api.config import OperationRetentionConfig, WatcherConfig
from idegym.api.orchestrator.operations import TERMINAL_ASYNC_OPERATION_STATUSES, AsyncOperationStatus
from idegym.api.type import Duration
from idegym.orchestrator.router import async_operation
from pydantic import ValidationError


def test_defaults_preserve_fourteen_day_payload_history():
    config = WatcherConfig()
    assert config.request_max_age == Duration(days=14)
    assert config.operation_retention.enabled
    assert not config.operation_retention.payload_expiration_enabled
    assert config.operation_retention.payload_max_age == Duration(days=1)


@pytest.mark.parametrize(
    "values",
    [
        {"batch_size": 0},
        {"batch_size": 1001},
        {"batch_size": 10, "max_rows_per_pass": 19},
        {"max_pass_seconds": 0},
        {"max_pass_seconds": 1, "batch_timeout_seconds": 2},
        {"batch_timeout_seconds": 0},
        {"interval": "PT0S"},
        {"payload_max_age": "PT0S"},
    ],
)
def test_invalid_retention_budgets_fail(values):
    with pytest.raises(ValidationError):
        OperationRetentionConfig(**values)


def test_audit_lifetime_cannot_cut_short_payload_grace():
    with pytest.raises(ValidationError, match="request_max_age"):
        WatcherConfig(
            request_max_age=Duration(hours=12),
            operation_retention=OperationRetentionConfig(payload_expiration_enabled=True),
        )


@pytest.mark.parametrize("operation_status", sorted(TERMINAL_ASYNC_OPERATION_STATUSES))
async def test_expired_results_have_an_explicit_gone_response_with_audit_metadata(mocker, operation_status):
    operation = SimpleNamespace(
        id=7,
        request_type="FORWARD_REQUEST",
        status=operation_status,
        request="payload written by an older process after expiration",
        result="payload written by an older process after expiration",
        scheduled_at=1000,
        finished_at=2000,
        payloads_expired_at=90000000,
    )
    mocker.patch.object(async_operation, "find_async_operation", return_value=operation)
    app = FastAPI()
    app.include_router(async_operation.router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/operations/status/7")
    assert response.status_code == 410
    detail = response.json()["detail"]
    assert detail["code"] == "operation_payload_expired"
    assert detail["operation"]["status"] == operation_status
    assert detail["operation"]["payloads_expired_at"] == 90000000
    assert detail["operation"]["request"] is None
    assert detail["operation"]["result"] is None


async def test_unknown_or_deleted_audit_row_returns_not_found(mocker):
    mocker.patch.object(async_operation, "find_async_operation", return_value=None)
    app = FastAPI()
    app.include_router(async_operation.router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/api/operations/status/7")).status_code == 404


async def test_result_missing_at_completion_is_not_reported_as_expired(mocker):
    operation = SimpleNamespace(id=7, request_type="STOP_SERVER", status=AsyncOperationStatus.SUCCEEDED, scheduled_at=1)
    mocker.patch.object(async_operation, "find_async_operation", return_value=operation)
    result = await async_operation.get_operation_status(7)
    assert result.status == AsyncOperationStatus.SUCCEEDED
    assert result.payloads_expired_at is None
