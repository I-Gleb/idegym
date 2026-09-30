"""Caller-supplied labels and annotations on a server.

Two things have to hold: the metadata actually lands on every object an operator would query,
and it can never displace the managed keys the platform addresses the pod by.
"""

from uuid import uuid4

import pytest
from idegym.api.orchestrator.servers import (
    ServerKind,
    StartServerRequest,
    is_managed_annotation_key,
    is_managed_label_key,
)
from idegym.backend.utils import kubernetes_client as kc
from kubernetes_asyncio.client import ApiClient
from pydantic import ValidationError


@pytest.fixture
async def api_client():
    client = ApiClient()
    try:
        yield client
    finally:
        await client.close()


def _patch_clients(mocker, api_client):
    deployment_result = mocker.MagicMock()
    deployment_result.api_version = "apps/v1"
    deployment_result.kind = "Deployment"
    deployment_result.metadata.name = "srv"
    deployment_result.metadata.uid = "uid-123"

    apps = mocker.MagicMock()
    apps.api_client = api_client
    apps.create_namespaced_deployment = mocker.AsyncMock(return_value=deployment_result)
    core = mocker.MagicMock()
    core.create_namespaced_service = mocker.AsyncMock()
    policy = mocker.MagicMock()
    policy.create_namespaced_pod_disruption_budget = mocker.AsyncMock()

    clients = (apps, mocker.MagicMock(), core, policy, mocker.MagicMock())
    mocker.patch.object(kc, "create_clients", mocker.AsyncMock(return_value=clients))
    return apps, core, policy


async def _deploy(mocker, api_client, **kwargs):
    apps, core, policy = _patch_clients(mocker, api_client)
    await kc.deploy_server(image_tag="img:latest", server_name="srv", namespace="ns", **kwargs)
    return {
        "deployment": apps.create_namespaced_deployment.call_args.kwargs["body"],
        "service": core.create_namespaced_service.call_args.kwargs["body"],
        "pdb": policy.create_namespaced_pod_disruption_budget.call_args.kwargs["body"],
    }


# --------------------------------------------------------------------------------------
# What lands in the cluster
# --------------------------------------------------------------------------------------


async def test_extra_labels_land_on_every_object_an_operator_queries(mocker, api_client) -> None:
    objects = await _deploy(mocker, api_client, extra_labels={"team": "research", "job": "run-42"})

    for name in ("deployment", "service", "pdb"):
        labels = objects[name].metadata.labels
        assert labels["team"] == "research", name
        assert labels["job"] == "run-42", name
    assert objects["deployment"].spec.template.metadata.labels["team"] == "research"


async def test_extra_annotations_land_on_the_pod(mocker, api_client) -> None:
    objects = await _deploy(mocker, api_client, extra_annotations={"example.com/task": "TASK-1"})

    annotations = objects["deployment"].spec.template.metadata.annotations
    assert annotations["example.com/task"] == "TASK-1"


async def test_managed_labels_survive_a_collision(mocker, api_client) -> None:
    """The API rejects these, but the deploy layer must not depend on that to stay correct."""
    objects = await _deploy(
        mocker,
        api_client,
        extra_labels={"app": "hijacked", "app.kubernetes.io/part-of": "somebody-else"},
    )

    labels = objects["deployment"].metadata.labels
    assert labels["app"] == "srv"
    assert labels["app.kubernetes.io/part-of"] == "idegym"


async def test_managed_annotations_survive_a_collision(mocker, api_client) -> None:
    objects = await _deploy(
        mocker,
        api_client,
        extra_annotations={"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
    )

    annotations = objects["deployment"].spec.template.metadata.annotations
    assert annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] == "false"


async def test_a_caller_snapshot_annotation_never_reaches_the_pod(mocker, api_client) -> None:
    """With no snapshot requested, a caller's ps-name would restore a snapshot nobody recorded."""
    objects = await _deploy(
        mocker,
        api_client,
        extra_annotations={"podsnapshot.gke.io/ps-name": "someone-elses-snapshot", "example.com/task": "TASK-1"},
    )

    annotations = objects["deployment"].spec.template.metadata.annotations
    assert "podsnapshot.gke.io/ps-name" not in annotations
    assert annotations["example.com/task"] == "TASK-1"


async def test_a_requested_snapshot_wins_over_a_caller_snapshot_annotation(mocker, api_client) -> None:
    objects = await _deploy(
        mocker,
        api_client,
        snapshot_tag="the-recorded-one",
        extra_annotations={"podsnapshot.gke.io/ps-name": "someone-elses-snapshot"},
    )

    annotations = objects["deployment"].spec.template.metadata.annotations
    assert annotations["podsnapshot.gke.io/ps-name"] == "the-recorded-one"


async def test_the_selector_never_picks_up_caller_labels(mocker, api_client) -> None:
    """A selector that grew a caller label would stop matching pods started without it."""
    objects = await _deploy(mocker, api_client, extra_labels={"team": "research"})

    assert "team" not in objects["deployment"].spec.selector.match_labels
    assert "team" not in objects["service"].spec.selector
    assert "team" not in objects["pdb"].spec.selector.match_labels


async def test_no_extra_metadata_leaves_the_objects_as_before(mocker, api_client) -> None:
    objects = await _deploy(mocker, api_client)

    assert set(objects["deployment"].metadata.labels) == {
        "app",
        "app.kubernetes.io/component",
        "app.kubernetes.io/name",
        "app.kubernetes.io/part-of",
        "app.kubernetes.io/version",
        "idegym.jetbrains.com/snapshot-id",
    }


@pytest.mark.parametrize("server_kind", list(ServerKind))
async def test_every_key_deploy_server_sets_is_reserved_from_callers(mocker, api_client, server_kind) -> None:
    """The drift guard: a managed key the validators do not reserve is one a caller can take over."""
    objects = await _deploy(
        mocker, api_client, server_kind=server_kind, snapshot_id="group-1", snapshot_tag="snapshot-1"
    )

    template = objects["deployment"].spec.template.metadata
    labels = {
        *objects["deployment"].metadata.labels,
        *template.labels,
        *objects["service"].metadata.labels,
        *objects["pdb"].metadata.labels,
    }
    assert sorted(key for key in labels if not is_managed_label_key(key)) == []
    assert sorted(key for key in template.annotations if not is_managed_annotation_key(key)) == []


# --------------------------------------------------------------------------------------
# What the request model accepts
# --------------------------------------------------------------------------------------


def _request(**kwargs) -> StartServerRequest:
    return StartServerRequest(client_id=uuid4(), image_tag="registry.test/env:latest", **kwargs)


def test_labels_and_annotations_default_to_empty() -> None:
    request = _request()

    assert (request.labels, request.annotations) == ({}, {})


@pytest.mark.parametrize(
    "reserved",
    ["app", "app.kubernetes.io/name", "app.kubernetes.io/anything", "idegym.jetbrains.com/snapshot-id"],
)
def test_a_managed_label_key_is_rejected(reserved) -> None:
    with pytest.raises(ValidationError, match="IdeGYM-managed keys"):
        _request(labels={reserved: "mine"})


def test_the_error_names_every_offending_key() -> None:
    with pytest.raises(ValidationError) as caught:
        _request(labels={"app": "a", "app.kubernetes.io/name": "b", "team": "research"})

    assert "labels may not set IdeGYM-managed keys: app, app.kubernetes.io/name" in str(caught.value)


@pytest.mark.parametrize("key", ["appliance", "team", "example.com/job", "idegym.example.com/task"])
def test_a_key_that_merely_resembles_a_managed_one_is_accepted(key) -> None:
    assert _request(labels={key: "value"}).labels == {key: "value"}


def test_an_annotation_may_use_a_managed_label_prefix() -> None:
    """Annotations carry no selector weight, so the label reservation does not apply to them."""
    assert _request(annotations={"app.kubernetes.io/notes": "long text"}).annotations


@pytest.mark.parametrize(
    "reserved",
    [
        "cluster-autoscaler.kubernetes.io/safe-to-evict",
        "podsnapshot.gke.io/ps-name",
        "podsnapshot.gke.io/anything",
        "prometheus.io/scrape",
        "prometheus.io/port",
    ],
)
def test_a_managed_annotation_key_is_rejected(reserved) -> None:
    with pytest.raises(ValidationError, match="annotations may not set IdeGYM-managed keys"):
        _request(annotations={reserved: "mine"})


def test_the_annotation_error_names_every_offending_key() -> None:
    with pytest.raises(ValidationError) as caught:
        _request(annotations={"prometheus.io/scrape": "false", "podsnapshot.gke.io/ps-name": "x", "team": "y"})

    assert "annotations may not set IdeGYM-managed keys: podsnapshot.gke.io/ps-name, prometheus.io/scrape" in str(
        caught.value
    )


@pytest.mark.parametrize("key", ["cluster-autoscaler.kubernetes.io/other", "example.com/prometheus.io", "gke.io/x"])
def test_an_annotation_that_merely_resembles_a_managed_one_is_accepted(key) -> None:
    assert _request(annotations={key: "value"}).annotations == {key: "value"}


@pytest.mark.parametrize(
    "labels",
    [{"": "value"}, {"Not A Key": "value"}, {"team": "x" * 64}],
)
def test_labels_are_held_to_the_kubernetes_syntax(labels) -> None:
    with pytest.raises(ValidationError):
        _request(labels=labels)


def test_annotation_values_are_not_length_limited_like_labels() -> None:
    """An annotation is where metadata too long to be a label goes."""
    assert _request(annotations={"example.com/notes": "x" * 5000}).annotations
