#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for broker frame encoding/decoding."""

import uuid

import pytest
from fastapi import HTTPException

from zenml.models import StreamEvent
from zenml.zen_server.streaming.brokers.frames import (
    EVENT_PAYLOAD_BYTES_MAX,
    EndFrame,
    EventFrame,
    UnknownFrame,
    decode_frame,
    encode_event_for_publish,
    encode_frame,
)


def _ev(run_id: uuid.UUID, kind: str = "token") -> StreamEvent:
    return StreamEvent(pipeline_run_id=run_id, kind=kind, payload={"v": 1})


def test_decode_round_trips_endframe():
    """An encoded EndFrame round-trips through `decode_frame`."""
    run_id = uuid.uuid4()
    payload = encode_frame(EndFrame(pipeline_run_id=run_id))
    decoded = decode_frame(payload)
    assert isinstance(decoded, EndFrame)
    assert decoded.pipeline_run_id == run_id


def test_decode_unknown_frame_type_yields_unknownframe():
    """Forward-compat: unknown `type` values decode as `UnknownFrame`."""
    decoded = decode_frame(b'{"type":"future_frame","whatever":1}')
    assert isinstance(decoded, UnknownFrame)
    assert decoded.type == "future_frame"


def test_decode_corrupt_payload_yields_unknown_question_mark():
    """A non-object JSON payload returns `UnknownFrame(type="?")`."""
    decoded = decode_frame(b"[]")
    assert isinstance(decoded, UnknownFrame)
    assert decoded.type == "?"


def test_encode_event_for_publish_rejects_run_id_mismatch():
    """A URL run id that doesn't match the event's run id raises 400."""
    event = _ev(uuid.uuid4())
    with pytest.raises(HTTPException) as exc:
        encode_event_for_publish(event, uuid.uuid4())
    assert exc.value.status_code == 400


def test_encode_event_for_publish_wraps_in_event_frame():
    """Producer payloads land on the broker tagged as `EventFrame`."""
    run_id = uuid.uuid4()
    event = _ev(run_id, kind="custom")
    payload = encode_event_for_publish(event, run_id)
    frame = decode_frame(payload)
    assert isinstance(frame, EventFrame)
    assert frame.event.kind == "custom"


def test_encode_event_for_publish_rejects_envelope_overage():
    """A payload that blows the wire envelope cap returns 413.

    The model no longer rejects oversize payloads at construction
    (that check lives on `streams.publishing.publish` to avoid re-encoding
    on server-side deserialization). The wire envelope check is the
    server-side authoritative gate.
    """
    run_id = uuid.uuid4()
    # Bypass any local validation and forge a payload that clearly
    # exceeds the envelope cap.
    bloat_payload = {"v": "x" * (EVENT_PAYLOAD_BYTES_MAX * 2)}
    forged = StreamEvent.model_construct(
        pipeline_run_id=run_id,
        kind="big",
        payload=bloat_payload,
    )
    with pytest.raises(HTTPException) as exc:
        encode_event_for_publish(forged, run_id)
    assert exc.value.status_code == 413


def test_encode_event_for_publish_returns_bytes():
    """Producer payload is bytes the broker decoder can parse back."""
    run_id = uuid.uuid4()
    event = _ev(run_id)
    payload = encode_event_for_publish(event, run_id)
    frame = decode_frame(payload)
    assert isinstance(frame, EventFrame)
    assert frame.event.pipeline_run_id == run_id
    assert frame.event.kind == "token"
