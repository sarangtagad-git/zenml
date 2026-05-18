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
"""Frame types stored as payloads in the broker."""

import json
from typing import Annotated, FrozenSet, Literal, Optional, Union
from uuid import UUID

from fastapi import HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from zenml.constants import STREAM_EVENT_PAYLOAD_BYTES_MAX
from zenml.logger import get_logger
from zenml.models import StreamEvent

logger = get_logger(__name__)

_ENVELOPE_OVERHEAD_BYTES: int = 4 * 1024
EVENT_PAYLOAD_BYTES_MAX: int = (
    STREAM_EVENT_PAYLOAD_BYTES_MAX + _ENVELOPE_OVERHEAD_BYTES
)


class EventFrame(BaseModel):
    """Event frame."""

    model_config = ConfigDict(frozen=True)

    type: Literal["event"] = "event"
    event: StreamEvent


class EndFrame(BaseModel):
    """End frame."""

    model_config = ConfigDict(frozen=True)

    type: Literal["end"] = "end"
    pipeline_run_id: UUID


class UnknownFrame(BaseModel):
    """Unknown frame."""

    model_config = ConfigDict(frozen=True)

    type: str


BrokerFrame = Annotated[
    Union[EventFrame, EndFrame], Field(discriminator="type")
]

_frame_adapter: TypeAdapter[BrokerFrame] = TypeAdapter(BrokerFrame)

DecodedFrame = Union[EventFrame, EndFrame, UnknownFrame]

_KNOWN_FRAME_TYPES: FrozenSet[str] = frozenset({"event", "end"})


def encode_frame(frame: BrokerFrame) -> bytes:
    """Encode a frame as JSON bytes for the broker.

    Args:
        frame: The frame to encode.

    Returns:
        The JSON-encoded payload.
    """
    return _frame_adapter.dump_json(frame)


def decode_frame(payload: bytes) -> DecodedFrame:
    """Decode a broker payload into a frame.

    Args:
        payload: Raw bytes pulled from the broker.

    Returns:
        A parsed frame, or `UnknownFrame` for unknown types.
    """
    try:
        return _frame_adapter.validate_json(payload)
    except ValidationError:
        parsed = json.loads(payload)
        type_value: Optional[object] = (
            parsed.get("type") if isinstance(parsed, dict) else None
        )
        type_str = str(type_value) if type_value else "?"
        if type_str in _KNOWN_FRAME_TYPES:
            # Known type with malformed body = server-side corrupt frame.
            logger.warning(
                "Corrupt %r frame on the broker.",
                type_str,
            )
        return UnknownFrame(type=type_str)


def encode_event_for_publish(
    event: StreamEvent, pipeline_run_id: UUID
) -> bytes:
    """Wrap a producer event in an `EventFrame` envelope for the broker.

    Args:
        event: Event to validate and encode.
        pipeline_run_id: Run id from the URL. Must match the event.

    Raises:
        HTTPException: 400 on URL/run mismatch, 413 on oversize payload.

    Returns:
        The JSON-encoded envelope as bytes.
    """
    if event.pipeline_run_id != pipeline_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Event pipeline_run_id {event.pipeline_run_id} does not "
                f"match URL run id {pipeline_run_id}."
            ),
        )
    payload = encode_frame(EventFrame(event=event))
    if len(payload) > EVENT_PAYLOAD_BYTES_MAX:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                f"Event exceeds {EVENT_PAYLOAD_BYTES_MAX} bytes "
                f"(was {len(payload)})."
            ),
        )
    return payload
