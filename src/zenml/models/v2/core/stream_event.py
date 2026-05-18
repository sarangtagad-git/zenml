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
"""Wire models for live event streaming on pipeline runs."""

from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from zenml.constants import (
    RESERVED_STREAM_EVENT_KINDS,
    STREAM_EVENT_KIND_PATTERN,
    STREAM_EVENT_MAX_BATCH_SIZE,
)


class StreamEvent(BaseModel):
    """Stream event."""

    pipeline_run_id: UUID
    step_run_id: Optional[UUID] = None
    step_name: Optional[str] = None
    kind: str = Field(pattern=STREAM_EVENT_KIND_PATTERN)
    correlation_id: Optional[str] = None
    index: Optional[int] = None
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("kind")
    @classmethod
    def _reject_reserved_kind(cls, value: str) -> str:
        """Reject kinds that collide with reserved SSE control event names.

        Args:
            value: The kind value to validate.

        Raises:
            ValueError: If the kind collides with a reserved SSE name.

        Returns:
            The validated kind value.
        """
        if value in RESERVED_STREAM_EVENT_KINDS:
            raise ValueError(
                f"Kind {value} collides with an SSE control event name "
                f"({sorted(RESERVED_STREAM_EVENT_KINDS)})"
            )
        return value


class StreamBatchRequest(BaseModel):
    """Stream batch request."""

    events: List[StreamEvent] = Field(max_length=STREAM_EVENT_MAX_BATCH_SIZE)


class StreamBatchResponse(BaseModel):
    """Stream batch response."""

    count: int
    last_id: Optional[str] = None
