"""A stand-in chat model, so the desk runs without an API key.

It answers from keywords, and fails on demand: ``DESK_AGENT_FAULT`` set to
``hang``, ``block`` or ``crash`` makes every call fail the way real model calls
do, and any other value that is not empty stops this module from importing.
Any LangChain chat model can replace it; the graph does not change.
"""

import asyncio
import os
import signal
import time
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

FAULTS = ("hang", "block", "crash")
FAULT = os.environ.get("DESK_AGENT_FAULT", "")
if FAULT not in ("", *FAULTS):
    raise ValueError(f"DESK_AGENT_FAULT must be empty or one of {', '.join(FAULTS)}, got {FAULT!r}")

CATEGORIES = {
    "billing": ("refund", "charge", "invoice", "card"),
    "outage": ("down", "error", "500", "timeout"),
}


def _classify(text: str) -> str:
    words = text.lower()
    category = next(
        (name for name, keys in CATEGORIES.items() if any(key in words for key in keys)),
        "question",
    )
    priority = "high" if category == "outage" or "urgent" in words else "normal"
    return f"{category} {priority}"


class DeskModel(BaseChatModel):
    @property
    def _llm_type(self) -> str:
        return "desk-scripted"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        instruction, ticket = (str(message.content) for message in messages[-2:])
        if instruction.startswith("Classify"):
            answer = _classify(ticket)
        else:
            answer = f"Thanks for writing in about {ticket.splitlines()[0]!r}; we are on it."
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=answer))])

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        if FAULT == "hang":
            # A request the provider accepts and never answers.
            await asyncio.Event().wait()
        elif FAULT == "block":
            # A sync SDK call, or CPU-bound work, on the event loop's thread.
            time.sleep(3600)
        elif FAULT == "crash":
            # What the OOM killer does to a process that loads too big a model.
            os.kill(os.getpid(), signal.SIGKILL)
        return self._generate(messages, stop)
