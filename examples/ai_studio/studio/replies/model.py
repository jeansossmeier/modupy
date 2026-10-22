"""A stand-in chat model, so the reply agent runs without an API key.

It answers from the instruction it gets. ``STUDIO_REPLIES_FAULT`` makes every
call fail on demand, as ``studio/faults.py`` describes. Any LangChain chat
model can replace it; the graph does not change.
"""

from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from studio.faults import fail, fault_for

FAULT = fault_for("replies")


class ReplyModel(BaseChatModel):
    @property
    def _llm_type(self) -> str:
        return "studio-scripted"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        instruction, review = (str(message.content) for message in messages[-2:])
        product = review.splitlines()[0]
        if instruction.startswith("Thank"):
            answer = f"Thank you for reviewing the {product}!"
        else:
            answer = f"We are sorry the {product} let you down; our team will be in touch."
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=answer))])

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        await fail(FAULT)
        return self._generate(messages, stop)
