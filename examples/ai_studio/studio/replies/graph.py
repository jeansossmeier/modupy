"""The reply agent: plain LangGraph, with nothing of modupy in it."""

from typing import Literal, TypedDict

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from studio.replies.model import ReplyModel

THANK = "Thank the customer for the review in one line."
APOLOGIZE = "Apologize to the customer in one line and offer help."

model: BaseChatModel = ReplyModel()


class ReviewState(TypedDict, total=False):
    product: str
    rating: int
    text: str
    reply: str


async def _reply(instruction: str, state: ReviewState) -> ReviewState:
    review = HumanMessage(f"{state['product']}\n\n{state['text']}")
    answer = await model.ainvoke([SystemMessage(instruction), review])
    return {"reply": str(answer.content)}


async def thank(state: ReviewState) -> ReviewState:
    return await _reply(THANK, state)


async def apologize(state: ReviewState) -> ReviewState:
    return await _reply(APOLOGIZE, state)


def route(state: ReviewState) -> Literal["thank", "apologize"]:
    return "thank" if state["rating"] >= 4 else "apologize"


builder = StateGraph(ReviewState)
builder.add_node("thank", thank)
builder.add_node("apologize", apologize)
builder.add_conditional_edges(START, route)
builder.add_edge("thank", END)
builder.add_edge("apologize", END)
graph = builder.compile()
