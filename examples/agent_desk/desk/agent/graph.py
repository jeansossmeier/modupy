"""The triage agent: plain LangGraph, with nothing of modupy in it."""

from typing import TypedDict

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from desk.agent.model import DeskModel

CLASSIFY = (
    "Classify the support ticket. Answer with a category (billing, outage or question) "
    "and a priority (high or normal), separated by a space."
)
DRAFT = "Draft a one-line first reply to the customer."

model: BaseChatModel = DeskModel()


class TicketState(TypedDict, total=False):
    subject: str
    body: str
    category: str
    priority: str
    reply: str


def _ticket(state: TicketState) -> HumanMessage:
    return HumanMessage(f"{state['subject']}\n\n{state['body']}")


async def classify(state: TicketState) -> TicketState:
    answer = await model.ainvoke([SystemMessage(CLASSIFY), _ticket(state)])
    category, _, priority = str(answer.content).partition(" ")
    if category not in ("billing", "outage", "question"):
        category = "question"
    return {"category": category, "priority": "high" if priority == "high" else "normal"}


async def draft(state: TicketState) -> TicketState:
    answer = await model.ainvoke([SystemMessage(DRAFT), _ticket(state)])
    return {"reply": str(answer.content)}


builder = StateGraph(TicketState)
builder.add_node("classify", classify)
builder.add_node("draft", draft)
builder.add_edge(START, "classify")
builder.add_edge("classify", "draft")
builder.add_edge("draft", END)
graph = builder.compile()
