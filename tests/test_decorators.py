"""Decorator behavior tests.

Covers @event and @listener edge cases that aren't exercised by the
zero-config integration tests:
  - acceptance paths (async, sync, functools.wraps chains)
  - rejection paths (missing arg, missing annotation)
  - decorator-composition paths (functools.wraps, lru_cache, custom wrappers)

The runtime singleton is reset before each test so listener registrations
don't leak between cases.
"""

from __future__ import annotations

import functools
import inspect
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest

from modulith import ConfigurationError, event, externalized, listener, publish
from modulith.builtin import outbox
from modulith.manifest import Manifest, verify_manifest
from modulith.runtime import _runtime


# Event types are defined at MODULE scope on purpose. This file uses
# ``from __future__ import annotations`` (PEP 563), so a listener's event
# annotation reaches @listener as the *string* "E" — @listener resolves it
# against the function's module globals. A class defined in local (function)
# scope would be invisible to that resolution; module scope is the supported
# pattern for PEP 563 (see tests/test_sync.py for the no-future-import
# alternative used with local-scope events).
@event
@dataclass(frozen=True)
class E:
    pass


@pytest.fixture(autouse=True)
def _reset_runtime() -> None:
    """Each test gets a clean runtime so listener registrations don't leak."""
    _runtime._reset_for_testing()
    yield
    _runtime._reset_for_testing()


# ---------------------------------------------------------------------------
# @event
# ---------------------------------------------------------------------------


def test_event_marks_class() -> None:
    @event
    @dataclass(frozen=True)
    class OrderCreated:
        order_id: str

    assert OrderCreated.__modulith_event__ is True


# ---------------------------------------------------------------------------
# @listener — happy paths
# ---------------------------------------------------------------------------


def test_listener_accepts_async_function() -> None:
    @listener
    async def handler(evt: E) -> None:
        pass

    assert _runtime._pending_listeners or _runtime._event_bus is not None
    registered = _runtime._pending_listeners[0][1]
    assert registered.__modulith_broker_targets__ == ()


def test_listener_accepts_parameterized_broker_targets() -> None:
    @listener(
        broker_targets=[
            "redis-streams:orders",
            "amqp:events:orders",
        ]
    )
    async def handler(evt: E) -> None:
        pass

    registered = _runtime._pending_listeners[0][1]
    assert registered is handler
    assert registered.__modulith_broker_targets__ == (
        "redis-streams:orders",
        "amqp:events:orders",
    )


def test_listener_accepts_wrapped_async_function() -> None:
    """Regression test for B1: @functools.wraps-decorated async listeners
    must register correctly even though the wrapper hides the coroutine
    nature from naive isinstance/iscoroutinefunction checks."""

    def timing(f):
        @functools.wraps(f)
        async def wrapped(*args, **kwargs):
            return await f(*args, **kwargs)

        return wrapped

    @listener
    @timing
    async def handler(evt: E) -> None:
        pass

    # If the bug is present, the line above raises TypeError before reaching
    # this assertion. Reaching here means @listener accepted the wrapped form.
    assert callable(handler)


def test_listener_accepts_double_wrapped_async_function() -> None:
    """Multiple decorator layers (each preserving __wrapped__) must work."""

    def deco_one(f):
        @functools.wraps(f)
        async def wrapped(*args, **kwargs):
            return await f(*args, **kwargs)

        return wrapped

    def deco_two(f):
        @functools.wraps(f)
        async def wrapped(*args, **kwargs):
            return await f(*args, **kwargs)

        return wrapped

    @listener
    @deco_one
    @deco_two
    async def handler(evt: E) -> None:
        pass

    assert callable(handler)


def test_listener_accepts_sync_function() -> None:
    """T1.2.4: sync functions with a valid event annotation are now accepted."""

    @listener
    def handler(evt: E) -> None:
        pass

    # Returns the original sync function (not the async wrapper).
    assert callable(handler)
    assert not inspect.iscoroutinefunction(handler)


def test_listener_accepts_wrapped_sync_function() -> None:
    """A sync wrapper around a sync target is accepted (executor dispatch)."""

    def naive_wrap(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    def sync_target(evt: E) -> None:
        pass

    # Must not raise — sync listeners are now accepted.
    result = listener(naive_wrap(sync_target))
    assert callable(result)


@pytest.mark.asyncio
async def test_sync_wrapper_around_async_listener_registers_async_adapter() -> None:
    received: list[E] = []

    def sync_wrap(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    @listener(broker_targets=("redis-streams:orders",))
    @sync_wrap
    async def handler(evt: E) -> None:
        received.append(evt)

    registered = _runtime._pending_listeners[0][1]
    assert not inspect.iscoroutinefunction(handler)
    assert inspect.iscoroutinefunction(registered)
    assert registered.__modulith_broker_targets__ == ("redis-streams:orders",)

    event_instance = E()
    await registered(event_instance)
    assert received == [event_instance]


def test_sync_wrapper_around_async_listener_preserves_manifest_identity() -> None:
    def sync_wrap(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    @listener
    @sync_wrap
    async def handler(evt: E) -> None:
        pass

    registered = _runtime._pending_listeners[0][1]
    manifest = Manifest(package="not.importable", listeners=(handler,))

    assert verify_manifest(manifest, {registered}) == []


@pytest.mark.asyncio
async def test_listener_accepts_async_callable_class_instance() -> None:
    received: list[E] = []

    class AsyncCallableListener:
        async def __call__(self, evt: E) -> None:
            received.append(evt)

    obj = AsyncCallableListener()
    listener(obj)

    event_instance = E()
    registered = _runtime._pending_listeners[0][1]
    await registered(event_instance)

    assert received == [event_instance]


@pytest.mark.asyncio
async def test_listener_accepts_async_bound_method() -> None:
    calls: list[tuple[object, E]] = []

    class Service:
        async def on_order(self, evt: E) -> None:
            calls.append((self, evt))

    service = Service()
    bound = service.on_order

    returned = listener(broker_targets=("redis-streams:orders",))(bound)

    assert returned == bound
    registered = _runtime._pending_listeners[0][1]
    assert inspect.iscoroutinefunction(registered)
    assert registered.__modulith_broker_targets__ == ("redis-streams:orders",)
    event_instance = E()
    await registered(event_instance)
    assert calls == [(service, event_instance)]
    manifest = Manifest(package="not.importable", listeners=(bound,))
    assert verify_manifest(manifest, {registered}) == []


def test_async_bound_method_listener_id_is_prefixed_with_its_module_package(
    make_fake_app,
) -> None:
    """A bound method is named ``owner:module.Class.method``, however it registers."""
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass

                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class Placed:
                    order_id: str

                class Notifier:
                    async def on_placed(self, evt: Placed) -> None: ...

                listener(Notifier().on_placed)
            """
        }
    )
    _runtime.configure(package="fakeapp")
    import fakeapp.orders as orders

    _runtime.ensure_bootstrapped()

    assert _runtime.event_bus is not None
    handlers = _runtime.event_bus.listeners_for(orders.Placed)
    assert [outbox._listener_id(h) for h in handlers] == [
        "fakeapp.orders:fakeapp.orders.Notifier.on_placed"
    ]


class AsyncEmailSender:
    async def __call__(self, evt: E) -> None:
        pass


class AsyncSmsSender:
    async def __call__(self, evt: E) -> None:
        pass


async def async_on_e(evt: E) -> None:
    pass


def sync_on_e(evt: E) -> None:
    pass


def test_listener_ids_name_the_class_of_a_callable_instance() -> None:
    listener(AsyncEmailSender())
    listener(AsyncSmsSender())

    ids = [outbox._listener_id(handler) for _, handler in _runtime._pending_listeners]

    assert ids == [f"{__name__}.AsyncEmailSender", f"{__name__}.AsyncSmsSender"]


def test_listener_ids_of_plain_functions_are_module_qualified_names() -> None:
    listener(async_on_e)
    listener(sync_on_e)

    ids = [outbox._listener_id(handler) for _, handler in _runtime._pending_listeners]

    assert ids == [f"{__name__}.async_on_e", f"{__name__}.sync_on_e"]


@pytest.mark.asyncio
async def test_in_memory_publish_reaches_two_instances_of_one_class() -> None:
    received: list[str] = []

    class Named:
        def __init__(self, name: str) -> None:
            self.name = name

        async def __call__(self, evt: E) -> None:
            received.append(self.name)

    _runtime.configure(package="decoratortest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    listener(Named("first"))
    listener(Named("second"))

    await publish(E())

    assert sorted(received) == ["first", "second"]


@pytest.mark.parametrize(
    "broker_targets",
    [
        "redis-streams:orders",
        ("",),
        ("redis-streams",),
        (":orders",),
        ("redis-streams:",),
        (1,),
    ],
)
def test_listener_rejects_invalid_broker_targets(broker_targets: object) -> None:
    with pytest.raises(TypeError, match="broker_targets"):

        @listener(broker_targets=broker_targets)  # type: ignore[call-overload]
        async def handler(evt: E) -> None:
            pass


# ---------------------------------------------------------------------------
# @listener — rejection paths
# ---------------------------------------------------------------------------


def test_listener_rejects_missing_event_arg() -> None:
    with pytest.raises(TypeError, match="must accept an event argument"):

        @listener
        async def handler() -> None:
            pass


def test_listener_rejects_unannotated_event_arg() -> None:
    with pytest.raises(TypeError, match="annotate"):

        @listener
        async def handler(evt) -> None:  # type: ignore[no-untyped-def]
            pass


# ---------------------------------------------------------------------------
# @listener — static typing contract
# ---------------------------------------------------------------------------


def test_listener_preserves_the_handler_signature_for_type_checkers(tmp_path: Path) -> None:
    """A decorated handler must keep its own signature, not collapse to Any.

    This package ships ``py.typed``, so a decorator declared only as
    ``-> Any`` silently switches OFF type checking for every call to every
    ``@listener`` function in a user's app, and the parameterized form trips
    ``untyped-decorator`` under ``--strict``. Only a real type checker can
    observe that, so this drives mypy over a probe module.

    ``--follow-imports=silent`` keeps the assertions about the *probe*: errors
    inside ``modulith`` itself (which runs under the project's own, looser
    settings) must not leak into this file's output.
    """
    pytest.importorskip("mypy")

    probe = tmp_path / "probe.py"
    probe.write_text(
        textwrap.dedent(
            """
            from dataclasses import dataclass

            from modulith import event, listener


            @event
            @dataclass(frozen=True)
            class Placed:
                n: int


            @listener
            async def on_placed(evt: Placed) -> None: ...


            @listener(broker_targets=["redis-streams:orders"])
            async def on_placed2(evt: Placed) -> None: ...


            async def main() -> None:
                await on_placed("nope")
                await on_placed2()
            """
        )
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--disallow-untyped-decorators",
            "--follow-imports=silent",
            "--cache-dir",
            str(tmp_path / "mypy_cache"),
            str(probe),
        ],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
    )

    # The bare form keeps the parameter type: a str where the event goes is an error.
    assert "[arg-type]" in result.stdout, result.stdout
    # The parameterized form keeps the arity AND is itself typed.
    assert "[call-arg]" in result.stdout, result.stdout
    assert "untyped-decorator" not in result.stdout, result.stdout


# ---------------------------------------------------------------------------
# @externalized — target validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("target", "stored"),
    [
        (" redis-streams : orders.placed ", "redis-streams:orders.placed"),
        ("redis-streams: orders.placed", "redis-streams:orders.placed"),
        ("amqp: orders:placed.eu ", "amqp:orders:placed.eu"),
    ],
)
def test_externalized_stores_whitespace_normalized_target(target: str, stored: str) -> None:
    @externalized(target=target)
    @dataclass(frozen=True)
    class Placed:
        order_id: str

    assert Placed.__modulith_broker_target__ == stored  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "target", ["", "   ", "orders", "redis-streams:", "redis-streams:  ", " :orders"]
)
def test_externalized_rejects_target_without_scheme_or_destination(target: str) -> None:
    with pytest.raises(ConfigurationError, match="scheme:destination"):

        @externalized(target=target)
        @dataclass(frozen=True)
        class Placed:
            order_id: str


@pytest.mark.parametrize(
    "target",
    [123, ["redis-streams:orders"], b"redis-streams:orders", object()],
    ids=["int", "list", "bytes", "object"],
)
def test_externalized_rejects_non_string_target(target: object) -> None:
    with pytest.raises(ConfigurationError) as excinfo:
        externalized(target=target)  # type: ignore[arg-type]

    message = str(excinfo.value)
    assert repr(target) in message
    assert "scheme:destination" in message
