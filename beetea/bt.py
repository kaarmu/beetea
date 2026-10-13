"""Reactive behavior-tree primitives with optional subtree scheduling.

All tree elements implement :class:`BTNode`. Composites tick reactively from
their first child, while :class:`BehaviorTree` controls whether ticks propagate
synchronously or run at an independent rate. Node statuses and tree snapshots
provide observation without changing execution behavior.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from enum import IntEnum, auto
import builtins
from itertools import count
import logging
import math
from threading import Event, Lock, RLock, Thread, current_thread
import time
from typing import Callable, Iterator, Mapping


class BTResponse(IntEnum):
    """Control-flow result returned by a node to its parent.

    ``SUCCESS`` and ``FAILURE`` are terminal for the current tick.
    ``RUNNING`` asks the parent to keep the node active and tick it again.
    """

    SUCCESS = auto()
    RUNNING = auto()
    FAILURE = auto()


class BTStatus(IntEnum):
    """Latest observable state of a node.

    The first three values mirror :class:`BTResponse`. ``SKIPPED`` means the
    node is inactive because it has not been selected or has been halted.
    """

    SUCCESS = auto()
    RUNNING = auto()
    FAILURE = auto()
    SKIPPED = auto()


_id_counter = count(1)
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="bt-action")
_logger = logging.getLogger(__name__)


def _new_id() -> str:
    """Create a process-local ID for nodes without an explicit identity."""
    return f"node-{next(_id_counter)}"


def _check_id(id: str) -> str:
    """Reject IDs that cannot be written in the program format."""
    if not id or any(character.isspace() for character in id):
        raise ValueError("BTNode IDs must be non-empty and contain no whitespace")
    return id


def node_identity(node: BTNode) -> int:
    """Return Python object identity separately from a node's public ID.

    Object identity is used during replacement to distinguish reused node
    instances from new instances that happen to have the same public ID.

    Args:
        node: Node whose in-process identity is required.

    Returns:
        The identity returned by Python's built-in :func:`id`.
    """
    return builtins.id(node)


def _response_status(response: BTResponse) -> BTStatus:
    """Map a control-flow response to its observable status."""
    return {
        BTResponse.SUCCESS: BTStatus.SUCCESS,
        BTResponse.RUNNING: BTStatus.RUNNING,
        BTResponse.FAILURE: BTStatus.FAILURE,
    }[response]


class BTNode:
    """Base interface shared by every behavior-tree node.

    Nodes begin in :attr:`BTStatus.SKIPPED`. Subclasses implement
    :meth:`tick` and override :meth:`halt` when they own child nodes or
    asynchronous work. Status listeners are observational and run
    synchronously in the thread that changes the status.
    """

    def __init__(self, id: str | None = None):
        """Initialize node identity and inactive status.

        Args:
            id: Optional whitespace-free name, unique within a tree. A
                process-local ID is generated when omitted.
        """
        self._id = _check_id(id) if id is not None else _new_id()
        self._status = BTStatus.SKIPPED
        self._status_listeners: list[Callable[[BTNode, BTStatus, BTStatus], None]] = []

    @property
    def id(self) -> str:
        """Return the stable identity used by lookup, events, and snapshots."""
        return self._id

    def tick(self) -> BTResponse:
        """Evaluate one non-blocking step.

        Returns:
            The control-flow result consumed by the parent node.

        Raises:
            NotImplementedError: If a subclass does not implement ticking.
        """
        raise NotImplementedError

    def halt(self) -> None:
        """Deactivate the node so a later tick starts a fresh activation.

        Stateful subclasses should cancel or invalidate owned work before
        delegating to this implementation.
        """
        self._set_status(BTStatus.SKIPPED)

    def get_status(self) -> BTStatus:
        """Return the node's latest observable state."""
        return self._status

    def children(self) -> tuple[BTNode, ...]:
        """Return structural children for traversal and introspection.

        Leaf nodes return an empty tuple. Child order is evaluation order for
        composites.
        """
        return ()

    def add_status_listener(self, callback: Callable[[BTNode, BTStatus, BTStatus], None]) -> None:
        """Subscribe to status transitions without polling the node.

        Args:
            callback: Called as ``callback(node, old_status, new_status)``.
                Registering the same callback more than once has no effect.
        """
        if callback not in self._status_listeners:
            self._status_listeners.append(callback)

    def remove_status_listener(self, callback: Callable[[BTNode, BTStatus, BTStatus], None]) -> None:
        """Stop delivering status transitions to ``callback``.

        Removing a callback that is not registered has no effect.
        """
        if callback in self._status_listeners:
            self._status_listeners.remove(callback)

    def _set_status(self, status: BTStatus) -> None:
        """Store a transition and notify listeners only when the value changes."""
        old = self._status
        if old == status:
            return
        self._status = status
        for callback in tuple(self._status_listeners):
            callback(self, old, status)

    def get_program(self) -> str:
        """Render the node and its identity in the indented program format."""
        name = self.__class__.__name__.lower()
        return _format_name(name, self.id)

    def with_children(self, children: tuple[BTNode, ...]) -> BTNode:
        """Return the node rebuilt around ``children``.

        Structural nodes override this hook so subtree replacement can reuse
        unaffected instances and preserve their state.

        Raises:
            TypeError: If this node type cannot own children.
        """
        raise TypeError(f"{self.__class__.__name__} does not have children")


def _format_name(name: str, id: str) -> str:
    """Format a program line with the node keyword and ID."""
    return f"{name} #{id}"


class Success(BTNode):
    """Stateless leaf that always returns :attr:`BTResponse.SUCCESS`."""

    def tick(self) -> BTResponse:
        """Return success and publish the matching status."""
        self._set_status(BTStatus.SUCCESS)
        return BTResponse.SUCCESS


class Failure(BTNode):
    """Stateless leaf that always returns :attr:`BTResponse.FAILURE`."""

    def tick(self) -> BTResponse:
        """Return failure and publish the matching status."""
        self._set_status(BTStatus.FAILURE)
        return BTResponse.FAILURE


class Running(BTNode):
    """Leaf that returns :attr:`BTResponse.RUNNING` until it is halted."""

    def tick(self) -> BTResponse:
        """Keep the node active by returning and publishing ``RUNNING``."""
        self._set_status(BTStatus.RUNNING)
        return BTResponse.RUNNING


class Condition(BTNode):
    """Evaluate a predicate as a reactive success-or-failure leaf.

    The predicate runs on every tick so changes are visible immediately.
    Predicates should therefore be non-blocking and free of long-running I/O.
    """

    def __init__(self, predicate: Callable[[], bool], id: str | None = None):
        """Create a condition around ``predicate``.

        Args:
            predicate: Zero-argument callable whose truth value determines the
                response.
            id: Optional unique, whitespace-free name.
        """
        super().__init__(id)
        self.predicate = predicate

    def tick(self) -> BTResponse:
        """Evaluate the predicate and return ``SUCCESS`` or ``FAILURE``."""
        response = BTResponse.SUCCESS if self.predicate() else BTResponse.FAILURE
        self._set_status(_response_status(response))
        return response

    def get_program(self) -> str:
        """Render the node using its behavior-tree keyword."""
        return _format_name("condition", self.id)


class Action(BTNode):
    """Run one callable asynchronously per activation.

    The first tick submits the callable and returns ``RUNNING``. Completion is
    cached until :meth:`halt` resets the activation. ``None``, ``True``, and
    ``BTResponse.SUCCESS`` mean success; ``False`` and
    ``BTResponse.FAILURE`` mean failure. Exceptions and unsupported return
    values are logged, exposed through :attr:`last_exception`, and converted
    to failure.
    """

    def __init__(self, action: Callable[[], bool | BTResponse | None], id: str | None = None):
        """Create an asynchronous callable leaf.

        Args:
            action: Zero-argument callable that eventually succeeds or fails.
            id: Optional unique, whitespace-free name.
        """
        super().__init__(id)
        self.action = action
        self._future: Future[BTResponse] | None = None
        self._completed_response: BTResponse | None = None
        self._last_exception: Exception | None = None
        self._generation = 0
        self._lock = Lock()

    def _run(self, generation: int) -> BTResponse:
        """Normalize callable output and turn exceptions into node failures."""
        try:
            result = self.action()
            if result is None or result is True or result == BTResponse.SUCCESS:
                return BTResponse.SUCCESS
            if result is False or result == BTResponse.FAILURE:
                return BTResponse.FAILURE
            if result == BTResponse.RUNNING:
                raise ValueError("Action callable must complete with SUCCESS/FAILURE")
            raise ValueError(f"unsupported Action result: {result!r}")
        except Exception as exception:
            self._last_exception = exception
            _logger.exception("Action callable failed")
            return BTResponse.FAILURE

    @property
    def last_exception(self) -> Exception | None:
        """Return the most recent callable or result-normalization exception."""
        return self._last_exception

    def _completed(self, future: Future[BTResponse], generation: int) -> None:
        """Publish completion only when it belongs to the active generation."""
        response = future.result()
        with self._lock:
            if generation != self._generation or future is not self._future:
                return
            self._future = None
            self._completed_response = response
        self._set_status(_response_status(response))

    def tick(self) -> BTResponse:
        """Launch once per activation, then return the cached completion.

        Returns:
            ``RUNNING`` while work is pending, otherwise the terminal result.
        """
        launch = False
        with self._lock:
            if self._completed_response is not None:
                return self._completed_response
            if self._future is None:
                generation = self._generation
                future = _executor.submit(self._run, generation)
                self._future = future
                launch = True
            else:
                future = self._future
        if launch:
            self._set_status(BTStatus.RUNNING)
            future.add_done_callback(lambda done: self._completed(done, generation))
            return BTResponse.RUNNING
        if not future.done():
            self._set_status(BTStatus.RUNNING)
        return BTResponse.RUNNING

    def halt(self) -> None:
        """Reset the activation and ignore any stale asynchronous completion.

        Python cannot stop a callable that is already executing. Generation
        tracking prevents such a callable from updating the node after halt.
        """
        with self._lock:
            self._generation += 1
            future = self._future
            self._future = None
            self._completed_response = None
        if future is not None:
            future.cancel()
        self._set_status(BTStatus.SKIPPED)

    def get_program(self) -> str:
        """Render the node ID for diagnostics."""
        return _format_name("action", self.id)


class Composite(BTNode):
    """Base class for nodes that coordinate ordered children.

    Composites halt children that are no longer selected by reactive traversal
    so stateful work cannot continue outside the active branch.
    """

    def __init__(self, children: tuple[BTNode, ...] = (), id: str | None = None):
        """Create a composite with a fixed evaluation order.

        Args:
            children: Nodes in the order they should be evaluated.
            id: Optional unique, whitespace-free name.
        """
        super().__init__(id)
        self._children = tuple(children)

    def children(self) -> tuple[BTNode, ...]:
        """Return children in evaluation order."""
        return self._children

    def _skip_from(self, index: int) -> None:
        """Halt children no longer selected by reactive traversal."""
        for child in self._children[index:]:
            if child.get_status() != BTStatus.SKIPPED:
                child.halt()

    def halt(self) -> None:
        """Halt active descendants before marking the composite inactive."""
        for child in self._children:
            if child.get_status() != BTStatus.SKIPPED:
                child.halt()
        self._set_status(BTStatus.SKIPPED)

    def with_children(self, children: tuple[BTNode, ...]) -> BTNode:
        """Rebuild the same composite type for structural replacement."""
        return type(self)(children, id=self.id)

    def get_program(self) -> str:
        """Render the composite followed by its indented child programs."""
        lines = [_format_name(self.__class__.__name__.lower(), self.id)]
        for child in self._children:
            lines.extend("  " + line for line in child.get_program().splitlines())
        return "\n".join(lines)


class Sequence(Composite):
    """Require every child to succeed in order.

    Evaluation stops at the first ``RUNNING`` or ``FAILURE`` response. An
    empty sequence succeeds, matching the identity value of logical AND.
    """

    def tick(self) -> BTResponse:
        """Require each child to succeed, stopping at the first other result."""
        for index, child in enumerate(self._children):
            response = child.tick()
            if response == BTResponse.SUCCESS:
                continue
            self._skip_from(index + 1)
            self._set_status(_response_status(response))
            return response
        self._set_status(BTStatus.SUCCESS)
        return BTResponse.SUCCESS


class Fallback(Composite):
    """Try children in order until one does not fail.

    Evaluation stops at the first ``RUNNING`` or ``SUCCESS`` response. An
    empty fallback fails, matching the identity value of logical OR.
    """

    def tick(self) -> BTResponse:
        """Try children until one does not fail."""
        for index, child in enumerate(self._children):
            response = child.tick()
            if response == BTResponse.FAILURE:
                continue
            self._skip_from(index + 1)
            self._set_status(_response_status(response))
            return response
        self._set_status(BTStatus.FAILURE)
        return BTResponse.FAILURE


def walk(root: BTNode) -> Iterator[BTNode]:
    """Traverse a node graph depth-first, including ``root`` itself.

    Args:
        root: Node at which traversal begins.

    Yields:
        Each node in pre-order, respecting structural child order.
    """
    yield root
    for child in root.children():
        yield from walk(child)


def _validate(root: BTNode) -> tuple[BTNode, ...]:
    """Materialize a graph and reject IDs that would make lookup ambiguous."""
    nodes = tuple(walk(root))
    ids = [node.id for node in nodes]
    if len(ids) != len(set(ids)):
        raise ValueError("node IDs must be unique within a BehaviorTree")
    return nodes


def _replace(node: BTNode, target_id: str, replacement: BTNode) -> BTNode:
    """Rebuild the path to ``target_id`` while reusing unaffected nodes."""
    if node.id == target_id:
        return replacement
    children = node.children()
    if not children:
        return node
    updated = tuple(_replace(child, target_id, replacement) for child in children)
    return node if updated == children else node.with_children(updated)


@dataclass(frozen=True)
class BTSnapshot:
    """Immutable view of a tree at one structure revision.

    Attributes:
        revision: Structure revision captured by the snapshot.
        program: Indented representation of the captured structure.
        statuses: Latest status for every captured node, keyed by node ID.
    """

    revision: int
    program: str
    statuses: dict[str, BTStatus]


@dataclass(frozen=True)
class _TreeState:
    """Atomically replaceable structural state of a behavior tree."""

    root: BTNode
    nodes: tuple[BTNode, ...]
    revision: int


class BehaviorTree(BTNode):
    """Own a root node and control how it is ticked.

    With ``tick_rate=None``, :meth:`tick` synchronously propagates to the root.
    This is useful when the parent should control every evaluation. With a
    positive rate, the first :meth:`start` or :meth:`tick` launches a dedicated
    scheduler thread. Parent ticks then read the latest cached response without
    controlling the inner rate.

    A scheduled tree remains reactive after ``SUCCESS`` or ``FAILURE`` and
    continues ticking until :meth:`halt` is called. The tree also implements
    :class:`BTNode`, so it can be placed anywhere another node is accepted.
    """

    def __init__(
        self,
        root: BTNode,
        *,
        tick_rate: float | None = None,
        id: str | None = None,
    ):
        """Create a tree around ``root``.

        Args:
            root: Root node evaluated by this tree.
            tick_rate: Independent tick frequency in hertz. ``None`` selects
                synchronous manual propagation.
            id: Optional unique, whitespace-free name for the tree node.

        Raises:
            TypeError: If ``root`` is not a :class:`BTNode`.
            ValueError: If ``tick_rate`` is not positive and finite, or if a
                node ID is duplicated within the tree.
        """
        if not isinstance(root, BTNode):
            raise TypeError("root must be a BTNode")
        if tick_rate is not None:
            tick_rate = float(tick_rate)
            if not math.isfinite(tick_rate) or tick_rate <= 0:
                raise ValueError("tick_rate must be positive or None")
        super().__init__(id=id)
        self._tick_rate = tick_rate
        self._tree_status_listeners: list[Callable[[int, BTNode, BTStatus, BTStatus], None]] = []
        self._structure_listeners: list[Callable[[int, str], None]] = []
        nodes = _validate(root)
        if self.id in {node.id for node in nodes}:
            raise ValueError("node IDs must be unique within a BehaviorTree")
        self._state = _TreeState(root, nodes, 0)
        self._state_lock = RLock()
        self._control_lock = RLock()
        self._tick_lock = RLock()
        self._active = False
        self._activation = 0
        self._cached_response = BTResponse.RUNNING
        self._stop_event: Event | None = None
        self._runner: Thread | None = None
        super().add_status_listener(self._on_tree_node_status)
        self._register(nodes)

    @property
    def root(self) -> BTNode:
        """Return the current contained root node."""
        with self._state_lock:
            return self._state.root

    @property
    def revision(self) -> int:
        """Return the structure revision incremented by :meth:`replace`."""
        with self._state_lock:
            return self._state.revision

    @property
    def tick_rate(self) -> float | None:
        """Return scheduler frequency in hertz, or ``None`` for manual mode."""
        return self._tick_rate

    @property
    def is_active(self) -> bool:
        """Report whether the current activation has started.

        Manual trees become active on their first tick. Scheduled trees become
        active when their scheduler starts. :meth:`halt` makes either mode
        inactive.
        """
        with self._state_lock:
            return self._active

    def children(self) -> tuple[BTNode, ...]:
        """Expose the root so nested trees remain structurally transparent."""
        return (self.root,)

    def with_children(self, children: tuple[BTNode, ...]) -> BTNode:
        """Rebuild a nested tree while retaining identity and run mode.

        Args:
            children: Exactly one node to use as the new root.

        Returns:
            A new tree with the same ID and tick rate.

        Raises:
            ValueError: If ``children`` does not contain exactly one node.
        """
        if len(children) != 1:
            raise ValueError("BehaviorTree requires exactly one root")
        return type(self)(
            children[0],
            tick_rate=self.tick_rate,
            id=self.id,
        )

    def _on_tree_node_status(self, node: BTNode, old: BTStatus, new: BTStatus) -> None:
        """Forward the tree node's own transitions to tree-wide listeners."""
        self._on_status(node, old, new)

    def _on_status(self, node: BTNode, old: BTStatus, new: BTStatus) -> None:
        """Attach the current revision to descendant status transitions."""
        with self._state_lock:
            revision = self._state.revision
            callbacks = tuple(self._tree_status_listeners)
        for callback in callbacks:
            callback(revision, node, old, new)

    def _register(self, nodes: tuple[BTNode, ...]) -> None:
        """Observe nodes so one tree-level stream covers every descendant."""
        for node in nodes:
            node.add_status_listener(self._on_status)

    def _unregister(self, nodes: tuple[BTNode, ...]) -> None:
        """Detach tree-level observation from nodes leaving the structure."""
        for node in nodes:
            node.remove_status_listener(self._on_status)

    def _run(self, activation: int, stop_event: Event) -> None:
        """Tick at a steady rate and reject results from obsolete activations."""
        assert self._tick_rate is not None
        period = 1.0 / self._tick_rate
        deadline = time.monotonic()
        while not stop_event.is_set():
            with self._tick_lock:
                with self._state_lock:
                    if not self._active or activation != self._activation:
                        return
                    root = self._state.root
                try:
                    response = root.tick()
                except Exception:
                    _logger.exception("BehaviorTree root tick failed")
                    response = BTResponse.FAILURE

                with self._state_lock:
                    if (
                        stop_event.is_set()
                        or not self._active
                        or activation != self._activation
                    ):
                        return
                    accepted = root is self._state.root
                    if accepted:
                        self._cached_response = response
                if accepted:
                    self._set_status(_response_status(response))

            deadline += period
            now = time.monotonic()
            if deadline < now:
                deadline = now + period
            stop_event.wait(max(0.0, deadline - now))

    def _start_locked(self) -> None:
        """Create one scheduler thread while the control lock is held."""
        with self._state_lock:
            if self._active:
                return
            self._active = True
            self._activation += 1
            activation = self._activation
            self._cached_response = BTResponse.RUNNING
            stop_event = Event()
            runner = Thread(
                target=self._run,
                args=(activation, stop_event),
                name=f"bt-tree-{self.id}",
                daemon=True,
            )
            self._stop_event = stop_event
            self._runner = runner
            self._set_status(BTStatus.RUNNING)
            runner.start()

    def start(self) -> None:
        """Start the independent scheduler when a tick rate is configured.

        The first internal tick is requested immediately. Calling ``start`` on
        an active tree, or on a manual tree, has no effect.
        """
        if self._tick_rate is None:
            return
        with self._control_lock:
            self._start_locked()

    def tick(self) -> BTResponse:
        """Advance a manual tree or poll a scheduled tree.

        In manual mode the call synchronously reaches the root and returns its
        response. In scheduled mode the first call starts the scheduler and
        returns ``RUNNING``; later calls return the latest cached response.

        Returns:
            The root response in manual mode, or the cached scheduler response
            in scheduled mode.

        Raises:
            Exception: Propagates an exception raised by a manually ticked
                root. Scheduled-root exceptions are logged and cached as
                ``FAILURE`` instead.
        """
        if self._tick_rate is None:
            with self._control_lock:
                with self._state_lock:
                    if not self._active:
                        self._active = True
                        self._activation += 1
                        self._set_status(BTStatus.RUNNING)
                    activation = self._activation
                with self._tick_lock:
                    root = self.root
                    try:
                        response = root.tick()
                    except Exception:
                        self._set_status(BTStatus.FAILURE)
                        raise
                    with self._state_lock:
                        accepted = (
                            self._active
                            and activation == self._activation
                            and root is self._state.root
                        )
                        if accepted:
                            self._cached_response = response
                    if accepted:
                        self._set_status(_response_status(response))
                    return response
        with self._control_lock:
            with self._state_lock:
                if self._active:
                    return self._cached_response
            self._start_locked()
            return BTResponse.RUNNING

    def _stop(self, *, halt_root: bool) -> None:
        """Stop scheduling and optionally halt the retained root structure."""
        with self._control_lock:
            with self._state_lock:
                if not self._active and self.get_status() == BTStatus.SKIPPED:
                    return
                self._active = False
                self._activation += 1
                stop_event = self._stop_event
                runner = self._runner
                self._stop_event = None
                self._runner = None
            if stop_event is not None:
                stop_event.set()
            if runner is not None and runner is not current_thread():
                runner.join()
            if halt_root:
                with self._tick_lock:
                    self.root.halt()
            with self._state_lock:
                self._cached_response = BTResponse.RUNNING
            self._set_status(BTStatus.SKIPPED)

    def halt(self) -> None:
        """Stop the current activation and recursively halt the root.

        A scheduler thread is interrupted and joined before the root is halted,
        preventing another internal tick after this method returns. Calling
        ``halt`` on an already inactive tree has no effect.
        """
        self._stop(halt_root=True)

    def walk(self) -> Iterator[BTNode]:
        """Traverse this tree node and all descendants in pre-order.

        Returns:
            An iterator beginning with the :class:`BehaviorTree` object itself.
        """
        return walk(self)

    def find(self, id: str) -> BTNode:
        """Find the node with ``id``.

        Args:
            id: Node ID to match.

        Returns:
            The matching node, including this tree node when applicable.

        Raises:
            KeyError: If no node has the ID.
        """
        with self._state_lock:
            nodes = (self,) + self._state.nodes
        for node in nodes:
            if node.id == id:
                return node
        raise KeyError(id)

    def replace(self, subtree: BTNode, id: str) -> None:
        """Replace one subtree while preserving unaffected node instances.

        Replacement is serialized with ticking. Removed active nodes are
        halted, reused nodes retain their state, and scheduled execution
        continues against the new root. A successful replacement increments
        :attr:`revision` and notifies structure listeners.

        Args:
            subtree: Node to install at the selected location.
            id: ID of the node to replace.

        Raises:
            ValueError: If the tree itself is selected or the resulting
                structure contains duplicate IDs.
            KeyError: If no node has the ID.
        """
        target = self.find(id)
        if target is self:
            raise ValueError("a BehaviorTree cannot replace itself")
        with self._tick_lock:
            with self._state_lock:
                old_state = self._state
                new_root = _replace(old_state.root, target.id, subtree)
                new_nodes = _validate(new_root)
                if self.id in {node.id for node in new_nodes}:
                    raise ValueError("node IDs must be unique within a BehaviorTree")
                old_node_ids = {node_identity(node) for node in old_state.nodes}
                new_node_ids = {node_identity(node) for node in new_nodes}
                self._register(tuple(
                    node for node in new_nodes
                    if node_identity(node) not in old_node_ids
                ))
                self._state = _TreeState(new_root, new_nodes, old_state.revision + 1)
                self._unregister(tuple(
                    node for node in old_state.nodes
                    if node_identity(node) not in new_node_ids
                ))
            self._halt_detached(old_state.root, old_node_ids - new_node_ids)
        for callback in tuple(self._structure_listeners):
            callback(self.revision, self.get_program())

    def _halt_detached(self, node: BTNode, detached: set[int], parent_detached: bool = False) -> None:
        """Halt removed work without resetting descendants reused elsewhere."""
        is_detached = node_identity(node) in detached
        has_retained_descendant = any(
            node_identity(descendant) not in detached
            for descendant in walk(node)
            if descendant is not node
        )
        if is_detached and isinstance(node, BehaviorTree) and has_retained_descendant:
            node._stop(halt_root=False)
        if is_detached and not parent_detached and not has_retained_descendant:
            if node.get_status() != BTStatus.SKIPPED:
                node.halt()
            return
        for child in node.children():
            self._halt_detached(child, detached, parent_detached)

    def get_program(self) -> str:
        """Render this tree and its descendants as an indented program."""
        lines = [_format_name("behaviortree", self.id)]
        lines.extend("  " + line for line in self.root.get_program().splitlines())
        return "\n".join(lines)

    def get_statuses(self) -> dict[str, BTStatus]:
        """Return the latest status of every node, keyed by stable node ID."""
        with self._state_lock:
            nodes = (self,) + self._state.nodes
            return {node.id: node.get_status() for node in nodes}

    def snapshot(self) -> BTSnapshot:
        """Capture a revision-consistent program and status view.

        Returns:
            An immutable :class:`BTSnapshot` suitable for transport or tooling.
        """
        with self._tick_lock:
            with self._state_lock:
                revision = self._state.revision
                return BTSnapshot(revision, self.get_program(), self.get_statuses())

    def to_dict(self) -> dict:
        """Describe structure, statuses, and run parameters as plain data.

        Returns:
            A nested dictionary containing the revision and root description.
            Tree nodes additionally include ``tick_rate`` and ``active``.
        """
        def describe(node: BTNode) -> dict:
            """Recursively serialize one node and its descendants."""
            result = {
                "id": node.id,
                "type": type(node).__name__,
                "status": node.get_status().name,
                "children": [describe(child) for child in node.children()],
            }
            if isinstance(node, BehaviorTree):
                result["tick_rate"] = node.tick_rate
                result["active"] = node.is_active
            return result
        with self._tick_lock:
            with self._state_lock:
                revision = self._state.revision
                return {"revision": revision, "root": describe(self)}

    def add_tree_status_listener(self, callback: Callable[[int, BTNode, BTStatus, BTStatus], None]) -> None:
        """Subscribe to status transitions from this tree and descendants.

        Args:
            callback: Called as ``callback(revision, node, old, new)`` in the
                thread that changes the status.
        """
        if callback not in self._tree_status_listeners:
            self._tree_status_listeners.append(callback)

    def add_structure_listener(self, callback: Callable[[int, str], None]) -> None:
        """Subscribe to program changes caused by subtree replacement.

        Args:
            callback: Called as ``callback(revision, program)`` after a
                successful replacement.
        """
        if callback not in self._structure_listeners:
            self._structure_listeners.append(callback)

    def remove_tree_status_listener(self, callback: Callable[[int, BTNode, BTStatus, BTStatus], None]) -> None:
        """Stop delivering tree-wide status transitions to ``callback``."""
        if callback in self._tree_status_listeners:
            self._tree_status_listeners.remove(callback)

    def remove_structure_listener(self, callback: Callable[[int, str], None]) -> None:
        """Stop delivering structure changes to ``callback``."""
        if callback in self._structure_listeners:
            self._structure_listeners.remove(callback)


def parse_bt(text: str, symbols: Mapping[str, Callable[[], BTNode] | BTNode]) -> BTNode:
    """Parse an indented behavior-tree program.

    Built-in keywords are ``sequence``, ``fallback``, ``success``, ``failure``,
    and ``running``. Children are indented by exactly two spaces. A token such
    as ``$navigate`` resolves through ``symbols``; the mapped value may be a
    node or a zero-argument node factory. ``#id`` may follow a node token.

    Args:
        text: Program text containing exactly one root.
        symbols: Application nodes or factories keyed without the ``$`` prefix.

    Returns:
        The parsed root node.

    Raises:
        ValueError: If syntax, indentation, or node IDs are invalid.
        KeyError: If a referenced symbol is absent.
        TypeError: If a symbol does not resolve to a :class:`BTNode`.
    """
    entries: list[tuple[int, int, str]] = []
    for line_number, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        if "\t" in raw:
            raise ValueError(f"line {line_number}: tabs are not allowed")
        indent = len(raw) - len(raw.lstrip(" "))
        if indent % 2:
            raise ValueError(f"line {line_number}: indentation must use two spaces")
        entries.append((indent, line_number, raw.strip()))
    if not entries:
        raise ValueError("tree text is empty")

    def build(index: int, indent: int) -> tuple[BTNode, int]:
        """Build one node and return the first unconsumed input entry."""
        current_indent, line_number, token = entries[index]
        if current_indent != indent:
            raise ValueError(f"line {line_number}: invalid indentation")
        parts = token.split()
        keyword = parts[0]
        identity = parts[1] if len(parts) == 2 else None
        node_id = identity[1:] if identity is not None else None
        if len(parts) > 2 or (identity is not None and (not identity.startswith("#") or not node_id)):
            raise ValueError(f"line {line_number}: invalid syntax: {token}")
        index += 1
        children: list[BTNode] = []
        if index < len(entries) and entries[index][0] > indent:
            if entries[index][0] != indent + 2:
                raise ValueError(f"line {entries[index][1]}: indentation jumps too far")
            while index < len(entries) and entries[index][0] == indent + 2:
                child, index = build(index, indent + 2)
                children.append(child)
        if keyword in ("sequence", "fallback"):
            node_type = Sequence if keyword == "sequence" else Fallback
            return node_type(tuple(children), id=node_id), index
        if children:
            raise ValueError(f"line {line_number}: leaf nodes cannot have children")
        if keyword == "success":
            return Success(id=node_id), index
        if keyword == "failure":
            return Failure(id=node_id), index
        if keyword == "running":
            return Running(id=node_id), index
        if keyword.startswith("$") and len(keyword) > 1:
            if keyword[1:] not in symbols:
                raise KeyError(f"line {line_number}: unknown symbol {keyword[1:]!r}")
            symbol = symbols[keyword[1:]]
            node = symbol() if callable(symbol) else symbol
            if not isinstance(node, BTNode):
                raise TypeError(f"line {line_number}: symbol must produce a BTNode")
            if node_id is not None:
                if callable(symbol):
                    node._id = node_id
                elif node.id != node_id:
                    raise ValueError(f"line {line_number}: IDs cannot override a node instance")
            return node, index
        raise ValueError(f"line {line_number}: unknown node {keyword!r}")

    root, next_index = build(0, entries[0][0])
    if next_index != len(entries):
        raise ValueError(f"line {entries[next_index][1]}: multiple roots")
    _validate(root)
    return root


# Backward-compatible name used by early BeeTea clients.
parse_tree = parse_bt


__all__ = [
    "BTNode", "BTResponse", "BTStatus", "BTSnapshot",
    "Action", "BehaviorTree", "Condition", "Failure", "Fallback", "Running",
    "Sequence", "Success", "parse_bt", "parse_tree", "walk",
]
