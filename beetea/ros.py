"""ROS 2 transport adapters for external behavior-tree subtrees.

Execution, scheduling, state, and synchronization belong to :mod:`beetea.bt`.
This module only exposes those operations over ROS and represents a remote
endpoint as a non-blocking local :class:`~beetea.bt.BTNode`.
"""

from __future__ import annotations

import json
from threading import RLock
import time
from typing import Callable

from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
from std_srvs.srv import Empty

from beetea_msgs.msg import BTNodeEvent, BTNodeStatus, BTTree
from beetea_msgs.srv import GetBTSnapshot, Tick

from svea_core import rosonic as rx
from .bt import BTNode, BTResponse, BTStatus, BehaviorTree


TREE_QOS = QoSProfile(
    history=HistoryPolicy.KEEP_LAST,
    depth=1,
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
)


def _remote_namespace(endpoint: str) -> str:
    endpoint = endpoint.rstrip("/")
    return endpoint if endpoint.endswith("/bt") else f"{endpoint}/bt"


def _walk_description(node: dict):
    """Yield nodes from a :meth:`BehaviorTree.to_dict` description."""
    yield node
    for child in node["children"]:
        yield from _walk_description(child)


class BTServer(rx.Field):
    """Expose one local behavior tree through ROS services and tooling topics."""

    def __init__(
        self,
        behavior: BTNode | BehaviorTree | str | Callable = "tree",
        *,
        name: str | None = "bt",
    ):
        super().__init__(name=name)
        self._behavior = behavior
        self.tree: BehaviorTree | None = None

        # Runtime children keep every BTServer's ROS handles independent while
        # Rosonic still owns their startup and shutdown.
        self._event_publisher = rx.add(rx.Publisher(BTNodeEvent, "events"), self)
        self._tree_publisher = rx.add(
            rx.Publisher(BTTree, "tree", TREE_QOS), self
        )

        tick_service = rx.Service(Tick, "tick")
        tick_service(
            lambda owner, request, response: owner._on_tick(request, response)
        )
        self._tick_service = rx.add(tick_service, self)

        halt_service = rx.Service(Empty, "halt")
        halt_service(
            lambda owner, request, response: owner._on_halt(request, response)
        )
        self._halt_service = rx.add(halt_service, self)

        snapshot_service = rx.Service(GetBTSnapshot, "snapshot")
        snapshot_service(
            lambda owner, request, response: owner._on_snapshot(request, response)
        )
        self._snapshot_service = rx.add(snapshot_service, self)

    def __rosonic_register__(self, owner, *, name: str | None = None):
        resource_name = self.__rosonic_name__ if name is None else name
        for resource in owner.__rosonic_resources__:
            if (
                resource is not self
                and resource.__rosonic_name__ == resource_name
            ):
                raise RuntimeError(
                    f"Rosonic resource name {resource_name!r} is already registered"
                )
        if name is None:
            return super().__rosonic_register__(owner)
        return super().__rosonic_register__(owner, name=name)

    def on_startup(self) -> None:
        behavior = self._behavior
        if isinstance(behavior, str):
            behavior = getattr(self.owner, behavior)
        elif callable(behavior) and not isinstance(behavior, BTNode):
            behavior = behavior(self.owner)
        if not isinstance(behavior, BTNode):
            raise TypeError("BTServer behavior must resolve to BTNode or BehaviorTree")
        self.tree = (
            behavior if isinstance(behavior, BehaviorTree) else BehaviorTree(behavior)
        )
        self.tree.add_tree_status_listener(self._on_tree_status)
        self.tree.add_structure_listener(self._on_structure)
        self._publish_tree()

    def _on_tick(self, request, response):
        del request
        try:
            result = self.tree.tick()
        except Exception as error:
            self.node.get_logger().error(f"Behavior-tree tick failed: {error}")
            result = BTResponse.FAILURE
        response.response = int(result)
        return response

    def _on_halt(self, request, response):
        del request
        self.tree.halt()
        return response

    def _on_tree_status(
        self,
        revision: int,
        node: BTNode,
        old: BTStatus,
        new: BTStatus,
    ) -> None:
        event = BTNodeEvent()
        event.revision = revision
        event.node_id = node.id
        event.previous_status = int(old)
        event.status = int(new)
        self._event_publisher.publish(event)

    @staticmethod
    def _tree_json(description: dict) -> str:
        return json.dumps(description, sort_keys=True, separators=(",", ":"))

    def _publish_tree(self) -> None:
        description = self.tree.to_dict()
        message = BTTree()
        message.revision = description["revision"]
        message.tree = self._tree_json(description)
        self._tree_publisher.publish(message)

    def _on_structure(self, revision: int, program: str) -> None:
        del revision, program
        self._publish_tree()

    def _on_snapshot(self, request, response):
        del request
        description = self.tree.to_dict()
        response.revision = description["revision"]
        response.tree = self._tree_json(description)
        statuses = []
        for node in _walk_description(description["root"]):
            node_status = BTNodeStatus()
            node_status.node_id = node["id"]
            node_status.status = int(BTStatus[node["status"]])
            statuses.append(node_status)
        response.statuses = statuses
        return response

    def on_shutdown(self) -> None:
        tree = self.tree
        if tree is None:
            return
        tree.remove_tree_status_listener(self._on_tree_status)
        tree.remove_structure_listener(self._on_structure)
        tree.halt()


class RemoteBT(BTNode, rx.Resource):
    """A non-blocking BT node backed by remote tick and halt services.

    Register instances explicitly with their Rosonic owner, for example::

        remote = rx.add(RemoteBT("/worker"), node)
    """

    def __init__(
        self,
        endpoint: str,
        *,
        timeout: float = 1.0,
        label: str | None = None,
        id: str | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        BTNode.__init__(self, label=label, id=id)
        rx.Resource.__init__(self, name=_remote_namespace(endpoint))
        self._timeout = timeout
        self._clock = clock
        self._lock = RLock()
        self._active = False
        self._cached_response = BTResponse.RUNNING
        self._pending_tick = None
        self._tick_deadline: float | None = None
        self._pending_halt = None
        self._halt_requested = False

        self._tick_client = rx.add(rx.Client(Tick, "tick"), self)
        self._halt_client = rx.add(rx.Client(Empty, "halt"), self)

    def _request_tick_locked(self) -> None:
        if (
            not self._active
            or self._pending_tick is not None
            or self._halt_requested
            or self._pending_halt is not None
        ):
            return
        try:
            future = self._tick_client(Tick.Request())
        except Exception:
            self._cached_response = BTResponse.FAILURE
            return
        self._pending_tick = future
        self._tick_deadline = self._clock() + self._timeout
        future.add_done_callback(self._on_tick_done)

    def _request_halt_locked(self) -> None:
        self._halt_requested = True
        if self._pending_tick is not None or self._pending_halt is not None:
            return
        try:
            future = self._halt_client(Empty.Request())
        except Exception:
            self._halt_requested = False
            if self._active:
                self._request_tick_locked()
            return
        self._pending_halt = future
        future.add_done_callback(self._on_halt_done)

    def _retire_timed_out_tick_locked(self) -> bool:
        if (
            self._pending_tick is None
            or self._tick_deadline is None
            or self._clock() < self._tick_deadline
        ):
            return False
        future = self._pending_tick
        self._pending_tick = None
        self._tick_deadline = None
        self._cached_response = BTResponse.FAILURE
        try:
            future.cancel()
        except Exception:
            pass
        if self._halt_requested:
            self._request_halt_locked()
        return True

    def _on_tick_done(self, future) -> None:
        with self._lock:
            if future is not self._pending_tick:
                return
            self._pending_tick = None
            self._tick_deadline = None
            if self._halt_requested:
                self._request_halt_locked()
                return
            if not self._active:
                return
            try:
                self._cached_response = BTResponse(future.result().response)
            except Exception:
                self._cached_response = BTResponse.FAILURE

    def _on_halt_done(self, future) -> None:
        with self._lock:
            if future is not self._pending_halt:
                return
            self._pending_halt = None
            self._halt_requested = False
            try:
                future.result()
            except Exception:
                pass
            if self._active:
                self._request_tick_locked()

    def tick(self) -> BTResponse:
        with self._lock:
            if not self._active:
                self._active = True
                self._cached_response = BTResponse.RUNNING
            timed_out = self._retire_timed_out_tick_locked()
            response = self._cached_response
            if not timed_out:
                self._request_tick_locked()
            self._set_status(BTStatus(response))
            return response

    def halt(self) -> None:
        with self._lock:
            if not self._active:
                self._set_status(BTStatus.SKIPPED)
                return
            self._active = False
            self._cached_response = BTResponse.RUNNING
            self._request_halt_locked()
            self._set_status(BTStatus.SKIPPED)

    def on_shutdown(self) -> None:
        with self._lock:
            should_halt = self._active or self._pending_tick is not None
            self._active = False
            self._cached_response = BTResponse.RUNNING
            future = self._pending_tick
            self._pending_tick = None
            self._tick_deadline = None
            if future is not None:
                try:
                    future.cancel()
                except Exception:
                    pass
            if should_halt:
                self._request_halt_locked()
            self._set_status(BTStatus.SKIPPED)


__all__ = ["BTServer", "RemoteBT", "TREE_QOS"]
