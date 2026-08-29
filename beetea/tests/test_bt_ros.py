import importlib
import json
import sys
import time
import types
import unittest

from beetea.bt import (
    BTNode,
    BTResponse,
    BTStatus,
    BehaviorTree,
    Condition,
    Failure,
    Fallback,
    Running,
    Sequence,
    Success,
)


class _Message:
    def __init__(self):
        for name in getattr(self, "_fields", ()):
            setattr(self, name, [] if name == "statuses" else 0)


class BTNodeEvent(_Message):
    SUCCESS = 1
    RUNNING = 2
    FAILURE = 3
    SKIPPED = 4
    _fields = ("revision", "node_id", "previous_status", "status")


class BTNodeStatus(_Message):
    _fields = ("node_id", "status")


class BTTree(_Message):
    _fields = ("revision", "tree")


class GetBTSnapshot:
    class Request:
        pass

    class Response:
        def __init__(self):
            self.revision = 0
            self.tree = ""
            self.statuses = []


class Tick:
    class Request:
        pass

    class Response:
        def __init__(self):
            self.response = 0


class Empty:
    class Request:
        pass

    class Response:
        pass


class _Logger:
    def warning(self, message):
        pass

    def exception(self, message):
        pass

    def error(self, message):
        pass


class _Publisher:
    def __init__(self, bus, topic, *, transient_local=False):
        self.bus = bus
        self.topic = topic
        self.transient_local = transient_local
        self.messages = []

    def publish(self, message):
        self.messages.append(message)
        if self.transient_local:
            self.bus.retained[self.topic] = message
        for callback in tuple(self.bus.subscriptions.get(self.topic, ())):
            callback(message)


class _Bus:
    def __init__(self):
        self.subscriptions = {}
        self.services = {}
        self.retained = {}


class _Future:
    def __init__(self, result=None, exception=None, *, done=True):
        self._result = result
        self._exception = exception
        self._done = done
        self._cancelled = False
        self._callbacks = []

    def add_done_callback(self, callback):
        self._callbacks.append(callback)
        if self._done:
            callback(self)

    def result(self):
        if self._exception is not None:
            raise self._exception
        return self._result

    def cancel(self):
        self._cancelled = True
        return True

    def complete(self, result=None, exception=None):
        self._result = result
        self._exception = exception
        self._done = True
        for callback in tuple(self._callbacks):
            callback(self)


class _Resource:
    __rosonic_preregistered__ = ()

    def __init__(self, *, name=None):
        self.__rosonic_name__ = name
        self.__rosonic_node__ = None
        self.__rosonic_owner__ = None
        self.__rosonic_resources__ = ()
        self.__rosonic_started__ = False

    @property
    def __rosonic_relname__(self):
        if self.__rosonic_owner__ is self:
            return ""
        name = self.__rosonic_name__
        parent = self.__rosonic_owner__.__rosonic_relname__
        if name.startswith(("/", "~")):
            return name
        return name if not parent else f"{parent}/{name}"

    def __rosonic_register__(self, owner):
        self.__rosonic_owner__ = owner
        if owner is not self:
            owner.__rosonic_resources__ += (self,)
        for child in self.__rosonic_preregistered__:
            child.__rosonic_register__(self)

    def __rosonic_startup__(self, node):
        for child in self.__rosonic_resources__:
            child.__rosonic_startup__(node)
        self.__rosonic_node__ = node
        self.on_startup()
        self.__rosonic_started__ = True

    def __rosonic_shutdown__(self, node):
        if not self.__rosonic_started__:
            return
        self.on_shutdown()
        self.__rosonic_node__ = None
        for child in self.__rosonic_resources__:
            child.__rosonic_shutdown__(node)
        self.__rosonic_started__ = False

    def start(self, node):
        self.__rosonic_startup__(node)

    def _is_started(self):
        return self.__rosonic_started__

    @property
    def node(self):
        return self.__rosonic_node__

    @property
    def owner(self):
        return self.__rosonic_owner__

    def on_startup(self):
        pass

    def on_shutdown(self):
        pass


def _add(resource, owner):
    if owner.__rosonic_owner__ is None:
        owner.__rosonic_preregistered__ += (resource,)
    else:
        resource.__rosonic_register__(owner)
        if owner._is_started():
            resource.start(owner.node)
    return resource


def _remove(resource, owner):
    if (
        resource in owner.__rosonic_preregistered__
        and not resource.__rosonic_owner__
    ):
        owner.__rosonic_preregistered__ = tuple(
            child for child in owner.__rosonic_preregistered__
            if child is not resource
        )
        return resource
    if resource._is_started():
        resource.__rosonic_shutdown__(resource.node)
    owner.__rosonic_resources__ = tuple(
        child for child in owner.__rosonic_resources__ if child is not resource
    )
    resource.__rosonic_owner__ = None
    return resource


class _Field(_Resource):
    def __set_name__(self, owner, name):
        owner.__rosonic_preregistered__ += (self,)

    @property
    def node(self):
        return self.__rosonic_node__


class _NamedField(_Field):
    def __set_name__(self, owner, name):
        if self.__rosonic_name__ is None:
            self.__rosonic_name__ = name
        owner.__rosonic_preregistered__ += (self,)


class _PublisherResource(_NamedField):
    def __init__(self, message_type, topic=None, qos_profile=None):
        super().__init__(name=topic)
        self.message_type = message_type
        self.qos_profile = qos_profile
        self.publisher = None

    @property
    def messages(self):
        return self.publisher.messages

    def publish(self, message):
        self.publisher.publish(message)

    def on_startup(self):
        self.publisher = self.node.create_publisher(
            self.message_type, self.__rosonic_relname__, self.qos_profile
        )

    def on_shutdown(self):
        if self.publisher is not None:
            self.node.destroy_publisher(self.publisher)
            self.publisher = None


class _SubscriberResource(_NamedField):
    def __init__(self, message_type, topic=None, qos_profile=None):
        super().__init__(name=topic)
        self.message_type = message_type
        self.qos_profile = qos_profile
        self.callback = None
        self.subscriber = None

    def __call__(self, callback):
        self.callback = callback
        return self

    def on_startup(self):
        owner = self.__rosonic_owner__
        self.subscriber = self.node.create_subscription(
            self.message_type,
            self.__rosonic_relname__,
            lambda message: self.callback(owner, message),
            self.qos_profile,
        )

    def on_shutdown(self):
        if self.subscriber is not None:
            self.node.destroy_subscription(self.subscriber)
            self.subscriber = None


class _ServiceResource(_NamedField):
    def __init__(self, service_type, service=None, qos_profile=None, **kwargs):
        super().__init__(name=service)
        self.service_type = service_type
        self.qos_profile = qos_profile
        self.kwargs = kwargs
        self.callback = None
        self.service = None

    def __call__(self, callback):
        self.callback = callback
        return self

    def on_startup(self):
        owner = self.__rosonic_owner__
        self.service = self.node.create_service(
            self.service_type,
            self.__rosonic_relname__,
            lambda request, response: self.callback(owner, request, response),
            qos_profile=self.qos_profile,
            **self.kwargs,
        )

    def on_shutdown(self):
        if self.service is not None:
            self.node.destroy_service(self.service)
            self.service = None


class _Client:
    def __init__(self, bus, service_type, topic):
        self.bus = bus
        self.service_type = service_type
        self.topic = topic
        self.requests = []

    def call_async(self, request):
        self.requests.append(request)
        callback = self.bus.services.get(self.topic)
        if callback is None:
            return _Future(done=False)
        try:
            response = callback(request, self.service_type.Response())
            return _Future(result=response)
        except Exception as error:
            return _Future(exception=error)


class _ClientResource(_NamedField):
    def __init__(self, service_type, service=None, qos_profile=None, **kwargs):
        super().__init__(name=service)
        self.service_type = service_type
        self.qos_profile = qos_profile
        self.kwargs = kwargs
        self.client = None

    def __call__(self, request):
        return self.client.call_async(request)

    def on_startup(self):
        self.client = self.node.create_client(
            self.service_type,
            self.__rosonic_relname__,
            qos_profile=self.qos_profile,
            **self.kwargs,
        )

    def on_shutdown(self):
        if self.client is not None:
            self.node.destroy_client(self.client)
            self.client = None


class _TimerResource(_NamedField):
    def __init__(self, period, name=None, **kwargs):
        super().__init__(name=name)
        self.period = period
        self.kwargs = kwargs
        self.callback = None
        self.tmr = None

    def __call__(self, callback):
        self.callback = callback
        return self

    def on_startup(self):
        owner = self.__rosonic_owner__
        self.tmr = self.node.create_timer(
            self.period, lambda: self.callback(owner), **self.kwargs
        )

    def on_shutdown(self):
        if self.tmr is not None:
            self.node.destroy_timer(self.tmr)
            self.tmr = None


class _Node(_Resource):
    def __init__(self, name="node", *, bus=None, autostart=True, **kwargs):
        _Resource.__init__(self, name=f"/{name}")
        del kwargs
        self._name = name
        self.bus = bus or _Bus()
        self.publishers = []
        self.subscriptions = []
        self.services = []
        self.clients = []
        self.__rosonic_register__(self)
        if type(self) is _Node and autostart:
            self.__rosonic_startup__(self)

    def get_name(self):
        return self._name

    def get_logger(self):
        return _Logger()

    def create_publisher(self, message_type, topic, qos):
        del message_type
        publisher = _Publisher(
            self.bus,
            topic,
            transient_local=getattr(qos, "durability", None) == 2,
        )
        self.publishers.append(publisher)
        return publisher

    def create_subscription(self, message_type, topic, callback, qos):
        del message_type
        self.bus.subscriptions.setdefault(topic, []).append(callback)
        subscription = (topic, callback)
        self.subscriptions.append(subscription)
        if getattr(qos, "durability", None) == 2 and topic in self.bus.retained:
            callback(self.bus.retained[topic])
        return subscription

    def create_service(self, service_type, topic, callback, **kwargs):
        del service_type, kwargs
        service = (topic, callback)
        self.services.append(service)
        self.bus.services[topic] = callback
        return service

    def create_client(self, service_type, topic, **kwargs):
        del kwargs
        client = _Client(self.bus, service_type, topic)
        self.clients.append(client)
        return client

    def create_timer(self, period, callback, **kwargs):
        del kwargs
        return (period, callback)

    def destroy_subscription(self, subscription):
        topic, callback = subscription
        self.bus.subscriptions[topic].remove(callback)

    def destroy_publisher(self, publisher):
        pass

    def destroy_service(self, service):
        topic, callback = service
        if self.bus.services.get(topic) is callback:
            del self.bus.services[topic]

    def destroy_client(self, client):
        pass

    def destroy_timer(self, timer):
        pass


class _QoSProfile:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _install_ros_stubs():
    missing = object()
    core_package = importlib.import_module("svea_core")
    beetea_package = sys.modules["beetea"]
    module_names = (
        "rclpy",
        "rclpy.node",
        "rclpy.qos",
        "beetea_msgs",
        "beetea_msgs.msg",
        "beetea_msgs.srv",
        "std_srvs",
        "std_srvs.srv",
        "svea_core.rosonic",
        "beetea.ros",
    )
    previous_modules = {
        name: sys.modules.get(name, missing) for name in module_names
    }
    previous_rosonic = core_package.__dict__.get("rosonic", missing)
    previous_bt_ros = beetea_package.__dict__.get("ros", missing)

    rclpy = types.ModuleType("rclpy")
    node = types.ModuleType("rclpy.node")
    qos = types.ModuleType("rclpy.qos")
    node.Node = _Node
    qos.QoSProfile = _QoSProfile
    qos.DurabilityPolicy = types.SimpleNamespace(VOLATILE=1, TRANSIENT_LOCAL=2)
    qos.HistoryPolicy = types.SimpleNamespace(KEEP_LAST=1)
    qos.ReliabilityPolicy = types.SimpleNamespace(RELIABLE=1)
    rclpy.node = node
    rclpy.qos = qos

    messages = types.ModuleType("beetea_msgs.msg")
    messages.BTNodeEvent = BTNodeEvent
    messages.BTNodeStatus = BTNodeStatus
    messages.BTTree = BTTree
    services = types.ModuleType("beetea_msgs.srv")
    services.GetBTSnapshot = GetBTSnapshot
    services.Tick = Tick
    package = types.ModuleType("beetea_msgs")
    package.msg = messages
    package.srv = services
    standard_services = types.ModuleType("std_srvs.srv")
    standard_services.Empty = Empty
    standard_package = types.ModuleType("std_srvs")
    standard_package.srv = standard_services

    rosonic = types.ModuleType("svea_core.rosonic")
    rosonic.Resource = _Resource
    rosonic.add = _add
    rosonic.remove = _remove
    rosonic.Node = _Node
    rosonic.Field = _Field
    rosonic.NamedField = _NamedField
    rosonic.Publisher = _PublisherResource
    rosonic.Subscriber = _SubscriberResource
    rosonic.Client = _ClientResource
    rosonic.Service = _ServiceResource
    rosonic.Timer = _TimerResource

    try:
        sys.modules.update({
            "rclpy": rclpy,
            "rclpy.node": node,
            "rclpy.qos": qos,
            "beetea_msgs": package,
            "beetea_msgs.msg": messages,
            "beetea_msgs.srv": services,
            "std_srvs": standard_package,
            "std_srvs.srv": standard_services,
            "svea_core.rosonic": rosonic,
        })
        core_package.rosonic = rosonic
        sys.modules.pop("beetea.ros", None)
        module = importlib.import_module("beetea.ros")
    finally:
        for name, previous in previous_modules.items():
            if previous is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
        if previous_rosonic is missing:
            core_package.__dict__.pop("rosonic", None)
        else:
            core_package.rosonic = previous_rosonic
        if previous_bt_ros is missing:
            beetea_package.__dict__.pop("ros", None)
        else:
            beetea_package.ros = previous_bt_ros
    return module


bt_ros = _install_ros_stubs()


def wait_until(predicate, timeout=1.0):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)
    return predicate()


class _CountingNode(BTNode):
    def __init__(self, response=BTResponse.RUNNING):
        super().__init__()
        self.response = response
        self.ticks = 0
        self.halts = 0

    def tick(self):
        self.ticks += 1
        self._set_status(BTStatus(self.response))
        return self.response

    def halt(self):
        self.halts += 1
        super().halt()


def _make_server(node, behavior, namespace="/worker/bt"):
    return bt_ros.rx.add(bt_ros.BTServer(behavior, name=namespace), node)


def _success(response):
    response.response = int(BTResponse.SUCCESS)
    return response


class BehaviorTreeROSTests(unittest.TestCase):
    def test_adapters_are_rosonic_resources_with_scoped_names(self):
        node = _Node()
        server = _make_server(node, Running())
        remote = bt_ros.rx.add(bt_ros.RemoteBT("/worker"), node)

        self.assertIsInstance(server, _Field)
        self.assertIsInstance(remote, _Resource)
        self.assertNotIsInstance(remote, _Field)
        self.assertIsInstance(server.tree, BehaviorTree)
        self.assertEqual(
            server._tick_service.__rosonic_relname__, "/worker/bt/tick"
        )
        self.assertEqual(
            remote._halt_client.__rosonic_relname__, "/worker/bt/halt"
        )

        other_remote = bt_ros.rx.add(bt_ros.RemoteBT("/other"), node)
        self.assertIsNot(remote._tick_client, other_remote._tick_client)
        bt_ros.rx.remove(other_remote, node)

        bt_ros.rx.remove(remote, node)
        bt_ros.rx.remove(server, node)
        node.__rosonic_shutdown__(node)
        self.assertIsNone(remote._tick_client.client)
        self.assertIsNone(server._tick_service.service)
        self.assertIsNone(server._event_publisher.publisher)

    def test_adapter_resources_defer_until_node_startup(self):
        node = _Node(autostart=False)
        server = bt_ros.rx.add(
            bt_ros.BTServer(Running(), name="/worker/bt"), node
        )

        self.assertIsNone(server._tick_service.service)
        self.assertIsNone(server._event_publisher.publisher)

        node.__rosonic_startup__(node)
        self.assertIsNotNone(server._tick_service.service)
        self.assertIsNotNone(server._event_publisher.publisher)
        node.__rosonic_shutdown__(node)
        self.assertIsNone(server._tick_service.service)
        self.assertIsNone(server._event_publisher.publisher)

    def test_server_instances_own_distinct_ros_resources(self):
        bus = _Bus()
        node = _Node(bus=bus)
        first = _make_server(node, Running(), "/first/bt")
        second = _make_server(node, Running(), "/second/bt")

        self.assertIsNot(first._tick_service, second._tick_service)
        self.assertIsNot(first._event_publisher, second._event_publisher)
        self.assertEqual(
            first._tick_service.__rosonic_relname__, "/first/bt/tick"
        )
        self.assertEqual(
            second._tick_service.__rosonic_relname__, "/second/bt/tick"
        )

        bt_ros.rx.remove(first, node)
        self.assertIn("/second/bt/tick", bus.services)
        response = bus.services["/second/bt/tick"](
            Tick.Request(), Tick.Response()
        )
        self.assertEqual(response.response, int(BTResponse.RUNNING))
        bt_ros.rx.remove(second, node)

    def test_server_rejects_a_duplicate_resource_name(self):
        node = _Node()
        first = _make_server(node, Running())

        with self.assertRaisesRegex(RuntimeError, "already registered"):
            _make_server(node, Running())

        bt_ros.rx.remove(first, node)

    def test_remote_tick_consumes_service_result_on_next_parent_tick(self):
        bus = _Bus()
        server_node = _Node("server", bus=bus)
        parent_node = _Node("parent", bus=bus)
        target = Success()
        server = _make_server(server_node, target)
        remote = bt_ros.rx.add(bt_ros.RemoteBT("/worker"), parent_node)

        self.assertEqual(remote.tick(), BTResponse.RUNNING)
        self.assertTrue(wait_until(lambda: target.get_status() == BTStatus.SUCCESS))
        self.assertEqual(remote.get_status(), BTStatus.RUNNING)
        self.assertEqual(remote.tick(), BTResponse.SUCCESS)

        remote.halt()
        self.assertEqual(remote.get_status(), BTStatus.SKIPPED)
        self.assertEqual(target.get_status(), BTStatus.SKIPPED)
        remote.tick()
        self.assertEqual(remote.get_status(), BTStatus.RUNNING)

        bt_ros.rx.remove(remote, parent_node)
        bt_ros.rx.remove(server, server_node)

    def test_remote_has_no_scheduler_or_observation_resources(self):
        bus = _Bus()
        server_node = _Node("server", bus=bus)
        parent_node = _Node("parent", bus=bus)
        target = _CountingNode()
        hosted = BehaviorTree(target, tick_rate=1)
        server = _make_server(server_node, hosted)
        remote = bt_ros.rx.add(bt_ros.RemoteBT("/worker"), parent_node)

        self.assertEqual(remote.tick(), BTResponse.RUNNING)
        self.assertTrue(wait_until(lambda: target.ticks == 1))
        self.assertEqual(remote.tick(), BTResponse.RUNNING)
        self.assertEqual(target.ticks, 1)
        self.assertEqual(
            sum(
                isinstance(child, _ClientResource)
                for child in remote.__rosonic_resources__
            ),
            2,
        )
        self.assertFalse(
            any(
                isinstance(child, (_SubscriberResource, _TimerResource))
                for child in remote.__rosonic_resources__
            )
        )

        bt_ros.rx.remove(remote, parent_node)
        bt_ros.rx.remove(server, server_node)

    def test_remote_timeout_retires_request_and_recovers(self):
        now = [3.0]
        bus = _Bus()
        node = _Node(bus=bus)
        remote = bt_ros.rx.add(
            bt_ros.RemoteBT("/missing", timeout=1.0, clock=lambda: now[0]),
            node,
        )
        self.assertEqual(remote.tick(), BTResponse.RUNNING)
        stale = remote._pending_tick
        now[0] = 4.0
        self.assertEqual(remote.tick(), BTResponse.FAILURE)
        self.assertIsNone(remote._pending_tick)
        self.assertTrue(stale._cancelled)

        bus.services["/missing/bt/tick"] = (
            lambda request, response: _success(response)
        )
        now[0] = 4.1
        self.assertEqual(remote.tick(), BTResponse.FAILURE)
        self.assertEqual(remote.tick(), BTResponse.SUCCESS)

        stale_response = Tick.Response()
        stale_response.response = int(BTResponse.FAILURE)
        stale.complete(stale_response)
        self.assertEqual(remote.tick(), BTResponse.SUCCESS)
        bt_ros.rx.remove(remote, node)

    def test_halt_orders_reactivation_after_an_inflight_tick(self):
        bus = _Bus()
        node = _Node(bus=bus)
        bus.services["/worker/bt/halt"] = lambda request, response: response
        remote = bt_ros.rx.add(bt_ros.RemoteBT("/worker"), node)

        remote.tick()
        old_tick = remote._pending_tick
        remote.halt()
        self.assertIs(remote._pending_tick, old_tick)
        self.assertEqual(remote.tick(), BTResponse.RUNNING)

        reply = Tick.Response()
        reply.response = int(BTResponse.SUCCESS)
        old_tick.complete(reply)
        self.assertIsNot(remote._pending_tick, old_tick)
        self.assertEqual(remote.get_status(), BTStatus.RUNNING)
        bt_ros.rx.remove(remote, node)

    def test_server_tick_and_halt_services(self):
        node = _Node()
        target = _CountingNode()
        server = _make_server(node, target)

        tick_response = server._on_tick(Tick.Request(), Tick.Response())
        self.assertTrue(wait_until(lambda: target.ticks >= 1))
        self.assertEqual(tick_response.response, int(BTResponse.RUNNING))

        server._on_halt(Empty.Request(), Empty.Response())
        self.assertEqual(target.halts, 1)
        self.assertEqual(target.get_status(), BTStatus.SKIPPED)
        server._on_halt(Empty.Request(), Empty.Response())
        self.assertEqual(target.halts, 1)

    def test_server_publishes_asynchronous_completion_event(self):
        node = _Node()
        target = _CountingNode()
        server = _make_server(node, target)
        server._on_tick(Tick.Request(), Tick.Response())

        target._set_status(BTStatus.SUCCESS)
        event = server._event_publisher.messages[-1]
        self.assertEqual(event.node_id, target.id)
        self.assertEqual(event.status, BTNodeEvent.SUCCESS)

    def test_local_sequence_sends_halt_to_remote(self):
        bus = _Bus()
        server_node = _Node("server", bus=bus)
        remote_node = _Node("parent", bus=bus)
        target = Running()
        server = _make_server(server_node, target)
        remote = bt_ros.rx.add(bt_ros.RemoteBT("/worker"), remote_node)
        ready = [True]
        tree = BehaviorTree(Sequence((Condition(lambda: ready[0]), remote)))

        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        self.assertTrue(wait_until(lambda: target.get_status() == BTStatus.RUNNING))
        ready[0] = False
        self.assertEqual(tree.root.tick(), BTResponse.FAILURE)
        self.assertEqual(remote.get_status(), BTStatus.SKIPPED)
        self.assertEqual(target.get_status(), BTStatus.SKIPPED)

        bt_ros.rx.remove(remote, remote_node)
        bt_ros.rx.remove(server, server_node)

    def test_local_fallback_semantics_with_remote_failure(self):
        bus = _Bus()
        server_node = _Node("server", bus=bus)
        parent_node = _Node("parent", bus=bus)
        server = _make_server(server_node, Failure())
        remote = bt_ros.rx.add(bt_ros.RemoteBT("/worker"), parent_node)
        tree = BehaviorTree(Fallback((remote, Success())))

        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        self.assertEqual(remote.get_status(), BTStatus.RUNNING)
        self.assertTrue(
            wait_until(lambda: server.tree.get_status() == BTStatus.FAILURE)
        )
        self.assertEqual(tree.root.tick(), BTResponse.SUCCESS)
        self.assertEqual(remote.get_status(), BTStatus.FAILURE)

        bt_ros.rx.remove(remote, parent_node)
        bt_ros.rx.remove(server, server_node)

    def test_tree_server_publishes_structure_events_and_snapshot(self):
        node = _Node()
        tree = BehaviorTree(Sequence((Success(id="ok"),), id="root"))
        server = _make_server(node, tree, "/tree/bt")
        tree.tick()

        self.assertTrue(server._tree_publisher.messages)
        self.assertTrue(wait_until(lambda: bool(server._event_publisher.messages)))

        response = types.SimpleNamespace(
            revision=0,
            tree="",
            statuses=[],
        )
        server._on_snapshot(None, response)
        description = json.loads(response.tree)
        self.assertEqual(description["revision"], response.revision)
        self.assertEqual(description["root"]["id"], tree.id)
        self.assertEqual(description["root"]["children"][0]["id"], "root")
        self.assertEqual(
            {status.node_id for status in response.statuses},
            {tree.id, "root", "ok"},
        )
        self.assertIsNotNone(server._snapshot_service.service)
        bt_ros.rx.remove(server, node)
        self.assertIsNone(server._snapshot_service.service)

    def test_whole_tree_can_be_used_as_one_remote_node(self):
        bus = _Bus()
        server_node = _Node("server", bus=bus)
        parent_node = _Node("parent", bus=bus)
        hosted = BehaviorTree(Sequence((Success(), Running())))
        server = _make_server(server_node, hosted, "/subtree/bt")
        remote = bt_ros.rx.add(bt_ros.RemoteBT("/subtree"), parent_node)

        self.assertEqual(remote.tick(), BTResponse.RUNNING)
        self.assertTrue(wait_until(lambda: hosted.root.get_status() == BTStatus.RUNNING))
        self.assertEqual(remote.tick(), BTResponse.RUNNING)

        bt_ros.rx.remove(remote, parent_node)
        bt_ros.rx.remove(server, server_node)


if __name__ == "__main__":
    unittest.main()
