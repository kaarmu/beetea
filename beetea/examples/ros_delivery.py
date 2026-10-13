"""Split a delivery mission across two ROS 2 nodes with rosonic.

The worker serves a behavior tree under ``/worker/bt``. The mission node runs
its own tree and ticks the worker's tree through a :class:`RemoteBT` node, as
if it were a local subtree.

Run each node in its own terminal::

    python -m beetea.examples.ros_delivery worker
    python -m beetea.examples.ros_delivery mission

Watch the worker's status transitions with::

    ros2 topic echo /worker/bt/events
"""

import sys
import time

import rosonic as rx

from beetea import Action, BehaviorTree, BTResponse, Condition, Sequence
import beetea.ros as br


class Worker(rx.Node):
    """Serve a delivery tree to other processes."""

    @br.BTServer(namespace="bt")
    def init_bt(self):
        return BehaviorTree(
            Sequence((
                Action(self.pick_up, id="pickUp"),
                Action(self.drive, id="drive"),
            ), id="delivery")
        )

    def pick_up(self):
        self.get_logger().info("Picking up package...")
        time.sleep(1.0)
        return True

    def drive(self):
        self.get_logger().info("Driving to destination...")
        time.sleep(2.0)
        return True


class Mission(rx.Node):
    """Check preconditions locally and delegate the delivery to the worker."""

    min_battery = rx.Parameter(20)

    def on_startup(self):
        self.battery = 80
        self.tree = BehaviorTree(
            Sequence((
                Condition(lambda: self.battery > self.min_battery, id="batteryOk"),
                rx.add(br.RemoteBT("/worker", timeout=1.0, id="worker"), self),
            ), id="mission")
        )

    @rx.Timer(0.1)
    def loop(self):
        response = self.tree.tick()
        if response != BTResponse.RUNNING:
            self.get_logger().info(f"Mission finished with {response.name}")
            self.tree.halt()
            self.loop.cancel()

    def on_shutdown(self):
        self.tree.halt()


if __name__ == "__main__":
    nodes = {"worker": Worker, "mission": Mission}
    if len(sys.argv) < 2 or sys.argv[1] not in nodes:
        sys.exit(f"usage: {sys.argv[0]} {{{'|'.join(nodes)}}}")
    nodes[sys.argv[1]].main()
