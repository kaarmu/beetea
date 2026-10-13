# Getting started

## Installation

BeeTea requires Python 3.10 or newer and has no runtime dependencies.

```console
python -m pip install beetea
```

The optional [`beetea.ros`](guide/ros.md) module also needs a ROS 2
environment with `rclpy`, `std_srvs`, `beetea_msgs` and `svea_core`.

## Your first tree

The following tree delivers a package, but only while the battery is above 20%:

```python
import time

from beetea import Action, BehaviorTree, BTResponse, Condition, Sequence

battery = {"level": 80}


def deliver_package():
    print("Driving to the destination...")
    time.sleep(0.5)  # (1)!
    print("Package delivered!")
    return True


tree = BehaviorTree(
    Sequence((
        Condition(lambda: battery["level"] > 20, id="batteryOk"),
        Action(deliver_package, id="deliver"),
    ))
)

while tree.tick() == BTResponse.RUNNING:  # (2)!
    time.sleep(0.05)

print("Tree finished with:", tree.get_status().name)
tree.halt()  # (3)!
```

1. Blocking is fine here, because actions run on a worker thread and the tick
   loop keeps going.
2. The first tick launches the action and returns `RUNNING`. Later ticks
   re-check the condition and return the action's cached result after it
   completes.
3. Halting deactivates the tree, so the next tick starts a new activation
   that runs the action again.

Output:

```text
Driving to the destination...
Package delivered!
Tree finished with: SUCCESS
```

If `battery["level"]` drops to 20 or below while the action is running, the
next tick returns `FAILURE`, and the sequence halts the action so its result
is discarded. See [Core concepts](guide/concepts.md) for details.

## Inspecting the tree

```python
print(tree.get_program())
```

```text
behaviortree #node-4
  sequence #node-3
    condition #batteryOk
    action #deliver
```

Each node is shown with its `#id`, generated (such as `node-4`) when you
didn't pass one. See [Observing trees](guide/observing.md) for listeners, snapshots
and dictionary export.

## Next steps

- [Nodes](guide/nodes.md): every built-in node type and how to write your own.
- [Running trees](guide/running.md): run a tree at a fixed rate in the
  background.
- [Text format](guide/text-format.md): describe trees as text.
