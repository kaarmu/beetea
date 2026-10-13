# BeeTea

**BeeTea** is a small, dependency-free Python library for **reactive behavior
trees**. You build a tree from conditions, asynchronous actions, sequences and
fallbacks, then tick it, either from your own loop or on a background scheduler
running at a fixed rate.

```python
from beetea import Action, BehaviorTree, Condition, Sequence

battery = {"level": 80}

tree = BehaviorTree(
    Sequence((
        Condition(lambda: battery["level"] > 20, id="batteryOk"),
        Action(lambda: print("Delivering package"), id="deliver"),
    ))
)

tree.tick()   # -> BTResponse.RUNNING while the action runs
```

## Features

- **Reactive by default.** Composites re-evaluate from their first child on
  every tick, so a condition that stops holding aborts the work that depends
  on it right away.
- **Non-blocking actions.** An [`Action`][beetea.bt.Action] runs its callable
  on a thread pool, so ticks never block on long-running work.
- **Manual or scheduled ticking.** A [`BehaviorTree`][beetea.bt.BehaviorTree]
  is either ticked by its parent or runs its own scheduler at a fixed
  `tick_rate`. Trees nest like any other node.
- **Observation.** You can subscribe to status changes, take
  revision-consistent snapshots, or export the tree to plain dictionaries.
  None of these affect execution.
- **Live subtree replacement.** You can swap a subtree while the tree runs.
  Nodes that are kept retain their state.
- **Text format.** You can describe trees in a compact indented syntax and
  parse them with [`parse_bt`][beetea.bt.parse_bt].
- **Optional ROS 2 transport.** `beetea.ros` exposes a tree over ROS services
  and topics, and lets you use a remote tree as a local node.

## Where to go next

<div class="grid cards" markdown>

- **[Getting started](getting-started.md)**

    Install BeeTea and run your first tree.

- **[Core concepts](guide/concepts.md)**

    Responses, statuses, reactivity and halting.

- **[Running trees](guide/running.md)**

    Manual ticking vs. scheduled trees, and nesting.

- **[API reference](reference/bt.md)**

    Every public class and function.

</div>
