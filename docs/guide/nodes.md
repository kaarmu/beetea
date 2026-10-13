# Nodes

All tree elements subclass [`BTNode`][beetea.bt.BTNode]. Every constructor
accepts an optional `id=` keyword argument (see
[Identity](concepts.md#identity)).

## Leaves

### Constant leaves

| Node                              | Tick returns |
| --------------------------------- | ------------ |
| [`Success`][beetea.bt.Success]    | `SUCCESS`    |
| [`Failure`][beetea.bt.Failure]    | `FAILURE`    |
| [`Running`][beetea.bt.Running]    | `RUNNING` until halted |

These leaves are useful as placeholders, in tests, and as idle branches,
for example `Running()` as the last child of a fallback.

### Condition

[`Condition`][beetea.bt.Condition] wraps a predicate that takes no arguments.
The predicate runs on **every** tick, and its truth value becomes `SUCCESS` or
`FAILURE`.

```python
Condition(lambda: robot.battery > 0.2, id="batteryOk")
```

Predicates run inside the tick, so they must be fast and must not block.
Don't do I/O in a predicate. Read a cached value instead.

### Action

[`Action`][beetea.bt.Action] runs a callable that takes no arguments on a
shared thread pool, once per activation.

1. The first tick submits the callable and returns `RUNNING`.
2. Later ticks return `RUNNING` until the callable finishes.
3. After that, the result is cached. Every tick returns it until the action
   is [halted](concepts.md#activations-and-halting).

The callable's return value is converted to a response:

| Callable returns                         | Response  |
| ---------------------------------------- | --------- |
| `None`, `True`, `BTResponse.SUCCESS`     | `SUCCESS` |
| `False`, `BTResponse.FAILURE`            | `FAILURE` |
| raises, `BTResponse.RUNNING`, or any other value | `FAILURE` |

If the callable raises an exception or returns an unsupported value, BeeTea
logs it and stores it in [`last_exception`][beetea.bt.Action.last_exception]:

```python
def risky():
    raise RuntimeError("gripper jammed")

grab = Action(risky, id="grab")
# ... after it has completed:
grab.get_status()       # BTStatus.FAILURE
grab.last_exception     # RuntimeError('gripper jammed')
```

!!! tip
    Because the result is cached, an action that has finished stays finished
    while it remains selected. A reactive sequence therefore does not run
    completed steps again when it goes on to later children.

## Composites

Composites take a **tuple** of children in evaluation order.

### Sequence

[`Sequence`][beetea.bt.Sequence] works like a logical AND. It ticks the
children in order and stops at the first child that returns anything other
than `SUCCESS`. An empty sequence returns `SUCCESS`.

```python
Sequence((
    Condition(door_open),
    Action(drive_through),
))
```

### Fallback

[`Fallback`][beetea.bt.Fallback] works like a logical OR. It ticks the
children in order and stops at the first child that returns anything other
than `FAILURE`. An empty fallback returns `FAILURE`.

```python
Fallback((
    Condition(door_open),      # already open? done
    Action(open_door),         # otherwise try to open it
    Action(call_for_help),     # last resort
))
```

Both composites halt any later children that are still active when they stop
early.

## BehaviorTree

[`BehaviorTree`][beetea.bt.BehaviorTree] is also a `BTNode`. It owns a root,
controls how that root is ticked, and provides lookup, observation and
replacement. See [Running trees](running.md).

## Custom nodes

You can create your own node by subclassing `BTNode`. Implement `tick()`, call
`self._set_status(...)` so that observers see the change, and override
`halt()` if the node holds state:

```python
from beetea import BTNode, BTResponse, BTStatus


class Counter(BTNode):
    """Return RUNNING for n ticks, then SUCCESS."""

    def __init__(self, n, **kwargs):
        super().__init__(**kwargs)
        self.n = n
        self.count = 0

    def tick(self):
        self.count += 1
        response = BTResponse.SUCCESS if self.count >= self.n else BTResponse.RUNNING
        self._set_status(BTStatus(response))
        return response

    def halt(self):
        self.count = 0
        super().halt()  # sets status to SKIPPED
```

A custom composite should subclass [`Composite`][beetea.bt.Composite], which
already provides `children()`, `halt()`, `get_program()` and
`with_children()`. `with_children()` is needed for
[subtree replacement](replacing.md). A new composite only has to implement
`tick()`.
