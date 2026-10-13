# Running trees

A [`BehaviorTree`][beetea.bt.BehaviorTree] wraps a root node and decides how
ticks reach it. There are two modes, chosen by `tick_rate`.

## Manual mode (default)

```python
tree = BehaviorTree(root)        # tick_rate=None
```

Each call to `tree.tick()` ticks the root **synchronously** and returns the
root's response. You decide when ticks happen, for example in a control loop,
a ROS timer or a test.

```python
while tree.tick() == BTResponse.RUNNING:
    time.sleep(0.05)
tree.halt()
```

If the root raises an exception, `tick()` sets the tree's status to `FAILURE`
and re-raises the exception.

## Scheduled mode

```python
tree = BehaviorTree(root, tick_rate=20.0)   # 20 Hz
tree.start()                                # or just call tree.tick()
...
tree.halt()                                 # stops and joins the thread
```

When `tick_rate` is positive, the first `start()` or `tick()` call starts a
daemon thread that ticks the root at the given frequency. In this mode:

- `tick()` does **not** tick the root. It returns the latest cached response,
  or `RUNNING` on the call that starts the scheduler.
- The scheduler keeps ticking after `SUCCESS` or `FAILURE`, so the tree stays
  reactive until you call `halt()`.
- If the root raises an exception, the scheduler logs it and caches `FAILURE`.
- `halt()` stops the thread and waits for it to exit before halting the root,
  so the root is not ticked again after `halt()` returns.

`tree.is_active` tells you whether the current activation has started.

## Nesting trees

`BehaviorTree` is itself a node, so one tree can be a child of another. This
lets parts of a tree run at different rates:

```python
perception = BehaviorTree(
    Sequence((Condition(camera_ok), Action(update_map))),
    tick_rate=30,
    id="perception",
)

mission = BehaviorTree(
    Sequence((perception, Action(execute_plan))),
    id="mission",
)

mission.tick()   # starts perception's scheduler, returns its cached response
```

When the parent ticks a scheduled subtree, the subtree's scheduler starts if
it is not running yet, and the parent gets the subtree's latest response.
When the parent stops selecting the subtree, it halts it, and that stops the
subtree's scheduler.

Nested trees are structurally transparent. `walk()`, `find()`, `get_program()`
and `to_dict()` all include the nested tree's descendants. Node IDs must be
unique across the whole tree, nested trees included.

## Thread safety

- Ticking, halting and [replacing](replacing.md) are serialized by internal
  locks, so you can call them from different threads.
- Actions run on a shared pool of four worker threads named `bt-action-*`.
  If you need more than four actions running at once, use your own threads
  and a custom node.
- Status listeners run **synchronously** in whichever thread changed the
  status: the ticking thread, the scheduler thread or an action worker. Keep
  listeners short.
