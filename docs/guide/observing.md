# Observing trees

BeeTea has several ways to watch a tree without changing how it runs.

## Querying status

```python
node.get_status()          # BTStatus of a single node
tree.get_statuses()        # {node_id: BTStatus} for the tree and all descendants
```

## Status listeners

### Per node

```python
def on_change(node, old, new):
    print(f"{node.id}: {old.name} -> {new.name}")

action.add_status_listener(on_change)
action.remove_status_listener(on_change)
```

A listener is called only when the status actually changes. Registering the
same callback twice has no effect.

### Tree-wide

```python
def on_event(revision, node, old, new):
    ...

tree.add_tree_status_listener(on_event)
```

A tree-wide listener receives transitions from the tree node and **all** of
its descendants, along with the current structure `revision`. Nodes added by
[`replace`](replacing.md) are subscribed automatically, and removed nodes are
unsubscribed.

!!! warning
    Listeners run synchronously in the thread that changed the status. That
    thread may be an action worker or a scheduler thread. Don't block in a
    listener, and protect any shared state it touches.

### Structure changes

```python
tree.add_structure_listener(lambda revision, program: print(revision, program))
```

A structure listener is called after each successful
[`replace`][beetea.bt.BehaviorTree.replace].

## Programs

[`get_program()`][beetea.bt.BTNode.get_program] returns the tree as indented
text:

```text
behaviortree #main
  sequence #mission
    condition #batteryOk
    action #deliver
```

## Snapshots

[`snapshot()`][beetea.bt.BehaviorTree.snapshot] captures the program and all
statuses at once, while holding the tick lock. The result is an immutable
[`BTSnapshot`][beetea.bt.BTSnapshot]:

```python
snap = tree.snapshot()
snap.revision    # 0
snap.program     # "behaviortree #main\n  sequence #mission\n ..."
snap.statuses    # {"node-4": BTStatus.SUCCESS, ...}
```

## Dictionary export

[`to_dict()`][beetea.bt.BehaviorTree.to_dict] returns plain data that you can
serialize to JSON:

```python
{
    "revision": 0,
    "root": {
        "id": "main", "type": "BehaviorTree",
        "status": "SUCCESS", "tick_rate": None, "active": True,
        "children": [
            {"id": "mission", "type": "Sequence",
             "status": "SUCCESS", "children": [...]},
        ],
    },
}
```

`BehaviorTree` entries also include `tick_rate` and `active`.

## Traversal and lookup

```python
for node in tree.walk():       # pre-order, starting at the tree itself
    ...

tree.find("deliver")
```

`find` raises `KeyError` if nothing matches. There is also a module-level
[`walk(node)`][beetea.bt.walk] that works on any node.
