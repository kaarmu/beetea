# Replacing subtrees

[`BehaviorTree.replace`][beetea.bt.BehaviorTree.replace] swaps one node, and
everything under it, for a new subtree. It works while the tree is running,
including in scheduled mode.

```python
tree = BehaviorTree(
    Sequence((
        Condition(battery_ok, id="batteryOk"),
        Action(drive_route_a, id="drive"),
    ), id="mission"),
)

tree.replace(Action(drive_route_b, id="drive"), "drive")
tree.revision   # 1
```

Select the node to replace by its ID.

## What happens

1. Replacement waits for any tick in progress to finish, and no new tick
   starts until it is done.
2. The path from the root to the target is rebuilt with
   [`with_children`][beetea.bt.BTNode.with_children]. Every node not on that
   path is **reused as the same instance** and keeps its state. For example,
   an action that is already running keeps running.
3. Removed nodes that were active are halted. A removed node is not halted if
   one of its descendants is reused in the new structure.
4. New nodes are subscribed to tree-wide status listeners, and removed nodes
   are unsubscribed.
5. `revision` is incremented and
   [structure listeners](observing.md#structure-changes) are notified.

## Rules

- The tree cannot replace itself. Calling `tree.replace(..., tree.id)`
  raises `ValueError`.
- After the replacement, all IDs must still be unique, or `ValueError` is
  raised and the tree is left unchanged.
- If no node has the ID, `KeyError` is raised.

## Custom nodes and replacement

If a node has children, its type must implement `with_children()` so the
path to the target can be rebuilt. [`Composite`][beetea.bt.Composite] and
`BehaviorTree` already implement it. Leaf nodes don't need it.
