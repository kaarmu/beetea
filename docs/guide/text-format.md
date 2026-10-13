# Text format

You can write trees as indented text and build them with
[`parse_bt`][beetea.bt.parse_bt]:

```python
from beetea import Action, BehaviorTree, Condition, parse_bt

root = parse_bt(
    """\
fallback #root
  sequence #deliver
    $batteryOk
    $drive
  running #idle
""",
    {
        "batteryOk": lambda: Condition(lambda: battery["level"] > 20),
        "drive": lambda: Action(deliver_package),
    },
)

tree = BehaviorTree(root)
```

## Syntax

Each non-blank line has one node:

```text
<keyword> [#id]
```

- Children are indented by **exactly two spaces** more than their parent.
  Tabs are rejected.
- The text must contain exactly one root.
- After the keyword you can give the node an ID with `#name`. `$` is only
  used for [symbols](#symbols).

### Keywords

| Keyword     | Node                                     | Children |
| ----------- | ---------------------------------------- | -------- |
| `sequence`  | [`Sequence`][beetea.bt.Sequence]         | any      |
| `fallback`  | [`Fallback`][beetea.bt.Fallback]         | any      |
| `success`   | [`Success`][beetea.bt.Success]           | none     |
| `failure`   | [`Failure`][beetea.bt.Failure]           | none     |
| `running`   | [`Running`][beetea.bt.Running]           | none     |
| `$name`     | looked up in `symbols["name"]`           | none     |

## Symbols

Conditions, actions and other application nodes are added through the
`symbols` mapping. You refer to them as `$name`, without the `$` in the
mapping key. A value can be:

- **A factory** that takes no arguments and returns a `BTNode`. The factory
  is called once for every occurrence, so the same symbol can appear in
  several places. An `#id` written after the symbol is applied to the new
  node.
- **A `BTNode` instance**, used as it is. An `#id` written after the symbol
  must match the instance's own ID, because the parser does
  not change an existing instance.

```text
sequence
  $batteryOk #checkBattery
  $drive #drive-1
```

If both symbols map to factories, the condition gets the ID
`checkBattery` and the action gets the ID `drive-1`. The format has no
comment syntax.

!!! warning "Reusing an instance"
    If a symbol maps to a node instance and appears twice, both places refer
    to the same object, and the duplicate ID check fails. Use factories for
    symbols that you want to use more than once.

## Errors

| Exception    | Raised when                                                      |
| ------------ | ---------------------------------------------------------------- |
| `ValueError` | empty text, tabs, odd or jumping indentation, multiple roots, children under a leaf, unknown keyword, malformed `#id` suffix, duplicate IDs |
| `KeyError`   | a `$symbol` is missing from `symbols`                             |
| `TypeError`  | a symbol does not produce a `BTNode`                              |

Error messages include the line number.

## Relation to `get_program()`

[`get_program()`][beetea.bt.BTNode.get_program] uses the same indented
layout, but it is meant for **diagnostics** and cannot be parsed back.
It prints `behaviortree`, `condition` and `action` lines, and `parse_bt` does
not accept those keywords. To parse a tree, write `$symbol` lines for those
nodes instead.

!!! note
    `parse_tree` is an alias of `parse_bt`, kept for older clients.
