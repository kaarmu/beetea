# BeeTea

BeeTea is a small Python library for reactive behavior trees. It provides
composable conditions, asynchronous actions, sequences, fallbacks, subtree
replacement, status observation, and optional ROS 2 transport adapters.

## Installation

Install the core behavior-tree library from PyPI:

```console
python -m pip install beetea
```

The core package has no runtime dependencies. The `beetea.ros` module is
optional and expects `rclpy`, `std_srvs`, `beetea_msgs`, and `svea_core` to be
installed in the ROS 2 environment.

## Quick start

```python
from beetea import Action, BehaviorTree, Condition, Sequence

ready = True

tree = BehaviorTree(
    Sequence(
        (
            Condition(lambda: ready, label="ready"),
            Action(lambda: print("Working"), label="work"),
        )
    )
)

response = tree.tick()
```

`Action` callables run asynchronously, so an action normally returns
`RUNNING` on its first tick and publishes its terminal status when the callable
finishes. Call `tree.halt()` when the tree should become inactive.

Trees can also be parsed from the indented text format:

```python
from beetea import Action, Condition, parse_bt

root = parse_bt(
    """\
sequence #mission
  $ready
  $work
""",
    {
        "ready": lambda: Condition(lambda: True),
        "work": lambda: Action(lambda: None),
    },
)
```

## Development

Run the dependency-free core test suite with:

```console
python -m unittest beetea.tests.test_bt
```

The ROS adapter tests additionally require the `svea_core` package to be
importable. To build and validate release artifacts:

```console
python -m build
python -m twine check dist/*
```

## License

A license has not yet been selected. Choose a license and add its text before
publishing the package.
