import threading
import time
import unittest

from beetea.bt import (
    AmbiguousNodeError,
    Action,
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
    parse_tree,
)


def wait_until(predicate, timeout=1.0):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)
    return predicate()


class BehaviorTreeTests(unittest.TestCase):
    def test_constant_leaves_update_status(self):
        for node, response in ((Success(), BTResponse.SUCCESS), (Failure(), BTResponse.FAILURE), (Running(), BTResponse.RUNNING)):
            self.assertEqual(node.get_status(), BTStatus.SKIPPED)
            self.assertEqual(node.tick(), response)
            self.assertEqual(node.get_status(), BTStatus(response))

    def test_sequence_is_reactive_and_skips_later_children(self):
        ready = [True]
        calls = []
        action = Running(label="action")
        tree = BehaviorTree(Sequence((Condition(lambda: ready[0]), action)))
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        ready[0] = False
        self.assertEqual(tree.root.tick(), BTResponse.FAILURE)
        self.assertEqual(action.get_status(), BTStatus.SKIPPED)

        counting = Condition(lambda: calls.append(1) is None or True)
        Sequence((counting, Success())).tick()
        Sequence((counting, Success())).tick()
        self.assertEqual(len(calls), 2)

    def test_fallback_identity_and_reactivity(self):
        self.assertEqual(Fallback(()).tick(), BTResponse.FAILURE)
        condition = [False]
        fallback = Fallback((Condition(lambda: condition[0]), Success()))
        self.assertEqual(fallback.tick(), BTResponse.SUCCESS)
        condition[0] = True
        self.assertEqual(fallback.tick(), BTResponse.SUCCESS)

    def test_action_is_async_and_has_observable_completion(self):
        started = threading.Event()
        release = threading.Event()

        def action():
            started.set()
            release.wait(2)
            return True

        node = Action(action)
        self.assertEqual(node.tick(), BTResponse.RUNNING)
        self.assertTrue(started.wait(1))
        self.assertEqual(node.get_status(), BTStatus.RUNNING)
        release.set()
        deadline = time.monotonic() + 1
        while node.get_status() != BTStatus.SUCCESS and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(node.get_status(), BTStatus.SUCCESS)
        self.assertEqual(node.tick(), BTResponse.SUCCESS)
        self.assertEqual(node.tick(), BTResponse.SUCCESS)
        node.halt()

    def test_completed_action_is_sticky_in_reactive_sequence(self):
        first = Action(lambda: True, label="first")
        second = Running(label="second")
        tree = BehaviorTree(Sequence((first, second)))
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        deadline = time.monotonic() + 1
        while first.get_status() != BTStatus.SUCCESS and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        self.assertEqual(first.get_status(), BTStatus.SUCCESS)
        self.assertEqual(second.get_status(), BTStatus.RUNNING)

    def test_skipping_terminal_action_deactivates_and_resets_it(self):
        ready = [True]
        calls = [0]

        def action():
            calls[0] += 1
            return True

        node = Action(action)
        tree = BehaviorTree(Sequence((Condition(lambda: ready[0]), node)))
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        deadline = time.monotonic() + 1
        while node.get_status() != BTStatus.SUCCESS and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(tree.root.tick(), BTResponse.SUCCESS)

        ready[0] = False
        self.assertEqual(tree.root.tick(), BTResponse.FAILURE)
        self.assertEqual(node.get_status(), BTStatus.SKIPPED)

        ready[0] = True
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        deadline = time.monotonic() + 1
        while node.get_status() != BTStatus.SUCCESS and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(calls[0], 2)

    def test_action_halt_ignores_stale_result(self):
        first_release = threading.Event()
        first_started = threading.Event()
        calls = [0]

        def action():
            calls[0] += 1
            if calls[0] == 1:
                first_started.set()
                first_release.wait(2)
            return calls[0] == 2

        node = Action(action)
        node.tick()
        self.assertTrue(first_started.wait(1))
        node.halt()
        self.assertEqual(node.tick(), BTResponse.RUNNING)
        first_release.set()
        deadline = time.monotonic() + 1
        while node.get_status() != BTStatus.SUCCESS and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(node.get_status(), BTStatus.SUCCESS)

    def test_action_exception_is_failure_with_diagnostic(self):
        error = RuntimeError("boom")
        node = Action(lambda: (_ for _ in ()).throw(error))
        self.assertEqual(node.tick(), BTResponse.RUNNING)
        deadline = time.monotonic() + 1
        while node.get_status() != BTStatus.FAILURE and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(node.tick(), BTResponse.FAILURE)
        self.assertIs(node.last_exception, error)

    def test_replacement_lookup_events_and_snapshot(self):
        old = Running(label="work", id="work-id")
        tree = BehaviorTree(Sequence((Success(id="first"), old), id="root"))
        status_events = []
        structure_events = []
        tree.add_status_listener(lambda *event: status_events.append(event))
        tree.add_structure_listener(lambda *event: structure_events.append(event))
        tree.add_tree_status_listener(lambda *event: status_events.append(event))
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        tree.replace(Failure(label="new"), id="work-id")
        self.assertEqual(tree.revision, 1)
        self.assertEqual(tree.find(label="new").get_status(), BTStatus.SKIPPED)
        self.assertEqual(tree.root.children()[0].id, "first")
        self.assertEqual(old.get_status(), BTStatus.SKIPPED)
        self.assertEqual(len(structure_events), 1)
        snapshot = tree.snapshot()
        self.assertEqual(snapshot.revision, tree.revision)
        self.assertEqual(set(snapshot.statuses), {node.id for node in tree.walk()})
        self.assertTrue(status_events)

    def test_replacement_does_not_halt_reused_nodes(self):
        condition = Condition(lambda: True, label="ready")
        old_action = Running(label="old")
        tree = BehaviorTree(Sequence((condition, old_action), id="root"))
        self.assertEqual(tree.root.tick(), BTResponse.RUNNING)
        new_root = Sequence((condition, Success(label="new")), id="root")
        tree.replace(new_root, id="root")
        self.assertEqual(condition.get_status(), BTStatus.SUCCESS)
        self.assertEqual(old_action.get_status(), BTStatus.SKIPPED)

    def test_duplicate_and_ambiguous_identity_are_rejected(self):
        with self.assertRaises(ValueError):
            BehaviorTree(Sequence((Success(id="same"), Failure(id="same"))))
        tree = BehaviorTree(Sequence((Success(label="same"), Failure(label="same"))))
        with self.assertRaises(AmbiguousNodeError):
            tree.find(label="same")

    def test_labels_must_not_contain_whitespace(self):
        for label in ("two words", "line\nbreak", "tab\tseparated"):
            with self.subTest(label=label), self.assertRaises(ValueError):
                Success(label=label)
        node = Success(label="valid-label")
        with self.assertRaises(ValueError):
            node.label = "not valid"

    def test_program_distinguishes_labels_and_ids(self):
        self.assertEqual(Success(label="leaf", id="leaf-id").get_program(), "success #leaf")
        self.assertEqual(Success(id="leaf-id").get_program(), "success $leaf-id")
        self.assertEqual(Action(lambda: True, id="work-id").get_program(), "action $work-id")

        root = parse_tree("sequence $root-id\n  success #done\n  failure $failure-id\n", {})
        self.assertEqual(root.id, "root-id")
        self.assertEqual(root.children()[0].label, "done")
        self.assertEqual(root.children()[1].id, "failure-id")

    def test_parser_uses_factories(self):
        made = []
        root = parse_tree("sequence #main\n  $ready\n  fallback\n    success\n    $recover\n", {
            "ready": lambda: Condition(lambda: True, label="ready"),
            "recover": lambda: Action(lambda: False),
        })
        tree = BehaviorTree(root)
        self.assertEqual(tree.root.label, "main")
        self.assertEqual(tree.root.tick(), BTResponse.SUCCESS)
        self.assertIn("sequence #main", tree.get_program())
        self.assertFalse(made)

    def test_behavior_tree_is_node_with_validated_run_parameters(self):
        tree = BehaviorTree(Success(id="root"), tick_rate=25, label="tree", id="tree")
        self.assertIsInstance(tree, BTNode)
        self.assertEqual(tree.tick_rate, 25.0)
        self.assertFalse(tree.is_active)
        self.assertEqual(tree.get_status(), BTStatus.SKIPPED)
        self.assertEqual(tuple(tree.walk()), (tree, tree.root))
        self.assertEqual(set(tree.get_statuses()), {"tree", "root"})
        self.assertIn("behaviortree #tree", tree.get_program())
        self.assertEqual(tree.to_dict()["root"]["tick_rate"], 25.0)
        with self.assertRaises(ValueError):
            BehaviorTree(Success(), tick_rate=0)
        with self.assertRaises(ValueError):
            BehaviorTree(Success(id="same"), id="same")

    def test_default_tree_ticks_manually_through_nested_trees(self):
        calls = [0]
        inner = BehaviorTree(
            Condition(lambda: calls.__setitem__(0, calls[0] + 1) or True),
            label="inner",
        )
        outer = BehaviorTree(Sequence((inner,)))

        self.assertIsNone(inner.tick_rate)
        self.assertIsNone(outer.tick_rate)
        outer.start()
        self.assertFalse(outer.is_active)
        self.assertEqual(calls[0], 0)

        self.assertEqual(outer.tick(), BTResponse.SUCCESS)
        self.assertEqual(calls[0], 1)
        self.assertTrue(outer.is_active)
        self.assertTrue(inner.is_active)
        self.assertEqual(outer.tick(), BTResponse.SUCCESS)
        self.assertEqual(calls[0], 2)

        outer.halt()
        self.assertFalse(outer.is_active)
        self.assertFalse(inner.is_active)
        self.assertEqual(inner.get_status(), BTStatus.SKIPPED)

    def test_start_ticks_continuously_until_halt(self):
        calls = [0]
        tree = BehaviorTree(
            Condition(lambda: calls.__setitem__(0, calls[0] + 1) or True),
            tick_rate=100,
        )
        tree.start()
        tree.start()
        self.assertTrue(wait_until(lambda: calls[0] >= 3))
        self.assertTrue(tree.is_active)
        self.assertEqual(tree.get_status(), BTStatus.SUCCESS)
        count_at_halt = calls[0]
        tree.halt()
        time.sleep(0.03)
        self.assertFalse(tree.is_active)
        self.assertEqual(tree.get_status(), BTStatus.SKIPPED)
        self.assertEqual(tree.root.get_status(), BTStatus.SKIPPED)
        self.assertEqual(calls[0], count_at_halt)

    def test_tick_activates_tree_and_returns_cached_response(self):
        calls = [0]
        tree = BehaviorTree(
            Condition(lambda: calls.__setitem__(0, calls[0] + 1) or True),
            tick_rate=1,
        )
        self.assertEqual(tree.tick(), BTResponse.RUNNING)
        self.assertTrue(wait_until(lambda: tree.get_status() == BTStatus.SUCCESS))
        self.assertEqual(tree.tick(), BTResponse.SUCCESS)
        self.assertEqual(tree.tick(), BTResponse.SUCCESS)
        self.assertEqual(calls[0], 1)
        tree.halt()

    def test_nested_tree_runs_independently_and_stops_when_skipped(self):
        guard = [True]
        outer_ticks = [0]
        inner_ticks = [0]
        inner = BehaviorTree(
            Condition(lambda: inner_ticks.__setitem__(0, inner_ticks[0] + 1) or True),
            tick_rate=100,
            label="inner",
        )
        outer = BehaviorTree(
            Sequence((
                Condition(lambda: outer_ticks.__setitem__(0, outer_ticks[0] + 1) or guard[0]),
                inner,
            )),
            tick_rate=20,
        )
        outer.start()
        self.assertTrue(wait_until(lambda: inner_ticks[0] >= 4))
        self.assertGreater(inner_ticks[0], outer_ticks[0])
        self.assertTrue(wait_until(lambda: outer.get_status() == BTStatus.SUCCESS))
        self.assertEqual(outer.tick(), BTResponse.SUCCESS)

        guard[0] = False
        self.assertTrue(wait_until(lambda: not inner.is_active))
        stopped_at = inner_ticks[0]
        time.sleep(0.03)
        self.assertEqual(inner_ticks[0], stopped_at)
        self.assertEqual(inner.get_status(), BTStatus.SKIPPED)

        guard[0] = True
        self.assertTrue(wait_until(lambda: inner.is_active))
        self.assertTrue(wait_until(lambda: inner_ticks[0] > stopped_at))
        outer.halt()
        self.assertFalse(inner.is_active)

    def test_scheduler_recovers_after_tick_exception(self):
        calls = [0]

        class Flaky(Success):
            def tick(self):
                calls[0] += 1
                if calls[0] == 1:
                    raise RuntimeError("first tick fails")
                return super().tick()

        tree = BehaviorTree(Flaky(), tick_rate=100)
        tree.start()
        self.assertTrue(wait_until(lambda: calls[0] >= 2))
        self.assertTrue(wait_until(lambda: tree.get_status() == BTStatus.SUCCESS))
        tree.halt()

    def test_replacement_is_safe_while_scheduler_is_active(self):
        old = Running(label="old")
        tree = BehaviorTree(old, tick_rate=100)
        tree.start()
        self.assertTrue(wait_until(lambda: old.get_status() == BTStatus.RUNNING))

        tree.replace(Success(label="new"), label="old")
        self.assertEqual(tree.revision, 1)
        self.assertTrue(tree.is_active)
        self.assertEqual(old.get_status(), BTStatus.SKIPPED)
        self.assertTrue(wait_until(lambda: tree.get_status() == BTStatus.SUCCESS))
        self.assertEqual(tree.find(label="new").get_status(), BTStatus.SUCCESS)
        tree.halt()

    def test_replacing_inside_active_nested_tree_stops_old_scheduler(self):
        retained = Condition(lambda: True, label="retained")
        old = Running(label="old")
        inner = BehaviorTree(Sequence((retained, old)), tick_rate=100)
        outer = BehaviorTree(Sequence((inner,)), tick_rate=100)
        outer.start()
        self.assertTrue(wait_until(lambda: inner.is_active))
        self.assertTrue(wait_until(lambda: old.get_status() == BTStatus.RUNNING))

        outer.replace(Success(label="new"), label="old")
        self.assertFalse(inner.is_active)
        self.assertEqual(inner.get_status(), BTStatus.SKIPPED)
        self.assertEqual(old.get_status(), BTStatus.SKIPPED)
        self.assertIs(outer.find(label="retained"), retained)
        self.assertTrue(wait_until(lambda: outer.get_status() == BTStatus.SUCCESS))
        outer.halt()

    def test_parser_rejects_bad_indentation_and_unknown_symbols(self):
        with self.assertRaises(ValueError):
            parse_tree("sequence\n   success\n", {})
        with self.assertRaises(KeyError):
            parse_tree("$missing\n", {})


if __name__ == "__main__":
    unittest.main()
