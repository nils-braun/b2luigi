from ..helpers import B2LuigiTestCase

import b2luigi


class TaskTestCase(B2LuigiTestCase):
    def test_file_path_usage(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("file_a")
                yield self.add_to_output("file_b")
        
        task = TaskA(some_parameter=3)

        b2luigi.set_setting("result_path", "results/some_crazy_path")
        
        self.assertEqual(task.get_filled_params(), {"some_parameter": 3})
        self.assertFalse(task.get_input_file_names())
        self.assertRaises(KeyError, lambda: task._get_input_targets("some_file"))
        self.assertEqual(task._get_output_target("file_a").path, task.get_output_file_name("file_a"))
        self.assertIn("file_a", task.get_output_file_name("file_a"))
        self.assertIn("file_b", task.get_output_file_name("file_b"))
        self.assertIn("some_parameter=3", task.get_output_file_name("file_a"))
        self.assertIn("some_crazy_path", task.get_output_file_name("file_a"))

    def test_dependencies(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("file_a")

        @b2luigi.requires(TaskA)
        class TaskB(b2luigi.Task):
            def output(self):
                yield self.add_to_output("file_b")

        task = TaskB(some_parameter=42)

        self.assertEqual(task.get_filled_params(), {"some_parameter": 42})
        self.assertEqual(len(task._get_input_targets("file_a")), 1)
        self.assertEqual(len(task.get_input_file_names("file_a")), 1)
        self.assertEqual(len(task.get_input_file_names().keys()), 1)
        self.assertEqual(task._get_input_targets("file_a")[0].path, task.get_input_file_names("file_a")[0])
        self.assertEqual(task._get_output_target("file_b").path, task.get_output_file_name("file_b"))
        self.assertIn("file_b", task.get_output_file_name("file_b"))
        self.assertIn("some_parameter=42", task.get_output_file_name("file_b"))

    def test_many_dependencies(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("file_a")

        class TaskB(b2luigi.Task):
            def requires(self):
                for i in range(100):
                    yield self.clone(TaskA, some_parameter=i)

        task = TaskB()

        self.assertEqual(len(task._get_input_targets("file_a")), 100)
        self.assertEqual(len(task.get_input_file_names("file_a")), 100)
        self.assertEqual(len(task.get_input_file_names().keys()), 1)

        self.assertEqual(len(task.get_input_file_names()["file_a"]), 100)

        input_file_names = task.get_input_file_names("file_a")

        for i in range(100):
            self.assertIn(f"results/some_parameter={i}/file_a", input_file_names)
