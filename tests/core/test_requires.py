from ..helpers import B2LuigiTestCase

import b2luigi


class RequiresTestCase(B2LuigiTestCase):
    def test_requires(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()
            some_other_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("test.txt")

        @b2luigi.requires(TaskA, some_parameter=3)
        class TaskB(b2luigi.Task):
            another_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("out.dat")

        task = TaskB(some_other_parameter=1, another_parameter=42)
        self.assertEqual(sorted(task.get_param_names()), ["another_parameter", "some_other_parameter"])
        self.assertEqual(task.another_parameter, 42)
        self.assertEqual(task.some_other_parameter, 1)

        self.assertTrue(task.get_output_file_name("out.dat").endswith("results/some_other_parameter=1/another_parameter=42/out.dat"))

        input_files = task.get_input_file_names("test.txt")
        self.assertEqual(len(input_files), 1)
        self.assertTrue(input_files[0].endswith("results/some_parameter=3/some_other_parameter=1/test.txt"))

        required_task = next(task.requires())
        self.assertEqual(sorted(required_task.get_param_names()), ["some_other_parameter", "some_parameter"])
        self.assertEqual(required_task.some_parameter, 3)
        self.assertEqual(required_task.some_other_parameter, 1)
        self.assertTrue(required_task.get_output_file_name("test.txt").endswith("results/some_parameter=3/some_other_parameter=1/test.txt"))
