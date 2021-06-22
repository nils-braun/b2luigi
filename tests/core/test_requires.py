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

        self.assertTrue(
            task.get_output_file_name("out.dat").endswith("results/some_other_parameter=1/another_parameter=42/out.dat")
        )

        input_files = task.get_input_file_names("test.txt")
        self.assertEqual(len(input_files), 1)
        self.assertTrue(input_files[0].endswith("results/some_parameter=3/some_other_parameter=1/test.txt"))

        required_task = next(task.requires())
        self.assertEqual(sorted(required_task.get_param_names()), ["some_other_parameter", "some_parameter"])
        self.assertEqual(required_task.some_parameter, 3)
        self.assertEqual(required_task.some_other_parameter, 1)
        self.assertTrue(
            required_task.get_output_file_name("test.txt").endswith("results/some_parameter=3/some_other_parameter=1/test.txt")
        )


class InheritsTestCase(B2LuigiTestCase):
    def test_inherits(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()
            some_other_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("test.txt")

        @b2luigi.inherits(TaskA, without='some_other_parameter')
        class TaskB(b2luigi.Task):
            another_parameter = b2luigi.IntParameter()

            def requires(self):
                for my_other_parameter in range(10):
                    yield self.clone(TaskA, some_other_parameter=my_other_parameter)

            def run(self):
                # somehow merge the output of TaskA to create "out.dat"
                pass

            def output(self):
                yield self.add_to_output("out.dat")

        task = TaskB(some_parameter=23, another_parameter=42)
        self.assertEqual(task.get_param_names(), ["some_parameter", "another_parameter"])
        self.assertEqual(task.another_parameter, 42)
        self.assertEqual(task.some_parameter, 23)
        self.assertFalse(hasattr(task, 'some_other_parameter'))

        self.assertTrue(
            task.get_output_file_name("out.dat").endswith("results/some_parameter=23/another_parameter=42/out.dat")
        )

        input_files = task.get_input_file_names("test.txt")
        self.assertEqual(len(input_files), 10)
        for my_other_parameter in range(10):
            self.assertTrue(input_files[my_other_parameter]
                            .endswith(f"results/some_parameter=23/some_other_parameter={my_other_parameter}/test.txt")
                            )

        required_tasks = list(task.requires())
        for some_other_parameter_values, required_task in enumerate(required_tasks):
            self.assertEqual(sorted(required_task.get_param_names()), ["some_other_parameter", "some_parameter"])
            self.assertEqual(required_task.some_other_parameter, some_other_parameter_values)
            self.assertEqual(required_task.some_parameter, 23)
            self.assertTrue(
                required_task
                .get_output_file_name("test.txt")
                .endswith(f"results/some_parameter=23/some_other_parameter={some_other_parameter_values}/test.txt")
            )
