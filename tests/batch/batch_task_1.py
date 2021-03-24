import b2luigi


class MyTask(b2luigi.Task):
    some_parameter = b2luigi.Parameter()

    def output(self):
        yield self.add_to_output("test.txt")

    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("test.txt"), "w") as f:
            f.write("Test")


@b2luigi.requires(MyTask)
class MyAdditionalTask(b2luigi.Task):
    def output(self):
        yield self.add_to_output("combined.txt")

    @b2luigi.dispatch
    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("combined.txt"), "w") as f:
            with open(self.get_input_file_names("test.txt")[0], "r") as from_f:
                f.write(from_f.read())


if __name__ == "__main__":
    b2luigi.set_setting("batch_system", "test")
    b2luigi.process(MyAdditionalTask(some_parameter="bla_blub"), batch=True)
