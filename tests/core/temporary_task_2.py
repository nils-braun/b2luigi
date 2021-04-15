import b2luigi


class MyTask(b2luigi.Task):
    def output(self):
        yield self.add_to_output("test.txt")

    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("test.txt"), "w") as f:
            f.write("Test")


if __name__ == "__main__":
    b2luigi.process(MyTask())
