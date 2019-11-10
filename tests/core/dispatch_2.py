import b2luigi
import os


class MyTask(b2luigi.Task):
    def output(self):
        yield self.add_to_output("some_file.txt")
        yield self.add_to_output("some_other_file.txt")

    @b2luigi.dispatch
    def run(self):
        print("Hello!")
        with open(self.get_output_file_name("some_file.txt"), "w") as f:
            f.write("Done")

        print("Bye!")
        import sys
        sys.stdout.flush()
        os.kill(os.getpid(), 11)

        with open(self.get_output_file_name("some_other_file.txt"), "w") as f:
            f.write("Done")


if __name__ == "__main__":
    b2luigi.set_setting("result_dir", "results")
    b2luigi.process(MyTask())