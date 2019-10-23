import b2luigi
import random


class MyNumberTask(b2luigi.Task):
    some_parameter = b2luigi.IntParameter()

    def output(self):
        yield self.add_to_output("output_file.txt")

    def run(self):
        random_number = random.random()

        with open(self.get_output_file_name("output_file.txt"), "w") as f:
            f.write(f"{random_number}\n")


if __name__ == "__main__":
    b2luigi.set_setting("result_path", "results")
    b2luigi.process([MyNumberTask(some_parameter=i) for i in range(10)])
