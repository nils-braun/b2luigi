import b2luigi as luigi
import random


class MyNumberTask(luigi.Task):
    some_parameter = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"results/output_file_{self.some_parameter}.txt")

    def run(self):
        random_number = random.random()
        with self.output().open("w") as f:
            f.write(f"{random_number}\n")


if __name__ == "__main__":
    luigi.process([MyNumberTask(some_parameter=i) for i in range(100)])
