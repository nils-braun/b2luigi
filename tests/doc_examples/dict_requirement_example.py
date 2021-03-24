import b2luigi as luigi
import random


class TaskA(luigi.Task):
    central_value = luigi.FloatParameter()
    index = luigi.IntParameter()

    def run(self):

        with open(self.get_output_file_name("random_numbers.txt"), "w") as f:
            for _ in range(1000):
                f.write(f"{random.gauss(self.central_value, 0.0)}\n")

    def output(self):
        yield self.add_to_output("random_numbers.txt")


class TaskB(luigi.Task):

    def requires(self):
        self.required_tasks = {
            "a": (TaskA(5.0, i) for i in range(100)),
            "b": (TaskA(1.0, i) for i in range(100)),
        }
        return self.required_tasks

    @staticmethod
    def average_file(input_file):
        # Build the mean
        summed_numbers = 0
        counter = 0
        with open(input_file, "r") as f:
            for line in f.readlines():
                summed_numbers += float(line)
                counter += 1

        return summed_numbers / counter

    def average_inputs(self, input_file_list):
        summed_averages = 0
        counter = 0
        for input_file in input_file_list:
            summed_averages += self.average_file(input_file)
            counter += 1

        return summed_averages / counter

    def run(self):
        print(self.get_input_file_names_from_dict("a"))

        average_a = self.average_inputs(self.get_input_file_names_from_dict("a", "random_numbers.txt"))
        average_b = self.average_inputs(self.get_input_file_names_from_dict("b")["random_numbers.txt"])

        with open(self.get_output_file_name("combined_average.txt"), "w") as out_file:
            out_file.write(f"{(average_a + average_b)/2}\n")

    def output(self):
        yield self.add_to_output("combined_average.txt")


if __name__ == "__main__":
    luigi.set_setting("result_path", "results")
    luigi.process(TaskB(), workers=4)
