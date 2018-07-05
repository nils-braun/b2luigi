Simple Basf2 Task
=================

.. code-block:: python

    import b2luigi
    import time
    import basf2

    class MyTask(b2luigi.Basf2Task):
        number = b2luigi.IntParameter()

        def create_path(self):
            path = basf2.create_path()
            path.add_module("EventInfoSetter", evtNumList=[self.number])
            path.add_module("RootOutput", outputFileName=self.get_output_file_names()["output.root"])

            return path

        def output(self):
            yield self.add_to_output("output.root")


    class MainTask(b2luigi.Basf2FileMergeTask):
        def requires(self):
            for i in range(1, 10):
                yield MyTask(number=i)

    if __name__ == "__main__":
        b2luigi.process(MainTask(), workers=5)

