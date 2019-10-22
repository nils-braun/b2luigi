import b2luigi
import os


class MyTask(b2luigi.DispatchableTask):
    env = {"MY_SECOND_SECRET_VARIABLE": "47"}

    def process(self):
        print("MY_SECRET_VARIABLE", os.getenv("MY_SECRET_VARIABLE"))
        print("MY_SECOND_SECRET_VARIABLE", os.getenv("MY_SECOND_SECRET_VARIABLE"))


if __name__ == "__main__":
    # We cheat a bit and create the setup script here:
    with open("setup_script.sh", "w") as f:
        f.write("""echo "Setup Script called"\nexport MY_SECRET_VARIABLE=42""")

    b2luigi.set_setting("env_script", os.path.abspath("setup_script.sh"))
    b2luigi.process(MyTask())
