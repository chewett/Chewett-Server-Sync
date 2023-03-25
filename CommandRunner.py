import subprocess


def run_command(command, cwd=None):
    print("")
    print("=============================================")
    print("$ " + command)
    subprocess.call(command, shell=True, cwd=cwd)
    print("")


