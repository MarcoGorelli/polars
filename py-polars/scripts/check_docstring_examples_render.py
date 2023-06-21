"""
Check that docstring examples will likely render.

Do this:

    Here is an example

    >>> my_func()
    my_output()

Not this:

    Here is an example
    >>> my_func()
    my_output()

"""
import re
import subprocess
import sys

if __name__ == "__main__":
    files = subprocess.run(
        ["git", "ls-files", "polars"], capture_output=True, text=True
    ).stdout.split()
    ret = 0
    for file in files:
        if not file.endswith(".py"):
            continue
        with open(file) as fd:
            content = fd.read()
        matches = re.findall(r"\n\n(        \w+.*)\n        >>>", content)
        if matches:
            lines = content.splitlines()
            for match in matches:
                lineno = lines.index(match) + 1
                print(
                    f"{file}:{lineno}:9: Found docstring example which probably will not render."
                )
                ret = 1
sys.exit(ret)
