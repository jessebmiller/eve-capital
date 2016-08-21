from python:onbuild
run python -m unittest
cmd ["python", "app"]