from python:onbuild
run mkdir -p /extracts/zkillboard_rdp
cmd ["python", "app"]