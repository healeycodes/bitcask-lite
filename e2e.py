import subprocess
import requests

proc = subprocess.Popen(
    ["go", "run", "."], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
)

try:
    # set/get
    s = requests.post("http://localhost:8000/set?key=a", data="b")
    assert s.status_code == 200
    print(s.status_code, s.text)
    g = requests.get("http://localhost:8000/get?key=a")
    print(g.status_code, g.text)
    assert g.status_code == 200
    assert g.text == "b"

    # get missing
    g = requests.get("http://localhost:8000/get?key=z")
    print(g.status_code, g.text)
    assert g.status_code == 404

    # set without key
    g = requests.get("http://localhost:8000/set")
    print(g.status_code, g.text)
    assert g.status_code == 400

    # set with empty key
    g = requests.get("http://localhost:8000/set?key=")
    print(g.status_code, g.text)
    assert g.status_code == 400

    # set/get empty value
    s = requests.post("http://localhost:8000/set?key=c")
    print(s.status_code, g.text)
    assert s.status_code == 200
    g = requests.get("http://localhost:8000/get?key=c")
    print(g.status_code, g.text)
    assert g.status_code == 200
    assert g.text == ""

    print("tests pass 🚀")
except Exception as e:
    print(e)
    quit(1)
finally:
    proc.terminate()
