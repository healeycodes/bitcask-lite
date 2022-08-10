import subprocess
import sys
import requests

proc = subprocess.Popen(
    ["go", "run", "."], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
)

addr = "http://localhost:8000"
if len(sys.argv) > 1:
    addr = sys.argv[1]

try:
    # set/get
    s = requests.post(f"{addr}/set?key=a", data="b")
    assert s.status_code == 200
    print(s.status_code, s.text)
    g = requests.get(f"{addr}/get?key=a")
    print(g.status_code, g.text)
    assert g.status_code == 200
    assert g.text == "b"

    # get missing
    g = requests.get(f"{addr}/get?key=z")
    print(g.status_code, g.text)
    assert g.status_code == 404

    # set without key
    g = requests.get(f"{addr}/set")
    print(g.status_code, g.text)
    assert g.status_code == 400

    # set with empty key
    g = requests.get(f"{addr}/set?key=")
    print(g.status_code, g.text)
    assert g.status_code == 400

    # set/get empty value
    s = requests.post(f"{addr}/set?key=c")
    print(s.status_code, g.text)
    assert s.status_code == 200
    g = requests.get(f"{addr}/get?key=c")
    print(g.status_code, g.text)
    assert g.status_code == 200
    assert g.text == ""

    # set/del/get
    s = requests.post(f"{addr}/set?key=x", data="y")
    print(s.status_code, g.text)
    assert s.status_code == 200
    d = requests.delete(f"{addr}/delete?key=c")
    print(d.status_code, d.text)
    assert d.status_code == 200
    g = requests.get(f"{addr}/get?key=c")
    print(g.status_code, g.text)
    assert g.status_code == 404

    print("tests pass 🚀")
except Exception as e:
    print("tests fail 🛑")
    raise
finally:
    proc.terminate()
