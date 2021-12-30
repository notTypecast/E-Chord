import json
import socket
from sys import argv
import os
import sys
sys.path.append(".")


with open(argv[1]) as f:
    data = json.load(f)

os.chdir("..")
from src import utils

if argv[2] in ("insert", "i"):
    req = "find_and_store_key"
    req_body = lambda e: {"key": e, "value": data[e]}
    print("Inserting keys...")
elif argv[2] in ("lookup", "l"):
    req = "find_key"
    req_body = lambda e: {"key": e}
    print("Looking up keys...")
else:
    print("Expected request type (i, l) as second argument.")
    exit(1)


failed_req = 0
total_req = 0
for event in data:
    response = utils.ask_peer(("", 9150), req, req_body(event))

    if not response or response["header"]["status"] not in (200, 300):
        failed_req += 1
    total_req += 1

    print(f"\rFail percentage: {failed_req*100/total_req}%" + 20*" ", end="")

















