from client import *

import alluxio
import json

host = '10.211.55.5'
port = 39999

# argv = {
#     "host": "10.211.55.5",
#     "port": 39999,
#     "requestor": "amp",
#     "worker": "alluxio",
#     "action": "write_file",
#     "task_id": "SAAB-001",
#     "payload": json.dumps({"path": "/test01", "content": "test01 content\n"}),
#     "name": "alluxio01",
#     "admin": "Rohit"
# }

payload = {
    # "path": "/test001",
    # "path": "/test02",
    # "dst": "/test04",
    "path": "/test04",
    # "path": "/create_file",
    # "path": "/create_dir",
    "content": "test04",
    "owner": "aki",
    "group": "aki222",
    "chmod": 777
}
payload = {"path": "/test01", "content": "test01 content\n"}
# conn = alluxio.Client(host, port)
# path = payload.get("path")
# r = conn.exists(path, opt)

# payload = {"path": "/test01", "owner": "aki", "group": "aki222", "chmod": 777}
# payload = {"path": "/test01", "owner": "root", "group": "root", "chmod": 777}
c = Client()
c.conn(host, port)
# r = c.dir_create(payload)
# r = c.path_delete(payload)
# r = c.exists(payload)
# r = c.file_create(payload)
# r = c.write_file(payload)
r = c.write_file(json.loads(json.dumps({"path": "/test01", "content": "test01 content\n"})))
# r = c.get_status(payload)
# r = c.list_status(payload)
# r = c.ls(payload)
# r = c.rename(payload)
# r = c.get_status(payload)
# r = c.set_attribute(payload)
# print(client.exists(payload))
# r = c.set_attribute(payload)
print(r)

