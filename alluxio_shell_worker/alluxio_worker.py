"""
@File    : alluxio_worker.py
@Time    : 2020/7/17 2:30 下午
@Author  : Akiqi
@Email   : linqi03@beyondsoft.com
@Software: PyCharm
"""
import json
import time

from client import *
from os import environ


def action():
    data_response = {
        'status': 0,
        'description': ''
    }
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    dict_args = json.loads(environ.get("INPUT_ARGS"))

    # dict_args = {
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

    # host = '10.211.55.5'
    host = dict_args.get("host")
    # port = 39999
    port = dict_args.get("port")

    action = dict_args.get("action")
    payload = json.loads(dict_args.get("payload"))

    c = Client()
    actions = {
        # "copy_file":,
        # "copy_dir":,
        "create_dir": c.create_dir,
        "create_file": c.create_file,
        "delete": c.delete,
        # "download_file":,
        "exists": c.exists,
        "get_status": c.get_status,
        "list_status": c.list_status,
        "ls": c.ls,
        "rename": c.rename,
        # "set_attribute": c.set_attribute, # 有问题
        # "upload_file":,
        "write_file": c.write_file,
    }

    if action not in actions:
        data_response['status'] = 405
        data_response['description'] = "Invalid input"
        return data_response

    try:
        # print("start connect alluxio server")
        # c.conn(host, port)
        # print("connect alluxio server success")
        r = actions.get(action)(payload)
        print("alluxio_worker 71")
        print(r)
        data_response['status'] = 200
        data_response['description'] = r
        return data_response
    except Exception as e:
        data_response['status'] = 500
        data_response['description'] = e
        return data_response


if __name__ == '__main__':
    print(action())
