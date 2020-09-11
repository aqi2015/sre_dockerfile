"""
@File    : alluxio_worker.py
@Time    : 2020/8/15 6:30 下午
@Author  : Akiqi
@Email   : linqi03@beyondsoft.com
@Software: PyCharm
"""


import subprocess
import sys

def cmd(command):
    subp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    # subp.wait(2)
    subp.wait()
    return subp.communicate()
    if subp.poll() == 0:
        # print(subp.communicate()[1])
        return subp.communicate()[1]
    else:
        # print("failed")
        return False

def main():
    args = sys.argv[1]
    print(args)
    command = "/alluxio/bin/alluxio fs %s" % args

    print(cmd(command))

if __name__ == '__main__':
    main()
