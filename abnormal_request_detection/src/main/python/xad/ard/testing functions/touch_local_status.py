import subprocess
import os

def _touch_local_status(args):
    dir = 'ard'+'/' + args
    cmd = 'mkdir -p '
    cmd = cmd + dir
    p = subprocess.Popen(cmd, shell = True)
    

_touch_local_status("us/2017/12/exchange")
    
