import subprocess
import sys

if __name__=="__main__":
    subprocess.check_call([sys.executable, 'setup.py'])
    subprocess.check_call([sys.executable, 'app.py'])
