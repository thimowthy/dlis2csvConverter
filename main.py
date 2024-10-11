import subprocess
import sys
import webbrowser

if __name__=="__main__":
    subprocess.check_call([sys.executable, 'setup.py'])
    webbrowser.open("http://127.0.0.1:5000/")
    subprocess.check_call([sys.executable, 'app.py'])
