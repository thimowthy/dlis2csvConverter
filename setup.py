import subprocess
import sys

def setup():
    bibliotecas = [
        'Flask>=2.0.0',
        'Werkzeug>=2.0.0',
        'pandas>=1.3.0',
        'numpy>=1.21.0',
        'openpyxl>=3.0.0',
        'python-dotenv>=0.19.0',
        'tqdm>=4.62.0',
        'dlisio>=1.0.0'
    ]
    
    for biblioteca in bibliotecas:
        try:
            nome_biblioteca = biblioteca.split('>=')[0]
            __import__(nome_biblioteca)
            print(f'{nome_biblioteca} já está instalado.')
        except ImportError:
            print(f'{nome_biblioteca} não está instalado. Instalando...')
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', nome_biblioteca])
            except Exception as e:
                print(e)


if __name__=="__main__":
    setup()