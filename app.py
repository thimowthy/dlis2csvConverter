import os
from time import sleep
from flask import Flask, jsonify, request, redirect, render_template
import subprocess
import sys

from tratamento_dados_dlis import pipeline_processamento

app = Flask(__name__)

CURVAS_ESCOLHIDAS = ['TDEP', 'GR', 'NPHI', 'RHOB', 'RHOZ', 'DRHO', 'HDRA',
                    'BSZ', 'BS', 'HCAL', 'CAL', 'CALI', 'DCALI', 'DCAL',
                    'PE', 'PEFZ', 'PEU', 'DT', 'DTC', 'ILD', 'RILD', 'IEL',
                    'AIT90', 'AHT90', 'RT90', 'AT90', 'AO90', 'RT', 'AF90',
                    'AHF90', 'AFH90', 'LLD', 'RLLD', 'HDRS', 'HLLD', 'LL7',
                    'RLL7']

@app.route('/upload_data', methods=['POST'])
def upload_data():
    global qualidade_params
    qualidade_params = request.get_json()

    return jsonify({'status': 'sucesso'})


@app.route('/', methods=['GET', 'POST'])
def select_folders():
    if request.method == 'POST':

        input_folder = request.form['input_folder']
        output_folder = request.form['output_folder']
        formato = request.form['output_format']
        
        print(formato)
        print(input_folder)
        print(output_folder)

        if not os.path.isdir(input_folder):
            return redirect(request.url)

        if not os.path.isdir(output_folder):
            return redirect(request.url)
        
        filtros_a_aplicar = request.form.getlist('filtros_a_aplicar')
        parametros = {
            'colunas_nulos': request.form.getlist('curvas_escolhidas_nulos'),
            'min_tamanho_transicao': int(request.form.get('min_tamanho_transicao', 5)),
            'min_tamanho_finos': int(request.form.get('min_tamanho_finos', 10)),
            'subset_constantes': request.form.getlist('curvas_escolhidas_constantes'),
            'qualidade_params': qualidade_params
        }
        print(filtros_a_aplicar)
        print(parametros)

        sleep(2)
        pipeline_processamento(input_folder, output_folder, formato, CURVAS_ESCOLHIDAS, filtros_a_aplicar, parametros)

    
    return render_template('upload.html')


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


if __name__=="__main__":
    app.run(debug=True)