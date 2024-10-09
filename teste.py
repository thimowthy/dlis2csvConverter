from pre_processamento import *

from dlisio import dlis
import pandas as pd
import numpy as np

# A função 'glob' do módulo 'glob' é usada para procurar todos os arquivos em um diretório com determinada extensão
import glob

nomes_arquivos = []     # Armazena os nomes dos arquivos .dlis
leituras_dlis = []      # Armazena as leituras dos arquivos .dlis
nomes_anp = []          # Armazemina os nomes obtidos das leituras

for file in glob.glob(r'**/uploads' + "/*.dlis", recursive=True):
    try:
        # Salva o nome do arquivo
        nomes_arquivos.append(file)

        # Salva os dados da leitura
        leitura, *tail = dlis.load(f'{file}')
        leituras_dlis.append(leitura)

        # Salva o nome do poço
        nome = leitura.origins[0].well_name
        nomes_anp.append(nome)
    except:
        print(nome)


# Casa itens da lista 'nome_anp_abreviados' com os itens da lista 'leituras_dlis'
pares = zip(nomes_anp, leituras_dlis)

# Cria dicionário 'dli_dict'
dli_dict = dict(pares)
dli_dict

channels_dict = {}

for key, poco in dli_dict.items():
    channels_list = []
    for frame in poco.frames:
        channels = frame.channels
        channels_list.append([channel.name for channel in channels])

    channels_dict[key] = sum(channels_list, [])

import csv

# Para cada poço no dicionário channels_dict, criar um arquivo CSV
for key, channels in channels_dict.items():
    # Definir o nome do arquivo CSV usando o identificador do poço
    filename = f'Curvas_CSV/{key}.csv'
    
    # Abrir o arquivo em modo de escrita
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Escrever o cabeçalho do CSV
        writer.writerow(['Poco', 'Channel'])
        
        # Escrever uma linha para cada canal, incluindo o nome do poço
        for channel in channels:
            writer.writerow([key, channel])

print("Arquivos CSV criados com sucesso!")

curvas_escolhidas = ['TDEP', 'GR', 'NPHI', 'RHOB', 'RHOZ', 'DRHO', 'HDRA', 'BSZ', 'BS', 'HCAL', 'CAL', 'CALI', 'DCALI', 'DCAL', 'PE', 'PEFZ', 'PEU', 'DT', 'DTC', 'ILD', 'RILD', 'IEL', 'AIT90', 'AHT90', 'RT90', 'AT90', 'AO90', 'RT', 'AF90', 'AHF90', 'AFH90', 'LLD', 'RLLD', 'HDRS', 'HLLD', 'LL7', 'RLL7']

for key, poco in dli_dict.items():
    print(f'{key}: {len(poco.frames)}')


for key, poco in dli_dict.items():
    unidade_medida = dli_dict[key].frames[0].attic['SPACING'].units

    print(f'{key}:{unidade_medida}')


frames_dict = {}

for key, poco in dli_dict.items():
    # if len(poco.frames) > 1:
    frames_dict[key] = cria_frames_dict(poco)


dataframes_dict = {}

for key, poco_frames_dict in frames_dict.items():
    dataframes_dict[key] = cria_dataframes_dict(poco_frames_dict, curvas_escolhidas)

import math

def checa_TDEP(dataframes, tolerancia=0.01):
    tdep_primeiro_frame = dataframes[0]['TDEP']
  
    for key, value in dataframes.items():
        if key == 0:  # Ignorar o primeiro DataFrame
            continue
        
        tdep_outro_frame = dataframes[key]['TDEP']
        
        for valor_tdep in tdep_primeiro_frame:
            # Verifica se algum valor em tdep_outro_frame está próximo o suficiente de valor_tdep
            if any(math.isclose(valor_tdep, outro_valor, abs_tol=tolerancia) for outro_valor in tdep_outro_frame):
                print(f'{valor_tdep} - Ok')
            else:
                print(f'{valor_tdep} - Faltando')
    

for chave, poco_dataframes_dict in dataframes_dict.items():
    for key, value in poco_dataframes_dict.items():
        try:
            poco_dataframes_dict[key] = value[value['TDEP'].isin(poco_dataframes_dict[0]['TDEP'])]
        except:
            print(chave)



for key, value in dataframes_dict.items():
    try:
        dataframes_dict[key] = unifica_dataframes(value)
    except:
        print(key)


for key, value in dataframes_dict.items():
    if 'RT' in value:
        dataframes_dict[key] = remove_colunas(value, ['AHT90', 'AHF90'])


for key, value in dataframes_dict.items():
    unidade_medida = dli_dict[key].frames[0].attic['SPACING'].units
    
    if unidade_medida == '0.1 in':
        #for value in poco_dataframes_dict.values():
        # move vírgula uma casa para a esquerda
        value['TDEP'] = value['TDEP'] / 10

        # converte de polegada para metro
        value['TDEP'] = value['TDEP'] * 0.0254


for poco in dataframes_dict.values():
    poco.replace([-999.25], [None], inplace = True)



aplica_mnemonico(dataframes_dict, ['BS', 'BSZ'], 'BS')
aplica_mnemonico(dataframes_dict, ['LLD',	'LL7',	'RLLD',	 'RLL7', 'HDRS', 'HLLD', 'ILD',	'RILD',	'IEL',	'AIT90', 'AHT90', 'RT90', 'AT90', 'AO90', 'RT', 'AF90',	'AHF90', 'AFH90'], 'RESD')
aplica_mnemonico(dataframes_dict, ['RHOB', 'RHOZ'], 'RHOB')
aplica_mnemonico(dataframes_dict, ['DTC', 'DT'], 'DT')
aplica_mnemonico(dataframes_dict, ['HCAL', 'CAL', 'CALI'], 'CAL')
aplica_mnemonico(dataframes_dict, ['DCAL', 'DCALI'], 'DCAL')
aplica_mnemonico(dataframes_dict, ['DRHO', 'HDRA'], 'DRHO')
aplica_mnemonico(dataframes_dict, ['PE', 'PEFZ', 'PEU'], 'PE')

add_DCAL(dataframes_dict)


for key, poco in dataframes_dict.items():
    curvas = sorted(poco.keys())
    print(f"{key}: {curvas}")


for key, poco in dataframes_dict.items():
    curvas = sorted(poco.keys())
    print(f"{key}: {curvas}")


for key in dataframes_dict.keys():
    dataframes_dict[key] = dataframes_dict[key].iloc[::-1].reset_index(drop=True)


ordem_desejada = ['TDEP', 'BS', 'CAL', 'DCAL', 'GR', 'RESD', 'DT', 'RHOB', 'DRHO', 'NPHI', 'PE']

for key in dataframes_dict.keys():
    try:
        dataframes_dict[key] = dataframes_dict[key].reindex(columns=ordem_desejada)
    except:
        pass

# Arredonda os valores de TDEP
for key, value in dataframes_dict.items():
    dataframes_dict[key]['TDEP'] = dataframes_dict[key]['TDEP'].round(1)

for key, value in dataframes_dict.items():
    file_name = f"output/curvas_{key}.csv"
    value.to_csv(file_name, index=False)
    print(f"Arquivo {file_name} criado com sucesso.")
