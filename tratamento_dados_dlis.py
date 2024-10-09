from pre_processamento import *
from dlisio import dlis
import glob
import csv
import os 
import pandas as pd
from tqdm import tqdm
from filtros import pipeline_filtros


def carregar_dados(path):
    print("Carregando dados dos arquivos .dlis...")
    nomes_arquivos = []
    leituras_dlis = []
    nomes_anp = []
    
    for file in tqdm(glob.glob(path + "/*.dlis", recursive=True), desc="Processando arquivos .dlis"):
        try:
            nomes_arquivos.append(file)

            leitura, *_ = dlis.load(f'{file}')
            leituras_dlis.append(leitura)

            nome = leitura.origins[0].well_name
            nomes_anp.append(nome)
            os.remove(file)

        except:
            print(f"Erro ao processar o arquivo {file}")
    return dict(zip(nomes_anp, leituras_dlis))


def extrair_curvas(dli_dict):
    print("Extraindo curvas dos poços...")
    channels_dict = {}
    
    for key, poco in tqdm(dli_dict.items(), desc="Extraindo curvas"):
        channels_list = []
        for frame in poco.frames:
            channels = frame.channels
            channels_list.append([channel.name for channel in channels])

        channels_dict[key] = sum(channels_list, [])
    
    return channels_dict


def exportar_curvas_csv(channels_dict, output_dir="Curvas_CSV"):
    print("Exportando curvas para arquivos CSV...")
    for key, channels in tqdm(channels_dict.items(), desc="Exportando curvas para CSV"):
        filename = f'{output_dir}/{key}.csv'
        
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Poco', 'Channel'])
            
            for channel in channels:
                writer.writerow([key, channel])

    print("Arquivos CSV das curvas criados com sucesso!")


def processar_frames(dli_dict):
    print("Criando dicionário de frames...")
    frames_dict = {}
    
    for key, poco in tqdm(dli_dict.items(), desc="Processando frames"):
        frames_dict[key] = cria_frames_dict(poco)
    
    return frames_dict


def criar_dataframes(frames_dict, curvas_escolhidas):
    print("Criando dataframes a partir dos frames...")
    dataframes_dict = {}
    
    for key, poco_frames_dict in tqdm(frames_dict.items(), desc="Criando dataframes"):
        dataframes_dict[key] = cria_dataframes_dict(poco_frames_dict, curvas_escolhidas)
    
    return dataframes_dict


def ajustar_profundidade(dataframes_dict, tolerancia=0.01):
    print("Ajustando profundidade (TDEP)...")
    # # Função para checar a profundidade
    # def checa_TDEP(tdep_primeiro_frame, tdep_outro_frame, tolerancia):
    #     tdep_primeiro_frame_set = set(np.round(tdep_primeiro_frame, decimals=1))
    #     tdep_outro_frame_set = set(np.round(tdep_outro_frame, decimals=1))
        
    #     # Checa se todos os valores de tdep_primeiro_frame estão em tdep_outro_frame
    #     for valor_tdep in tdep_primeiro_frame_set:
    #         if not any(np.isclose(valor_tdep, outro_valor, atol=tolerancia) for outro_valor in tdep_outro_frame_set):
    #             print(f'{valor_tdep} - Faltando')

    for _, poco_dataframes_dict in tqdm(dataframes_dict.items(), desc="Ajustando profundidade"):
        if len(poco_dataframes_dict) > 1:
            tdep_primeiro_frame = poco_dataframes_dict[0]['TDEP']
            
            # Ajusta todos os dataframes para ter apenas os valores de TDEP do primeiro dataframe
            for key, value in poco_dataframes_dict.items():
                if key != 0:
                    tdep_outro_frame = value['TDEP']
                    #checa_TDEP(tdep_primeiro_frame, tdep_outro_frame, tolerancia)
                    poco_dataframes_dict[key] = value[value['TDEP'].isin(poco_dataframes_dict[0]['TDEP'])]


def unificar_dataframes(dataframes_dict):
    print("Unificando dataframes por poço...")
    for key, value in tqdm(dataframes_dict.items(), desc="Unificando dataframes"):
        try:
            dataframes_dict[key] = unifica_dataframes(value)
        except:
            print(f"Erro ao unificar dataframes para o poço {key}")


def aplicar_mnemonicos(dataframes_dict):
    print("Aplicando mnemônicos...")
    aplica_mnemonico(dataframes_dict, ['BS', 'BSZ'], 'BS')
    aplica_mnemonico(dataframes_dict, ['LLD', 'LL7', 'RLLD', 'RLL7', 'HDRS', 'HLLD', 'ILD', 'RILD', 'IEL', 'AIT90', 'AHT90', 'RT90', 'AT90', 'AO90', 'RT', 'AF90', 'AHF90', 'AFH90'], 'RESD')
    aplica_mnemonico(dataframes_dict, ['RHOB', 'RHOZ'], 'RHOB')
    aplica_mnemonico(dataframes_dict, ['DTC', 'DT'], 'DT')
    aplica_mnemonico(dataframes_dict, ['HCAL', 'CAL', 'CALI'], 'CAL')
    aplica_mnemonico(dataframes_dict, ['DCAL', 'DCALI'], 'DCAL')
    aplica_mnemonico(dataframes_dict, ['DRHO', 'HDRA'], 'DRHO')
    aplica_mnemonico(dataframes_dict, ['PE', 'PEFZ', 'PEU'], 'PE')

    add_DCAL(dataframes_dict)


def ajustar_unidades_valores(dataframes_dict, dli_dict):
    print("Ajustando unidades e valores nulos...")
    for key, value in tqdm(dataframes_dict.items(), desc="Ajustando unidades e valores nulos"):
        unidade_medida = dli_dict[key].frames[0].attic['SPACING'].units
        
        if unidade_medida == '0.1 in':
            value['TDEP'] = value['TDEP'] / 10 * 0.0254

        value.replace([-999.25], [None], inplace=True)


def remover_colunas_duplicadas(df):
    """
    Remove colunas duplicadas do DataFrame, mantendo a coluna com mais dados não nulos.
    
    Args:
        df (pd.DataFrame): DataFrame com possíveis colunas duplicadas.
        
    Returns:
        pd.DataFrame: DataFrame com colunas duplicadas removidas.
    """
    if df.columns.duplicated().any():
        cols = df.columns
        for col in cols[cols.duplicated()].unique():
            # Encontrar as colunas duplicadas
            duplicated_cols = cols[cols == col]
            
            # Remover a coluna com menos dados não nulos
            non_null_counts = df[duplicated_cols].notnull().sum()
            col_to_remove = non_null_counts.idxmin()
            
            # Remover a coluna com menos dados
            df = df.drop(columns=[col_to_remove])
            print(f"Coluna duplicada removida: {col_to_remove}")

    return df


def ordenar_salvar_dataframes(dataframes_dict, output_dir="output"):
    ordem_desejada = ['TDEP', 'BS', 'CAL', 'DCAL', 'GR', 'RESD', 'DT', 'RHOB', 'DRHO', 'NPHI', 'PE']

    for key, value in dataframes_dict.items():
        try:
            # Remover colunas duplicadas
            value = remover_colunas_duplicadas(value)
            
            # Reindexa as colunas na ordem desejada
            value = value.reindex(columns=ordem_desejada)
            
            # Arredonda os valores de TDEP
            if 'TDEP' in value.columns:
                value['TDEP'] = value['TDEP'].round(1)
            
            # Salva o DataFrame em um arquivo CSV
            file_name = f"{output_dir}/{key}.csv"
            value.to_csv(file_name, index=False)
            print(f"Arquivo {file_name} criado com sucesso.")
        except Exception as e:
            print(f"Erro ao processar o poço {key}: {e}")


def filtrar(filtros, parametros, formato, caminho_csv="output", output_folder="filtered"):
    
    for arquivo in os.listdir(caminho_csv):
        if arquivo.endswith(".csv"):
            caminho_arquivo = os.path.join(caminho_csv, arquivo)
            print(f"Lendo o arquivo {arquivo} para aplicar filtros...")

            df = pd.read_csv(caminho_arquivo)
            os.remove(caminho_arquivo)
            
            df_filtrado = pipeline_filtros(df, filtros, parametros)

            df_filtrado = df_filtrado.sort_values(by="TDEP")
            
            if formato == "csv":
                nome_saida = f"{os.path.splitext(arquivo)[0]}.csv"
                caminho_saida = os.path.join(output_folder, nome_saida)
                df_filtrado.to_csv(caminho_saida, index=False)
            if formato == "xlsx":
                nome_saida = f"{os.path.splitext(arquivo)[0]}.xlsx"
                caminho_saida = os.path.join(output_folder, nome_saida)
                df_filtrado.to_excel(caminho_saida, index=False)
                
            print(f"Arquivo filtrado salvo em: {caminho_saida}")


def pipeline_processamento(path_dlis, output_path, formato, curvas_escolhidas, filtros, parametros):
    print("Iniciando o pipeline de processamento...")
    dli_dict = carregar_dados(path_dlis)
    nome_poco = list(dli_dict.keys())[0]
    channels_dict = extrair_curvas(dli_dict)
    exportar_curvas_csv(channels_dict)
    frames_dict = processar_frames(dli_dict)
    dataframes_dict = criar_dataframes(frames_dict, curvas_escolhidas)
    ajustar_profundidade(dataframes_dict)
    unificar_dataframes(dataframes_dict)
    aplicar_mnemonicos(dataframes_dict)
    ajustar_unidades_valores(dataframes_dict, dli_dict)
    ordenar_salvar_dataframes(dataframes_dict)
    filtrar(filtros, parametros, formato, output_folder=output_path)
    
    return os.path.join("filtered", nome_poco + ".csv")



# # Executando o pipeline com filtros
# if __name__ == "__main__":

#     filtros_a_aplicar = ['nulos', 'curvas', 'constantes'] # ['nulos', 'curvas', 'transicao', 'finos', 'constantes']
#     parametros = {
#         'colunas_nulos': ['GR'],
#         'filtros_curvas': {
#             'DCAL': (-1, 1.5),
#             'DRHO': (-0.15, 0.15)
#         },
#         'min_tamanho_transicao': 5,
#         'min_tamanho_finos': 10,
#         'subset_constantes': ['GR', 'RESD', 'DT', 'RHOB', 'DRHO', 'NPHI', 'PE']
#     }
#     curvas_escolhidas = ['TDEP', 'GR', 'NPHI', 'RHOB', 'RHOZ', 'DRHO', 'HDRA',
#                          'BSZ', 'BS', 'HCAL', 'CAL', 'CALI', 'DCALI', 'DCAL',
#                          'PE', 'PEFZ', 'PEU', 'DT', 'DTC', 'ILD', 'RILD', 'IEL',
#                          'AIT90', 'AHT90', 'RT90', 'AT90', 'AO90', 'RT', 'AF90',
#                          'AHF90', 'AFH90', 'LLD', 'RLLD', 'HDRS', 'HLLD', 'LL7',
#                          'RLL7']
    
#     pipeline_processamento("**/uploads", curvas_escolhidas, filtros_a_aplicar, parametros, "filtered")
