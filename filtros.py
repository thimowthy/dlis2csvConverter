import pandas as pd

# Funções de Filtro
def filtro_nulos(df, colunas=None):
    if colunas is None:
        colunas = ['GR', 'RESD', 'DT', 'RHOB', 'DRHO', 'NPHI', 'PE']
    return df.dropna(subset=colunas)


def filtrar_blocos_finos(df, min_tamanho):
    df_copia = df.copy()
    indices_remover = []
    
    for poço, df_poco in df_copia.groupby('Poço'):
        df_poco["Tamanho"] = 0
        litologia = df_poco.iloc[0]["Litologia"]
        indexes = []
        cumsum = 0
        
        for linha in df_poco.itertuples(index=False):
            profundidade = linha.TDEP
            
            if litologia == linha.Litologia:
                indexes.append(profundidade)
                cumsum += 1
            else:
                topo, base = indexes[0], indexes[-1]
                mascara = (df_copia["Poço"] == poço) & (df_copia["TDEP"].between(topo, base))
                df_copia.loc[mascara, "Tamanho"] = cumsum
                
                litologia = linha.Litologia
                indexes = [profundidade]
                cumsum = 1
        
        if indexes:
            topo, base = indexes[0], indexes[-1]
            mascara = (df_copia["Poço"] == poço) & (df_copia["TDEP"].between(topo, base))
            df_copia.loc[mascara, "Tamanho"] = cumsum
        
        indices_remover.extend(df_copia[(df_copia["Poço"] == poço) & (df_copia["Tamanho"] <= min_tamanho)].index)
    
    df_copia.drop(indices_remover, inplace=True)
    del df_copia["Tamanho"]
    
    return df_copia


def filtrar_transicao(df, n):
    df_copia = df.copy()
    indices_topo = set(df_copia[df_copia["Topo"] == True].index)
    indices_base = set(df_copia[df_copia["Base"] == True].index)
    
    indices_para_remover = indices_topo.union(indices_base)
    
    for i in indices_para_remover.copy():
        for j in range(1, n+1):
            if i-j >= 0:
                indices_para_remover.add(i - j)
            if i+j < len(df):
                indices_para_remover.add(i + j)
    
    df_copia.drop(indices_para_remover, inplace=True)
    
    return df_copia


def filtrar_coluna(df, coluna, min=None, max=None):
    indices_para_remover = []
    for index, linha in df.iterrows():
        if (min is not None and linha[coluna] <= min) or (max is not None and linha[coluna] >= max):
            indices_para_remover.append(index)
    df.drop(index=indices_para_remover, inplace=True)
    return df


def filtrar_curvas(df, filtros_curvas):
    """
    Filtra as curvas do DataFrame com base nos limites fornecidos para cada curva.

    :param df: DataFrame contendo os dados.
    :param filtros_curvas: Dicionário onde as chaves são nomes das curvas e os valores são tuplas com os limites permitidos.
    :return: DataFrame filtrado com base nos limites das curvas.
    """
    mascara_final = pd.Series([True] * len(df))  # Inicializa uma máscara final com True para todas as linhas
    print(filtros_curvas)
    for curva, limites in filtros_curvas.items():
        if any(v is None for v in limites.values()):
            continue  # Se os limites forem None, ignore essa curva
        
        limite_inferior, limite_superior = limites['min'], limites['max'],
        mascara_curva = df[curva].between(limite_inferior, limite_superior) | df[curva].isnull()
        mascara_final &= mascara_curva  # Combina a máscara da curva com a máscara final
    
    return df[mascara_final]



def filtrar_constantes(df, subset):
    def encontrar_sequencias_constantes(df, subset):
        a_remover = set()
        for col in subset:
            inicio_sequencia = None
            for i in range(1, len(df)):
                if df[col].iloc[i] == df[col].iloc[i-1]:
                    if inicio_sequencia is None:
                        inicio_sequencia = i-1
                    a_remover.add(df[col].iloc[inicio_sequencia])
                    a_remover.add(df[col].iloc[i])
                else:
                    inicio_sequencia = None
        return a_remover
    
    valores_constantes = encontrar_sequencias_constantes(df, subset)
    
    for col in subset:
        df = df[~df[col].isin(valores_constantes)]
    return df.reset_index(drop=True)


# Função principal do pipeline
def pipeline_filtros(df, filtros, parametros: dict):
    if 'nulos' in filtros:
        df = filtro_nulos(df, colunas=parametros.get('colunas_nulos'))
        print(f"Filtro de nulos aplicado. Linhas restantes: {df.shape[0]}")

    if 'curvas' in filtros:
        params = parametros.get('qualidade_params')
        df = filtrar_curvas(df, params)
        print(f"Filtro de qualidade aplicado. Linhas restantes: {df.shape[0]}")

    if 'transicao' in filtros:
        tamanho = parametros.get('min_tamanho_transicao', 5)
        df = filtrar_transicao(df, tamanho)
        print(f"Filtro de transição aplicado. Linhas restantes: {df.shape[0]}")

    if 'finos' in filtros:
        min_tamanho = parametros.get('min_tamanho_finos', 10)
        df = filtrar_blocos_finos(df, min_tamanho)
        print(f"Filtro de trechos finos aplicado. Linhas restantes: {df.shape[0]}")

    if 'constantes' in filtros:
        subset = parametros.get('subset_constantes', ['GR', 'RESD', 'DT', 'RHOB', 'DRHO', 'NPHI', 'PE'])
        df = filtrar_constantes(df, subset)
        print(f"Filtro de constantes aplicado. Linhas restantes: {df.shape[0]}")

    return df