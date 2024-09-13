import pandas as pd

# Carregar o CSV
df = pd.read_csv("rala-idft0006.csv", sep=";", encoding='iso-8859-1')
df = df.drop('Dados Adicionais NF', axis=1)


# Ajustar para exibir todas as colunas
#pd.set_option('display.max_columns', None)
#print(df.dtypes)

# Converter df.dtypes para DataFrame e salvar em Excel
#df_types = df.dtypes.reset_index()  # Converte para DataFrame
#df_types.columns = ['Column', 'Data Type']  # Renomear colunas
#df_types.to_excel("testes_tipos.xlsx", index=False)  # Salvar em Excel

# Função para gerar o SQL de INSERT INTO com as colunas parametrizadas
def gerar_insert_sql_parametrizado(df, tabela_nome, colunas_normalizadas):
    insert_statements = []
    for index, row in df.iterrows():
        columns = ', '.join(colunas_normalizadas)
        values = ', '.join(f"'{v}'" if isinstance(v, str) else str(v) for v in row.values)
        sql = f"INSERT INTO {tabela_nome} ({columns}) VALUES ({values});"
        insert_statements.append(sql)
    return insert_statements

# Atualizando a lista de colunas normalizadas com todas as colunas do CREATE TABLE fornecido
colunas_normalizadas_completas = [
    'EST', 'SERIE', 'NR_NOTA', 'EMITENTE', 'NOME', 'EMISSAO', 'PEDIDO', 'USUAR_IMPLANT', 'TIPO', 'OBSOLETO',
    'ITEM', 'DESCRICAO', 'FAMILIA_COMERCIAL', 'CLASS_FIS', 'QUANT', 'BASE_ICMS', 'PERCENT_ICM', 'VL_ICM',
    'BASE_IPI', 'PERCENT_IPI', 'VL_IPI', 'BASE_CALC_PIS', 'PERCENT_PIS', 'VL_PIS', 'BASE_CALC_COFINS',
    'PERCENT_COF', 'VL_COF', 'VL_SUBS', 'VALOR_TOTAL', 'NAT_OPER', 'C_VENDA', 'TABELA_DE_PRECOS', 'PERCENT_DESCONTO',
    'VALOR_DESCONTO', 'VALOR_LIQUIDO', 'MOV_ESTOQ', 'PRECO_MEDIO', 'PRECO_TOTAL', 'PERCENTUAL', 'CFOP',
    'COMP_FAB', 'TIPO_ITEM', 'UF', 'UN', 'TIPO_OPER', 'DATA_IMPLANTACAO', 'CST', 'DT_ENTREGA',
    'ICMS_PARTILHA_ORIGEM', 'ICMS_PARTILHA_DESTINO', 'TIPO_DO_ITEM', 'TIPO_CONTROLE_DO_ITEM', 'DESCRICAO_FAMILIA_COMERCIAL',
    'GRUPO_DE_ESTOQUE', 'ORIGEM', 'NRO_FCI', 'DT_AFERICAO_FCI', 'ORDEM_PRODUCAO', 'CHAVE_DE_ACESSO_NFE',
    'CENTRO_DE_LUCRO', 'SEQ_PEDIDO', 'PARC_ORDEM', 'PRIORIDADE', 'PRECO_ULT_ENTRADA', 'TOTAL_PRECO_ULT_ENTRADA',
    'ITEM_DO_CLIENTE', 'REPRESENTANTE', 'CNPJ_DO_CLIENTE'
]

# Gerando os comandos SQL com as colunas normalizadas atualizadas
insert_statements_parametrizado_completo = gerar_insert_sql_parametrizado(df, '[mkt_sales].[dbo].[FATO_FATURAMENTO]', colunas_normalizadas_completas)

# Salvando as declarações INSERT INTO em um arquivo .txt
output_file_parametrizado_completo = 'insert_statements_parametrizado_completo.txt'

with open(output_file_parametrizado_completo, 'w') as f:
    for statement in insert_statements_parametrizado_completo:
        f.write(statement + '\n')

# Confirmando o caminho onde o arquivo foi salvo
output_file_parametrizado_completo


