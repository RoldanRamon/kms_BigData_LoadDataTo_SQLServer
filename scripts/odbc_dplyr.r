# Removendo objetos da sessão
rm(list = ls())
library(dplyr)
library(stringr)
library(odbc)

# Lendo o dataset
df <- read.csv2(file = 'rala-idft0006.csv', sep = ';', encoding = 'latin1', quote = "") |>
  select(-c(X,Dados.Adicionais.NF)) |> 
  mutate(across(everything(), ~ str_remove_all(., "'"))) |> janitor::clean_names()

# NomeDasColunas ----------------------------------------------------------
NomeDasColunas = c(
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
)

# Função para gerar uma query de INSERT para cada linha do dataframe
concat_cols_to_list <- function(df, col_names) {
  # Verifica se o número de colunas no data.frame e na lista de colunas são iguais
  if (ncol(df) != length(col_names)) {
    stop("O número de colunas no dataframe e no vetor de nomes de colunas não corresponde.")
  }
  
  # Concatena os nomes das colunas
  columns <- paste(col_names, collapse = ", ")
  
  # Gera um INSERT para cada linha do dataframe
  query_list <- apply(df, 1, function(row) {
    # Concatena os valores da linha com aspas simples, separadas por vírgulas
    values <- paste0("'", row, "'", collapse = ", ")
    
    # Gera a query completa para cada linha
    query <- paste0('INSERT INTO [mkt_sales].[dbo].[FATO_FATURAMENTO] (', columns, ') VALUES (', values, ');')
    
    return(query)
  })
  
  return(query_list)
}

# Usando a função para concatenar as 10 primeiras colunas em uma lista
result_list <- concat_cols_to_list(df, NomeDasColunas)

# Conectando ao banco de dados SQL Server
con <- dbConnect(odbc::odbc(),driver = "ODBC Driver 17 for SQL Server",
                 server = "10.105.40.196\\powerbi",
                 database = "mkt_sales",
                 UID = Sys.getenv("SQL_UID"),
                 PWD = Sys.getenv("SQL_PWD"))

# Executando cada comando INSERT INTO
for (sql in result_list) {
  dbExecute(con, sql)
}

# Fechando a conexão
dbDisconnect(con)
