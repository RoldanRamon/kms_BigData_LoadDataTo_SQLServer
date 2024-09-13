rm(list = ls())
library(sparklyr)
library(dplyr)
library(dbplyr)
library(DBI)
library(stringr)
library(odbc)


# Conectar ao Spark
sc <- spark_connect(
  master = "local",
  config = spark_config(),
  jars = "C:/mssql-jdbc-12.8.1.jre8.jar"  # Verifique se o caminho está correto
)

# Lendo o dataset
df <- read.csv2(file = 'rala-idft0006.csv', sep = ';', encoding = 'latin1', quote = "") |>
  select(-c(X,Dados.Adicionais.NF)) |> 
  mutate(across(everything(), ~ str_remove_all(., "'"))) |> janitor::clean_names()

teste <- head(df)

# Copiar o data.frame 'df' para o Spark
df_spark <- copy_to(sc, teste, "my_table", overwrite = TRUE)


# Preparar os nomes das colunas para inserção
NomeDasColunas <- c(
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

# Conectando ao banco de dados SQL Server
con <- dbConnect(odbc::odbc(), 
                 driver = "ODBC Driver 17 for SQL Server",
                 server = "10.105.40.196\\powerbi",
                 database = "mkt_sales",
                 UID = Sys.getenv("SQL_UID"),
                 PWD = Sys.getenv("SQL_PWD"))

# Criar a tabela temporária para inserção em blocos no SQL Server
df_spark %>% 
  sdf_register("temp_data") %>%  # Registrar a tabela temporária
  spark_write_jdbc(
    name = "FATO_FATURAMENTO", 
    mode = "append", 
    options = list(
      url = "jdbc:sqlserver://10.105.40.196\\powerbi;databaseName=mkt_sales",
      dbtable = "FATO_FATURAMENTO",
      user = Sys.getenv("SQL_UID"),
      password = Sys.getenv("SQL_PWD"),
      driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"  # Forçar o driver JDBC
    )
  )


# Fechar conexão ao banco e ao Spark
dbDisconnect(con)
spark_disconnect(sc)
