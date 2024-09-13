# Carregar as bibliotecas necessárias
rm(list = ls())
library(data.table)
library(DBI)
library(odbc)

# Carregar o CSV original e modificar
file_path <- "data/rala-idft0006.csv"
dt <- fread(file_path, quote="", encoding = 'Latin-1')

# Excluir a coluna indesejada (exemplo: 'coluna_a_excluir')
dt[, c("V70", "Dados Adicionais NF") := NULL]


# Aplicar mutate para remover o caractere ' de todas as colunas
dt <- dt[, lapply(.SD, function(x) gsub("'", "", x))]

# Salvar o CSV modificado em um local onde o SQL Server tenha acesso
temp_file_path <- "data/arquivo_modificado.csv"
fwrite(dt, temp_file_path, sep = ";", quote = FALSE)

# Conectar ao SQL Server
con <- dbConnect(odbc::odbc(),
                 Driver = "ODBC Driver 17 for SQL Server",  # Verifique o nome do driver
                 Server = "10.105.40.196\\powerbi", 
                 Database = "mkt_sales",
                 UID = Sys.getenv('SQL_UID'), 
                 PWD = Sys.getenv('SQL_PWD')
                 )

# Executar o BULK INSERT no SQL Server usando o arquivo modificado
# Definir o comando sqlcmd
sqlcmd_command <- paste(
   "sqlcmd -S seu_servidor -U seu_usuario -P sua_senha -Q \"",
   "BULK INSERT [mkt_sales].[dbo].[FATO_FATURAMENTO] ",
   "FROM 'C:/Users/rala/Downloads/idft0006/data/arquivo_modificado.csv' ",
   "WITH (FIELDTERMINATOR = ';', ROWTERMINATOR = '\\n', FIRSTROW = 2);\"",
   sep = ""
)

# Executar o comando sqlcmd
system(sqlcmd_command)

# Fechar a conexão
dbDisconnect(con)
