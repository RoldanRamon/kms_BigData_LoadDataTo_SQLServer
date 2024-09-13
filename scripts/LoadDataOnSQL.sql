BULK INSERT [mkt_sales].[dbo].[FATO_FATURAMENTO]
FROM 'C:\Users\rala\Downloads\idft0006\data\arquivo_modificado.csv'
WITH (
    FIELDTERMINATOR = ';',   -- Define o delimitador de campo
    ROWTERMINATOR = '\n',    -- Define o terminador de linha
    FIRSTROW = 2,            -- Ignora a primeira linha (cabeçalho)
    TABLOCK                 -- Otimiza a carga em massa
);




