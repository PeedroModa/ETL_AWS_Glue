# ETL AWS Glue

# Visão Geral

Este repositório contém um pipeline de ETL (Extração, Transformação e Carregamento) para processar informações de vendas da Olist (ecossistema líder em soluções para vender online, composto por Olist Store, Vnda e Tiny). O pipeline foi desenvolvido para processar dados armazenados em um bucket S3 utilizando serviços da AWS, como AWS Glue e AWS Athena. O objetivo do projeto é transformar arquivos CSV brutos em arquivos Parquet particionados por estado do cliente e armazená-los em uma pasta de destino dentro do mesmo bucket S3. Por fim, os dados transformados são carregados no AWS Athena para análise.

# Estrutura do Bucket S3

O bucket S3 possui a seguinte estrutura de diretórios:

- **logs/**: Armazena logs gerados durante a execução dos scripts e jobs, úteis para debugging e monitoramento.

- **scripts/**: Contém scripts utilizados no processo de ETL.

- **source_data/**: Pasta onde os arquivos CSV brutos são colocados.

- **datalake/**: Pasta de destino onde os arquivos transformados em formato Parquet são armazenados.

# Arquitetura 

