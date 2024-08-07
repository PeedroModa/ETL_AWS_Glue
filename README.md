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

![Captura de Tela](image/Captura%20de%20tela%202024-08-07%20103744.png)


# Etapas do Pipeline de ETL

1. **Ingestão dos Dados**
   
Carregamento dos arquivos CSV brutos na pasta source_data do bucket S3.

2. **Configuração do AWS Glue**

Utilizado o AWS Glue para criar um Crawler que rastreia a pasta source_data e catalogue os dados brutos.
Criado um database no AWS Glue para armazenar as tabelas catalogadas.

3. **Job de Transformação**

Job desenvolvido em Python no AWS Glue para:

- Ler os arquivos CSV brutos da pasta source_data.

- Realizar a união dos DataFrames.

- Transformar os dados no formato Parquet.

- Particionar os dados por estado do cliente (customer_state).

- Carregar os arquivos Parquet transformados na pasta datalake.

4. **Particionamento**

O particionamento de dados é relevante para o AWS Athena por algumas razões, dentre elas, destacam-se duas principais: redução de custo e melhoria de performance nas consultas.

Redução de custos: A cobrança do Athena é realizada de acordo com a quantidade de dados gerados por consulta. Com o particionamento, asseguramos que apenas as partições relevantes são lidas, reduzindo a quantidade de dados processados e custo.

Melhora na performance das consultas: Assegura que apenas as partições necessárias para uma consulta específica, acelerando o tempo de resposta das consultas.

5. **Catalogação Final**

Executado um novo Crawler para catalogar os arquivos Parquet na pasta datalake.
  
6. **Consultas com AWS Athena**

Utilizado o AWS Athena para executar consultas SQL diretamente nos arquivos Parquet armazenados na pasta datalake.
