import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

# Configuração do cliente S3
s3_client = boto3.client('s3')

# Função para ler um arquivo CSV do S3 para um DataFrame do pandas
def read_csv_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(response['Body'])

# Função para salvar um DataFrame do pandas em arquivos Parquet particionados por customer_state no S3
def save_parquet_partitions_to_s3(df, bucket, prefix):
    # Cria um buffer para o arquivo Parquet
    parquet_buffer = io.BytesIO()
    
    # Cria uma tabela PyArrow a partir do DataFrame pandas
    table = pa.Table.from_pandas(df)
    
    # Itera sobre cada grupo de customer_state
    for state, group in df.groupby('customer_state'):
        state_table = pa.Table.from_pandas(group)
        
        # Cria um buffer para cada partição
        partition_buffer = io.BytesIO()
        pq.write_table(state_table, partition_buffer)
        
        # Salva a partição no S3
        partition_key = f"{prefix}/customer_state={state}/resultado_join.parquet"
        s3_client.put_object(Bucket=bucket, Key=partition_key, Body=partition_buffer.getvalue())

bucket_name = 'datalake-new-project'

# Lista de arquivos CSV a serem lidos
csv_files = [
    'source_data/olist_customers_dataset.csv',
    'source_data/olist_orders_dataset.csv',
    'source_data/olist_order_items_dataset.csv',
    'source_data/olist_order_payments_dataset.csv',
    'source_data/olist_order_reviews_dataset.csv',
    'source_data/olist_products_dataset.csv',
    'source_data/olist_sellers_dataset.csv',
    'source_data/product_category_name_translation.csv'
]

# Leitura dos arquivos CSV para DataFrames
dfs = {key.split('/')[-1].replace('.csv', ''): read_csv_from_s3(bucket_name, key) for key in csv_files}

# Verificar as colunas de cada DataFrame carregado
for nome_variavel, df in dfs.items():
    print(f"{nome_variavel}:")
    print(df.columns)
    print()

# Renomear colunas para evitar duplicatas
dfs['olist_orders_dataset'] = dfs['olist_orders_dataset'].rename(columns={'customer_id': 'customer_id_orders'})
dfs['olist_order_items_dataset'] = dfs['olist_order_items_dataset'].rename(columns={'order_id': 'order_id_items', 'product_id': 'product_id_items'})
dfs['olist_order_payments_dataset'] = dfs['olist_order_payments_dataset'].rename(columns={'order_id': 'order_id_payments'})
dfs['olist_order_reviews_dataset'] = dfs['olist_order_reviews_dataset'].rename(columns={'order_id': 'order_id_reviews'})
dfs['olist_products_dataset'] = dfs['olist_products_dataset'].rename(columns={'product_id': 'product_id_products'})
dfs['olist_sellers_dataset'] = dfs['olist_sellers_dataset'].rename(columns={'seller_id': 'seller_id_sellers'})
dfs['product_category_name_translation'] = dfs['product_category_name_translation'].rename(columns={'product_category_name': 'product_category_name_translation'})

# Unir os DataFrames
df1 = dfs['olist_customers_dataset']
df2 = dfs['olist_orders_dataset']
df3 = dfs['olist_order_items_dataset']
df4 = dfs['olist_order_payments_dataset']
df5 = dfs['olist_order_reviews_dataset']
df6 = dfs['olist_products_dataset']
df7 = dfs['olist_sellers_dataset']
df8 = dfs['product_category_name_translation']

# Realizando os joins
resultado_join1 = pd.merge(df1, df2, left_on='customer_id', right_on='customer_id_orders', how='inner')
resultado_join2 = pd.merge(resultado_join1, df3, left_on='order_id', right_on='order_id_items', how='inner')
resultado_join3 = pd.merge(resultado_join2, df4, left_on='order_id', right_on='order_id_payments', how='inner')
resultado_join4 = pd.merge(resultado_join3, df5, left_on='order_id', right_on='order_id_reviews', how='inner')
resultado_join5 = pd.merge(resultado_join4, df6, left_on='product_id_items', right_on='product_id_products', how='inner')
resultado_join6 = pd.merge(resultado_join5, df7, left_on='seller_id', right_on='seller_id_sellers', how='inner')
resultado_join7 = pd.merge(resultado_join6, df8, left_on='product_category_name', right_on='product_category_name_translation', how='inner')

# Adiciona a coluna 'customer_state' para particionamento (ajuste conforme necessário)
resultado_join7['customer_state'] = resultado_join7['customer_state'].astype(str)  # Assegura que 'customer_state' é uma string

# Salvando o resultado no S3 em formato Parquet particionado por customer_state
save_parquet_partitions_to_s3(resultado_join7, bucket_name, 'datalake')

print("Job concluído com sucesso!")
