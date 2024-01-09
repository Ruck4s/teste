import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

# Configuração da conexão com o PostgreSQL
pg_connection = f'postgresql://<user>:<password>@<host>/<name_database>'
engine = create_engine(pg_connection)

# Configuração do BigQuery
project_id = '<PROJECT_ID>'
dataset_id = '<DATASET_ID'
table_id = '<TABLE_ID'
table_full_id = f"{project_id}.{dataset_id}.{table_id}"

# Query SQL para selecionar dados do PostgreSQL
sql_query = 'SELECT * FROM <TABLE_NAME>'

# Executando a query e obtendo os resultados em um DataFrame pandas
df = pd.read_sql_query(sql_query, con=engine)

# Adicionando uma nova coluna de data como referência
df['stg_processed'] = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")

# Carregando dados no BigQuery usando to_gbq (replace para upsert)
df.to_gbq(destination_table=table_full_id, project_id=project_id, if_exists='replace')
