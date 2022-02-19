from dagster import op, Nothing, String
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import snowflake.connector as sfc
import pandas as pd


@op(
    name="snowflake_query_to_pandas",
    description="Save Snowflake query as a pandas dataframe",
    config_schema={
        'sf_account': String,
        'sf_warehouse': String,
        'sf_database': String,
        'sf_schema': String,
        'sf_role': String,
        'sf_authenticator': String,
        'sf_username': String,
        'sf_password': String,
        'table_name': String
    }
)
def snowflake_query_to_pandas(context) -> pd.DataFrame:
    with sfc.connect(
        user=context.op_config['sf_username'],
        password=context.op_config['sf_password'],
        account=context.op_config['sf_account'],
        authenticator=context.op_config['sf_authenticator'],
        database=context.op_config['sf_database'],
        schema=context.op_config['sf_schema'],
        warehouse=context.op_config['sf_warehouse'],
        role=context.op_config['sf_role'],
    ) as con:
        schema = context.op_config['sf_schema']
        table_name = context.op_config['table_name']

        # You will be able to send at most 1000 rows
        sql = f"select * from {schema}.{table_name} limit 1000"
        df = pd.read_sql(sql, con)

    return df


@op(
    name="pandas_to_snowflake",
    description="Load pandas dataframe as Snowflake table",
    config_schema={
        'sf_account': String,
        'sf_warehouse': String,
        'sf_database': String,
        'sf_schema': String,
        'sf_role': String,
        'sf_authenticator': String,
        'sf_username': String,
        'sf_password': String,
        'table_name': String,
        'if_exists': String
    }
)
def pandas_to_snowflake(context, df: pd.DataFrame) -> Nothing:
    engine = create_engine(
        URL(
            user=context.op_config['sf_username'],
            password=context.op_config['sf_password'],
            account=context.op_config['sf_account'],
            authenticator=context.op_config['sf_authenticator'],
            database=context.op_config['sf_database'],
            schema=context.op_config['sf_schema'],
            warehouse=context.op_config['sf_warehouse'],
            role=context.op_config['sf_role'],
        )
    )

    table_name = context.op.config['table_name']
    with engine.connect() as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            index=False,
            method=pd_writer,
            if_exists=context.op_config['if_exists']
        )
