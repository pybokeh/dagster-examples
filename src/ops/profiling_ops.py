from dagster import job, make_values_resource, op, repository, schedule, Nothing, String
from pathlib import Path
import configparser
import numpy as np
import pandas as pd
import sidetable
import snowflake.connector as sfc
import win32com.client as win32

# Documentation on configuration multiple ops with the same configuration value
# https://docs.dagster.io/concepts/configuration/config-schema#passing-configuration-to-multiple-ops-in-a-job
# In this case, we need to pass the same table name to multiple ops

# This dagster job will execute 3 tasks:
# - Return the results of a Snowflake query as a pandas dataframe
# - Create percent missing report using sidetable library
# - Email the % missing report to the designated recipient


@op(
    name="get_pandas_df_from_snowflake",
    description="Query a Snowflake table and save results as a pandas dataframe",
    required_resource_keys={"globals"},
)
def get_pandas_df_from_snowflake(context) -> pd.DataFrame:
    table_name = context.resources.globals["table_name"]

    config = configparser.ConfigParser()
    config.read(Path.home() / '.config' / 'config.ini')
    SF_USERNAME = config['snowflake']['USERNAME']
    SF_PASSWORD = config['snowflake']['PASSWORD']
    SF_ACCOUNT = config['snowflake']['ACCOUNT']
    SF_AUTHENTICATOR = config['snowflake']['AUTHENTICATOR']

    context.log.info(f"Fetching table: {table_name}")

    with sfc.connect(
            user=SF_USERNAME,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            authenticator=SF_AUTHENTICATOR,
            database='some_database',
            schema='some_schema',
            warehouse='some_warehouse',
            role='some_role',
    ) as conn:
        sql = f"""
        SELECT
            *
        FROM
            some_schema.{table_name}
        """
        df = pd.read_sql(sql, conn)

    context.log.info(f"Pandas DataFrame has {df.shape[0]} rows and {df.shape[1]} columns")

    return df


@op(
    name="perc_missing_report",
    description="Creates % missing report using sidetable library",
    required_resource_keys={"globals"},
)
def create_perc_missing_report(context, df: pd.DataFrame) -> String:

    context.log.info(f"Creating % missing report for {context.resources.globals['table_name']}")

    df_formatted = df.replace(r'^\s*$', np.nan, regex=True)

    context.log.info(f"Report output: \n{df_formatted.stb.missing().to_html()}")

    return df_formatted.stb.missing().to_html()


@op(
    name="email_perc_missing_report",
    description="Emails the % Missing Report to the designated recipient",
    config_schema={
        'email_recipient': String
    },
    required_resource_keys={"globals"},
)
def email_perc_missing_report(context, report: String) -> Nothing:
    to_email = context.op_config['email_recipient']

    context.log.info(f"Will send profile report for table: {context.resources.globals['table_name']}")
    context.log.info(f"Emailing the report to: {to_email}")

    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = to_email
    mail.Subject = f"Percent Missing Report for Table: {context.resources.globals['table_name']}"
    mail.HTMLBody = f"{report}<br><br>Do NOT reply - this email was automatically sent via 🐍 Script"
    mail.Send()


@job(
    resource_defs={
        "globals": make_values_resource(table_name=String),
    },
    description="A job that automatically emails a percent missing report of a Snowflake table"
)
def profile_sf_table():
    df = get_pandas_df_from_snowflake()
    report = create_perc_missing_report(df)
    email_perc_missing_report(report)


@schedule(
    cron_schedule="*/1 * * * *",
    job=profile_sf_table,
    execution_timezone="US/Eastern",
)
def profile_basic_data_every_1_minute(context):
    return {
        "resources": {
            "globals": {
                "config": {
                    "table_name": "d1_basic_data"
                }
            }
        }
    }


@repository
def profile_d1_data():
    return [profile_basic_data_every_1_minute]
