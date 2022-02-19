import pandas as pd
from dagster import op, Nothing, String
from pathlib import Path
import win32com.client as win32


@op(
    name="email_without_attachments",
    description="Emails using Outlook account",
    config_schema={
        'email': String,
        'subject': String,
        'template_file_name': String
    }
)
def email_without_attachments(context) -> Nothing:
    # Folder location where email template files are saved (.txt)
    email_template_folder = Path.home() / '.dagster' / 'email_templates'

    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = context.op_config['email']
    mail.Subject = context.op_config['subject']
    template_file = email_template_folder / context.op_config['template_file_name']

    with open(template_file, 'r', encoding="utf8") as f:
        message = f.read()

    mail.HTMLBody = message
    mail.Send()


@op(
    name="email_pandas_df_html",
    description="Email pandas dataframe as html",
    config_schema={
        'email': String,
        'subject': String,
    }
)
def email_pandas_df_html(context, df: String) -> Nothing:
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = context.op_config['email']
    mail.Subject = context.op_config['subject']
    mail.HTMLBody = f"{df}<br><br>Email was sent via automated script"
    mail.Send()
