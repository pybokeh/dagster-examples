from dagster import op, String
import pandas as pd


@op
def df_to_html(context, df: pd.DataFrame) -> String:
    df_styled = (
        df
        .style
        .set_table_styles(
            [
                {'selector': 'th',
                 'props': 'border: 1px solid black; background-color: #0066cc; color: white; text-align: center;'},
                {'selector': 'td', 'props': 'border: 1px solid black'},
                {'selector': 'td:hover', 'props': 'background-color: yellow; font-size:14pt;'},
                {'selector': 'caption', 'props': 'font-size: 14pt; font-weight: bold;'},
            ]
        )
        .hide(axis='index')
    )

    return df_styled.to_html()
