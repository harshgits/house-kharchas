from text_table_tools import TextTableTools
import pandas as pd

df_json = '''
    {
        "columns": ["name", "age", "city"],
        "index": [0, 1, 2],
        "data": [
            ["Alice", 25, "New York"],
            ["Bob", 30, "Los Angeles"],
            ["Charlie", 35, "Chicago is such a great city. Why are we talking about the city again?"]
        ]
    }
    '''
df = pd.read_json(df_json, orient="split")
text_table = TextTableTools.convert_dataframe_to_texttable(df, max_width=25)
print(text_table)
