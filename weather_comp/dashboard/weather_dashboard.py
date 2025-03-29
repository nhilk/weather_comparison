from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import polars as pl
import sys
import os
sys.path.append(os.path.abspath(""))
from database.db import DB
import logging
import toml

logging.basicConfig(
    filename='weather_comparison_ambient_main.log',  # Specify the log file name
    level=logging.DEBUG,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode='w',  # Mode: 'w' to overwrite, 'a' to append
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' # Define the log message format
)

def get_config():
    config = toml.load('config.toml')
    return config

config = get_config()
db = DB(get_config())

df = db.read_from_table(table = 'source_comparison')
app = Dash()

app.layout = [
    html.H1(children='Title of Dash App', style={'textAlign':'center'}),
    dcc.Dropdown(df.get_column('source1').unique().to_list(), id='dropdown1'),
    dcc.Dropdown(df.get_column('source2').unique().to_list(), id='dropdown2'),
    dcc.Graph(id='graph-content')
]

@callback(
    Output('graph-content', 'figure'),
    [Input('dropdown1', 'value'),
    Input('dropdown2', 'value')]
    
)
def update_graph(source1, source2):
    if source2 is None:
        dff = df.filter((pl.col('source1') == source1))
    else:
        dff = df.filter((pl.col('source1') == source1) & (pl.col('source2') == source2))
    return px.scatter(dff, x='date1', y='temperature')

if __name__ == '__main__':
    app.run(debug=True) 