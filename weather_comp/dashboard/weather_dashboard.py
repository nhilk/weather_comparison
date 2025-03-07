from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
from database import DB
import logging
import toml

logging.basicConfig(
    filename='weather_comparison_ambient_main.log',  # Specify the log file name
    level=logging.DEBUG,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filemode='w',  # Mode: 'w' to overwrite, 'a' to append
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' # Define the log message format
)

def get_config():
    config = toml.load('weather_comp/config.toml')
    return config

config = get_config()
db = DB(get_config())

df = db.read_from_fact_weather()

app = Dash()

# Requires Dash 2.17.0 or later
app.layout = [
    html.H1(children='Title of Dash App', style={'textAlign':'center'}),
    dcc.Dropdown(df.col('source').unique(), 'Canada', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
]

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = df[df.country==value]
    return px.line(dff, x='year', y='pop')

if __name__ == '__main__':
    app.run(debug=True)