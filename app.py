from dash import Dash, html, dcc, callback, Output, Input
import dash
import plotly.express as px
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes


credentials = service_account.Credentials.from_service_account_file(
    'snprojectd4a01e34-0cb888d91a40.json')

project_id = 'snprojectd4a01e34'
client = bigquery.Client(credentials=credentials, project=project_id)
df = client.query(
    '''SELECT * FROM snprojectd4a01e34.measurelab_path_to_publish_dashboard.measurelab_aje_submissions_dashboard_permanent''').to_dataframe()

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True
)
server = app.server  # Expose server variable for Procfile

app.layout = html.Div([
    html.H1(children='AJE Visualisation', style={'textAlign':'center'}),
    dcc.Dropdown(options= df['User_Country'].unique(), id='dropdown-selection'),
    dcc.Graph(id='graph-content')
])

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = df[df.User_Country==value]
    return px.bar(dff, x='Access_Type', y='Word_Count')

if __name__ == '__main__':
    app.run_server(host="localhost", port="8080", debug=True)
