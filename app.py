from dash import Dash, html, dcc, callback, Output, Input
import dash
import plotly.express as px
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes
import os

project_id = 'snprojectd4a01e34'

if os.getenv("DEPLOYMENT_ENVIRONMENT", "").startswith("staging"):
    # Code is running in the staging environment
    print("GCP hosted: staging")
elif os.getenv("DEPLOYMENT_ENVIRONMENT", "").startswith("production"):
    # Code is running in the production environment
    print("GCP hosted: production")
else:
    # Local execution
    print("locally hosted")
    key_path = "snprojectd4a01e34-0cb888d91a40.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

scopes = (
    "https://www.googleapis.com/auth/drive",
)

# Fetch BigQuery data
bigquery.Client.SCOPE += scopes

client = bigquery.Client(project=project_id)
df = client.query(
    '''
  WITH main AS (
  SELECT 
    user_identity,
    m.preferred_group_code,
    m.preferred_group_name,
    access_type,
    CASE WHEN grandparent_area_of_study IS NULL THEN "None Provided" ELSE grandparent_area_of_study END as grandparent_area_of_study,
    CASE WHEN parent_area_of_study IS NULL THEN "None Provided" ELSE parent_area_of_study END as parent_area_of_study ,
    date(created_at_timestamp) as created_at_date,
    extract(YEAR from Created_At_Timestamp) as created_at_year,
    LEFT(CAST(Created_At_Timestamp as STRING),7) as created_at_year_month,
    --format_datetime("%b %Y",Created_At_Timestamp) as created_at_year_month,
    File_Name as versions_submitted,
    word_count,
    Name_Translation,
    --NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(Name_Translation), '[^a-zA-Z0-9]+', ''), ' ', ''), '') AS  institution_name_match_format -- no spaces
    NULLIF(REGEXP_REPLACE(LOWER(Name_Translation), r'[^\p{L}\p{N}\s]+', ''), '') AS institution_name_match_format

FROM `snprojectd4a01e34.measurelab_path_to_publish_dashboard.measurelab_aje_submissions_dashboard_permanent` m
LEFT JOIN `snprojectd4a01e34.measurelab_path_to_publish_plotly_dash.aje_institutions_translated` t
ON m.preferred_group_code = t.preferred_group_code
WHERE m.preferred_group_code NOT IN ("Digital Editing Subscription","Digital Translation Subscription","Digital Editing,Digital Translation Subscription")
)
,
lookup_key AS (
SELECT
DISTINCT
  lookup_id,
  aje_with_space AS institution_match
FROM `snprojectd4a01e34.measurelab_path_to_publish_dashboard.measurelab_kk_institution_lookup`
)

SELECT
DISTINCT 
    user_identity,
    m.preferred_group_code,
    m.preferred_group_name AS institution_name,
    access_type,
    grandparent_area_of_study,
    parent_area_of_study,
    created_at_date,
    created_at_year,
    created_at_year_month,
    versions_submitted,
    name_translation,
    word_count,
    institution_name_match_format,
    institution_match,
REGEXP_REPLACE(CAST(TO_BASE64(MD5(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(COALESCE(lk.lookup_id,institution_name_match_format)), r'[^a-zA-Z0-9]+', ''), ' ', ''), ''))) AS STRING), r'[^a-zA-Z\\d\\s]', '') AS md5_contract
FROM main m
LEFT JOIN lookup_key lk
ON m.institution_name_match_format = lk.institution_match
''').to_dataframe()

#  `snprojectd4a01e34.measurelab_path_to_publish_plotly_dash.aje_institutions_translated` t

# `snprojectd4a01e34.measurelab_path_to_publish_dashboard.measurelab_kk_institution_lookup`

# Dashboard component dataframes/values
# TODO: Store each scorecard's calculated value in a variable so they can be referenced when returning to the scorecard component
# TODO: Once each dashboard component is defined in the app layout with an ID, these dataframe operations should be moved to the app callbacks

# AJE - Main Scorecards - Unique Users - the unique count of 'user_identity'
df['user_identity'].nunique()

# AJE - Main Scorecards - Versions Submitted - the unique count of versions_submitted (field name in the main ds 'file_name')
df['versions_submitted'].nunique()

# AJE - Main Scorecards - Average Word Count - the average/mean of 'word_count'
df['word_count'].mean()

# AJE - Monthly Column Chart - Unique Users by created_at_year_month
# TODO: Is rbm being used in the app?
rbm = df.groupby('created_at_year_month')['user_identity'].nunique(
).reset_index().sort_values('created_at_year_month')

# TODO: AJE - Monthly Combo Chart - Unique Users & Versions Submitted by created_at_year_month - Use plotly graph objects Figure

# TODO: AJE - Top Subject Areas by Submissions - Schools Bar Chart - the unique count of versions_submitted (file_name) by grandparent_area_of_study - Use plotly express bar

# TODO: AJE - Top Subject Areas by Submissions - Departments Table - the unique count of versions_submitted (file_name) by parent_area_of_study - Use plotly graph objects Table

# Define app
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True
)
server = app.server  # Expose server variable for Procfile

# APP LAYOUT
# TODO: Create a function that returns a scorecard component that can be rendered in the app's layout
# TODO: Add Data Picker component - Use DatePickerRange from dcc?
app.layout = html.Div([
    html.H1(children='AJE Visualisation', style={'textAlign': 'center'}),
    dcc.Dropdown(options=df['created_at_year_month'], value=df['created_at_year_month']
                 [0], id='dropdown-selection', clearable=False),
    dcc.Graph(id='graph-content')
])

# APP CALLBACKS


@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_bar_chart(value):
    dff = df[df.created_at_year_month == value]
    fig_1 = px.bar(dff, x='created_at_year_month', y='user_identity', text_auto=True, barmode="group", labels={'created_at_year_month': 'Year Month', 'user_identity': 'Unique Users'}, template='plotly_white'
                   ).update_traces(marker=dict(color='#192c55'))
    return fig_1


if __name__ == '__main__':
    app.run_server(host="localhost", port="8080", debug=True)
