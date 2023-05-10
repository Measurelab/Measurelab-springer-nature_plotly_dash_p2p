from datetime import date
import plotly.express as px
import plotly.graph_objects as go
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

# AJE - Main Scorecards - Unique Users - the unique count of 'user_identity'
score_card_1 = df['user_identity'].nunique()

# AJE - Main Scorecards - Versions Submitted - the unique count of versions_submitted (field name in the main ds 'file_name')
score_card_2 = df['versions_submitted'].nunique()

# AJE - Main Scorecards - Average Word Count - the average/mean of 'word_count'
score_card_3 = df['word_count'].mean().astype(int)

# AJE - Monthly Column Chart - Unique Users by created_at_year_month
# TODO: This rbm code should be applied to the dff variable in the update_bar_chart callback
fig_1 = df.groupby('created_at_year_month')['user_identity'].nunique(
).reset_index().sort_values('created_at_year_month')
fig_1 = df.groupby('created_at_year_month')['user_identity'].nunique(
).reset_index().sort_values('created_at_year_month')
fig1_plot = px.bar(fig_1, x='created_at_year_month', y='user_identity', text_auto=True,
                   labels={'created_at_year_month': 'Year Month', 'user_identity': 'Unique Users'}, template='plotly_white'
                   ).update_traces(marker=dict(color='#192c55'))
fig1_plot.update_traces(textfont_size=12, textangle=0,
                        textposition="outside", cliponaxis=False)

# AJE - Monthly Combo Chart - Unique Users & Versions Submitted by created_at_year_month - Use plotly graph objects Figure

def create_second_fig(df):
    y1 = df.groupby('created_at_year_month')['user_identity'].nunique(
    ).reset_index().sort_values('created_at_year_month')
    y2 = df.groupby('created_at_year_month')['versions_submitted'].nunique(
    ).reset_index().sort_values('created_at_year_month')
    layout = dict(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
    fig_2 = go.Figure(layout=layout)
    fig_2.add_trace(
        go.Bar(
            x=y1['created_at_year_month'],
            y=y1['user_identity'],
            name="Unique Users",
            text=y1['user_identity'],
            textposition="auto",
            marker=dict(color='#192c55'),

        )

    )
    fig_2.add_trace(
        go.Scatter(
            x=y2['created_at_year_month'],
            y=y2['versions_submitted'],
            name="Versions Submitted",
            mode='lines+markers+text',
            marker={'size': 9},
            line=dict(color='#057266', width=3),
            text=y2['versions_submitted'],
            textposition="top center"

        )
    )
    return fig_2


# TODO: AJE - Top Subject Areas by Submissions - Schools Bar Chart - the unique count of versions_submitted (file_name) by grandparent_area_of_study
fig_3 = df.groupby('grandparent_area_of_study')['versions_submitted'].nunique(
).reset_index().sort_values('versions_submitted', ascending=False)
fig3_plot = px.bar(fig_3, x='grandparent_area_of_study', y='versions_submitted', text_auto=True,
                   labels={'grandparent_area_of_study': 'School', 'versions_submitted': 'Versions Submitted'}, template='plotly_white'
                   ).update_traces(marker=dict(color='#192c55'))
fig3_plot.update_traces(textfont_size=12, textangle=0,
                        textposition="outside", cliponaxis=False)

# AJE - Top Subject Areas by Submissions - Departments Table - the unique count of versions_submitted (file_name) by parent_area_of_study

# Institution filter
# df = df[df['md5_contract'] == 'ZgAikV0XuC67t2d7KOog']

df_4 = df.rename(columns={
    'parent_area_of_study': 'Departments', 'versions_submitted': 'Submissions'})
df_4 = df_4.groupby('Departments')['Submissions'].nunique(
).reset_index().sort_values('Submissions', ascending=False)
fig4_table = go.Figure(data=[go.Table(header=dict(values=list(
    df_4.columns)), cells=dict(values=[df_4.Departments, df_4.Submissions]))])