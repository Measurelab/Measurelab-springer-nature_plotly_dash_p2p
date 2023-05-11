from utils.pd_gbq import *
from components.functions import *
import dash
from dash import html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc

# Define app
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=False

)

app.layout = html.Div(children=[
    # All elements from the top of the page
    html.Div([
        html.H1(children="AJE"),
        dbc.Row([
                html.Div(
                    id="scorecard-container",
                    style={
                        "display": "flex",
                        "justifyContent": "space-evenly", 
                    },
                    children=[
                        generateScorecard(
                            "Unique Users",
                            score_card_1,
                            "summary-unique-users",
                        ),
                        generateScorecard(
                            "Versions Submitted",
                            score_card_2,
                            "summary-versions-submitted",
                        ),
                        generateScorecard(
                            "Average Word Count",
                            score_card_3,
                            "summary-average-word-count",
                        ),

                    ]
                )
                ]),
        dbc.Row([
                html.H2(
                    children='''AJE - Monthly Column Chart - Unique Users by created_at_year_month'''),
                dcc.Graph(
                    id='fig-1',
                    figure=fig1_plot
                ),
                ])
    ]),
    html.Br(),
    dbc.Row([
            html.H2(
                children='''AJE - Monthly Combo Chart - Unique Users & Versions Submitted by created_at_year_month'''),
            dcc.DatePickerRange(
                id='date-picker-range',
                start_date=df["created_at_year_month"].min(),
                end_date=df["created_at_year_month"].max(),
                min_date_allowed=df["created_at_year_month"].min(),
                max_date_allowed=df["created_at_year_month"].max(),
                persistence=True,
                persisted_props=["start_date"],
                persistence_type="session"
            ),
            html.Div(id='output-container-date-picker-range'),
            dcc.Graph(
                id='fig-2',
                figure={}

            )]),
    html.Br(),
    dbc.Row([
            html.H2(children='''AJE - Top Subject Areas by Submissions - Schools Bar Chart - the unique count of versions_submitted (file_name) by grandparent_area_of_study'''),
            dcc.Graph(
                id='fig-3',
                figure=fig3_plot
            )]),
    html.Br(),
    dbc.Row([
            html.H2(children='''AJE - Top Subject Areas by Submissions - Departments Table - the unique count of versions_submitted (file_name) by parent_area_of_study'''),
            dcc.Graph(
                id='fig-4',
                figure=fig4_table
            )])
])

# APP CALLBACKS


@app.callback(
    Output('fig-2', 'figure'),
    [Input("date-picker-range", "start_date"),
     Input("date-picker-range", "end_date")]
)
def update_output(start_date, end_date):
    filtered_data = df.query(
        "created_at_year_month >= @start_date and created_at_year_month <= @end_date")
    fig_2 = create_second_fig(filtered_data)
    return fig_2


server = app.server  # Expose server variable for Procfile

if __name__ == '__main__':
    app.run_server(host="localhost", port="8080", debug=True)
