
from dash import Dash, html, dcc, callback, Output, Input
from datetime import date
import dash
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes
import os



def generateScorecard(title, value, id, style={}):
    return html.Div(
        children=[
            html.P(children=title, style={
                "textAlign": "center", "margin": "14px", "fontSize": "16px"}),
            html.P(
                id=id, children=value, style={
                    "textAlign": "center", "fontSize": "16px"}
            )
        ]
    )