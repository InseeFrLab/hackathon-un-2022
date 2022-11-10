import io
import base64
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import plotly.express as px
#import dash_leaflet as dl

import utils.functions as fc

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.JOURNAL]
    )

load_figure_template("JOURNAL")


# IMPORT DATA --------------------------------

ports = fc.import_ports()

ship_data_enriched = fc.create_ship_data_enriched()

# 



SIDEBAR_STYLE = {
    "position": "relative",
    #"top": 0,
    #"left": 0,
    #"bottom": 0,
    #"width": "24rem",
    #"padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}


sidebar = html.Div(
    [
        html.H3("Regional analysis"),
        html.Hr(),
        html.P(
            "Choose region of interest", className="lead"
        ),
        dbc.Nav(
            [
                   dcc.Dropdown(
            options=[
                {'label': 'Black Sea', 'value': 'Black'},
                {'label': 'Suez Canal', 'value': 'Suez'},
            ],
                value = 'Black Sea',
                id='region-problem',
                clearable=False,
                placeholder="Select a region"
            ) 
            ],
            vertical=True,
            pills=True,
        ),
    ],
    #style=SIDEBAR_STYLE,
)

ports_map = dcc.Graph(id='worldmap-ports')

app.layout = html.Div(children=[
    
    html.H1(children='AIS dashboard'),

    html.Br(),
    html.Br(),

    html.H2(children='What happens when an exceptional shock reduces a region capacity ?'),

    html.Br(),

    # html.Div(
    #     [
    #         dbc.Row(
    #             [dbc.Col(
    #                 sidebar
    #             ),
    #             dbc.Col(
    #                 ports_map, width = 10
    #                 )
    #             ])
    #     ]
    # ),

    html.Div(
        sidebar,
        style={'width': '25%'}
    ),

    html.Div(
        ports_map
    ),

    # html.Div(
    #     children = dcc.Dropdown(
    #         options=[
    #             {'label': 'Black Sea', 'value': 'Black'},
    #             {'label': 'Suez Canal', 'value': 'Suez'},
    #         ],
    #             value = 'Black Sea',
    #             id='region-problem',
    #             clearable=False,
    #             placeholder="Select a region"
    #         ),
    #     style={'width': '25%'}
    # ),


    html.Br(),

    html.Div(id='region-number-boat'),  

    html.Br(),

    html.Div(children='State of traffic in this period:'),  

    html.Img(id='waffle-simple'),

    html.Br(),


    html.H2(children='''
        Simulating problem in this region
    '''),

    html.Div( children = [
        dcc.Slider(0, 100,
        id='my-slider',
    marks={
        0: 'No blockage',
        100: 'No capacity',
        25: '25%',
        50: '50%',
        75: '75%'
    },
    value=10)
    ]),


    html.Div(
        children = dcc.Dropdown(
                ["ShiptypeLevel1", "ShipTypeLevel2"],
                'ShiptypeLevel1',
                id='xaxis-column'
            ),
        style={'width': '25%'}
    ),

    html.H2(children='''
        What are the consequences on other countries ?
    '''),

    # compare our map with Lloyd's

    html.Img(id='waffle')
])


# INITIAL MAP CARGO SIMULATION ----------

@app.callback(
    Output('worldmap-ports', 'figure'),
    Input('region-problem', 'value')
    )
def update_figure(region_name):
    fig = fc.plot_worldmap_ports(ports, region = region_name)
    return fig


# WAFFLE CHART BEGINNING ---------

@app.callback(
    Output('waffle-simple', 'src'),
    Input('region-problem', 'value')
    )
def update_graph(region_name):
    AIS_enriched = fc.read_ais_prepared(region = region_name)
    buf = io.BytesIO() # in-memory files
    fc.waffle_chart_zone(AIS_enriched, by="ShiptypeLevel1")
    plt.savefig(buf, format="png") # save to the above file object
    plt.close()
    data = base64.b64encode(buf.getbuffer()).decode("utf8") # encode to html elements
    return "data:image/png;base64,{}".format(data)

@app.callback(
    Output(component_id='region-number-boat', component_property='children'),
    Input('region-problem', 'value')
    )
def update_output_div(input_value):
    AIS_enriched = fc.read_ais_prepared(region = input_value)
    #boat_position = fc.random_sample_position(AIS_enriched)
    nb_boats = int(
        fc.count_boats(AIS_enriched, unique_id = "mmsi")
    )
    text = f'In the selected region ({input_value}), in a normal week, {nb_boats} different boats cross this territory'
    return text


@app.callback(
    Output('waffle', 'src'),
    Input('xaxis-column', 'value'),
    Input('my-slider', 'value'),
    )
def update_graph(xaxis_column_name, share_block):
    AIS_enriched = fc.read_ais_prepared(region = xaxis_column_name)
    buf = io.BytesIO() # in-memory files
    fc.waffle_chart_zone(AIS_enriched, by = xaxis_column_name, share_blocked=share_block/100)
    plt.savefig(buf, format = "png") # save to the above file object
    plt.close()
    data = base64.b64encode(buf.getbuffer()).decode("utf8") # encode to html elements
    return "data:image/png;base64,{}".format(data)



if __name__ == '__main__':
    app.run_server(
        debug=True, port = 5000, host='0.0.0.0')