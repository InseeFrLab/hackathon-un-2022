import io
import base64
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
#import dash_leaflet as dl

import utils.functions as fc

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.JOURNAL]
    )



ports = fc.import_ports()
ship_data_enriched = fc.create_ship_data_enriched()
AIS = fc.read_ais_parquet()
AIS_enriched = fc.enrich_AIS_data(
    AIS, ship_data_enriched
)

# these could be dynamically assigned if we have data on suez canal
nb_boats = int(
    fc.count_boats(AIS_enriched, unique_id = "mmsi")
)


app.layout = html.Div(children=[
    
    html.H1(children='TITRE NIVEAU 1'),

    html.Br(),
    html.Br(),

    html.H2(children='What happens when an exceptional shock reduces a region capacity ?'),

    html.Br(),


    html.Div(
        children = dcc.Dropdown(
            options=[
                {'label': 'Black Sea', 'value': 'Black'},
                {'label': 'Suez Canal', 'value': 'Suez'},
            ],
                value = 'Black Sea',
                id='region-problem',
                clearable=False,
                placeholder="Select a region"
            ),
        style={'width': '25%'}
    ),

    dcc.Graph(
        id='worldmap-ports'
    ), 

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


@app.callback(
    Output('waffle-simple', 'src'),
    Input('region-problem', 'value')
    )
def update_graph(xaxis_column_name):
    buf = io.BytesIO() # in-memory files
    fc.waffle_chart_zone(AIS_enriched, by = "ShiptypeLevel1")
    plt.savefig(buf, format = "png") # save to the above file object
    plt.close()
    data = base64.b64encode(buf.getbuffer()).decode("utf8") # encode to html elements
    return "data:image/png;base64,{}".format(data)

@app.callback(
    Output(component_id='region-number-boat', component_property='children'),
    Input('region-problem', 'value')
    )
def update_output_div(input_value):
    text = f'In the selected region ({input_value}), in a normal week, {nb_boats} different boats cross this territory'
    return text


@app.callback(
    Output('waffle', 'src'),
    Input('xaxis-column', 'value'),
    Input('my-slider', 'value'),
    )
def update_graph(xaxis_column_name, share_block):
    buf = io.BytesIO() # in-memory files
    fc.waffle_chart_zone(AIS_enriched, by = xaxis_column_name, share_blocked=share_block/100)
    plt.savefig(buf, format = "png") # save to the above file object
    plt.close()
    data = base64.b64encode(buf.getbuffer()).decode("utf8") # encode to html elements
    return "data:image/png;base64,{}".format(data)

@app.callback(
    Output('worldmap-ports', 'figure'),
    Input('region-problem', 'value')
    )
def update_figure(region_name):
    fig = fc.plot_worldmap_ports(ports, region = region_name)
    return fig



if __name__ == '__main__':
    app.run_server(
        debug=True, port = 5000, host='0.0.0.0')