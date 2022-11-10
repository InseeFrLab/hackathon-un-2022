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

    html.Div([
        html.Div(
            normal_line_plot_map,
            style={'width': '45%'}
        ),
        html.Div(
            crisis_line_plot_map,
            style={'width': '45%'}
        ),
    ], style={'display': 'flex', 'flex-direction': 'row'}),

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
    boat_position = fc.read_random_boats_prepared(region = region_name)
    fig = fc.plot_worldmap_ports(ports, region = region_name, boat_position = boat_position)
    return fig


# LINE PLOTS FOR BOAT COUNTS

@app.callback(
    Output('normal-count-line-plot', 'figure'),
    Input('region-problem', 'value')
    )
def normal_line_count_figure(region_name):
    return fc.plot_normal_line_count(region_name.lower())


@app.callback(
    Output('crisis-count-line-plot', 'figure'),
    Input('region-problem', 'value')
    )
def crisis_line_count_figure(region_name):
    return fc.plot_crisis_line_count(region_name.lower())


# WAFFLE CHART BEGINNING ---------

fs = fc.create_s3_fs()

@app.callback(
    Output('waffle-simple', 'src'),
    Input('region-problem', 'value')
    )
def update_graph(region_name):
    region_name = region_name.split(" ")[0]
    path = f"projet-hackathon-un-2022/output/waffle-{region_name}-2019-04-01.png"
    image_filename = f"waffle-{region_name}-2019-04-01.png"
    fs.download(path,image_filename)
    encoded_image = base64.b64encode(open(image_filename, 'rb').read())
    return 'data:image/png;base64,{}'.format(encoded_image.decode())

@app.callback(
    Output(component_id='region-number-boat', component_property='children'),
    Input('region-problem', 'value')
    )
def update_output_div(input_value):
    AIS_enriched = fc.read_ais_prepared(region = input_value)
    nb_boats = int(
        fc.count_boats(AIS_enriched, unique_id = "mmsi")
    )
    text = f'In the selected region ({input_value}), in a normal week, {nb_boats} different boats cross this territory'
    return text

# WAFFLE CHART BLOCKED ---------

@app.callback(
    Output('waffle', 'src'),
    Input('region-problem', 'value'),
    Input('xaxis-column', 'value'),
    Input('my-slider', 'value'),
    )
def update_graph(region_name, xaxis_column_name, share_block):
    AIS_enriched = fc.read_ais_prepared(region = region_name)
    buf = io.BytesIO() # in-memory files
    fc.waffle_chart_zone(AIS_enriched, by = xaxis_column_name, share_blocked=share_block/100)
    plt.savefig(buf, format = "png") # save to the above file object
    plt.close()
    data = base64.b64encode(buf.getbuffer()).decode("utf8") # encode to html elements
    return "data:image/png;base64,{}".format(data)



if __name__ == '__main__':
    app.run_server(
        debug=True, port = 5000, host='0.0.0.0')