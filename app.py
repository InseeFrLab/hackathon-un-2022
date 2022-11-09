from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import pandas as pd
import io
import base64
import matplotlib.pyplot as plt

import utils.functions as fc

app = Dash(__name__)



ports = fc.import_ports()
ship_data_enriched = fc.create_ship_data_enriched()
AIS = fc.read_ais_parquet()
AIS_enriched = fc.enrich_AIS_data(
    AIS, ship_data_enriched
)
#fc.waffle_chart_zone(AIS_enriched, by = )


# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
df = pd.DataFrame({
    "Fruit": ["Apples", "Oranges", "Bananas", "Apples", "Oranges", "Bananas"],
    "Amount": [4, 1, 2, 2, 4, 5],
    "City": ["SF", "SF", "SF", "Montreal", "Montreal", "Montreal"]
})

fig = px.bar(df, x="Fruit", y="Amount", color="City", barmode="group")

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for your data.
    '''),

    html.Div(
        children = dcc.Dropdown(
                ["ShiptypeLevel1", "ShipTypeLevel2"],
                'ShiptypeLevel1',
                id='xaxis-column'
            ),
        style={'width': '25%'}
    ),

    #dcc.Graph(
    #    id='waffle',
    #    figure=fig
    #)

    html.Img(id='waffle')
])

@app.callback(
    Output('waffle', 'src'),
    Input('xaxis-column', 'value')
    )
def update_graph(xaxis_column_name):
    buf = io.BytesIO() # in-memory files
    fc.waffle_chart_zone(AIS_enriched, by = xaxis_column_name)
    plt.savefig(buf, format = "png") # save to the above file object
    plt.close()
    data = base64.b64encode(buf.getbuffer()).decode("utf8") # encode to html elements
    return "data:image/png;base64,{}".format(data)

if __name__ == '__main__':
    app.run_server(debug=True, port = 5000, host='0.0.0.0')