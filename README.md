# hackathon-un-2022

In this repo, we store the code that we have developed during the `2022 UN big data hackathon`. For more information 
about the hackathon, you cloud visit this [page](https://unstats.un.org/bigdata/events/2022/hackathon/)

We mainly used below data sets:
- AIS (provided by UN)
- IHS (open data)
- FAO (provided by UN)


## Repository content

The data preparation (e.g. extraction from UN cluster, transformation), and analytics are realized by using 
- pyspark
- pandas

They are mainly located at the [notebooks](./notebooks) folder.

The data visualization dashboard is implemented by using `plotly dash`. You can find the main rendering logic at
[app.py](./app.py) and the helper functions in the [utils](./utils) folder.

This dashboard is deployed on our k8s cluster. You can visit it by using this url : [https://datadive.lab.sspcloud.fr/](https://datadive.lab.sspcloud.fr/)

