import json
import pandas as pd

def get_schema(client):
    schema = client.schema.get()
    return schema    


def get_object_count(client, wv_classname):
    try:
        obj_count = client.query.aggregate(wv_classname).with_fields('meta { count }').do()
        return obj_count['data']['Aggregate'][wv_classname][0]['meta']['count']
    except Exception as e:
        return e


def parse_json(path):
    with open(path, 'r') as g:
        line = g.readline()
        while line:
            yield json.loads(line)
            line = g.readline()  


def get_preview_df(fpath):
    dlist = list()
    for i, l in enumerate(parse_json(fpath)):
        dlist.append(l)
        if i+1 >= 100:
            break

    df = pd.DataFrame(dlist)    
    return df
