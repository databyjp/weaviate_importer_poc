import json
import pandas as pd
import streamlit as st

dtype_maps = {object: "string", int: "int", bool: "boolean", float: "number", "default": "string"}
dtype_map_keys = list(dtype_maps.keys())


def get_schema(client):
    schema = client.schema.get()
    return schema    


def get_tot_object_count(client):
    schema = get_schema(client)
    class_names = [i['class'] for i in schema['classes']]
    obj_counts = [get_object_count(client, c) for c in class_names]
    print(obj_counts)
    return sum(obj_counts)    


def get_object_count(client, class_name):
    try:
        c_count_resp = client.query.aggregate(class_name).with_fields('meta { count }').do()
        return c_count_resp['data']['Aggregate'][class_name][0]['meta']['count']
    except:
        return 0


def parse_json(path):
    with open(path, 'r') as g:
        # line = g.readline()
        # while line:
        #     yield json.loads(line)
        #     line = g.readline()  
        data = json.loads(g.read())
        print(data['documents'])
        return data['documents']


def get_preview_data(fpath):
    dlist = list()
    for i, l in enumerate(parse_json(fpath)):
        dlist.append(l)
        if i+1 >= 100:
            break

    return dlist


def get_preview_df(fpath):
    if fpath.suffix == '.json':
        dlist = get_preview_data(fpath)      
        df = pd.DataFrame(dlist)
    else:
        df = pd.read_csv(fpath)
    return df[:100]


def build_schema(client, class_obj_in):
    client.schema.create_class(class_obj_in)
    return True


def get_dtype_index(datatype):
    try:
        return dtype_map_keys.index(datatype)
    except:
        return dtype_map_keys.index(dtype_maps["default"])


def get_dtype(datatype):
    return dtype_maps[dtype_map_keys[get_dtype_index(datatype)]]


def get_csv_to_class(fpath, class_name):
    df = pd.read_csv(fpath, index_col=0)
    dtype_sers = df.dtypes

    props = list()
    for colname, datatype in dtype_sers.iteritems():
        prop = {
            "dataType": [get_dtype(datatype)],
            "name": colname,
            "description": f"Contains_{colname}"
        }
        props.append(prop)

    class_obj = {
        "class": class_name,
        "description": class_name,
        "properties": props,
    }
    return class_obj
            