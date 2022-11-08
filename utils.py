def get_schema(client):
    schema = client.schema.get()
    return schema    


def delete_schema(client):
    # client.schema.delete_all()
    return True 


def get_object_count(client, wv_classname):
    try:
        obj_count = client.query.aggregate(wv_classname).with_fields('meta { count }').do()
        return obj_count['data']['Aggregate'][wv_classname][0]['meta']['count']
    except Exception as e:
        return e
        