import weaviate

client = weaviate.Client("http://localhost:8080")
client.schema.delete_all()
client = weaviate.Client("https://importtest.semi.network/")
client.schema.delete_all()
