import weaviate

client = weaviate.Client("http://localhost:8080")
client.schema.delete_all()
