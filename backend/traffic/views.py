from rest_framework.decorators import api_view
from rest_framework.response import Response
from pymongo import MongoClient

@api_view(['GET'])
def get_predictions(request):
    client = MongoClient("mongodb://localhost:27017")
    db = client["cti_db"]
    collection = db["predictions"]

    # Query last 100 documents sorted by insertion order descending
    docs_cursor = collection.find({}, {
        "Destination_Port": 1,
        "Flow_Duration": 1,
        "Total_Fwd_Packets": 1,
        "Total_Backward_Packets": 1,
        "Fwd_Header_Length": 1,
        "Bwd_Header_Length": 1,
        "Fwd_Packets_s": 1,
        "Bwd_Packets_s": 1,
        "prediction": 1,
        "_id": 0  # exclude Mongo _id from result
    }).sort('_id', -1).limit(100)

    docs = list(docs_cursor)
    return Response(docs)
