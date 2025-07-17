from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["cti_db"]
collection = db["predictions"]

collection.insert_one({
    "Destination_Port": 8080,
    "Flow_Duration": 12000,
    "Total_Fwd_Packets": 20,
    "Total_Backward_Packets": 30,
    "Fwd_Header_Length": 150,
    "Bwd_Header_Length": 125,
    "Fwd_Packets_s": 2.5,
    "Bwd_Packets_s": 4.4,
    "prediction": 0
})
