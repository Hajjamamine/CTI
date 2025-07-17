import asyncio
import json
from channels.generic.websocket import AsyncWebsocketConsumer
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client["cti_db"]
collection = db["predictions"]

FIELDS = [
    "Destination_Port", "Flow_Duration", "Total_Fwd_Packets",
    "Total_Backward_Packets", "Fwd_Header_Length", "Bwd_Header_Length",
    "Fwd_Packets_s", "Bwd_Packets_s", "prediction"
]


class TrafficConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        self.running = True
        asyncio.create_task(self.watch_changes())

    async def disconnect(self, close_code):
        self.running = False

    async def watch_changes(self):
        pipeline = [{"$match": {"operationType": "insert"}}]
        with collection.watch(pipeline) as stream:
            for change in stream:
                if not self.running:
                    break
                doc = change["fullDocument"]
                data = {field: doc.get(field) for field in FIELDS}
                await self.send(text_data=json.dumps(data))

