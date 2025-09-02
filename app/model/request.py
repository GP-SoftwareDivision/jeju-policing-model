from pydantic import BaseModel

class SampleRequest(BaseModel):
    name: str
    