import json
import dotenv
from google.cloud import storage

# Load dotenv
dotenv.load_dotenv()  # make sure .env is in the same folder as this script

client = storage.Client()
bucket = client.bucket("spotify-audio-features")

blobs = list(bucket.list_blobs(prefix="tracks/"))
for blob in blobs:
    print(blob.name)

blob = blobs[2]
content = blob.download_as_text()
data = json.loads(content)
print(data)