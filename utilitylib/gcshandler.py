import json
from google.cloud import storage

class GCS:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def save(self, data, blob_name, local = False):
        # Saves Python dict as a .json file to GCS. Blob name is required.
        try:
            json_content = json.dumps(data, ensure_ascii=False, indent=2)
            if local:
                with open(blob_name, "w", encoding='utf-8') as f:
                    f.write(json_content)
                return True
            else:
                client = storage.Client()
                bucket = client.bucket(self.bucket_name)
                blob = bucket.blob(blob_name)
                blob.upload_from_string(json_content, content_type='application/json; charset=utf-8')
                return True
        except Exception as e:
            print(f"Error saving JSON dict to GCS: {e}")
            return False

    def load(self, local_file_name, blob_name = "", local = False):
        # Loads .json file from GCS. Blob name is set to local file name if not provided.
        try:
            if not blob_name: blob_name = local_file_name
            if local:
                with open(local_file_name, "r", encoding='utf-8') as f:
                    content = f.read()
                    return json.loads(content) if content else False
            else:
                client = storage.Client()
                bucket = client.bucket(self.bucket_name)
                blob = bucket.blob(blob_name)
                if not blob.exists(): return False
                content = blob.download_as_text(encoding='utf-8')
                return json.loads(content) if content else False
        except Exception:
            return False

__all__ = ['GCS']
