def get_line_count(data, context):
        from google.cloud import storage
        file = data['name']
        bucket = client.get_bucket('bdl22_ch18b067')
        blob = bucket.get_blob(file)
        x = blob.download_as_string()
        x = x.decode('utf-8')
        print(len(x.split('\n')))
        return
