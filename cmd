Get-ChildItem -Path "C:\your\folder\path" -File -Recurse | Out-File -FilePath "C:\path\to\output.txt"

 lines = s3_hook.read_key(bucket_name, input_file_key, encoding=encoding).splitlines()
        
