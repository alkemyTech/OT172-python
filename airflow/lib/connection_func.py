def load_s3(path, id_conn, bucket_name, key):
    hook = S3Hook(id_conn)
    hook.load_file(filename=f'{path}/files/ET_Univ_tres_de_febrero.txt',
                   key=key, bucket_name=bucket_name)
