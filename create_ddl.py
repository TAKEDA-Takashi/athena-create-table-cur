import json
import re
from pathlib import Path

import boto3

from jinja2 import Environment, FileSystemLoader

TEMPLATE_NAME = 'template/ddl.hql.j2'


def create_ddl(manifest_path, profile_name=None):
    """
    CURのマニフェストファイルからAthena用のDDLを作成する。
    params:
        manifest_path : マニフェストファイルのS3パス
        profile_name : S3にアクセスする際に利用するプロファイル。指定がなければdefault
    """
    session = boto3.Session(profile_name=profile_name)

    bucket_name, manifest_path = __parse_s3path(manifest_path)

    manifest_data = __get_manifest_data(bucket_name, manifest_path, session)
    template = __get_template(TEMPLATE_NAME)

    cur_dirpath = Path(manifest_path).parent

    columns = manifest_data['columns']
    s3_cur_dirpath = f's3://{bucket_name}/{cur_dirpath}/'

    render_params = {
        's3_cur_dirpath': s3_cur_dirpath,
        'columns': columns
    }
    ddl_sql = template.render(**render_params)

    return ddl_sql


def __parse_s3path(s3_path):
    """
    S3のパスをバケット名とオブジェクトのキーに分ける
    """
    m = re.match(r's3://(?P<bucket_name>.+?)/(?P<manifest_path>.+)', s3_path)

    bucket_name = m.group('bucket_name')
    object_key = m.group('manifest_path')

    return (bucket_name, object_key)


def __get_manifest_data(bucket_name, manifest_path, session):
    """
    マニフェストファイルの内容をdictで返す
    """
    s3 = session.client('s3')

    object_body = s3.get_object(Bucket=bucket_name, Key=manifest_path)['Body']
    manifest_data = json.loads(object_body.read().decode())
    return manifest_data


def __get_template(template_name):
    """
    DDLのテンプレートを返す
    """
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(template_name)
    return template


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='CURのマニフェストファイルからAthena用のDDLを作成する')

    parser.add_argument('s3_manifest_path')
    parser.add_argument('--profile')

    args = parser.parse_args()

    s3_manifest_path = args.s3_manifest_path
    profile_name = args.profile
    template_dir = '.'

    ddl = create_ddl(s3_manifest_path, profile_name)

    print(ddl)
