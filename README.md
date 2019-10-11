# athena-create-table-cur

S3に保存されているCURマニフェストファイルからAthenaテーブル作成用のDDLを出力します。

- printモード
    - 標準出力にDDLを出力します
- athenaモード
    - AthenaにDDLを実行します

## 事前準備

```bash
$ pip3 install pipenv
$ pipenv install
```

## 実行方法

ヘルプについては`pipenv run python main.py -h`または`pipenv run python main.py athena -h`などで確認できます。各モードでマニフェストファイルを指定しますが、トップレベルのマニフェストのパスを指定してください。自動的に最新のassemblyIdを対象としたDDLを生成します。

### printモード

標準出力にDDLを出力します。ファイルに保存などをしたい場合はリダイレクトなどで保存してください。

```bash
$ pipenv run python main.py -m s3://${your_bucket}/${manifest_path}

or

$ pipenv run python main.py -m s3://${your_bucket}/${manifest_path} print
```

### athenaモード

AthenaにDDLを実行します。Athenaクエリの結果保存用のS3パスを指定する必要があります。

```bash
$ pipenv run python main.py -m s3://${your_bucket}/${manifest_path} athena -o s3://${your_bucket}/${output_path}
```

## 補足情報

CURが出力されているパスにマニフェストファイルが保存されている場合、マニフェストファイル自体もAthenaクエリの対象となります。これは2019-10-11現在でAthenaの仕様です。

https://docs.aws.amazon.com/ja_jp/athena/latest/ug/tables-location-format.html

そのため、結果セットにマニフェストのデータがゴミとして入ってしまう場合は、明示的に除外するクエリを書くようにしてください。

```sql
SELECT * FROM cur_table_name
WHERE identity_timeinterval IS NOT NULL AND identity_timeinterval <> ''
LIMIT 10;
```
