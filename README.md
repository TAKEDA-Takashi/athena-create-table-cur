# athena-create-table-cur

S3に保存されているCURマニフェストファイルからAthenaテーブル作成用のDDLを出力します。

- printモード
    - 標準出力にDDLを出力します
- athenaモード
    - AthenaにDDLを実行します

## 事前準備

```bash
$ pip3 install uv
$ uv sync
```

## 実行方法

ヘルプについては`uv run main.py -h`または`uv run main.py athena -h`などで確認できます。各モードでマニフェストファイルを指定しますが、トップレベルのマニフェストのパスを指定してください。自動的に最新のassemblyIdを対象としたDDLを生成します。

### printモード

標準出力にDDLを出力します。ファイルに保存などをしたい場合はリダイレクトなどで保存してください。

```bash
$ uv run main.py -m s3://${your_bucket}/${manifest_path}

or

$ uv run main.py -m s3://${your_bucket}/${manifest_path} print
```

### athenaモード

AthenaにDDLを実行します。Athenaクエリの結果保存用のS3パスを指定する必要があります。

```bash
$ uv run main.py -m s3://${your_bucket}/${manifest_path} athena -o s3://${your_bucket}/${output_path}
```

すでにテーブルが存在する場合は、`-f`オプションを付けることで再作成します。

```bash
$ uv run main.py -m s3://${your_bucket}/${manifest_path} athena -o s3://${your_bucket}/${output_path} -f
```

下記補足情報にあるように、マニフェストファイルの情報もクエリの対象になります。`-v`オプションを付けることでマニフェストファイルの情報を除外したビューを作成します。

```bash
$ uv run main.py -m s3://${your_bucket}/${manifest_path} athena -o s3://${your_bucket}/${output_path} -v
```

## 補足情報

CURが出力されているパスにマニフェストファイルが保存されている場合、マニフェストファイル自体もAthenaクエリの対象となります。これは2019-10-11現在でAthenaの仕様です。

https://docs.aws.amazon.com/ja_jp/athena/latest/ug/tables-location-format.html

そのため、結果セットにマニフェストのデータがゴミとして入ってしまう場合は、明示的に除外するクエリを書くようにしてください。

```sql
SELECT * FROM cur_table_name
WHERE "$path" NOT LIKE '%.json'
LIMIT 10;
```
