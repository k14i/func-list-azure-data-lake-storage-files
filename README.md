# Azure Functions to list up files on Azure Data Lake Storage Gen2

Azure Data Lake Storage Gen2 のファイルのリストを返却する Azure Functions です。

レスポンスボディ (JSON) を Azure Data Factory の ForEach アクティビティに渡し、ファイルごとにマッピング データフロー を実行するような用途を想定しています。

このコードはプロトタイプです。このコードの利用やその結果に対して、作者は一切の責任を負いません。


## Prerequisites (前提条件)

* Python 3.9
* Visual Studio Code


## How to

### Install

```bash
pip install -U pip
pip install -r requitements.txt
```

### Test

```bash
make test
```
