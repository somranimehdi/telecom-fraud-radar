import io
import json

import polars as pl
import pytest

from quality_gate.io import read_source
from quality_gate.runner import EmptySourceError, validate_source


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, _name):
        return self

    def paginate(self, **kwargs):
        prefix = kwargs["Prefix"]
        yield {"Contents": [
            {"Key": key, "Size": len(body)}
            for key, body in self.objects.items()
            if key.startswith(prefix)
        ]}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


def test_auto_reads_parquet():
    buf = io.BytesIO()
    pl.DataFrame({"n": [1]}).write_parquet(buf)
    df, count = read_source(FakeS3({"raw/one.parquet": buf.getvalue()}), "b", "raw/", "auto")
    assert count == 1
    assert df.to_dict(as_series=False) == {"n": [1]}


def test_auto_reads_jsonl():
    s3 = FakeS3({"raw/one.jsonl": b'{"n":2}\n'})
    df, count = read_source(s3, "b", "raw/", "auto")
    assert count == 1
    assert df.to_dict(as_series=False) == {"n": [2]}


def test_auto_reads_mixed_folder_and_ignores_empty_folder_key():
    buf = io.BytesIO()
    pl.DataFrame({"n": [1]}).write_parquet(buf)
    s3 = FakeS3({
        "raw/folder/": b"",
        "raw/a.parquet": buf.getvalue(),
        "raw/b.json": (json.dumps({"n": 2}) + "\n").encode(),
    })
    df, count = read_source(s3, "b", "raw/", "auto")
    assert count == 2
    assert sorted(df["n"].to_list()) == [1, 2]


def test_empty_source_error_names_source_and_prefix():
    with pytest.raises(EmptySourceError, match=r"activation.*raw/activation/"):
        validate_source(FakeS3({}), "b", "activation", "run", verbose=False)
