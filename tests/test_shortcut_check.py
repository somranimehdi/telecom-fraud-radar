import polars as pl

from model.shortcut_check import single_feature_auc


def test_single_feature_auc_perfect_and_uninformative():
    frame = pl.DataFrame(
        {
            "label": [0, 0, 0, 0, 1, 1, 1, 1],
            "perfect": [1, 2, 3, 4, 5, 6, 7, 8],
            "random": [1, 4, 2, 3, 4, 1, 3, 2],
        }
    )
    assert single_feature_auc(frame, "perfect") == 1.0
    assert single_feature_auc(frame, "random") == 0.5
