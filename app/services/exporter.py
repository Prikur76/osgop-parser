import polars as pl


def to_csv(contracts):
    df = pl.DataFrame([c.dict() for c in contracts])
    return df.write_csv()
