from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl

url_template = (
    'https://archive.blocknative.com/{year}{month:02}{day:02}/{hour:02}.csv.gz'
)


class Mempool(absorb.Table):
    source = 'blocknative'
    write_range = 'overwrite_all'
    range_format = 'per_hour'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'detecttime': pl.String,
            'hash': pl.String,
            'status': pl.String,
            'region': pl.String,
            'reorg': pl.String,
            'replace': pl.String,
            'curblocknumber': pl.Int64,
            'failurereason': pl.String,
            'blockspending': pl.Int64,
            'timepending': pl.Int64,
            'nonce': pl.Int64,
            'gas': pl.Int64,
            'gasprice': pl.Float64,
            'value': pl.Float64,
            'toaddress': pl.String,
            'fromaddress': pl.String,
            'input': pl.String,
            'network': pl.String,
            'type': pl.Int64,
            'maxpriorityfeepergas': pl.Float64,
            'maxfeepergas': pl.Float64,
            'basefeepergas': pl.Float64,
            'dropreason': pl.String,
            'rejectionreason': pl.String,
            'stuck': pl.Boolean,
            'gasused': pl.Int64,
            'detect_date': pl.String,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        url = url_template.format(
            year=data_range.year,
            month=data_range.month,
            day=data_range.day,
            hour=data_range.hour,
        )
        polars_kwargs = {'separator': '\t', 'schema': self.get_schema()}
        return absorb.ops.download_csv_gz_to_dataframe(
            url=url, polars_kwargs=polars_kwargs
        )

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        return (
            datetime.datetime(year=2019, month=11, day=1, hour=0),
            datetime.datetime(year=2025, month=3, day=1, hour=0),
        )
