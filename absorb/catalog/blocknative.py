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
    description = 'Snapshots of the Ethereum mempool'
    url = 'https://docs.blocknative.com/data-archive/mempool-archive'
    write_range = 'append_only'
    chunk_size = 'hour'
    index_column = 'detecttime'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'detecttime': pl.Datetime('us', 'UTC'),
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

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        url = url_template.format(
            year=chunk.year,  # type: ignore
            month=chunk.month,  # type: ignore
            day=chunk.day,  # type: ignore
            hour=chunk.hour,  # type: ignore
        )
        polars_kwargs = {'separator': '\t', 'schema': self.get_schema()}
        return absorb.ops.download_csv_gz_to_dataframe(
            url=url, polars_kwargs=polars_kwargs
        ).with_columns(
            pl.col.detecttime.str.to_datetime(time_unit='us', time_zone='UTC')
        )

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        return (
            datetime.datetime(year=2019, month=11, day=1, hour=0),
            datetime.datetime(year=2025, month=3, day=1, hour=0),
        )
