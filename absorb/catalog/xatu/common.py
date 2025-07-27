from __future__ import annotations

import functools
import typing
import absorb

if typing.TYPE_CHECKING:
    import datetime
    import polars as pl


root_url = 'https://data.ethpandaops.io/xatu'

url_templates = {
    'per_day': root_url
    + '{network}/databases/default/{datatype}/{year}/{month}/{day}.parquet',
    'per_hour': root_url
    + '{network}/databases/default/{datatype}/{year}/{month}/{day}/{hour}.parquet',
}


class XatuTable(absorb.Table):
    source: str
    description = 'Xatu data from EthPandaOps'
    url = 'https://ethpandaops.io/data/xatu/'
    datatype: str
    per: typing.Literal['day', 'hour']
    parameter_types = {'network': str}
    name_template = '{class_name}_{network}'
    required_packages = ['yaml >= 5.4.1']

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return _fetch(
            datatype=self.datatype,
            network=self.parameters['network'],
            timestamp=chunk,  # type: ignore
            per=self.per,
        )

    def get_available_range(self) -> absorb.Coverage:
        return get_table_range(
            table=self.datatype, network=self.parameters['network']
        )


def _fetch(
    datatype: str, network: str, timestamp: datetime.datetime, per: str
) -> pl.DataFrame:
    url_template = url_templates['per_' + per]
    url = url_template.format(
        network=network,
        datatpye=datatype,
        year=timestamp.year,
        month=timestamp.month,
        day=timestamp.day,
        hour=timestamp.hour,
    )
    return absorb.ops.download_parquet_to_dataframe(url)


@functools.lru_cache()
def get_manifest() -> dict[str, typing.Any]:
    import requests
    import yaml  # type: ignore

    url = 'https://raw.githubusercontent.com/ethpandaops/xatu-data/refs/heads/master/config.yaml'
    response = requests.get(url)
    result: dict[str, typing.Any] = yaml.safe_load(response.text)
    return result


def get_table_range(
    table: str, network: str
) -> tuple[int, int] | tuple[datetime.datetime, datetime.datetime]:
    manifest = get_manifest()
    for item in manifest['tables']:
        if item['name'] == table:
            break
    else:
        raise Exception('could not find manifest for table')
    network_range = item['networks'][network]
    start = network_range['from']
    end = network_range['to']
    if item['partitioning']['type'] == 'integer':
        return int(start), int(end)
    elif item['partitioning']['type'] == 'datetime':
        import datetime

        return (
            datetime.datetime.strptime(start, '%Y-%m-%d'),
            datetime.datetime.strptime(end, '%Y-%m-%d'),
        )
    else:
        raise Exception()
