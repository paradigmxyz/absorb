from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl


class MempoolDumpster:
    data_

    renamed = {
        'hash': 'transaction_hash',
        'gas': 'gas_used',
        'from': 'from_address',
        'to': 'to_address',
        'chainId': 'chain_id',
        # 'txType': 'tx_type',
        'gasPrice': 'gas_price',
        'gasTipCap': 'max_priority_fee_per_gas',
        'gasFeeCap': 'max_fee_per_gas',
        'dataSize': 'n_data_bytes',
        'data4Bytes': 'data_4byte',
        'rawTx': 'raw_transaction',
        'timestamp': 'submission_timestamp',
        'includedAtBlockHeight': 'inclusion_block_number',
        'includedBlockTimestamp': 'inclusion_timestamp',
        'inclusionDelayMs': 'inclusion_delay',
    }

    reordered = [
        'includedAtBlockHeight',
        'inclusionDelayMs',
        'includedBlockTimestamp',
        'timestamp',
        'hash',
        'from',
        'to',
        'value',
        'nonce',
        'gas',
        'gasPrice',
        'gasTipCap',
        'gasFeeCap',
        'dataSize',
        'data4Bytes',
        'sources',
        'rawTx',
        'chainId',
    ]

    binary_columns = [
        'hash',
        'from',
        'to',
        'data4Bytes',
        'rawTx',
    ]

    @classmethod
    def scan(
        dataset: DatasetReference,
        *,
        start_time: tooltime.Timestamp | None = None,
        end_time: tooltime.Timestamp | None = None,
        root_dir: str | None = None,
        flat: bool | None = None,
        extra_kwargs: dict[str, typing.Any] | None = None,
    ) -> pl.LazyFrame:
        lf = super().scan()

        rename: bool = extra_kwargs.get('rename', True)
        reorder: bool = extra_kwargs.get('reorder', True)
        deduplicate: bool = extra_kwargs.get('deduplicate', True)
        columns: typing.Sequence[str] | None = extra_kwargs.get('columns', None)

        lf = lf.with_columns(
            value=pl.col.value.cast(pl.Float64),
            nonce=pl.col.nonce.cast(pl.Int64),
            gas=pl.col.gas.cast(pl.Float64),
            gasPrice=pl.col.gasPrice.cast(pl.Float64),
            gasTipCap=pl.col.gasTipCap.cast(pl.Float64),
            gasFeeCap=pl.col.gasFeeCap.cast(pl.Float64),
        )

        # select and reorder columns
        if reorder:
            lf = lf.select(reordered)
        if columns is not None:
            renamed_reverse = {v: k for k, v in renamed.items()}
            columns = [
                renamed_reverse.get(column, column) for column in columns
            ]
            lf = lf.select(columns)

        # deduplicate redundant transactions
        if deduplicate:
            if columns is None:
                columns = lf.collect_schema().names()
            column_kwargs = {}
            for column in columns:
                if column != 'hash':
                    column_kwargs[column] = pl.col(column).min()
            lf = lf.group_by('hash').agg(**column_kwargs)

        # rename columns
        if rename:
            if columns is not None:
                use_rename = {k: v for k, v in renamed.items() if k in columns}
            else:
                use_rename = renamed
            lf = lf.rename(use_rename)

        return lf


def load_block_stats(
    *, start_block: int | None = None, end_block: int | None = None
) -> pl.DataFrame:
    import cryo_manager
    import polars as pl

    return (
        cryo_manager.scan(
            'blocks',
            network='ethereum',
            include_timestamps=True,
        )
        .sort('block_number')
        .select(
            'block_number',
            'timestamp',
            'base_fee_per_gas',
            'gas_used',
            'gas_limit',
            utilization=pl.col.gas_used / pl.col.gas_limit,
        )
        .with_columns(prev_utilization=pl.col.utilization.shift(1))
        .collect()
    )


def join_block_stats(txs: pl.DataFrame) -> pl.DataFrame:
    import polars as pl

    blocks = load_block_stats(
        start_block=txs['inclusion_block_number'].min(),  # type: ignore
        end_block=txs['inclusion_block_number'].max(),  # type: ignore
    )
    blocks = blocks[
        'block_number', 'base_fee_per_gas', 'utilization', 'prev_utilization'
    ]

    return (
        txs.join(
            blocks,
            left_on='inclusion_block_number',
            right_on='block_number',
            how='left',
            coalesce=True,
        )
        .sort('inclusion_block_number')
        .with_columns(priority_fee=pl.col.gas_price - pl.col.base_fee_per_gas)
    )
