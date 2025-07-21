from __future__ import annotations

import absorb


def _print_title(title: str) -> None:
    import rich

    rich.print('[bold green]' + title + '[/bold green]')


def _print_dataset_bullet(dataset: absorb.TableReference) -> None:
    import toolstr

    table_dict = absorb.Table.instantiate(dataset).create_table_dict()
    full_name = table_dict['source_name'] + '.' + table_dict['table_name']
    toolstr.print_bullet(
        '[white bold]' + full_name + '[/white bold]',
        **absorb.ops.bullet_styles,
    )
