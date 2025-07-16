from __future__ import annotations

import absorb


def _print_title(title: str) -> None:
    import rich

    rich.print('[bold green]' + title + '[/bold green]')


def _dataset_to_str(dataset: absorb.TableDict) -> str:
    return dataset['source_name'] + '.' + dataset['table_name']


def _print_dataset_bullet(dataset: absorb.TableDict) -> None:
    import toolstr

    toolstr.print_bullet(
        '[white bold]' + _dataset_to_str(dataset) + '[/white bold]',
        **absorb.ops.bullet_styles,
    )
