from __future__ import annotations

import truck


def _print_title(title: str) -> None:
    import rich

    rich.print('[bold green]' + title + '[/bold green]')


def _dataset_to_str(dataset: truck.TrackedTable) -> str:
    return dataset['source_name'] + '.' + dataset['table_name']


def _print_dataset_bullet(dataset: truck.TrackedTable) -> None:
    import toolstr

    toolstr.print_bullet(
        '[white bold]' + _dataset_to_str(dataset) + '[/white bold]',
        **truck.ops.formatting.bullet_styles,
    )


def _print_source_datasets_bullet(
    source: str, datasets: list[type[truck.Table]]
) -> None:
    import toolstr

    names = [cls.class_name(allow_generic=True) for cls in datasets]
    toolstr.print_bullet(
        key=source,
        value='[green],[/green] '.join(names),
        **truck.ops.formatting.bullet_styles,
    )
