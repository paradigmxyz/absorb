from __future__ import annotations

import typing

import truck


bullet_styles = {
    'key_style': 'white bold',
    'bullet_style': 'green',
    'colon_style': 'green',
}


def _print_title(title: str) -> None:
    import rich

    rich.print('[bold green]' + title + '[/bold green]')


def _dataset_to_str(dataset: truck.TrackedTable) -> str:
    return dataset['source_name'] + '.' + dataset['table_name']


def _print_dataset_bullet(dataset: truck.TrackedTable) -> None:
    import toolstr

    toolstr.print_bullet(
        '[white bold]' + _dataset_to_str(dataset) + '[/white bold]',
        **bullet_styles,
    )


def _print_source_datasets_bullet(
    source: str, datasets: list[type[truck.Table]]
) -> None:
    import toolstr

    names = [cls.class_name(snake=True) for cls in datasets]
    toolstr.print_bullet(
        key=source,
        value='[green],[/green] '.join(names),
        **bullet_styles,
    )


def _format_range(data_range: typing.Any) -> str:
    if isinstance(data_range, list):
        date_strs = [
            '-' if dt is None else dt.strftime('%Y-%m-%d') for dt in data_range
        ]
        return '\[' + ', '.join(date_strs) + ']'
    else:
        return str(data_range)
