from __future__ import annotations

import typing

import absorb
from .. import cli_outputs

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def ls_command(args: Namespace) -> dict[str, Any]:
    import os
    import toolstr

    # decide which sections to print
    sections = set()
    if not args.available and not args.tracked and not args.untracked_collected:
        sections.add('available')
        sections.add('tracked')
        sections.add('untracked_collected')
    if args.available:
        sections.add('available')
    if args.tracked:
        sections.add('tracked')
    if args.untracked_collected:
        sections.add('untracked_collected')

    # available datasets
    if 'available' in sections:
        cli_outputs._print_title('Available datasets')
        if args.source is not None:
            sources = [args.source]
        else:
            sources = absorb.ops.get_sources()
        for source in sorted(sources):
            table_classes = absorb.ops.get_source_table_classes(source)
            if args.one_per_line:
                for dataset in table_classes:
                    toolstr.print_bullet(
                        '[white bold]'
                        + dataset.source
                        + '.'
                        + dataset.name_classmethod(allow_generic=True)
                        + '[/white bold]',
                        **absorb.ops.bullet_styles,
                    )
            else:
                if len(table_classes) > 0:
                    names = [
                        cls.name_classmethod(allow_generic=True)
                        for cls in table_classes
                    ]

                    if args.verbose >= 1:
                        max_width = os.get_terminal_size().columns
                        without_formatting = (
                            '- ' + source + ': ' + ', '.join(names)
                        )
                        if len(without_formatting) > max_width:
                            with_cutoff = without_formatting[: max_width - 5]
                            head, tail = with_cutoff.split(': ')
                            entries = tail.split(', ')[:-1]
                            if len(entries) >= 1:
                                last_entry = entries[-1]
                                names = names[: names.index(last_entry) + 1]
                            else:
                                names = []
                            suffix = '[green],[/green] ...'
                        else:
                            suffix = ''
                    else:
                        suffix = ''

                    toolstr.print_bullet(
                        key=source,
                        value='[green],[/green] '.join(names) + suffix,
                        **absorb.ops.bullet_styles,
                    )

    # get tracked datasets
    tracked_datasets = absorb.ops.get_tracked_tables()
    if args.source is not None:
        tracked_datasets = [
            dataset
            for dataset in tracked_datasets
            if dataset['source_name'] == args.source
        ]
    tracked_datasets = sorted(
        tracked_datasets, key=lambda x: (x['source_name'], x['table_name'])
    )

    # print tracked datasets
    if 'tracked' in sections:
        if 'available' in sections:
            print()
        _print_tracked_datasets(
            tracked_datasets,
            verbose=args.verbose,
            one_per_line=args.one_per_line,
        )

    # get untracked collected datasets
    if 'untracked_collected' in sections:
        _print_untracked_datasets(
            tracked_datasets=tracked_datasets,
            source=args.source,
            verbose=args.verbose,
            one_per_line=args.one_per_line,
            skip_line='tracked' in sections or 'available' in sections,
        )

    return {}


def _print_tracked_datasets(
    tracked_datasets: list[absorb.TableDict],
    verbose: int = 1,
    one_per_line: bool = False,
) -> None:
    cli_outputs._print_title(
        'Tracked datasets (n = {})'.format(len(tracked_datasets))
    )
    if len(tracked_datasets) == 0:
        print('[none]')
    else:
        _print_datasets(
            tracked_datasets, verbose=verbose, one_per_line=one_per_line
        )


def _print_untracked_datasets(
    tracked_datasets: list[absorb.TableDict],
    source: str | None = None,
    skip_line: bool = False,
    verbose: int = 1,
    one_per_line: bool = False,
) -> None:
    untracked_collected_datasets = absorb.ops.get_untracked_collected_tables(
        tracked_datasets=tracked_datasets
    )
    if source is not None:
        untracked_collected_datasets = [
            dataset
            for dataset in untracked_collected_datasets
            if dataset['source_name'] == source
        ]
    untracked_collected_datasets = sorted(
        untracked_collected_datasets,
        key=lambda x: (x['source_name'], x['table_name']),
    )

    # print untracked collected datasets
    if len(untracked_collected_datasets) > 0:
        if skip_line:
            print()
        cli_outputs._print_title(
            'Untracked collected datasets (n = {})'.format(
                len(untracked_collected_datasets)
            )
        )
        _print_datasets(
            untracked_collected_datasets,
            verbose=verbose,
            one_per_line=one_per_line,
        )


def _print_datasets(
    datasets: list[absorb.TableDict], verbose: int, one_per_line: bool
) -> None:
    import toolstr

    if verbose >= 1:
        _print_datasets_verbose(datasets)
    elif one_per_line:
        for dataset in datasets:
            cli_outputs._print_dataset_bullet(dataset)
    else:
        tracked_sources = sorted(
            {dataset['source_name'] for dataset in datasets}
        )
        for source in tracked_sources:
            names = []
            for dataset in datasets:
                if dataset['source_name'] == source:
                    try:
                        instance = absorb.Table.instantiate(dataset)
                        name = instance.name()
                    except Exception:
                        name = (
                            dataset['source_name'] + '.' + dataset['table_name']
                        )
                    names.append(name)

            toolstr.print_bullet(
                key=source,
                value='[green],[/green] '.join(names),
                **absorb.ops.bullet_styles,
            )


def _print_datasets_verbose(datasets: list[absorb.TableDict]) -> None:
    import toolstr

    rows = []
    for dataset in datasets:
        instance = absorb.Table.instantiate(dataset)
        available_range = instance.get_available_range()
        available_range_str = absorb.ops.format_coverage(
            available_range, instance.get_chunk_size()
        )
        collected_range = instance.get_collected_range()
        if collected_range is not None:
            collected_range_str = absorb.ops.format_coverage(
                collected_range, instance.get_chunk_size()
            )
        else:
            collected_range_str = '-'
        row = [
            dataset['source_name'],
            dataset['table_name'],
            available_range_str + '\n' + collected_range_str,
        ]
        rows.append(row)
    columns = ['source', 'table', 'available range\ncollected range']
    toolstr.print_multiline_table(rows, labels=columns)
