from __future__ import annotations

import typing

import absorb
from .. import cli_outputs

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def ls_command(args: Namespace) -> dict[str, Any]:
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
            source_tables = absorb.ops.get_source_tables(source)
            if args.one_per_line:
                for dataset in source_tables:
                    toolstr.print_bullet(
                        '[white bold]'
                        + dataset.source
                        + '.'
                        + dataset.class_name(allow_generic=True)
                        + '[/white bold]',
                        **absorb.ops.bullet_styles,
                    )
            else:
                if len(source_tables) > 0:
                    names = [
                        cls.class_name(allow_generic=True)
                        for cls in source_tables
                    ]
                    toolstr.print_bullet(
                        key=source,
                        value='[green],[/green] '.join(names),
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
        cli_outputs._print_title(
            'Tracked datasets (n = {})'.format(len(tracked_datasets))
        )
        if len(tracked_datasets) == 0:
            print('[none]')
        else:
            _print_datasets(tracked_datasets, args)

    # get untracked collected datasets
    if 'untracked_collected' in sections:
        untracked_collected_datasets = (
            absorb.ops.get_untracked_collected_tables(
                tracked_datasets=tracked_datasets
            )
        )
        if args.source is not None:
            untracked_collected_datasets = [
                dataset
                for dataset in untracked_collected_datasets
                if dataset['source_name'] == args.source
            ]
        untracked_collected_datasets = sorted(
            untracked_collected_datasets,
            key=lambda x: (x['source_name'], x['table_name']),
        )

        # print untracked collected datasets
        if len(untracked_collected_datasets) > 0:
            if 'tracked' in sections or 'available' in sections:
                print()
            cli_outputs._print_title(
                'Untracked collected datasets (n = {})'.format(
                    len(untracked_collected_datasets)
                )
            )
            _print_datasets(untracked_collected_datasets, args)

    return {}


def _print_datasets(
    datasets: list[absorb.TrackedTable], args: Namespace
) -> None:
    import toolstr

    if args.verbose:
        _print_datasets_verbose(datasets)
    elif args.one_per_line:
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
                        name = instance.class_name(allow_generic=True)
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


def _print_datasets_verbose(datasets: list[absorb.TrackedTable]) -> None:
    import toolstr

    rows = []
    for dataset in datasets:
        instance = absorb.Table.instantiate(dataset)
        available_range = instance.get_available_range()
        available_range_str = absorb.ops.format_coverage(
            available_range, instance.index_type
        )
        collected_range = instance.get_collected_range()
        if collected_range is not None:
            collected_range_str = absorb.ops.format_coverage(
                collected_range, instance.index_type
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
