from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import datetime


# def get_min_local_timestamp(
#     dataset: absorb.TableReference, context: absorb.Context | None = None
# ) -> datetime.datetime:
#     return absorb.resolve_table_class(dataset).get_min_local_timestamp(context)


# def get_max_local_timestamp(
#     dataset: absorb.TableReference, context: absorb.Context | None = None
# ) -> datetime.datetime:
#     return absorb.resolve_table_class(dataset).get_max_local_timestamp(context)


# def get_min_available_timestamp(
#     dataset: absorb.TableReference, context: absorb.Context | None = None
# ) -> datetime.datetime:
#     return absorb.resolve_table_class(dataset).get_min_available_timestamp(
#         context
#     )


# def get_max_available_timestamp(
#     dataset: absorb.TableReference, context: absorb.Context | None = None
# ) -> datetime.datetime:
#     return absorb.resolve_table_class(dataset).get_max_available_timestamp(
#         context
#     )
