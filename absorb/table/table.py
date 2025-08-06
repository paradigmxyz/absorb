from __future__ import annotations

import typing

import absorb
from . import table_collect
from . import table_create


class Table(
    table_collect.TableCollect,
    table_create.TableCreate,
):
    #
    # # ops wrapper functions
    #

    def add(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """same args as absorb.ops.add"""
        return absorb.ops.add(self, *args, **kwargs)

    def remove(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """same args as absorb.ops.remove"""
        return absorb.ops.remove(self, *args, **kwargs)

    def upload(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """same args as absorb.ops.upload"""
        return absorb.ops.upload(self, *args, **kwargs)

    def download(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """same args as absorb.ops.download"""
        return absorb.ops.download(self, *args, **kwargs)

    def query(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """same args as absorb.ops.query"""
        return absorb.ops.query(self, *args, **kwargs)

    def preview(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """same args as absorb.ops.preview"""
        return absorb.ops.preview(self, *args, **kwargs)

    def print_info(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """same args as absorb.ops.print_info"""
        return absorb.ops.print_table_info(self, *args, **kwargs)
