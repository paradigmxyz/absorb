from __future__ import annotations

import typing


class Context:
    def __init__(
        self,
        datatype: type[Datatype],
        parameter_values: dict[str, typing.Any],
        collection_range: dict[str, typing.Any],
    ):
        self._datatype = datatype
        self._parameter_values = parameter_values
        self._collection_range = collection_range

    @classmethod
    def get_parameter(cls, name: str) -> typing.Any:
        value = self._parameter_values.get(name)
        if value is None:
            import types

            valuetype = self.datatype.parameters[name]
            if isinstance(valuetype, types.GenericAlias):
                valuetype = valuetype.__origin__
            value = valuetype()
        return value

    @classmethod
    def get_range(cls) -> dict[str, typing.Any]:
        return self._collection_range
