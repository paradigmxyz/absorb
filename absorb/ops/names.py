from __future__ import annotations

import typing
import absorb


def _camel_to_snake(name: str) -> str:
    result = []
    for i, char in enumerate(name):
        if char.isupper():
            if i != 0:
                result.append('_')
            result.append(char.lower())
        else:
            result.append(char)
    return ''.join(result)


def _snake_to_camel(name: str) -> str:
    result = []
    capitalize_next = False

    for i, char in enumerate(name):
        if char == '_':
            capitalize_next = True
        elif capitalize_next:
            result.append(char.upper())
            capitalize_next = False
        else:
            result.append(char)

    return result[0].upper() + ''.join(result[1:])


def get_table_name(
    *,
    base_name: str,
    name_template: absorb.NameTemplate,
    parameter_types: dict[str, typing.Any],
    parameters: dict[str, typing.Any],
    default_parameters: dict[str, typing.Any],
    static_parameters: list[str],
    allow_generic: bool = False,
) -> str:
    """
    keys
    - base_name: the name of the table datatype
    - {parameter_name}: the name of any of the dataset parameters
    """
    if name_template.get('default') is None:
        pass

    if base_name.isupper():
        base_name = base_name.lower()
    else:
        base_name = absorb.ops.names._camel_to_snake(base_name)

    if len(parameter_types) == 0:
        if len(name_template) == 0:
            return base_name
        elif name_template.get('default') is not None:
            template = name_template['default']
        elif name_template.get('custom') is not None:
            if isinstance(name_template['custom'], str):
                template = name_template['custom']
            else:
                raise Exception()
        else:
            raise Exception()
    else:
        parameters_are_default = all(
            parameters.get(key) == default_parameters.get(key)
            for key in parameter_types
        )
        if name_template.get('default') is not None and parameters_are_default:
            template = name_template['default']
        elif name_template.get('custom') is not None:
            custom_template = name_template['custom']
            if isinstance(custom_template, str):
                template = custom_template
            elif isinstance(custom_template, dict):
                for key, value in custom_template.items():
                    if isinstance(key, str):
                        variables: str | tuple[str] = (key,)
                    elif isinstance(key, tuple):
                        variables = key
                    else:
                        raise Exception()
                    if all(variable is not None for variable in variables):
                        template = value
                        break
                else:
                    raise Exception()
            else:
                raise Exception()
        elif len(name_template) == 0 and parameters_are_default:
            return base_name
        else:
            raise Exception('need parameters specified')

    if allow_generic:
        template_items = dict(base_name=base_name, **parameters)
        result = template
        for key, value in template_items.items():
            result = result.replace('{' + key + '}', str(value))
        return result
    else:
        return template.format(base_name=base_name, **parameters)
