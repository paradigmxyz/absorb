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


def is_valid_name(name: str) -> bool:
    import re

    if '__' in name or name[0] == '_' or name[-1] == '_':
        return False
    return bool(re.match(r'^[a-zA-Z0-9_]+$', name))


def get_table_name(
    *,
    class_name: str,
    template: str | list[str],
    parameters: dict[str, typing.Any] | None = None,
    allow_generic: bool = False,
) -> str:
    # get template str
    if isinstance(template, list):
        if len(template) == 1:
            template = template[0]
        else:
            for subtemplate in template:
                # get variable names from template (in curly braces)
                template_parameters = _get_template_variables(subtemplate)
                if all(
                    parameters is not None and parameters.get(var) is not None
                    for var in template_parameters
                ):
                    template = subtemplate
                    break
            else:
                raise ValueError(
                    'could not determine template string from list'
                )
    if not isinstance(template, str):
        raise TypeError('template must be a string or a list of strings')

    # assemble the template variables
    if class_name.isupper():
        class_name = class_name.lower()
    else:
        class_name = absorb.ops.names._camel_to_snake(class_name)
    template_vars = {'class_name': class_name}
    if parameters:
        template_vars.update(parameters)

    # whether to allow generic template variables in the output
    if allow_generic:
        result: str = template
        for key, value in template_vars.items():
            result = result.replace(f'{{{key}}}', str(value))
        return result
    else:
        return template.format(**template_vars)


def _get_template_variables(template: str) -> list[str]:
    """
    Extracts variable names from a template string.
    Variables are expected to be in curly braces, e.g., {variable_name}.
    """
    variables = []
    in_variable = False
    current_var: list[str] = []

    for char in template:
        if char == '{':
            in_variable = True
            current_var = []
        elif char == '}':
            if in_variable:
                variables.append(''.join(current_var))
                in_variable = False
        elif in_variable:
            current_var.append(char)

    return variables
