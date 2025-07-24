from __future__ import annotations

import typing

import absorb


def parse_table_str(
    ref: str,
    *,
    parameters: dict[str, typing.Any] | None = None,
    raw_parameters: dict[str, str] | None = None,
    use_all_parameters: bool = True,
    allow_generic: bool = False,
) -> tuple[type[absorb.Table], dict[str, typing.Any]]:
    """
    Parses a table string of the form 'source.table_name' and returns the class
    and parameters.
    """
    source, table_name = ref.split('.')

    for cls in absorb.ops.get_source_table_classes(source):
        try:
            name_parameters = cls.parse_name_parameters(table_name)
            for key, value in name_parameters.items():
                if '{' + key + '}' == value and not allow_generic:
                    raise absorb.NameParseError(
                        f'Generic parameter {key} not allowed in {ref}'
                    )
            break
        except absorb.NameParseError:
            continue
    else:
        raise Exception('Could not find table class for: ' + ref)

    # parse input parameters
    if parameters is None:
        parameters = {}
    if raw_parameters is not None:
        parameters.update(
            convert_raw_parameter_types(raw_parameters, cls.parameter_types)
        )

    # merge name parameters into input parameters
    parameters = dict(parameters, **name_parameters)

    # only use subset relevant to Table class
    for key, value in list(parameters.items()):
        if key not in cls.parameter_types:
            if not use_all_parameters:
                del parameters[key]
            else:
                raise Exception(
                    'Invalid parameter ' + key + ' for ' + cls.full_class_name()
                )

    return cls, parameters


def parse_string_from_template(template: str, string: str) -> dict[str, str]:
    import re

    # Find all placeholders in the template
    placeholder_pattern = r'\{([^}]+)\}'
    placeholders = re.findall(placeholder_pattern, template)

    # Escape special regex characters in the template, but preserve placeholders
    regex_pattern = re.escape(template)

    # Replace escaped placeholders with capture groups
    # Use non-greedy matching by default to handle multiple placeholders better
    for placeholder in placeholders:
        escaped_placeholder = re.escape('{' + placeholder + '}')
        # Create a named capture group for each placeholder
        # Use a non-greedy match that stops at the next literal character
        regex_pattern = regex_pattern.replace(
            escaped_placeholder, f'(?P<{placeholder}>.*?)'
        )

    # For the last placeholder, we might want greedy matching
    # Adjust the pattern to make the last group greedy if it's at the end
    if regex_pattern.endswith('.*?)'):
        regex_pattern = regex_pattern[:-4] + '.*)'

    # Add anchors to ensure full string match
    regex_pattern = f'^{regex_pattern}$'

    try:
        match = re.match(regex_pattern, string)
        if not match:
            raise absorb.NameParseError(
                f"String '{string}' does not match template '{template}'"
            )

        return match.groupdict()
    except re.error as e:
        raise absorb.NameParseError(f'Invalid regex pattern: {e}')


def convert_raw_parameter_types(
    raw_parameters: dict[str, str],
    parameter_types: dict[str, type | tuple[type, ...]],
) -> dict[str, typing.Any]:
    parameters = {}
    for key, value in raw_parameters.items():
        if key == 'class_name':
            parameter_type: type | tuple[type, ...] = str
        else:
            parameter_type = parameter_types[key]
        if parameter_type == str:  # noqa: E721
            converted: typing.Any = value
        elif parameter_type == int:  # noqa: E721
            converted = int(value)
        elif parameter_type == list[str]:
            converted = value.split(',')
        elif parameter_type == list[int]:
            converted = [int(subvalue) for subvalue in value.split(',')]
        else:
            raise Exception('invalid parameter type: ' + str(parameter_type))
        parameters[key] = converted
    return parameters
