from __future__ import annotations


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
