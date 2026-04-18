def get_from_dot_path(obj: dict, dot_path: str, default=None, white_spaces_as_empty=True):
    """Get a value from a dict using a dot path, returning a default value if not found.
    If any step of the path is not a dict, it will return the default value.
    If white_spaces_as_empty is True, it will assume that a white space is an empty value.
    """
    keys = dot_path.split(".")
    for key in keys:
        if not isinstance(obj, dict) or key not in obj:
            return default
        obj = obj[key]
    if white_spaces_as_empty and isinstance(obj, str) and not obj.strip():
        return default
    return obj
