class DocDefaultException(Exception):
    """Subclass exceptions use docstring as default message"""

    def __init__(self, msg=None, *args, **kwargs):
        super().__init__(msg or self.__doc__, *args, **kwargs)
