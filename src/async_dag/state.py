from enum import IntEnum, auto


class State(IntEnum):
    UNDISCOVERED = auto()
    TEMPORARY = auto()
    PERMANENT = auto()
