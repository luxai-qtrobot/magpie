
from dataclasses import dataclass, field, fields
from ulid import ULID

@dataclass
class Frame:    
    gid: int = None
    id: int = None       
    name: str = None

    def __post_init__(self):  
        self.gid = self.gid if self.gid else str(ULID())  # TODO: check if it's better to use ULID().bytes. 
        self.id = self.id if self.id else 0
        self.name = self.name if self.name else self.__class__.__name__

    def __str__(self):
        return f"{self.name}#{self.gid}:{self.id}"

    def to_dict(frame) -> dict:
        # Use fields from the dataclass to dynamically build the dictionary
        frame_dict = {}
        for f in fields(frame):            
            frame_dict[f.name] = getattr(frame, f.name)
        return frame_dict

    @classmethod
    def from_dict(cls, data: dict):
        # Extract the fields except 'name', which will be set in __post_init__
        field_names = {f.name for f in fields(cls)}
        init_args = {key: value for key, value in data.items() if key in field_names}
        return cls(**init_args)