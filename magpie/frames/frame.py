
from dataclasses import dataclass, field, fields
from ulid import ULID

# TODO: check if having 'name' is useful
# NOTE: in python < 3.10, dataclass does not support 'kw_only'
#       therefore, all inherited class's attributes must have default value
#       because the Frame class has fields with default value.   
@dataclass
class Frame:    
    gid: int = field(default=None)
    id: int = field(default=None)
    name: str = field(init=False)

    def __post_init__(self):  
        self.gid = self.gid if self.gid else str(ULID())  # TODO: check if it's better to use ULID().bytes. 
        self.id = self.id if self.id else 0
        self.name = self.__class__.__name__

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
        field_names = {f.name for f in fields(cls) if f.name != 'name'}
        init_args = {key: value for key, value in data.items() if key in field_names}
        return cls(**init_args)