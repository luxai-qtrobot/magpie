
from dataclasses import dataclass, field, fields
from typing import ClassVar, Dict, Type, TypeVar
from luxai.magpie.utils.common import get_utc_timestamp, get_uinque_id

F = TypeVar("F", bound="Frame")

# NOTE: in python < 3.10, dataclass does not support 'kw_only'
#       therefore, all inherited class's attributes must have default value
#       because the Frame class has fields with default value.   
@dataclass
class Frame:    

    # --- class-level registry of all subclasses ---
    _registry: ClassVar[Dict[str, Type["Frame"]]] = {}

    gid: int = field(default=None)
    id: int = field(default=None)
    name: str = field(init=False)
    timestamp: str = field(init=False)

    def __post_init__(self):  
        self.gid = self.gid if self.gid else get_uinque_id()
        self.id = self.id if self.id else 0
        self.name = self.__class__.__name__
        self.timestamp = get_utc_timestamp()

    # --- automatic registration of subclasses ---
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # avoid registering the base class itself
        if cls is not Frame:
            Frame._registry[cls.__name__] = cls
    
    def __str__(self):
        return f"{self.name}#{self.gid}:{self.id}"

    def to_dict(frame) -> dict:
        # Use fields from the dataclass to dynamically build the dictionary
        frame_dict = {}
        for f in fields(frame):            
            frame_dict[f.name] = getattr(frame, f.name)
        return frame_dict

    @classmethod
    def from_dict(cls, data):
        # case 1: called on Frame → try dispatch, otherwise fallback to plain Frame
        if cls is Frame:
            frame_type = data.get("name")
            # if name corresponds to a registered subclass → dispatch
            if frame_type is not None and isinstance(frame_type, str) and frame_type in cls._registry:
                subcls = cls._registry[frame_type]
                return subcls.from_dict(data)

        # case 2: called on subclass for regular init
        field_names = {f.name for f in fields(cls) if f.name not in ['name', 'timestamp']}
        init_args = {k: v for k, v in data.items() if k in field_names}
        return cls(**init_args)
    