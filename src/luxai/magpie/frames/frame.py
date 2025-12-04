
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
        self.timestamp = str(get_utc_timestamp())

    # --- automatic registration of subclasses ---
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # avoid registering the base class itself
        if cls is not Frame:
            Frame._registry[cls.__name__] = cls
    
    def __str__(self):
        return f"{self.name}#{self.gid}:{self.id}"

    def to_dict(frame) -> dict:
        """
        Serialize this Frame into the standard wire-format dictionary.

        This method is NOT a generic Python object to dict converter. It produces
        a strictly defined frame envelope used by Magpie for transport over ZMQ or
        any other communication layer.

        The resulting dict always contains:
            {
                "gid": <str>,        # globally unique frame identifier
                "id": <int>,         # sequence number or user-set ID
                "name": <str>,       # frame type name (used for dispatch)
                "timestamp": <float>,# creation or receive time
                 <subclass-specific payload fields>
            }

        Subclasses (e.g. DictFrame, ImageFrameCV) override/extend the payload stored
        in "value", but the frame envelope remains the same.

        Returns:
            dict: A wire-format frame dictionary suitable for serialization and
                transmission. NOT intended to represent arbitrary user data.
        """        
        # Use fields from the dataclass to dynamically build the dictionary
        frame_dict = {}
        for f in fields(frame):            
            frame_dict[f.name] = getattr(frame, f.name)
        return frame_dict

    @classmethod
    def from_dict(cls, data):
        """
        Deserialize a frame dictionary produced by Frame.to_dict() and reconstruct
        the appropriate Frame subclass.

        This method is NOT a generic dict to Frame parser. It expects a dictionary
        that follows the Magpie wire-format schema:

            {
                "gid": ...,
                "id": ...,
                "name": "DictFrame" | "ImageFrameCV" | ...,
                "timestamp": ...,
                <subclass-specific payload fields>
            }

        Dispatch logic:
            - When called as Frame.from_dict(...):
                Uses the "name" field to select and call the registered subclass
                (e.g., DictFrame.from_dict).
                If no subclass matches, a plain Frame is returned containing only
                metadata (no payload).

            - When called on a subclass (e.g., DictFrame.from_dict):
                Builds the object using the subclass's declared dataclass fields.

        Important:
            Passing an arbitrary dictionary (e.g. {"a": 1, "b": 2}) will NOT embed
            that data into the Frame, because Frame itself has no payload fields.
            Only frame-formatted dicts can reconstruct payload-carrying subclasses.

        Args:
            data (dict): A wire-format frame dictionary created by to_dict().

        Returns:
            Frame: An instance of Frame or a registered Frame subclass.
        """        
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
    