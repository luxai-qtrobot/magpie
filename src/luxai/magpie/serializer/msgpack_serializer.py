import sys
from luxai.magpie.utils.logger import Logger
from .base_serializer import BaseSerializer

try:
    import msgpack
except ImportError as e:
    Logger.error(f"Could not import msgpack. Please install it using 'pip install msgpack'.")
    sys.exit()

class MsgpackSerializer(BaseSerializer):
    """
    MsgpackSerializer class.
    
    This class provides serialization and deserialization methods using the
    MessagePack (msgpack) format. It implements the BaseSerializer interface.
    MessagePack is an efficient binary serialization format that allows data to be
    serialized and deserialized quickly, with a smaller footprint compared to other
    serialization formats like JSON.
    """
 
    def serialize(self, data: object) -> bytes:
        """
        Serializes a Python object into MessagePack byte format.

        Args:
            data (object): The data object to be serialized.

        Returns:
            bytes: The serialized data in MessagePack byte format.
        """
        return msgpack.packb(data)
 
    def deserialize(self, byte_data: bytes) -> object:
        """
        Deserializes MessagePack byte data back into a Python object.

        Args:
            byte_data (bytes): The byte data to be deserialized.

        Returns:
            object: The deserialized data object.
        """
        return msgpack.unpackb(byte_data)
