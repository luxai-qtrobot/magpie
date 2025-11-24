from abc import ABC, abstractmethod

class BaseSerializer(ABC):
    """
    BaseSerializer class.
    
    This abstract base class defines the interface for serializers.
    Serializers are responsible for converting objects to a byte format
    and vice versa, allowing for data storage, transmission, or any other
    operation that requires data to be in a byte format.
    """
 
    @abstractmethod
    def serialize(self, data: object) -> bytes:
        """
        Serializes an object into bytes.
        Must be implemented by subclasses.

        Args:
            data (object): The data object to be serialized.

        Returns:
            bytes: The serialized data in byte format.

        """
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, byte_data: bytes) -> object:
        """
        Deserializes bytes back into an object.
        Must be implemented by subclasses.
        
        Args:
            byte_data (bytes): The byte data to be deserialized.

        Returns:
            object: The deserialized data object.

        """
        raise NotImplementedError
