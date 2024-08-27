
import os, sys
import time
import cv2

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.utils.logger import Logger
from magpie.nodes.source_node import SourceNode
from magpie.transport.zmq.zmq_publisher import ZMQPublisher
from magpie.frames.image import ImageFrameCV


class ZmqVideoCapture(SourceNode):

    def setup(self, camera=0, frame_rate=30, size=(640, 480)):
        # Initialize camera capture        
        self.cap = cv2.VideoCapture(camera)
                
        if frame_rate > 0:
            self.cap.set(cv2.CAP_PROP_FPS, frame_rate)        
        w, h = size
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, w)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, h)
        actual_w = self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        actual_h = self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
        actual_fps = self.cap.get(cv2.CAP_PROP_FPS)
        Logger.info(f"{self.name} initilized with size=({actual_w}, {actual_h}) and fps={actual_fps}")

    def process(self):
        ret, image = self.cap.read()
        frame =  ImageFrameCV.from_cv_image(image)
        self.stream_writer.write(frame.to_dict())
        


if __name__ == '__main__':

    node = ZmqVideoCapture(name='VideoCapture', 
                            stream_writer=ZMQPublisher("tcp://*:5555"),
                            setup_kwargs={'camera': 0, 'size': (1280, 720)})
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break
    Logger.info("Closing...")
    node.terminate()
        