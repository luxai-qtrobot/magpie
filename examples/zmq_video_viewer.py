import os, sys
import time
import cv2
import numpy as np
from time import perf_counter
from collections import deque

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.utils.logger import Logger
from magpie.nodes.sink_node import SinkNode
from magpie.transport.zmq.zmq_subscriber import ZMQSubscriber
from magpie.frames.image import ImageFrameCV


class ZmqVideoViewer(SinkNode):

    def setup(self, show_statistics=False):
        self.show_statistics = show_statistics
        self.prev_time = None
        self.fps_values = deque(maxlen=10)        

    def process(self):        
        data = self.stream_reader.read()
        if not data: return
        frame =  ImageFrameCV.from_dict(data)
        image = frame.to_cv_image()
        # add info 
        if self.show_statistics and self.prev_time:
            fps = 1.0 / (perf_counter() - self.prev_time)
            self.fps_values.append(fps)
            avg_fps = int(sum(self.fps_values) / len(self.fps_values))
            height, width, _ = image.shape
            position = position = (10, height - 10)
            cv2.putText(image, f"[{frame.timestamp}] {width}x{height} {avg_fps}fps", position, cv2.FONT_HERSHEY_SIMPLEX, 0.5, (112,82,204), 1, cv2.LINE_AA)
        self.prev_time = perf_counter()
        # Display the image
        cv2.imshow(self.name, image)
        cv2.waitKey(1)

if __name__ == '__main__':

    node = ZmqVideoViewer(name='VideoViewer', 
                           stream_reader=ZMQSubscriber("tcp://127.0.0.1:5555"),
                           setup_kwargs={'show_statistics': True})    
    
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break
    Logger.info("Closing...")
    node.terminate()        