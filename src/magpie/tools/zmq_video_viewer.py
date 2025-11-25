import argparse
import os, sys
import time
import cv2
import numpy as np
from time import perf_counter
from collections import deque



from magpie.utils.logger import Logger
from magpie.nodes.sink_node import SinkNode
from magpie.transport.zmq.zmq_subscriber import ZMQSubscriber
from magpie.frames.image import ImageFrameCV


class ZmqVideoViewer(SinkNode):

    def setup(self, show_statistics=False):
        self.show_statistics = show_statistics
        self.prev_time = None
        self.fps_values = deque(maxlen=10)     
        Logger.info(f"{self.name} showing video from {self.stream_reader.endpoint}")


    def process(self):
        _data = self.stream_reader.read()
        if _data is None:
            return

        data, topic = _data
        frame_type = data.get('name')        
        if not frame_type or frame_type not in ['ImageFrameCV', 'ImageFrameJpeg']:
            Logger.warning(f"{self.name} received unsupported frame type: {frame_type}")
            return

        if frame_type == 'ImageFrameCV':
            frame = ImageFrameCV.from_dict(data)
            image = frame.to_cv_image()
        else:  # ImageFrameJpeg
            from magpie.frames.image import ImageFrameJpeg
            frame = ImageFrameJpeg.from_dict(data)
            # Decode to BGR so OpenCV can display it directly
            image = frame.to_np_image(pixel_format="BGR")

        # add info 
        if self.show_statistics and self.prev_time:
            fps = 1.0 / (perf_counter() - self.prev_time)
            self.fps_values.append(fps)
            avg_fps = int(sum(self.fps_values) / len(self.fps_values))
            height, width, _ = image.shape
            position = (10, height - 10)
            cv2.putText(
                image,
                f"[{frame.timestamp}] {width}x{height} {avg_fps}fps",
                position,
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                (112, 82, 204),
                1,
                cv2.LINE_AA,
            )

        self.prev_time = perf_counter()

        # Display the image
        cv2.imshow(self.name, image)
        cv2.waitKey(1)


def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint", 
                        help="ZeroMQ subscribing socket endpoint (e.g. tcp://127.0.0.1:5555)",
                        type=str)
    parser.add_argument(
        "topic",
        help="ZeroMQ subscribing topic on endpoint (e.g. /mytopic)",
        type=str,
    )
    parser.add_argument("-v", "--verbose", 
                        help="show verbose information on video viewer",
                        action="store_true")
    
    args = parser.parse_args()
    node = ZmqVideoViewer(name='MagpieVideoViewer', 
                          stream_reader=ZMQSubscriber(
                          endpoint=args.endpoint,
                          topic=args.topic,
                          bind=False,
                          queue_size=1,                           
                          delivery="latest"),
                          setup_kwargs={'show_statistics': args.verbose})
    
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break
    Logger.info("Closing...")
    node.terminate()        


if __name__ == "__main__":
    main()  
