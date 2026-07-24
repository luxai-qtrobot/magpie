import argparse
import os, sys
import time
from time import perf_counter
from collections import deque

try:
    import cv2
    import numpy as np
except ImportError:
    from luxai.magpie.utils import Logger
    Logger.error(
        "Could not import required video dependencies. Please install with:\n"
        "  pip install \"luxai-magpie[video]\""
    )
    sys.exit(1)



from luxai.magpie.utils import Logger
from luxai.magpie.nodes import SinkNode
from luxai.magpie.transport import ZmqStreamReader
from luxai.magpie.frames import Frame, ImageFrameCV, ImageFrameJpeg, ImageFrameRaw


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

        # Let the Frame factory pick the right subclass
        try:
            frame = Frame.from_dict(data)            
        except Exception as e:
            Logger.warning(f"{self.name} failed to deserialize frame: {e}")
            return

        # Accept only the image frame types we know how to render.
        # Check the more specific subclasses before the generic ImageFrameRaw
        # fallback, since ImageFrameJpeg/ImageFrameCV are themselves ImageFrameRaw
        # subclasses and would otherwise always match the raw-pixel branch first.
        if isinstance(frame, ImageFrameJpeg):
            # Decode to BGR so OpenCV can display it directly
            image = frame.to_np_image(pixel_format="BGR")
        elif isinstance(frame, ImageFrameCV):
            image = frame.to_cv_image()
        elif isinstance(frame, ImageFrameRaw):
            image = np.frombuffer(frame.data, np.uint8).reshape(frame.height, frame.width, frame.channels)
        else:
            Logger.warning(f"{self.name} received unsupported frame type: {getattr(frame, 'name', type(frame).__name__)}")
            return

        # add info 
        if self.show_statistics and self.prev_time:
            fps = 1.0 / (perf_counter() - self.prev_time)
            self.fps_values.append(fps)
            avg_fps = int(sum(self.fps_values) / len(self.fps_values))
            image = image.copy()
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
    
    parser.add_argument(
        "--bind",
        action="store_true",
        help="Bind the reader socket instead of connecting (default: connect).",
    )

    args = parser.parse_args()
    node = ZmqVideoViewer(name='MagpieVideoViewer',
                          stream_reader=ZmqStreamReader(
                          endpoint=args.endpoint,
                          topic=args.topic,
                          bind=args.bind,
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
