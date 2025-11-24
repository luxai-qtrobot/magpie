
import argparse
import os, sys
import time
import cv2

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.utils.logger import Logger
from magpie.nodes.source_node import SourceNode
from magpie.transport.zmq.zmq_publisher import ZMQPublisher
from magpie.frames.image import ImageFrameCV, ImageFrameJpeg


class ZmqVideoCapture(SourceNode):

    def setup(self, camera=0, frame_rate=30, size=(640, 480), encoder='jpeg'):
        # Initialize camera capture        
        self.encoder = encoder
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
        Logger.info(f"{self.name} streaming video on {self.stream_writer.endpoint} using {self.encoder} encoding.")    

    def process(self):        
        ret, image = self.cap.read()

        # Select encoding method
        if self.encoder == "cv":
            frame = ImageFrameCV.from_cv_image(image)
        else:  # turbojpeg
            frame = ImageFrameJpeg.from_np_image(image, quality=80, pixel_format="BGR")

        self.stream_writer.write(frame.to_dict())
                


if __name__ == '__main__':
    Logger.set_level('DEBUG')

    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", 
                        help="ZeroMQ publishing socket endpoint (e.g. tcp://*:5555)",
                        default="tcp://*:5555",                        
                        type=str)
    parser.add_argument("-c", "--camera", 
                        help="opencv capturing camera id (e.g. 0)",
                        default=0,       
                        type=int)

    parser.add_argument("-f", "--framerate", 
                        help="opencv capturing frame rate (e.g. 30)",
                        default=30,       
                        type=int)

    parser.add_argument("-s", "--size",
                        help="Frame size: width height (e.g. 1280 720)",
                        nargs=2,
                        type=int,
                        default=[1280, 720])

    parser.add_argument("--encoder",
                        choices=["cv", "jpeg"],
                        default="cv",
                        help="Encoding backend: 'cv' for ImageFrameCV, 'jpeg' for ImageFrameJpeg")

    args = parser.parse_args()

    node = ZmqVideoCapture(name='VideoCapture',
                            stream_writer=ZMQPublisher(args.address, queue_size=0, delivery="latest"),
                            setup_kwargs={
                                'camera': args.camera,
                                'size':  tuple(args.size),
                                'frame_rate': args.framerate,
                                'encoder': args.encoder,})
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break
    Logger.info("Closing...")
    node.terminate()


