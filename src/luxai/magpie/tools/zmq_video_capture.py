
import argparse
import os, sys
import time

try:
    import cv2
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "Could not import required video dependencies. Please install with:\n"
        "  pip install \"luxai-magpie[video]\""
    )
    sys.exit(1)



from luxai.magpie.utils.common import get_uinque_id
from luxai.magpie.utils.logger import Logger
from luxai.magpie.nodes.source_node import SourceNode
from luxai.magpie.transport.zmq.zmq_publisher import ZMQPublisher
from luxai.magpie.frames.image import ImageFrameCV, ImageFrameJpeg, ImageFrameRaw


class ZmqVideoCapture(SourceNode):

    def setup(self, camera=0, frame_rate=30, size=(640, 480), encoder='jpeg', topic="/camera"):
        # Initialize camera capture        
        self.encoder = encoder
        self.cap = cv2.VideoCapture(camera)
        self.topic = topic
        self.frame_rate = frame_rate if frame_rate > 0 else 30
        self.frame_gid = get_uinque_id()
        self._frame_id_counter = 0
        self._frame_write_time = None

                
        if frame_rate > 0:
            self.cap.set(cv2.CAP_PROP_FPS, frame_rate)        
        w, h = size
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, w)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, h)
        actual_w = self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        actual_h = self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
        # actual_fps = self.cap.get(cv2.CAP_PROP_FPS)
        Logger.info(f"{self.name} initilized with size=({actual_w}x{actual_h}) and fps={self.frame_rate}")
        Logger.info(f"{self.name} streaming video on {self.stream_writer.endpoint} using {self.encoder} encoding.")    

    def process(self):        
        self._frame_write_time = time.time()
        ret, image = self.cap.read()

        # Select encoding method
        if self.encoder == "raw":            
            # Infer dimensions and channels from the OpenCV image
            if image.ndim == 2:
                height, width = image.shape
                channels = 1
                pixel_format = 'GRAY'
            elif image.ndim == 3:
                height, width, channels = image.shape
                # OpenCV default color order
                pixel_format = 'BGR'
            else:
                raise ValueError(f"Unsupported image shape: {image.shape}")
            
            frame = ImageFrameRaw(
                data=image.data.tobytes(),                
                width=width,
                height=height,
                channels=channels,
                pixel_format=pixel_format
                )
        elif self.encoder == "cv":
            frame = ImageFrameCV.from_cv_image(image)
        else:  # turbojpeg
            frame = ImageFrameJpeg.from_np_image(image, quality=80, pixel_format="BGR")

        frame.gid = self.frame_gid
        frame.id = self._frame_id_counter  
        self._frame_id_counter += 1        
        self.stream_writer.write(frame.to_dict(), topic=self.topic)

        # ensure frame rate 
        time_diff = time.time() - self._frame_write_time
        frame_period = 1.0 / float(self.frame_rate)
        if time_diff < frame_period:
            time.sleep(frame_period - time_diff)



def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint", 
                        help="ZeroMQ subscribing socket endpoint (e.g. tcp://127.0.0.1:5555)",
                        type=str)
    parser.add_argument(
        "topic",
        help="ZeroMQ publishing topic on endpoint (e.g. /mytopic)",
        type=str,
    )

    parser.add_argument(
        "--bind",
        action="store_true",
        help="Bind the publisher socket instead of connecting (default: connect).",
    )

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
                        choices=["raw", "cv", "jpeg"],
                        default="raw",
                        help="Encoding backend: 'cv' for ImageFrameCV, 'jpeg' for ImageFrameJpeg")

    args = parser.parse_args()

    node = ZmqVideoCapture(name='MagpieVideoCapture',
                            stream_writer=ZMQPublisher(
                                endpoint=args.endpoint,                                 
                                bind=args.bind,
                                queue_size=0,                                 
                                delivery="latest"),
                            setup_kwargs={
                                'camera': args.camera,
                                'size':  tuple(args.size),
                                'frame_rate': args.framerate,
                                'encoder': args.encoder,
                                'topic': args.topic})
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break
    Logger.info("Closing...")
    node.terminate()


if __name__ == "__main__":
    main()  
