"""
WebRTC Reader example.

Receives sensor data from the robot over a WebRTC data channel.

Usage (run together with webrtc_writer.py):
    Terminal 1 (robot):    python examples/webrtc_writer.py
    Terminal 2 (operator): python examples/webrtc_reader.py
"""

from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamReader, WebRTCOptions
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"  # MQTT broker used only for signaling
SESSION_ID = "magpie/examples/webrtc"         # shared rendezvous name — must match writer
TOPIC      = "robot/state"                    # topic to subscribe to


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    # For broker-less LAN use with_zmq() instead:    
    conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", 
                                    SESSION_ID, 
                                    bind=False,
                                    reconnect=True,
                                    options=WebRTCOptions(
                                        stun_servers=[],            # disable stun server in local network for faster connections
                                    )
    )    
    #conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID, client_id="magpie-webrtc-sub")
    
    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    sub = WebRtcStreamReader(conn, topic=TOPIC)

    while True:
        try:
            data, topic = sub.read(timeout=5.0)
            Logger.info(f"received on '{topic}': {data}")
        except TimeoutError:
            Logger.warning("no data — is webrtc_writer.py running?")
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    sub.close()
    conn.disconnect()
