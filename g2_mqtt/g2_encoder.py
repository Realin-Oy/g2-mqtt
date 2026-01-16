import logging
from typing import Generator, Iterable, TypedDict

from bitstring import BitStream, ConstBitStream

logger = logging.getLogger(__name__)

"""

Copied from g2_mqtt package

Header:
version: uint8  # 1..100 =  0x01..0x64 int
timestamp: uint32 big endian  # unix timestamp

repeat payload:
    byte channel, starts from 101
    float32 big endian value

```mermaid

---
title: "Header (5 bytes)"
---
packet
+8: "Version uint:8 (0..0x64) = 0x01"
+32: "Unix Timestamp uintbe:32"
```

Channel datatype and unit are transmitted out of band

```mermaid
---
title: "payload (5 bytes per channel)"
---
packet
+8: "channel uint:8 (0x65..0xFF)"
+32: "Value floatbe:32"
```

# Example packet size: g2 measurements 11 channels x 5 bytes = 55 bytes + version + timestamp = 60 bytes
"""

MQTT_ENCODER = 'g2'
MQTT_TOPIC = 'g2/{network}/{node}/data'

TYPE_FORMAT = 'uint:8'
TIMESTAMP_FORMAT = 'uintbe:32'
VALUE_FORMAT = 'floatbe:32'

G2_VERSION = 1

CHANNELS = [
    # Reference to IPSO smart objects
    'version,=1',

    'temperature,C=103',
    'humidity,%=104',
    'barometer,Pa=115',

    'pegasor:PN,n/cm^3=120',
    'pegasor:OmeFT,=121',
    'pegasor:humidity,%=122',
    'pegasor:FeedPressure,kPa=123',
    'pegasor:ambient_temperature,C=124',
    'pegasor:board_temperature,C=125',
    'pegasor:CMD,nm=126',
    'pegasor:error,=127',
    'pegasor:PM,µg/m^3=128',
    'pegasor:LDSA,µm^2/cm^3=129',
    'pegasor:PN_uncut,n/cm^3=130',

    'lat,deg=136',
    'lon,deg=137',

    'activity,=200',
    'disk,%=201',
    'load,=202',
    'memory,%=203',

    'pump:i_term,=230',
    'pump:last_time,=231',
    'pump:set_w,W=232',
    'pump:p_w,W=233',
    'pump:u_set_v,V=234',
    'pump:u_v,V=235',
    'pump:i_a,A=236',
    'pump:freq,Hz=237',
]

# Map from datatype to channel number
ENCODE_CHANNELS = {item.split(',')[0]: int(item.split('=')[1]) for item in CHANNELS}

# Map from channel number to datatype, unit
DECODE_CHANNELS = {int(item.split('=')[1]): item.split('=')[0] for item in CHANNELS}


class Measurement(TypedDict):
    datatype: str
    value: float
    unit: str
    time: float | None


Item = tuple[float, Measurement]


class G2Encoder:
    version = G2_VERSION
    encode_channels = ENCODE_CHANNELS
    decode_channels = DECODE_CHANNELS
    PART_SIZE_BITS = 5 * 8
    VERSION_LIMIT = 0x64

    verbose = True

    def encode(self, measurements: Iterable[Item]) -> Generator[bytes, None, None]:
        # presumes time-sorted measurements
        previous = None

        payload = BitStream()

        for at, measurement in measurements:
            # unit = measurement['unit']
            # timestamp = measurement['time']
            channel = measurement['datatype']
            value = measurement['value']

            if previous is None or at != previous:
                if len(payload) > self.PART_SIZE_BITS:
                    yield payload.bytes
                payload = BitStream()
                payload.append(f'uint:8={self.version}')
                payload.append(f'{TIMESTAMP_FORMAT}={int(at)}')

            data_channel = self.encode_channels.get(channel)
            if data_channel:
                payload.append(f'{TYPE_FORMAT}={int(data_channel)}')
                payload.append(f'{VALUE_FORMAT}={value}')
            else:
                if self.verbose:
                    logger.debug(f'Unknown {self.version=} channel {channel}={value}')

            previous = at

        if payload and len(payload) > self.PART_SIZE_BITS:
            yield payload.bytes

    def decode(self, payload: bytes) -> Generator[
        Measurement, None, None]:
        stream = ConstBitStream(payload)
        bits = len(stream)
        timestamp = 0
        version = 0

        while stream.bitpos <= (bits - self.PART_SIZE_BITS):
            channel = stream.read(TYPE_FORMAT)

            if channel < self.VERSION_LIMIT:
                # Start of new data packet
                version = channel
                if self.verbose and version != self.version:
                    logger.warning(f'Unknown {version=} (expected {self.version})')
                timestamp: int = stream.read(TIMESTAMP_FORMAT)

                channel = stream.read(TYPE_FORMAT)

            value: float = stream.read(VALUE_FORMAT)
            data_channel = self.decode_channels.get(channel)

            if data_channel:
                datatype, unit = data_channel.split(',')
                yield Measurement(time=timestamp, datatype=datatype, value=value, unit=unit)
            else:
                if self.verbose:
                    logger.debug(f'Unknown {version=} (expected {self.version}) channel {channel}={value}')
