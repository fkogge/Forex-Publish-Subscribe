"""
Contains helper functions for deserializing Forex provider message contents,
which are sent as byte streams, and one function for serializing an IP address.

Author: Francis Kogge
Date: 10/26/2021
Course: CPSC 5520 (Distributed Systems)
"""

from array import array
from datetime import datetime, timezone
import socket

BYTES_PER_QUOTE = 32
MICROS_PER_SECOND = 1_000_000


def deserialize_price(price_bytes: bytes) -> float:
    """
    Converts a byte stream to an 8-byte floating point number.
    :param price_bytes: bytes to convert
    :return: price
    """
    float_array = array('d')  # Array of 8-byte floating-point number
    float_array.frombytes(price_bytes)  # Append float representation of bytes
    return float_array[0]


def serialize_address(address: (str, int)) -> bytes:
    """
    Converts an IP address and port number pair to a byte stream.
    :param address: address to serialize
    :return: serialized address
    """
    # IPv4Address in 4-byte, big endian
    host_ip_bytes = socket.inet_aton(address[0])
    # Port number in 2-byte, big endian
    port_bytes = address[1].to_bytes(2, byteorder='big')
    return host_ip_bytes + port_bytes


def deserialize_utcdatetime(dt_bytes: bytes) -> datetime:
    """
    Converts a byte stream to a UTC datetime object. Uses the timezone aware
    datetimes to convert to local time, so that the client doesn't need to
    worry about converting global UTC time to their local time zone.
    :param dt_bytes: bytes to convert
    :return: datetime object
    """
    # Convert bytes to integer timestamp
    int_time = int.from_bytes(dt_bytes, byteorder='big')
    # Convert integer timestamp to utc datetime
    utc = datetime.fromtimestamp(int_time / MICROS_PER_SECOND, tz=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    time_delta = utc - epoch  # Difference from now and Jan 1, 1970
    return datetime.fromtimestamp(time_delta.total_seconds())


def unmarshal_message(msg_bytes: bytes,
                      bytes_per_quote: int = BYTES_PER_QUOTE) -> list:
    """
    Converts a byte stream of Forex quotes to a list of the deserialized quotes
    in bytes_per_quote (default of 32 byte) intervals.
    :param msg_bytes: bytes to convert
    :param bytes_per_quote: number of bytes per quote
    :return: list of deserialized quotes from the Forex message
    """
    if len(msg_bytes) % bytes_per_quote != 0:
        raise ValueError('Quotes must be in {} byte sequences.'
                         .format(bytes_per_quote))

    message = []

    # Process bytes until there are none left
    while msg_bytes:
        quote = {}

        # Splice the byte array based on the Forex provider message format
        timestamp_bytes = msg_bytes[0:8]
        currency_bytes = msg_bytes[8:14]
        exchange_rate_bytes = msg_bytes[14:22]

        # Deserialize bytes into their respective readable formats
        timestamp = deserialize_utcdatetime(timestamp_bytes)
        currency = currency_bytes.decode('utf-8')
        price = deserialize_price(exchange_rate_bytes)

        # Add the deserialized data to the final message
        quote['timestamp'] = timestamp
        quote['cross'] = currency[0:3] + ' ' + currency[3:6]
        quote['price'] = price
        message.append(quote)

        # Discard un-marshaled bytes to move onto the next sequence of bytes
        msg_bytes = msg_bytes[bytes_per_quote:]

    return message
