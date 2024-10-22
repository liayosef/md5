
import socket


def send_protocol(message):
    """
    returns a string with the lenght
    :param message: the message
    :return: the message with her lenght
    """
    length = str(len(message)).encode()
    message1 = length.zfill(10)
    message = message1 + message
    return message


def recv_protocol(socket1):
    """
    gets the socket the lenght and the message
    :param socket:the socket contaninig the full message
    :return:the messag without her lenght
    """
    length = b""
    message = b""
    try:
        while len(length) < 10:

            buf = socket1.recv(1)
            if buf == b'':
                length = b''
                break
            length += buf
        length = length.decode()
        while length != '' and len(message) < int(length):
            message += socket1.recv(int(length) - len(message))
    except socket.error as err:
        message = b"error"
    return message