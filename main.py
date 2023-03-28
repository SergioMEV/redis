############
# This is my implementation of Redis without an event loop.
# You can find more info in the README file.
############

import socket
import threading
import re
import time

# ========== #
#  Globals   #
STD_RESPONSE = 'PONG'
KEY_VALUE_STORE, KEY_EXPIRY_STORE = {}, {}
KEY_VALUE_STORE_LOCKS = {}
KEY_VALUE_STORE_LOCKS_LOCK = threading.Lock()

# ========== #
#  Parsers   #
def simpleStringParser(message: str) -> list:
    """ Parses Redis Protocol simple strings. 
    Returns empty string if message is not properly formatted

    Args:
        message (str): raw message string received from client

    Returns:
        str: decoded string
    """
    if len(message) < 4:
        return ''

    finalString, index = '', 1
    while message[index].isdigit() or message[index] == '-':
        finalString += message[index]
        index += 1

    return [finalString]

def bulkStringParser(message: str) -> list:
    """ Parses Redis Protocol bulk strings. 
    Returns empty string if message is not properly formatted

    Args:
        message (str): raw message string received from client

    Returns:
        str: decoded string
    """
    if len(message) <= 1:
        return ['']
    
    # Finding message length
    index = 1
    lengthOfMessage = ''

    while message[index].isdigit() or message[index] == '-':
        lengthOfMessage += message[index]
        index += 1

    # Checking that bulk string is the appropriate length
    if len(message) != (6 + int(lengthOfMessage)):
        return ['']
    
    # Skipping escaped characters and reading in message
    index += 2
    finalString = message[index:index + int(lengthOfMessage)]
    
    return [finalString]

def arrayParser(message: str) -> list:
    """ Parses Redis Protocol arrays. 
    Returns empty string if message is not properly formatted. 
    Does not support arrays of arrays.

    Args:
        message (str): raw message string received from client

    Returns:
        str: decoded string
    """
    if len(message) < 4:
        return ['']

    # Finding array element count
    index = 1
    count = ''
    while message[index].isdigit() or message[index] == '-':
        count += message[index]
        index += 1

    index += 2

    elementsString = message[index:]
    elementsStrings = [s for s in re.split(re.escape('\r\n'),elementsString) if s != '']
    
    if len(elementsStrings) != 2 * int(count):
        return ['']

    results = []
    elemIdx = 0 
    while elemIdx < len(elementsStrings)-1:
        match elementsStrings[elemIdx][0]:
            case '+':
                processedString = elementsStrings[elemIdx][1:]
            case '$':
                processedString = elementsStrings[elemIdx + 1]
            case _:
                processedString = ''
        
        elemIdx += 2
        results.append(processedString)
    
    return results

def messageParser(message: str) -> str:
    """ Parses a message and passes it on to its appropriate sub-parser.

    Args:
        message (str): encoded message received from connection

    Returns:
        str: decoded message
    """
    rawString = message.decode('utf-8')

    match rawString[0]:
        case '+':
            processedString = simpleStringParser(rawString)
        case '$':
            processedString = bulkStringParser(rawString)
        case '*':
            processedString = arrayParser(rawString)
        case _:
            processedString = ''
        
    return processedString

# ================== #
#  Expiry Functions  #
def setExpiry(key: str, arguments: list[str]):
    """ Helper functions that sets an expiry if needed for a key. 
    Only to be called from inside set().

    Args:
        key (str): key of value with given expiry
        arguments (list[str]): list of arguments
    """
    # Get current time in milliseconds and time limit
    currentTime = time.time() * 1000
    timeLimit = int(arguments[0])

    # Set expiration time
    expirationTime = currentTime + timeLimit

    # Store
    KEY_EXPIRY_STORE[key] = expirationTime

def checkExpiry(key: str) -> bool:
    """ Helper functions that checks if the key is expired, if so it removes the key_value pair from store. 
    Only to be called from inside get().

    Args:
        key (str): key of value with given expiry
    
    Returns:
        bool: boolean indicating whether key is expired
    """
    # Get current time in milliseconds
    currentTime = time.time() * 1000

    # Check whether key has expiry and whether current time is after expiry time
    if key in KEY_EXPIRY_STORE and currentTime >= KEY_EXPIRY_STORE[key]:
        # Key is expired, so remove all data on key
        KEY_EXPIRY_STORE.pop(key)
        KEY_VALUE_STORE.pop(key)
        
        return True
    
    return False

    
# ========== #
#  Commands  #
def echo(arguments: list[str], connection: socket):
    """ Auxiliary function that handles ECHO command

    Args:
        arguments (list[str]): list of arguments
        connection (socket): socket pointing to connection that sent command
    """
    outgoingMessage = ''.join(arguments) 
    sendMessage(outgoingMessage, connection)


def set(arguments: list[str], connection: socket):
    """ Auxiliary function that handles SET command

    Args:
        arguments (list[str]): list of arguments
        connection (socket): socket pointing to connection that sent command
    """
    # Separating arguments
    key = arguments[0]
    value = arguments[1]

    if "px" in arguments:
        hasExpiry = True
        expiryArguments = arguments[arguments.index("px") + 1:]
    else:
        hasExpiry = False

    # Checking if key is already in store,
    #  we lock to make sure no other threads can add the key after our check.
    KEY_VALUE_STORE_LOCKS_LOCK.acquire()

    if key not in KEY_VALUE_STORE_LOCKS:
        KEY_VALUE_STORE_LOCKS[key] = threading.Lock()

    KEY_VALUE_STORE_LOCKS_LOCK.release()

    # Updating/Adding key-value pair to KEY_VALUE_STORE
    KEY_VALUE_STORE_LOCKS[key].acquire()

    KEY_VALUE_STORE[key] =  value
    if hasExpiry:
        setExpiry(key, expiryArguments)

    KEY_VALUE_STORE_LOCKS[key].release()

    # Once we are done, we send back OK.
    sendMessage("OK", connection)


def get(arguments: list[str], connection: socket):
    """ Auxiliary function that handles GET command

    Args:
        arguments (list[str]): list of arguments
        connection (socket): socket pointing to connection that sent command
    """
    key = arguments[0]

    # Checking if key is stored
    KEY_VALUE_STORE_LOCKS_LOCK.acquire()

    if key not in KEY_VALUE_STORE_LOCKS:
        KEY_VALUE_STORE_LOCKS_LOCK.release()

        # Sending back (nil) because key does not exist
        outgoingMessage = "(nil)"
        sendMessage(outgoingMessage, connection)
        return
    
    KEY_VALUE_STORE_LOCKS_LOCK.release()

    # Querying KEY_VALUE_STORE for requested value
    KEY_VALUE_STORE_LOCKS[key].acquire()

    if checkExpiry(key):
        isExpired = True
        outgoingMessage = "(nil)"
    else:
        isExpired = False
        outgoingMessage = KEY_VALUE_STORE[key]

    KEY_VALUE_STORE_LOCKS[key].release()
    
    if isExpired:
        KEY_VALUE_STORE_LOCKS.pop(key)

    # Sending message
    sendMessage(outgoingMessage, connection)


# ========== #
#  Handlers   #  
def sendMessage(message: str, connection: socket):
    """ Formats message and sends it back to connection.

    Args:
        message (str): Parsed string
        connection (socket): socket object that points to connection
    """
    if message == "(nil)":
        outgoingMessage = "$-1\r\n"
    else:
        outgoingMessage = '+' + message + '\r\n'

    connection.send(outgoingMessage.encode('utf-8'))



def receiveMessage(connection):
    """ Receives a message passes it on to a parser and then passes parsed message to correct command function.

    Args:
        connection (socket): socket object that points to connection
    """
    incomingMessage = connection.recv(4096)
    decodedMessage = messageParser(incomingMessage)

    if len(decodedMessage) > 1:
        # decodedMessage is a command with arguments
        command = decodedMessage[0]
        args = decodedMessage[1:]
        
        match command:
            case "echo":
                echo(args, connection)
            case "set":
                set(args, connection)     
            case "get":
                get(args, connection)  
            case _:
                sendMessage('', connection)
    else:
        # Decoded message is a single string
        if decodedMessage[0] == 'ping':
            outgoingMessage = STD_RESPONSE
            sendMessage(outgoingMessage, connection)


def connectionWorker(connection: socket) -> None:
    """ Serves as worker thread for each connection.

    Args:
        connection (socket): socket object that points to connection
    """
    with connection:
        while True:
            receiveMessage(connection)


def main():
    print("Starting...")

    # Creating Server
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Listening for connections
    with server_socket:
        while True:
            # Wait for connection
            connection, address = server_socket.accept()

            # Start worker thread to listen and return messages
            newThread = threading.Thread(target=connectionWorker, args=[connection])
            newThread.start()
            

    
if __name__ == "__main__":
    # main() works as the listener for connections.
    main()
