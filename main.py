# NASDAQ’s file stored ITCH data is encapsulated in a format similar to the wire protocol
# which separates the messages by two bytes. After that according to documentation, we get to know
# how deep to go into the buffer based on message type which is first char, and pulls the remainder of the message off.
# Then it cracks the message type to determine what it wants to do with the content.

# Parsing binary data in python - Simple binary unpacking using file reading and int.from_bytes method
# In earlier assignment submission I used struct module which I have not used in this file
# For storing ORDERS and EXECUTIONS - USing dictionaries
# The overall logic is same as earlier submission but I have used different methods to parse the binary data

# I HAVE ALSO INCLUDED REDUNDANT MESSAGE CODES WHICH ARE NOT USED FOR VWAP CALCULATION FOR SAKE OF TRUTHFULNESS
# ABOVE examples are message codes - L, V, K, J, h etc

ORDERS = dict()  # Dict of reference to a list of [ref, BUY or SELL, Shares, Stock, Price]
EXECUTED = dict()  # Dict of match to a list of [ref, Shares, Match, Stock, Price]
OUTPUT_FILE_PATH = 'vwap.txt'
HOURS = 0
START_TIME = 0
END_TIME = 0

def read_current_timestamp(payload):
    """
    Read the current timestamp from the message
    """
    return int.from_bytes(payload[4:10], byteorder='big', signed=False)

def parse_from_int(payload, i1, i2):
    """
    Read the data as big endian integer from payload from index i1 to i2
    """
    return int.from_bytes(payload[i1:i2], byteorder='big', signed=False)

def add_order_message(payload):
    """
    Parse the add order message and store it in ORDERS dictionary for A and F messages codes
    """
    global ORDERS
    curr_timestamp = read_current_timestamp(payload)
    ref = parse_from_int(payload, 10, 18)
    status = payload[18:19]  # Buy or Sell
    share = parse_from_int(payload, 19, 23)
    stock = payload[23:31].decode('ascii', 'ignore').strip()
    price = parse_from_int(payload, 31, 35)
    if curr_timestamp >= START_TIME and curr_timestamp < END_TIME:
        ORDERS[ref] = [ref, status, share, stock, price]
    return curr_timestamp

def add_execute_message(payload, is_price):
    """
    Parse the execute message and store it in EXECUTED dictionary for E and C messages codes
    is_price is a boolean value to check if the message is of type C or E
    is_price is False for E and True for C
    """
    global EXECUTED, START_TIME, END_TIME
    curr_timestamp = read_current_timestamp(payload)
    ref = parse_from_int(payload, 10, 18)
    share = parse_from_int(payload, 18, 22)
    match = parse_from_int(payload, 22, 30)
    if is_price:
        status_printable = payload[30:31].decode('ascii', 'ignore').strip()
        price = parse_from_int(payload, 31, 35)
        if curr_timestamp >= START_TIME and curr_timestamp < END_TIME and status_printable == 'Y':
            EXECUTED[match] = [ref, share, match, ORDERS[ref][3], price]
    else:
        if curr_timestamp >= START_TIME and curr_timestamp < END_TIME:
            EXECUTED[match] = [ref, share, match, ORDERS[ref][3], ORDERS[ref][4]]

    return curr_timestamp

def calculate_vwap():
    """
    Calculate the VWAP for each stock and write it to the file and print it to the console
    """
    global START_TIME, END_TIME, ORDERS, EXECUTED, OUTPUT_FILE_PATH
    VWAP = dict()
    for index, txs in EXECUTED.items():
        stock = txs[3]
        if stock not in VWAP:
            VWAP[stock] = [txs[1], txs[1]*txs[4]]
        else:
            VWAP[stock][0] += txs[1]
            VWAP[stock][1] += txs[1]*txs[4]

    with open(OUTPUT_FILE_PATH, "a+") as out:
        out.write(f"\n\nTRADING HOUR: {HOURS}\n")
        print(f"\n\nTRADING HOUR: {HOURS}\n")
        for k, v in VWAP.items():
            vwap = v[1]/(v[0]*1e4) if v[0] > 0 else 0
            out.write(f"{k}: {vwap}\n")
            print(f"{k}: {vwap}\n")

# Main Function Starts Here
with open('01302019.NASDAQ_ITCH50', 'rb') as file:
    # Read 2 bytes from the file
    # Since messages are separated by two zero bytes.
    while file.read(2):
        payload_type = file.read(1)
        curr_timestamp = 0
        # Cases based on different payload types like S,P, etc
        # since we are directly reading from file, we have to compare with byte string instead of direct characters
        # I have mentioned payload size in comments for each payload type
        # Reading first byte for message type above and remaining bytes in if cases below

        if payload_type == b'S': # System event message - Payload 12 bytes
            payload = file.read(11)
            locate_stock = parse_from_int(payload, 0, 1)
            track_num = parse_from_int(payload, 2, 4)
            curr_timestamp = read_current_timestamp(payload)
            ev_code = payload[10:11]
            if ev_code == b'Q': # Start of trading hours
                START_TIME = curr_timestamp
                with open(OUTPUT_FILE_PATH, "w+") as out:
                    out.write(f"Start of Trading Hours - Timestamp: {curr_timestamp}\n")
                    print(f"Start of Trading Hours - Timestamp: {curr_timestamp}\n")

            elif ev_code == b'M': # End of trading hours
                END_TIME = curr_timestamp
                with open(OUTPUT_FILE_PATH, "a+") as out:
                    out.write(f"End of Trading Hours - Timestamp: {curr_timestamp}\n")
                    print(f"End of Trading Hours - Timestamp: {curr_timestamp}\n")

        elif payload_type == b'A': # Add Order No MPID Attribution - Payload 36 bytes
            payload = file.read(35)
            curr_timestamp = add_order_message(payload)

        elif payload_type == b'F':  # Add Order MPID Attribution - Payload 40 bytes
            payload = file.read(39)
            curr_timestamp = add_order_message(payload)

        elif payload_type == b'E': # Order Executed Message - Payload 31 bytes
            payload = file.read(30)
            curr_timestamp = add_execute_message(payload, False)

        elif payload_type == b'C': # Order Executed With Price Message - Payload 36 bytes
            payload = file.read(35)
            curr_timestamp = add_execute_message(payload, True)

        elif payload_type == b'B': # Broken Trade Execution Message - Payload 19 bytes
            payload = file.read(18)
            curr_timestamp = read_current_timestamp(payload)
            EXECUTED[parse_from_int(payload, 10, 18)] = [0, 0, 0, 0, 0]

        elif payload_type == b'U': # Order Replace Message - Payload 35 bytes
            payload = file.read(34)
            curr_timestamp = read_current_timestamp(payload)
            old_ref, new_ref = parse_from_int(payload, 10, 18), parse_from_int(payload, 18, 26)
            share = parse_from_int(payload, 26, 30)
            price = parse_from_int(payload, 30, 34)
            if curr_timestamp >= START_TIME and curr_timestamp < END_TIME:
                ORDERS[new_ref] = [new_ref, ORDERS[old_ref][1], share, ORDERS[old_ref][3], price]

        elif payload_type == b'P': # Trade Message Non Cross - Payload 44 bytes
            payload = file.read(43)
            curr_timestamp = read_current_timestamp(payload)
            ref = parse_from_int(payload, 10, 18)
            status = payload[18:19]
            share = parse_from_int(payload, 19, 23)
            stock = payload[23:31].decode('ascii', 'ignore').strip()
            price = parse_from_int(payload, 31, 35)
            match = parse_from_int(payload, 35, 43)
            EXECUTED[match] = [ref, share, match, stock, price]

        elif payload_type == b'R': # Stock Directory Message - Payload 39 bytes
            payload = file.read(38)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'H': # Stock Trading Action Message - Payload 25 bytes
            payload = file.read(24)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'L': # Market Participant Position message - Payload 26 bytes
            payload = file.read(25)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'V': # Market wide circuit breaker Decline Level Message - Payload 35 bytes
            payload = file.read(34)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'K': # Quoting Period Update - Payload 28 bytes
            payload = file.read(27)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'J': # LULD Auction Collar - Payload 35 bytes
            payload = file.read(34)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'h': # Operational Halt Message - Payload 21 bytes
            payload = file.read(20)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'Y':  # Reg SHO Short Sale Price Test Indicator Message - Payload 20 bytes
            payload = file.read(19)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'W': # Market Wide Circuit Breaker Status message - payload 12 byets
            payload = file.read(11)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'X': # Order Cancel Message - Payload 19 bytes
            payload = file.read(22)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'D':  # Order Delete Message - Payload 19 bytes
            payload = file.read(18)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'Q':  # Cross Trade Message - Payload 40 bytes
            payload = file.read(39)
            curr_timestamp = read_current_timestamp(payload)

        elif payload_type == b'I':  # Net Order Imbalance Indicator Message - Payload 50 bytes
            payload = file.read(49)
            curr_timestamp = read_current_timestamp(payload)

        # Calculate VWAP if curr timestamp is greater than start time and start time is not 0
        if(START_TIME != 0 and curr_timestamp > START_TIME + ((HOURS+1)*(3600*1e9))):
            HOURS += 1
            calculate_vwap()

        # Break if curr timestamp is greater than end time and end time is not 0
        # We dont want data after end time
        if(END_TIME != 0 and curr_timestamp >= END_TIME):
            break