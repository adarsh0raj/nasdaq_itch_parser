# Program Details

Program that parses trades from a NASDAQ ITCH 5.0 tick data file and
outputs a running volume-weighted average price (VWAP) for each stock at every hour
including market close.

### Inputs

The program takes as input a NASDAQ ITCH 5.0 tick data file. The file is expected to be in the same directory as the program and named "01302019.NASDAQ_ITCH50". The file can be downloaded from - https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz

### Outputs

I HAVE ALSO INCLUDED 'vwap.txt' WITH PARSED DATA OF FIRST 3 hours  FOR REFERENCE.

After each hour - VWAP for each stock will be printed in the terminal, and will also be stored in a file
"vwap.txt"  with the following format:

"""
Trading Hour : Hour Number
Stock1 VWAP1
Stock2 VWAP2
...

Trading Hour : Hour Number
Stock1 VWAP1
Stock2 VWAP2
...

"""

### Prerequisites

Python 3

### Running the Program

```
python main.py
```
