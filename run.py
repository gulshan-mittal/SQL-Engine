import sys
import argparse
from src.mini_sql import Engine


if __name__ == '__main__':

    typ = 1 
    # print("Welcome to the MiniSQL monitor.")
    mini_sql_engine = Engine(typ)
    mini_sql_engine.execute(sys.argv[1: ])