
""""""

import argparse
import sys

def build_spy_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--filter-status')
    parser.add_argument('connect_info', nargs=1)
    parser.add_argument('output', nargs='?')
    return parser


def run_spy(args=None):
    from .spy import Spy, is_not_status

    parser = build_spy_parser()

    options = parser.parse_args(args)

    spy = Spy(info=options.connect_info[0])

    print('Starting logging, press CTRL+C to stop...', file=sys.stderr)
    filter_function = is_not_status if options.filter_status else None
    if options.output:
        with open(options.output, 'w', encoding='utf8') as f:
            spy.log_iopub(output=f, filter_function=filter_function)
    else:
        spy.log_iopub(filter_function=filter_function)


run_spy()
