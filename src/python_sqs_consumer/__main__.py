"""
Entrypoint module, in case you use `python -mpython_sqs_consumer`.


Why does this file exist, and why __main__? For more info, read:

- https://www.python.org/dev/peps/pep-0338/
- https://docs.python.org/2/using/cmdline.html#cmdoption-m
- https://docs.python.org/3/using/cmdline.html#cmdoption-m
"""
import sys
from python_sqs_consumer.cli import main


if __name__ == "__main__":
    sys.exit(main())
