from .. import fs_based
import pdb
import sys

def main():
    fs_based.main(*sys.argv) # pylint: disable=no-value-for-parameter

# If run directly, rather than via the 'entry-point',
# then pdb will be used.
if __name__ == "__main__":
    #pdb.set_trace()
    main()
