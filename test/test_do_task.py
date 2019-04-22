from pypeflow import do_task as M
import pytest

testdata = [
        # no subs
        ({}, {}, {},
"""\
echo hello
""",
"""\
echo hello
"""),
        # simple subs (with quoting)
        ({'ii': 'II'}, {'oo': 'O O'}, {'pp': 'PP'},
"""\
echo {input.ii}
echo {output.oo}
echo {params.pp}
""",
"""\
echo II
echo 'O O'
echo PP
"""),
        # input.ALL
        ({'ii': 'II', 'ij': 'IJ'}, {'oo': 'OO'}, {'pp': 'PP'},
"""\
echo {input.ALL}
echo {output.oo}
echo {params.pp}
""",
"""\
echo II IJ
echo OO
echo PP
"""),
]

@pytest.mark.parametrize("args", testdata)
def test_sub(args):
    myi, myo, myp, t, expected = args
    got = M.sub(t, myi, myo, myp)
    assert expected == got
