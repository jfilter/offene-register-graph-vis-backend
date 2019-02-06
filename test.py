import pytest

import get


def test_company_name():
    x = get.by_company_name("Deutsche Wohnen")
    print(x)
