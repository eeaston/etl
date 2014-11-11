'''
Created on Apr 11, 2014

@author: Edward Easton
'''

from pytest import raises
from mock import Mock, patch, sentinel, call, create_autospec
from schematics.types import IntType, StringType

from etl import model


def test_constructor_missing_pk():
    with raises(ValueError):
        model.CollectorConfig({})


def test_constructor_set_missing_attrs():
    class TestCfg(model.CollectorConfig):
        id = IntType(required=True)
        primary_key = 'id'

    cfg = TestCfg({'id': 123})
    assert cfg['needs_update'] is False
    assert cfg['updating'] is None


def test_get_pk_single():
    class TestCfg(model.CollectorConfig):
        primary_key = 'id'
    assert TestCfg.get_pk() == 'id'


def test_get_pk_compound():
    class TestCfg(model.CollectorConfig):
        primary_key = ('foo', 'bar', 'baz')

    assert TestCfg.get_pk() == 'foo'


def test_make_primary_key_single():
    class TestCfg(model.CollectorConfig):
        id = IntType(required=True)
        primary_key = 'id'

    cfg = TestCfg({'id': 123})
    assert getattr(cfg, cfg.get_pk()) == 123


def test_make_primary_key_single_missing():
    class TestCfg(model.CollectorConfig):
        id = IntType(required=True)
        primary_key = 'id'

    with raises(ValueError) as e:
        TestCfg({})
    assert "Missing key" in str(e.value.args)


def test_make_primary_key_compound():
    class TestCfg(model.CollectorConfig):
        id = StringType(required=True)
        category = StringType(required=True)
        code = IntType(required=True)
        primary_key = ('id', 'category', 'code')

    cfg = TestCfg({'category': 'foo', 'code': 123})
    assert getattr(cfg, cfg.get_pk()) == 'foo/123'
