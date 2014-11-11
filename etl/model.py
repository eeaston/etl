""" Core collector model
"""
from schematics.models import Model
from schematics.types import BooleanType, DateTimeType, UUIDType


class CollectorConfig(Model):
    """ Base Collector model.

    Parameters
    ----------
    needs_update: `bool`
        True if this collector needs updating by the updater
    last_updater: `datetime`
        Timestamp for the last time the updater ran for this collector


    Notes
    -----
    primary_key: Primary key indicator.
           Key can be a string (single) or tuple (compound).
           In the case of compound keys, the first item is the name of the
           key and the rest are the key components

    Example:
        'id' : Primary key is the 'id' field
        ('id_1', 'name', 'address'):  Primary key is 'id_1', which will be
                                      spec['name']:spec['address']
    """
    needs_update = BooleanType(required=True)
    last_updated = DateTimeType()
    updating = UUIDType()

    primary_key = None

    def __init__(self, spec):
        if not 'needs_update' in spec:
            spec['needs_update'] = False
        if not 'updating' in spec:
            spec['updating'] = None
        self.make_primary_key(spec)
        super(CollectorConfig, self).__init__(spec)

    @classmethod
    def get_pk(cls):
        """ Return the name of the primary key """
        try:
            return cls.primary_key + ''
        except TypeError:
            return cls.primary_key[0]

    def make_primary_key(self, spec):
        """ Build a primary key in the spec
        """
        if not self.primary_key:
            raise ValueError("No keys defined for {}".format(self))

        try:
            if self.primary_key + '' not in spec:
                raise ValueError("Missing key '{}' from spec".format(self.primary_key))
        except TypeError:
            # Compound key. First item is the key name, the rest are the key components
            spec[self.primary_key[0]] = '/'.join(str(spec[i]) for i in self.primary_key[1:])
