from django.db import models

from django_hstore import forms
from django_hstore.query import HStoreQuerySet
from django_hstore.util import acquire_reference, serialize_references, unserialize_references

try:
    # Django 1.6 custom lookup support
    from django.db.models.lookups.lookups import Lookup
    from django.db.models.fields import CharField, IntegerField, DateField
    class HStoreLookup(Lookup):

        def __init__(self, lookup_type, nested_lookups=None):
            self.lookup_type = lookup_type
            self.retval_field = None
            self.nested_lookup = None
            if self.lookup_type in ('exact', 'contains'):
                if nested_lookups:
                    raise LookupError('The hstore lookup "%s" does not support nested lookups'
                                      % self.lookup_type)
                return
            if len(nested_lookups) > 2:
                raise LookupError('Lookup nesting too deep for hstore field!')
            self.cast = '%s'
            self.retval_field = CharField()
            if nested_lookups:
                if nested_lookups[0] in ('asint', 'asdate'):
                    type_convert = nested_lookups[0]
                    nested_lookups = [nested_lookups[1] if len(nested_lookups) == 2 else 'exact']
                    if type_convert == 'asint':
                        self.retval_field = IntegerField()
                        self.cast = '(%s)::INTEGER'
                    if type_convert == 'asdate':
                        self.retval_field = DateField()
                        self.cast = '(%s)::DATE'
                else:
                    if len(nested_lookups) == 2:
                        raise LookupError('Lookup nesting too deep for hstore field!')
            self.nested_lookup = self.retval_field.get_lookup(
                nested_lookups or ['exact'], None)
            if not self.nested_lookup:
                raise LookupError("Unknown nested lookup!")

        def common_normalize(self, params, field, qn, connection):
            if self.nested_lookup:
                rhs_sql, params = self.nested_lookup.common_normalize(
                    params, self.retval_field, qn, connection)
            else:
                rhs_sql, params = super(HStoreLookup, self).common_normalize(
                    params, field, qn, connection)
                params = [field.get_prep_lookup(self.lookup_type, params[0])]
            return rhs_sql, params

        def prepare_lhs(self, lvalue, qn, connection, lhs_only=False):
            lhs_clause, params = super(HStoreLookup, self).prepare_lhs(lvalue, qn, connection)
            if self.nested_lookup:
                lhs_clause = self.cast % ('%s -> %%s' % lhs_clause)
                try:
                    params.insert(0, self.lookup_type)
                except:
                    params = (self.lookup_type,) + params
                lhs_clause, inner_params = self.nested_lookup.prepare_lhs(
                    lhs_clause, qn, connection, lhs_only)
                params.extend(inner_params)
            return lhs_clause, params

        def as_constraint_sql(self, qn, connection, lhs_clause, value_annotation, rhs_sql, params,
                              field):
            rhs_format = self.rhs_format(value_annotation, connection, rhs_sql)
            if self.lookup_type == 'exact':
                if isinstance(params[0], dict):
                    return '%s = %s' % (lhs_clause, rhs_format), params
                else:
                    raise ValueError("Invalid value")  # Could do in get_prep_lookup
            elif self.lookup_type == 'contains':
                if isinstance(params[0], dict):
                    return ('%s @> %s' % (lhs_clause, rhs_format), params)
                elif isinstance(params[0], (list, tuple)):
                    if params:
                        return ('%s ?& %s' % (lhs_clause, rhs_format), params)
                    else:
                        raise ValueError('invalid value')
                elif isinstance(params[0], basestring):
                    return ('%s ? %s' % (lhs_clause, rhs_format), params)
                else:
                    raise ValueError('invalid value')
            nested_sql, params = self.nested_lookup.as_constraint_sql(
                qn, connection, lhs_clause, value_annotation, rhs_sql, params, field)
            return nested_sql, params

        def as_sql(self, qn, connection, lvalue):
            lvalue, params = self.prepare_lhs(lvalue, qn, connection, lhs_only=True)
            if self.nested_lookup:
                inner_sql, inner_params = self.nested_lookup.as_sql(qn, connection, lvalue)
                params.extend(inner_params)
                return inner_sql, params
            return super(HStoreLookup, self).as_sql(qn, connection, lvalue)

except ImportError:
    HStoreLookup = None

class HStoreDictionary(dict):
    """A dictionary subclass which implements hstore support."""

    def __init__(self, value=None, field=None, instance=None, **params):
        super(HStoreDictionary, self).__init__(value, **params)
        self.field = field
        self.instance = instance

    def remove(self, keys):
        """Removes the specified keys from this dictionary."""

        queryset = self.instance._base_manager.get_query_set()
        queryset.filter(pk=self.instance.pk).hremove(self.field.name, keys)

class HStoreDescriptor(object):
    def __init__(self, field):
        self.field = field

    def __get__(self, instance=None, owner=None):
        if instance is not None:
            return instance.__dict__[self.field.name]
        else:
            raise AttributeError()

    def __set__(self, instance, value):
        if not isinstance(value, HStoreDictionary):
            value = self.field._attribute_class(value, self.field, instance)
        instance.__dict__[self.field.name] = value

class HStoreField(models.Field):

    _attribute_class = HStoreDictionary
    _descriptor_class = HStoreDescriptor

    def contribute_to_class(self, cls, name):
        super(HStoreField, self).contribute_to_class(cls, name)
        setattr(cls, self.name, self._descriptor_class(self))

    def get_prep_value(self, value):
        from datetime import date
        if isinstance(value, dict):
            value = value.copy()
            for k, v in value.items():
                if isinstance(v, date):
                    value[k] = v.strftime('%Y-%m-%d')
        return value

    def get_lookup(self, lookup_names, target_field):
        return HStoreLookup(lookup_names[0], lookup_names[1:])

    def db_type(self, connection=None):
        return 'hstore'

class DictionaryField(HStoreField):
    """Stores a python dictionary in a postgresql hstore field."""

    def formfield(self, **params):
        params['form_class'] = forms.DictionaryField
        return super(DictionaryField, self).formfield(**params)

    def get_prep_lookup(self, lookup, value):
        return value

    def to_python(self, value):
        return value or {}

    def _value_to_python(self, value):
        return value

class ReferencesField(HStoreField):
    """Stores a python dictionary of references to model instances in an hstore field."""

    def formfield(self, **params):
        params['form_class'] = forms.ReferencesField
        return super(ReferencesField, self).formfield(**params)

    def get_prep_lookup(self, lookup, value):
        return (serialize_references(value) if isinstance(value, dict) else value)

    def get_prep_value(self, value):
        return (serialize_references(value) if value else {})

    def to_python(self, value):
        return (unserialize_references(value) if value else {})

    def _value_to_python(self, value):
        return (acquire_reference(value) if value else None)

class Manager(models.Manager):
    """Object manager which enables hstore features."""

    use_for_related_fields = True

    def get_query_set(self):
        return HStoreQuerySet(self.model, using=self._db)

    def hkeys(self, attr, **params):
        queryset = (self.filter(**params) if params else self.get_query_set())
        return queryset.hkeys(attr)

    def hpeek(self, attr, key, **params):
        queryset = (self.filter(**params) if params else self.get_query_set())
        return queryset.hpeek(attr, key)

    def hslice(self, attr, keys, **params):
        queryset = (self.filter(**params) if params else self.get_query_set())
        return queryset.hslice(attr, keys)

try:
    from south.modelsinspector import add_introspection_rules
    add_introspection_rules(rules=[], patterns=['django_hstore\.hstore'])
except ImportError:
    pass
