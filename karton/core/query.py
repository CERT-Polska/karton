import fnmatch
import re
from collections.abc import Mapping, Sequence
from typing import Dict, Type

# Source code adopted from https://github.com/kapouille/mongoquery
# Original licenced under "The Unlicense" license.


class QueryError(Exception):
    """Query error exception"""

    pass


class _Undefined(object):
    pass


def is_non_string_sequence(entry):
    """Returns True if entry is a Python sequence iterable, and not a string"""
    return isinstance(entry, Sequence) and not isinstance(entry, str)


class Query(object):
    """The Query class is used to match an object against a MongoDB-like query"""

    def __init__(self, definition, _type_coercion=False):
        """
        If _type_coercion is enabled: header values are coerced to string
        when condition is also a string. It's implemented for compatibility
        with old syntax e.g. {"execute": "!False"} filter vs {"execute": False}
        header value.
        """
        self._definition = definition
        self._type_coercion = _type_coercion

    def match(self, entry):
        """Matches the entry object against the query specified on instanciation"""
        return self._match(self._definition, entry)

    def _match(self, condition, entry):
        if isinstance(condition, Mapping):
            return all(
                self._process_condition(sub_operator, sub_condition, entry)
                for sub_operator, sub_condition in condition.items()
            )
        if is_non_string_sequence(entry):
            return condition in entry
        return self._eq(condition, entry)

    def _extract(self, entry, path):
        if not path:
            return entry
        if entry is None:
            return entry
        if is_non_string_sequence(entry):
            try:
                index = int(path[0])
                return self._extract(entry[index], path[1:])
            except ValueError:
                return [self._extract(item, path) for item in entry]
        elif isinstance(entry, Mapping) and path[0] in entry:
            return self._extract(entry[path[0]], path[1:])
        else:
            return _Undefined()

    def _path_exists(self, operator, condition, entry):
        keys_list = list(operator.split("."))
        for i, k in enumerate(keys_list):
            if isinstance(entry, Sequence) and not k.isdigit():
                for elem in entry:
                    operator = ".".join(keys_list[i:])
                    if self._path_exists(operator, condition, elem) == condition:
                        return condition
                return not condition
            elif isinstance(entry, Sequence):
                k = int(k)
            try:
                entry = entry[k]
            except (TypeError, IndexError, KeyError):
                return not condition
        return condition

    def _process_condition(self, operator, condition, entry):
        if isinstance(condition, Mapping) and "$exists" in condition:
            if isinstance(operator, str) and operator.find(".") != -1:
                return self._path_exists(operator, condition["$exists"], entry)
            elif condition["$exists"] != (operator in entry):
                return False
            elif tuple(condition.keys()) == ("$exists",):
                return True
        if isinstance(operator, str):
            if operator.startswith("$"):
                try:
                    return getattr(self, "_" + operator[1:])(condition, entry)
                except AttributeError:
                    raise QueryError(f"{operator} operator isn't supported")
            else:
                try:
                    extracted_data = self._extract(entry, operator.split("."))
                except IndexError:
                    extracted_data = _Undefined()
        else:
            if operator not in entry:
                return False
            extracted_data = entry[operator]
        return self._match(condition, extracted_data)

    @staticmethod
    def _not_implemented(*_):
        raise NotImplementedError

    @staticmethod
    def _noop(*_):
        return True

    def _eq(self, condition, entry):
        try:
            if (
                self._type_coercion
                and type(condition) is str
                and type(entry) is not str
            ):
                return str(entry) == condition
            return entry == condition
        except TypeError:
            return False

    @staticmethod
    def _gt(condition, entry):
        try:
            return entry > condition
        except TypeError:
            return False

    @staticmethod
    def _gte(condition, entry):
        try:
            return entry >= condition
        except TypeError:
            return False

    @staticmethod
    def _in(condition, entry):
        if is_non_string_sequence(condition):
            for elem in condition:
                if is_non_string_sequence(entry) and elem in entry:
                    return True
                elif not is_non_string_sequence(entry) and elem == entry:
                    return True
            return False
        else:
            raise TypeError("condition must be a list")

    @staticmethod
    def _lt(condition, entry):
        try:
            return entry < condition
        except TypeError:
            return False

    @staticmethod
    def _lte(condition, entry):
        try:
            return entry <= condition
        except TypeError:
            return False

    def _ne(self, condition, entry):
        return not self._eq(condition, entry)

    def _nin(self, condition, entry):
        return not self._in(condition, entry)

    def _and(self, condition, entry):
        if isinstance(condition, Sequence):
            return all(self._match(sub_condition, entry) for sub_condition in condition)
        raise QueryError(f"$and has been attributed incorrect argument {condition}")

    def _nor(self, condition, entry):
        if isinstance(condition, Sequence):
            return all(
                not self._match(sub_condition, entry) for sub_condition in condition
            )
        raise QueryError(f"$nor has been attributed incorrect argument {condition}")

    def _not(self, condition, entry):
        return not self._match(condition, entry)

    def _or(self, condition, entry):
        if isinstance(condition, Sequence):
            return any(self._match(sub_condition, entry) for sub_condition in condition)
        raise QueryError(f"$or has been attributed incorrect argument {condition}")

    @staticmethod
    def _type(condition, entry):
        bson_type: Dict[int, Type] = {
            1: float,
            2: str,
            3: Mapping,
            4: Sequence,
            5: bytearray,
            7: str,  # object id (uuid)
            8: bool,
            9: str,  # date (UTC datetime)
            10: type(None),
            11: re.Pattern,  # regex,
            13: str,  # Javascript
            15: str,  # JavaScript (with scope)
            16: int,  # 32-bit integer
            17: int,  # Timestamp
            18: int,  # 64-bit integer
        }
        bson_alias = {
            "double": 1,
            "string": 2,
            "object": 3,
            "array": 4,
            "binData": 5,
            "objectId": 7,
            "bool": 8,
            "date": 9,
            "null": 10,
            "regex": 11,
            "javascript": 13,
            "javascriptWithScope": 15,
            "int": 16,
            "timestamp": 17,
            "long": 18,
        }

        if condition == "number":
            return any(
                [
                    isinstance(entry, bson_type[bson_alias[alias]])
                    for alias in ["double", "int", "long"]
                ]
            )

        # resolves bson alias, or keeps original condition value
        condition = bson_alias.get(condition, condition)

        if condition not in bson_type:
            raise QueryError(f"$type has been used with unknown type {condition}")

        return isinstance(entry, bson_type[condition])

    _exists = _noop

    @staticmethod
    def _mod(condition, entry):
        return entry % condition[0] == condition[1]

    @staticmethod
    def _regex(condition, entry):
        if not isinstance(entry, str):
            return False
        # If the caller has supplied a compiled regex, assume options are already
        # included.
        if isinstance(condition, re.Pattern):
            return bool(re.search(condition, entry))

        try:
            regex = re.match(r"\A/(.+)/([imsx]{,4})\Z", condition, flags=re.DOTALL)
        except TypeError:
            raise QueryError(
                f"{condition} is not a regular expression and should be a string"
            )

        flags = 0
        if regex:
            options = regex.group(2)
            for option in options:
                flags |= getattr(re, option.upper())
            exp = regex.group(1)
        else:
            exp = condition

        try:
            match = re.search(exp, entry, flags=flags)
        except Exception as error:
            raise QueryError(f"{condition} failed to execute with error {error!r}")
        return bool(match)

    _options = _text = _where = _not_implemented

    def _all(self, condition, entry):
        return all(self._match(item, entry) for item in condition)

    def _elemMatch(self, condition, entry):
        if not isinstance(entry, Sequence):
            return False
        return any(
            all(
                self._process_condition(sub_operator, sub_condition, element)
                for sub_operator, sub_condition in condition.items()
            )
            for element in entry
        )

    @staticmethod
    def _size(condition, entry):
        if not isinstance(condition, int):
            raise QueryError(
                f"$size has been attributed incorrect argument {condition}"
            )

        if is_non_string_sequence(entry):
            return len(entry) == condition

        return False

    def __repr__(self):
        return f"<Query({self._definition})>"


def toregex(wildcard):
    if not isinstance(wildcard, str):
        raise QueryError(f"Unexpected value in the regex conversion: {wildcard}")
    # If is not neessary, but we avoid unnecessary regular expressions.
    if any(c in wildcard for c in "?*[]!"):
        return {"$regex": fnmatch.translate(wildcard)}
    return wildcard


def convert(filters):
    """Convert filters to the mongo query syntax.
    A special care is taken to handle old-style negative filters correctly
    """
    # Negative_filters are old-style negative assertions, and behave differently.
    # See issue #246 for the original bug report.
    #
    # For a short example:
    # [{"platform": "!win32"}, {"platform": "!linux"}]
    # will match all non-linux non-windows samples, but:
    # [{"platform": {"$not": "win32"}}, {"platform": {"$not": "linux"}}]
    # means `platform != "win32" or "platform != "linux"` and will match everything.
    # To get equivalent behaviour with mongo syntax, you should use:
    # [{"platform": {"$not": {"$or": ["win32", "linux"]}}}]
    regular_filter, negative_filter = [], []
    for rule in filters:
        positive_checks, negative_checks = [], []
        for key, value in rule.items():
            if isinstance(value, str):
                if value and value[0] == "!":  # negative check
                    negative_checks.append({key: toregex(value[1:])})
                else:
                    positive_checks.append({key: toregex(value)})
            else:
                positive_checks.append({key: value})
        regular_filter.append({"$and": positive_checks})
        negative_filter.append({"$and": positive_checks + [{"$or": negative_checks}]})
    return Query(
        {
            "$and": [
                {"$not": {"$or": negative_filter}},
                {"$or": regular_filter},
            ]
        },
        _type_coercion=True,
    )
