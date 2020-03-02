import json
import os.path

DB_PATH = ""
"""str: Database path"""
CACHE = {}
"""dict: Database cache"""


class WormDB(object):
    """This class represents an interface for exchanging data with a database."""

    def __init__(self, db_path: str):
        """Database interface initialization method

        When the class is initialized, it is necessary to set the path to the
        database, it will be passed to the global variable. If the database already
        exists, there will be an attempt to read it.

        Parameters
        ----------
        db_path : str
            Database path

        """
        global DB_PATH
        DB_PATH = db_path
        # Attempt to read it if exists
        if os.path.exists(DB_PATH):
            self.read()

    @staticmethod
    def read():
        """Database read method

        The method tries to read the database at the specified path in the global
        variable. If successful, it will put the data in the global cache.

        Raises
        ------
        json.decoder.JSONDecodeError
            If the data structure is incorrect and not empty

        """
        global DB_PATH
        global CACHE
        try:
            with open(DB_PATH, "r") as db:
                CACHE = json.load(db)
        except json.decoder.JSONDecodeError:
            """If the database structure is incorrect, but it is completely empty, the
            exception will be ignored."""
            if os.path.getsize(DB_PATH) == 0:
                CACHE = {}
            else:
                raise

    @staticmethod
    def write():
        """Database write method

        The method tries to write data from the global cache to the database at the
        specified path in the global variable.

        """
        global DB_PATH
        global CACHE
        with open(DB_PATH, "w") as db:
            json.dump(CACHE, db)


class Field(object):
    """The class represents the base field of a database document.

    Attributes
    ----------
    name : :obj:`str`, optional
        Field name.
    primary key : :obj:`bool`, optional
        True if the field is primary key.
    default : :obj:`object`, optional
        Default field value.

    """

    def __init__(
        self, name: str = None, primary_key: bool = False, default: object = None
    ):
        """Field initialization method

        Parameters
        ----------
        name : :obj:`str`, optional
            Field name. Defaults to document attribute name.
        primary_key : :obj:`bool`, optional
            True if the field is primary key. Defaults fo False.
        default : :obj:`object`, optional
            If the field value is not set, then the default value will be set.
            Defaults to None. The default value type must match the data type of the field.

        """
        self.name = name
        self.primary_key = primary_key
        self.default = default

    def __get__(self, instance, owner):
        """__get__ method override

        This override allows us to make the following declaration of a document possible:
            field = Field()
        Although the field is an instance of the Field class, we can set it to a value
        thanks to __set__ method:
            self.field = 2
        The __get__ method allows us to get the value stored in a special dict:
            foo = self.field # 2

        """
        if instance is None:
            return self

        return getattr(instance, "_data").get(self.name)

    def __set__(self, instance, value):
        """__set__ method override

        This override allows us to make the following declaration of a document possible:
            field = Field()
        Although the field is an instance of the Field class, we can set it to a value
        thanks to __set__ method:
            self.field = 2
        The __get__ method allows us to get the value stored in a special dict:
            foo = self.field # 2

        """
        if value is None and self.default is not None:
            value = self.default
        getattr(instance, "_data")[self.name] = value

    def __set_name__(self, owner, name):
        """This method assigns the name of the document attribute to the field, if it was
         not assigned."""
        if self.name is None:
            self.name = name


class DocumentMeta(type):
    """The class represents the metaclass of base database document class"""
    def __new__(mcs, name, bases, attrs):
        """Metaclass __new__ method allows us to perform actions on the class before
        __get__ and __set__ are overwritten.

        Raises
        ------
        MultiplePrimaryKey
            If more than one primary key field is detected

        """
        _meta = attrs["_meta"] = {"fields": [], "id_field": None}
        # _data stores field values
        _data = attrs["_data"] = {}

        if attrs.get("meta_class") == DocumentMeta:
            return super().__new__(mcs, name, bases, attrs)
        # The following operations will be performed only for document instances.
        # Field list will be filled with field instances, which will allow us to access
        # them directly, bypassing __get__ and __set__.
        # If a primary key field is detected, it will be recorded.
        for key, value in attrs.items():
            if isinstance(value, Field):
                _meta["fields"].append({key: value})
                if value.primary_key:
                    if _meta["id_field"] is None:
                        _meta["id_field"] = value.name
                    else:
                        raise MultiplePrimaryKey

        # If the field is not found, an id field will be created and a value will be
        # automatically assigned.
        if _meta["id_field"] is None:
            field_name = "id"
            id_field = Field(name=field_name, primary_key=True)
            _meta["id_field"] = field_name
            _meta["fields"].insert(0, {field_name: id_field})
            attrs[field_name] = id_field
            _data[field_name] = mcs._next_id(name)

        return super().__new__(mcs, name, bases, attrs)

    @staticmethod
    def _next_id(name: str) -> int:
        """Returns the next id for auto increment"""
        try:
            seq = [instance["id"] for instance in CACHE[name]]
            if seq:
                last_id = max(seq)
                return last_id + 1
            return 1
        except KeyError:
            return 1

    # noinspection PyUnusedLocal
    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(cls)

    @classmethod
    def __prepare__(mcs, cls, bases, **kwargs):
        return super().__prepare__(cls, bases, **kwargs)

    def __call__(cls, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class Document(object, metaclass=DocumentMeta):
    """The class represents the base database document"""

    meta_class = DocumentMeta
    """:obj:`DocumentMeta`: Indicates that this is a base document and does not apply to
    subclasses."""

    def delete(self):
        """Deletes existing document from the database

        Raises
        ------
        DocumentDoesNotExists
            If the document is not found in the database

        """
        global CACHE
        query = list(self.get_raw(**{self.pk: getattr(self, self.pk)}))
        if query:
            CACHE[self.get_class_name()].remove(query[0])
            WormDB.write()
        else:
            raise DocumentDoesNotExist

    @classmethod
    def get_raw(cls, **kwargs) -> list:
        """Returns a raw representation of documents by the specified parameters.

        Raw means that documents will be presented as dicts. This prevents them from
        being used as objects and methods will be inaccessible. This method allows us
        to get read-only document field values faster.

        Parameters
        ----------
        **kwargs
            Criteria for the selection of documents. Will return all documents if there
            are no criteria.

        Returns
        -------
        list
            List of documents as dicts. Empty list if not found.

        """
        try:
            seq = CACHE[cls.get_class_name()]
            for key, value in kwargs.items():
                seq = list(filter(lambda _instance: value == _instance[key], seq))
            return seq
        except KeyError:
            return []

    @classmethod
    def get(cls, **kwargs) -> list:
        """Returns documents by the specified parameters.

        Unlike raw documents, it will return full-fledged instances. All methods and
        parameters will be available.

        Parameters
        ----------
        **kwargs
            Criteria for the selection of documents. Will return all documents if there
            are no criteria.

        Returns
        -------
        list
            List of document instances. Empty list if not found.

        """
        seq = cls.get_raw(**kwargs)
        query = []
        for instance in seq:
            _instance = cls()
            for key, value in instance.items():
                getattr(_instance, "_data")[key] = value
            query.append(_instance)
        return query

    @classmethod
    def get_class_name(cls) -> str:
        """Gets the instance name

        Returns
        -------
        str
            Name of the instance

        """
        return cls.__name__

    @property
    def pk(self) -> str:
        """str: Primary key field name."""
        return getattr(self, "_meta")["id_field"]

    def save(self):
        """Saves a new document in the database

        Raises
        ------
        DocumentAlreadyExists
            If a document with this primary key already exists in the database

        """
        global CACHE
        data = self.to_dict()
        class_name = self.get_class_name()
        try:
            cache = CACHE[class_name]
        except KeyError:
            cache = CACHE[class_name] = []
        if not self.get(**{self.pk: getattr(self, "_data").get(self.pk)}):
            cache.append(data)
        else:
            raise DocumentAlreadyExists
        WormDB.write()

    def to_dict(self) -> dict:
        """Represents a raw document as a dict.

        Returns
        -------
        dict
            Dict of attribute names and values

        """
        return getattr(self, "_data")

    def update(self):
        """Updates existing document in the database

        Raises
        ------
        DocumentDoesNotExists
            If the document is not found in the database

        """
        global CACHE
        query = list(self.get_raw(**{self.pk: getattr(self, self.pk)}))
        if query:
            data = self.to_dict()
            class_name = self.get_class_name()
            _index = CACHE[class_name].index(query[0])
            CACHE[class_name][_index].update(data)
            WormDB.write()
        else:
            raise DocumentDoesNotExist


class MultiplePrimaryKey(Exception):

    def __str__(self):
        return "A document cannot have more than one primary key field."


class DocumentAlreadyExists(Exception):

    def __str__(self):
        return "A document with this primary key field value already exists in the database."


class DocumentDoesNotExist(Exception):

    def __str__(self):
        return "The document does not exist in the database."
