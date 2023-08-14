import re
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Optional, Union, Any, TypeVar, ClassVar, Type, Iterator
from typing_extensions import Protocol
from dataclasses import field
from functools import partialmethod

import requests
import backoff
from marshmallow_dataclass import dataclass, NewType
from marshmallow import Schema, fields, ValidationError

REDASH_BASE_URL = ''
REDASH_API_KEY = ''
REDSHIFT_HOST = ''
REDSHIFT_PORT = ''
REDSHIFT_USER = ''
REDSHIFT_PASSWORD = ''
REDSHIFT_DBNAME = ''

Jsondict = dict[str, Any]
JsonValue = Union[Jsondict, list[Jsondict]]

Email = NewType("Email", str, field=fields.Email)


class IsDataclass(Protocol):
    __dataclass_fields__: dict


Leaf = tuple[list[str], Any]


def _get_leaves(obj, current_path: list[str] = None) -> Optional[list[Leaf]]:
    """
    Returns a list of Leaves, each is a tuple of the path to the leaf and the leaf's value.
    e.g: {'a': {'b': 1, 'c': 2}} -> [(['a', 'b'], 1), (['a', 'c'], 2)]
    """
    if not isinstance(obj, dict):
        return None

    current_path = current_path or []
    paths = []
    for key, value in obj.items():
        item_path = current_path + [key]
        result = _get_leaves(value, item_path)
        if result is None:
            paths.append((item_path, value))
        elif isinstance(result, list):
            paths += result

    return paths


def get_leaves(obj) -> list[Leaf]:
    return _get_leaves(obj) or []


def pop_leaf(obj, path: list[str]):
    """
    Removes a leaf from an object in the given path
    """
    for key in path[:-1]:
        obj = obj[key]

    obj = obj.pop(path[-1])

    return obj


def add_leaf(obj, path, value):
    """
    Adds a leaf to an object in the given path
    """
    for key in path[:-1]:
        obj = obj[key]

    obj[path[-1]] = value


def _should_give_up(error: requests.exceptions.RequestException):
    return error.response.status_code < 500


class RedashApiClient:
    GET = 'GET'
    POST = 'POST'
    DELETE = 'DELETE'
    PUT = 'PUT'
    PATCH = 'PATCH'

    def __init__(self, base_url: str, api_key: str, timeout_seconds: Optional[int] = 30) -> None:
        self.base_url = base_url
        self.headers = {'Authorization': 'Key {api_key}'.format(api_key=api_key),
                        'Content-Type': 'application/json'}
        self.timeout_seconds = timeout_seconds

    def _url(self, endpoint: str) -> str:
        """Return the full URL for upcoming request

        :param endpoint: endpoint we would like to request
        :return: Full URL
        """
        return f'{self.base_url}/api/{endpoint}'

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=3, giveup=_should_give_up)
    def _request(self, endpoint: str, method: str, **kwargs) -> Optional[JsonValue]:
        res = requests.request(method=method, url=self._url(endpoint), headers=self.headers,
                               timeout=self.timeout_seconds, **kwargs)

        res.raise_for_status()
        return res.json() if res.content else None

    get = partialmethod(_request, method=GET)
    post = partialmethod(_request, method=POST)
    put = partialmethod(_request, method=PUT)
    delete = partialmethod(_request, method=DELETE)
    patch = partialmethod(_request, method=PATCH)


redash_client = RedashApiClient(
    base_url=REDASH_BASE_URL,
    api_key=REDASH_API_KEY
)

T = TypeVar('T', bound=IsDataclass)


class Entitylist(list):
    _unknown_fields: list


class RedashEntity:
    """
    This is an ORM for a redash entity, containing the basic functionality of an entity:
        - get
        - save
        - delete
        - fetch (refresh)
        - objects (get all)

    This base class serves as a wrapper to redash's REST API.

    When getting data from redash, the json response is converted to the appropriate Model (defined as a dataclass).
    When sending data to redash, the model is converted to json.

    The conversion is made using the Model's schema (marshmallow schema: https://marshmallow.readthedocs.io/) which
    is auto generated from definition of the Model (using marshmallow_dataclass:
    https://github.com/lovasoa/marshmallow_dataclass)

    In order to define a model, create a subclass with the dataclass decorator from marshmallow_dataclass and define
    the fields of the entity.

    e.g:

        @dataclass
        class User(RedashEntity:
            user_name: str
            password: str

    If one of the field is a different model but there isn't an API dedicated for that model, define it as just a
    dataclass:

        @dataclass
        class Address:
            street: str
            house_number: str
            postal_code: int

        @dataclass
        class User(RedashEntity):
            user_name: str
            password: str
            address: Address

    If a field is optional, define it with dataclasses.field as follows:

        @dataclass
        class Address:
            street: str = field(default=None)  # or other default value

    !!    DONT define it as:
    !!        street: str = None
    !!    OR
    !!        street: Optional[str]
    !!
    !!    If you do - it will raise an error when trying to generate the schema!

    To define additional properties of the fields, see the following docs:
        - marshmallow schema: https://marshmallow.readthedocs.io/
        - marshmallow_dataclass: https://github.com/lovasoa/marshmallow_dataclass
    """
    base_endpoint: ClassVar[str]
    Schema: ClassVar[Type[Schema]]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # if a model didn't define a base_endpoint, we use the model's name in snake_case in plural as the base_endpoint
        if not hasattr(cls, 'base_endpoint') and ABC not in cls.__bases__:
            cls.base_endpoint = re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower() + 's'

    @classmethod
    def schema(cls) -> Schema:
        """
        A singleton for the model's schema instance
        """
        if not hasattr(cls, '_schema'):
            cls._schema = cls.Schema()

        return cls._schema

    @classmethod
    def _load(cls: Type[T], obj: JsonValue, **kwargs) -> Union[T, list[T]]:
        """
        Loads an object using the model's schema.
        If any unknown fields are found, they are saved aside for when we dump the object to preserve it's original
        fields
        """
        try:
            entity = cls.schema().load(obj, **kwargs)
        except ValidationError as validation_error:
            # If we ran into validation errors, we want to use all the 'Unknown field' errors to save the unknown
            # fields aside, and raise the rest of the errors (if any)
            errors = validation_error.messages
            raise_validation_errors = False

            unknown_fields_paths = []
            for error_path, error in get_leaves(errors):
                if error == ['Unknown field.']:
                    unknown_fields_paths.append(error_path)
                    pop_leaf(errors, error_path)
                else:
                    raise_validation_errors = True

            if raise_validation_errors:
                raise ValidationError(errors)

            unknown_fields = [(path, pop_leaf(obj, path)) for path in unknown_fields_paths]
            entity = cls.schema().load(obj, **kwargs)
            if isinstance(entity, list):
                entity = Entitylist(entity)

            entity._unknown_fields = unknown_fields

        return entity

    @classmethod
    def load(cls: Type[T], obj: JsonValue, **kwargs) -> T:
        return cls._load(obj, **kwargs)

    @classmethod
    def load_many(cls: Type[T], obj: JsonValue, **kwargs) -> list[T]:
        return cls._load(obj, many=True, **kwargs)

    def dump(self) -> JsonValue:
        """
        Dumps an object using the model's schema and the validates it.
        If the object has any unknown fields that where found when it was loaded, they are added to the result json
        """
        json_value = self.schema().dump(self)
        errors = self.schema().validate(json_value)
        if errors:
            raise ValidationError(errors)

        for field_path, value in getattr(self, '_unknown_fields', {}):
            add_leaf(json_value, field_path, value)

        return json_value

    @property
    @abstractmethod
    def id(self):
        pass

    @classmethod
    def object_endpoint_by_id(cls, object_id: int) -> str:
        return f'{cls.base_endpoint}/{object_id}'

    @property
    def object_endpoint(self) -> str:
        return self.object_endpoint_by_id(self.id)

    @classmethod
    def objects(cls: Type[T]) -> list[T]:
        """
        Gets all entity objects
        """
        response = redash_client.get(cls.base_endpoint)
        return cls.load(response, many=True)

    @classmethod
    def get(cls: Type[T], object_id: int) -> Optional[T]:
        """
        Gets full entity data by id
        """
        try:
            response = redash_client.get(cls.object_endpoint_by_id(object_id))
            return cls.load(response)
        except requests.HTTPError as error:
            if error.response.status_code == 404:
                return None

            raise

    def fetch(self):
        """
        Gets full data for an existing object
        """
        response = redash_client.get(self.object_endpoint)
        self._init_from_object(self.load(response))

    def _init_from_object(self: T, obj: T):
        self.__init__(**{field_name: getattr(obj, field_name) for field_name in self.__dataclass_fields__})

    def save(self):
        """
        Saves the object if it has an id or create a new one otherwise.
        Afterwards it refreshes the fields in the object the the new data from the server (eg - the new id)
        """
        json_data = self.dump()

        if self.id:
            endpoint = self.object_endpoint
        else:
            endpoint = self.base_endpoint

        response = redash_client.post(endpoint, json=json_data)
        self._init_from_object(self.load(response))

    def delete(self):
        redash_client.delete(self.object_endpoint)


class IterableRedashEntity(ABC, RedashEntity):
    @classmethod
    def objects(cls: Type[T], page_size: int = 250, **params) -> Iterator[T]:
        """
        This method iterates through the entity objects by pagination, and each time returns the next object.
        After returning the last object of a page, it seamlessly gets the next page.
        @param page_size: Optionally, set the page_size for pagination of the objects, default is 250, which is the
        max page size redash supports
        """
        params['page_size'] = page_size
        response = redash_client.get(cls.base_endpoint, params=params)
        page = 1
        yield from cls.load_many(response['results'])

        objects_yielded = page_size
        while objects_yielded < response['count']:
            page += 1
            params['page'] = page
            response = redash_client.get(cls.base_endpoint, params=params)
            yield from cls.load_many(response['results'])
            objects_yielded += page_size


@dataclass
class DataSourceOptions:
    host: str = field(default=REDSHIFT_HOST, metadata=dict(missing=None))
    port: int = field(default=REDSHIFT_PORT, metadata=dict(missing=None))
    user: str = field(default=REDSHIFT_USER, metadata=dict(missing=None))
    password: str = field(default=REDSHIFT_PASSWORD, metadata=dict(missing=None))
    dbname: str = field(default=REDSHIFT_DBNAME, metadata=dict(missing=None))


@dataclass
class DataSource(RedashEntity):
    name: str
    options: DataSourceOptions = field(default_factory=DataSourceOptions, metadata=dict(missing=None))
    id: int = field(default=None)
    type: str = field(default='redshift')
    scheduled_queue_name: str = field(default=None)
    paused: int = field(default=None)
    pause_reason: str = field(default=None)
    queue_name: str = field(default=None)
    syntax: str = field(default=None)
    groups: dict[str, bool] = field(default=None)
    view_only: bool = field(default=None)


@dataclass
class Group(RedashEntity):
    name: str
    id: int = field(default=None)
    created_at: datetime = field(default=None)
    permissions: list[str] = field(default=None)
    type: str = field(default=None)

    @property
    def data_sources(self) -> list[DataSource]:
        response = redash_client.get(f'{self.object_endpoint}/data_sources')
        return DataSource.load_many(response)

    def add_data_source(self, data_source_id: int):
        redash_client.post(f'{self.object_endpoint}/data_sources', json=dict(id=self.id, data_source_id=data_source_id))

    def set_data_source_access(self, data_source_id: int, view_only: bool = True):
        redash_client.post(f'{self.object_endpoint}/data_sources/{data_source_id}', json=dict(view_only=view_only))

    def remove_data_source(self, data_source_id: int):
        redash_client.delete(f'{self.object_endpoint}/data_sources/{data_source_id}')

    def add_member(self, user_id):
        redash_client.post(f'{self.object_endpoint}/members', json=dict(user_id=user_id))

    def remove_member(self, user_id):
        redash_client.delete(f'{self.object_endpoint}/members/{user_id}')


@dataclass
class User(IterableRedashEntity):
    name: str
    email: Email
    id: int = field(default=None)
    auth_type: str = field(default=None)
    is_disabled: bool = field(default=None)
    profile_image_url: str = field(default=None)
    is_invitation_pending: bool = field(default=None)
    created_at: datetime = field(default=None)
    disabled_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)
    is_email_verified: bool = field(default=None)
    active_at: datetime = field(default=None)
    api_key: str = field(default=None)
    group_ids: list[int] = field(default=None)  # To change the user's groups, change this value and save

    # This is a READ-ONLY field, use group_ids to change the user's groups
    groups: Union[list[int], list[Group]] = field(default=None, metadata=dict(required=False, load_only=True))

    def __post_init__(self):
        if self.groups:
            self.group_ids = self.groups if isinstance(self.groups[0], int) else [group.id for group in self.groups]


@dataclass
class QuerySchedule:
    interval: int
    time: str = field(default=None)
    day_of_week: str = field(default=None)
    until: str = field(default=None)


@dataclass
class Visualization:
    description: str = field(default=None)
    name: str = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)
    id: int = field(default=None)
    type: str = field(default=None)
    options: Any = field(default=None)


@dataclass
class QueryParameter:
    name: str
    title: str
    value: Union[str, int, list[Union[str, int]]]
    type: str
    is_global: bool = field(metadata=dict(data_key='global'))
    locals: Any = field(default=None)
    queryId: int = field(default=None)
    enumOptions: str = field(default=None)


@dataclass
class QueryOptions:
    parameters: list[QueryParameter] = field(default=None)


@dataclass
class Query(IterableRedashEntity):
    base_endpoint = 'queries'
    name: str
    data_source_id: int
    query: str = field(default='')
    is_archived: bool = field(default=None)
    updated_at: datetime = field(default=None)
    is_favorite: bool = field(default=None)
    id: int = field(default=None)
    description: str = field(default=None)
    tags: list[str] = field(default=None)
    version: int = field(default=None)
    query_hash: str = field(default=None)
    api_key: str = field(default=None)
    is_safe: bool = field(default=None)
    latest_query_data_id: int = field(default=None)
    schedule: QuerySchedule = field(default=None)
    user: User = field(default=None)
    is_draft: bool = field(default=None)
    can_edit: bool = field(default=None)
    created_at: datetime = field(default=None)
    last_modified_by: User = field(default=None)
    visualizations: list[Visualization] = field(default=None)
    options: QueryOptions = field(default=None)

    def fork(self: T) -> T:
        response = redash_client.post(f'{self.object_endpoint}/fork')
        return self.load(response)


@dataclass
class WidgetVisualization(Visualization):
    query: Query = field(default=None)


@dataclass
class Widget(RedashEntity):
    dashboard_id: int
    visualization_id: int = field(default=None)
    visualization: WidgetVisualization = field(default=None, metadata=dict(load_only=True))
    text: str = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)
    options: Any = field(default=None)
    width: int = field(default=None)
    id: int = field(default=None)

    # Widgets don't have get, fetch or objects endpoints on redash, so to avoid 404 errors, this methods will instead
    # raise a NotImplementedError
    @classmethod
    def objects(cls):
        raise NotImplementedError()

    @classmethod
    def get(cls, object_id):
        raise NotImplementedError()

    def fetch(self):
        raise NotImplementedError()


@dataclass
class Dashboard(IterableRedashEntity):
    name: str
    tags: list[str] = field(default=None)
    is_archived: bool = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)
    is_favorite: bool = field(default=None)
    user: User = field(default=None)
    layout: Any = field(default=None, metadata=dict(load_only=True))
    is_draft: bool = field(default=False)
    id: int = field(default=None)
    can_edit: bool = field(default=True)
    user_id: int = field(default=None)
    slug: str = field(default=None)
    version: int = field(default=None)
    dashboard_filters_enabled: bool = field(default=True)
    widgets: list[Widget] = field(default=None, metadata=dict(load_only=True))

    @classmethod
    def get(cls: Type[T], slug: str) -> Optional[T]:
        """
        Gets full entity data by id
        """
        try:
            response = redash_client.get(f'dashboards/{slug}')
            return cls.load(response)
        except requests.HTTPError as error:
            if error.response.status_code == 404:
                return None

            raise

    def fetch(self):
        """
        Gets full data for an existing object
        """
        response = redash_client.get(f'dashboards/{self.slug}')
        self._init_from_object(self.load(response))

    def delete(self):
        redash_client.delete(f'dashboards/{self.slug}')
