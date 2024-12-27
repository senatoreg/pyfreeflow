import importlib

__EXTENSIONS__ = [
    "file_operator",
    "buffer_operator",
    "rest_api_requester",
    "data_transformer",
    "pgsql_executor",
]

for m in __EXTENSIONS__:
    ext_name = ".".join([__name__, m])
    importlib.import_module(ext_name)
