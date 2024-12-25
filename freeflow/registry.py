class ClassRegistry():
    REGISTRY = {}

    @classmethod
    def class_register_class(cls, other):
        typename = other.__typename__
        version = other.__version__

        if typename is None or version is None:
            return

        if typename not in cls.REGISTRY.keys():
            cls.REGISTRY[typename] = {}

        if version not in cls.REGISTRY[typename].keys():
            cls.REGISTRY[typename][version] = other

    @classmethod
    def get_registered_class(cls, typename, version):
        return cls.REGISTRY[typename][version]


class ExtRegistry(ClassRegistry):
    REGISTRY = {}


class ExtRegister(type):
    __typename__ = None
    __version__ = None

    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        ExtRegistry.class_register_class(cls)
        return cls
