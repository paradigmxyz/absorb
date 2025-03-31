
def get_min_local_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_min_local_timestamp()


def get_max_local_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_max_local_timestamp()


def get_min_available_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_min_available_timestamp()


def get_max_available_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_max_available_timestamp()
