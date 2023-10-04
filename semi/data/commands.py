"""
Weaviate CLI data group functions.
"""

import sys
import json
import click
import time
import weaviate

from semi.utils import get_client_from_context
from semi.prompt import is_question_answer_yes


@click.group("data", help="Data object manipulation in weaviate.")
def data_group():
    pass


@data_group.command("import", help="Import data from json file.")
@click.pass_context
@click.argument('file')
@click.option('--fail-on-error', required=False, default=False, is_flag=True, help="Fail if entity loading throws an error")
def concept_import(ctx, file, fail_on_error):
    import_data_from_file(get_client_from_context(ctx), file, fail_on_error)


@data_group.command("delete", help="Delete all data objects in weaviate.")
@click.pass_context
@click.option('--force', required=False, default=False, is_flag=True)
def data_empty(ctx, force):
    delete_all_data(get_client_from_context(ctx), force)

# Our new custom command for exporting
# TODO: add callback prompt for file if that file already exists
# TODO: support restricting export to subset of classes in Weaviate
# TODO: support restricting properties exported from each class (will require validating list of properties against the class' schema)
# TODO: export *all* classes as a default
@data_group.command("export", help="Export data to json file.")
@click.pass_context
@click.argument('file')
@click.option('--batch-size', '-b', required=False, default=150, is_flag=False, help="Number of objects to retrieve per request", type=int)
@click.option('--vectors/--no-vectors', 'include_vectors', required=False, default=True, is_flag=True, help="Include the vector property on exported objects")
@click.option("--pretty/--no-pretty", "pretty", required=False, default=False, is_flag=True, help="Write the exported file in a more human-readable format")
@click.option('--fail-on-error', required=False, default=True, is_flag=True, help="Fail if export throws an error")
def export_classes(ctx, file, batch_size, include_vectors: bool, pretty:bool, fail_on_error: bool=True):
    print("retrieving entities...")

    # TODO: parameterize class_names & fields
    # TODO: remove profiling code
    read_start = time.perf_counter_ns()
    entities = export_class(get_client_from_context(ctx), "WebPage", batch_size, include_vectors, fail_on_error)

    with open(file, 'x', encoding="utf-8") as f:
        print("writing file...")

        format_options = {
            "indent": 4 if pretty else None,
            "separators": (',', ': ') if pretty else (',', ':'),
        }

        json.dump(dict(classes=entities, timestamp=time.time()), f, **format_options)
        write_finish = time.perf_counter_ns()

        total_time = (write_finish - read_start) / 1000000000
        print(f"exported {len(entities)} entities in {total_time:0.4f}s\n")


####################################################################################################
# Helper functions
####################################################################################################

def export_class(client: weaviate.Client, class_name, batch_size, include_vectors: bool, fail_on_error: bool=False) -> list:
    cursor = None

    class_schema = client.schema.get(class_name)
    class_properties = [prop.get('name') for prop in class_schema.get('properties')]

    results = []

    # Don't love this while True, python does not have do-while loops
    while True:
        next_results = _get_batch_with_cursor(client, class_name, class_properties, batch_size, include_vectors, cursor)

        if len(next_results["data"]["Get"][class_name]) == 0:
            break

        entities = next_results["data"]["Get"][class_name]
        for entity in entities:
            formatted_entity = {
                "class": class_name,
                "id": entity["_additional"]["id"],
                "properties": {prop: entity[prop] for prop in class_properties},
            }
            if include_vectors:
                formatted_entity['vector'] = entity["_additional"]['vector']

            results.append(formatted_entity)

        cursor = next_results["data"]["Get"][class_name][-1]["_additional"]["id"]

    return results


def _get_batch_with_cursor(client: weaviate.Client, class_name, class_properties, batch_size: int, include_vectors: bool, cursor=None):
    query = (
        client.query.get(class_name, class_properties)
        .with_additional(["id", "vector" if include_vectors else ""])
        .with_limit(batch_size)
    )
    if cursor is not None:
        return query.with_after(cursor).do()
    else:
        return query.do()


def delete_all_data(client: weaviate.Client, force: bool) -> None:
    """
    Delete all weaviate objects.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    force : bool
        If True force delete all objects, if False ask for permission.
    """

    if force:
        _delete_all(client)
        sys.exit(0)
    if not is_question_answer_yes("Do you really want to delete all data?"):
        sys.exit(0)
    _delete_all(client)


def _delete_all(client: weaviate.Client):
    """
    Delete all weaviate data.

    Parameters
    ----------
    client : weaviate.Client
        A weaviate client.
    """

    schema = client.schema.get()
    client.schema.delete_all()
    client.schema.create(schema)


def import_data_from_file(client: weaviate.Client, file: str, fail_on_error: bool) -> None:
    """
    Import data from a file.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    file : str
        The data file path.
    fail_on_error : bool
        If True exits at the first error, if False prints the error only.
    """

    importer = DataFileImporter(client, file, fail_on_error)
    importer.load()


####################################################################################################
# DataFileImporter
####################################################################################################


class DataFileImporter:

    def __init__(self, client: weaviate.Client, data_path: str, fail_on_error: bool):
        """
        Initialize a DataFileImporter.

        Parameters
        ----------
        client : weaviate.Client
            A weaviate client.
        data_path : str
            The data file path.
        fail_on_error : bool
            If True exits at the first error, if False prints the error only.
        """

        self.client = client
        self.fail_on_error = fail_on_error

        self.batcher = client.batch(
            batch_size=512,
            callback=self._exit_on_error,
        )

        with open(data_path, 'r') as data_io:
            self.data = json.load(data_io)

    def _exit_on_error(self, batch_results: list):
        """
        Exit if an error occurred.

        Parameters
        ----------
        batch_results : list
            weaviate batch create results.
        """

        for entry in batch_results:
            result = entry.get('result', {})
            error = result.get('errors')
            if error is not None:
                print(error)
                if self.fail_on_error:
                    sys.exit(1)

    def load(self) -> None:
        """
        Load data into weaviate.
        """

        schema = self.client.schema.get()
        print("Validating data")
        vasd = ValidateAndSplitData(self.data, schema)
        vasd.validate_and_split()
        print("Importing data")
        for obj in vasd.data_objects:
            self.batcher.add_data_object(**obj)
        for ref in vasd.data_references:
            self.batcher.add_reference(**ref)
        self.batcher.flush()


class ValidateAndSplitData:

    def __init__(self, data: dict, schema: dict):
        """
        Initialize a ValidateAndSplitData class instance.

        Parameters
        ----------
        data : dict
            The objects to be validated.
        schema : dict
            The schema against which to validate the objects.
        """

        self.data = data
        self.schema = dissect_schema(schema)
        self.data_objects = []
        self.data_references = []

    def validate_and_split(self) -> None:
        """
        Go through the entire data and validate it against a schema
        if not valid exit with error, if valid split it into the
        primitive object and the references.
        """

        for obj in self.data.get("classes", []):
            self._validate_obj(obj)

    def _validate_obj(self, obj):
        obj_class_name = obj.get('class')
        schema_definition = self.schema.get(obj_class_name)
        object_id = obj.get('id')

        # Check if class exists
        if schema_definition is None:
            _exit_validation_failed(f"Class {obj_class_name} not in schema!")

        import_object_parameter = {
            'class_name': obj_class_name,
            'uuid': object_id,
            'data_object': {},
        }

        # Support for custom vectors
        vector = obj.get('vector')
        if vector is not None:
            import_object_parameter['vector'] = vector

        for obj_property_name, obj_property_val in obj.get('properties', {}).items():
            if obj_property_name in schema_definition['primitive']:
                # property is primitive -> add to data import list
                import_object_parameter['data_object'][obj_property_name] = obj_property_val
            elif obj_property_name in schema_definition['ref']:
                # property is reference to a different object
                # convert property into batch request parameters
                ref_parameters = dissect_reference(obj_property_val, obj_class_name, object_id, obj_property_name)
                self.data_references += ref_parameters
            else:
                _exit_validation_failed(f"Property {obj_property_name} of class {obj_class_name} not in schema!")
        self.data_objects.append(import_object_parameter)


def _exit_validation_failed(reason: str):
    """
    Exit if validation failed.

    Parameters
    ----------
    reason : str
        Message to print.
    """

    print("Error validation failed:", reason)
    sys.exit(1)


def dissect_reference(refs: list, from_class: str, from_id: str, from_prop: str) -> list:
    """
    Dissect a reference list into the parametes required for a batch request.

    Parameters
    ----------
    refs : list
        A list of references to be dissected.
    from_class : str
        The object's class name.
    from_id : str
        The id of the object.
    from_prop : str
        The property name.

    Returns
    -------
    list
        A list of batcher parameters used to upload data.
    """

    result = []
    for ref in refs:
        beacon_split = ref.get('beacon', 'e').split('/')
        ref_batch_parameters = {
            "from_object_uuid": from_id,
            "from_object_class_name": from_class,
            "from_property_name": from_prop,
            "to_object_uuid": beacon_split[-1]
        }
        result.append(ref_batch_parameters)
    return result


def dissect_schema(schema: dict) -> dict:
    """
    Dissect the schema into a dict listing all classes with their name as key to have faster
    validation access.

    Parameters
    ----------
    schema : dict
        The schema as exported from weaviate

    Returns
    -------
    dict
        A dict with each class and separated primitive and complex properties.
    """
    dissected = {
    }
    for class_ in schema.get('classes', []):
        prim, ref = _get_schema_properties(class_['properties'])
        dissected[class_['class']] = {
            'primitive': prim,
            'ref': ref,
        }
    return dissected


def _get_schema_properties(properties: list) -> tuple:
    """
    Split properties into references and primitive types.

    Parameters
    ----------
    properties : list
         A list of class properties.

    Returns
    -------
    tuple
        A tuple of two lists of property names one for primitives one for references.
    """

    properties_primitive = []
    properties_reference = []
    for property_ in properties:
        if is_primitive_prop(property_["dataType"][0]):
            properties_primitive.append(property_['name'])
        else:
            properties_reference.append(property_['name'])
    return (properties_primitive, properties_reference)


def is_primitive_prop(data_type: str) -> bool:
    """
    Check if property is a primitive one.

    Parameters
    ----------
    data_type : str
        Property data type.

    Returns
    -------
    bool
        True if 'data_type' is of a primitive property.
    """

    return data_type in ['text', 'string', 'int', 'boolean', 'number', 'date', 'geoCoordinates', \
                         'phoneNumber']
