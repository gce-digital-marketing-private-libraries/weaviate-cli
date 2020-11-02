import click
from semi.config.configuration import Configuration
from semi.commands.schema import import_schema, export_schema, truncate_schema
from semi.commands.ping import ping
from semi.commands.data import delete_all_data

@click.group()
@click.pass_context
def main(ctx):
    ctx.obj = {
        "config": Configuration()
    }

# First order commands
@main.group("schema", help="Importing and exporting schema files.")
def schema_group():
    pass

@main.group("config", help="Configuration of the CLI.")
def config_group():
    pass

@main.group("data", help="Data object manipulation in weaviate.")
def data_group():
    pass

# @main.group("cloud")
# def cloud_group():
#     pass


@main.command("ping", help="Check if the configured weaviate is reachable.")
@click.pass_context
def main_ping(ctx):
    ping(_get_config_from_context(ctx))

@main.command("version", help="Version of the CLI")
def main_version():
    print("TODO impl")

# schema
@schema_group.command("import", help="Import a weaviate schema from a json file.")
@click.pass_context
@click.argument('filename')
#@click.option('--force', required=False, default=False, type=bool, nargs=0)
@click.option('--force', required=False, default=False, is_flag=True)
def schema_import(ctx, filename, force):
    import_schema(_get_config_from_context(ctx), filename, force)

@schema_group.command("export", help="Export a weaviate schema to into a json file.")
@click.pass_context
@click.argument('filename')
def schema_export(ctx, filename):
    export_schema(_get_config_from_context(ctx), filename)

@schema_group.command("truncate", help="Remove the entire schema and all the data associated with it.")
@click.pass_context
def schema_truncate(ctx):
    truncate_schema(_get_config_from_context(ctx))


# config
@config_group.command("view", help="Print the current CLI configuration.")
@click.pass_context
def config_view(ctx):
    print(ctx.obj["config"])

@config_group.command("set", help="Set a new CLI configuration.")
@click.pass_context
def config_set(ctx):
    _get_config_from_context(ctx).init()

# concept
# @data_group.command("import")
# def concept_import():
#     click.echo("TODO impl")

@data_group.command("empty")
@click.pass_context
def concept_empty(ctx):
    delete_all_data(_get_config_from_context(ctx))

# @cloud_group.command("create")
# def cloud_create():
#     click.echo("TODO impl")
#
# @cloud_group.command("delete")
# def cloud_delete():
#     click.echo("TODO impl")


def _get_config_from_context(ctx):
    """

    :param ctx:
    :return:
    :rtype: semi.config.configuration.Configuration
    """
    return ctx.obj["config"]


if __name__ == "__main__":
    main()