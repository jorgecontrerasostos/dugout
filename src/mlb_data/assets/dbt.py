from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from constants import MANIFEST_PATH


@dbt_assets(manifest=MANIFEST_PATH)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
