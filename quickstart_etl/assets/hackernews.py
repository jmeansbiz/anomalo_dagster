import base64
import json
import os
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset, Field, AssetIn, AssetOut, Definitions, define_asset_job, op, graph, job, repository


@asset(config_schema={'api_key': Field(str, is_required=True), 'test_id': Field(int, is_required=True)}, group_name="anomalo", compute_kind="Anomalo API")
def run_anomalo_tests_asset(context: AssetExecutionContext):
    """Run Anomalo tests."""
    api_key = context.solid_config['api_key']
    test_id = context.solid_config['test_id']
    url = f"https://demo.anomalo.com/api/tests/{test_id}/run"
    headers = {'Authorization': f'Bearer {api_key}'}
    
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        test_results = response.json()
        with open("data/test_results.json", "w") as f:
            json.dump(test_results, f)
        return test_results
    else:
        raise Exception(f"Failed to run test: {response.text}")


@asset(deps=[AssetIn('run_anomalo_tests_asset')], group_name="anomalo", compute_kind="Anomalo API")
def verify_anomalo_results_asset(context: AssetExecutionContext, run_anomalo_tests_asset) -> MaterializeResult:
    """Verify Anomalo test results."""
    api_key = os.getenv("ANOMALO_API_KEY")
    test_id = int(os.getenv("ANOMALO_TEST_ID"))
    url = f"https://demo.anomalo.com/api/tests/{test_id}/results"
    headers = {'Authorization': f'Bearer {api_key}'}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        results = response.json()
        if results['status'] == 'passed':
            return MaterializeResult(metadata={"status": "passed"})
        else:
            return MaterializeResult(metadata={"status": "failed"})
    else:
        raise Exception(f"Failed to verify test results: {response.text}")


@asset(deps=[AssetIn('verify_anomalo_results_asset')], group_name="anomalo", compute_kind="Anomalo API")
def quarantine_bad_records_asset(context: AssetExecutionContext, verify_anomalo_results_asset) -> None:
    """Quarantine bad records if the tests fail."""
    if verify_anomalo_results_asset.metadata['status'] != 'passed':
        quarantine_table = os.getenv("QUARANTINE_TABLE")
        # Implement logic to move bad records to quarantine_table
        context.log.info(f"Bad records moved to {quarantine_table}")


@asset(deps=[AssetIn('verify_anomalo_results_asset')], group_name="anomalo", compute_kind="Next Task")
def run_next_task_asset(context: AssetExecutionContext, verify_anomalo_results_asset) -> None:
    """Run the next task if tests pass."""
    if verify_ano
