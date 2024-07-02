import os
import json
import requests
from dagster import asset, AssetIn, Output, AssetExecutionContext, MaterializeResult


@asset(config_schema={'api_key': str, 'test_id': int}, key_prefix="anomalo", compute_kind="Anomalo API")
def run_anomalo_tests_asset(context: AssetExecutionContext):
    """Run Anomalo tests."""
    api_key = context.op_config['api_key']
    test_id = context.op_config['test_id']
    url = f"https://demo.anomalo.com/api/tests/{test_id}/run"
    headers = {'Authorization': f'Bearer {api_key}'}
    
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        test_results = response.json()
        with open("data/test_results.json", "w") as f:
            json.dump(test_results, f)
        return Output(test_results)
    else:
        raise Exception(f"Failed to run test: {response.text}")


@asset(deps=[AssetIn("anomalo/run_anomalo_tests_asset")], key_prefix="anomalo", compute_kind="Anomalo API")
def verify_anomalo_results_asset(context: AssetExecutionContext, run_anomalo_tests_asset) -> MaterializeResult:
    """Verify Anomalo test results."""
    api_key = os.getenv("ANOMALO_API_KEY")
    test_id = int(os.getenv("ANOMALO_TEST_ID"))
    url = f"https://demo.anomalo.com/api/tests/{test_id}/results"
    headers = {'Authorization': f'Bearer {api_key}'}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        results = response.json()
        status = "passed" if results['status'] == 'passed' else "failed"
        return MaterializeResult(output=status, metadata={"status": status})
    else:
        raise Exception(f"Failed to verify test results: {response.text}")


@asset(deps=[AssetIn("anomalo/verify_anomalo_results_asset")], key_prefix="anomalo", compute_kind="Anomalo API")
def quarantine_bad_records_asset(context: AssetExecutionContext, verify_anomalo_results_asset) -> None:
    """Quarantine bad records if the tests fail."""
    if verify_anomalo_results_asset.metadata['status'] != 'passed':
        quarantine_table = os.getenv("QUARANTINE_TABLE")
        # Implement logic to move bad records to quarantine_table
        context.log.info(f"Bad records moved to {quarantine_table}")


@asset(deps=[AssetIn("anomalo/verify_anomalo_results_asset")], key_prefix="anomalo", compute_kind="Next Task")
def run_next_task_asset(context: AssetExecutionContext, verify_anomalo_results_asset) -> None:
    """Run the next task if tests pass."""
    if verify_anomalo_results_asset.metadata['status'] == 'passed':
        # Implement the logic for the next task
        context.log.info("Running next task...")
