#!/usr/bin/env python3
"""
BrickBrain Workspace Setup - All-in-One
========================================
Complete setup script for deploying BrickBrain to a new Databricks workspace.

This script will:
1. Verify Databricks CLI configuration
2. Create Unity Catalog and schemas
3. Set up secret scope and secrets
4. Validate and deploy Databricks bundle
5. Optionally run the data ingestion job

Usage:
    python setup_brickbrain_workspace.py
    python setup_brickbrain_workspace.py --environment dev
    python setup_brickbrain_workspace.py --skip-secrets --skip-deploy
    python setup_brickbrain_workspace.py --run-job

Author: BrickBrain Team
"""

import subprocess
import sys
import argparse
import json
from getpass import getpass
from typing import Dict, List, Tuple
import time

# ANSI color codes
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    """Print formatted header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text:^70}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.ENDC}\n")

def print_success(text: str):
    """Print success message."""
    print(f"{Colors.GREEN}✅ {text}{Colors.ENDC}")

def print_error(text: str):
    """Print error message."""
    print(f"{Colors.RED}❌ {text}{Colors.ENDC}")

def print_warning(text: str):
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.ENDC}")

def print_info(text: str):
    """Print info message."""
    print(f"{Colors.CYAN}ℹ️  {text}{Colors.ENDC}")

def run_command(cmd: str, input_text: str = None, check: bool = True) -> Tuple[bool, str, str]:
    """Execute shell command and return success status and output."""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            input=input_text
        )
        
        if check and result.returncode != 0:
            return False, result.stdout, result.stderr
        
        return True, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def verify_databricks_cli() -> Tuple[bool, str]:
    """Verify Databricks CLI is installed and configured."""
    print_header("Step 1: Verifying Databricks CLI")
    
    # Check if CLI is installed
    print("🔍 Checking Databricks CLI installation...")
    success, stdout, stderr = run_command('databricks --version', check=False)
    
    if not success:
        print_error("Databricks CLI not installed")
        print_info("Install: pip install databricks-cli")
        return False, None
    
    print_success(f"Databricks CLI installed: {stdout.strip()}")
    
    # Check if configured
    print("\n🔍 Checking Databricks CLI configuration...")
    success, stdout, stderr = run_command('databricks workspace list / --output json', check=False)
    
    if not success:
        print_error("Databricks CLI not configured")
        print_info("Run: databricks configure --token")
        return False, None
    
    # Get workspace host
    success, host, _ = run_command('databricks config get host', check=False)
    host = host.strip() if success else "Unknown"
    
    print_success(f"Connected to workspace: {host}")
    
    return True, host

def get_or_create_sql_warehouse() -> str:
    """Get existing SQL warehouse ID or guide user to create one."""
    print("\n🏭 Checking SQL warehouses...")
    
    success, stdout, stderr = run_command(
        'databricks sql warehouses list --output json',
        check=False
    )
    
    if success and stdout:
        try:
            warehouses = json.loads(stdout)
            if warehouses and len(warehouses) > 0:
                wh_id = warehouses[0].get('id')
                wh_name = warehouses[0].get('name', 'Unknown')
                print_success(f"Using SQL warehouse: {wh_name} ({wh_id})")
                return wh_id
        except:
            pass
    
    print_warning("No SQL warehouse found")
    print_info("SQL warehouse is required for catalog/schema creation")
    
    manual_id = input("\n  Enter SQL warehouse ID (or press Enter to skip catalog setup): ").strip()
    return manual_id if manual_id else None

def create_catalogs_and_schemas(warehouse_id: str, environment: str) -> bool:
    """Create Unity Catalog and schemas."""
    print_header("Step 2: Creating Unity Catalog & Schemas")
    
    # Define catalog configurations with consistent naming
    configs = {
        'dev': [('main', 'brickbrain_dev')],
        'stage': [('main', 'brickbrain_stg')],
        'prod': [('main', 'brickbrain_prod')],
        'all': [
            ('main', 'brickbrain_dev'),
            ('main', 'brickbrain_stg'),
            ('main', 'brickbrain_prod')
        ]
    }
    
    catalog_schema_list = configs.get(environment, configs['dev'])
    
    if not warehouse_id:
        print_warning("No SQL warehouse ID provided")
        print_info("Run these SQL commands manually in Databricks SQL Editor:\n")
        for catalog, schema in catalog_schema_list:
            print(f"  CREATE CATALOG IF NOT EXISTS `{catalog}`;")
            print(f"  CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;")
        print()
        return False
    
    success_count = 0
    total_count = 0
    
    for catalog, schema in catalog_schema_list:
        print(f"\n📦 Creating catalog: {catalog}")
        
        # Create catalog
        sql = f"CREATE CATALOG IF NOT EXISTS `{catalog}`"
        cmd = f'databricks sql warehouse execute --warehouse-id {warehouse_id} --statement "{sql}"'
        
        total_count += 1
        success, _, stderr = run_command(cmd, check=False)
        
        if success:
            print_success(f"Catalog ready: {catalog}")
            success_count += 1
        else:
            print_error(f"Failed to create catalog: {stderr[:100]}")
        
        # Create schema
        print(f"  📁 Creating schema: {catalog}.{schema}")
        sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
        cmd = f'databricks sql warehouse execute --warehouse-id {warehouse_id} --statement "{sql}"'
        
        total_count += 1
        success, _, stderr = run_command(cmd, check=False)
        
        if success:
            print_success(f"Schema ready: {catalog}.{schema}")
            success_count += 1
        else:
            print_error(f"Failed to create schema: {stderr[:100]}")
    
    print(f"\n📊 Result: {success_count}/{total_count} operations succeeded")
    return success_count == total_count

def setup_secrets(scope_name: str, skip_secrets: bool) -> bool:
    """Create secret scope and store secrets."""
    print_header("Step 3: Setting up Databricks Secrets")
    
    if skip_secrets:
        print_warning("Skipping secrets setup (--skip-secrets flag)")
        return True
    
    # Check if scope exists
    print(f"🔍 Checking secret scope: {scope_name}")
    success, stdout, _ = run_command(
        'databricks secrets list-scopes --output json',
        check=False
    )
    
    scope_exists = scope_name in stdout if success else False
    
    if not scope_exists:
        print(f"📦 Creating secret scope: {scope_name}")
        success, _, stderr = run_command(
            f'databricks secrets create-scope --scope {scope_name}',
            check=False
        )
        if success:
            print_success(f"Secret scope created: {scope_name}")
        else:
            print_error(f"Failed to create scope: {stderr}")
            return False
    else:
        print_success(f"Secret scope exists: {scope_name}")
    
    # Define required secrets
    secrets_config = {
        'webshare_proxy_username': 'Webshare proxy username (for YouTube API, optional)',
        'webshare_proxy_password': 'Webshare proxy password (optional)',
        'youtube_api_key': 'YouTube Data API v3 key (optional for video ingestion)'
    }
    
    print(f"\n🔑 Configuring {len(secrets_config)} secrets")
    print_info("Press Enter to skip optional secrets\n")
    
    for key, description in secrets_config.items():
        print(f"  📌 {key}")
        print(f"     {description}")
        
        skip = input(f"     Skip this secret? (y/N): ").lower() == 'y'
        
        if skip:
            print(f"     ⏭️  Skipped\n")
            continue
        
        value = getpass(f"     Enter value (hidden): ")
        
        if value:
            cmd = f'databricks secrets put --scope {scope_name} --key {key}'
            success, _, stderr = run_command(cmd, input_text=value, check=False)
            
            if success:
                print_success(f"Stored: {key}\n")
            else:
                print_error(f"Failed: {stderr}\n")
        else:
            print_warning(f"No value provided, skipped\n")
    
    # Verify secrets
    print("📋 Verifying secrets...")
    success, stdout, _ = run_command(
        f'databricks secrets list --scope {scope_name}',
        check=False
    )
    if success:
        print(stdout)
    
    return True

def deploy_bundle(environment: str, skip_deploy: bool) -> bool:
    """Validate and deploy Databricks bundle."""
    print_header("Step 4: Deploying Databricks Bundle")
    
    if skip_deploy:
        print_warning("Skipping bundle deployment (--skip-deploy flag)")
        return True
    
    # Validate bundle
    print(f"🔍 Validating bundle for environment: {environment}")
    success, stdout, stderr = run_command(
        f'databricks bundle validate -t {environment}',
        check=False
    )
    
    if not success:
        print_error("Bundle validation failed")
        print(stderr)
        return False
    
    print_success("Bundle validation passed")
    print(stdout)
    
    # Deploy bundle
    print(f"\n🚀 Deploying bundle to {environment}...")
    success, stdout, stderr = run_command(
        f'databricks bundle deploy -t {environment}',
        check=False
    )
    
    if not success:
        print_error("Bundle deployment failed")
        print(stderr)
        return False
    
    print_success("Bundle deployed successfully")
    print(stdout)
    
    # Get bundle summary
    print("\n📊 Bundle summary:")
    success, stdout, _ = run_command(
        f'databricks bundle summary -t {environment}',
        check=False
    )
    if success:
        print(stdout)
    
    return True

def run_data_ingestion_job(environment: str) -> bool:
    """Run the data ingestion job."""
    print_header("Step 5: Running Data Ingestion Job")
    
    print(f"🚀 Starting data ingestion job in {environment}...")
    print_info("This may take several minutes...\n")
    
    success, stdout, stderr = run_command(
        f'databricks bundle run data_ingestion_job -t {environment}',
        check=False
    )
    
    if success:
        print_success("Job completed successfully!")
        print(stdout)
        return True
    else:
        print_error("Job failed")
        print(stderr)
        return False

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='BrickBrain Workspace Setup - All-in-One',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full setup for dev environment
  python setup_brickbrain_workspace.py --environment dev
  
  # Setup and run job
  python setup_brickbrain_workspace.py --run-job
  
  # Skip secrets and deploy only
  python setup_brickbrain_workspace.py --skip-secrets
  
  # Setup all environments
  python setup_brickbrain_workspace.py --environment all
        """
    )
    
    parser.add_argument(
        '--environment', '-e',
        choices=['dev', 'stage', 'prod', 'all'],
        default='dev',
        help='Target environment (default: dev)'
    )
    
    parser.add_argument(
        '--secret-scope',
        default='brickbrain_ssa_agent_scope',
        help='Secret scope name (default: brickbrain_ssa_agent_scope)'
    )
    
    parser.add_argument(
        '--skip-secrets',
        action='store_true',
        help='Skip secrets setup'
    )
    
    parser.add_argument(
        '--skip-deploy',
        action='store_true',
        help='Skip bundle deployment'
    )
    
    parser.add_argument(
        '--run-job',
        action='store_true',
        help='Run data ingestion job after deployment'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print(f"\n{Colors.BOLD}{Colors.CYAN}")
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║                                                                    ║")
    print("║                🧱  BrickBrain Workspace Setup 🧠                   ║")
    print("║                                                                    ║")
    print("║                   All-in-One Deployment Script                    ║")
    print("║                                                                    ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.ENDC}\n")
    
    print(f"📋 Configuration:")
    print(f"   Environment: {args.environment}")
    print(f"   Secret Scope: {args.secret_scope}")
    print(f"   Skip Secrets: {args.skip_secrets}")
    print(f"   Skip Deploy: {args.skip_deploy}")
    print(f"   Run Job: {args.run_job}")
    
    # Step 1: Verify Databricks CLI
    success, workspace_host = verify_databricks_cli()
    if not success:
        sys.exit(1)
    
    # Step 2: Create catalogs and schemas
    warehouse_id = get_or_create_sql_warehouse()
    catalog_success = create_catalogs_and_schemas(warehouse_id, args.environment)
    
    # Step 3: Setup secrets
    secrets_success = setup_secrets(args.secret_scope, args.skip_secrets)
    if not secrets_success:
        print_warning("Secrets setup had issues, but continuing...")
    
    # Step 4: Deploy bundle
    deploy_success = deploy_bundle(args.environment, args.skip_deploy)
    if not deploy_success:
        print_error("Bundle deployment failed")
        sys.exit(1)
    
    # Step 5: Run job (optional)
    if args.run_job and not args.skip_deploy:
        job_success = run_data_ingestion_job(args.environment)
        if not job_success:
            print_warning("Job execution failed, but deployment is complete")
    
    # Final summary
    print_header("✅ Setup Complete!")
    
    print(f"{Colors.GREEN}{'='*70}{Colors.ENDC}")
    print(f"{Colors.BOLD}Summary:{Colors.ENDC}")
    print(f"  Workspace: {workspace_host}")
    print(f"  Environment: {args.environment}")
    print(f"  Catalogs: {'✅' if catalog_success else '⚠️ '}")
    print(f"  Secrets: {'✅' if secrets_success else '⚠️ '}")
    print(f"  Bundle: {'✅' if deploy_success else '❌'}")
    print(f"{Colors.GREEN}{'='*70}{Colors.ENDC}\n")
    
    print(f"{Colors.BOLD}Next Steps:{Colors.ENDC}")
    print("  1. Verify catalogs: databricks catalogs list")
    print("  2. Verify secrets: databricks secrets list --scope brickbrain_ssa_agent_scope")
    print(f"  3. Run job: databricks bundle run data_ingestion_job -t {args.environment}")
    print(f"  4. Monitor job: Check Databricks UI > Workflows\n")
    
    sys.exit(0)

if __name__ == '__main__':
    main()

