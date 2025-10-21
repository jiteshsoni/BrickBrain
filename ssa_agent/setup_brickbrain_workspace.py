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
    python setup_brickbrain_workspace.py --run-job
    
Prerequisites:
    - Databricks CLI installed and configured
    - .env file in project root with required secrets (optional but recommended)

Author: BrickBrain Team
"""

import subprocess
import sys
import argparse
import json
import os
from pathlib import Path
from getpass import getpass
from typing import Dict, List, Tuple, Optional
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

def load_env_file(env_path: str = None) -> Dict[str, str]:
    """Load environment variables from .env file."""
    if env_path is None:
        # Look for .env in parent directory (project root)
        script_dir = Path(__file__).parent
        env_path = script_dir.parent / '.env'
    else:
        env_path = Path(env_path)
    
    env_vars = {}
    
    if not env_path.exists():
        print_warning(f".env file not found at {env_path}")
        return env_vars
    
    try:
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
        
        print_info(f"Loaded {len(env_vars)} variables from .env file")
        return env_vars
    except Exception as e:
        print_warning(f"Error reading .env file: {e}")
        return env_vars

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

def get_or_create_sql_warehouse(auto_select: bool = True) -> Optional[str]:
    """Get existing SQL warehouse ID or guide user to create one."""
    print("\n🏭 Checking SQL warehouses...")
    
    success, stdout, stderr = run_command(
        'databricks warehouses list --output json',
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
        except Exception as e:
            print_warning(f"Error parsing warehouses: {e}")
    
    print_warning("No SQL warehouse found")
    print_info("SQL warehouse is required for catalog/schema creation")
    
    if not auto_select:
        try:
            manual_id = input("\n  Enter SQL warehouse ID (or press Enter to skip catalog setup): ").strip()
            return manual_id if manual_id else None
        except EOFError:
            return None
    
    return None

def create_catalogs_and_schemas(warehouse_id: str, environment: str) -> bool:
    """Create Unity Catalog and schemas."""
    print_header("Step 2: Creating Unity Catalog & Schemas")
    
    # Use 'brickbrain' catalog with 'default' schema for all environments
    # This gives us full control without permission issues on 'main' catalog
    configs = {
        'dev': [('brickbrain', 'default')],
        'stage': [('brickbrain', 'default')],
        'prod': [('brickbrain', 'default')],
        'all': [('brickbrain', 'default')]
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
    
    # Get current user email for permissions
    success, user_email, _ = run_command('databricks current-user me --output json', check=False)
    if success:
        try:
            import json
            user_data = json.loads(user_email)
            user_email = user_data.get('userName', 'current user')
        except:
            user_email = 'current user'
    else:
        user_email = 'current user'
    
    for catalog, schema in catalog_schema_list:
        print(f"\n📦 Working with catalog: {catalog}")
        
        # Create catalog first (this gives us full ownership)
        print(f"  📦 Creating catalog: {catalog}")
        success, _, stderr = run_command(
            f'databricks catalogs create {catalog}',
            check=False
        )
        
        total_count += 1
        if success:
            print_success(f"Catalog created: {catalog}")
            success_count += 1
        elif "already exists" in stderr.lower() or "catalog already exists" in stderr.lower():
            print_success(f"Catalog already exists: {catalog}")
            success_count += 1
        else:
            print_error(f"Failed to create catalog: {stderr[:200]}")
            # Continue anyway, catalog might exist but command failed
        
        # Create schema in our own catalog (no permission issues!)
        print(f"  📁 Creating schema: {catalog}.{schema}")
        success, _, stderr = run_command(
            f'databricks schemas create {schema} {catalog}',
            check=False
        )
        
        total_count += 1
        if success:
            print_success(f"Schema created: {catalog}.{schema}")
            success_count += 1
        elif "already exists" in stderr.lower() or "schema already exists" in stderr.lower():
            print_success(f"Schema already exists: {catalog}.{schema}")
            success_count += 1
        else:
            print_error(f"Failed to create schema: {stderr[:200]}")
    
    print(f"\n📊 Result: {success_count}/{total_count} operations succeeded")
    return success_count == total_count

def setup_secrets(scope_name: str, env_vars: Dict[str, str] = None, skip_secrets: bool = False) -> bool:
    """Create secret scope and store secrets from .env file or interactive input."""
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
            f'databricks secrets create-scope {scope_name}',
            check=False
        )
        if success:
            print_success(f"Secret scope created: {scope_name}")
        else:
            print_error(f"Failed to create scope: {stderr}")
            return False
    else:
        print_success(f"Secret scope exists: {scope_name}")
    
    # Define required secrets with their .env variable names
    secrets_config = {
        'youtube_api_key': {
            'env_key': 'YOUTUBE_API_KEY',
            'description': 'YouTube Data API v3 key (optional for video ingestion)'
        },
        'webshare_proxy_username': {
            'env_key': 'WEBSHARE_PROXY_USERNAME',
            'description': 'Webshare proxy username (for YouTube API, optional)'
        },
        'webshare_proxy_password': {
            'env_key': 'WEBSHARE_PROXY_PASSWORD',
            'description': 'Webshare proxy password (optional)'
        }
    }
    
    print(f"\n🔑 Configuring {len(secrets_config)} secrets")
    
    # Try to use values from .env file first
    if env_vars:
        print_info("Using values from .env file")
        
        for key, config in secrets_config.items():
            env_key = config['env_key']
            value = env_vars.get(env_key)
            
            if value:
                print(f"  📌 {key}")
                cmd = f'databricks secrets put-secret {scope_name} {key}'
                success, _, stderr = run_command(cmd, input_text=value, check=False)
                
                if success:
                    print_success(f"Stored: {key}")
                else:
                    print_error(f"Failed: {stderr}")
            else:
                print_warning(f"No value found for {key} ({env_key}) in .env file, skipping")
    else:
        print_warning("No .env file found, secrets not configured")
        print_info("You can manually add secrets later using:")
        print(f"  databricks secrets put-secret {scope_name} <key>")
    
    # Verify secrets
    print("\n📋 Verifying secrets...")
    success, stdout, _ = run_command(
        f'databricks secrets list-secrets {scope_name}',
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
  # Full setup for dev environment (catalogs + secrets + deploy)
  python setup_brickbrain_workspace.py --environment dev
  
  # Setup and run data ingestion job
  python setup_brickbrain_workspace.py --run-job
  
  # Setup without deploying bundle
  python setup_brickbrain_workspace.py --skip-deploy
  
  # Setup all environments
  python setup_brickbrain_workspace.py --environment all

Prerequisites:
  - .env file in project root with YOUTUBE_API_KEY, WEBSHARE_PROXY_USERNAME, WEBSHARE_PROXY_PASSWORD
  - Databricks CLI configured with host and token
  - SQL warehouse available in workspace
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
        help='Skip secrets setup (by default, secrets are loaded from .env and deployed)'
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
    
    # Load .env file
    print_info("Loading environment variables from .env file...")
    env_vars = load_env_file()
    
    # Step 1: Verify Databricks CLI
    success, workspace_host = verify_databricks_cli()
    if not success:
        sys.exit(1)
    
    # Step 2: Create catalogs and schemas
    warehouse_id = get_or_create_sql_warehouse(auto_select=True)
    catalog_success = create_catalogs_and_schemas(warehouse_id, args.environment)
    
    # Step 3: Setup secrets (always run unless explicitly skipped)
    secrets_success = setup_secrets(args.secret_scope, env_vars, args.skip_secrets)
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
    print("  2. Verify secrets: databricks secrets list-secrets brickbrain_ssa_agent_scope")
    print(f"  3. Run job: databricks bundle run data_ingestion_job -t {args.environment}")
    print(f"  4. Monitor job: Check Databricks UI > Workflows")
    print(f"  5. View run URL in the output above\n")
    
    sys.exit(0)

if __name__ == '__main__':
    main()

