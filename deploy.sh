#!/bin/bash
# Multi-Bundle Deployment Script for BrickBrain

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to deploy a specific bundle
deploy_bundle() {
    local bundle_name=$1
    local bundle_path="bundles/$bundle_name"
    
    if [ ! -d "$bundle_path" ]; then
        print_error "Bundle directory $bundle_path does not exist!"
        return 1
    fi
    
    print_status "Deploying bundle: $bundle_name"
    
    # Validate the bundle
    print_status "Validating bundle configuration..."
    if ! databricks bundle validate "$bundle_path"; then
        print_error "Bundle validation failed for $bundle_name"
        return 1
    fi
    
    # Deploy the bundle
    print_status "Deploying to Databricks workspace..."
    if databricks bundle deploy "$bundle_path"; then
        print_success "Bundle $bundle_name deployed successfully!"
    else
        print_error "Bundle deployment failed for $bundle_name"
        return 1
    fi
}

# Function to run a specific bundle
run_bundle() {
    local bundle_name=$1
    local bundle_path="bundles/$bundle_name"
    
    print_status "Running bundle: $bundle_name"
    
    # List available resources
    print_status "Available resources in $bundle_name:"
    databricks bundle run --help "$bundle_path" 2>/dev/null || echo "No runnable resources found"
}

# Function to show available bundles
show_bundles() {
    print_status "Available bundles:"
    for bundle_dir in bundles/*/; do
        if [ -d "$bundle_dir" ] && [ -f "$bundle_dir/databricks.yml" ]; then
            bundle_name=$(basename "$bundle_dir")
            echo "  - $bundle_name"
        fi
    done
}

# Main script logic
case "${1:-help}" in
    "deploy")
        if [ -z "$2" ]; then
            print_error "Please specify a bundle name to deploy"
            echo "Usage: $0 deploy <bundle-name>"
            echo "Available bundles:"
            show_bundles
            exit 1
        fi
        deploy_bundle "$2"
        ;;
    "run")
        if [ -z "$2" ]; then
            print_error "Please specify a bundle name to run"
            echo "Usage: $0 run <bundle-name>"
            echo "Available bundles:"
            show_bundles
            exit 1
        fi
        run_bundle "$2"
        ;;
    "deploy-all")
        print_status "Deploying all bundles..."
        for bundle_dir in bundles/*/; do
            if [ -d "$bundle_dir" ] && [ -f "$bundle_dir/databricks.yml" ]; then
                bundle_name=$(basename "$bundle_dir")
                deploy_bundle "$bundle_name"
            fi
        done
        print_success "All bundles deployed successfully!"
        ;;
    "list")
        show_bundles
        ;;
    "help"|*)
        echo "BrickBrain Multi-Bundle Deployment Script"
        echo ""
        echo "Usage: $0 <command> [bundle-name]"
        echo ""
        echo "Commands:"
        echo "  deploy <bundle-name>    Deploy a specific bundle"
        echo "  run <bundle-name>       Run a specific bundle"
        echo "  deploy-all              Deploy all bundles"
        echo "  list                    List available bundles"
        echo "  help                    Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 deploy blog-scraper"
        echo "  $0 run blog-scraper"
        echo "  $0 deploy-all"
        echo "  $0 list"
        ;;
esac

