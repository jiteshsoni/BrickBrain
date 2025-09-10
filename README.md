# BrickBrain - Multi-Bundle Databricks Repository

A comprehensive repository containing multiple Databricks Asset Bundles for data engineering, ML, and automation tasks.

## 🏗️ Repository Structure

```
brickbrain/
├── bundles/                    # Individual Databricks Asset Bundles
│   ├── blog-scraper/          # Blog content scraping and processing
│   ├── data-pipeline/         # ETL and data processing pipelines
│   └── ml-models/             # Machine learning model training and inference
├── shared/                    # Shared utilities and configurations
│   ├── utils/                 # Common Python modules
│   └── configs/               # Shared configuration files
├── deploy.sh                  # Multi-bundle deployment script
└── README.md                  # This file
```

## 📦 Available Bundles

### 1. Blog Scraper Bundle (`bundles/blog-scraper/`)
- **Purpose**: Automated blog content scraping and storage
- **Schedule**: Daily at 6 AM UTC
- **Features**:
  - Scrapes content from multiple websites
  - Stores data in Delta tables with merge functionality
  - Supports hardcoded URLs and dynamic discovery
  - Comprehensive logging and error handling

### 2. Data Pipeline Bundle (`bundles/data-pipeline/`)
- **Purpose**: ETL and data processing workflows
- **Schedule**: Daily at 2 AM UTC
- **Features**:
  - Extract, Transform, Load (ETL) operations
  - Data quality checks and validation
  - Automated data pipeline orchestration

### 3. ML Models Bundle (`bundles/ml-models/`)
- **Purpose**: Machine learning model training and inference
- **Schedule**: 
  - Training: Weekly on Sunday at 3 AM UTC
  - Inference: Daily at 4 AM UTC
- **Features**:
  - Automated model training workflows
  - Model versioning with MLflow
  - Batch inference processing

## 🚀 Quick Start

### Prerequisites
- Databricks CLI (version 0.218.0+)
- Access to a Databricks workspace
- Configured authentication

### Deploy a Specific Bundle
```bash
# Deploy the blog scraper bundle
./deploy.sh deploy blog-scraper

# Deploy the data pipeline bundle
./deploy.sh deploy data-pipeline

# Deploy the ML models bundle
./deploy.sh deploy ml-models
```

### Deploy All Bundles
```bash
./deploy.sh deploy-all
```

### Run a Bundle
```bash
# Run the blog scraper pipeline
./deploy.sh run blog-scraper

# List available bundles
./deploy.sh list
```

## 🔧 Individual Bundle Usage

### Blog Scraper Bundle
```bash
# Navigate to the bundle directory
cd bundles/blog-scraper

# Deploy the bundle
databricks bundle deploy

# Run the pipeline
databricks bundle run blog_scraper_pipeline

# Run the daily job
databricks bundle run blog_scraper_daily_job
```

### Data Pipeline Bundle
```bash
cd bundles/data-pipeline
databricks bundle deploy
databricks bundle run data_pipeline_daily_job
```

### ML Models Bundle
```bash
cd bundles/ml-models
databricks bundle deploy
databricks bundle run model_training_job
databricks bundle run model_inference_job
```

## 📊 Monitoring and Results

### Blog Scraper Results
- **Delta Table**: `main.default.blog_content`
- **Query Results**: 
  ```sql
  SELECT COUNT(*) FROM main.default.blog_content;
  SELECT * FROM main.default.blog_content ORDER BY scraped_at DESC LIMIT 10;
  ```

### Job Monitoring
- View job runs in Databricks UI: **Workflows > Jobs**
- Monitor pipeline runs: **Workflows > Pipelines**
- Check logs in job run details

## ⚙️ Configuration

### Environment Variables
Set these environment variables for deployment:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
```

### Bundle-Specific Configuration
Each bundle has its own `databricks.yml` file with:
- Resource definitions (jobs, pipelines, clusters)
- Variable configurations
- Schedule settings
- Library dependencies

### Shared Configuration
Common configurations are stored in `shared/configs/`:
- `requirements.txt`: Python dependencies
- `urls.txt`: Hardcoded URLs for blog scraping

## 🔄 CI/CD Integration

### GitHub Actions Example
```yaml
name: Deploy BrickBrain Bundles
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install Databricks CLI
        run: pip install databricks-cli
      - name: Deploy All Bundles
        run: ./deploy.sh deploy-all
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

## 🛠️ Development

### Adding a New Bundle
1. Create a new directory in `bundles/`
2. Add a `databricks.yml` configuration file
3. Create necessary notebooks and source files
4. Update the deployment script if needed

### Shared Utilities
Common Python modules are stored in `shared/utils/`:
- `blog_scraper.py`: Core blog scraping functionality
- `run_scraper.py`: Blog scraper entry point

### Best Practices
- Use shared utilities to avoid code duplication
- Follow consistent naming conventions
- Include comprehensive logging
- Add proper error handling and retries
- Document all configuration options

## 📚 Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Bundle Configuration Reference](https://docs.databricks.com/aws/en/dev-tools/bundles/config.html)
- [CI/CD with Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd.html)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test the bundle deployment
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.