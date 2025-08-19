# AGT Data Enrichment - Multi-Client Platform

## Overview
AGT Data Enrichment is a scalable platform for automating research and cold email generation for multiple clients. The platform uses AI-powered research bots and email generators to create highly personalized outreach campaigns.

## 🏗️ Project Structure

```
AGT_Data_Enrichment/
├── clients/                          # All client projects
│   ├── schreiber/                   # Schreiber Foods project
│   │   ├── data/                    # CSV imports and data files
│   │   ├── scripts/                 # Client-specific scripts
│   │   ├── outputs/                 # Generated emails, research data
│   │   ├── config/                  # Client-specific configs
│   │   └── README.md                # Client project documentation
│   ├── [client2]/                   # Future client project
│   └── [client3]/                   # Future client project
├── core/                            # Shared, reusable components
│   ├── research_engine/             # Research bot framework
│   ├── email_generator/             # Email generation framework
│   ├── sheets_handler/              # Google Sheets integration
│   └── utils/                       # Common utilities
├── templates/                        # Reusable templates and prompts
│   ├── research_prompts/            # Research prompt templates
│   ├── email_prompts/               # Email prompt templates
│   └── config_templates/            # Configuration templates
├── docs/                            # Project documentation
├── test_scripts/                    # Testing and development scripts
└── README.md                        # This file
```

## 🚀 Key Features

- **Multi-Client Support**: Separate projects for each client with isolated configurations
- **AI-Powered Research**: Automated company and contact research using Perplexity Sonar API
- **Personalized Email Generation**: AI-generated cold emails using OpenAI GPT-4
- **Google Sheets Integration**: Automated data storage and management
- **Quality Control**: Research quality scoring and email validation
- **Scalable Architecture**: Reusable components for easy client onboarding

## 📚 Documentation

- **[Research Script Guide](docs/research_script.md)** - How to build research bots using Perplexity
- **[Email Writer Guide](docs/email_writer_script.md)** - How to create personalized cold email outreach
- **[Client Onboarding Template](templates/config_templates/client_onboarding_template.py)** - Template for adding new clients

## 🏢 Current Clients

### Schreiber Foods
- **Industry**: Dairy Manufacturing
- **Product**: Heat-Stable Cream Cheese
- **Focus**: Food industry contacts and pain points
- **Status**: Active development

## 🔧 Core Components

### Research Engine
- Perplexity Sonar API integration
- Automated company and contact research
- Research quality scoring
- Data validation and fallbacks

### Email Generator
- OpenAI GPT-4 integration
- Personalized email creation
- Three-email variation strategy
- CTA optimization and validation

### Google Sheets Handler
- Automated data writing
- Column mapping management
- Error handling and retry logic
- Data validation

## 🚀 Getting Started

### 1. Setup Environment
```bash
# Clone the repository
git clone [repository-url]
cd AGT_Data_Enrichment

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env_example.txt .env
# Edit .env with your API keys
```

### 2. Add a New Client
```bash
# Create client directory
mkdir -p clients/[client_name]/{data,scripts,outputs,config}

# Copy and customize the onboarding template
cp templates/config_templates/client_onboarding_template.py clients/[client_name]/config/client_config.py

# Edit the configuration file with client-specific settings
```

### 3. Run the Pipeline
```bash
# Navigate to client directory
cd clients/[client_name]/scripts

# Run the research and email pipeline
python super_pipeline.py
```

## 📁 Client Project Structure

Each client project follows this structure:
```
[client_name]/
├── data/           # CSV imports and data files
├── scripts/        # Client-specific scripts
├── outputs/        # Generated emails, research data
├── config/         # Client-specific configurations
└── README.md       # Client project documentation
```

## 🔑 Configuration

Each client has their own configuration file (`config/client_config.py`) containing:
- Client information and industry details
- API keys and endpoints
- Google Sheets configuration
- Research and email prompts
- Column mappings
- Quality thresholds

## 📊 Data Flow

1. **CSV Import** → `clients/[client]/data/` folder
2. **Research Collection** → Perplexity API → Google Sheets
3. **Email Generation** → OpenAI API → Google Sheets
4. **Outputs** → `clients/[client]/outputs/` folder

## 🧪 Testing

- Use scripts in `test_scripts/` for development and testing
- Each client can have their own test scripts
- Core components are tested independently

## 🔄 Knowledge Transfer

- **Shared Components**: Core research and email generation logic
- **Client Templates**: Reusable configuration templates
- **Best Practices**: Documented approaches and learnings
- **Prompt Libraries**: Industry-specific prompt collections

## 📈 Scaling

The platform is designed to scale horizontally:
- Add new clients by copying the template structure
- Customize configurations for each client's needs
- Share successful approaches across clients
- Maintain separation while enabling knowledge transfer

## 🤝 Contributing

1. Follow the established project structure
2. Use the client onboarding template for new clients
3. Document any new approaches or learnings
4. Test thoroughly before deploying to production

## 📞 Support

For questions or support:
- Check the documentation in `docs/`
- Review client-specific README files
- Use the test scripts for troubleshooting
- Refer to the onboarding template for configuration help

## 🔮 Future Enhancements

- **Multi-API Support**: Support for additional research and AI APIs
- **Advanced Analytics**: Research quality metrics and email performance tracking
- **Template Library**: Industry-specific prompt and email templates
- **Automation**: Scheduled research and email generation
- **Integration**: CRM and email marketing platform integrations

## Latest updates
- Local CSV workflow: pipeline reads/writes `clients/schreiber/data/Schreiber Sheet 5_11 test - Sheet1 (2).fixed.csv` (cleaned). No Google Sheets required.
- Primary key: `Contact id` is the authoritative row selector. Fallback: company + email.
- Guardrails (enforced in code):
  - No URLs in body; all links handled via CTA fields only.
  - CTA mapping: Email 1 "Simply reply to the email"; Email 2 "Visit our website"; Email 3 "Request a free sample". Supporting text is psychological and role/outcome-oriented; hyperlinks appended by the sending platform.
  - Plant-based/vegan context: never mention dairy in the same sentence; keep dairy suggestions separate/conditional.
  - Subjects: <= 50 chars, token-wise construction to avoid mid-word cuts; distinct, role/outcome-oriented; safe fallbacks ensure complete phrasing.
  - Post-generation validator cleans body (no CTAs/links), fixes conflicts, and enforces subject/CTA rules.

## Running the super pipeline
- Configure API keys in `.env`.
- Ensure the fixed CSV exists at `clients/schreiber/data/Schreiber Sheet 5_11 test - Sheet1 (2).fixed.csv` and contains `Contact id`.
- From `clients/schreiber/scripts/` run:
  - `python3 super_pipeline.py`
- The script processes a batch of Contact IDs (configurable in code) and writes:
  - Research to columns AV–AZ
  - Emails to columns BA–BL

## Personalization rules (summary)
- Icebreaker: role/company insight; no greeting; no name in the icebreaker.
- Body: include (1) explicit pain point, (2) role-specific benefit, (3) company anchor; no CTAs or URLs; introduce heat‑stable cream cheese tied to an outcome.
- CTAs: live in CTA columns only; no embedded URLs in body text.

## Troubleshooting
- Truncated subjects: handled by validator; token-wise trim + fallbacks.
- Wrong-row writes: confirm `Contact id` field; pipeline selects by ID.
- Mixed plant-based/dairy: validator decouples and rewrites conflicted sentences.
