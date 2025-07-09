# Spanish Data Validator

## Overview

This is a Flask web application designed to validate and clean Spanish user data from CSV files. The application specializes in validating Spanish identification documents (DNI, NIE, CIF), email addresses, and normalizing phone numbers. It provides a simple web interface for uploading CSV files and downloading processed results.

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes

- **July 9, 2025**: Added comprehensive email validation using email-validator library
- **July 9, 2025**: Configured database connectivity with MariaDB using environment variables
- **July 9, 2025**: Added .env file support for secure credential management
- **July 9, 2025**: Created database connection testing functionality

## System Architecture

### Frontend Architecture
- **Technology**: HTML templates with Bootstrap 5 (dark theme)
- **Framework**: Jinja2 templating engine integrated with Flask
- **UI Components**: 
  - File upload form with drag-and-drop interface
  - Results display with summary statistics
  - Flash messaging system for user feedback
- **Styling**: Bootstrap 5 with dark theme and Bootstrap Icons

### Backend Architecture
- **Framework**: Flask (Python web framework)
- **Architecture Pattern**: Simple MVC pattern
- **Request Handling**: RESTful endpoints for file upload and processing
- **File Processing**: Pandas for CSV manipulation and data validation
- **Security**: Werkzeug utilities for secure file handling

## Key Components

### Data Validation Module (`validators.py`)
- **Purpose**: Validates Spanish identification documents, email addresses, and phone numbers
- **DNI Validation**: 8-digit number + control letter validation
- **NIE Validation**: Foreign identification number validation (X/Y/Z prefix)
- **CIF Validation**: Company identification number validation
- **Email Validation**: RFC-compliant email validation with normalization
- **Phone Normalization**: Cleans and standardizes Spanish phone numbers

### File Processing System
- **Upload Handler**: Secure file upload with extension validation
- **Processing Pipeline**: CSV parsing → validation → results generation
- **Download System**: Processed file generation and delivery
- **File Storage**: Local filesystem with configurable upload/download folders

### Web Interface
- **Main Page** (`index.html`): File upload interface with instructions
- **Results Page** (`results.html`): Validation results with statistics and download options
- **Flash Messaging**: User feedback system for errors and success messages

## Data Flow

1. **File Upload**: User uploads CSV file through web interface
2. **Validation**: Flask validates file type and size constraints
3. **Processing**: 
   - CSV is parsed using Pandas
   - Each record is validated for DNI/NIE/CIF, email (if present), and phone numbers
   - Results are compiled with statistics and error details
4. **Output Generation**: Processed CSV is generated with validation results
5. **Results Display**: Summary statistics and download link are presented

## External Dependencies

### Core Dependencies
- **Flask**: Web framework for HTTP handling and templating
- **Pandas**: Data manipulation and CSV processing
- **Werkzeug**: Security utilities and file handling
- **email-validator**: RFC-compliant email validation and normalization
- **pymysql**: MySQL/MariaDB database connectivity
- **python-dotenv**: Environment variable management from .env files

### Frontend Dependencies
- **Bootstrap 5**: CSS framework for responsive UI
- **Bootstrap Icons**: Icon library for visual elements
- **CDN Delivery**: External CSS/JS resources loaded via CDN

### Python Standard Library
- **os**: File system operations
- **re**: Regular expressions for pattern matching
- **uuid**: Unique identifier generation
- **datetime**: Timestamp handling
- **logging**: Application logging

## Deployment Strategy

### Development Configuration
- **Host**: 0.0.0.0 (all interfaces)
- **Port**: 5000
- **Debug Mode**: Enabled for development
- **File Limits**: 16MB maximum upload size

### Production Considerations
- **Proxy Support**: ProxyFix middleware for reverse proxy deployments
- **Session Security**: Environment-based secret key configuration
- **File Management**: Automatic directory creation for uploads/downloads
- **Error Handling**: Comprehensive logging and user feedback

### File System Requirements
- **Upload Directory**: `static/uploads/` for temporary file storage
- **Download Directory**: `static/downloads/` for processed results
- **Permissions**: Write access required for file operations
- **Cleanup**: Manual cleanup required (no automatic file removal implemented)

### Security Features
- **File Type Restriction**: Only CSV files allowed
- **Filename Sanitization**: Werkzeug secure_filename utility
- **File Size Limits**: 16MB maximum to prevent abuse
- **Input Validation**: Comprehensive validation of identification documents