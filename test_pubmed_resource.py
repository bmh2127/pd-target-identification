#!/usr/bin/env python3
"""
Simple test script for PubMedResource to verify API key configuration
without importing the full Dagster definitions that might trigger NumPy issues.
"""

import sys
import os
from pathlib import Path

# Add the src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Test basic imports
try:
    from dotenv import load_dotenv
    print("✅ python-dotenv imported successfully")
    
    # Load environment variables
    load_dotenv()
    
    # Check if our environment variables are loaded
    api_key = os.getenv("NCBI_API_KEY")
    email = os.getenv("NCBI_EMAIL") 
    tool = os.getenv("NCBI_TOOL")
    
    print(f"📧 NCBI_EMAIL: {'✅ Set' if email else '❌ Missing'}")
    print(f"🔑 NCBI_API_KEY: {'✅ Set' if api_key else '❌ Missing'}")
    print(f"🛠️  NCBI_TOOL: {tool or 'Using default'}")
    
    if api_key:
        print(f"🔑 API Key (first 8 chars): {api_key[:8]}...")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Run: uv add python-dotenv")
    sys.exit(1)

# Test direct resource import (avoiding full Dagster definitions)
try:
    # Import just the resource class without triggering the full __init__ chain
    import importlib.util
    
    spec = importlib.util.spec_from_file_location(
        "resources", 
        src_path / "pd_target_identification" / "defs" / "shared" / "resources.py"
    )
    resources_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(resources_module)
    
    PubMedResource = resources_module.PubMedResource
    
    print("✅ PubMedResource imported successfully")
    
    # Test resource instantiation
    pubmed = PubMedResource()
    pubmed._log_configuration()
    
    print(f"⏱️  Rate limit delay: {pubmed.rate_limit_delay} seconds")
    print(f"📍 Base URL: {pubmed.base_url}")
    
    # Test API parameters
    params = pubmed._get_common_params()
    print(f"🔧 Common params: {list(params.keys())}")
    
    print("✅ PubMedResource configuration test passed!")
    
except Exception as e:
    print(f"❌ Error testing PubMedResource: {e}")
    import traceback
    traceback.print_exc()

print("\n💡 Next steps:")
print("1. Make sure you've created .env file with your credentials")
print("2. Run: uv install  # to install numpy<2.0 constraint")
print("3. Run: uv sync     # to sync dependencies") 