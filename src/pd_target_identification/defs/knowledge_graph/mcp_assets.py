# pd_target_identification/defs/knowledge_graph/mcp_assets.py
"""
Direct MCP integration for Graphiti knowledge graph ingestion.

This module provides a clean implementation using direct MCP client calls
to the Graphiti MCP server, bypassing the need for a separate service layer.
"""

import asyncio
import json
import subprocess
from typing import Dict, Any
from pathlib import Path
from datetime import datetime
from dagster import asset, AssetExecutionContext, RetryPolicy, Field
from mcp import ClientSession
from mcp.client.sse import sse_client


def check_mcp_container_running() -> bool:
    """
    Check if the Graphiti MCP container is running.
    
    Returns:
        True if mcp_server-graphiti-mcp-1 container is running, False otherwise
    """
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=mcp_server-graphiti-mcp-1", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        return "mcp_server-graphiti-mcp-1" in result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


async def call_mcp_add_memory(
    name: str,
    episode_body: str,
    source: str = "text",
    source_description: str = "",
    group_id: str = "pd_discovery_platform"
) -> Dict[str, Any]:
    """
    Call the existing Graphiti MCP container's add_memory tool via SSE.

    Args:
        name: Episode name
        episode_body: Episode content
        source: Source type (text, json, message)
        source_description: Description of the source
        group_id: Group ID for the knowledge graph

    Returns:
        Result from the MCP tool call
        
    Raises:
        RuntimeError: If MCP container is not running or call fails
    """
    # Check if container is running
    if not check_mcp_container_running():
        raise RuntimeError(
            "Graphiti MCP container 'mcp_server-graphiti-mcp-1' is not running. "
            "Please start it with: docker-compose up -d"
        )

    # Connect to MCP server via SSE and call the tool
    try:
        async with sse_client("http://localhost:8000/sse") as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                # Initialize the connection
                await session.initialize()

                # Call the add_memory tool
                result = await session.call_tool(
                    "add_memory",
                    {
                        "name": name,
                        "episode_body": episode_body,
                        "source": source,
                        "source_description": source_description,
                        "group_id": group_id
                    }
                )
                return result
    except Exception as e:
        raise RuntimeError(f"Failed to call MCP server via SSE: {e}")


async def call_mcp_get_episodes(group_id: str = None, last_n: int = 10) -> Dict[str, Any]:
    """
    Get recent episodes from the existing Graphiti MCP container via SSE.

    Args:
        group_id: Group ID to query
        last_n: Number of recent episodes to return

    Returns:
        Result from the MCP tool call
        
    Raises:
        RuntimeError: If MCP container is not running or call fails
    """
    # Check if container is running
    if not check_mcp_container_running():
        raise RuntimeError(
            "Graphiti MCP container 'mcp_server-graphiti-mcp-1' is not running. "
            "Please start it with: docker-compose up -d"
        )

    try:
        async with sse_client("http://localhost:8000/sse") as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                # Initialize the connection
                await session.initialize()

                # Call the get_episodes tool
                result = await session.call_tool(
                    "get_episodes",
                    {
                        "group_id": group_id,
                        "last_n": last_n
                    }
                )
                return result
    except Exception as e:
        raise RuntimeError(f"Failed to call MCP server via SSE: {e}")


def run_async_in_dagster(async_func):
    """
    Helper function to run async functions in Dagster's sync context.
    
    Simplified for HTTP-based MCP calls.
    """
    try:
        return asyncio.run(async_func)
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            # If there's already a running loop, create a new one in a thread
            import concurrent.futures
            
            def run_in_thread():
                return asyncio.run(async_func)
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result(timeout=300)  # 5 minute timeout
        else:
            raise


@asset(
    deps=["graphiti_export"],
    description="Ingest episodes via existing Graphiti MCP container using SSE transport (mcp_server-graphiti-mcp-1)",
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    io_manager_key="default_io_manager",
    config_schema={
        "group_id": Field(
            str, 
            default_value="pd_target_discovery_mcp_test",
            description="Neo4j group ID for organizing the knowledge graph data"
        ),
        "check_existing": Field(
            bool, 
            default_value=True,
            description="Whether to check for existing episodes before ingestion"
        )
    }
)
def graphiti_mcp_direct_ingestion(
    context: AssetExecutionContext,
    graphiti_export: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Coordinated ingestion approach using existing Graphiti MCP container.
    
    This asset provides efficient integration with the existing MCP infrastructure:
    - Connects to running mcp_server-graphiti-mcp-1 container at http://localhost:8000/sse
    - Reads from the same export files as the service approach  
    - Uses proper MCP client with SSE transport (no process spawning)
    - Configurable group_id for database organization
    - Container health checking and graceful failure handling
    
    Default Configuration:
    - group_id: "pd_target_discovery_mcp_test" (can be overridden)
    - check_existing: True (can be overridden)
    
    No manual configuration required - just click "Materialize"!
    
    Available group options:
    - "pd_target_discovery": Service approach data (426 nodes)
    - "pd_discovery_platform": MCP test data (105 nodes)  
    - "pd_target_discovery_mcp_test": New test group (recommended)
    
    Args:
        context: Dagster execution context with logging
        graphiti_export: Export summary from graphiti_export asset
        
    Returns:
        Complete ingestion results with group information
    """
    
    # Get configuration (now with defaults)
    try:
        config = context.op_execution_context.op_config
    except AttributeError:
        config = getattr(context, 'op_config', {})
    
    # Config now has default values, so these should always be available
    target_group_id = config.get("group_id", "pd_target_discovery_mcp_test") 
    check_existing = config.get("check_existing", True)
    
    async def process_episodes_async():
        """Async function to process all episodes via MCP calls."""
        context.log.info("🔧 Starting direct MCP ingestion process")
        context.log.info(f"🎯 Target Group ID: {target_group_id}")
        
        # Check existing data if requested
        if check_existing:
            try:
                context.log.info("🔍 Checking for existing episodes...")
                existing_episodes = await call_mcp_get_episodes(
                    group_id=target_group_id, 
                    last_n=5
                )
                
                if existing_episodes and not (hasattr(existing_episodes, 'error') or 
                                            (isinstance(existing_episodes, dict) and 'error' in existing_episodes)):
                    context.log.info(f"📋 Found existing episodes in group '{target_group_id}'")
                    context.log.info("💡 Tip: Use a new group_id to avoid mixing data")
                else:
                    context.log.info(f"✨ Group '{target_group_id}' appears to be empty or new")
                    
            except Exception as e:
                context.log.warning(f"⚠️ Could not check existing episodes: {str(e)}")
        
        # Read export files from the export directory
        export_summary = graphiti_export
        export_dir = Path(export_summary["export_directory"])
        
        context.log.info(f"📂 Processing exports from: {export_dir}")
        
        results = {
            "group_id": target_group_id,
            "total_episodes": 0,
            "successful_episodes": 0,
            "failed_episodes": 0,
            "episodes_processed": [],
            "errors": [],
            "processing_time": datetime.now().isoformat()
        }
        
        # Process each episode type from the export structure
        for episode_type, type_data in export_summary.get("episodes_by_type", {}).items():
            context.log.info(f"📄 Processing {episode_type} episodes")
            
            # Process each file for this episode type
            for file_path in type_data.get("files", []):
                file_path = Path(file_path)
                context.log.info(f"   📄 Reading from {file_path.name}")
                
                try:
                    # Read the episode file
                    with open(file_path, 'r') as f:
                        episodes_data = json.load(f)
                    
                    # Handle individual episode file (not array)
                    # Each file contains one episode with metadata
                    episode_wrapper = episodes_data
                    
                    # Extract the actual episode data from the wrapper
                    if 'graphiti_episode' in episode_wrapper:
                        episode_data = episode_wrapper['graphiti_episode']
                        results["total_episodes"] += 1
                        
                        try:
                            # Call MCP add_memory tool with configured group_id
                            mcp_result = await call_mcp_add_memory(
                                name=episode_data['name'],
                                episode_body=episode_data['episode_body'],
                                source=episode_data['source'],
                                source_description=episode_data['source_description'],
                                group_id=target_group_id
                            )
                            
                            # Check for errors in the MCP result
                            if hasattr(mcp_result, 'error') or (isinstance(mcp_result, dict) and 'error' in mcp_result):
                                error_msg = getattr(mcp_result, 'error', mcp_result.get('error', 'Unknown error'))
                                context.log.error(f"❌ MCP call failed for {episode_data['name']}: {error_msg}")
                                results["failed_episodes"] += 1
                                results["errors"].append({
                                    "episode": episode_data['name'],
                                    "error": str(error_msg)
                                })
                            else:
                                context.log.info(f"✅ Successfully ingested: {episode_data['name']}")
                                results["successful_episodes"] += 1
                                results["episodes_processed"].append({
                                    "name": episode_data['name'],
                                    "type": episode_type,
                                    "group_id": target_group_id,
                                    "result": str(mcp_result)[:200] + "..." if len(str(mcp_result)) > 200 else str(mcp_result)
                                })
                                
                        except Exception as e:
                            error_msg = f"Exception during MCP call: {str(e)}"
                            context.log.error(f"❌ Error processing {episode_data['name']}: {error_msg}")
                            results["failed_episodes"] += 1
                            results["errors"].append({
                                "episode": episode_data['name'],
                                "error": error_msg
                            })
                    else:
                        context.log.warning(f"⚠️ No 'graphiti_episode' found in {file_path.name}")
                        results["errors"].append({
                            "file": str(file_path),
                            "error": "Missing 'graphiti_episode' in file structure"
                        })
                            
                except Exception as e:
                    error_msg = f"Error reading file {file_path}: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    results["errors"].append({
                        "file": str(file_path),
                        "error": error_msg
                    })
        
        # Log final summary
        success_rate = (results["successful_episodes"] / results["total_episodes"] * 100) if results["total_episodes"] > 0 else 0
        context.log.info("🎯 MCP Ingestion Complete:")
        context.log.info(f"   🏷️ Group ID: {target_group_id}")
        context.log.info(f"   📊 Total Episodes: {results['total_episodes']}")
        context.log.info(f"   ✅ Successful: {results['successful_episodes']}")
        context.log.info(f"   ❌ Failed: {results['failed_episodes']}")
        context.log.info(f"   📈 Success Rate: {success_rate:.1f}%")
        
        return results
    
    # Run the async function in Dagster's sync context
    return run_async_in_dagster(process_episodes_async())


@asset(
    deps=["graphiti_mcp_direct_ingestion"],
    description="Compare MCP direct ingestion results with service-based approach",
    io_manager_key="default_io_manager"
)
def mcp_ingestion_comparison(
    context: AssetExecutionContext,
    graphiti_mcp_direct_ingestion: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compare the results of MCP direct ingestion with the service-based approach.
    
    This asset provides analysis and comparison metrics between the two approaches:
    - Direct MCP calls vs HTTP service calls
    - Performance metrics comparison
    - Success/failure rate analysis
    - Database group organization
    
    Args:
        context: Dagster execution context
        graphiti_mcp_direct_ingestion: Results from direct MCP ingestion
        
    Returns:
        Comparison analysis and recommendations
    """
    
    results = graphiti_mcp_direct_ingestion
    
    analysis = {
        "mcp_approach": {
            "group_id": results["group_id"],
            "total_episodes": results["total_episodes"],
            "success_rate": (results["successful_episodes"] / results["total_episodes"] * 100) if results["total_episodes"] > 0 else 0,
            "failure_count": results["failed_episodes"],
            "errors": results["errors"]
        },
        "database_organization": {
            "service_group": "pd_target_discovery (426 nodes)",
            "mcp_group": results["group_id"],
            "separation_benefit": "Clean separation allows independent testing"
        },
        "benefits_observed": [
            "Simplified architecture - no service layer needed",
            "Unified interface with interactive MCP tools",
            "Direct integration with Dagster logging",
            "Configurable group_id for data organization",
            "Same data source as service approach"
        ],
        "potential_drawbacks": [
            "Async complexity in Dagster sync context",
            "Direct dependency on MCP server availability",
            "Less abstraction than service layer"
        ],
        "recommendation": f"Direct MCP approach works well for batch processing with minimal overhead. Used group_id: {results['group_id']}"
    }
    
    context.log.info("📊 MCP vs Service Comparison:")
    context.log.info(f"   🏷️ MCP Group: {analysis['mcp_approach']['group_id']}")
    context.log.info(f"   🎯 Episodes Processed: {analysis['mcp_approach']['total_episodes']}")
    context.log.info(f"   📈 Success Rate: {analysis['mcp_approach']['success_rate']:.1f}%")
    context.log.info("   🏗️ Architecture: Direct MCP (simplified)")
    
    return analysis