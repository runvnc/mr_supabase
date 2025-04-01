from lib.providers.services import service
from lib.providers.commands import command
from lib.pipelines.pipe import pipe
from typing import Dict, List, Any, Optional, Union
import os
import json
import traceback
from .client import SupabaseClient
from .postgres_client import PostgresClient
from .utils import (
    load_agent_db_settings,
    extract_schema_info,
    clean_db_schema_from_messages,
    DB_SCHEMA_START_DELIMITER,
    DB_SCHEMA_END_DELIMITER,
    format_error_response
)
from lib.utils.debug import debug_box

# Initialize Supabase client service
@service()
async def get_db_client():
    """Get the Supabase client instance."""
    try:
        return SupabaseClient.get_instance()
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        traceback.print_exc()
        return None

# Helper function to format schema info from PostgreSQL data
def format_schema_from_postgres_data(tables_info: Dict[str, Any]) -> str:
    """Format schema information from PostgreSQL data for injection into agent context.
    
    Args:
        tables_info: Dictionary with table schemas and relationships
        
    Returns:
        Formatted schema string
    """
    schema_text = "DATABASE SCHEMA INFORMATION:\n\n"
    
    # Add tables and columns
    for table_name, table_info in tables_info.items():
        schema_text += f"Table: {table_name}\n"
        
        # Add columns
        schema_text += "Columns:\n"
        for column in table_info.get("columns", []):
            nullable = "NULL" if column.get("is_nullable") == "YES" else "NOT NULL"
            default = f" DEFAULT {column.get('column_default')}" if column.get("column_default") else ""
            schema_text += f"  - {column.get('column_name')}: {column.get('data_type')} {nullable}{default}\n"
        
        # Add relationships
        relations = table_info.get("relationships", [])
        if relations:
            schema_text += "Relationships:\n"
            for rel in relations:
                schema_text += f"  - {rel.get('column_name')} → {rel.get('foreign_table_name')}.{rel.get('foreign_column_name')}\n"
        
        schema_text += "\n"
    
    return schema_text

# Helper function to get all table names
async def get_all_table_names(use_postgres: bool, pg_client=None, db_client=None) -> List[str]:
    """Get all table names from the database.
    
    Args:
        use_postgres: Whether to use PostgreSQL client
        pg_client: PostgreSQL client instance (if use_postgres is True)
        db_client: Supabase client instance (if use_postgres is False)
        
    Returns:
        List of table names
    """
    try:
        if use_postgres and pg_client is not None:
            tables = pg_client.list_tables()
            if tables and isinstance(tables[0], dict) and 'table_name' in tables[0]:
                return [t.get('table_name') for t in tables if t.get('table_name')]
            return tables
        elif db_client:
            tables = await db_client.list_tables()
            if tables and isinstance(tables[0], dict) and 'table_name' in tables[0]:
                return [t.get('table_name') for t in tables if t.get('table_name')]
            return tables
        return []
    except Exception as e:
        print(f"Error getting table names: {e}")
        return []
        
# Service to inject schema info
@service()
async def db_inject_schema_info(agent_name: str, tables: List[str] = None):
    """Inject database schema information into agent context.

    Args:
        agent_name: Name of the agent
        tables: Optional list of tables to include (if None, uses agent settings)

    Returns:
        Formatted schema information
    """
    try:
        # Try to use PostgreSQL client for schema information
        try:
            pg_client = PostgresClient.get_instance()
            db_client = None  # Initialize to None when using PostgreSQL
            use_postgres = True
        except ValueError as e:
            print(f"PostgreSQL client not available: {e}. Falling back to Supabase client.")
            use_postgres = False
            db_client = await get_db_client()
            if not db_client:
                return "Error: Database client unavailable"

        # If no tables specified, load from agent settings
        if tables is None:
            settings = load_agent_db_settings(agent_name)
            enabled_tables = settings.get("enabled_tables", [])
            tables = enabled_tables if enabled_tables else None

        if not tables:
            # Get all tables instead of returning None
            tables = await get_all_table_names(use_postgres, pg_client, db_client)
            if not tables:
                debug_box("No tables found in database")
                return "No tables found in database."
            else:
                debug_box(f"Found {len(tables)} tables in database")

        # Get schema information for each table
        debug_box(f"Getting schema info for {len(tables)} tables")
        
        # If we're using PostgreSQL, initialize Supabase client for fallback
        # because some tables might not be accessible via PostgreSQL
        if use_postgres and db_client is None:
            try:
                db_client = await get_db_client()
            except Exception as e:
                debug_box(f"Warning: Could not initialize Supabase client for fallback: {e}")
                # Continue without Supabase fallback
                pass
            
        tables_info = {}
        tables_with_errors = []
        
        for table in tables:
            debug_box(f"Getting schema for table: {table}")
            columns = None
            relationships = None
            
            if use_postgres:
                # Use PostgreSQL client for schema information
                try:
                    columns = pg_client.describe_table(table)
                    relationships = pg_client.get_table_relationships(table)
                except Exception as e:
                    print(f"Error getting schema info with PostgreSQL client: {e}")
                    debug_box(f"Error getting schema for table {table} with PostgreSQL, trying Supabase fallback")
                    if db_client:
                        # Try Supabase as fallback
                        try:
                            columns = await db_client.describe_table(table)
                            relationships = await db_client.get_table_relationships(table)
                        except Exception as inner_e:  
                            print(f"Both PostgreSQL and Supabase failed for table {table}: {inner_e}")
                            tables_with_errors.append(table)
                            continue
            elif db_client:  # If not using PostgreSQL, use Supabase client
                # Fallback to Supabase client
                # Get columns
                try:
                    columns = await db_client.describe_table(table)
                    relationships = await db_client.get_table_relationships(table)
                except Exception as e:
                    print(f"Error getting schema for table {table} with Supabase: {e}")
                    tables_with_errors.append(table)
                    continue
            
            # Only add table info if we successfully got columns
            if columns is not None:
                tables_info[table] = {
                    "columns": columns,
                    "relationships": relationships or []
                }
                
        # Check if we got any table information successfully
        if not tables_info:
            debug_box(f"Failed to get schema for any tables. Errors in {len(tables_with_errors)} tables.")
            if tables_with_errors:
                debug_box(f"Tables with errors: {', '.join(tables_with_errors)}")
            return "Could not retrieve schema information for any tables."

        # Format schema information
        if use_postgres:
            # Format schema information manually with similar structure as SupabaseClient does
            schema_text = format_schema_from_postgres_data(tables_info)
            return schema_text
        else:
            # Use Supabase client's formatter
            return db_client.format_schema_for_agent(tables_info)
            
    except Exception as e:
        trace = traceback.format_exc()
        print(f"Error injecting schema info: {str(e)}\n{trace}")
        return None

# DB Commands
@command()
async def query_db(table: str, select: str = "*", filters: Dict[str, Any] = None, 
                  order: str = None, limit: int = None, offset: int = None,
                  raw_filters: str = None, context=None):
    """Query records from a database table.

    Args:
        table: Name of the table to query
        select: Columns to select (default: "*")
        filters: Dictionary of column-value pairs to filter by
        order: Column to order by (format: "column.asc" or "column.desc")
        limit: Maximum number of records to return
        offset: Number of records to skip
        raw_filters: Comma-separated list of raw filter expressions in the format 
                    "column.operator.value". Supports all Supabase filter operators.
                    Example: "status.eq.active,created_at.gt.2025-01-01,email.like.%example.com"
                    This provides more advanced filtering than the simple filters parameter.

    Example:
        {"query_db": {"table": "users", "select": "*", "filters": {"role": "admin"}, "limit": 10}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        results = await db_client.query_table(
            table=table,
            select=select,
            filters=filters,
            order=order,
            limit=limit,
            offset=offset,
            raw_filters=raw_filters
        )

        # Format results
        if not results:
            return f"No records found in table '{table}' matching the criteria."

        # Return as formatted string (easier for AI to read)
        formatted_results = json.dumps(results, indent=2)
        return f"Query results from '{table}':\n\n```json\n{formatted_results}\n```"

    except Exception as e:
        return format_error_response(e)

@command()
async def insert_db(table: str, data: Dict[str, Any], context=None):
    """Insert a new record into a database table.

    Args:
        table: Name of the table to insert into
        data: Dictionary of column-value pairs

    Example:
        {"insert_db": {"table": "tasks", "data": {"title": "New task", "status": "pending"}}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        result = await db_client.insert_record(table=table, data=data)

        if not result:
            return f"Record was inserted into '{table}', but no data was returned."

        formatted_result = json.dumps(result, indent=2)
        return f"Successfully inserted record into '{table}':\n\n```json\n{formatted_result}\n```"

    except Exception as e:
        return format_error_response(e)

@command()
async def update_db(table: str, data: Dict[str, Any], filters: Dict[str, Any] = None, 
                   raw_filters: str = None, context=None):
    """Update existing records in a database table.

    Args:
        table: Name of the table to update
        data: Dictionary of column-value pairs to update
        filters: Dictionary of column-value pairs to filter by using equality (column = value)
        raw_filters: Comma-separated list of raw filter expressions in the format 
                    "column.operator.value". Supports all Supabase filter operators.
                    Example: "status.eq.active,created_at.gt.2025-01-01,email.like.%example.com"

    Example:
        {"update_db": {"table": "tasks", "data": {"status": "completed"}, "filters": {"id": 123}}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        results = await db_client.update_records(
            table=table,
            data=data,
            filters=filters or {},
            raw_filters=raw_filters
        )

        if not results:
            return f"No records in '{table}' were updated matching the filter criteria."

        count = len(results)
        formatted_results = json.dumps(results, indent=2)
        return f"Successfully updated {count} record(s) in '{table}':\n\n```json\n{formatted_results}\n```"

    except Exception as e:
        return format_error_response(e)

@command()
async def delete_db(table: str, filters: Dict[str, Any] = None, 
                   raw_filters: str = None, context=None):
    """Delete records from a database table.

    Args:
        table: Name of the table to delete from
        filters: Dictionary of column-value pairs to filter by using equality (column = value)
        raw_filters: Comma-separated list of raw filter expressions in the format 
                    "column.operator.value". Supports all Supabase filter operators.
                    Example: "status.eq.active,created_at.gt.2025-01-01,email.like.%example.com"

    Example:
        {"delete_db": {"table": "tasks", "filters": {"id": 123}}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        results = await db_client.delete_records(
            table=table,
            filters=filters or {},
            raw_filters=raw_filters
        )

        if not results:
            return f"No records in '{table}' were deleted matching the filter criteria."

        count = len(results)
        formatted_results = json.dumps(results, indent=2)
        return f"Successfully deleted {count} record(s) from '{table}':\n\n```json\n{formatted_results}\n```"

    except Exception as e:
        return format_error_response(e)

@command()
async def list_db_tables(context=None):
    """List all available tables in the database.

    Example:
        {"list_db_tables": {}}
    """
    try:
        # Try to use PostgreSQL client for schema information first
        try:
            pg_client = PostgresClient.get_instance()
            tables = pg_client.list_tables()
            
            if not tables:
                # Fall back to Supabase if PostgreSQL returns no tables
                db_client = await get_db_client()
                if not db_client:
                    return "Error: Database client unavailable"
                tables = await db_client.list_tables()
        except ValueError as e:
            # Fall back to Supabase if PostgreSQL is not available
            print(f"PostgreSQL client not available: {e}. Falling back to Supabase client.")
            db_client = await get_db_client()
            if not db_client:
                return "Error: Database client unavailable"
            tables = await db_client.list_tables()

        if not tables:
            return "No tables found in the database."

        # Extract table names from table information
        if isinstance(tables[0], dict) and 'table_name' in tables[0]:
            table_names = [t.get('table_name') for t in tables if t.get('table_name')]
        else:
            table_names = tables

        return "Available tables in database:\n\n" + "\n".join([f"- {table}" for table in table_names])

    except Exception as e:
        return format_error_response(e)

@command()
async def describe_db_table(table: str, context=None):
    """Get detailed schema information for a specific table.

    Args:
        table: Name of the table to describe

    Example:
        {"describe_db_table": {"table": "users"}}
    """
    try:
        # Try to use PostgreSQL client for schema information first
        try:
            pg_client = PostgresClient.get_instance()
            # Get columns
            columns = pg_client.describe_table(table)
            # Get relationships
            relationships = pg_client.get_table_relationships(table)
        except ValueError as e:
            # Fall back to Supabase if PostgreSQL is not available
            print(f"PostgreSQL client not available: {e}. Falling back to Supabase client.")
            db_client = await get_db_client()
            if not db_client:
                return "Error: Database client unavailable"
            # Get columns
            columns = await db_client.describe_table(table)
            # Get relationships
            relationships = await db_client.get_table_relationships(table)

        if not columns:
            return f"Table '{table}' not found or has no columns."

        # Format schema information
        output = f"Schema for table '{table}':\n\n"

        # Add columns
        output += "Columns:\n"
        for col in columns:
            nullable = "NULL" if col.get("is_nullable") == "YES" else "NOT NULL"
            default = f" DEFAULT {col.get('column_default')}" if col.get("column_default") else ""
            output += f"  - {col.get('column_name')}: {col.get('data_type')} {nullable}{default}\n"

        # Add relationships
        if relationships:
            output += "\nRelationships:\n"
            for rel in relationships:
                output += f"  - {rel.get('column_name')} → {rel.get('foreign_table_name')}.{rel.get('foreign_column_name')}\n"

        return output

    except Exception as e:
        return format_error_response(e)

@command()
async def get_db_relationships(table: str = None, context=None):
    """Get information about relationships between tables.

    Args:
        table: Optional name of the table to get relationships for

    Example:
        {"get_db_relationships": {"table": "posts"}}
    """
    try:
        # Try to use PostgreSQL client for schema information first
        try:
            pg_client = PostgresClient.get_instance()
            relationships = pg_client.get_table_relationships(table)
        except ValueError as e:
            # Fall back to Supabase if PostgreSQL is not available
            print(f"PostgreSQL client not available: {e}. Falling back to Supabase client.")
            db_client = await get_db_client()
            if not db_client:
                return "Error: Database client unavailable"
            relationships = await db_client.get_table_relationships(table)

        if not relationships:
            if table:
                return f"No relationships found for table '{table}'."
            else:
                return "No relationships found in the database."

        # Format relationships
        output = f"Relationships{' for table ' + table if table else ''}:\n\n"

        current_table = None
        for rel in relationships:
            table_name = rel.get('table_name')
            if table_name != current_table:
                current_table = table_name
                output += f"Table '{table_name}':\n"

            output += f"  - {rel.get('column_name')} → {rel.get('foreign_table_name')}.{rel.get('foreign_column_name')}\n"

        return output

    except Exception as e:
        return format_error_response(e)

# Pipe to inject schema info into the first system message
@pipe(name='filter_messages', priority=10)
async def inject_db_schema(data: dict, context=None) -> dict:
    """Inject database schema information into the system message."""
    try:
        debug_box("Starting inject_db_schema pipe")

        # Skip if no messages
        if 'messages' not in data or not isinstance(data['messages'], list) or not data['messages']:
            debug_box("Aborting schema injection, missing messages")
            return data

        has_system_message = data['messages'] and data['messages'][0]['role'] == 'system'

        # Get agent name from context
        try:
            agent_name = context.agent_name
            if not agent_name:
                debug_box("Aborting inject schema because no agent name")
                return data
        except Exception as e:
            print(f"Error accessing agent_name from context: {e}")
            return data

        # Load agent DB settings
        settings = load_agent_db_settings(agent_name)
        enabled_tables = settings.get("enabled_tables", [])
        enabled_tables = None
        
        # Check if schema information already exists in system message
        schema_exists = False
        if has_system_message and isinstance(data['messages'][0].get('content'), str):
            system_content = data['messages'][0].get('content', '')
            schema_exists = DB_SCHEMA_START_DELIMITER in system_content and DB_SCHEMA_END_DELIMITER in system_content
        
        debug_box(f"Schema exists in system message: {schema_exists}")

        # Only query database for schema if it doesn't already exist in system message
        schema_info = None
        if not schema_exists:
            # If no tables are specifically enabled, we'll get all tables
            tables_to_use = enabled_tables if enabled_tables else None
            schema_info = await db_inject_schema_info(agent_name, tables_to_use)
            debug_box(f"Generated schema info: {schema_info is not None}")

        # Skip if no schema info
        if not schema_info:
            debug_box("No schema info generated")
            return data

        # Add schema info to system message (first message)
        if has_system_message:
            system_msg = data['messages'][0]
            debug_box("Adding schema to system message")
            
            # Add delimited schema info
            delimited_schema = f"\n\n{DB_SCHEMA_START_DELIMITER}\n{schema_info}\n{DB_SCHEMA_END_DELIMITER}"

            if isinstance(system_msg.get('content'), str):
                system_msg['content'] += delimited_schema
                debug_box("Added schema to system message content")
            elif isinstance(system_msg.get('content'), list):
                # Handle multipart messages
                system_msg['content'].append({
                    "type": "text",
                    "text": delimited_schema
                })

            debug_box("Schema injection complete")
        else:
            debug_box("No system message to add schema to")

        return data

    except Exception as e:
        trace = traceback.format_exc()
        print(f"Error in inject_db_schema pipe: {str(e)}\n{trace}")
        return data
