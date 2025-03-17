from lib.providers.services import service
from lib.providers.commands import command
from lib.pipelines.pipe import pipe
from typing import Dict, List, Any, Optional, Union
import os
import json
import traceback
from .client import SupabaseClient
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
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        # If no tables specified, load from agent settings
        if tables is None:
            settings = load_agent_db_settings(agent_name)
            tables = settings.get("enabled_tables", [])

        if not tables:
            return None

        # Get schema information for each table
        tables_info = {}
        for table in tables:
            # Get columns
            columns = await db_client.describe_table(table)

            # Get relationships
            relationships = await db_client.get_table_relationships(table)

            tables_info[table] = {
                "columns": columns,
                "relationships": relationships
            }

        # Format schema information
        return db_client.format_schema_for_agent(tables_info)

    except Exception as e:
        trace = traceback.format_exc()
        print(f"Error injecting schema info: {str(e)}\n{trace}")
        return None

# DB Commands
@command()
async def query_db(table: str, select: str = "*", filters: Dict[str, Any] = None, 
                  order: str = None, limit: int = None, offset: int = None, context=None):
    """Query records from a database table.

    Args:
        table: Name of the table to query
        select: Columns to select (default: "*")
        filters: Dictionary of column-value pairs to filter by
        order: Column to order by (format: "column.asc" or "column.desc")
        limit: Maximum number of records to return
        offset: Number of records to skip

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
            offset=offset
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
async def update_db(table: str, data: Dict[str, Any], filters: Dict[str, Any], context=None):
    """Update existing records in a database table.

    Args:
        table: Name of the table to update
        data: Dictionary of column-value pairs to update
        filters: Dictionary of column-value pairs to filter by

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
            filters=filters
        )

        if not results:
            return f"No records in '{table}' were updated matching the filter criteria."

        count = len(results)
        formatted_results = json.dumps(results, indent=2)
        return f"Successfully updated {count} record(s) in '{table}':\n\n```json\n{formatted_results}\n```"

    except Exception as e:
        return format_error_response(e)

@command()
async def delete_db(table: str, filters: Dict[str, Any], context=None):
    """Delete records from a database table.

    Args:
        table: Name of the table to delete from
        filters: Dictionary of column-value pairs to filter by

    Example:
        {"delete_db": {"table": "tasks", "filters": {"id": 123}}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        results = await db_client.delete_records(
            table=table,
            filters=filters
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
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        tables = await db_client.list_tables()

        if not tables:
            return "No tables found in the database."

        return "Available tables in database:\n\n" + "\n".join([f"- {table}" for table in tables])

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
async def execute_db_query(query: str, context=None):
    """Run a simplified SQL-like query (with limited support).
    
    Args:
        query: SQL query to execute
            
    Example:
        {"execute_db_query": {"query": "SELECT * FROM users WHERE email LIKE '%example.com'"}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        # Parse the query to determine what to do
        query = query.strip()
        query_lower = query.lower()
        
        # Simple SELECT query parser
        if query_lower.startswith("select"):
            # Try to parse a simple query like: SELECT columns FROM table WHERE condition
            parts = query_lower.split()
            if "from" not in parts:
                return "Error: FROM clause required in SELECT query"
            
            # Extract the column list
            from_index = parts.index("from")
            columns = query[len("select"):].strip().split("from")[0].strip()
            
            # Extract the table name
            table_name = parts[from_index + 1].strip().rstrip(';')
            
            # Extract WHERE conditions if present
            filters = {}
            if "where" in parts:
                where_index = parts.index("where")
                conditions_text = query.split("where", 1)[1].strip().rstrip(';')
                
                # Very simple condition parser (only handles equals conditions)
                conditions = conditions_text.split("and")
                for condition in conditions:
                    if "=" in condition:
                        col, val = condition.split("=", 1)
                        filters[col.strip()] = val.strip().strip('\'"')
            
            # Execute the query using the client's query_table method
            results = await db_client.query_table(
                table=table_name,
                select=columns,
                filters=filters
            )
            
        else:
            return "Error: Only SELECT queries are supported. For other operations, use specific commands like query_db, insert_db, etc."

        if not results:
            return "Query executed successfully but returned no results."

        formatted_results = json.dumps(results, indent=2)
        return f"Query results:\n\n```json\n{formatted_results}\n```"

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
            return data

        has_system_message = data['messages'] and data['messages'][0]['role'] == 'system'

        # Get agent name from context
        try:
            agent_name = context.agent_name
            if not agent_name:
                return data
        except Exception as e:
            print(f"Error accessing agent_name from context: {e}")
            return data

        # Load agent DB settings
        settings = load_agent_db_settings(agent_name)
        enabled_tables = settings.get("enabled_tables", [])

        # Skip if no tables enabled
        if not enabled_tables:
            return data

        # Only clean db schema from non-system messages
        data['messages'] = [
            msg if i == 0 and msg['role'] == 'system' else 
            {
                **msg,
                'content': msg['content'] if not isinstance(msg.get('content'), str) else 
                           msg['content'].replace(
                               DB_SCHEMA_START_DELIMITER, '').replace(DB_SCHEMA_END_DELIMITER, '')
            }
            for i, msg in enumerate(data['messages'])
        ]
        
        # Check if schema information already exists in system message
        schema_exists = False
        if has_system_message and isinstance(data['messages'][0].get('content'), str):
            system_content = data['messages'][0].get('content', '')
            schema_exists = DB_SCHEMA_START_DELIMITER in system_content and DB_SCHEMA_END_DELIMITER in system_content
        
        debug_box(f"Agent {agent_name} has {len(enabled_tables)} enabled tables. Schema exists: {schema_exists}")

        # Only query database for schema if it doesn't already exist in system message
        schema_info = None
        if not schema_exists:
            schema_info = await db_inject_schema_info(agent_name, enabled_tables)

        # Skip if no schema info
        if not schema_info:
            return data

        # Add schema info to system message (first message)
        if has_system_message:
            system_msg = data['messages'][0]

            # Add delimited schema info
            delimited_schema = f"\n\n{DB_SCHEMA_START_DELIMITER}\n{schema_info}\n{DB_SCHEMA_END_DELIMITER}"

            if isinstance(system_msg.get('content'), str):
                system_msg['content'] += delimited_schema
            elif isinstance(system_msg.get('content'), list):
                # Handle multipart messages
                system_msg['content'].append({
                    "type": "text",
                    "text": delimited_schema
                })

            debug_box("Added schema information to system message")

        return data

    except Exception as e:
        trace = traceback.format_exc()
        print(f"Error in inject_db_schema pipe: {str(e)}\n{trace}")
        return data
