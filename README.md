# MindRoot Supabase Database Integration

This plugin integrates Supabase database functionality into MindRoot agents using generic database commands.

## Features

- Query, insert, update, and delete records in Supabase tables
- Get database schema information
- Execute custom SQL queries
- View table relationships
- Automatic schema injection into agent context

## Configuration

The plugin uses environment variables for configuration:

- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase service role key (for admin access)

For security in production environments, it's recommended to store these in the environment rather than hardcoding them.

## Commands Available to Agents

- `query_db`: Query records from a database table
- `insert_db`: Insert new records into a database table
- `update_db`: Update existing records in a database table
- `delete_db`: Delete records from a database table
- `list_db_tables`: List all available tables in the database
- `describe_db_table`: Get detailed schema information for a specific table
- `execute_db_query`: Run a custom SQL query (with safety checks)
- `get_db_relationships`: Get information about relationships between tables

## Usage Example

```json
{
  "query_db": {
    "table": "users", 
    "select": "id, name, email", 
    "filters": {"is_active": true}, 
    "limit": 10
  }
}
```
