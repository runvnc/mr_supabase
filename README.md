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

### Supabase Configuration (Required)
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase service role key (for admin access)

### Direct PostgreSQL Connection (Optional but Recommended)
- `POSTGRES_CONNECTION_STRING`: PostgreSQL connection string for direct database access
  - Format: `postgresql://postgres:[PASSWORD]@db.[PROJECT_ID].supabase.co:5432/postgres`
  - Available in Supabase dashboard: Project Settings > Database > Connection string > URI format
Using the direct PostgreSQL connection is recommended for reliable schema information injection, as the Supabase client has limitations when accessing information_schema tables. If not provided, the plugin will fall back to the Supabase client for all operations.

For security in production environments, it's recommended to store all credentials in the environment rather than hardcoding them.

## Commands Available to Agents

- `query_db`: Query records from a database table
- `insert_db`: Insert new records into a database table
- `update_db`: Update existing records in a database table
- `delete_db`: Delete records from a database table
- `list_db_tables`: List all available tables in the database
- `describe_db_table`: Get detailed schema information for a specific table
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

## Dual Client Architecture

This plugin uses a dual client architecture for optimal functionality:

1. **PostgreSQL Direct Connection**: Used specifically for accessing schema information (tables, columns, relationships)
   - Provides complete and reliable schema information for the agent context
   - Requires the `POSTGRES_CONNECTION_STRING` environment variable
   - Connects directly to the PostgreSQL database hosted by Supabase

2. **Supabase Client**: Used for all data operations (query, insert, update, delete)
   - Leverages Supabase's security and Row Level Security (RLS) features
   - Provides a consistent interface for database operations
   - Requires the `SUPABASE_URL` and `SUPABASE_KEY` environment variables

### Fallback Mechanism

If the PostgreSQL connection string is not provided or if there's an error connecting directly:

- The plugin automatically falls back to using the Supabase client for all operations
- Schema information may be limited when using only the Supabase client
- All commands will continue to function, but with potentially less detailed schema information
