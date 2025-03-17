import os
from typing import Dict, List, Any, Optional, Union
import json
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

class SupabaseClient:
    """Client for interacting with Supabase database."""
    
    _instance = None
    
    @classmethod
    def get_instance(cls) -> 'SupabaseClient':
        """Get or create a singleton instance of the Supabase client."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize the Supabase client using environment variables."""
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError(
                "Supabase URL and key must be set as environment variables "
                "(SUPABASE_URL, SUPABASE_KEY)"
            )
        
        self.client = create_client(self.supabase_url, self.supabase_key)
    
    def get_client(self) -> Client:
        """Get the raw Supabase client."""
        return self.client
    
    async def query_table(
        self,
        table: str,
        select: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query records from a table with filters and pagination.
        
        Args:
            table: Name of the table to query
            select: Columns to select (default: "*")
            filters: Dictionary of column-value pairs to filter by
            order: Column to order by (format: "column.asc" or "column.desc")
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of records as dictionaries
        """
        query = self.client.from_(table).select(select)
        
        # Apply filters
        if filters:
            for column, value in filters.items():
                query = query.eq(column, value)
        
        # Apply order
        if order:
            col, direction = order.split(".") if "." in order else (order, "asc")
            query = query.order(col, desc=(direction == "desc"))
        
        # Apply pagination
        if limit:
            query = query.limit(limit)
        
        if offset:
            query = query.offset(offset)
        
        # Execute query
        response = query.execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase query error: {response.error}")
        
        return response.data
    
    async def insert_record(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Insert a new record into a table.
        
        Args:
            table: Name of the table to insert into
            data: Dictionary of column-value pairs
            
        Returns:
            Inserted record
        """
        response = self.client.from_(table).insert(data).execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase insert error: {response.error}")
        
        return response.data[0] if response.data else None
    
    async def update_records(
        self,
        table: str,
        data: Dict[str, Any],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Update records in a table based on filters.
        
        Args:
            table: Name of the table to update
            data: Dictionary of column-value pairs to update
            filters: Dictionary of column-value pairs to filter by
            
        Returns:
            List of updated records
        """
        query = self.client.from_(table).update(data)
        
        # Apply filters
        for column, value in filters.items():
            query = query.eq(column, value)
        
        response = query.execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase update error: {response.error}")
        
        return response.data
    
    async def delete_records(
        self,
        table: str,
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Delete records from a table based on filters.
        
        Args:
            table: Name of the table to delete from
            filters: Dictionary of column-value pairs to filter by
            
        Returns:
            List of deleted records
        """
        query = self.client.from_(table).delete()
        
        # Apply filters
        for column, value in filters.items():
            query = query.eq(column, value)
        
        response = query.execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase delete error: {response.error}")
        
        return response.data
    
    async def execute_sql(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a raw SQL query.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results
        """
        # Simple security check to prevent destructive operations
        query_lower = query.lower().strip()
        if any(keyword in query_lower for keyword in ["drop", "truncate", "delete", "update", "alter"]):
            raise ValueError("Potentially destructive SQL operations are not allowed")
        
        response = self.client.rpc("execute_sql", {"query": query}).execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase SQL error: {response.error}")
        
        return response.data
    
    async def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List of table names
        """
        # PostgreSQL query to get all tables
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
        """
        
        response = await self.execute_sql(query)
        return [record.get("table_name") for record in response if record.get("table_name")]
    
    async def describe_table(self, table: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table: Name of the table to describe
            
        Returns:
            List of column descriptions
        """
        # PostgreSQL query to get table schema
        query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable, 
            column_default
        FROM 
            information_schema.columns
        WHERE 
            table_schema = 'public' 
            AND table_name = '{table}'
        ORDER BY 
            ordinal_position;
        """
        
        return await self.execute_sql(query)
    
    async def get_table_relationships(self, table: str = None) -> List[Dict[str, Any]]:
        """
        Get foreign key relationships for a table or all tables.
        
        Args:
            table: Optional name of the table to get relationships for
            
        Returns:
            List of relationship descriptions
        """
        # Base query to get foreign key relationships
        query = """
        SELECT
            tc.table_schema,
            tc.constraint_name,
            tc.table_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
        """
        
        # Add table filter if specified
        if table:
            query += f" AND tc.table_name = '{table}'"
        
        query += " ORDER BY tc.table_name, kcu.column_name;"
        
        return await self.execute_sql(query)
    
    def format_schema_for_agent(self, tables_info: Dict[str, Any]) -> str:
        """
        Format schema information for injection into agent context.
        
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
