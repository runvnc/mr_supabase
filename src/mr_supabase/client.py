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
    
    def _apply_raw_filters(self, query, raw_filters: str):
        """Apply raw filters to a Supabase query.
        
        Args:
            query: The Supabase query object
            raw_filters: A comma-separated string of filter expressions in the format 
                        "column.operator.value,column.operator.value"
                        Example: "name.eq.John,age.gt.25,email.like.%example.com"
        
        Returns:
            The query with filters applied
        """
        if not raw_filters:
            return query
        
        filter_expressions = raw_filters.split(',')
        
        for expr in filter_expressions:
            parts = expr.strip().split('.')
            if len(parts) < 3:
                print(f"Warning: Invalid filter expression '{expr}'. Expected format: column.operator.value")
                continue
                
            column = parts[0]
            operator = parts[1]
            # Join the remaining parts with dots in case the value itself contains dots
            value = '.'.join(parts[2:])
            
            # Convert value if needed (handle string, number, boolean, null)
            if value.lower() == 'null':
                value = None
            elif value.lower() == 'true':
                value = True
            elif value.lower() == 'false':
                value = False
            elif value.isdigit():
                value = int(value)
            
            # Apply filter based on operator
            if hasattr(query, operator) and callable(getattr(query, operator)):
                query = getattr(query, operator)(column, value)
            else:
                print(f"Warning: Unsupported filter operator '{operator}'")
        
        return query
    
    async def query_table(
        self,
        table: str,
        select: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        raw_filters: Optional[str] = None
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
            raw_filters: Raw filters in the format "column.operator.value,column.operator.value"
            
        Returns:
            List of records as dictionaries
        """
        query = self.client.from_(table).select(select)
        
        # Apply filters
        if filters:
            for column, value in filters.items():
                query = query.eq(column, value)
        
        # Apply raw filters
        query = self._apply_raw_filters(query, raw_filters)
        
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
        data: Dict[str, Any],
        raw_filters: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Insert a new record into a table.
        
        Args:
            table: Name of the table to insert into
            data: Dictionary of column-value pairs
            raw_filters: Optional raw filters to apply after insertion
            
        Returns:
            Inserted record
        """
        response = self.client.from_(table).insert(data).execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase insert error: {response.error}")
            
        # TODO: Raw filters can't be applied to insert operations as currently structured
        # Would need to return a selection after insert to do filtering
        
        return response.data[0] if response.data else None
    
    async def update_records(
        self,
        table: str,
        data: Dict[str, Any],
        filters: Dict[str, Any],
        raw_filters: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Update records in a table based on filters.
        
        Args:
            table: Name of the table to update
            data: Dictionary of column-value pairs to update
            filters: Dictionary of column-value pairs to filter by
            raw_filters: Raw filters in the format "column.operator.value,column.operator.value"
            
        Returns:
            List of updated records
        """
        query = self.client.from_(table).update(data)
        
        # Apply filters
        for column, value in filters.items():
            query = query.eq(column, value)
        
        # Apply raw filters
        query = self._apply_raw_filters(query, raw_filters)
        
        response = query.execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase update error: {response.error}")
        
        return response.data
    
    async def delete_records(
        self,
        table: str,
        filters: Dict[str, Any],
        raw_filters: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Delete records from a table based on filters.
        
        Args:
            table: Name of the table to delete from
            filters: Dictionary of column-value pairs to filter by
            raw_filters: Raw filters in the format "column.operator.value,column.operator.value"
            
        Returns:
            List of deleted records
        """
        query = self.client.from_(table).delete()
        
        # Apply filters
        for column, value in filters.items():
            query = query.eq(column, value)
        
        # Apply raw filters
        query = self._apply_raw_filters(query, raw_filters)
        
        response = query.execute()
        
        # Check for errors
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase delete error: {response.error}")
        
        return response.data
    
    async def execute_sql(self, query: str, unsafe: bool = False) -> List[Dict[str, Any]]:
        """
        Execute a raw SQL query.
        
        Args:
            query: SQL query to execute
            unsafe: Whether to allow potentially unsafe operations (default: False)
            
        Returns:
            Query results
        """
        if not unsafe:
            # Simple security check to prevent destructive operations
            query_lower = query.lower().strip()
            if any(keyword in query_lower for keyword in ["drop", "truncate", "delete", "update", "alter"]):
                raise ValueError("Potentially destructive SQL operations are not allowed")
        
        # For Supabase, we can't directly execute arbitrary SQL through RPC
        # unless a specific function is created in the database
        raise NotImplementedError("Direct SQL execution is not supported by default in Supabase")

    async def query_information_schema(self, query_type: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Query information schema tables directly.
        
        This is a safer method to get database metadata without needing to execute raw SQL.
        """
        table = f"information_schema.{query_type}"
        query = self.client.from_(table).select('*')
        
        # Apply filters
        if filters:
            for column, value in filters.items():
                query = query.eq(column, value)
        
        # Apply raw filters
        query = self._apply_raw_filters(query, raw_filters)
        
        response = query.execute()
        
        return response.data
    
    async def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List of table names
        """
        results = await self.client.from_("information_schema.tables").select("table_name") \
            .eq("table_schema", "public") \
            .order("table_name") \
            .execute()
        
        # Extract table names from results
        return [record.get("table_name") for record in results.data if record.get("table_name")]
    
    async def describe_table(self, table: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table: Name of the table to describe
            
        Returns:
            List of column descriptions
        """
        results = await self.client.from_("information_schema.columns").select(
            "column_name,data_type,is_nullable,column_default"
        ) \
        .eq("table_schema", "public") \
        .eq("table_name", table) \
        .order("ordinal_position") \
        .execute()
        
        return results.data
    
    async def get_table_relationships(self, table: str = None) -> List[Dict[str, Any]]:
        """
        Get foreign key relationships for a table or all tables.
        
        Args:
            table: Optional name of the table to get relationships for
            
        Returns:
            List of relationship descriptions
        """
        # For getting relationships we need to use a more complex approach
        # since we need to join multiple tables
        # 
        # First, get constraint names for the specified table(s)
        constraints_query = self.client.from_("information_schema.table_constraints") \
            .select("constraint_name,table_name") \
            .eq("constraint_type", "FOREIGN KEY") \
            .eq("table_schema", "public")
            
        if table:
            constraints_query = constraints_query.eq("table_name", table)
            
        constraints = await constraints_query.execute()
        
        if not constraints.data:
            return []
            
        # Now get the column information for each constraint
        result = []
        for constraint in constraints.data:
            # Get the local column
            kcu_data = await self.client.from_("information_schema.key_column_usage") \
                .select("column_name") \
                .eq("constraint_name", constraint["constraint_name"]) \
                .eq("table_schema", "public") \
                .execute()
                
            # Get the referenced column
            ccu_data = await self.client.from_("information_schema.constraint_column_usage") \
                .select("table_name,column_name") \
                .eq("constraint_name", constraint["constraint_name"]) \
                .eq("table_schema", "public") \
                .execute()
                
            if kcu_data.data and ccu_data.data:
                result.append({
                    "table_name": constraint["table_name"],
                    "column_name": kcu_data.data[0]["column_name"],
                    "foreign_table_name": ccu_data.data[0]["table_name"],
                    "foreign_column_name": ccu_data.data[0]["column_name"]
                })
                
        return result
    
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
