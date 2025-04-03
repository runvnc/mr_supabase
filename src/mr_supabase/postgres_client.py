import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Optional, Union
import json
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

class PostgresClient:
    """Client for direct PostgreSQL connection to access schema information."""
    
    _instance = None
    
    @classmethod
    def get_instance(cls) -> 'PostgresClient':
        """Get or create a singleton instance of the PostgreSQL client."""
        try:
            cls._instance.close()
        except Exception:
            pass
        cls._instance = cls()
       
        return cls._instance
    
    def __init__(self):
        """Initialize the PostgreSQL client using environment variables."""
        self.connection_string = os.environ.get("POSTGRES_CONNECTION_STRING")
        self.conn = None
        
        if not self.connection_string:
            raise ValueError(
                "PostgreSQL connection string must be set as environment variable "
                "(POSTGRES_CONNECTION_STRING)"
            )
    
    def _get_connection(self):
        """Get a PostgreSQL connection, creating one if needed."""
        if self.conn is None or self.conn.closed:
            print("Tryin to connect to postgres, connection string is ", self.connection_string)
            self.conn = psycopg2.connect(self.connection_string)
        return self.conn
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            List of records as dictionaries
        """
        conn = self._get_connection()
        try:
            # Convert dictionary params to tuple if needed
            if isinstance(params, dict):
                # If the query uses named parameters (%s(name)s) keep it as a dict
                # Otherwise convert to a tuple for positional parameters (%s)
                if "%(" in query and ")s" in query:
                    pass  # Keep as dict for named parameters
                else: 
                    # Convert dict to tuple for positional parameters
                    params = tuple(params.values())
            elif params is None:
                params = ()
                
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:                
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            conn.rollback()
            raise e
    
    def list_tables(self) -> List[Dict[str, Any]]:
        """List all tables in the public schema.
        
        Returns:
            List of table information
        """
        query = """
        SELECT 
            table_name,
            table_type
        FROM 
            information_schema.tables
        WHERE 
            table_schema = 'public'
        ORDER BY 
            table_name
        """
        return self.execute_query(query)
    
    def describe_table(self, table: str) -> List[Dict[str, Any]]:
        """Get detailed schema information for a table.
        
        Args:
            table: Name of the table to describe
            
        Returns:
            List of column descriptions
        """
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM 
            information_schema.columns
        WHERE 
            table_schema = 'public'
            AND table_name = %s
        ORDER BY 
            ordinal_position;
        """
        return self.execute_query(query, (table,))
    
    def get_table_relationships(self, table: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get foreign key relationships for a table or all tables.
        
        Args:
            table: Optional name of the table to get relationships for
            
        Returns:
            List of relationship descriptions
        """
        query = """
        SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
        """
        
        if table:
            query += " AND tc.table_name = %s"
            return self.execute_query(query, (table,))
        else:
            return self.execute_query(query)
    
    def close(self):
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None
