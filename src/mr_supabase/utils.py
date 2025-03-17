import os
import json
from typing import Dict, List, Any, Optional
import traceback

# Path for storing agent DB settings
DB_SETTINGS_DIR = "data/db/agent_settings"
DB_SCHEMA_START_DELIMITER = "<!-- DB_SCHEMA_START -->"
DB_SCHEMA_END_DELIMITER = "<!-- DB_SCHEMA_END -->"

def ensure_settings_dir():
    """Ensure the settings directory exists."""
    os.makedirs(DB_SETTINGS_DIR, exist_ok=True)

def load_agent_db_settings(agent_name: str) -> Dict[str, Any]:
    """Load database settings for an agent.
    
    Args:
        agent_name: Name of the agent
        
    Returns:
        Dictionary with settings or empty dict if not found
    """
    settings_file = f"{DB_SETTINGS_DIR}/{agent_name}.json"
    
    if not os.path.exists(settings_file):
        return {}
    
    try:
        with open(settings_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading DB settings for agent {agent_name}: {e}")
        return {}

def save_agent_db_settings(agent_name: str, settings: Dict[str, Any]) -> bool:
    """Save database settings for an agent.
    
    Args:
        agent_name: Name of the agent
        settings: Dictionary with settings
        
    Returns:
        True if successful, False otherwise
    """
    ensure_settings_dir()
    settings_file = f"{DB_SETTINGS_DIR}/{agent_name}.json"
    
    try:
        with open(settings_file, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving DB settings for agent {agent_name}: {e}")
        return False

def clean_db_schema_from_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove DB schema information from old messages and leave in system message.
    
    This helps prevent context window from filling up with duplicate schema info.
    
    Args:
        messages: List of chat messages
        
    Returns:
        Cleaned message list
    """
    if not messages:
        return messages
    
    # Clean all non-system messages
    for i, msg in enumerate(messages):
        if msg.get('role') != 'system':
            if isinstance(msg.get('content'), str):
                content = msg['content']
                if DB_SCHEMA_START_DELIMITER in content and DB_SCHEMA_END_DELIMITER in content:
                    start_idx = content.find(DB_SCHEMA_START_DELIMITER)
                    end_idx = content.find(DB_SCHEMA_END_DELIMITER) + len(DB_SCHEMA_END_DELIMITER)
                    cleaned_content = content[:start_idx] + content[end_idx:]
                    messages[i]['content'] = cleaned_content
    
    return messages

def extract_schema_info(content: str) -> Optional[str]:
    """Extract DB schema information from a message.
    
    Args:
        content: Message content
        
    Returns:
        Schema information or None if not found
    """
    if DB_SCHEMA_START_DELIMITER in content and DB_SCHEMA_END_DELIMITER in content:
        start_idx = content.find(DB_SCHEMA_START_DELIMITER) + len(DB_SCHEMA_START_DELIMITER)
        end_idx = content.find(DB_SCHEMA_END_DELIMITER)
        return content[start_idx:end_idx].strip()
    return None

def format_error_response(error: Exception) -> str:
    """Format an error for response to the agent.
    
    Args:
        error: Exception object
        
    Returns:
        Formatted error message
    """
    trace = traceback.format_exc()
    return f"Error: {str(error)}\n\nDetails:\n{trace}"
