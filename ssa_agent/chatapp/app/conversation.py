import psycopg 
from databricks.sdk import WorkspaceClient
from uuid import uuid4
from datetime import datetime
from typing import Optional, List, Dict, Any

class ConversationDB:
    def __init__(self, online_store: str, catalog_name: str):
        self.online_store = online_store
        self.catalog_name = catalog_name
        self.conn = None
        self._connect()
        self._create_table_if_not_exists()
    
    def _connect(self):
        """Establish database connection using Databricks workspace client"""
        workspace = WorkspaceClient()    

        current_user = workspace.current_user.me().user_name
        
        instance = workspace.database.get_database_instance(name=self.online_store)
        cred = workspace.database.generate_database_credential(
            request_id=str(uuid4()), 
            instance_names=[self.online_store]
        )
        
        self.conn = psycopg.connect(
            host=instance.read_write_dns,
            dbname=self.catalog_name,
            user=current_user,
            password=cred.token,
            sslmode="require"
        )
        
        with self.conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            print(f"[SUCCESS] Connected to: {version}")
    
    def _create_table_if_not_exists(self):
        """Create conversation_history table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS conversation_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            thread_id VARCHAR(255) NOT NULL, 
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            user_message TEXT,
            assistant_message TEXT
        )
        """
        
        with self.conn.cursor() as cur:
            cur.execute(create_table_sql)
            self.conn.commit()
            print("[SUCCESS] Conversation history table ready")
    
    def save_conversation(
        self, 
        thread_id: str,
        user_message: Optional[str] = None,
        assistant_message: Optional[str] = None,
    ) -> str:
        """Save a conversation exchange to the database"""
        insert_sql = """
        INSERT INTO conversation_history 
        (thread_id, timestamp, user_message, assistant_message)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """
        
        with self.conn.cursor() as cur:
            cur.execute(insert_sql, (
                thread_id,
                datetime.now(),
                user_message,
                assistant_message,
            ))
            conversation_id = cur.fetchone()[0]
            self.conn.commit()
            return str(conversation_id)
    
    def get_conversation_history(
        self, 
        thread_id: str, 
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Retrieve conversation history for a Slack thread"""
        select_sql = """
        SELECT id, thread_id, timestamp, user_message, assistant_message
        FROM conversation_history 
        WHERE thread_id = %s 
        ORDER BY timestamp DESC 
        LIMIT %s
        """
        
        with self.conn.cursor() as cur:
            cur.execute(select_sql, (thread_id, limit))
            rows = cur.fetchall()
            
            conversations = []
            for row in rows:
                conversations.append({
                    'id': str(row[0]),
                    'thread_id': row[1],
                    'timestamp': row[2],
                    'user_message': row[3],
                    'assistant_message': row[4],
                })
            
            return conversations
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
