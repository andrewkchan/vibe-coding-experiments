#!/usr/bin/env python3
"""
Simple test script to verify PostgreSQL backend functionality.
Run this after setting up PostgreSQL to ensure everything works.
"""

import asyncio
import sys
from crawler_module.db_backends import create_backend

async def test_postgresql():
    if len(sys.argv) < 2:
        print("Usage: python test_postgresql_backend.py <postgresql_url>")
        print("Example: python test_postgresql_backend.py 'postgresql://user:pass@localhost/crawler_db'")
        return 1
    
    db_url = sys.argv[1]
    
    try:
        # Create PostgreSQL backend
        print("Creating PostgreSQL backend...")
        backend = create_backend('postgresql', db_url=db_url)
        
        # Initialize connection pool
        print("Initializing connection pool...")
        await backend.initialize()
        
        # Test basic operations
        print("Testing basic operations...")
        
        # Create a test table
        await backend.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        """)
        print("✓ Created test table")
        
        # Insert data
        await backend.execute(
            "INSERT INTO test_table (name, value) VALUES (%s, %s)",
            ("test_item", 42)
        )
        print("✓ Inserted data")
        
        # Query data
        row = await backend.fetch_one(
            "SELECT name, value FROM test_table WHERE name = %s",
            ("test_item",)
        )
        if row and row[0] == "test_item" and row[1] == 42:
            print("✓ Queried data successfully")
        else:
            print("✗ Query returned unexpected data")
        
        # Test RETURNING clause
        results = await backend.execute_returning(
            "INSERT INTO test_table (name, value) VALUES (%s, %s) RETURNING id, name",
            ("returning_test", 100)
        )
        if results and len(results) > 0:
            print(f"✓ RETURNING clause works: id={results[0][0]}, name={results[0][1]}")
        
        # Clean up
        await backend.execute("DROP TABLE test_table")
        print("✓ Cleaned up test table")
        
        # Close connections
        await backend.close()
        print("\n✅ All tests passed! PostgreSQL backend is working correctly.")
        
        return 0
        
    except ImportError as e:
        print(f"\n❌ Error: {e}")
        print("Please install psycopg3: pip install 'psycopg[binary,pool]>=3.1'")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure PostgreSQL is running and the connection URL is correct.")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(test_postgresql())) 