#!/usr/bin/env python3
"""
Clean Database Script
Clears all data from Show, SetlistEntry, Prediction, and PredictedSetlistEntry tables
"""

import subprocess
import requests
import sys
import json

def execute_clickhouse_query(query: str) -> bool:
    """Execute a query in ClickHouse via docker"""
    try:
        # Find the ClickHouse container
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=clickhouse", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        container_name = result.stdout.strip().split('\n')[0] if result.stdout.strip() else None
        
        if not container_name:
            print("❌ ClickHouse container not found")
            return False
        
        # Execute the query
        cmd = [
            "docker", "exec", "-i", container_name,
            "clickhouse-client",
            "--query", query
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Query failed: {result.stderr}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        return False

def get_table_counts() -> dict:
    """Get row counts for all tables"""
    counts = {}
    tables = ["Show", "SetlistEntry", "Prediction", "PredictedSetlistEntry", "PredictionMetadata"]
    
    for table in tables:
        query = f"SELECT COUNT(*) FROM local.{table}"
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=clickhouse", "--format", "{{.Names}}"],
                capture_output=True,
                text=True
            )
            container_name = result.stdout.strip().split('\n')[0] if result.stdout.strip() else None
            
            if container_name:
                cmd = [
                    "docker", "exec", "-i", container_name,
                    "clickhouse-client",
                    "--query", query
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    counts[table] = int(result.stdout.strip())
                else:
                    counts[table] = -1
            else:
                counts[table] = -1
        except:
            counts[table] = -1
    
    return counts

def clean_database():
    """Clean all data from the database tables"""
    
    print("🧹 CLEANING DATABASE")
    print("=" * 50)
    
    # Get initial counts
    print("\n📊 Current database state:")
    counts_before = get_table_counts()
    for table, count in counts_before.items():
        if count >= 0:
            print(f"   {table}: {count:,} rows")
        else:
            print(f"   {table}: unable to count")
    
    # Ask for confirmation
    print("\n⚠️  WARNING: This will delete ALL data from the database!")
    response = input("Are you sure you want to continue? (yes/no): ")
    
    if response.lower() != 'yes':
        print("❌ Cleanup cancelled")
        return False
    
    # Clean each table
    print("\n🗑️  Cleaning tables...")
    tables = ["Show", "SetlistEntry", "Prediction", "PredictedSetlistEntry", "PredictionMetadata"]
    
    for table in tables:
        print(f"   Truncating {table}...", end=" ")
        if execute_clickhouse_query(f"TRUNCATE TABLE local.{table}"):
            print("✅")
        else:
            print("❌")
    
    # Verify cleanup
    print("\n📊 Database state after cleanup:")
    counts_after = get_table_counts()
    all_clean = True
    
    for table, count in counts_after.items():
        if count >= 0:
            print(f"   {table}: {count:,} rows")
            if count > 0:
                all_clean = False
        else:
            print(f"   {table}: unable to verify")
            all_clean = False
    
    if all_clean:
        print("\n✅ Database successfully cleaned!")
        return True
    else:
        print("\n⚠️  Some tables may not be fully cleaned")
        return False

def verify_moose_running() -> bool:
    """Check if Moose is running"""
    try:
        response = requests.get("http://localhost:4000/health", timeout=2)
        return response.status_code == 200
    except:
        return False

def main():
    """Main cleanup function"""
    print("🗑️  DATABASE CLEANUP UTILITY")
    print("=" * 50)
    
    # Check if Moose is running
    if not verify_moose_running():
        print("❌ Moose dev server not running on localhost:4000")
        print("💡 Start it with: moose dev")
        sys.exit(1)
    
    print("✅ Moose dev server is running")
    
    # Clean the database
    if clean_database():
        print("\n🎉 Database cleanup complete!")
        print("💡 You can now ingest fresh data")
    else:
        print("\n❌ Database cleanup failed or was cancelled")
        sys.exit(1)

if __name__ == "__main__":
    main()