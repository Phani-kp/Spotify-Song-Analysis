import requests
import sqlite3
import pandas as pd
from datetime import datetime

# Step 1: Extract
def extract_github_data(owner, repo, token=None):
    """
    Extracts repository data from GitHub API.
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    headers = {"Authorization": f"token {token}"} if token else {}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# Step 2: Transform
def transform_github_data(raw_data):
    """
    Transforms raw GitHub data into a structured format.
    """
    transformed_data = []
    for issue in raw_data:
        transformed_data.append({
            "id": issue.get("id"),
            "title": issue.get("title"),
            "state": issue.get("state"),
            "created_at": issue.get("created_at"),
            "updated_at": issue.get("updated_at"),
            "closed_at": issue.get("closed_at"),
            "user": issue.get("user", {}).get("login"),
        })
    return transformed_data

# Step 3: Load
def load_data_to_db(transformed_data, db_name="github_data.db"):
    """
    Loads transformed data into an SQLite database.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS issues (
        id INTEGER PRIMARY KEY,
        title TEXT,
        state TEXT,
        created_at TEXT,
        updated_at TEXT,
        closed_at TEXT,
        user TEXT
    )
    """)
    
    # Insert data
    df = pd.DataFrame(transformed_data)
    df.to_sql("issues", conn, if_exists="append", index=False)
    
    conn.commit()
    conn.close()

# Main ETL Execution
def run_etl(owner, repo, token=None):
    """
    Runs the ETL pipeline.
    """
    print("Starting ETL pipeline...")
    
    # Extract
    print("Extracting data from GitHub...")
    raw_data = extract_github_data(owner, repo, token)
    
    # Transform
    print("Transforming data...")
    transformed_data = transform_github_data(raw_data)
    
    # Load
    print("Loading data into the database...")
    load_data_to_db(transformed_data)
    
    print("ETL pipeline completed successfully!")

# Example Usage
if __name__ == "__main__":
    # Replace 'owner' and 'repo' with the repository details you want to extract
    OWNER = "octocat"
    REPO = "Hello-World"
    TOKEN = None  # Optional GitHub personal access token for authenticated requests
    
    run_etl(OWNER, REPO, TOKEN)
