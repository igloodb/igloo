#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Update package lists
sudo apt-get update -y

# Install PostgreSQL and its client
sudo apt-get install -y postgresql postgresql-client

# Start PostgreSQL service
# It might already be started by the installation, but this ensures it is.
sudo systemctl start postgresql

# Wait for PostgreSQL to be ready
# Simple sleep, more robust checks might be needed in some environments
sleep 5

# Create database
sudo -u postgres psql -c "CREATE DATABASE igloo_test_db;" || echo "Database igloo_test_db may already exist."

# Create users table
sudo -u postgres psql -d igloo_test_db -c "
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"

# Insert sample data (optional, can be commented out)
# This uses a simple ON CONFLICT DO NOTHING to avoid errors if data with the same unique email already exists.
sudo -u postgres psql -d igloo_test_db -c "
INSERT INTO users (name, email) VALUES
('Alice Wonderland', 'alice@example.com'),
('Bob The Builder', 'bob@example.com'),
('Charlie Brown', 'charlie@example.com')
ON CONFLICT (email) DO NOTHING;
"

# Verify insertion (optional)
echo "Current users in igloo_test_db:"
sudo -u postgres psql -d igloo_test_db -c "SELECT * FROM users;"

echo "PostgreSQL setup complete. Database 'igloo_test_db' and table 'users' should be ready."
