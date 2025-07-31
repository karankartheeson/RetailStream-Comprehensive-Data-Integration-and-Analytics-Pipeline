#!/bin/bash
# Define variables
S3_FOLDER="s3://dmartfeatures-buc/features_dataset/"
LOCAL_DIR="/home/hadoop/features_dataset/"
HDFS_DIR="/user/hadoop/features_data/"
# Step 1: Create local directory if not exists
mkdir -p "$LOCAL_DIR"
# Step 2: Download all CSV files from S3 folder to local directory
echo "Downloading all CSV files from $S3_FOLDER to $LOCAL_DIR..."
aws s3 cp "$S3_FOLDER" "$LOCAL_DIR" --recursive --exclude "*" --include "*.csv"
# Step 3: Create HDFS directory if not exists
echo "Creating HDFS directory: $HDFS_DIR"
sudo -u hadoop hdfs dfs -mkdir -p "$HDFS_DIR"
# Step 4: Upload all local CSV files to HDFS
echo "Uploading all CSV files to HDFS..."
sudo -u hadoop hdfs dfs -put -f "$LOCAL_DIR"*.csv "$HDFS_DIR"
# Step 5: Verify
echo "✅ Listing files in HDFS directory $HDFS_DIR:"
sudo -u hadoop hdfs dfs -ls "$HDFS_DIR"
