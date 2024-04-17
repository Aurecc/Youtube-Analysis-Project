# YouTube Data Extraction and Database Management Script

## Overview

This repository contains a script designed to interface with the YouTube API to extract data about channels and videos, perform data cleaning, and subsequently upload the information to a PostgreSQL database. The script is structured to circumvent the YouTube API's daily request limit by rotating through seven different sets of channels, each corresponding to a day of the week. This ensures a diverse range of data is collected without exceeding the API's constraints. 

## Features

### Data Extraction
- **Channel Data**: The script extracts various details about YouTube channels, including channel names, channel IDs, and subscriber counts.
- **Video Data**: For each channel, the script collects data on videos such as video names, video IDs, views, and likes.

### Data Management
- **DataFrame Storage**: After extraction, the data is cleaned and stored in a pandas DataFrame for easy manipulation and analysis.
- **Database Connection**: The script establishes a connection with a PostgreSQL database to store the collected data.
- **Table Creation**: It automatically creates two separate tables within the database: one for channel data and another for video data.
- **Data Update**: Each time the script runs, it updates the existing tables with new data, ensuring that the database remains current.
- **Connection Termination**: Upon completing the data upload process, the script safely closes the database connection.

## Usage

### Prerequisites
- A valid YouTube API key with access to the required scopes for data retrieval.
- PostgreSQL installed and running on your system or accessible remotely.
- Python environment with necessary libraries installed (e.g., `google-api-python-client`, `pandas`, `psycopg2`).

### Running the Script
1. Clone this repository to your local machine.
2. Ensure that you have set up your PostgreSQL database with the appropriate user permissions.
3. Enter your YouTube API key and PostgreSQL database credentials in the designated areas within the script.
4. Run the script using Python on the appropriate day to target the set of channels designated for that weekday.
5. The script will handle all phases of data extraction, cleaning, and uploading automatically.

## Notes
- It is crucial to respect the YouTube API's terms of service and request limits when using this script.
- Ensure that your database schema matches the expectations of the script. If necessary, modify the table creation queries within the script accordingly.
- Regularly monitor the performance of the script and database to ensure data integrity and consistency.

## Contributions
Contributions are welcome. If you would like to improve the script or add new features, please fork this repository, make your changes, and submit a pull request.

