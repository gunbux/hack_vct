FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the importer script and wait-for-it.sh
COPY importer.py .
COPY wait-for-it.sh .

# Make the script executable
RUN chmod +x wait-for-it.sh

# Run the importer script after waiting for PostgreSQL
CMD ["./wait-for-it.sh", "mysql:3306", "--", "python", "importer.py"]

