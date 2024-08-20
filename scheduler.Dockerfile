FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the 'praice' directory into the container at /app
COPY praice /app/praice

# Install any needed packages specified in requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Run scheduler.py when the container launches
CMD ["python", "-m", "praice.scheduler"]
