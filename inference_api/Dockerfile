FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the app code
COPY ./app /app/app

# Install dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Expose the port
EXPOSE 8001

# Run the app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
