# Python image
FROM python:3.6-slim

# Stage our workspace
WORKDIR /app

# Get dependency manager
RUN pip install poetry==0.12.11

# Copy deps lists
COPY poetry.lock .
COPY pyproject.toml .

# Install deps
RUN poetry install

# Copy raw data and code
COPY export ./export
COPY *.py ./

# Run
CMD [ "poetry", "run", "python", "./sailthru.py" ]