### Introduction
#### Trading Strategy Playground
A web-based platform for systematic evaluation of trading strategies.

Users can define parameter ranges to perform combinatorial testing of strategy variations on 
historical stock time series data. The platform automates backtesting across all parameter 
combinations and surfaces performance metrics for comparative analysis.

### Tech Stack
1. PostgresSQL database (Running in Docker on local/cloud server)
2. Python backend codebase (FastAPI app, runs with Uvicorn, running in Docker on local/cloud server)
3. Redis (Caching/temp data for Python backend, running in Docker on local/cloud server)
4. React frontend (Vite + TypeScript, served as static files by Nginx on local/cloud server)
5. Nginx for SSL and routing (Runs directly on the local/cloud server)


### Developer Setup Instructions
#### Backend

1.	Create a local keys.list file with environment variables, and configure your Python run settings to use it.
2.	Install dependencies from requirements.txt.
3.	Clone the Git repository and create a development branch based on main.
4.	Push changes to the development branch and open a merge request to merge into main upon review.


#### Frontend

1. Save index.html to the local/cloud server


#### Docker setup instructions

Restart:
```
docker-compose up -d
```

If you make changes to producer/consumer code and want to rebuild:
```
docker-compose up --build
```