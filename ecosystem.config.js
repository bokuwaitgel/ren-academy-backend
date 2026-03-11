module.exports = {
  apps: [
    {
      name: "ren-academy-api",
      script: ".venv/Scripts/gunicorn",   // Windows venv path; use .venv/bin/gunicorn on Linux
      args: "serve:app --worker-class uvicorn.workers.UvicornWorker --workers 4 --bind 0.0.0.0:8000 --timeout 120",
      cwd: __dirname,
      interpreter: "none",
      env: {
        PYTHONPATH: ".",
        RELOAD: "false",
      },
      // Restart policy
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      // Logging
      out_file: "logs/out.log",
      error_file: "logs/error.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
