# mlops

Danish electricity consumption forecasting pipeline. Ingests hourly data from [Energi Data Service](https://www.energidataservice.dk/), stores it in MotherDuck (cloud DuckDB) via a bronze → silver medallion architecture, orchestrated by Dagster.

## Local development

```bash
make install   # install dependencies
make test      # run tests
make dev       # start Dagster UI at http://localhost:3000
```

Requires a `.env` file (copy from `.env.example` and fill in your `MOTHERDUCK_TOKEN`).

## VM setup (dev / staging / prod)

### 1. Deploy a server on UpCloud
- Location: DK-CPH1
- Plan: 1 core / 2 GB / 20 GB — €6/month
- OS: Ubuntu 24.04 LTS
- Login: add your SSH key before deploying

### 2. SSH in
```bash
ssh root@<vm-ip>
```

### 3. Run the setup script
```bash
curl -o setup.sh https://raw.githubusercontent.com/johannesemme/mlops/main/deploy/setup.sh
bash setup.sh https://github.com/johannesemme/mlops.git
```

The script will:
1. Install Node.js and Claude Code
2. Install uv
3. Clone the repo to `/opt/mlops`
4. Install Python dependencies
5. Open `.env` for editing — set `MOTHERDUCK_TOKEN`, `ENV` (dev/staging/prod), and `DAGSTER_HOME=/opt/dagster_home`
6. Install and start the Dagster systemd services

### 4. Access the Dagster UI
Open an SSH tunnel from your Mac:
```bash
ssh -L 3000:localhost:3000 root@<vm-ip>
```
Then open **http://localhost:3000** in your browser.

### 5. Deploy code updates
On your Mac, push changes to GitHub. Then on the VM:
```bash
cd /opt/mlops && git pull && systemctl restart dagster-webserver dagster-daemon
```

Or from your Mac in one command:
```bash
ssh root@<vm-ip> "cd /opt/mlops && git pull && systemctl restart dagster-webserver dagster-daemon"
```
