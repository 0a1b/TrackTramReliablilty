Configuration (to be implemented in later steps)

- config/local.yml (gitignored) will hold:
  - db_url: e.g., sqlite:///./data/reliability.db or postgres://...
  - polling_interval_seconds: default 300
  - stations:
      - names: ["Hauptbahnhof", "Marienplatz"]
      - ids: ["de:09162:1"]
  - log_level: INFO
